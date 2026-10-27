// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// THE APPLICATION API, over gRPC, the way an application beside the server
// reaches it on Windows and Linux alike: TCP on loopback, with the token.
// A call without the token is refused; Hello answers; a consumer group gets
// a device's message, with who published it; Publish reaches a device.

#include "TestClient.h"

#include "ShardedBroker.h"
#include "RetainedKeeper.h"
#include "AppHub.h"
#include "AppAdmin.h"
#include "ApiServer.h"
#include "modbee/app/v1/app.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <mutex>

using check::Client;
using check::Fleet;
using check::equal;
using check::ok;
using check::section;

namespace v1 = modbee::app::v1;

namespace {

const char* const kToken = "api-tests-token";

struct Server {
    AppHub hub{AppHubConfig()};
    AppAdmin admin;
    ShardExtensionChain chain;
    ShardedBroker broker;
    ApiServer api;
    std::string target;     // host:port for a gRPC channel
    bool started = false;

    Server() {
        chain.add(&hub);
        chain.add(&admin);
        ShardedBrokerConfig c;
        c.address = "127.0.0.1";
        c.port = 0;
        c.shards = 2;
        c.extension = &chain;
        if (!broker.start(c)) return;
        ApiServerConfig a;
        a.addresses.push_back("tcp://127.0.0.1:0");
        a.token = kToken;
        a.serverVersion = "modbee-mqtt test";
        a.serverId = "api-tests";
        ApiServer::Services s;
        s.messages = &hub;
        s.admin = &admin;
        s.server = &broker;
        std::string why;
        started = api.start(a, s, why);
        if (!started) std::printf("    api: %s\n", why.c_str());
        else target = api.listening()[0].substr(6);
    }
    ~Server() {
        api.stop();
        broker.stop();
    }
    std::shared_ptr<grpc::Channel> channel() {
        return grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    }
};

void authorize(grpc::ClientContext& context) {
    context.AddMetadata("authorization", std::string("Bearer ") + kToken);
}

void helloAndToken(Server& server) {
    section("loopback TCP with the token");
    ok(server.started, "the API listens on " + server.target);
    auto system = v1::System::NewStub(server.channel());
    {
        grpc::ClientContext context;
        v1::HelloRequest request;
        v1::HelloReply reply;
        equal<int>("without the token: UNAUTHENTICATED", system->Hello(&context, request, &reply).error_code(),
                   grpc::StatusCode::UNAUTHENTICATED);
    }
    grpc::ClientContext context;
    authorize(context);
    v1::HelloRequest request;
    request.set_api_version("1.0");
    v1::HelloReply reply;
    ok(system->Hello(&context, request, &reply).ok() && reply.server_id() == "api-tests",
       "with it: Hello answers, its server id");
}

void consumeAndPublish(Server& server) {
    section("Messages: a consumer group gets a device's message; Publish reaches a device");
    auto messages = v1::Messages::NewStub(server.channel());
    grpc::ClientContext context;
    authorize(context);
    std::unique_ptr<grpc::ClientReaderWriter<v1::ConsumeUp, v1::ConsumeDown>> stream(messages->Consume(&context));
    v1::ConsumeUp open;
    open.mutable_open()->set_group("ingest");
    open.mutable_open()->add_filters("plant/#");
    stream->Write(open);
    v1::ConsumeDown down;
    ok(stream->Read(&down) && down.has_opened(), "a member opens the group");

    std::mutex lock;
    std::vector<v1::Delivery> got;
    std::thread reader([&] {
        v1::ConsumeDown d;
        while (stream->Read(&d)) {
            if (!d.has_delivery()) continue;
            v1::ConsumeUp ack;
            ack.mutable_ack()->add_ids(d.delivery().id());
            stream->Write(ack);
            std::lock_guard<std::mutex> guard(lock);
            got.push_back(d.delivery());
        }
    });

    Fleet fleet(server.broker.port());
    Client& device = fleet.make("pump-1");
    device.subscribe("cmd/pump-1/#", MQ_QOS_1);
    ok(fleet.connected(device) && fleet.until([&] { return device.subscribed >= 1; }), "a device connects");
    device.publish("plant/pump-1/pressure", "3.2", MQ_QOS_1);
    const bool delivered = fleet.until([&] {
        std::lock_guard<std::mutex> guard(lock);
        return !got.empty();
    });
    {
        std::lock_guard<std::mutex> guard(lock);
        ok(delivered && got[0].topic() == "plant/pump-1/pressure" && got[0].payload() == "3.2" &&
               got[0].publisher().client_id() == "pump-1",
           "the group gets it, with the device that published it");
    }

    grpc::ClientContext publishContext;
    authorize(publishContext);
    v1::PublishRequest publish;
    publish.mutable_message()->set_topic("cmd/pump-1/speed");
    publish.mutable_message()->set_payload("1450");
    publish.mutable_message()->set_qos(1);
    publish.set_confirm(v1::CONFIRM_DELIVERED);
    v1::PublishReply reply;
    // The device answers while Publish waits for its acknowledgement: polled
    // on a thread of its own meanwhile.
    std::atomic<bool> waiting{true};
    std::thread devicePoll([&] {
        while (waiting) {
            fleet.poll();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    const grpc::Status status = messages->Publish(&publishContext, publish, &reply);
    waiting = false;
    devicePoll.join();
    if (!status.ok() || reply.outcome() != v1::PUBLISH_DELIVERED) {
        std::printf("    status %d %s, outcome %s, matched %u acknowledged %u\n", (int)status.error_code(),
                    status.error_message().c_str(), v1::PublishOutcome_Name(reply.outcome()).c_str(), reply.matched(),
                    reply.acknowledged());
    }
    ok(status.ok() && reply.outcome() == v1::PUBLISH_DELIVERED, "Publish, answered once the device acknowledged it");
    ok(fleet.until([&] { return !device.got.empty(); }) && device.got[0].payload == "1450", "the device has it");

    context.TryCancel();
    reader.join();
}

v1::Session sessionOf(Server& server, const std::string& clientId) {
    auto admin = v1::Admin::NewStub(server.channel());
    grpc::ClientContext context;
    authorize(context);
    v1::ListSessionsRequest request;
    request.set_client_id_prefix(clientId);
    v1::ListSessionsReply reply;
    admin->ListSessions(&context, request, &reply);
    for (const v1::Session& s : reply.sessions()) {
        if (s.client_id() == clientId) return s;
    }
    return v1::Session();
}

void sessionCounters(Server& server) {
    section("ListSessions: what each device has said and been sent, and how often it has connected");
    Fleet fleet(server.broker.port());
    Client& first = fleet.make("meter-9");
    first.persistent(60);                                    // its session outlives a connection
    first.subscribe("cmd/meter-9", MQ_QOS_1);
    ok(fleet.connected(first) && fleet.until([&] { return first.subscribed >= 1; }), "a device connects");
    const int64_t before = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch()).count();
    for (int n = 0; n < 3; n++) first.series("plant/meter-9/kwh", std::to_string(n), MQ_QOS_1);
    first.publish("cmd/meter-9", "read", MQ_QOS_1);          // to itself: one in, one out
    ok(fleet.until([&] { return first.completed >= 4 && !first.got.empty(); }), "  publishes 4, is sent 1");
    v1::Session s = sessionOf(server, "meter-9");
    equal<uint64_t>("  messages_in", s.messages_in(), 4u);
    equal<uint64_t>("  messages_out", s.messages_out(), 1u);
    ok(s.bytes_in() > 0 && s.bytes_out() > 0 && s.bytes_in() > s.bytes_out(), "  bytes both ways, more in than out");
    ok(s.last_activity_ms() >= before - 1000 && s.connects() == 1, "  its last packet just now, connected once");

    first.mqtt.disconnect();
    fleet.until([&] { return !first.mqtt.isConnected(); }, 1000);
    Client& resumed = fleet.make("meter-9");
    resumed.persistent(60);
    resumed.config.cleanStart = false;
    ok(fleet.connected(resumed), "it reconnects, resuming its session");
    v1::Session t = sessionOf(server, "meter-9");
    ok(t.connects() == 2 && t.messages_in() == 4 && t.bytes_in() > s.bytes_in(),
       "  connected twice, its counts kept, the new connection's bytes added");
    resumed.mqtt.disconnect();
    fleet.until([&] { return !resumed.mqtt.isConnected(); }, 1000);
    Client& clean = fleet.make("meter-9");                   // clean start: a new session, the same device
    clean.persistent(60);
    ok(fleet.connected(clean), "it connects again, starting clean");
    v1::Session u = sessionOf(server, "meter-9");
    ok(u.connects() == 3 && u.messages_in() == 4, "  a new MQTT session, the device's counts carried over");
}

// A port no one listens on: one the kernel picked, given back.
uint16_t freePort() {
    uint16_t port = 0;
    std::string error;
    const int socket = mq_socket::openListeningSocket("127.0.0.1", 0, false, 16, port, error);
    if (socket >= 0) MQ_CLOSE(socket);
    return port;
}

void listenersLive(Server& server) {
    section("Admin.PutListeners: the port moved and max packet lowered, while devices are connected");
    auto admin = v1::Admin::NewStub(server.channel());
    const uint16_t first = server.broker.port();
    Fleet before(first);
    Client& early = before.make("early");
    early.subscribe("echo/early", MQ_QOS_1);
    ok(before.connected(early) && before.until([&] { return early.subscribed >= 1; }),
       "a device connects on the first port");

    const uint16_t moved = freePort();
    v1::ListenersChange change;
    change.set_port(moved);
    change.set_max_packet(512);
    v1::Listeners reply;
    {
        grpc::ClientContext context;
        authorize(context);
        const grpc::Status status = admin->PutListeners(&context, change, &reply);
        ok(status.ok() && reply.listeners_size() == 1 && reply.listeners(0).port() == moved &&
               reply.max_packet() == 512,
           "PutListeners: the plain listener on another port, max packet 512");
    }
    early.publish("echo/early", std::string(1000, 'x'), MQ_QOS_1);
    ok(before.until([&] { return !early.got.empty(); }) && early.mqtt.isConnected(),
       "the device on the old port stays connected, and keeps the packet size it was told");

    Fleet after(moved);
    Client& late = after.make("late");
    ok(after.connected(late), "a new device connects on the new port");
    const MqttResult big = late.publish("echo/late", std::string(1000, 'x'), MQ_QOS_1);
    ok(big == MqttResult::PacketTooLarge || after.until([&] { return late.failed > 0; }),
       "  and is held to 512 bytes");
    Fleet gone(first);
    Client& stray = gone.make("stray");
    ok(!gone.connected(stray, 1000), "nothing listens on the first port");

    {
        grpc::ClientContext context;
        authorize(context);
        v1::ListenersChange caps;
        caps.set_queue(5);
        caps.set_queue_bytes(4096);
        caps.set_queue_bytes_total(1u << 20);
        v1::Listeners now;
        ok(admin->PutListeners(&context, caps, &now).ok() && now.queue() == 5 && now.queue_bytes() == 4096 &&
               now.queue_bytes_total() == (1u << 20),
           "the three queue caps, live, and Listeners says them");
        grpc::ClientContext statsContext;
        authorize(statsContext);
        v1::StatsReply stats;
        ok(admin->Stats(&statsContext, v1::Empty(), &stats).ok() && stats.queued_messages() == 0,
           "Stats says what is queued now: nothing");
    }
    {
        grpc::ClientContext context;
        authorize(context);
        v1::ListenersChange tooMany;
        tooMany.set_max_connections(reply.max_connections_ceiling() + 1);
        v1::Listeners refused;
        equal<int>("more connections than the room kept at start: FAILED_PRECONDITION",
                   admin->PutListeners(&context, tooMany, &refused).error_code(),
                   grpc::StatusCode::FAILED_PRECONDITION);
    }
}

} // namespace

int main() {
    Server server;
    helloAndToken(server);
    if (server.started) consumeAndPublish(server);
    if (server.started) sessionCounters(server);
    if (server.started) listenersLive(server);
    return check::report("api_tests");
}

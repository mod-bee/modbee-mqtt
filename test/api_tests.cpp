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

} // namespace

int main() {
    Server server;
    helloAndToken(server);
    if (server.started) consumeAndPublish(server);
    return check::report("api_tests");
}

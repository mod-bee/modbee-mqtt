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

// A birth with something of every kind in it.
v1::SparkplugPayload everything() {
    v1::SparkplugPayload p;
    p.set_timestamp(1793000000000ull);
    p.set_seq(0);
    auto metric = [&](const char* name, v1::SparkplugDataType type) {
        v1::SparkplugMetric* m = p.add_metrics();
        m->set_name(name);
        m->set_datatype(type);
        m->set_alias((uint64_t)p.metrics_size());
        return m;
    };
    metric("i8", v1::DATATYPE_INT8)->mutable_value()->set_int_value(-5);
    metric("i64", v1::DATATYPE_INT64)->mutable_value()->set_int_value(-9000000000ll);
    metric("u64", v1::DATATYPE_UINT64)->mutable_value()->set_uint_value(18000000000000000000ull);
    metric("f", v1::DATATYPE_FLOAT)->mutable_value()->set_float_value(1.5f);
    metric("d", v1::DATATYPE_DOUBLE)->mutable_value()->set_double_value(-2.25);
    metric("b", v1::DATATYPE_BOOLEAN)->mutable_value()->set_boolean_value(true);
    metric("s", v1::DATATYPE_STRING)->mutable_value()->set_string_value("pump \"7\"");
    metric("t", v1::DATATYPE_DATETIME)->mutable_value()->set_uint_value(1793000000123ull);
    metric("raw", v1::DATATYPE_BYTES)->mutable_value()->set_bytes_value(std::string("\0\1\2", 3));
    metric("null", v1::DATATYPE_INT32)->set_is_null(true);
    v1::SparkplugArray* a = metric("i16s", v1::DATATYPE_INT16_ARRAY)->mutable_value()->mutable_array_value();
    for (int v : {-32768, -1, 0, 32767}) a->add_ints(v);
    a = metric("bits", v1::DATATYPE_BOOLEAN_ARRAY)->mutable_value()->mutable_array_value();
    for (int i = 0; i < 10; i++) a->add_booleans(i % 3 == 0);
    a = metric("names", v1::DATATYPE_STRING_ARRAY)->mutable_value()->mutable_array_value();
    a->add_strings("a");
    a->add_strings("");
    a->add_strings("ccc");
    a = metric("ds", v1::DATATYPE_DOUBLE_ARRAY)->mutable_value()->mutable_array_value();
    a->add_doubles(0.5);
    a->add_doubles(-1e300);
    v1::SparkplugDataSet* set = metric("table", v1::DATATYPE_DATASET)->mutable_value()->mutable_dataset_value();
    set->add_columns("n");
    set->add_columns("name");
    set->add_types(v1::DATATYPE_INT32);
    set->add_types(v1::DATATYPE_STRING);
    v1::SparkplugRow* row = set->add_rows();
    row->add_values()->set_int_value(-3);
    row->add_values()->set_string_value("x");
    v1::SparkplugTemplate* t = metric("Motor", v1::DATATYPE_TEMPLATE)->mutable_value()->mutable_template_value();
    t->set_version("1.0");
    t->set_is_definition(true);
    v1::SparkplugMetric* rpm = t->add_metrics();
    rpm->set_name("rpm");
    rpm->set_datatype(v1::DATATYPE_UINT16);
    rpm->mutable_value()->set_uint_value(1450);
    v1::SparkplugParameter* param = t->add_parameters();
    param->set_name("poles");
    param->set_type(v1::DATATYPE_INT32);
    param->mutable_value()->set_int_value(4);
    v1::SparkplugMetric* level = metric("level", v1::DATATYPE_DOUBLE);
    level->mutable_value()->set_double_value(3.2);
    level->set_is_historical(true);
    level->set_timestamp(1792999999000ull);
    v1::SparkplugProperty* unit = level->add_properties();
    unit->set_key("engUnit");
    unit->set_type(v1::DATATYPE_STRING);
    unit->mutable_value()->set_string_value("m");
    v1::SparkplugProperty* nested = level->add_properties();
    nested->set_key("limits");
    nested->set_type(v1::DATATYPE_PROPERTY_SET);
    v1::SparkplugProperty* high = nested->mutable_set()->add_properties();
    high->set_key("high");
    high->set_type(v1::DATATYPE_DOUBLE);
    high->mutable_value()->set_double_value(9.5);
    level->mutable_metadata()->set_description("tank level");
    return p;
}

void sparkplugCodec(Server& server) {
    section("Sparkplug B through the API: every datatype, decoded and encoded, delivered decoded, published typed");
    auto sparkplug = v1::Sparkplug::NewStub(server.channel());
    const v1::SparkplugPayload birth = everything();
    for (bool compress : {false, true}) {
        grpc::ClientContext encodeContext;
        authorize(encodeContext);
        v1::SparkplugEncodeRequest encode;
        *encode.mutable_payload() = birth;
        encode.set_compress(compress);
        v1::SparkplugEncodeReply bytes;
        ok(sparkplug->Encode(&encodeContext, encode, &bytes).ok() && !bytes.payload().empty(),
           compress ? "Encode, compressed (DEFLATE)" : "Encode every datatype");
        grpc::ClientContext decodeContext;
        authorize(decodeContext);
        v1::SparkplugDecodeRequest decode;
        decode.set_topic("spBv1.0/plant/NBIRTH/gw-1");
        decode.set_payload(bytes.payload());
        v1::SparkplugMessage read;
        ok(sparkplug->Decode(&decodeContext, decode, &read).ok() && read.error().empty() &&
               read.kind() == v1::SPARKPLUG_NBIRTH && read.group() == "plant" && read.node() == "gw-1",
           "  Decode: an NBIRTH, its group and node");
        v1::SparkplugPayload back = read.payload();
        equal<bool>("  says whether it came compressed", back.compressed(), compress);
        back.set_compressed(false);
        ok(back.SerializeAsString() == birth.SerializeAsString(),
           "  every metric back as it was: arrays, datasets, templates, nested properties, metadata, nulls");
    }

    // What the library's own codec (a board's, TCK-tested) writes, read here.
    {
        MqttSparkplugPayload board(8, 256);
        MqttSparkplugMetric m;
        m.name = board.intern("Inputs/Level");
        m.value.type = MqttSparkplugDataType::Int32;
        m.value.as.signedValue = -7;
        board.addMetric(m);
        MqttSparkplugMetric flag;
        flag.name = board.intern("Run");
        flag.value.type = MqttSparkplugDataType::Boolean;
        flag.value.as.boolean = true;
        board.addMetric(flag);
        board.hasSeq = true;
        board.seq = 3;
        std::vector<uint8_t> wire(512);
        MqttSparkplugError error = MqttSparkplugError::None;
        const size_t length = mq_sparkplugEncode(wire.data(), wire.size(), board, MqttSparkplugType::NData, error);
        grpc::ClientContext context;
        authorize(context);
        v1::SparkplugDecodeRequest decode;
        decode.set_topic("spBv1.0/plant/NDATA/plc-1");
        decode.set_payload(std::string((const char*)wire.data(), length));
        v1::SparkplugMessage read;
        sparkplug->Decode(&context, decode, &read);
        ok(read.payload().seq() == 3 && read.payload().metrics_size() == 2 &&
               read.payload().metrics(0).value().int_value() == -7 && read.payload().metrics(1).value().boolean_value(),
           "what the library's board codec writes reads here: seq, a negative Int32, a Boolean");
    }

    // A device's message, delivered decoded; a typed publish, reaching a device as Sparkplug bytes.
    auto messages = v1::Messages::NewStub(server.channel());
    grpc::ClientContext context;
    authorize(context);
    std::unique_ptr<grpc::ClientReaderWriter<v1::ConsumeUp, v1::ConsumeDown>> stream(messages->Consume(&context));
    v1::ConsumeUp open;
    open.mutable_open()->add_filters("spBv1.0/#");
    open.mutable_open()->set_sparkplug(true);
    stream->Write(open);
    v1::ConsumeDown down;
    ok(stream->Read(&down) && down.has_opened(), "a Consume stream with sparkplug: true");
    Fleet fleet(server.broker.port());
    Client& node = fleet.make("gw-1");
    node.subscribe("spBv1.0/plant/NCMD/gw-1", MQ_QOS_1);
    ok(fleet.connected(node) && fleet.until([&] { return node.subscribed >= 1; }), "an edge node connects");
    std::string bytes;
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEncodeRequest encode;
        *encode.mutable_payload() = birth;
        v1::SparkplugEncodeReply reply;
        sparkplug->Encode(&c, encode, &reply);
        bytes = reply.payload();
    }
    node.publish("spBv1.0/plant/DDATA/gw-1/tank", bytes, MQ_QOS_1);
    std::atomic<bool> got{false};
    v1::Delivery delivery;
    std::thread reader([&] {
        v1::ConsumeDown d;
        while (stream->Read(&d)) {
            if (!d.has_delivery()) continue;
            delivery = d.delivery();
            got = true;
            v1::ConsumeUp ack;
            ack.mutable_ack()->add_ids(d.delivery().id());
            stream->Write(ack);
            return;
        }
    });
    ok(fleet.until([&] { return got.load(); }), "  its DDATA is delivered");
    reader.join();
    ok(delivery.sparkplug().kind() == v1::SPARKPLUG_DDATA && delivery.sparkplug().device() == "tank" &&
           delivery.sparkplug().payload().metrics_size() == birth.metrics_size() &&
           delivery.sparkplug().payload().metrics(0).value().int_value() == -5,
       "  decoded: DDATA, its device, every metric typed");
    context.TryCancel();
    {
        grpc::ClientContext c;
        authorize(c);
        v1::PublishRequest publish;
        publish.mutable_message()->set_topic("spBv1.0/plant/NCMD/gw-1");
        publish.mutable_message()->set_qos(1);
        v1::SparkplugMetric* rebirth = publish.mutable_sparkplug()->add_metrics();
        rebirth->set_name("Node Control/Rebirth");
        rebirth->set_datatype(v1::DATATYPE_BOOLEAN);
        rebirth->mutable_value()->set_boolean_value(true);
        publish.mutable_sparkplug()->set_timestamp(1793000000000ull);
        v1::PublishReply reply;
        ok(messages->Publish(&c, publish, &reply).ok(), "Publish with a typed Sparkplug payload: an NCMD");
    }
    ok(fleet.until([&] { return !node.got.empty(); }), "  the node has it");
    if (!node.got.empty()) {
        MqttSparkplugPayload read(8, 256);
        const std::string& p = node.got[0].payload;
        ok(mq_sparkplugDecode((const uint8_t*)p.data(), p.size(), read) == MqttSparkplugError::None &&
               read.metricCount == 1 && std::string(read.metrics[0].name) == "Node Control/Rebirth" &&
               read.metrics[0].value.as.boolean,
           "  and the library's board codec reads it: Node Control/Rebirth = true");
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::PublishRequest publish;
        publish.mutable_message()->set_topic("spBv1.0/plant/NCMD/gw-1");
        v1::SparkplugMetric* bad = publish.mutable_sparkplug()->add_metrics();
        bad->set_name("x");
        bad->set_datatype(v1::DATATYPE_PROPERTY_SET);
        bad->mutable_value()->set_int_value(1);
        v1::PublishReply reply;
        equal<int>("a metric its datatype cannot carry: INVALID_ARGUMENT", messages->Publish(&c, publish, &reply).error_code(),
                   grpc::StatusCode::INVALID_ARGUMENT);
    }
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
    if (server.started) sparkplugCodec(server);
    if (server.started) sessionCounters(server);
    if (server.started) listenersLive(server);
    return check::report("api_tests");
}

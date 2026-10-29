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
#include "AppConnections.h"
#include "SparkplugAware.h"
#include "ApiServer.h"
#include "modbee/app/v1/app.grpc.pb.h"
#include "modbee/sparkplug/v1/wire.pb.h"

#include <zlib.h>

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <map>
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
    SparkplugAware aware;
    std::unique_ptr<AppConnections> connections;
    ShardExtensionChain chain;
    ShardedBroker broker;
    ApiServer api;
    std::string target;     // host:port for a gRPC channel
    bool started = false;

    Server() {
        chain.add(&hub);
        chain.add(&admin);
        chain.add(&aware);
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
        // Connections - one to this server itself among them, which is where a
        // Sparkplug host on its own devices runs.
        connections.reset(new AppConnections(hub, ""));
        connections->reachThisServer([this](AppConnections::ThisServer& where) {
            where.port = broker.port();
            return where.port != 0;
        });
        std::string error;
        connections->start(error);
        ApiServer::Services s;
        s.messages = &hub;
        s.admin = &admin;
        s.server = &broker;
        s.connections = connections.get();
        s.sparkplugAware = [this] { return aware.enabled(); };
        s.setSparkplugAware = [this](bool on) { aware.enable(on); };
        std::string why;
        started = api.start(a, s, why);
        if (!started) std::printf("    api: %s\n", why.c_str());
        else target = api.listening()[0].substr(6);
    }
    ~Server() {
        api.stop();
        if (connections) connections->stop();
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
    {
        // A DEFLATE bomb: a few kilobytes that inflate to 8 MiB of NULs, a
        // StringArray of eight million empty strings - which decoded to about
        // half a gigabyte. Refused, said why, and the server is still here.
        modbee::sparkplug::v1::Payload inner;
        modbee::sparkplug::v1::Payload::Metric* m = inner.add_metrics();
        m->set_name("bomb");
        m->set_datatype(v1::DATATYPE_STRING_ARRAY);
        m->set_bytes_value(std::string(8u << 20, '\0'));
        const std::string plain = inner.SerializeAsString();
        uLongf size = compressBound((uLong)plain.size());
        std::string packed(size, '\0');
        compress2((Bytef*)&packed[0], &size, (const Bytef*)plain.data(), (uLong)plain.size(), Z_BEST_COMPRESSION);
        packed.resize(size);
        modbee::sparkplug::v1::Payload outer;
        outer.set_uuid("SPBV1.0_COMPRESSED");
        outer.set_body(packed);
        modbee::sparkplug::v1::Payload::Metric* algorithm = outer.add_metrics();
        algorithm->set_name("algorithm");
        algorithm->set_datatype(v1::DATATYPE_STRING);
        algorithm->set_string_value("DEFLATE");
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugDecodeRequest decode;
        decode.set_topic("spBv1.0/plant/NDATA/bomb");
        decode.set_payload(outer.SerializeAsString());
        v1::SparkplugMessage read;
        const grpc::Status status = sparkplug->Decode(&c, decode, &read);
        std::printf("  note  the bomb: %zu bytes, 8 MiB inflated\n", decode.payload().size());
        ok(status.ok(), "  the answer comes (no reply too large to take)");
        ok(read.payload().metrics_size() == 1 && read.payload().metrics(0).value().array_value().strings_size() == 0,
           "a DEFLATE bomb: its array is not expanded");
        ok(!read.error().empty(), "  and the answer says why");
        grpc::ClientContext h;
        authorize(h);
        v1::HelloRequest hello;
        v1::HelloReply reply;
        ok(v1::System::NewStub(server.channel())->Hello(&h, hello, &reply).ok(), "  the server answers on");
    }
}

// What a watcher on the broker has seen on a topic.
size_t seen(Client& c, const std::string& topic) {
    size_t n = 0;
    for (const check::Got& g : c.got) n += g.topic == topic ? 1 : 0;
    return n;
}

void sparkplugHost(Server& server) {
    section("a Sparkplug host in the server: STATE, aliases resolved in what it delivers, the registry, writes, rebirths");
    auto connections = v1::Connections::NewStub(server.channel());
    auto sparkplug = v1::Sparkplug::NewStub(server.channel());
    auto messages = v1::Messages::NewStub(server.channel());
    Fleet fleet(server.broker.port());
    Client& watcher = fleet.make("watcher");
    watcher.subscribe("spBv1.0/STATE/#", MQ_QOS_1);
    watcher.subscribe("spBv1.0/plant/NBIRTH/#", MQ_QOS_1);
    ok(fleet.connected(watcher) && fleet.until([&] { return watcher.subscribed >= 2; }), "a watcher on STATE and births");
    {
        grpc::ClientContext c;
        authorize(c);
        v1::ConnectionSpec local;
        local.set_id("local");
        local.set_this_server(true);
        local.set_client_id("sparkplug-host");
        v1::ConnectionInfo info;
        ok(connections->Put(&c, local, &info).ok(), "a connection to this server itself");
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugHostSpec host;
        host.set_id("h");
        host.set_connection("local");
        host.set_host_id("platform");
        host.set_group("plant");
        host.set_reorder_timeout_ms(200);
        host.set_rebirth_min_interval_ms(0);
        v1::SparkplugHostInfo info;
        ok(sparkplug->PutHost(&c, host, &info).ok(), "PutHost on it");
    }
    ok(fleet.until([&] {
           for (const check::Got& g : watcher.got) {
               if (g.topic == "spBv1.0/STATE/platform" && g.payload.find("\"online\":true") != std::string::npos) return true;
           }
           return false;
       }),
       "  its STATE is born");

    grpc::ClientContext consumeContext;
    authorize(consumeContext);
    std::unique_ptr<grpc::ClientReaderWriter<v1::ConsumeUp, v1::ConsumeDown>> stream(messages->Consume(&consumeContext));
    v1::ConsumeUp open;
    open.mutable_open()->set_connection("local");
    open.mutable_open()->add_filters("spBv1.0/plant/#");
    open.mutable_open()->set_sparkplug(true);
    stream->Write(open);
    v1::ConsumeDown down;
    ok(stream->Read(&down) && down.has_opened(), "a Consume stream on the host's connection, sparkplug: true");
    std::mutex lock;
    std::vector<v1::SparkplugMessage> got;
    std::thread reader([&] {
        v1::ConsumeDown d;
        while (stream->Read(&d)) {
            if (!d.has_delivery()) continue;
            {
                std::lock_guard<std::mutex> guard(lock);
                got.push_back(d.delivery().sparkplug());
            }
            v1::ConsumeUp ack;
            ack.mutable_ack()->add_ids(d.delivery().id());
            stream->Write(ack);
        }
    });
    auto find = [&](v1::SparkplugKind kind, std::function<bool(const v1::SparkplugMessage&)> also) {
        std::lock_guard<std::mutex> guard(lock);
        for (const v1::SparkplugMessage& m : got) {
            if (m.kind() == kind && also(m)) return true;
        }
        return false;
    };

    // The library's edge node (a board's, TCK-tested): aliases in its data, datatypes only in its birth.
    MqttTcpTransport transport;
    transport.configure("127.0.0.1", server.broker.port());
    MqttSparkplugNode node(transport);
    node.group("plant");
    node.node("gw-1");
    node.server("127.0.0.1", server.broker.port());
    node.epochNow((uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch()).count());
    node.declare("Level", MqttSparkplugDataType::Int8);
    node.declareWritable("Setpoint", MqttSparkplugDataType::Double);
    MqttSparkplugValue level;
    level.type = MqttSparkplugDataType::Int8;
    node.connect();
    auto run = [&](std::function<bool()> done, int ms = 5000) {
        return check::until(done, [&] { node.poll(); fleet.poll(); }, ms);
    };
    ok(run([&] { return node.isBorn(); }), "an edge node is born");
    level.as.signedValue = -5;
    node.set("Level", level);
    node.publishChanged();
    ok(run([&] {
           return find(v1::SPARKPLUG_NDATA, [](const v1::SparkplugMessage& m) {
               return m.payload().metrics_size() == 1 && m.payload().metrics(0).name() == "Level" &&
                      m.payload().metrics(0).datatype() == v1::DATATYPE_INT8 &&
                      m.payload().metrics(0).value().int_value() == -5 && m.payload().metrics(0).has_alias();
           });
       }),
       "its NDATA is delivered with the name and datatype its birth gave the alias: Level, Int8, -5");

    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugNodesRequest request;
        request.set_host("h");
        v1::SparkplugNodesReply reply;
        bool shown = false;
        if (sparkplug->ListNodes(&c, request, &reply).ok() && reply.nodes_size() == 1) {
            const v1::SparkplugNode& n = reply.nodes(0);
            for (const v1::SparkplugNodeMetric& m : n.metrics()) {
                if (m.name() == "Level") shown = n.online() && n.has_bd_seq() && m.value().int_value() == -5;
            }
        }
        ok(shown, "ListNodes: the node online, its bdSeq, Level's last value");
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugWriteRequest write;
        write.set_host("h");
        write.set_group("plant");
        write.set_node("gw-1");
        v1::SparkplugMetric* setpoint = write.add_metrics();
        setpoint->set_name("Setpoint");
        setpoint->mutable_value()->set_double_value(42.5);
        v1::SparkplugCommandReply reply;
        ok(sparkplug->Write(&c, write, &reply).ok() && reply.sent(), "Write Setpoint = 42.5 (an NCMD, by its alias)");
    }
    const int setpointAt = node.indexOf(nullptr, "Setpoint");
    MqttSparkplugValue written;
    ok(run([&] { return setpointAt >= 0 && node.commandPendingAt((size_t)setpointAt); }) &&
           node.takeAt((size_t)setpointAt, written) && written.as.float64 == 42.5,
       "  the node has it: 42.5");

    const size_t births = seen(watcher, "spBv1.0/plant/NBIRTH/gw-1");
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugRebirthRequest rebirth;
        rebirth.set_host("h");
        rebirth.set_group("plant");
        rebirth.set_node("gw-1");
        v1::SparkplugCommandReply reply;
        ok(sparkplug->Rebirth(&c, rebirth, &reply).ok() && reply.sent(), "Rebirth");
    }
    ok(run([&] { return seen(watcher, "spBv1.0/plant/NBIRTH/gw-1") > births; }), "  the node is born again");

    // A gap the reorder window does not close: a rebirth asked for, and the message marked.
    const size_t before = seen(watcher, "spBv1.0/plant/NBIRTH/gw-1");
    {
        v1::SparkplugPayload p;
        p.set_seq(200);
        p.set_timestamp(1793000000000ull);
        std::string bytes, error;
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEncodeRequest encode;
        *encode.mutable_payload() = p;
        v1::SparkplugEncodeReply reply;
        sparkplug->Encode(&c, encode, &reply);
        Client& rogue = fleet.make("rogue");
        ok(fleet.connected(rogue), "something publishes as the node, out of sequence");
        rogue.publish("spBv1.0/plant/NDATA/gw-1", reply.payload(), MQ_QOS_1);
    }
    ok(run([&] { return find(v1::SPARKPLUG_NDATA, [](const v1::SparkplugMessage& m) { return m.gap(); }); }),
       "  delivered marked gap");
    ok(run([&] { return seen(watcher, "spBv1.0/plant/NBIRTH/gw-1") > before; }),
       "  and once the reorder window passes, the node is asked to rebirth, and is born again");

    // A death whose bdSeq is not the live session's - an old Will, say - is no death.
    {
        v1::SparkplugPayload p;
        p.set_timestamp(1793000000000ull);
        v1::SparkplugMetric* bd = p.add_metrics();
        bd->set_name("bdSeq");
        bd->set_datatype(v1::DATATYPE_UINT64);
        bd->mutable_value()->set_uint_value(99);
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEncodeRequest encode;
        *encode.mutable_payload() = p;
        v1::SparkplugEncodeReply reply;
        sparkplug->Encode(&c, encode, &reply);
        Client& old = fleet.make("old-will");
        fleet.connected(old);
        old.publish("spBv1.0/plant/NDEATH/gw-1", reply.payload(), MQ_QOS_1);
    }
    ok(run([&] { return find(v1::SPARKPLUG_NDEATH, [](const v1::SparkplugMessage& m) { return m.stale(); }); }),
       "an NDEATH of a session already replaced: delivered marked stale");
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugNodesRequest request;
        request.set_host("h");
        v1::SparkplugNodesReply reply;
        sparkplug->ListNodes(&c, request, &reply);
        ok(reply.nodes_size() == 1 && reply.nodes(0).online(), "  and the node is still online");
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugHostRef ref;
        ref.set_id("h");
        v1::Empty none;
        ok(sparkplug->DeleteHost(&c, ref, &none).ok(), "DeleteHost");
    }
    ok(fleet.until([&] {
           for (const check::Got& g : watcher.got) {
               if (g.topic == "spBv1.0/STATE/platform" && g.payload.find("\"online\":false") != std::string::npos) return true;
           }
           return false;
       }),
       "  its STATE made offline first");
    node.disconnect();
    run([] { return false; }, 200);
    consumeContext.TryCancel();
    reader.join();
}

void sparkplugEdgeNode(Server& server) {
    section("a Sparkplug edge node in the server: births, history while its host is away, data, a host's writes");
    auto connections = v1::Connections::NewStub(server.channel());
    auto sparkplug = v1::Sparkplug::NewStub(server.channel());
    auto messages = v1::Messages::NewStub(server.channel());
    auto decode = [&](const check::Got& g) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugDecodeRequest request;
        request.set_topic(g.topic);
        request.set_payload(g.payload);
        v1::SparkplugMessage out;
        sparkplug->Decode(&c, request, &out);
        return out;
    };
    Fleet fleet(server.broker.port());
    Client& watcher = fleet.make("edge-watcher");
    watcher.subscribe("spBv1.0/plant2/#", MQ_QOS_1);
    ok(fleet.connected(watcher) && fleet.until([&] { return watcher.subscribed >= 1; }), "a watcher on the group");
    {
        grpc::ClientContext c;
        authorize(c);
        v1::ConnectionSpec edge;
        edge.set_id("edge");
        edge.set_this_server(true);
        edge.set_client_id("gw-9");
        v1::ConnectionInfo info;
        connections->Put(&c, edge, &info);
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeSpec spec;
        spec.set_id("gw");
        spec.set_connection("edge");
        spec.set_group("plant2");
        spec.set_node("gw-9");
        spec.set_primary_host_id("ph");
        spec.add_devices("plc1");
        v1::SparkplugMetricDeclaration* temp = spec.add_metrics();
        temp->set_name("Temp");
        temp->set_datatype(v1::DATATYPE_DOUBLE);
        temp->set_units("C");
        v1::SparkplugMetricDeclaration* run = spec.add_metrics();
        run->set_name("Run");
        run->set_device("plc1");
        run->set_datatype(v1::DATATYPE_BOOLEAN);
        run->set_writable(true);
        v1::SparkplugEdgeNodeInfo info;
        ok(sparkplug->PutEdgeNode(&c, spec, &info).ok() && !info.born(), "PutEdgeNode, waiting for its primary host");
    }
    auto setTemp = [&](double value, uint64_t at) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugValuesRequest request;
        request.set_edge_node("gw");
        v1::SparkplugMetric* m = request.add_metrics();
        m->set_name("Temp");
        m->mutable_value()->set_double_value(value);
        if (at) m->set_timestamp(at);
        v1::SparkplugValuesReply reply;
        sparkplug->SetValues(&c, request, &reply);
        return reply;
    };
    fleet.until([&] { return false; }, 300);
    uint32_t kept = 0;
    for (int i = 1; i <= 3; i++) kept += setTemp(i, 1793000000000ull + (uint64_t)i).kept();
    equal<uint32_t>("its host away: three values kept", kept, 3);
    Client& host = fleet.make("ph-host");
    ok(fleet.connected(host), "the primary host comes online");
    host.publish("spBv1.0/STATE/ph", "{\"online\":true,\"timestamp\":1793000000000}", MQ_QOS_1, true);
    auto births = [&](const char* topic) {
        size_t n = 0;
        for (const check::Got& g : watcher.got) n += g.topic == topic ? 1 : 0;
        return n;
    };
    ok(fleet.until([&] { return births("spBv1.0/plant2/NBIRTH/gw-9") >= 1 && births("spBv1.0/plant2/DBIRTH/gw-9/plc1") >= 1; }),
       "  it is born: NBIRTH, and a DBIRTH for its device");
    std::vector<double> history;
    ok(fleet.until([&] {
           history.clear();
           for (const check::Got& g : watcher.got) {
               if (g.topic != "spBv1.0/plant2/NDATA/gw-9") continue;
               const v1::SparkplugMessage read = decode(g);
               for (const v1::SparkplugMetric& m : read.payload().metrics()) {
                   if (m.is_historical()) history.push_back(m.value().double_value());
               }
           }
           return history.size() >= 3;
       }),
       "  what it kept is sent, is_historical");
    ok(history == std::vector<double>({1, 2, 3}), "  in order, as it was set");
    const v1::SparkplugEdgeNodeRef ref = [] { v1::SparkplugEdgeNodeRef r; r.set_id("gw"); return r; }();
    {
        v1::SparkplugValuesReply reply = setTemp(21.5, 0);
        ok(reply.sent() == 1 && reply.kept() == 0, "born: a value is sent at once");
    }
    ok(fleet.until([&] {
           for (const check::Got& g : watcher.got) {
               if (g.topic != "spBv1.0/plant2/NDATA/gw-9") continue;
               const v1::SparkplugMessage read = decode(g);
               for (const v1::SparkplugMetric& m : read.payload().metrics()) {
                   if (!m.is_historical() && m.value().double_value() == 21.5) return true;
               }
           }
           return false;
       }),
       "  an NDATA, live");

    // A host's write: the application reads it on the connection, by name.
    uint64_t runAlias = 0;
    for (const check::Got& g : watcher.got) {
        if (g.topic != "spBv1.0/plant2/DBIRTH/gw-9/plc1") continue;
        const v1::SparkplugMessage read = decode(g);
               for (const v1::SparkplugMetric& m : read.payload().metrics()) {
            if (m.name() == "Run") runAlias = m.alias();
        }
    }
    ok(runAlias != 0, "the DBIRTH gave Run an alias");
    grpc::ClientContext consumeContext;
    authorize(consumeContext);
    std::unique_ptr<grpc::ClientReaderWriter<v1::ConsumeUp, v1::ConsumeDown>> stream(messages->Consume(&consumeContext));
    v1::ConsumeUp open;
    open.mutable_open()->set_connection("edge");
    open.mutable_open()->add_filters("spBv1.0/plant2/DCMD/gw-9/#");
    open.mutable_open()->set_sparkplug(true);
    stream->Write(open);
    v1::ConsumeDown down;
    stream->Read(&down);
    {
        v1::SparkplugPayload p;
        p.set_timestamp(1793000000000ull);
        v1::SparkplugMetric* run = p.add_metrics();
        run->set_alias(runAlias);
        run->set_datatype(v1::DATATYPE_BOOLEAN);
        run->mutable_value()->set_boolean_value(true);
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEncodeRequest encode;
        *encode.mutable_payload() = p;
        v1::SparkplugEncodeReply bytes;
        sparkplug->Encode(&c, encode, &bytes);
        host.publish("spBv1.0/plant2/DCMD/gw-9/plc1", bytes.payload(), MQ_QOS_0);
    }
    std::atomic<bool> written{false};
    std::thread reader([&] {
        v1::ConsumeDown d;
        while (stream->Read(&d)) {
            if (!d.has_delivery()) continue;
            const v1::SparkplugMessage& m = d.delivery().sparkplug();
            written = m.kind() == v1::SPARKPLUG_DCMD && m.payload().metrics_size() == 1 &&
                      m.payload().metrics(0).name() == "Run" && m.payload().metrics(0).value().boolean_value();
            return;
        }
    });
    ok(fleet.until([&] { return written.load(); }), "a host's DCMD reaches the application: Run = true, by name");
    consumeContext.TryCancel();
    reader.join();
    {
        grpc::ClientContext c;
        authorize(c);
        v1::Empty none;
        ok(sparkplug->DeleteEdgeNode(&c, ref, &none).ok(), "DeleteEdgeNode");
    }
    ok(fleet.until([&] { return births("spBv1.0/plant2/NDEATH/gw-9") >= 1; }), "  its NDEATH first");
}

// What a gateway needs of the edge node in the server: a quality with each
// value, properties in the births, devices changed in the session, store and
// forward's limits and rate, events before samples, watching, and a host that
// keeps what the births said.
void sparkplugEdgeChanges(Server& server) {
    section("an edge node changed while it runs: quality, properties, devices in the session, history limits and rate");
    auto connections = v1::Connections::NewStub(server.channel());
    auto sparkplug = v1::Sparkplug::NewStub(server.channel());
    auto decode = [&](const check::Got& g) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugDecodeRequest request;
        request.set_topic(g.topic);
        request.set_payload(g.payload);
        v1::SparkplugMessage out;
        sparkplug->Decode(&c, request, &out);
        return out;
    };
    Fleet fleet(server.broker.port());
    Client& watcher = fleet.make("edge-changes-watcher");
    watcher.subscribe("spBv1.0/plant4/#", MQ_QOS_1);
    ok(fleet.connected(watcher) && fleet.until([&] { return watcher.subscribed >= 1; }), "a watcher on the group");
    auto on = [&](const std::string& topic) {
        std::vector<v1::SparkplugMessage> out;
        for (const check::Got& g : watcher.got) {
            if (g.topic == topic) out.push_back(decode(g));
        }
        return out;
    };
    auto count = [&](const std::string& topic) {
        size_t n = 0;
        for (const check::Got& g : watcher.got) n += g.topic == topic ? 1 : 0;
        return n;
    };
    auto property = [](const v1::SparkplugMetric& m, const std::string& key) -> const v1::SparkplugProperty* {
        for (const v1::SparkplugProperty& p : m.properties()) {
            if (p.key() == key) return &p;
        }
        return nullptr;
    };
    auto quality = [&](const v1::SparkplugMetric& m) {
        const v1::SparkplugProperty* p = property(m, "Quality");
        return p ? (int)p->value().int_value() : -1;
    };
    for (const char* id : {"edge4", "host4", "edge5"}) {
        grpc::ClientContext c;
        authorize(c);
        v1::ConnectionSpec spec;
        spec.set_id(id);
        spec.set_this_server(true);
        spec.set_client_id(std::string(id) + "-client");
        v1::ConnectionInfo info;
        connections->Put(&c, spec, &info);
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugHostSpec host;
        host.set_id("h4");
        host.set_connection("host4");
        host.set_host_id("reader4");
        host.set_group("plant4");
        v1::SparkplugHostInfo info;
        ok(sparkplug->PutHost(&c, host, &info).ok(), "a host in the server, following the group");
    }
    // Online before the node is born: a host whose session begins asks every node it knows to be born again.
    fleet.until([&] {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugHostRef ref;
        ref.set_id("h4");
        v1::SparkplugHostInfo info;
        return sparkplug->GetHost(&c, ref, &info).ok() && info.online();
    });
    v1::SparkplugEdgeNodeSpec spec;
    spec.set_id("gw4");
    spec.set_connection("edge4");
    spec.set_group("plant4");
    spec.set_node("gw-4");
    spec.set_primary_host_id("ph4");
    spec.add_devices("plc1");
    spec.set_history_rate(20);
    {
        v1::SparkplugMetricDeclaration* alarm = spec.add_metrics();
        alarm->set_name("Alarm");
        alarm->set_datatype(v1::DATATYPE_STRING);
        alarm->set_history_first(true);
        v1::SparkplugMetricDeclaration* level = spec.add_metrics();
        level->set_name("Level");
        level->set_datatype(v1::DATATYPE_DOUBLE);
        level->set_units("m");
        level->set_low(0);
        level->set_high(10);
        v1::SparkplugProperty* description = level->add_properties();
        description->set_key("Description");
        description->set_type(v1::DATATYPE_STRING);
        description->mutable_value()->set_string_value(std::string(200, 'd') + " - raw water tank level");
        v1::SparkplugProperty* format = level->add_properties();
        format->set_key("Format");
        format->set_type(v1::DATATYPE_STRING);
        format->mutable_value()->set_string_value("%.2f");
        v1::SparkplugProperty* readOnly = level->add_properties();
        readOnly->set_key("readOnly");
        readOnly->set_type(v1::DATATYPE_BOOLEAN);
        readOnly->mutable_value()->set_boolean_value(true);
        v1::SparkplugMetricDeclaration* flow = spec.add_metrics();
        flow->set_name("Flow");
        flow->set_device("plc1");
        flow->set_datatype(v1::DATATYPE_FLOAT);
    }
    {
        v1::SparkplugEdgeNodeSpec wrong = spec;
        v1::SparkplugProperty* q = wrong.mutable_metrics(1)->add_properties();
        q->set_key("Quality");
        q->set_type(v1::DATATYPE_INT32);
        q->mutable_value()->set_int_value(192);
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeInfo info;
        equal<int>("a declaration's Quality property: INVALID_ARGUMENT (a value's)",
                   sparkplug->PutEdgeNode(&c, wrong, &info).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeInfo info;
        ok(sparkplug->PutEdgeNode(&c, spec, &info).ok() && !info.born() && info.spec().history_rate() == 20,
           "PutEdgeNode with properties, history_first and a history rate; waiting for its primary host");
    }
    // Watched from the start.
    grpc::ClientContext watchContext;
    authorize(watchContext);
    v1::SparkplugWatchEdgeNodesRequest watchRequest;
    watchRequest.add_ids("gw4");
    std::unique_ptr<grpc::ClientReader<v1::SparkplugEdgeNodeInfo>> watch(sparkplug->WatchEdgeNodes(&watchContext, watchRequest));
    std::mutex watchedLock;
    std::vector<v1::SparkplugEdgeNodeInfo> watched;
    std::thread watching([&] {
        v1::SparkplugEdgeNodeInfo info;
        while (watch->Read(&info)) {
            std::lock_guard<std::mutex> guard(watchedLock);
            watched.push_back(info);
        }
    });
    auto watchedThat = [&](const std::function<bool(const v1::SparkplugEdgeNodeInfo&)>& what) {
        std::lock_guard<std::mutex> guard(watchedLock);
        return std::any_of(watched.begin(), watched.end(), what);
    };
    ok(fleet.until([&] { return watchedThat([](const v1::SparkplugEdgeNodeInfo& i) { return !i.born(); }); }),
       "WatchEdgeNodes: its state now, not born");

    auto set = [&](const std::string& device, std::function<void(v1::SparkplugValuesRequest&)> fill) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugValuesRequest request;
        request.set_edge_node("gw4");
        request.set_device(device);
        fill(request);
        v1::SparkplugValuesReply reply;
        sparkplug->SetValues(&c, request, &reply);
        return reply;
    };
    auto withQuality = [](v1::SparkplugMetric* m, int64_t q) {
        v1::SparkplugProperty* p = m->add_properties();
        p->set_key("Quality");
        p->set_type(v1::DATATYPE_INT32);
        p->mutable_value()->set_int_value(q);
    };
    const uint64_t t0 = 1793000000000ull;
    uint32_t kept = 0;
    for (int i = 1; i <= 40; i++) {
        kept += set("", [&](v1::SparkplugValuesRequest& r) {
                    v1::SparkplugMetric* m = r.add_metrics();
                    m->set_name("Level");
                    m->mutable_value()->set_double_value(i);
                    m->set_timestamp(t0 + (uint64_t)i);
                }).kept();
    }
    kept += set("", [&](v1::SparkplugValuesRequest& r) {
                v1::SparkplugMetric* m = r.add_metrics();
                m->set_name("Alarm");
                m->mutable_value()->set_string_value("high level");
                m->set_timestamp(t0 + 50);
            }).kept();
    kept += set("plc1", [&](v1::SparkplugValuesRequest& r) {
                v1::SparkplugMetric* m = r.add_metrics();
                m->set_name("Flow");
                m->mutable_value()->set_float_value(2.5f);
                withQuality(m, 0);
            }).kept();
    equal<uint32_t>("its host away: 42 values kept, a Bad one among them", kept, 42);
    {
        v1::SparkplugValuesReply reply = set("", [&](v1::SparkplugValuesRequest& r) {
            v1::SparkplugMetric* a = r.add_metrics();
            a->set_name("Level");
            a->mutable_value()->set_double_value(1);
            v1::SparkplugProperty* d = a->add_properties();
            d->set_key("Description");
            d->set_type(v1::DATATYPE_STRING);
            d->mutable_value()->set_string_value("x");
            v1::SparkplugMetric* b = r.add_metrics();
            b->set_name("Level");
            b->mutable_value()->set_double_value(1);
            withQuality(b, 7);
            v1::SparkplugMetric* c = r.add_metrics();
            c->set_name("Level");
            c->mutable_value()->set_double_value(1);
            c->mutable_metadata()->set_content_type("text/plain");
        });
        ok(reply.refused_size() == 3 && reply.kept() == 0 && reply.refused(0).find("Description") != std::string::npos &&
               reply.refused(1).find("500") != std::string::npos && reply.refused(2).find("metadata") != std::string::npos,
           "SetValues refuses another property, a quality Sparkplug does not have, and metadata - never drops them");
    }
    Client& primary = fleet.make("ph4-host");
    ok(fleet.connected(primary), "the primary host comes online");
    primary.publish("spBv1.0/STATE/ph4", "{\"online\":true,\"timestamp\":1793000000000}", MQ_QOS_1, true);
    ok(fleet.until([&] { return count("spBv1.0/plant4/NBIRTH/gw-4") >= 1 && count("spBv1.0/plant4/DBIRTH/gw-4/plc1") >= 1; }),
       "  born: NBIRTH and DBIRTH");
    const auto bornAt = std::chrono::steady_clock::now();
    std::map<std::string, uint64_t> alias;
    {
        const v1::SparkplugMessage birth = on("spBv1.0/plant4/NBIRTH/gw-4")[0];
        bool all = false;
        for (const v1::SparkplugMetric& m : birth.payload().metrics()) {
            alias[m.name()] = m.alias();
            if (m.name() != "Level") continue;
            const v1::SparkplugProperty* d = property(m, "Description");
            all = property(m, "engUnit") && property(m, "engUnit")->value().string_value() == "m" &&
                  property(m, "engLow") && property(m, "engHigh") && property(m, "engHigh")->value().double_value() == 10 &&
                  quality(m) == 192 && d && d->value().string_value().size() > 200 && property(m, "Format") &&
                  property(m, "readOnly") && property(m, "readOnly")->value().boolean_value();
        }
        ok(all, "  its NBIRTH: engUnit, engLow, engHigh, Quality, and Description (uncut), Format, readOnly");
        const v1::SparkplugMessage device = on("spBv1.0/plant4/DBIRTH/gw-4/plc1")[0];
        ok(device.payload().metrics_size() == 1 && quality(device.payload().metrics(0)) == 0 &&
               device.payload().metrics(0).value().float_value() == 2.5f,
           "  its DBIRTH: Flow at 2.5, Quality 0 - the quality it was given while not born");
        alias["Flow"] = device.payload().metrics(0).alias();
    }
    // History: the event first, then the samples, at 20 a second.
    auto historical = [&](const std::string& topic) {
        std::vector<v1::SparkplugMetric> out;
        for (const v1::SparkplugMessage& m : on(topic)) {
            for (const v1::SparkplugMetric& x : m.payload().metrics()) {
                if (x.is_historical()) out.push_back(x);
            }
        }
        return out;
    };
    ok(fleet.until([&] { return !historical("spBv1.0/plant4/NDATA/gw-4").empty(); }), "  what it kept is sent");
    ok(historical("spBv1.0/plant4/NDATA/gw-4")[0].alias() == alias["Alarm"],
       "  history_first's first: the alarm before the forty samples kept before it");
    fleet.until([&] { return std::chrono::steady_clock::now() - bornAt > std::chrono::milliseconds(1000); }, 3000);
    const size_t inASecond = historical("spBv1.0/plant4/NDATA/gw-4").size();
    ok(inASecond < 41, "  at history_rate: " + std::to_string(inASecond) + " of 41 within a second of the birth");
    ok(fleet.until([&] { return historical("spBv1.0/plant4/NDATA/gw-4").size() >= 41; }, 8000),
       "  and all of it, in time");
    ok(fleet.until([&] {
           const std::vector<v1::SparkplugMetric> sent = historical("spBv1.0/plant4/DDATA/gw-4/plc1");
           return sent.size() == 1 && quality(sent[0]) == 0;
       }),
       "  the device's kept value went with its Quality 0");
    ok(fleet.until([&] { return watchedThat([](const v1::SparkplugEdgeNodeInfo& i) { return i.born() && i.births() >= 2; }); }),
       "WatchEdgeNodes: born, its births counted");

    // Live: a quality that moves is news, the value the same or not.
    set("plc1", [&](v1::SparkplugValuesRequest& r) {
        v1::SparkplugMetric* m = r.add_metrics();
        m->set_name("Flow");
        m->mutable_value()->set_float_value(2.5f);
        withQuality(m, 192);
    });
    ok(fleet.until([&] {
           for (const v1::SparkplugMessage& m : on("spBv1.0/plant4/DDATA/gw-4/plc1")) {
               for (const v1::SparkplugMetric& x : m.payload().metrics()) {
                   if (!x.is_historical() && quality(x) == 192) return true;
               }
           }
           return false;
       }),
       "the same value, Good again: a DDATA saying Quality 192");

    // The host in the server kept what the births said.
    auto nodeMetric = [&](const std::string& device, const std::string& name) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugNodesRequest request;
        request.set_host("h4");
        request.set_node("gw-4");
        v1::SparkplugNodesReply reply;
        sparkplug->ListNodes(&c, request, &reply);
        v1::SparkplugNodeMetric out;
        for (const v1::SparkplugNode& n : reply.nodes()) {
            for (const v1::SparkplugNodeMetric& m : n.metrics()) {
                if (m.device() == device && m.name() == name) out = m;
            }
        }
        return out;
    };
    ok(fleet.until([&] {
           const v1::SparkplugNodeMetric level = nodeMetric("", "Level");
           bool description = false, format = false;
           for (const v1::SparkplugProperty& p : level.properties()) {
               description = description || (p.key() == "Description" && p.value().string_value().size() > 200);
               format = format || p.key() == "Format";
           }
           return description && format && level.has_quality() && level.quality() == 192;
       }),
       "ListNodes: Level's birth properties (Description, Format) and its Quality");
    ok(fleet.until([&] {
           const v1::SparkplugNodeMetric flow = nodeMetric("plc1", "Flow");
           return flow.has_quality() && flow.quality() == 192;
       }),
       "  Flow's quality the latest a DDATA gave");

    // Devices changed in the session: no NDEATH, no new bdSeq.
    uint32_t bdSeq = 0;
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeRef ref;
        ref.set_id("gw4");
        v1::SparkplugEdgeNodeInfo info;
        sparkplug->GetEdgeNode(&c, ref, &info);
        bdSeq = info.bd_seq();
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugDeviceSpec device;
        device.set_edge_node("gw4");
        device.set_device("plc2");
        v1::SparkplugMetricDeclaration* speed = device.add_metrics();
        speed->set_name("Speed");
        speed->set_datatype(v1::DATATYPE_DOUBLE);
        v1::SparkplugDeviceInfo info;
        ok(sparkplug->PutDevice(&c, device, &info).ok() && info.online() && info.metrics() == 1,
           "PutDevice: a device added while the node is born");
    }
    ok(fleet.until([&] { return count("spBv1.0/plant4/DBIRTH/gw-4/plc2") == 1; }), "  its DBIRTH");
    const size_t plc1Births = count("spBv1.0/plant4/DBIRTH/gw-4/plc1"), plc1Deaths = count("spBv1.0/plant4/DDEATH/gw-4/plc1");
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugDeviceSpec device;
        device.set_edge_node("gw4");
        device.set_device("plc1");
        v1::SparkplugMetricDeclaration* flow = device.add_metrics();
        flow->set_name("Flow");
        flow->set_datatype(v1::DATATYPE_FLOAT);
        v1::SparkplugMetricDeclaration* pressure = device.add_metrics();
        pressure->set_name("Pressure");
        pressure->set_datatype(v1::DATATYPE_DOUBLE);
        v1::SparkplugDeviceInfo info;
        ok(sparkplug->PutDevice(&c, device, &info).ok() && info.online() && info.metrics() == 2,
           "PutDevice: a born device given another metric");
    }
    auto lastIndex = [&](const std::string& topic) {
        long at = -1;
        for (size_t i = 0; i < watcher.got.size(); i++) {
            if (watcher.got[i].topic == topic) at = (long)i;
        }
        return at;
    };
    if (!ok(fleet.until([&] {
               return count("spBv1.0/plant4/DDEATH/gw-4/plc1") == plc1Deaths + 1 &&
                      count("spBv1.0/plant4/DBIRTH/gw-4/plc1") >= plc1Births + 1 &&
                      lastIndex("spBv1.0/plant4/DDEATH/gw-4/plc1") < lastIndex("spBv1.0/plant4/DBIRTH/gw-4/plc1");
           }),
            "  its DDEATH, then a DBIRTH")) {
        std::printf("    DDEATH %zu DBIRTH %zu NBIRTH %zu\n", count("spBv1.0/plant4/DDEATH/gw-4/plc1"),
                    count("spBv1.0/plant4/DBIRTH/gw-4/plc1"), count("spBv1.0/plant4/NBIRTH/gw-4"));
        for (const check::Got& g : watcher.got) {
            if (g.topic.find("gw-4") != std::string::npos && g.topic.find("DATA") == std::string::npos) {
                std::printf("      %s seq %llu\n", g.topic.c_str(), (unsigned long long)decode(g).payload().seq());
            }
        }
    }
    {
        const std::vector<v1::SparkplugMessage> births = on("spBv1.0/plant4/DBIRTH/gw-4/plc1");
        const v1::SparkplugPayload& again = births.back().payload();
        uint64_t flowAlias = 0;
        float flowValue = 0;
        for (const v1::SparkplugMetric& m : again.metrics()) {
            if (m.name() != "Flow") continue;
            flowAlias = m.alias();
            flowValue = m.value().float_value();
        }
        ok(again.metrics_size() == 2 && flowAlias != 0 && flowAlias != alias["Flow"] && flowValue == 2.5f,
           "  with both metrics, Flow's value carried over and a new alias (never an old one)");
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugDeviceRef ref;
        ref.set_edge_node("gw4");
        ref.set_device("plc2");
        v1::Empty none;
        ok(sparkplug->DeleteDevice(&c, ref, &none).ok(), "DeleteDevice");
    }
    ok(fleet.until([&] { return count("spBv1.0/plant4/DDEATH/gw-4/plc2") == 1; }), "  its DDEATH");
    const size_t nbirths = count("spBv1.0/plant4/NBIRTH/gw-4");
    {
        v1::SparkplugEdgeNodeSpec again = spec;
        v1::SparkplugMetricDeclaration* mode = again.add_metrics();
        mode->set_name("Mode");
        mode->set_datatype(v1::DATATYPE_STRING);
        again.clear_devices();
        again.add_devices("plc1");
        v1::SparkplugMetricDeclaration* pressure = again.add_metrics();
        pressure->set_name("Pressure");
        pressure->set_device("plc1");
        pressure->set_datatype(v1::DATATYPE_DOUBLE);
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeInfo info;
        ok(sparkplug->PutEdgeNode(&c, again, &info).ok() && info.born() && info.bd_seq() == bdSeq,
           "PutEdgeNode with a node metric more: still born, the same bdSeq");
    }
    ok(fleet.until([&] { return count("spBv1.0/plant4/NBIRTH/gw-4") >= nbirths + 1; }), "  a rebirth in the session");
    {
        const v1::SparkplugMessage birth = on("spBv1.0/plant4/NBIRTH/gw-4").back();
        bool mode = false;
        for (const v1::SparkplugMetric& m : birth.payload().metrics()) mode = mode || m.name() == "Mode";
        ok(mode, "  its NBIRTH declares Mode");
    }
    fleet.until([] { return false; }, 300);
    equal<size_t>("and in all of it no NDEATH", count("spBv1.0/plant4/NDEATH/gw-4"), 0);
    watchContext.TryCancel();
    watching.join();

    // What it keeps, by bytes and by age.
    v1::SparkplugEdgeNodeSpec tight;
    tight.set_id("gw5");
    tight.set_connection("edge5");
    tight.set_group("plant4");
    tight.set_node("gw-5");
    tight.set_primary_host_id("nobody");
    tight.set_history_max_bytes(400);
    {
        v1::SparkplugMetricDeclaration* level = tight.add_metrics();
        level->set_name("Level");
        level->set_datatype(v1::DATATYPE_DOUBLE);
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeInfo info;
        sparkplug->PutEdgeNode(&c, tight, &info);
    }
    for (int i = 0; i < 50; i++) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugValuesRequest request;
        request.set_edge_node("gw5");
        v1::SparkplugMetric* m = request.add_metrics();
        m->set_name("Level");
        m->mutable_value()->set_double_value(i);
        m->set_timestamp((uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch()).count() - 60000);
        v1::SparkplugValuesReply reply;
        sparkplug->SetValues(&c, request, &reply);
    }
    auto gw5 = [&] {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeRef ref;
        ref.set_id("gw5");
        v1::SparkplugEdgeNodeInfo info;
        sparkplug->GetEdgeNode(&c, ref, &info);
        return info;
    };
    {
        const v1::SparkplugEdgeNodeInfo info = gw5();
        ok(info.history() > 0 && info.history() < 50 && info.history_bytes() <= 400 &&
               info.history_lost() + info.history() == 50 && info.history_dropping(),
           "history_max_bytes 400: " + std::to_string(info.history()) + " kept in " +
               std::to_string(info.history_bytes()) + " bytes, the oldest dropped and counted, history_dropping");
    }
    tight.set_history_max_age_ms(30000);
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeInfo info;
        ok(sparkplug->PutEdgeNode(&c, tight, &info).ok(), "history_max_age_ms 30 s, changed live");
    }
    ok(fleet.until([&] { return gw5().history() == 0 && gw5().history_lost() == 50; }, 5000),
       "  values a minute old are dropped, and counted");
    for (const char* id : {"gw4", "gw5"}) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeRef ref;
        ref.set_id(id);
        v1::Empty none;
        sparkplug->DeleteEdgeNode(&c, ref, &none);
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugHostRef ref;
        ref.set_id("h4");
        v1::Empty none;
        sparkplug->DeleteHost(&c, ref, &none);
    }
}

void sparkplugStructures(Server& server) {
    section("an edge node in the server publishes a PLC struct (Template), an array, a DataSet and text; a host here reads them");
    auto connections = v1::Connections::NewStub(server.channel());
    auto sparkplug = v1::Sparkplug::NewStub(server.channel());
    auto decode = [&](const check::Got& g) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugDecodeRequest request;
        request.set_topic(g.topic);
        request.set_payload(g.payload);
        v1::SparkplugMessage out;
        sparkplug->Decode(&c, request, &out);
        return out;
    };
    Fleet fleet(server.broker.port());
    Client& watcher = fleet.make("structures-watcher");
    watcher.subscribe("spBv1.0/plant3/#", MQ_QOS_1);
    ok(fleet.connected(watcher) && fleet.until([&] { return watcher.subscribed >= 1; }), "a watcher on the group");
    for (const char* id : {"edge3", "host3"}) {
        grpc::ClientContext c;
        authorize(c);
        v1::ConnectionSpec spec;
        spec.set_id(id);
        spec.set_this_server(true);
        spec.set_client_id(std::string(id) + "-client");
        v1::ConnectionInfo info;
        connections->Put(&c, spec, &info);
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugHostSpec host;
        host.set_id("h3");
        host.set_connection("host3");
        host.set_host_id("reader3");
        host.set_group("plant3");
        v1::SparkplugHostInfo info;
        ok(sparkplug->PutHost(&c, host, &info).ok(), "a host in the server, following the group");
    }
    // Online once its session has its Will - and its subscriptions, made just before its STATE.
    ok(fleet.until([&] {
           grpc::ClientContext c;
           authorize(c);
           v1::SparkplugHostRef ref;
           ref.set_id("h3");
           v1::SparkplugHostInfo info;
           return sparkplug->GetHost(&c, ref, &info).ok() && info.online();
       }),
       "  and online");
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeSpec spec;
        spec.set_id("gw3");
        spec.set_connection("edge3");
        spec.set_group("plant3");
        spec.set_node("gw-30");
        v1::SparkplugTemplateDefinition* pump = spec.add_templates();
        pump->set_name("Pump");
        v1::SparkplugMetric* running = pump->mutable_definition()->add_metrics();
        running->set_name("running");
        running->set_datatype(v1::DATATYPE_BOOLEAN);
        running->mutable_value()->set_boolean_value(false);
        v1::SparkplugMetric* hours = pump->mutable_definition()->add_metrics();
        hours->set_name("hours");
        hours->set_datatype(v1::DATATYPE_DOUBLE);
        hours->mutable_value()->set_double_value(0);
        const std::pair<const char*, v1::SparkplugDataType> metrics[] = {
            {"pump1", v1::DATATYPE_TEMPLATE}, {"curve", v1::DATATYPE_FLOAT_ARRAY},
            {"table", v1::DATATYPE_DATASET}, {"note", v1::DATATYPE_STRING}};
        for (const auto& one : metrics) {
            v1::SparkplugMetricDeclaration* d = spec.add_metrics();
            d->set_name(one.first);
            d->set_datatype(one.second);
        }
        v1::SparkplugEdgeNodeInfo info;
        ok(sparkplug->PutEdgeNode(&c, spec, &info).ok(), "PutEdgeNode with a template definition and those metrics");
        ok(info.spec().templates_size() == 1 && info.spec().templates(0).name() == "Pump" &&
               info.spec().templates(0).definition().is_definition() &&
               info.spec().templates(0).definition().metrics_size() == 2,
           "  its spec gives the definition back");
    }
    auto births = [&](const char* topic) {
        size_t n = 0;
        for (const check::Got& g : watcher.got) n += g.topic == topic ? 1 : 0;
        return n;
    };
    ok(fleet.until([&] { return births("spBv1.0/plant3/NBIRTH/gw-30") >= 1; }), "  it is born");
    {
        bool definition = false;
        for (const check::Got& g : watcher.got) {
            if (g.topic != "spBv1.0/plant3/NBIRTH/gw-30") continue;
            const v1::SparkplugMessage read = decode(g);
            for (const v1::SparkplugMetric& m : read.payload().metrics()) {
                definition |= m.name() == "Pump" && m.datatype() == v1::DATATYPE_TEMPLATE &&
                              m.value().template_value().is_definition();
            }
        }
        ok(definition, "  the NBIRTH carries the Pump definition");
    }
    auto set = [&](const std::vector<v1::SparkplugMetric>& metrics) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugValuesRequest request;
        request.set_edge_node("gw3");
        for (const v1::SparkplugMetric& m : metrics) *request.add_metrics() = m;
        v1::SparkplugValuesReply reply;
        sparkplug->SetValues(&c, request, &reply);
        return reply;
    };
    std::vector<v1::SparkplugMetric> values(4);
    values[0].set_name("pump1");
    v1::SparkplugTemplate* instance = values[0].mutable_value()->mutable_template_value();
    instance->set_template_ref("Pump");
    v1::SparkplugMetric* member = instance->add_metrics();
    member->set_name("running");
    member->set_datatype(v1::DATATYPE_BOOLEAN);
    member->mutable_value()->set_boolean_value(true);
    member = instance->add_metrics();
    member->set_name("hours");
    member->set_datatype(v1::DATATYPE_DOUBLE);
    member->mutable_value()->set_double_value(3.5);
    values[1].set_name("curve");
    for (float f : {1.0f, 2.0f, 3.0f}) values[1].mutable_value()->mutable_array_value()->add_floats(f);
    values[2].set_name("table");
    v1::SparkplugDataSet* table = values[2].mutable_value()->mutable_dataset_value();
    table->add_columns("tag");
    table->add_types(v1::DATATYPE_STRING);
    table->add_columns("count");
    table->add_types(v1::DATATYPE_INT32);
    v1::SparkplugRow* row = table->add_rows();
    row->add_values()->set_string_value("a");
    row->add_values()->set_int_value(-4);
    values[3].set_name("note");
    values[3].mutable_value()->set_string_value("hello from the server");
    {
        const v1::SparkplugValuesReply reply = set(values);
        equal<uint32_t>("SetValues: all four sent", reply.sent(), 4);
        ok(reply.refused_size() == 0 && reply.unknown_size() == 0, "  none refused");
    }
    ok(fleet.until([&] {
           bool pump = false, curve = false, rows = false;
           for (const check::Got& g : watcher.got) {
               if (g.topic != "spBv1.0/plant3/NDATA/gw-30") continue;
               const v1::SparkplugMessage read = decode(g);
            for (const v1::SparkplugMetric& m : read.payload().metrics()) {
                   const v1::SparkplugTemplate& t = m.value().template_value();
                   pump |= t.template_ref() == "Pump" && t.metrics_size() == 2 && t.metrics(1).value().double_value() == 3.5;
                   curve |= m.value().array_value().floats_size() == 3 && m.value().array_value().floats(2) == 3.0f;
                   rows |= m.value().dataset_value().rows_size() == 1 &&
                           m.value().dataset_value().rows(0).values(1).int_value() == -4;
               }
           }
           return pump && curve && rows;
       }),
       "  NDATA carries the instance of Pump, the FloatArray and the DataSet");
    {
        std::vector<v1::SparkplugMetric> stranger(1, values[0]);
        stranger[0].mutable_value()->mutable_template_value()->set_template_ref("Valve");
        const v1::SparkplugValuesReply reply = set(stranger);
        ok(reply.sent() == 0 && reply.refused_size() == 1 && reply.refused(0).find("Valve") != std::string::npos,
           "an instance of a type the node does not declare is refused, saying why");
        std::vector<v1::SparkplugMetric> wrong(1);
        wrong[0].set_name("curve");
        wrong[0].mutable_value()->set_double_value(1);
        ok(set(wrong).refused_size() == 1, "and an array metric given a number");
    }
    {
        const v1::SparkplugValuesReply again = set(std::vector<v1::SparkplugMetric>(1, values[3]));
        ok(again.sent() == 1 && again.kept() == 0, "the same text again: nothing to send, and not kept as history");
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeRef ref;
        ref.set_id("gw3");
        v1::SparkplugEdgeNodeInfo info;
        sparkplug->GetEdgeNode(&c, ref, &info);
        equal<uint64_t>("  nothing waiting as history", info.history(), 0);
    }
    {
        // A rebirth reads every value again - the text from the server's own copy, not the request long gone.
        const size_t before = births("spBv1.0/plant3/NBIRTH/gw-30");
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeRef ref;
        ref.set_id("gw3");
        v1::Empty none;
        sparkplug->RebirthEdgeNode(&c, ref, &none);
        ok(fleet.until([&] { return births("spBv1.0/plant3/NBIRTH/gw-30") > before; }), "RebirthEdgeNode");
        std::string note;
        for (const check::Got& g : watcher.got) {
            if (g.topic != "spBv1.0/plant3/NBIRTH/gw-30") continue;
            const v1::SparkplugMessage read = decode(g);
            for (const v1::SparkplugMetric& m : read.payload().metrics()) {
                if (m.name() == "note") note = m.value().string_value();
            }
        }
        ok(note == "hello from the server", "  the new NBIRTH carries the text set earlier");
    }
    {
        bool text = false, array = false, instance = false;
        fleet.until([&] {
            grpc::ClientContext c;
            authorize(c);
            v1::SparkplugNodesRequest request;
            request.set_host("h3");
            v1::SparkplugNodesReply reply;
            sparkplug->ListNodes(&c, request, &reply);
            for (const v1::SparkplugNode& n : reply.nodes()) {
                for (const v1::SparkplugNodeMetric& m : n.metrics()) {
                    text |= m.name() == "note" && m.value().string_value() == "hello from the server";
                    array |= m.name() == "curve" && m.value().array_value().floats_size() == 3;
                    instance |= m.name() == "pump1" && m.value().template_value().template_ref() == "Pump";
                }
            }
            return text && array && instance;
        });
        ok(text, "ListNodes: the host reports the text");
        ok(array && instance, "  and the array and the template instance, decoded");
    }
    for (const char* id : {"gw3"}) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugEdgeNodeRef ref;
        ref.set_id(id);
        v1::Empty none;
        sparkplug->DeleteEdgeNode(&c, ref, &none);
    }
    {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugHostRef ref;
        ref.set_id("h3");
        v1::Empty none;
        sparkplug->DeleteHost(&c, ref, &none);
    }
}

void kafkaWithoutKafka(Server& server) {
    section("Kafka.* on a server without Kafka: said, not pretended");
    auto kafka = v1::Kafka::NewStub(server.channel());
    auto system = v1::System::NewStub(server.channel());
    grpc::ClientContext c;
    authorize(c);
    v1::Empty none;
    v1::KafkaClusterList clusters;
    const grpc::Status status = kafka->ListClusters(&c, none, &clusters);
    ok(status.error_code() == grpc::StatusCode::FAILED_PRECONDITION, "ListClusters: FAILED_PRECONDITION");
    grpc::ClientContext h;
    authorize(h);
    v1::HelloRequest hello;
    v1::HelloReply reply;
    system->Hello(&h, hello, &reply);
    bool listed = false, links = false;
    for (const std::string& f : reply.features()) {
        listed |= f == "kafka";
        links |= f == "connections.kafka";
    }
    ok(!listed, "  and Hello does not list kafka");
    ok(!links, "  nor connections.kafka");

    // A connection whose link is Kafka: refused, saying why - not a mistake in the request.
    auto connections = v1::Connections::NewStub(server.channel());
    grpc::ClientContext p;
    authorize(p);
    v1::ConnectionSpec spec;
    spec.set_id("over-kafka");
    spec.mutable_kafka()->set_kafka_topic("plant-out");
    v1::ConnectionInfo info;
    const grpc::Status put = connections->Put(&p, spec, &info);
    ok(put.error_code() == grpc::StatusCode::FAILED_PRECONDITION, "Connections.Put with kafka: FAILED_PRECONDITION");
    ok(put.error_message().find("without Kafka") != std::string::npos, "  saying the build has no Kafka");
}

void sparkplugAware(Server& server) {
    section("Sparkplug Aware: births kept on $sparkplug/certificates/..., cleared by deaths, turned on and off live");
    auto sparkplug = v1::Sparkplug::NewStub(server.channel());
    auto set = [&](bool on) {
        grpc::ClientContext c;
        authorize(c);
        v1::SparkplugAwareSetting setting;
        setting.set_enabled(on);
        v1::SparkplugAwareSetting now;
        return sparkplug->PutAware(&c, setting, &now).ok() && now.enabled() == on;
    };
    ok(set(true), "PutAware: on");
    Fleet fleet(server.broker.port());
    Client& node = fleet.make("aware-node");
    ok(fleet.connected(node), "an edge node connects");
    const std::string nbirth("\x08\x01\x18\x00", 4), dbirth("\x08\x02\x18\x01", 4);
    node.publish("spBv1.0/plant/NBIRTH/n1", nbirth, MQ_QOS_1);
    node.publish("spBv1.0/plant/DBIRTH/n1/pump", dbirth, MQ_QOS_1);
    fleet.until([&] { return node.completed >= 2; });
    Client& late = fleet.make("late-host");
    late.subscribe("$sparkplug/certificates/#", MQ_QOS_0);
    ok(fleet.connected(late) && fleet.until([&] { return late.got.size() >= 2; }),
       "a host that subscribes later is sent both births");
    bool same = late.got.size() >= 2;
    for (const check::Got& g : late.got) {
        if (g.topic == "$sparkplug/certificates/spBv1.0/plant/NBIRTH/n1") same = same && g.payload == nbirth && g.retain;
        else if (g.topic == "$sparkplug/certificates/spBv1.0/plant/DBIRTH/n1/pump") same = same && g.payload == dbirth && g.retain;
        else same = false;
    }
    ok(same, "  retained, at their topics, the bytes as the node sent them");
    node.publish("spBv1.0/plant/NDEATH/n1", std::string("\x08\x03", 2), MQ_QOS_1);
    fleet.until([&] { return node.completed >= 3; });
    Client& after = fleet.make("after-death");
    after.subscribe("$sparkplug/certificates/#", MQ_QOS_0);
    fleet.connected(after);
    fleet.until([&] { return false; }, 400);
    ok(after.got.empty(), "the NDEATH clears the node's and its device's");
    node.publish("spBv1.0/plant/NBIRTH/n2", nbirth, MQ_QOS_1);
    fleet.until([&] { return node.completed >= 4; });
    ok(set(false), "PutAware: off");
    fleet.until([&] { return false; }, 300);
    Client& off = fleet.make("when-off");
    off.subscribe("$sparkplug/certificates/#", MQ_QOS_0);
    fleet.connected(off);
    fleet.until([&] { return false; }, 400);
    ok(off.got.empty(), "  and clears what it kept");
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
    for (uint32_t rate : {250u, 0u}) {
        grpc::ClientContext context;
        authorize(context);
        v1::ListenersChange change;
        change.set_connect_rate(rate);
        v1::Listeners now;
        ok(admin->PutListeners(&context, change, &now).ok() && now.connect_rate() == rate,
           rate ? "connect_rate 250 a second, live, and Listeners says it" : "  and 0, no limit");
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
    if (server.started) sparkplugHost(server);
    if (server.started) sparkplugAware(server);
    if (server.started) sparkplugEdgeNode(server);
    if (server.started) sparkplugStructures(server);
    if (server.started) sparkplugEdgeChanges(server);
    if (server.started) kafkaWithoutKafka(server);
    if (server.started) sessionCounters(server);
    if (server.started) listenersLive(server);
    return check::report("api_tests");
}

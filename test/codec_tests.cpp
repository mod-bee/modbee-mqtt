// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// THE WIRE FORMAT: every packet type through both codecs and back, and the
// malformed input each must refuse - with the error it must refuse it with.

#include "Check.h"

#include <cstring>
#include <vector>

using check::equal;
using check::ok;
using check::same;
using check::section;
using check::span;
using check::text;

namespace {

const MqttCodecV5 v5;
const MqttCodecV311 v311;

// Decoded spans point into the buffer, so it outlives the decode.
struct Wire {
    std::vector<uint8_t> bytes;
    MqttError error = MqttError::None;
};

Wire encode(const MqttCodec& codec, const MqttPacket& packet) {
    Wire w;
    const size_t size = codec.size(packet);
    if (size == 0) {
        w.error = MqttError::NotSupportedByProtocol;
        return w;
    }
    w.bytes.resize(size);
    size_t written = 0;
    w.error = codec.encode(w.bytes.data(), w.bytes.size(), packet, written);
    w.bytes.resize(written);
    return w;
}

MqttError decode(const MqttCodec& codec, const std::vector<uint8_t>& bytes, MqttPacket& out) {
    MqttFixedHeader header;
    const MqttError error = mq_decodeFixedHeader(bytes.data(), bytes.size(), header);
    if (error != MqttError::None) return error;
    if (header.packetSize() != bytes.size()) return MqttError::Truncated;
    return codec.decode(header, bytes.data() + header.headerLength, header.remainingLength, out);
}

// Encode, decode, and the decoded packet.
bool roundTrip(const MqttCodec& codec, const MqttPacket& in, Wire& wire, MqttPacket& out) {
    wire = encode(codec, in);
    if (wire.error != MqttError::None) return false;
    return decode(codec, wire.bytes, out) == MqttError::None;
}

const char* name(const MqttCodec& codec) {
    return codec.version() == MqttVersion::V5 ? "5.0" : "3.1.1";
}

void connect(const MqttCodec& codec) {
    const std::string at = std::string(" (") + name(codec) + ")";
    section(("CONNECT and CONNACK" + at).c_str());
    const std::string id = "pump-7", user = "acme", pass = "s3cret", willTopic = "plant/pump-7/state",
                      willPayload = std::string("\x00\x01gone", 6);
    MqttPacket in;
    in.header.type = MQ_PKT_CONNECT;
    in.connect.version = codec.version();
    in.connect.clientId = span(id);
    in.connect.cleanStart = false;
    in.connect.keepAlive = 45;
    in.connect.hasUserName = true;
    in.connect.userName = span(user);
    in.connect.hasPassword = true;
    in.connect.password = span(pass);
    in.connect.hasWill = true;
    in.connect.will.topic = span(willTopic);
    in.connect.will.payload = span(willPayload);
    in.connect.will.qos = MQ_QOS_1;
    in.connect.will.retain = true;
    if (codec.version() == MqttVersion::V5) {
        in.connect.properties.sessionExpiryInterval = 3600;
        in.connect.properties.markPresent(MQ_PROP_SESSION_EXPIRY_INTERVAL);
        in.connect.properties.receiveMaximum = 20;
        in.connect.properties.markPresent(MQ_PROP_RECEIVE_MAXIMUM);
    }
    Wire wire;
    MqttPacket out;
    if (!ok(roundTrip(codec, in, wire, out), "CONNECT encodes and decodes")) return;
    same("  client id", text(out.connect.clientId), id);
    ok(!out.connect.cleanStart && out.connect.keepAlive == 45, "  clean start 0, keep alive 45");
    ok(out.connect.hasUserName && text(out.connect.userName) == user, "  user name");
    ok(out.connect.hasPassword && text(out.connect.password) == pass, "  password");
    ok(out.connect.hasWill && text(out.connect.will.topic) == willTopic &&
           text(out.connect.will.payload) == willPayload && out.connect.will.qos == 1 && out.connect.will.retain,
       "  Will: topic, binary payload, QoS 1, retain");
    if (codec.version() == MqttVersion::V5) {
        ok(out.connect.properties.has(MQ_PROP_SESSION_EXPIRY_INTERVAL) &&
               out.connect.properties.sessionExpiryInterval == 3600 &&
               out.connect.properties.receiveMaximum == 20,
           "  properties: session expiry 3600, receive maximum 20");
    }

    MqttPacket ack;
    ack.header.type = MQ_PKT_CONNACK;
    ack.connack.version = codec.version();
    ack.connack.sessionPresent = true;
    ack.connack.reasonCode = 0;
    MqttPacket ackOut;
    ok(roundTrip(codec, ack, wire, ackOut) && ackOut.connack.sessionPresent && ackOut.connack.reasonCode == 0,
       "CONNACK: session present, accepted");
}

void publish(const MqttCodec& codec) {
    const std::string at = std::string(" (") + name(codec) + ")";
    section(("PUBLISH and its acknowledgements" + at).c_str());
    const std::string topic = "plant/line-3/pressure";
    for (uint8_t qos = 0; qos <= 2; qos++) {
        const std::string payload = "3." + std::to_string(qos);
        MqttPacket in;
        in.header.type = MQ_PKT_PUBLISH;
        in.publish.topic = span(topic);
        in.publish.payload = span(payload);
        in.publish.qos = qos;
        in.publish.retain = qos == 1;
        in.publish.dup = qos == 2;
        in.publish.packetId = qos ? (uint16_t)(100 + qos) : 0;
        Wire wire;
        MqttPacket out;
        const bool done = roundTrip(codec, in, wire, out);
        ok(done && text(out.publish.topic) == topic && text(out.publish.payload) == payload &&
               out.publish.qos == qos && out.publish.retain == in.publish.retain && out.publish.dup == in.publish.dup &&
               out.publish.packetId == in.publish.packetId,
           "QoS " + std::to_string(qos) + ": topic, payload, flags and packet identifier");
    }

    // A payload has no length prefix of its own: past 64 kB it must still
    // round-trip (a 16-bit length once capped it silently).
    std::string big(200000, 'x');
    big[0] = 'a';
    big[big.size() - 1] = 'z';
    MqttPacket in;
    in.header.type = MQ_PKT_PUBLISH;
    in.publish.topic = span(topic);
    in.publish.payload = span(big);
    in.publish.qos = MQ_QOS_1;
    in.publish.packetId = 9;
    Wire wire;
    MqttPacket out;
    ok(roundTrip(codec, in, wire, out) && out.publish.payload.length == big.size() &&
           text(out.publish.payload) == big,
       "a 200,000-byte payload, whole");

    MqttPacket empty;
    empty.header.type = MQ_PKT_PUBLISH;
    empty.publish.topic = span(topic);
    empty.publish.retain = true;
    ok(roundTrip(codec, empty, wire, out) && out.publish.payload.length == 0 && out.publish.retain,
       "an empty retained payload (clears a retained message)");

    if (codec.version() == MqttVersion::V5) {
        const std::string type = "application/json", response = "replies/7", correlation = "abc", key = "site",
                          value = "north";
        MqttPacket p;
        p.header.type = MQ_PKT_PUBLISH;
        p.publish.topic = span(topic);
        p.publish.payload = span(std::string("{}"));
        MqttProperties& props = p.publish.properties;
        props.contentType = span(type);
        props.markPresent(MQ_PROP_CONTENT_TYPE);
        props.responseTopic = span(response);
        props.markPresent(MQ_PROP_RESPONSE_TOPIC);
        props.correlationData = span(correlation);
        props.markPresent(MQ_PROP_CORRELATION_DATA);
        props.messageExpiryInterval = 30;
        props.markPresent(MQ_PROP_MESSAGE_EXPIRY_INTERVAL);
        props.payloadFormatIndicator = 1;
        props.markPresent(MQ_PROP_PAYLOAD_FORMAT_INDICATOR);
        props.addUserProperty(span(key), span(value));
        const std::string payload = "{}";
        p.publish.payload = span(payload);
        MqttPacket o;
        const bool done = roundTrip(codec, p, wire, o);
        const MqttProperties& got = o.publish.properties;
        ok(done && text(got.contentType) == type && text(got.responseTopic) == response &&
               text(got.correlationData) == correlation && got.messageExpiryInterval == 30 &&
               got.payloadFormatIndicator == 1 && got.userCount == 1 && text(got.user[0].key) == key &&
               text(got.user[0].value) == value,
           "properties: content type, response topic, correlation, expiry, format, a user property");
    }

    const uint8_t types[] = {MQ_PKT_PUBACK, MQ_PKT_PUBREC, MQ_PKT_PUBREL, MQ_PKT_PUBCOMP};
    const char* names[] = {"PUBACK", "PUBREC", "PUBREL", "PUBCOMP"};
    for (int i = 0; i < 4; i++) {
        MqttPacket a;
        a.header.type = types[i];
        if (types[i] == MQ_PKT_PUBREL) a.header.flags = 0x02;
        a.ack.packetId = (uint16_t)(4000 + i);
        a.ack.reasonCode = codec.version() == MqttVersion::V5 && types[i] == MQ_PKT_PUBACK ? MQ_REASON_NO_MATCHING_SUBSCRIBERS : 0;
        MqttPacket o;
        ok(roundTrip(codec, a, wire, o) && o.type() == types[i] && o.ack.packetId == a.ack.packetId &&
               o.ack.reasonCode == a.ack.reasonCode,
           std::string(names[i]) + ": packet identifier" +
               (a.ack.reasonCode ? ", reason 0x10 No matching subscribers" : ""));
    }
}

void subscribe(const MqttCodec& codec) {
    const std::string at = std::string(" (") + name(codec) + ")";
    section(("SUBSCRIBE, UNSUBSCRIBE and their acknowledgements" + at).c_str());
    const std::string a = "plant/+/pressure", b = "$share/ingest/plant/#", c = "alarms/#";
    MqttPacket in;
    in.header.type = MQ_PKT_SUBSCRIBE;
    in.header.flags = 0x02;
    in.subscribe.packetId = 77;
    in.subscribe.filters[0].filter = span(a);
    in.subscribe.filters[0].options = MQ_QOS_1;
    in.subscribe.filters[1].filter = span(b);
    in.subscribe.filters[1].options = MQ_QOS_2;
    in.subscribe.filters[2].filter = span(c);
    in.subscribe.filters[2].options = MQ_QOS_0;
    in.subscribe.count = 3;
    Wire wire;
    MqttPacket out;
    ok(roundTrip(codec, in, wire, out) && out.subscribe.count == 3 && out.subscribe.packetId == 77 &&
           text(out.subscribe.filters[0].filter) == a && out.subscribe.filters[0].qos() == 1 &&
           text(out.subscribe.filters[1].filter) == b && out.subscribe.filters[1].qos() == 2 &&
           text(out.subscribe.filters[2].filter) == c && out.subscribe.filters[2].qos() == 0,
       "SUBSCRIBE: three filters, a shared one among them, each with its QoS");

    MqttPacket ack;
    ack.header.type = MQ_PKT_SUBACK;
    ack.suback.packetId = 77;
    ack.suback.reasons[0] = 1;
    ack.suback.reasons[1] = 2;
    ack.suback.reasons[2] = codec.version() == MqttVersion::V5 ? MQ_REASON_NOT_AUTHORIZED : 0x80;
    ack.suback.count = 3;
    ok(roundTrip(codec, ack, wire, out) && out.suback.count == 3 && out.suback.reasons[0] == 1 &&
           out.suback.reasons[1] == 2 && out.suback.reasons[2] == ack.suback.reasons[2],
       "SUBACK: granted 1, granted 2, refused");

    MqttPacket un;
    un.header.type = MQ_PKT_UNSUBSCRIBE;
    un.header.flags = 0x02;
    un.unsubscribe.packetId = 78;
    un.unsubscribe.filters[0] = span(a);
    un.unsubscribe.filters[1] = span(c);
    un.unsubscribe.count = 2;
    ok(roundTrip(codec, un, wire, out) && out.unsubscribe.count == 2 && text(out.unsubscribe.filters[1]) == c,
       "UNSUBSCRIBE: two filters");

    MqttPacket unack;
    unack.header.type = MQ_PKT_UNSUBACK;
    unack.suback.packetId = 78;
    unack.suback.count = codec.version() == MqttVersion::V5 ? 2 : 0;
    ok(roundTrip(codec, unack, wire, out) && out.suback.packetId == 78, "UNSUBACK");
}

void control(const MqttCodec& codec) {
    const std::string at = std::string(" (") + name(codec) + ")";
    section(("PINGREQ, PINGRESP, DISCONNECT, AUTH" + at).c_str());
    Wire wire;
    MqttPacket out;
    MqttPacket ping;
    ping.header.type = MQ_PKT_PINGREQ;
    ok(roundTrip(codec, ping, wire, out) && wire.bytes.size() == 2 && out.type() == MQ_PKT_PINGREQ,
       "PINGREQ: two bytes");
    MqttPacket pong;
    pong.header.type = MQ_PKT_PINGRESP;
    ok(roundTrip(codec, pong, wire, out) && out.type() == MQ_PKT_PINGRESP, "PINGRESP");
    MqttPacket bye;
    bye.header.type = MQ_PKT_DISCONNECT;
    bye.disconnect.reasonCode = codec.version() == MqttVersion::V5 ? MQ_REASON_DISCONNECT_WITH_WILL : 0;
    ok(roundTrip(codec, bye, wire, out) && out.disconnect.reasonCode == bye.disconnect.reasonCode,
       codec.version() == MqttVersion::V5 ? "DISCONNECT 0x04 with Will" : "DISCONNECT");
    MqttPacket auth;
    auth.header.type = MQ_PKT_AUTH;
    auth.auth.reasonCode = 0x18;
    const std::string method = "SCRAM-SHA-256";
    auth.auth.properties.authenticationMethod = span(method);
    auth.auth.properties.markPresent(MQ_PROP_AUTHENTICATION_METHOD);
    if (codec.version() == MqttVersion::V5) {
        ok(roundTrip(codec, auth, wire, out) && out.auth.reasonCode == 0x18 &&
               text(out.auth.properties.authenticationMethod) == method,
           "AUTH: continue, with its method");
    } else {
        ok(codec.size(auth) == 0, "AUTH does not exist at 3.1.1: nothing to encode");
    }
}

void refused() {
    section("malformed input is refused, with the error it deserves");
    MqttPacket out;
    // A PUBLISH whose Remaining Length promises more than arrives.
    const std::vector<uint8_t> shortPublish = {0x30, 0x0A, 0x00, 0x03, 'a', '/', 'b'};
    equal<int>("a packet shorter than its Remaining Length: Truncated", (int)decode(v5, shortPublish, out),
               (int)MqttError::Truncated);
    // A variable byte integer of five bytes.
    const std::vector<uint8_t> longVbi = {0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
    equal<int>("a five-byte Remaining Length: VbiTooLong", (int)decode(v5, longVbi, out),
               (int)MqttError::VbiTooLong);
    // A topic that is not UTF-8.
    const std::vector<uint8_t> badUtf8 = {0x30, 0x06, 0x00, 0x02, 0xC3, 0x28, 'x', 'y'};
    equal<int>("a topic that is not UTF-8: InvalidUtf8", (int)decode(v311, badUtf8, out),
               (int)MqttError::InvalidUtf8);
    // A topic holding U+0000.
    const std::vector<uint8_t> nul = {0x30, 0x06, 0x00, 0x02, 'a', 0x00, 'x', 'y'};
    equal<int>("a topic holding U+0000: NullCharacter", (int)decode(v311, nul, out),
               (int)MqttError::NullCharacter);
    // A property appearing twice where it may appear once.
    const std::vector<uint8_t> repeated = {0x30, 0x0A, 0x00, 0x01, 't', 0x04, 0x01, 0x01, 0x01, 0x00, 'x', 'y'};
    equal<int>("a property given twice: PropertyRepeated", (int)decode(v5, repeated, out),
               (int)MqttError::PropertyRepeated);
    // Bytes left over after a PINGREQ.
    const std::vector<uint8_t> trailing = {0xC0, 0x01, 0x00};
    ok(decode(v5, trailing, out) != MqttError::None, "a PINGREQ with a body is refused");

    section("topics and filters");
    ok(mq_topicNameIsValid(span(std::string("plant/line-3/pressure"))), "a topic name");
    ok(!mq_topicNameIsValid(span(std::string("plant/+/pressure"))), "  a wildcard is not a topic name");
    ok(!mq_topicNameIsValid(span(std::string(""))), "  nor is nothing");
    ok(mq_topicFilterIsValid(span(std::string("plant/+/pressure"))), "a filter with +");
    ok(mq_topicFilterIsValid(span(std::string("plant/#"))), "a filter ending in #");
    ok(!mq_topicFilterIsValid(span(std::string("plant/#/x"))), "  # anywhere but the end is not");
    ok(!mq_topicFilterIsValid(span(std::string("plant/a+"))), "  + sharing a level is not");
    ok(mq_topicMatches(span(std::string("plant/+/pressure")), span(std::string("plant/3/pressure"))),
       "plant/+/pressure matches plant/3/pressure");
    ok(mq_topicMatches(span(std::string("plant/#")), span(std::string("plant"))), "plant/# matches plant itself");
    ok(!mq_topicMatches(span(std::string("#")), span(std::string("$SYS/broker"))),
       "# does not match a $ topic");
    ok(!mq_topicMatches(span(std::string("+/broker")), span(std::string("$SYS/broker"))),
       "+ at the first level does not either");
    ok(mq_isSharedFilter(span(std::string("$share/g/plant/#"))), "$share/g/plant/# is a shared subscription");
}

} // namespace

int main() {
    connect(v5);
    connect(v311);
    publish(v5);
    publish(v311);
    subscribe(v5);
    subscribe(v311);
    control(v5);
    control(v311);
    refused();
    return check::report("codec_tests");
}

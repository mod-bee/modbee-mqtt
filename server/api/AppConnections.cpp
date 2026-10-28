// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AppConnections.h"

#include "Json.h"
#include "MqttClient.h"
#include "sparkplug/MqttSparkplugHost.h"
#include "sparkplug/MqttSparkplugNode.h"
#include "transport/MqttTcpTransport.h"
#if MQTT_TLS_SOCKETS
#include "transport/MqttTlsTransport.h"
#endif

#include <algorithm>
#include <set>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <mutex>
#include <random>
#include <sstream>

#include "ServerOs.h"
#include "GroupStore.h"

namespace {

typedef std::chrono::steady_clock Clock;

int64_t utcNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// What a refusal by the remote broker means for the publish that asked.
// Why a connection could not be made, as its transport put it ("cannot connect to plant.local: connection refused",
// "cannot resolve ...", a TLS handshake that failed) - a person acts on that; "could not connect" alone they cannot.
template <typename C>
std::string whyNotConnected(const C& c) {
    std::string why;
    if (c.tcp) why = c.tcp->lastError();
#if MQTT_TLS_SOCKETS
    if (c.tls) why = c.tls->lastError();
#endif
    return why.empty() ? std::string("could not connect") : why;
}

AppPublishResult resultOf(MqttResult result, bool confirmDelivered) {
    AppPublishResult out;
    switch (result) {
    case MqttResult::Ok:
        out.outcome = confirmDelivered ? AppPublishOutcome::Delivered : AppPublishOutcome::Accepted;
        out.matched = 1;
        out.acknowledged = confirmDelivered ? 1 : 0;
        return out;
    case MqttResult::NoMatchingSubscribers:
        out.outcome = confirmDelivered ? AppPublishOutcome::NoSubscribers : AppPublishOutcome::Accepted;
        return out;
    case MqttResult::ConnectionLost:
    case MqttResult::SessionLost:
    case MqttResult::AckTimeout:
        // It may have arrived: nobody can say.
        out.outcome = AppPublishOutcome::Partial;
        out.pending = 1;
        return out;
    case MqttResult::NotConnected:
        out.outcome = AppPublishOutcome::NotConnected;
        return out;
    case MqttResult::NotAuthorized: out.reasonCode = MQ_REASON_NOT_AUTHORIZED; break;
    case MqttResult::TopicNameInvalid: out.reasonCode = MQ_REASON_TOPIC_NAME_INVALID; break;
    case MqttResult::PayloadFormatInvalid: out.reasonCode = MQ_REASON_PAYLOAD_FORMAT_INVALID; break;
    case MqttResult::QuotaExceededByPeer: out.reasonCode = MQ_REASON_QUOTA_EXCEEDED; break;
    case MqttResult::PacketIdentifierInUse: out.reasonCode = MQ_REASON_PACKET_IDENTIFIER_IN_USE; break;
    default: out.reasonCode = MQ_REASON_UNSPECIFIED_ERROR; break;
    }
    out.outcome = AppPublishOutcome::Refused;
    return out;
}

std::string hex(const std::string& bytes) {
    static const char digits[] = "0123456789abcdef";
    std::string out;
    for (unsigned char c : bytes) {
        out.push_back(digits[c >> 4]);
        out.push_back(digits[c & 15]);
    }
    return out;
}

std::string unhex(const std::string& text) {
    std::string out;
    for (size_t i = 0; i + 1 < text.size(); i += 2) out.push_back((char)std::strtoul(text.substr(i, 2).c_str(), nullptr, 16));
    return out;
}

// The Will, whole - its MQTT 5 properties too - as the stores keep a message.
std::string willText(const AppPublish& will) {
    AppMessage m;
    m.topic = will.topic;
    m.payload = will.payload;
    m.qos = will.qos;
    m.retain = will.retain;
    m.properties = will.properties;
    return hex(encodeAppMessage(m));
}

bool willFrom(const std::string& text, AppPublish& will) {
    const std::string bytes = unhex(text);
    AppMessage m;
    if (!decodeAppMessage(bytes.data(), (int)bytes.size(), m)) return false;
    will.topic = m.topic;
    will.payload = m.payload;
    will.qos = m.qos;
    will.retain = m.retain;
    will.properties = m.properties;
    return true;
}

// The CONNECT's Will: its message, properties and delay.
void applyWill(const AppConnectionSpec& spec, MqttConnectionConfig& connection) {
    connection.will = MqttWillConfig();
    if (!spec.hasWill) return;
    connection.will.set(spec.will.topic.c_str(), (const uint8_t*)spec.will.payload.data(), spec.will.payload.size(),
                        spec.will.qos, spec.will.retain);
    connection.will.properties = spec.will.properties;
    connection.will.delayInterval = spec.willDelaySeconds;
}

// A Sparkplug host's STATE, as the library writes it (QoS 1, retained).
std::string stateJson(bool online, uint64_t timestamp) {
    return std::string("{\"online\":") + (online ? "true" : "false") + ",\"timestamp\":" + std::to_string(timestamp) + "}";
}

// How long a backoff of up to `base` waits: anywhere from 100 ms to all of it
// (full jitter, as the library's client: MqttClient::scheduleReconnect).
std::chrono::milliseconds jittered(uint32_t base) {
    static std::mt19937 random{std::random_device{}()};
    static std::mutex lock;
    const uint32_t least = std::min<uint32_t>(base, 100);
    std::lock_guard<std::mutex> guard(lock);
    return std::chrono::milliseconds(least + random() % (base - least + 1));
}

const uint32_t kPacketBytes = 65536;       // a connection's buffers: the largest packet either way
const uint32_t kLinkPacketBytes = 1114112;  // a Kafka link's: Kafka's 1 MiB record, and the packet around it
const uint32_t kBackoffMaxMs = 60000;      // the library's reconnectMaximumMs
const uint32_t kStableMs = 30000;          // MQ_RECONNECT_STABLE_MS
const uint32_t kBusyFloorMs = 60000;       // quota exceeded, connection rate exceeded: not sooner
const uint32_t kRefusedRetryMs = 300000;   // a refused login, tried again (MqttClientConfig::rejectedCredentialsRetryMs)

// A connection's id names files beside connections.json (its TLS material),
// so it is a name, not a path: letters, digits and . _ - : @, at most 128,
// and not starting with a dot - as EMQX names its bridges.
bool idIsName(const std::string& id) {
    if (id.empty() || id.size() > 128 || id[0] == '.') return false;
    for (char ch : id) {
        const bool ok = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
                        ch == '.' || ch == '_' || ch == '-' || ch == ':' || ch == '@';
        if (!ok) return false;
    }
    return true;
}

// A CONNACK that refused this login - the only refusal that waits for
// kRefusedRetryMs (and Put or Reconnect) rather than the backoff. Every other
// one - unavailable, busy, quota, rate, use another server - backs off.
bool loginRefusal(uint8_t code, bool v311) {
    if (v311) return code == MQ_RETURN_BAD_CREDENTIALS || code == MQ_RETURN_NOT_AUTHORIZED;
    return code == MQ_REASON_BAD_USER_NAME_OR_PASSWORD || code == MQ_REASON_NOT_AUTHORIZED || code == 0x8A /* banned */ ||
           code == 0x8C /* bad authentication method */;
}

std::vector<std::string> topicParts(const std::string& topic) {
    std::vector<std::string> parts;
    size_t start = 0;
    for (size_t i = 0; i <= topic.size(); i++) {
        if (i == topic.size() || topic[i] == '/') {
            parts.push_back(topic.substr(start, i - start));
            start = i + 1;
        }
    }
    return parts;
}

bool writePrivate(const std::string& path, const std::string& text) {
    std::string ignored;
    return server_os::writeFileAtomically(path, text, ignored, 0600);
}

std::string toJson(const AppConnectionSpec& s) {
    json::Object o;
    o.add("id", s.id);
    std::vector<std::string> servers;
    for (const AppEndpoint& e : s.servers) servers.push_back(e.host + ":" + std::to_string(e.port));
    o.raw("servers", json::array(servers));
    o.add("clientId", s.clientId);
    if (s.hasUsername) o.add("username", s.username);
    if (s.hasPassword) o.add("password", s.password);
    o.add("tls", s.tls);
    o.add("ca", s.caPem);
    o.add("certificate", s.certificatePem);
    o.add("key", s.keyPem);
    o.add("protocol", (long long)s.protocol);
    o.add("keepalive", (long long)s.keepaliveSeconds);
    o.add("cleanStart", s.cleanStart);
    o.add("sessionExpiry", (long long)s.sessionExpirySeconds);
    if (s.hasWill) {
        json::Object w;
        w.add("topic", s.will.topic);
        w.add("payload", s.will.payload);
        w.add("qos", (long long)s.will.qos);
        w.add("retain", s.will.retain);
        w.add("whole", willText(s.will));
        w.add("delay", (long long)s.willDelaySeconds);
        o.raw("will", w.str());
    }
    o.add("whenAsked", s.reconnectWhenAsked);
    if (s.thisServer) o.add("thisServer", true);
    if (s.kafka) {
        json::Object k;
        k.add("cluster", s.link.cluster);
        k.raw("in", json::array(s.link.inTopics));
        k.add("pattern", s.link.inPattern);
        k.add("out", s.link.outTopic);
        k.add("key", s.link.key);
        k.add("group", s.link.consumerGroup);
        k.add("stateConnection", s.link.stateConnection);
        o.raw("kafka", k.str());
    }
    o.add("primaryCheck", (long long)s.primaryCheckSeconds);
    o.add("receiveMaximum", (long long)s.receiveMaximum);
    o.add("topicAliasMaximum", (long long)s.topicAliasMaximum);
    o.add("maximumPacketSize", (long long)s.maximumPacketSize);
    if (s.connectTimeoutSeconds) o.add("connectTimeout", (long long)s.connectTimeoutSeconds);
    if (s.ackTimeoutSeconds) o.add("ackTimeout", (long long)s.ackTimeoutSeconds);
    if (s.storeAndForward) {
        json::Object k;
        k.add("messages", (long long)s.storeMaxMessages);
        k.add("bytes", (long long)s.storeMaxBytes);
        k.add("dropOldest", s.storeDropOldest);
        o.raw("store", k.str());
    }
    return o.str();
}

bool fromJson(const json::Value& v, AppConnectionSpec& s) {
    s.id = v.str("id");
    std::vector<std::string> servers;
    v.strings("servers", servers);
    for (const std::string& server : servers) {
        const size_t colon = server.rfind(':');
        AppEndpoint e;
        e.host = server.substr(0, colon);
        e.port = colon == std::string::npos ? 0 : (uint16_t)std::atoi(server.c_str() + colon + 1);
        s.servers.push_back(e);
    }
    s.clientId = v.str("clientId");
    s.hasUsername = v.has("username");
    s.username = v.str("username");
    s.hasPassword = v.has("password");
    s.password = v.str("password");
    s.tls = v.flag("tls");
    s.caPem = v.str("ca");
    s.certificatePem = v.str("certificate");
    s.keyPem = v.str("key");
    s.protocol = (uint8_t)v.integer("protocol", 5);
    s.keepaliveSeconds = (uint32_t)v.integer("keepalive", 30);
    s.cleanStart = v.flag("cleanStart");
    s.sessionExpirySeconds = (uint32_t)v.integer("sessionExpiry");
    if (v.has("will")) {
        const json::Value& w = v["will"];
        s.hasWill = true;
        s.will.topic = w.str("topic");
        s.will.payload = w.str("payload");
        s.will.qos = (uint8_t)w.integer("qos");
        s.will.retain = w.flag("retain");
        if (w.has("whole")) willFrom(w.str("whole"), s.will);
        s.willDelaySeconds = (uint32_t)w.integer("delay");
    }
    s.reconnectWhenAsked = v.flag("whenAsked");
    s.thisServer = v.flag("thisServer");
    if (v.has("kafka")) {
        const json::Value& k = v["kafka"];
        s.kafka = true;
        s.link.cluster = k.str("cluster");
        k.strings("in", s.link.inTopics);
        s.link.inPattern = k.str("pattern");
        s.link.outTopic = k.str("out");
        s.link.key = k.str("key");
        s.link.consumerGroup = k.str("group");
        s.link.stateConnection = k.str("stateConnection");
    }
    s.primaryCheckSeconds = (uint32_t)v.integer("primaryCheck", 60);
    s.receiveMaximum = (uint16_t)v.integer("receiveMaximum");
    s.topicAliasMaximum = (uint16_t)v.integer("topicAliasMaximum");
    s.maximumPacketSize = (uint32_t)v.integer("maximumPacketSize");
    s.connectTimeoutSeconds = (uint32_t)v.integer("connectTimeout");
    s.ackTimeoutSeconds = (uint32_t)v.integer("ackTimeout");
    if (v.has("store")) {
        const json::Value& k = v["store"];
        s.storeAndForward = true;
        s.storeMaxMessages = (uint64_t)k.integer("messages", 100000);
        s.storeMaxBytes = (uint64_t)k.integer("bytes", 64ll << 20);
        s.storeDropOldest = k.flag("dropOldest");
    }
    return !s.id.empty() && (!s.servers.empty() || s.thisServer || s.kafka);
}

bool sameSpec(const AppConnectionSpec& a, const AppConnectionSpec& b) { return toJson(a) == toJson(b); }

} // namespace

// =============================================================================
// A SPARKPLUG HOST ON A CONNECTION
// =============================================================================
//
// The library's host does the protocol - STATE, sessions, seq, aliases - on
// the connection's own client. This decides when to ask for a rebirth, with
// limits, and tells the connection what each alias of the message it is about
// to hand on meant: the library sees a message first (a client's hooks come
// before its handler), so its tables are this message's.

struct AppConnections::Host {
    AppSparkplugHostSpec spec;
    std::unique_ptr<MqttSparkplugHost> library;
    // What the message being received means, for received() straight after.
    std::vector<AppSparkplugName> names;
    uint8_t flags = 0;
    bool fresh = false;
    std::map<std::string, uint32_t> gapsSeen;                // node -> its gaps at its last message
    std::map<std::string, Clock::time_point> lastAsked;      // node -> its last rebirth request
    std::map<std::string, mq_time_t> reorderAnswered;        // node -> the gap already answered
    uint64_t rebirths = 0;
    bool wasBorn = false;
    uint32_t failures = 0;                                   // decode failures already answered

    static std::string keyOf(const char* group, const char* node) {
        return std::string(group ? group : "") + "/" + (node ? node : "");
    }

    // A rebirth, unless this node was asked within the limit (or a person asks).
    bool ask(const char* group, const char* node, bool force) {
        const std::string key = keyOf(group, node);
        const Clock::time_point now = Clock::now();
        auto last = lastAsked.find(key);
        if (!force && last != lastAsked.end() &&
            now - last->second < std::chrono::milliseconds(spec.rebirthMinIntervalMs)) {
            return false;
        }
        if (!library->requestRebirth(group, node)) return false;
        lastAsked[key] = now;
        rebirths++;
        return true;
    }

    static void inbound(void* context, const MqttSparkplugInbound& in) {
        Host& h = *static_cast<Host*>(context);
        h.names.clear();
        h.flags = 0;
        h.fresh = true;
        const MqttSparkplugTrackedNode* node = h.library->findNode(in.group, in.node);
        const bool data = in.type == MqttSparkplugType::NData || in.type == MqttSparkplugType::DData;
        bool unknownAlias = false;
        for (size_t i = 0; in.payload != nullptr && i < in.payload->metricCount; i++) {
            const MqttSparkplugMetric& m = in.payload->metrics[i];
            if (!m.hasAlias) continue;
            AppSparkplugName n;
            n.alias = m.alias;
            const char* name = node ? node->metricName(m.alias) : nullptr;
            if (name) n.name = name;
            n.datatype = node ? (uint32_t)node->metricType(m.alias) : 0;
            if (!name && data) unknownAlias = true;
            h.names.push_back(n);
        }
        bool gap = false;
        if (node != nullptr) {
            if (in.type == MqttSparkplugType::NDeath && node->online) h.flags |= AppMessage::kSparkplugStale;
            uint32_t& seen = h.gapsSeen[keyOf(in.group, in.node)];
            if (node->gaps > seen) {
                h.flags |= AppMessage::kSparkplugGap;
                gap = true;
            }
            seen = node->gaps;
        }
        if (!h.spec.rebirthOnGap) return;
        const bool noSession = (data || in.type == MqttSparkplugType::DBirth) && (node == nullptr || !node->online);
        if ((gap && h.spec.reorderTimeoutMs == 0) || unknownAlias || noSession) h.ask(in.group, in.node, false);
    }
};

// =============================================================================
// A SPARKPLUG EDGE NODE ON A CONNECTION
// =============================================================================
//
// The library's node does the protocol on the connection's own client. Values
// set while it is not born are kept in the outbox under this node's key, and
// sent with is_historical once it is.

struct AppConnections::EdgeNode {
    AppSparkplugEdgeNodeSpec spec;
    std::unique_ptr<MqttSparkplugNode> library;
    uint64_t history = 0, historyLost = 0;
    // The bytes of each span metric's current value, by its index: the
    // library's node keeps a span into them until the next value (a rebirth
    // reads it), so they outlive the call that set them.
    std::vector<std::string> values;
    // History sent this session, and when: QoS 0, so it leaves the outbox only
    // once the node has stayed born for two keep-alives after it went (as
    // MqttSparkplugNode::publishRecords asks) - lost with the link before then,
    // it goes again after the next birth, is_historical as before.
    std::vector<std::pair<uint64_t, Clock::time_point>> sentHistory;
    uint64_t sentUpTo = 0;
    // WHAT OUTLIVES A RESTART (AppConnections::saveEdgeState): the bdSeq last
    // used, so a node started again moves on from it rather than from zero,
    // and every metric's current value with when it was set, so the birth
    // after a restart carries what the application last gave - not nulls,
    // which a host would put over the values it had.
    std::vector<uint64_t> setAtMs;
    int savedBdSeq = -1;
    bool valuesChanged = false, saveFailed = false;
    Clock::time_point savedAt;
    std::string key() const { return "sparkplug-node:" + spec.id; }
    const char* device(const std::string& d) const { return d.empty() ? nullptr : d.c_str(); }
    // A metric's current value - what the next NDATA, or the next birth,
    // carries - its text in `values`. Whether it changed.
    bool setCurrent(size_t index, MqttSparkplugValue value, const std::string& text, uint64_t at) {
        if (value.isSpan() && !value.isNull) {
            std::string owned = text;           // a buffer of its own: see `values`
            values[index].swap(owned);
            value.as.bytes.data = (const uint8_t*)values[index].data();
            value.as.bytes.length = (uint32_t)values[index].size();
        }
        if (!library->setAt(index, value)) return false;
        setAtMs[index] = at;
        valuesChanged = true;
        return true;
    }
};

namespace {

// A value kept while the node was not born, as an outbox message: its metric
// name the topic, its device the publisher, its time the broker time.
AppMessage keptValue(const std::string& device, const AppSparkplugSet& set) {
    AppMessage m;
    m.topic = set.name;
    m.clientId = device;
    m.brokerTimeMs = (int64_t)set.timestamp;
    const uint32_t type = (uint32_t)set.value.type;
    m.payload.assign((const char*)&type, sizeof type);
    m.payload.push_back(set.value.isNull ? 1 : 0);
    m.payload.append((const char*)&set.value.as.unsignedValue, sizeof set.value.as.unsignedValue);
    if (set.value.isSpan()) m.payload += set.text;
    return m;
}

bool valueOfKept(const AppMessage& m, AppSparkplugSet& set) {
    const size_t head = sizeof(uint32_t) + 1 + sizeof(uint64_t);
    if (m.payload.size() < head) return false;
    uint32_t type = 0;
    std::memcpy(&type, m.payload.data(), sizeof type);
    set.name = m.topic;
    set.timestamp = (uint64_t)m.brokerTimeMs;
    set.value.type = (MqttSparkplugDataType)type;
    set.value.isNull = m.payload[sizeof type] != 0;
    std::memcpy(&set.value.as.unsignedValue, m.payload.data() + sizeof type + 1, sizeof(uint64_t));
    if (set.value.isSpan()) {
        set.text = m.payload.substr(head);
        set.value.as.bytes.data = (const uint8_t*)set.text.data();
        set.value.as.bytes.length = (uint32_t)set.text.size();
    }
    return true;
}

// The name a Template instance's template_ref gives; "" when it has none.
std::string templateRefOf(const MqttSparkplugValue& value) {
    bool isDefinition = false;
    const uint8_t* ref = nullptr;
    size_t length = 0;
    if (!mq_sparkplugTemplateHeader(value.as.bytes.data, value.as.bytes.length, isDefinition, ref, length) ||
        isDefinition || ref == nullptr) {
        return std::string();
    }
    return std::string((const char*)ref, length);
}

// An edge node's file in the state directory: its id hashed (FNV-1a), since an
// edge node's id may hold what a file name may not. The id is in the file too,
// and checked.
std::string edgeStateName(const std::string& id) {
    uint64_t h = 1469598103934665603ull;
    for (unsigned char ch : id) {
        h ^= ch;
        h *= 1099511628211ull;
    }
    char name[48];
    std::snprintf(name, sizeof name, "sparkplug-node-%016llx.json", (unsigned long long)h);
    return name;
}

std::string hexOf(const std::string& bytes) {
    static const char digits[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        out.push_back(digits[c >> 4]);
        out.push_back(digits[c & 15]);
    }
    return out;
}

std::string bytesOfHex(const std::string& hex) {
    auto nibble = [](char c) { return c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10; };
    std::string out;
    for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back((char)(nibble(hex[i]) << 4 | nibble(hex[i + 1])));
    return out;
}

} // namespace

// =============================================================================
// ONE CONNECTION
// =============================================================================

struct AppConnections::Connection {
    AppConnections* owner = nullptr;
    AppConnectionSpec spec;
    std::unique_ptr<MqttTransport> transport;
    // The same transport, typed, for its own words when a connect fails (this target has no RTTI).
    const MqttTcpTransport* tcp = nullptr;
#if MQTT_TLS_SOCKETS
    const MqttTlsTransport* tls = nullptr;
#endif
    MqttClient client;

    AppConnectionState state = AppConnectionState::Connecting;
    uint32_t serverIndex = 0;
    bool sessionPresent = false;
    std::string lastError;
    int64_t sinceMs = 0;
    uint64_t messagesIn = 0, messagesOut = 0;

    MqttState seen = MqttState::Idle;          // the client's state at the last look
    bool attempting = false;                   // a connect() is in progress
    bool reachedThisAttempt = false;           // ...and got to Connected
    bool asked = false;                        // reconnect() wants a new attempt now
    Clock::time_point nextAttempt = Clock::now();
    // The backoff the next failure waits up to (full jitter, nextDelay()): it
    // doubles to kBackoffMaxMs and starts again only after a connection that
    // stayed up kStableMs - so a broker that accepts and drops at once is not
    // asked every 200 ms, and a site's gateways do not come back in step.
    uint32_t backoffMs = 1000;
    Clock::time_point upSince;
    // Back to the first server: how often it went wrong lately - each time, the
    // first is checked half as often (a load balancer that takes TCP for a
    // broker that is down would otherwise move the connection every minute).
    bool switchedBack = false;
    uint32_t primaryFailures = 0;
    // Store and forward: the other broker said quota exceeded (0x97) - nothing
    // more is sent until then, and what it refused goes again.
    Clock::time_point forwardAfter;
    uint32_t quotaBackoffMs = 0;

    std::map<std::string, uint8_t> subscribed; // what the remote broker has been asked for, with its options
    bool filtersChanged = true;

    // The first server, while connected to another: is it back?
    std::unique_ptr<MqttTcpTransport> probe;
    Clock::time_point nextProbe;

    struct Pending {
        AppConnections::Connection* connection;
        bool confirmDelivered;
        std::function<void(const AppPublishResult&)> done;
    };
    std::vector<std::unique_ptr<Pending>> pending;

    static void completed(void* context, MqttResult result, uint16_t) {
        Pending* p = static_cast<Pending*>(context);
        if (p->done) p->done(resultOf(result, p->confirmDelivered));
        p->done = nullptr;
        Connection& self = *p->connection;
        self.pending.erase(std::remove_if(self.pending.begin(), self.pending.end(),
                                          [p](const std::unique_ptr<Pending>& one) { return one.get() == p; }),
                           self.pending.end());
    }

    // --- Store and forward ---
    uint64_t stored = 0, storedBytes = 0, storedDropped = 0, storedExpired = 0;
    std::set<uint64_t> inClient;               // kept messages handed to the client, not yet answered
    struct Waiting {                           // CONFIRM_DELIVERED: until the other broker has it, or the deadline
        uint64_t id;
        Clock::time_point deadline;
        std::function<void(const AppPublishResult&)> done;
    };
    std::vector<Waiting> waiting;
    struct Sent {
        Connection* connection;
        uint64_t id;
        size_t bytes;
    };
    std::vector<std::unique_ptr<Sent>> sent;

    // A kept message is answered by everyone waiting for it.
    void settle(uint64_t id, const AppPublishResult& result) {
        for (size_t i = 0; i < waiting.size(); i++) {
            if (waiting[i].id != id) continue;
            if (waiting[i].done) waiting[i].done(result);
            waiting.erase(waiting.begin() + (long)i--);
        }
    }

    // The other broker's answer to a kept message. Taken (or refused: its answer
    // is final) - it leaves the store; lost on the way - it stays, and goes again.
    static void forwarded(void* context, MqttResult result, uint16_t) {
        Sent* s = static_cast<Sent*>(context);
        Connection& self = *s->connection;
        // Not with the client any more: given up to make room while it was on its way.
        const bool held = self.inClient.erase(s->id) > 0;
        // Quota exceeded is the other broker saying "not now": kept, sent again
        // once forward() waits out the pause it sets. Any other refusal is final.
        const bool busy = result == MqttResult::QuotaExceededByPeer;
        if (busy) {
            self.quotaBackoffMs = self.quotaBackoffMs ? std::min<uint32_t>(self.quotaBackoffMs * 2, kBackoffMaxMs) : 1000;
            self.forwardAfter = Clock::now() + jittered(self.quotaBackoffMs);
        } else if (result == MqttResult::Ok) {
            self.quotaBackoffMs = 0;
        }
        // Cancelled: the client was ended under it (close() - a stop, a Put with
        // a change). Not acknowledged, so still kept - it used to be dropped,
        // and every shutdown lost what was on its way.
        const bool lost = result == MqttResult::ConnectionLost || result == MqttResult::SessionLost ||
                          result == MqttResult::AckTimeout || result == MqttResult::NotConnected ||
                          result == MqttResult::Cancelled || busy;
        if (held && !lost) {
            self.owner->_outbox.drop(s->id);
            if (self.stored > 0) self.stored--;
            self.storedBytes -= std::min<uint64_t>(self.storedBytes, s->bytes);
            AppPublishResult answer = resultOf(result, true);
            if (result == MqttResult::NoMatchingSubscribers) answer.outcome = AppPublishOutcome::NoSubscribers;
            self.settle(s->id, answer);
        }
        self.sent.erase(std::remove_if(self.sent.begin(), self.sent.end(),
                                       [s](const std::unique_ptr<Sent>& one) { return one.get() == s; }),
                        self.sent.end());
    }

    static void established(void* context, bool sessionPresent) {
        static_cast<Connection*>(context)->sessionPresent = sessionPresent;
    }

    static void received(void* context, const MqttMessage& message) {
        Connection& self = *static_cast<Connection*>(context);
        self.messagesIn++;
        std::shared_ptr<AppMessage> m(new AppMessage());
        m->topic.assign((const char*)message.topic.data, message.topic.length);
        m->payload.assign((const char*)message.payload.data, message.payload.length);
        m->qos = message.qos;
        m->retain = message.retain;
        if (message.properties) m->properties.copyForForwarding(*message.properties);
        m->brokerTimeMs = utcNowMs();
        m->connection = self.spec.id;
        if (self.edge && (m->topic.find("/NCMD/") != std::string::npos || m->topic.find("/DCMD/") != std::string::npos)) {
            // A command to this node names its metrics by the aliases its births gave them.
            const MqttSparkplugNode& node = *self.edge->library;
            for (size_t i = 0; i < node.pointCount(); i++) {
                const MqttSparkplugPoint* point = node.pointAt(i);
                if (point == nullptr) continue;
                AppSparkplugName n;
                n.alias = point->alias;
                n.name = point->name;
                n.datatype = (uint32_t)point->type;
                m->sparkplugNames.push_back(n);
            }
        }
        if (self.host) {
            Host& h = *self.host;
            if (h.fresh) {
                m->sparkplugNames.swap(h.names);
                m->sparkplugFlags = h.flags;
            }
            h.fresh = false;
            // A payload the library could not read: its node is asked again.
            if (h.library->decodeFailures() != h.failures) {
                h.failures = h.library->decodeFailures();
                const std::vector<std::string> parts = topicParts(m->topic);
                if (h.spec.rebirthOnGap && parts.size() >= 4 && parts[0] == "spBv1.0" && parts[1] != "STATE") {
                    h.ask(parts[1].c_str(), parts[3].c_str(), false);
                }
            }
        }
        self.owner->_hub.connectionMessage(self.spec.id, m);
    }

    // This server itself, as reachThisServer() says now, or a Kafka link's
    // own broker; or one of `servers`.
    AppEndpoint here;
    bool hereTls = false;
    bool local() const { return spec.thisServer || spec.kafka; }
    const AppEndpoint& endpoint(uint32_t index) const {
        return local() || spec.servers.empty() ? here : spec.servers[index % spec.servers.size()];
    }
    bool usesTls() const { return local() ? hereTls : spec.tls; }

    // --- A Kafka link: what it is doing, looked at twice a second ---
    AppKafkaLinkStatus link;
    Clock::time_point linkPolled;

    // --- The STATE connection of a host on a Kafka link (AppKafkaLinkSpec::
    // stateConnection): its Will the host's offline STATE, chosen at
    // `stateTimestamp`, and the online one published on each session it was
    // armed for - Kafka has no Will, so the STATE lives here.
    bool stateWill = false;                    // the next CONNECT carries it
    bool stateArmed = false;                   // ...and this session's did
    bool stateBorn = false;                    // online published on this session
    std::string stateTopic;
    uint64_t stateTimestamp = 0;
    bool stateReleasing = false;               // reconnect without it, once the offline STATE has gone
    Clock::time_point stateReleaseAt;

    // The host stopped: offline by hand, and its Will let go - its CONNECT
    // still carries the old offline STATE, which would be published, retained,
    // over a newer host's if it dropped. The reconnect waits a moment, so the
    // offline STATE leaves before the DISCONNECT.
    void releaseState() {
        if (!stateWill) return;
        if (stateBorn && client.isConnected()) {
            const std::string offline = stateJson(false, (uint64_t)utcNowMs());
            client.publish(stateTopic.c_str(), (const uint8_t*)offline.data(), offline.size(), MQ_QOS_1, true);
        }
        stateBorn = false;
        stateWill = false;
        if (client.state() != MqttState::Idle) {
            stateReleasing = true;
            stateReleaseAt = Clock::now() + std::chrono::milliseconds(500);
        }
    }
    uint16_t portOf(const AppEndpoint& e) const { return e.port ? e.port : (usesTls() ? 8883 : 1883); }

    // Last, so they go before the client they are attached to.
    std::unique_ptr<Host> host;
    std::unique_ptr<EdgeNode> edge;

    void setState(AppConnectionState next, const std::string& error = std::string()) {
        if (next == state && error == lastError) return;
        state = next;
        if (!error.empty() || next == AppConnectionState::Connected) lastError = error;
        sinceMs = utcNowMs();
        owner->announce(*this);
    }
};

namespace {

// The Will this connection's next CONNECT carries: its own, or - the STATE
// connection of a host on a Kafka link - that host's offline STATE.
void willOf(AppConnections::Connection& c, MqttConnectionConfig& connection) {
    applyWill(c.spec, connection);
    c.stateArmed = c.stateWill;
    if (!c.stateWill) return;
    const std::string offline = stateJson(false, c.stateTimestamp);
    connection.will = MqttWillConfig();
    connection.will.set(c.stateTopic.c_str(), (const uint8_t*)offline.data(), offline.size(), MQ_QOS_1, true);
    connection.will.delayInterval = 0;
}

// Its client's login, from where it connects to (this server, or a link's broker).
void loginOf(const AppLocalAddress& where, MqttConnectionConfig& connection) {
    if (!where.hasLogin) return;
    connection.userName = where.user.c_str();
    connection.hasUserName = true;
    connection.password.assign(where.password.begin(), where.password.end());
    connection.hasPassword = true;
}

} // namespace

// =============================================================================
// THE MANAGER
// =============================================================================

AppConnections::AppConnections(AppHub& hub, const std::string& stateDirectory, bool syncFull)
    : _hub(hub), _directory(stateDirectory), _syncFull(syncFull) {}

AppConnections::~AppConnections() { stop(); }

bool AppConnections::start(std::string& error) {
    // What store and forward held when the server stopped, before the
    // connections it belongs to are opened again.
    if (!_outbox.open(_directory, _syncFull, error)) return false;
    if (!_directory.empty()) {
        server_os::makeDirectory(_directory, 0700);
        if (!loadHosts(error)) return false;
        if (!load(error)) return false;
    }
    _hub.attachConnections(this);
    _thread = std::thread([this] { run(); });
    return true;
}

void AppConnections::stop() {
    {
        std::lock_guard<std::mutex> guard(_lock);
        if (_quit) return;
        _quit = true;
    }
    _wake.notify_all();
    if (_thread.joinable()) _thread.join();
    _hub.attachConnections(nullptr);
    for (auto& one : _connections) close(*one.second);
}

void AppConnections::post(std::function<void()> command) {
    {
        std::lock_guard<std::mutex> guard(_lock);
        _commands.push_back(std::move(command));
    }
    _wake.notify_all();
}

void AppConnections::call(std::function<void()> command) const {
    {
        // Stopped (or never started): there is no thread to run it.
        std::lock_guard<std::mutex> guard(_lock);
        if (_quit || !_thread.joinable()) return;
    }
    std::mutex done;
    std::condition_variable answered;
    bool finished = false;
    const_cast<AppConnections*>(this)->post([&] {
        command();
        std::lock_guard<std::mutex> guard(done);
        finished = true;
        answered.notify_all();
    });
    std::unique_lock<std::mutex> guard(done);
    answered.wait(guard, [&] { return finished; });
}

// --- On the manager's thread -------------------------------------------------

void AppConnections::run() {
    std::unique_lock<std::mutex> guard(_lock);
    while (!_quit) {
        std::vector<std::function<void()>> commands;
        commands.swap(_commands);
        guard.unlock();
        for (std::function<void()>& command : commands) command();
        for (auto& one : _connections) service(*one.second);
        guard.lock();
        if (_commands.empty() && !_quit) _wake.wait_for(guard, std::chrono::milliseconds(5));
    }
}

void AppConnections::open(Connection& c) {
    c.owner = this;
    MqttConnectionConfig connection;
    connection.clientId = c.spec.clientId.c_str();
    connection.version = c.spec.protocol == 4 ? MqttVersion::V311 : MqttVersion::V5;
    connection.keepAlive = (uint16_t)c.spec.keepaliveSeconds;
    connection.cleanStart = c.spec.cleanStart;
    if (c.spec.sessionExpirySeconds) {
        connection.sessionExpiryInterval = c.spec.sessionExpirySeconds;
        connection.hasSessionExpiry = true;
    }
    if (c.spec.hasUsername) {
        connection.userName = c.spec.username.c_str();
        connection.hasUserName = true;
    }
    if (c.spec.hasPassword) {
        connection.password.assign(c.spec.password.begin(), c.spec.password.end());
        connection.hasPassword = true;
    }
    if (c.spec.thisServer) {
        ThisServer where;
        if (_thisServer && _thisServer(where)) {
            c.here = AppEndpoint{where.host, where.port};
            c.hereTls = where.tls;
            loginOf(where, connection);
        }
    }
    if (c.spec.kafka) {
        // Its own broker, bridged to Kafka: made now (or kept, its link unchanged).
        AppLocalAddress where;
        std::string why;
        c.hereTls = false;
        if (_links && _links->open(c.spec.id, c.spec.link, where, why)) {
            c.here = AppEndpoint{where.host, where.port};
            loginOf(where, connection);
        }
    }
    willOf(c, connection);
    connection.receiveMaximum = c.spec.receiveMaximum;
    connection.topicAliasMaximum = c.spec.topicAliasMaximum;
    // What it can take, always said: its receive buffer, or less if the spec
    // asks. Not saying it let the other broker send a packet the buffer could
    // not hold - the connection failed, and the message came again with the
    // next session, for ever. (MQTT 5 only: 3.1.1 cannot say it.)
    const uint32_t buffer = c.spec.kafka ? kLinkPacketBytes : kPacketBytes;
    connection.maximumPacketSize =
        c.spec.maximumPacketSize != 0 ? std::min<uint32_t>(c.spec.maximumPacketSize, buffer) : buffer;
    const AppEndpoint& first = c.endpoint(c.serverIndex);
    connection.host = first.host.c_str();
    connection.port = c.portOf(first);
#if MQTT_TLS_SOCKETS
    if (c.usesTls()) {
        MqttTlsTransport* tls = new MqttTlsTransport();
        // This server, over loopback: its own certificate, not checked.
        if (c.spec.thisServer) tls->setInsecureNoCertificateValidation();
        if (!c.spec.caPem.empty()) tls->setCaCertificate(c.spec.caPem.c_str());
        if (!c.spec.certificatePem.empty() && !_directory.empty()) {
            const std::string base = _directory + "/" + c.spec.id;
            writePrivate(base + ".crt", c.spec.certificatePem);
            writePrivate(base + ".key", c.spec.keyPem);
            tls->setClientCertificate((base + ".crt").c_str(), (base + ".key").c_str());
        }
        c.transport.reset(tls);
        c.tls = tls;
        c.tcp = nullptr;
    } else
#endif
    {
        MqttTcpTransport* tcp = new MqttTcpTransport();
        c.transport.reset(tcp);
        c.tcp = tcp;
#if MQTT_TLS_SOCKETS
        c.tls = nullptr;
#endif
    }
    const ConnectionOutbox::Totals kept = _outbox.totals(c.spec.id);
    c.stored = kept.messages;
    c.storedBytes = kept.bytes;
    MqttClientConfig behaviour;
    // A Kafka link's broker acknowledges once Kafka has the record, however long
    // Kafka is away: the acknowledgement is not given up on (what is held is
    // in order, in librdkafka's queue, and goes when Kafka is back).
    if (c.spec.kafka) {
        behaviour.acknowledgementTimeoutMs = 0;
    } else {
        const uint32_t ack = c.spec.ackTimeoutSeconds
                                 ? c.spec.ackTimeoutSeconds
                                 : std::max<uint32_t>(120, 2 * std::max<uint32_t>(c.spec.keepaliveSeconds, 1));
        behaviour.acknowledgementTimeoutMs = (mq_time_t)ack * 1000;
    }
    behaviour.connectTimeoutMs = (mq_time_t)(c.spec.connectTimeoutSeconds ? c.spec.connectTimeoutSeconds : 30) * 1000;
    behaviour.autoReconnect = false;     // the manager decides: which server, and when
    behaviour.queueCapacity = 256;       // room for store and forward's window as well
    behaviour.receiveBufferSize = buffer;
    behaviour.transmitBufferSize = buffer;
    c.client.begin(*c.transport, connection, behaviour);
    c.client.onMessage(&Connection::received, &c);
    c.client.onSessionEstablished(&Connection::established, &c);
    c.state = AppConnectionState::Connecting;
    c.nextAttempt = Clock::now();
    c.filtersChanged = true;
    startHost(c);
    startEdgeNode(c);
}

void AppConnections::startEdgeNode(Connection& c) {
    if (c.edge) saveEdgeState(*c.edge, true);
    c.edge.reset();
    for (const auto& one : _edgeNodes) {
        if (one.second.connection != c.spec.id) continue;
        std::unique_ptr<EdgeNode> e(new EdgeNode());
        e->spec = one.second;
        e->library.reset(new MqttSparkplugNode(c.client));
        MqttSparkplugNode& node = *e->library;
        node.group(e->spec.group.c_str());
        node.node(e->spec.node.c_str());
        if (!e->spec.primaryHostId.empty()) node.primaryHost(e->spec.primaryHostId.c_str());
        node.epochNow((uint64_t)utcNowMs());
        for (const std::string& d : e->spec.devices) node.addDevice(d.c_str());
        // The definitions first: an instance set later is checked against them.
        for (const AppSparkplugTemplateDefinition& t : e->spec.templates) {
            node.declareTemplate(t.name.c_str(), (const uint8_t*)t.encoded.data(), t.encoded.size());
        }
        for (const AppSparkplugMetricDeclaration& m : e->spec.metrics) {
            const MqttSparkplugDataType type = (MqttSparkplugDataType)m.datatype;
            const char* device = e->device(m.device);
            if (device) {
                if (m.writable) node.declareWritableFor(device, m.name.c_str(), type);
                else node.declareFor(device, m.name.c_str(), type);
            } else {
                if (m.writable) node.declareWritable(m.name.c_str(), type);
                else node.declare(m.name.c_str(), type);
            }
            const int at = node.indexOf(device, m.name.c_str());
            if (at < 0) continue;
            if (!m.units.empty()) node.unitsAt((size_t)at, m.units.c_str());
            if (m.hasRange) node.rangeAt((size_t)at, m.low, m.high);
            if (m.deadband != 0 || m.deadbandPercent != 0) node.deadbandAt((size_t)at, m.deadband, m.deadbandPercent);
        }
        for (const std::string& d : e->spec.devices) node.deviceOnline(d.c_str());
        e->values.resize(node.pointCount());
        e->setAtMs.resize(node.pointCount());
        e->history = _outbox.totals(e->key()).messages;
        // Its bdSeq and values from before a restart; then what it kept and
        // has not sent, the newest of which is each metric's current value.
        loadEdgeState(*e);
        for (uint64_t after = 0;;) {
            std::vector<ConnectionOutbox::Kept> page = _outbox.next(e->key(), after, 1024);
            if (page.empty()) break;
            for (const ConnectionOutbox::Kept& k : page) {
                AppSparkplugSet set;
                if (!valueOfKept(k.message, set)) continue;
                const int at = node.indexOf(e->device(k.message.clientId), set.name.c_str());
                if (at < 0 || node.pointAt((size_t)at)->type != set.value.type) continue;
                if (set.timestamp >= e->setAtMs[(size_t)at]) e->setCurrent((size_t)at, set.value, set.text, set.timestamp);
            }
            after = page.back().id;
        }
        // What a link that died unnoticed swallowed, sent again after the next
        // birth as history (MqttSparkplugNode::replayWindow): 4096 values, 96 kB.
        node.replayWindow(4096);
        node.connect();                            // its NDEATH is this connection's Will from now on
        c.edge = std::move(e);
        return;
    }
}

void AppConnections::serviceEdgeNode(Connection& c) {
    if (!c.edge) return;
    EdgeNode& e = *c.edge;
    MqttSparkplugNode& node = *e.library;
    node.epochNow((uint64_t)utcNowMs());
    node.poll();
    saveEdgeState(e, false);
    // A host's writes reach the application on the connection, as NCMD and
    // DCMD; the node's own queue of them is let go.
    MqttSparkplugValue ignored;
    for (size_t i = 0; i < node.pointCount(); i++) {
        while (node.commandPendingAt(i) && node.takeAt(i, ignored)) {}
    }
    // Born again: what was kept goes, oldest first, one device at a time.
    if (!node.isBorn() || !c.client.isConnected()) {
        // The session ended before what was sent could be trusted to have arrived.
        e.sentHistory.clear();
        e.sentUpTo = 0;
        return;
    }
    const Clock::time_point now = Clock::now();
    const std::chrono::seconds hold(2 * std::max<uint32_t>(c.spec.keepaliveSeconds, 5));
    size_t confirmed = 0;
    while (confirmed < e.sentHistory.size() && now - e.sentHistory[confirmed].second >= hold) {
        _outbox.drop(e.sentHistory[confirmed].first);
        confirmed++;
    }
    if (confirmed) {
        e.sentHistory.erase(e.sentHistory.begin(), e.sentHistory.begin() + (long)confirmed);
        e.history -= std::min<uint64_t>(e.history, confirmed);
    }
    if (e.history <= e.sentHistory.size() || e.sentHistory.size() >= 65536) return;
    std::vector<ConnectionOutbox::Kept> kept = _outbox.next(e.key(), e.sentUpTo, 64);
    std::vector<AppSparkplugSet> sets(kept.size());
    std::vector<MqttSparkplugRecord> records;
    std::vector<uint64_t> sent;
    std::string device;
    for (size_t i = 0; i < kept.size(); i++) {
        if (!valueOfKept(kept[i].message, sets[i])) {
            _outbox.drop(kept[i].id);           // unreadable: let go
            e.history--;
            continue;
        }
        if (!records.empty() && kept[i].message.clientId != device) break;
        device = kept[i].message.clientId;
        const int at = node.indexOf(e.device(device), sets[i].name.c_str());
        if (at < 0) {
            _outbox.drop(kept[i].id);           // no longer declared
            e.history--;
            continue;
        }
        MqttSparkplugRecord r;
        r.index = (size_t)at;
        r.value = sets[i].value;
        r.timestamp = sets[i].timestamp;
        records.push_back(r);
        sent.push_back(kept[i].id);
    }
    if (records.empty() || !node.publishRecords(records.data(), records.size(), true)) return;
    for (uint64_t id : sent) e.sentHistory.push_back(std::make_pair(id, now));
    e.sentUpTo = sent.back();
}

void AppConnections::saveEdgeState(EdgeNode& e, bool now) {
    if (_directory.empty()) return;
    MqttSparkplugNode& node = *e.library;
    const int bdSeq = node.birthDeathSequence();
    const bool moved = bdSeq != e.savedBdSeq;
    if (!moved && !e.valuesChanged) return;
    // A new bdSeq at once - its CONNECT has gone, and a restart now must not
    // use it again - values at most once a second, a failed write a second on.
    const Clock::time_point at = Clock::now();
    if (!now && at - e.savedAt < std::chrono::seconds(1) && (!moved || e.saveFailed)) return;
    std::string values = "[";
    bool first = true;
    for (const AppSparkplugMetricDeclaration& m : e.spec.metrics) {
        const int index = node.indexOf(e.device(m.device), m.name.c_str());
        if (index < 0 || e.setAtMs[(size_t)index] == 0) continue;     // never set
        const MqttSparkplugValue v = node.valueAt((size_t)index);
        json::Object o;
        o.add("device", m.device).add("name", m.name).add("type", (long long)v.type).add("null", v.isNull);
        if (v.isSpan()) {
            o.add("text", v.isNull ? std::string() : hexOf(std::string((const char*)v.as.bytes.data, v.as.bytes.length)));
        } else {
            o.add("bits", std::to_string(v.as.unsignedValue));
        }
        o.add("at", (long long)e.setAtMs[(size_t)index]);
        values += (first ? "\n" : ",\n") + o.str();
        first = false;
    }
    json::Object o;
    o.add("id", e.spec.id).add("bdSeq", (long long)bdSeq).raw("values", values + "]");
    e.savedAt = at;
    e.saveFailed = !writePrivate(_directory + "/" + edgeStateName(e.spec.id), o.str() + "\n");
    if (e.saveFailed) return;
    e.savedBdSeq = bdSeq;
    e.valuesChanged = false;
}

void AppConnections::loadEdgeState(EdgeNode& e) {
    if (_directory.empty()) return;
    std::ifstream in(_directory + "/" + edgeStateName(e.spec.id));
    if (!in) return;
    std::stringstream text;
    text << in.rdbuf();
    json::Value v;
    if (!json::parse(text.str(), v) || v.str("id") != e.spec.id) return;
    MqttSparkplugNode& node = *e.library;
    if (v.has("bdSeq")) {
        const uint8_t last = (uint8_t)v.integer("bdSeq");
        node.resumeBirthDeathSequence(last);
        e.savedBdSeq = last;
    }
    for (const json::Value& m : v["values"].items) {
        const int index = node.indexOf(e.device(m.str("device")), m.str("name").c_str());
        if (index < 0) continue;
        MqttSparkplugValue value;
        value.type = (MqttSparkplugDataType)m.integer("type");
        if (node.pointAt((size_t)index)->type != value.type) continue;   // declared again, as another type
        value.isNull = m.flag("null");
        std::string bytes;
        if (value.isSpan()) {
            bytes = bytesOfHex(m.str("text"));
        } else {
            value.as.unsignedValue = std::strtoull(m.str("bits").c_str(), nullptr, 10);
        }
        e.setCurrent((size_t)index, value, bytes, (uint64_t)m.integer("at"));
    }
    e.valuesChanged = false;
}

void AppConnections::startHost(Connection& c) {
    c.host.reset();
    for (const auto& one : _hosts) {
        if (one.second.connection != c.spec.id) continue;
        std::unique_ptr<Host> h(new Host());
        h->spec = one.second;
        h->library.reset(new MqttSparkplugHost(c.client));
        h->library->hostId(h->spec.hostId.c_str());
        h->library->watch(h->spec.group.empty() ? nullptr : h->spec.group.c_str());
        h->library->reorderTimeout(h->spec.reorderTimeoutMs);
        h->library->actAsPrimary(false);          // rebirths are asked here, with limits
        h->library->onMessage(&Host::inbound, h.get());
        h->library->connect();                     // its STATE is this connection's Will from now on
        c.host = std::move(h);
        return;
    }
}

void AppConnections::serviceHost(Connection& c) {
    if (!c.host) return;
    Host& h = *c.host;
    // A gap the reorder window did not close: asked before the library's own
    // pass forgets it.
    if (h.spec.rebirthOnGap && h.spec.reorderTimeoutMs != 0) {
        const mq_time_t now = mq_millis();
        for (size_t i = 0; i < h.library->nodeCount(); i++) {
            const MqttSparkplugTrackedNode* node = h.library->nodeAt(i);
            if (node == nullptr || !node->reordering) continue;
            if (mq_elapsed(node->reorderStartedAt, now) < h.spec.reorderTimeoutMs) continue;
            mq_time_t& answered = h.reorderAnswered[Host::keyOf(node->group.c_str(), node->node.c_str())];
            if (answered == node->reorderStartedAt) continue;
            answered = node->reorderStartedAt;
            h.ask(node->group.c_str(), node->node.c_str(), false);
        }
    }
    h.library->poll();
    // Its own session begun: every node it knows is asked again - what it
    // missed while it was away.
    const bool born = h.library->isBorn() && c.client.isConnected();
    if (born && !h.wasBorn && h.spec.rebirthOnGap) {
        for (size_t i = 0; i < h.library->nodeCount(); i++) {
            const MqttSparkplugTrackedNode* node = h.library->nodeAt(i);
            if (node != nullptr) h.ask(node->group.c_str(), node->node.c_str(), false);
        }
    }
    h.wasBorn = born;
}

// A KAFKA LINK: Connections.Watch sees it go down when Kafka cannot be
// reached, and up again when it can - its client stays connected to the
// link's broker throughout, and what is published waits there for Kafka.
void AppConnections::serviceLink(Connection& c) {
    if (!c.spec.kafka || _links == nullptr) return;
    const Clock::time_point now = Clock::now();
    if (now < c.linkPolled) return;
    c.linkPolled = now + std::chrono::milliseconds(500);
    _links->status(c.spec.id, c.link);
    if (!c.client.isConnected()) return;
    if (c.link.reachable && c.state != AppConnectionState::Connected) {
        c.setState(AppConnectionState::Connected);
    } else if (!c.link.reachable && c.state == AppConnectionState::Connected) {
        c.setState(AppConnectionState::BackingOff, "Kafka cannot be reached");
    }
}

// A SPARKPLUG HOST ON A KAFKA LINK, ITS STATE ON MQTT. Kafka has no Will, so
// the host's STATE is published on the link's state connection, by the one
// owner there is - this server - and that connection's Will is the offline
// STATE: its CONNECT carries {"online":false,"timestamp":T} and the online
// STATE that follows carries the same T [tck-id-payloads-state-birth-payload].
// The host itself runs on the link's own broker as it would anywhere; its
// STATE there is not written to Kafka (KafkaLinks leaves spBv1.0/STATE/# out).
// It goes offline when its session on the link ends; born again, with a new
// T, which takes a new CONNECT of the state connection (an edge node ignores
// a STATE older than the last it saw).
void AppConnections::serviceHostState(Connection& c) {
    if (!c.spec.kafka || c.spec.link.stateConnection.empty()) return;
    auto at = _connections.find(c.spec.link.stateConnection);
    if (at == _connections.end() || at->second.get() == &c) return;
    Connection& s = *at->second;
    const bool want = c.host && c.client.isConnected() && c.host->library->isBorn();
    if (want) {
        const std::string topic = "spBv1.0/STATE/" + c.host->spec.hostId;
        if (!s.stateWill || s.stateTopic != topic) {
            if (s.stateBorn && s.client.isConnected()) {
                // Another host id: the old one's STATE dies first.
                const std::string offline = stateJson(false, (uint64_t)utcNowMs());
                s.client.publish(s.stateTopic.c_str(), (const uint8_t*)offline.data(), offline.size(), MQ_QOS_1, true);
            }
            s.stateWill = true;
            s.stateReleasing = false;
            s.stateTopic = topic;
            s.stateTimestamp = (uint64_t)utcNowMs();
            s.stateBorn = false;
            // A new CONNECT, to carry it: a clean DISCONNECT first, which sends no Will.
            if (s.client.state() != MqttState::Idle) s.client.disconnect();
            if (s.state == AppConnectionState::Waiting) s.state = AppConnectionState::Connecting;
            s.asked = true;
            return;
        }
        if (s.client.isConnected() && s.stateArmed && !s.stateBorn) {
            const std::string online = stateJson(true, s.stateTimestamp);
            if (mq_accepted(s.client.publish(s.stateTopic.c_str(), (const uint8_t*)online.data(), online.size(),
                                             MQ_QOS_1, true))) {
                s.stateBorn = true;
            }
        }
    } else {
        // Not running (its link's session ended, or it was deleted): offline
        // now - and armed afresh when it is born again.
        s.releaseState();
    }
}

void AppConnections::close(Connection& c) {
    // A host's STATE dies first, on purpose rather than by its Will.
    if (c.host || c.edge) {
        if (c.host) c.host->library->disconnect();
        if (c.edge) c.edge->library->disconnect();
        for (int pass = 0; pass < 20 && c.client.isConnected(); pass++) {
            c.client.poll();
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        if (c.edge) saveEdgeState(*c.edge, true);
        c.host.reset();
        c.edge.reset();
    }
    if (c.client.isConnected()) c.client.disconnect();
    for (int pass = 0; pass < 20 && c.client.isConnected(); pass++) {
        c.client.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    c.client.end();
    if (c.transport) c.transport->end();
    if (c.probe) c.probe->end();
    for (auto& p : c.pending) {
        if (p->done) p->done(resultOf(MqttResult::ConnectionLost, p->confirmDelivered));
    }
    c.pending.clear();
    // Kept, and still kept: whoever waits is told so.
    AppPublishResult kept;
    kept.outcome = AppPublishOutcome::Stored;
    kept.pending = 1;
    for (auto& w : c.waiting) {
        if (w.done) w.done(kept);
    }
    c.waiting.clear();
    c.sent.clear();
    c.inClient.clear();
}

void AppConnections::service(Connection& c) {
    const Clock::time_point now = Clock::now();
    c.client.poll();
    if (c.stateReleasing && now >= c.stateReleaseAt) {
        c.stateReleasing = false;
        if (c.client.state() != MqttState::Idle) c.client.disconnect();
        c.asked = true;
    }
    serviceHost(c);
    serviceHostState(c);
    serviceEdgeNode(c);
    serviceLink(c);
    // CONFIRM_DELIVERED past its deadline: kept, and sent when it can be.
    for (size_t i = 0; i < c.waiting.size(); i++) {
        if (now < c.waiting[i].deadline) continue;
        AppPublishResult kept;
        kept.outcome = AppPublishOutcome::Stored;
        kept.pending = 1;
        if (c.waiting[i].done) c.waiting[i].done(kept);
        c.waiting.erase(c.waiting.begin() + (long)i--);
    }
    // A server that cannot even be opened sends the client into its own
    // retry loop, whatever autoReconnect says. The manager decides which
    // server is next and when, so it takes the attempt back.
    // (It may go there and be taken back within one pass, so the attempt's
    // end is said, not inferred from a change of state.)
    bool takenBack = false;
    if (c.attempting && c.client.state() == MqttState::Reconnecting) {
        c.client.disconnect();
        takenBack = true;
    }
    const MqttState state = c.client.state();

    // --- Starting an attempt ---
    if (!c.attempting && state == MqttState::Idle &&
        c.state != AppConnectionState::Waiting && (c.asked || now >= c.nextAttempt)) {
        c.asked = false;
        if (c.spec.thisServer) {
            ThisServer where;
            if (!_thisServer || !_thisServer(where)) {
                c.nextAttempt = now + std::chrono::seconds(1);
                c.setState(AppConnectionState::BackingOff, "this server has no listener to reach it on");
                return;
            }
            if (where.tls != c.hereTls) {
                // TLS turned on or off since it was opened: a transport of the other kind.
                close(c);
                open(c);
                return;
            }
            c.here = AppEndpoint{where.host, where.port};
        }
        MqttConnectionConfig connection = c.client.connection();
        if (c.spec.kafka) {
            AppLocalAddress where;
            std::string why = "this server was built without Kafka";
            if (!_links || !_links->open(c.spec.id, c.spec.link, where, why)) {
                c.nextAttempt = now + std::chrono::seconds(1);
                c.setState(AppConnectionState::BackingOff, why);
                return;
            }
            c.here = AppEndpoint{where.host, where.port};
            loginOf(where, connection);
        }
        const AppEndpoint& e = c.endpoint(c.serverIndex);
        connection.host = e.host.c_str();
        connection.port = c.portOf(e);
        willOf(c, connection);
        c.client.reconfigure(connection);
#if MQTT_TLS_SOCKETS
        if (c.usesTls()) static_cast<MqttTlsTransport*>(c.transport.get())->configure(e.host.c_str(), c.portOf(e));
        else
#endif
            static_cast<MqttTcpTransport*>(c.transport.get())->configure(e.host.c_str(), c.portOf(e));
        c.attempting = true;
        c.reachedThisAttempt = false;
        c.setState(AppConnectionState::Connecting);
        c.client.connect();
        // As the attempt began - so it ending at Idle again, however soon, is
        // a change the next pass sees. This pass's `state` is from before it.
        c.seen = c.client.state();
        return;
    }

    // --- How it went ---
    if (state != c.seen || takenBack) {
        const MqttState was = c.seen;
        c.seen = state;
        if (state == MqttState::Connected) {
            c.reachedThisAttempt = true;
            c.stateBorn = false;
            c.upSince = now;
            c.filtersChanged = true;
            c.subscribed.clear();
            c.nextProbe = now + std::chrono::seconds((uint64_t)c.spec.primaryCheckSeconds
                                                     << std::min<uint32_t>(c.primaryFailures, 6));
            c.setState(AppConnectionState::Connected);
        } else if (state == MqttState::Idle && (was != MqttState::Idle || takenBack) && c.attempting) {
            c.attempting = false;
            const uint8_t refused = c.reachedThisAttempt ? 0 : c.client.status().connectReasonCode;
            const bool stable = c.reachedThisAttempt && now - c.upSince >= std::chrono::milliseconds(kStableMs);
            // Back on the first server, and it did not last: it is checked less often.
            if (c.switchedBack && c.serverIndex == 0 && !stable) c.primaryFailures++;
            if (stable) {
                c.backoffMs = 1000;                // the network failed; the next try is soon
                if (c.serverIndex == 0) c.primaryFailures = 0;
            }
            c.switchedBack = false;
            if (loginRefusal(refused, c.spec.protocol == 4)) {
                // Never for good: an account fixed at the broker comes back by itself.
                char why[96];
                std::snprintf(why, sizeof why, "the server refused the login (0x%02X); tried again in 5 minutes", refused);
                c.nextAttempt = now + std::chrono::milliseconds(kRefusedRetryMs);
                c.setState(AppConnectionState::Refused, why);
            } else if (c.reachedThisAttempt && c.spec.reconnectWhenAsked) {
                c.setState(AppConnectionState::Waiting, "the connection was lost");
            } else {
                // Failed to connect: the next server. Lost after connecting:
                // the same one first.
                if (!c.reachedThisAttempt && !c.spec.servers.empty()) {
                    c.serverIndex = (c.serverIndex + 1) % (uint32_t)c.spec.servers.size();
                }
                std::chrono::milliseconds wait = jittered(c.backoffMs);
                // Quota exceeded, connection rate exceeded: asking again soon is the problem.
                if (refused == MQ_REASON_QUOTA_EXCEEDED || refused == 0x9F) {
                    wait = std::max(wait, std::chrono::milliseconds(kBusyFloorMs) + jittered(kBusyFloorMs / 2));
                }
                c.backoffMs = std::min<uint32_t>(c.backoffMs * 2, kBackoffMaxMs);
                c.nextAttempt = now + wait;
                c.setState(AppConnectionState::BackingOff,
                           c.reachedThisAttempt ? std::string("the connection was lost") : whyNotConnected(c));
            }
        }
    }

    if (state != MqttState::Connected) return;

    forward(c);

    // --- Subscriptions: what the groups on it want ---
    if (c.filtersChanged) {
        c.filtersChanged = false;
        const std::map<std::string, uint8_t> want = _hub.filtersFor(c.spec.id);
        // A filter asked for again with other options is subscribed again: the
        // remote broker replaces the subscription.
        for (const auto& filter : want) {
            auto had = c.subscribed.find(filter.first);
            if (had == c.subscribed.end() || had->second != filter.second) {
                c.client.subscribeWithOptions(filter.first.c_str(), filter.second);
            }
        }
        for (const auto& filter : c.subscribed) {
            if (!want.count(filter.first)) c.client.unsubscribe(filter.first.c_str());
        }
        c.subscribed = want;
    }

    // --- Back to the first server, once it answers ---
    if (c.serverIndex != 0 && c.spec.primaryCheckSeconds != 0) {
        if (!c.probe && now >= c.nextProbe) {
            const AppEndpoint& first = c.endpoint(0);
            c.probe.reset(new MqttTcpTransport());
            c.probe->configure(first.host.c_str(), c.portOf(first));
            c.probe->begin();
        }
        if (c.probe) {
            if (c.probe->isConnected()) {
                c.probe->end();
                c.probe.reset();
                c.serverIndex = 0;
                c.switchedBack = true;
                c.client.disconnect();
                c.asked = true;
            } else if (c.probe->connectFailed()) {
                c.probe->end();
                c.probe.reset();
                c.nextProbe = now + std::chrono::seconds((uint64_t)c.spec.primaryCheckSeconds
                                                         << std::min<uint32_t>(c.primaryFailures, 6));
            }
        }
    }
}

// --- The API's side ----------------------------------------------------------

std::string AppConnections::stateUser(const std::string& id) const {
    for (const auto& one : _connections) {
        const AppConnectionSpec& spec = one.second->spec;
        if (spec.kafka && spec.link.stateConnection == id) return one.first;
    }
    return std::string();
}

AppConnectionInfo AppConnections::infoOf(const Connection& c) const {
    AppConnectionInfo info;
    info.spec = c.spec;
    info.spec.password.clear();
    info.spec.keyPem.clear();
    info.state = c.state;
    info.serverIndex = c.serverIndex;
    info.sessionPresent = c.sessionPresent;
    info.lastError = c.lastError;
    info.sinceMs = c.sinceMs;
    info.messagesIn = c.messagesIn;
    info.messagesOut = c.messagesOut;
    info.stored = c.stored;
    info.storedBytes = c.storedBytes;
    info.storedDropped = c.storedDropped;
    info.storedExpired = c.storedExpired;
    info.link = c.link;
    if (c.stored != 0) info.oldestStoredMs = const_cast<ConnectionOutbox&>(_outbox).totals(c.spec.id).oldestMs;
    return info;
}

void AppConnections::announce(Connection& c) {
    const AppConnectionInfo info = infoOf(c);
    std::vector<std::function<void(const AppConnectionInfo&)>> tell;
    {
        std::lock_guard<std::mutex> guard(_lock);
        for (auto& w : _watchers) {
            if (w.second.ids.empty() ||
                std::find(w.second.ids.begin(), w.second.ids.end(), c.spec.id) != w.second.ids.end()) {
                tell.push_back(w.second.changed);
            }
        }
    }
    for (auto& one : tell) one(info);
}

bool AppConnections::put(const AppConnectionSpec& spec, AppConnectionInfo* info, std::string& error) {
    if (spec.id.empty()) {
        error = "a connection needs an id";
        return false;
    }
    if (!idIsName(spec.id)) {
        error = "a connection's id is letters, digits and . _ - : @ (at most 128, not starting with '.'), not \"" +
                spec.id + "\"";
        return false;
    }
    if (spec.servers.empty() && !spec.thisServer && !spec.kafka) {
        error = "a connection needs at least one server (or this_server, or kafka)";
        return false;
    }
    if ((int)!spec.servers.empty() + (int)spec.thisServer + (int)spec.kafka > 1) {
        error = "a connection is to servers, to this_server, or over kafka - one of them";
        return false;
    }
    if (spec.kafka) {
        if (_links == nullptr) {
            error = "this server was built without Kafka (librdkafka), so a connection cannot have kafka";
            return false;
        }
        if (spec.tls) {
            error = "a kafka connection's TLS is its Kafka cluster's (Kafka.PutCluster), not the connection's";
            return false;
        }
        if (spec.link.stateConnection == spec.id) {
            error = "state_connection names this connection: it has to be an MQTT one";
            return false;
        }
    }
    bool ok = true;
    call([&] {
        if (spec.kafka && !_links->check(spec.id, spec.link, error)) {
            ok = false;
            return;
        }
        // A STATE CONNECTION (another connection's link.stateConnection) is
        // MQTT, and its Will is the host's: none of its own, and it reconnects
        // by itself.
        const auto unfitForState = [&](const AppConnectionSpec& state, const std::string& user) {
            std::string why;
            if (state.kafka) why = "it is over Kafka itself";
            else if (state.hasWill) why = "it has a Will of its own (a STATE connection's Will is the host's)";
            else if (state.reconnectWhenAsked) why = "it reconnects only when asked";
            for (const auto& h : _hosts) {
                if (h.second.connection == state.id) why = "a host runs on it: \"" + h.first + "\"";
            }
            for (const auto& e : _edgeNodes) {
                if (e.second.connection == state.id) why = "an edge node runs on it: \"" + e.first + "\"";
            }
            if (!why.empty()) {
                error = "\"" + state.id + "\" cannot carry the STATE of \"" + user + "\": " + why;
                return true;
            }
            return false;
        };
        if (spec.kafka && !spec.link.stateConnection.empty()) {
            auto state = _connections.find(spec.link.stateConnection);
            if (state != _connections.end() && unfitForState(state->second->spec, spec.id)) {
                ok = false;
                return;
            }
        }
        for (const auto& one : _connections) {
            const AppConnectionSpec& other = one.second->spec;
            if (other.kafka && other.link.stateConnection == spec.id && one.first != spec.id &&
                unfitForState(spec, other.id)) {
                ok = false;
                return;
            }
            // One Will each: two hosts' STATE on one connection would take it in turns, for ever.
            if (spec.kafka && !spec.link.stateConnection.empty() && other.kafka && one.first != spec.id &&
                other.link.stateConnection == spec.link.stateConnection) {
                error = "\"" + spec.link.stateConnection + "\" carries the STATE of \"" + one.first +
                        "\" already: one Will each";
                ok = false;
                return;
            }
        }
        std::unique_ptr<Connection>& slot = _connections[spec.id];
        if (!slot || !sameSpec(slot->spec, spec)) {
            if (slot && slot->spec.kafka) {
                // Its host's STATE was on another connection: let go there.
                if (!slot->spec.link.stateConnection.empty() &&
                    slot->spec.link.stateConnection != spec.link.stateConnection) {
                    auto state = _connections.find(slot->spec.link.stateConnection);
                    if (state != _connections.end()) state->second->releaseState();
                }
                if (!spec.kafka && _links) _links->close(spec.id);
            }
            if (slot) close(*slot);
            slot.reset(new Connection());
            slot->spec = spec;
            open(*slot);
            save();
        }
        if (info) *info = infoOf(*slot);
    });
    return ok;
}

bool AppConnections::get(const std::string& id, AppConnectionInfo& info) const {
    bool found = false;
    call([&] {
        auto at = _connections.find(id);
        if (at == _connections.end()) return;
        info = infoOf(*at->second);
        found = true;
    });
    return found;
}

std::vector<AppConnectionInfo> AppConnections::list() const {
    std::vector<AppConnectionInfo> out;
    call([&] {
        for (const auto& one : _connections) out.push_back(infoOf(*one.second));
    });
    return out;
}

bool AppConnections::remove(const std::string& id) {
    bool found = false;
    call([&] {
        auto at = _connections.find(id);
        if (at == _connections.end()) return;
        const AppConnectionSpec& spec = at->second->spec;
        if (spec.kafka && !spec.link.stateConnection.empty()) {
            auto state = _connections.find(spec.link.stateConnection);
            if (state != _connections.end()) state->second->releaseState();
        }
        const bool kafka = spec.kafka;
        if (!_directory.empty()) {
            // Its TLS material, written at each open.
            std::remove((_directory + "/" + id + ".crt").c_str());
            std::remove((_directory + "/" + id + ".key").c_str());
        }
        close(*at->second);
        if (kafka && _links) _links->close(id);
        _connections.erase(at);
        _outbox.dropAll(id);              // nobody left to send it to
        found = true;
        save();
    });
    return found;
}

bool AppConnections::reconnect(const std::string& id, const AppPublish* will, AppConnectionInfo* info,
                               std::string& error) {
    bool found = false;
    call([&] {
        auto at = _connections.find(id);
        if (at != _connections.end()) {
            Connection& c = *at->second;
            if (will != nullptr) {
                c.spec.hasWill = true;
                c.spec.will = *will;
                save();
            }
            if (c.client.state() != MqttState::Idle) {
                c.client.disconnect();
            }
            c.state = AppConnectionState::Connecting;
            c.asked = true;
            if (info) *info = infoOf(c);
            found = true;
        }
    });
    if (!found) error = "no connection \"" + id + "\"";
    return found;
}

uint64_t AppConnections::watch(const std::vector<std::string>& ids,
                               std::function<void(const AppConnectionInfo&)> changed) {
    uint64_t id;
    {
        std::lock_guard<std::mutex> guard(_lock);
        id = _nextWatcher++;
        _watchers[id] = Watcher{ids, changed};
    }
    // Each one's state now, from the manager's thread - unless it was let go first.
    post([this, id, ids, changed] {
        {
            std::lock_guard<std::mutex> guard(_lock);
            if (!_watchers.count(id)) return;
        }
        for (auto& one : _connections) {
            if (ids.empty() || std::find(ids.begin(), ids.end(), one.first) != ids.end()) {
                changed(infoOf(*one.second));
            }
        }
    });
    return id;
}

void AppConnections::unwatch(uint64_t watcher) {
    {
        std::lock_guard<std::mutex> guard(_lock);
        _watchers.erase(watcher);
    }
    // announce() calls its copies of the callbacks outside the lock, on the
    // manager's thread, and watch()'s first call is a command queued there:
    // one may be under way now. Once a command queued after them has run,
    // neither can be - and the caller may free what its callback points at.
    if (std::this_thread::get_id() != _thread.get_id()) call([] {});
}

void AppConnections::publish(const AppPublish& message, std::function<void(const AppPublishResult&)> done) {
    post([this, message, done] {
        auto at = _connections.find(message.connection);
        if (at != _connections.end() && at->second->spec.storeAndForward) {
            store(*at->second, message, done);
            return;
        }
        // A Kafka link whose Kafka cannot be reached is down, as any other
        // connection would be (store and forward keeps them, above).
        if (at == _connections.end() || !at->second->client.isConnected() ||
            (at->second->spec.kafka && !at->second->link.reachable)) {
            AppPublishResult result;
            result.outcome = AppPublishOutcome::NotConnected;
            done(result);
            return;
        }
        Connection& c = *at->second;
        std::unique_ptr<Connection::Pending> pending(new Connection::Pending());
        pending->connection = &c;
        pending->confirmDelivered = message.confirmDelivered;
        pending->done = done;
        Connection::Pending* p = pending.get();
        c.pending.push_back(std::move(pending));
        const MqttResult queued = c.client.publishSeries(
            message.topic.c_str(), (const uint8_t*)message.payload.data(), message.payload.size(), message.qos,
            message.retain, message.properties, &Connection::completed, p);
        c.messagesOut++;
        if (queued != MqttResult::Queued && queued != MqttResult::Ok) {
            Connection::completed(p, queued, 0);
            return;
        }
        // CONFIRM_ACCEPTED: taken by the client is taken; the rest is the
        // remote broker's acknowledgement, which only DELIVERED waits for.
        if (!message.confirmDelivered) {
            AppPublishResult result;
            result.outcome = AppPublishOutcome::Accepted;
            p->done(result);
            p->done = nullptr;
        }
    });
}

// --- Store and forward ---------------------------------------------------------

void AppConnections::store(Connection& c, const AppPublish& message,
                           std::function<void(const AppPublishResult&)> done) {
    const uint64_t bytes = message.topic.size() + message.payload.size();
    AppPublishResult result;
    if (bytes > c.spec.storeMaxBytes) {
        result.outcome = AppPublishOutcome::Refused;
        result.reasonCode = MQ_REASON_PACKET_TOO_LARGE;
        return done(result);
    }
    // Full: the oldest makes room, or the new one is refused - as it was told.
    while (c.stored + 1 > c.spec.storeMaxMessages || c.storedBytes + bytes > c.spec.storeMaxBytes) {
        ConnectionOutbox::Kept oldest;
        if (!c.spec.storeDropOldest || !_outbox.dropOldest(c.spec.id, &oldest)) {
            result.outcome = AppPublishOutcome::Refused;
            result.reasonCode = MQ_REASON_QUOTA_EXCEEDED;
            return done(result);
        }
        c.stored = c.stored ? c.stored - 1 : 0;
        c.storedBytes -= std::min<uint64_t>(c.storedBytes, oldest.bytes);
        c.storedDropped++;
        c.inClient.erase(oldest.id);
        AppPublishResult dropped;
        dropped.outcome = AppPublishOutcome::Refused;
        dropped.reasonCode = MQ_REASON_QUOTA_EXCEEDED;
        c.settle(oldest.id, dropped);
    }
    AppMessage m;
    m.topic = message.topic;
    m.payload = message.payload;
    m.qos = message.qos;
    m.retain = message.retain;
    m.properties = message.properties;
    m.origin = message.origin;
    m.brokerTimeMs = utcNowMs();
    m.connection = c.spec.id;
    uint64_t id = 0;
    if (!_outbox.push(c.spec.id, m, id)) {
        result.outcome = AppPublishOutcome::Refused;
        result.reasonCode = MQ_REASON_IMPLEMENTATION_SPECIFIC_ERROR;
        return done(result);
    }
    c.stored++;
    c.storedBytes += bytes;
    if (message.confirmDelivered) {
        c.waiting.push_back(Connection::Waiting{
            id, Clock::now() + std::chrono::milliseconds(message.deadlineMs ? message.deadlineMs : 10000), done});
    } else {
        // Up: on its way now. Down: kept until it is back.
        result.outcome = c.client.isConnected() ? AppPublishOutcome::Accepted : AppPublishOutcome::Stored;
        done(result);
    }
    if (c.client.isConnected()) forward(c);
}

void AppConnections::forward(Connection& c) {
    // A window of kept messages with the client at once, oldest first; the
    // rest wait their turn, in order.
    const size_t kWindow = 64;
    if (c.stored <= c.inClient.size() || c.inClient.size() >= kWindow) return;
    if (Clock::now() < c.forwardAfter) return;          // the other broker's quota, waited out
    const int64_t now = utcNowMs();
    for (ConnectionOutbox::Kept& k : _outbox.next(c.spec.id, 0, kWindow + c.inClient.size())) {
        if (c.inClient.size() >= kWindow) break;
        if (c.inClient.count(k.id)) continue;
        MqttOwnedProperties properties = k.message.properties;
        // [MQTT-3.3.2-6] what is left of its Message Expiry Interval; gone, it is not sent.
        if (properties.hasExpiry) {
            const int64_t waited = (now - k.message.brokerTimeMs) / 1000;
            if (waited >= (int64_t)properties.expiryInterval) {
                _outbox.drop(k.id);
                c.stored = c.stored ? c.stored - 1 : 0;
                c.storedBytes -= std::min<uint64_t>(c.storedBytes, k.bytes);
                c.storedExpired++;
                AppPublishResult expired;
                expired.outcome = AppPublishOutcome::Refused;
                expired.reasonCode = MQ_REASON_UNSPECIFIED_ERROR;
                c.settle(k.id, expired);
                continue;
            }
            properties.setMessageExpiry((uint32_t)(properties.expiryInterval - waited));
        }
        std::unique_ptr<Connection::Sent> sent(new Connection::Sent{&c, k.id, k.bytes});
        // At least QoS 1: it leaves the store only on the other broker's word.
        const uint8_t qos = k.message.qos > MQ_QOS_1 ? k.message.qos : MQ_QOS_1;
        const MqttResult queued = c.client.publishSeries(
            k.message.topic.c_str(), (const uint8_t*)k.message.payload.data(), k.message.payload.size(), qos,
            k.message.retain, properties, &Connection::forwarded, sent.get());
        // No room now - the queue, the send quota, packet identifiers: next pass.
        if (queued == MqttResult::QueueFull || queued == MqttResult::NotConnected ||
            queued == MqttResult::QuotaExceeded || queued == MqttResult::NoPacketIdentifier) {
            break;
        }
        if (queued != MqttResult::Queued && queued != MqttResult::Ok) {
            // Refused for good by this connection (retain or a QoS the other
            // broker does not offer, a property 3.1.1 cannot carry): it can
            // never go, and waiting for it stopped everything behind it.
            _outbox.drop(k.id);
            c.stored = c.stored ? c.stored - 1 : 0;
            c.storedBytes -= std::min<uint64_t>(c.storedBytes, k.bytes);
            c.settle(k.id, resultOf(queued, true));
            continue;
        }
        c.inClient.insert(k.id);
        c.sent.push_back(std::move(sent));
        c.messagesOut++;
    }
}

// --- Sparkplug hosts ------------------------------------------------------------

namespace {

// Whether a host's connection is up: on a Kafka link with a state connection,
// that one too, with the host's online STATE published on it.
bool hostConnected(const std::map<std::string, std::unique_ptr<AppConnections::Connection>>& connections,
                   const std::string& id) {
    auto at = connections.find(id);
    if (at == connections.end() || !at->second->client.isConnected()) return false;
    const AppConnectionSpec& spec = at->second->spec;
    if (!spec.kafka || spec.link.stateConnection.empty()) return true;
    auto state = connections.find(spec.link.stateConnection);
    return state != connections.end() && state->second->client.isConnected() && state->second->stateBorn;
}

AppSparkplugHostInfo hostInfoOf(const AppSparkplugHostSpec& spec, const AppConnections::Host* host,
                                bool connected) {
    AppSparkplugHostInfo info;
    info.spec = spec;
    if (host == nullptr) return info;
    info.online = connected && host->library->isBorn();
    info.nodes = host->library->nodeCount();
    info.decodeFailures = host->library->decodeFailures();
    info.rebirthsRequested = host->rebirths;
    return info;
}

} // namespace

bool AppConnections::putHost(const AppSparkplugHostSpec& spec, AppSparkplugHostInfo* info, std::string& error) {
    if (spec.id.empty() || spec.connection.empty()) {
        error = "a host needs an id and the connection it runs on";
        return false;
    }
    if (!mq_sparkplugValidId(spec.hostId.c_str())) {
        error = "host_id \"" + spec.hostId + "\" is not a Sparkplug id (no '/', '+' or '#')";
        return false;
    }
    if (!spec.group.empty() && !mq_sparkplugValidId(spec.group.c_str())) {
        error = "group \"" + spec.group + "\" is not a Sparkplug id";
        return false;
    }
    bool ok = true;
    call([&] {
        for (const auto& one : _hosts) {
            if (one.first != spec.id && one.second.connection == spec.connection) {
                error = "the connection \"" + spec.connection + "\" has a host already: \"" + one.first + "\"";
                ok = false;
                return;
            }
        }
        for (const auto& one : _edgeNodes) {
            if (one.second.connection == spec.connection) {
                error = "the connection \"" + spec.connection + "\" has an edge node on it: one Will each";
                ok = false;
                return;
            }
        }
        if (!stateUser(spec.connection).empty()) {
            error = "the connection \"" + spec.connection + "\" carries the STATE of \"" + stateUser(spec.connection) +
                    "\": one Will each";
            ok = false;
            return;
        }
        auto had = _hosts.find(spec.id);
        const std::string before = had == _hosts.end() ? std::string() : had->second.connection;
        _hosts[spec.id] = spec;
        saveHosts();
        // Started again where it runs: its STATE born anew, its sessions followed afresh.
        for (const std::string& id : {before, spec.connection}) {
            auto at = _connections.find(id);
            if (at == _connections.end()) continue;
            Connection& c = *at->second;
            if (c.host) {
                c.host->library->disconnect();
                c.host.reset();
            }
            startHost(c);
        }
        auto at = _connections.find(spec.connection);
        if (info) {
            const bool up = hostConnected(_connections, spec.connection);
            *info = hostInfoOf(spec, at != _connections.end() ? at->second->host.get() : nullptr, up);
        }
    });
    return ok;
}

bool AppConnections::getHost(const std::string& id, AppSparkplugHostInfo& info) const {
    bool found = false;
    call([&] {
        auto spec = _hosts.find(id);
        if (spec == _hosts.end()) return;
        auto at = _connections.find(spec->second.connection);
        const bool up = hostConnected(_connections, spec->second.connection);
        info = hostInfoOf(spec->second, at != _connections.end() ? at->second->host.get() : nullptr, up);
        found = true;
    });
    return found;
}

std::vector<AppSparkplugHostInfo> AppConnections::hosts() const {
    std::vector<AppSparkplugHostInfo> out;
    call([&] {
        for (const auto& spec : _hosts) {
            auto at = _connections.find(spec.second.connection);
            const bool up = hostConnected(_connections, spec.second.connection);
            out.push_back(hostInfoOf(spec.second, at != _connections.end() ? at->second->host.get() : nullptr, up));
        }
    });
    return out;
}

bool AppConnections::removeHost(const std::string& id) {
    bool found = false;
    call([&] {
        auto spec = _hosts.find(id);
        if (spec == _hosts.end()) return;
        auto at = _connections.find(spec->second.connection);
        if (at != _connections.end() && at->second->host) {
            Connection& c = *at->second;
            c.host->library->disconnect();
            for (int pass = 0; pass < 20 && c.client.isConnected(); pass++) {
                c.client.poll();
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            c.host.reset();
        }
        _hosts.erase(spec);
        saveHosts();
        found = true;
    });
    return found;
}

bool AppConnections::nodes(const std::string& host, const std::string& group, const std::string& node,
                           std::vector<AppSparkplugNodeInfo>& out, std::string& error) const {
    bool found = false;
    call([&] {
        auto spec = _hosts.find(host);
        if (spec == _hosts.end()) return;
        found = true;
        auto at = _connections.find(spec->second.connection);
        if (at == _connections.end() || !at->second->host) return;
        const MqttSparkplugHost& library = *at->second->host->library;
        for (size_t i = 0; i < library.nodeCount(); i++) {
            const MqttSparkplugTrackedNode* n = library.nodeAt(i);
            if (n == nullptr || (!group.empty() && n->group != group) || (!node.empty() && n->node != node)) continue;
            AppSparkplugNodeInfo info;
            info.group = n->group;
            info.node = n->node;
            info.online = n->online;
            info.hasBdSeq = n->hasBdSeq;
            info.bdSeq = n->bdSeq;
            info.hasSeq = n->hasSeq;
            info.lastSeq = n->lastSeq;
            info.messages = n->messages;
            info.gaps = n->gaps;
            info.rebirthsRequested = n->rebirthsRequested;
            // The library keeps an alias, not whether there was one: a birth that gave any gave them all
            // ([tck-id-payloads-alias-birth-requirement]), 0 among them.
            bool aliases = false;
            for (size_t m = 0; m < n->metricCount; m++) aliases = aliases || n->metrics[m].alias != 0;
            for (size_t m = 0; m < n->metricCount; m++) {
                const MqttSparkplugTrackedMetric& tracked = n->metrics[m];
                AppSparkplugMetricInfo metric;
                metric.name = tracked.name;
                metric.device = tracked.device;
                metric.alias = tracked.alias;
                metric.hasAlias = aliases;
                metric.datatype = (uint32_t)tracked.type;
                metric.at = tracked.at;
                const MqttSparkplugValue& v = tracked.value;
                if (tracked.hasValue && !v.isNull) {
                    switch (v.type) {
                    case MqttSparkplugDataType::Int8: case MqttSparkplugDataType::Int16:
                    case MqttSparkplugDataType::Int32: case MqttSparkplugDataType::Int64:
                        metric.kind = AppSparkplugMetricInfo::Signed;
                        metric.i = v.as.signedValue;
                        break;
                    case MqttSparkplugDataType::UInt8: case MqttSparkplugDataType::UInt16:
                    case MqttSparkplugDataType::UInt32: case MqttSparkplugDataType::UInt64:
                    case MqttSparkplugDataType::DateTime:
                        metric.kind = AppSparkplugMetricInfo::Unsigned;
                        metric.u = v.as.unsignedValue;
                        break;
                    case MqttSparkplugDataType::Float: case MqttSparkplugDataType::Double:
                        metric.kind = AppSparkplugMetricInfo::Real;
                        metric.d = v.asNumber();
                        break;
                    case MqttSparkplugDataType::Boolean:
                        metric.kind = AppSparkplugMetricInfo::Boolean;
                        metric.b = v.as.boolean;
                        break;
                    default:
                        if (!v.isSpan()) break;
                        metric.kind = AppSparkplugMetricInfo::Span;
                        if (v.as.bytes.length > 0) {
                            metric.bytes.assign((const char*)v.as.bytes.data, v.as.bytes.length);
                        }
                        break;
                    }
                }
                if (!tracked.device.empty() &&
                    std::find(info.devices.begin(), info.devices.end(), tracked.device) == info.devices.end()) {
                    info.devices.push_back(tracked.device);
                }
                info.metrics.push_back(metric);
            }
            out.push_back(std::move(info));
        }
    });
    if (!found) error = "no host \"" + host + "\"";
    return found;
}

bool AppConnections::rebirth(const std::string& host, const std::string& group, const std::string& node,
                             const std::string& device, std::string& error) {
    bool sent = false;
    call([&] {
        auto spec = _hosts.find(host);
        if (spec == _hosts.end()) {
            error = "no host \"" + host + "\"";
            return;
        }
        auto at = _connections.find(spec->second.connection);
        if (at == _connections.end() || !at->second->host || !at->second->client.isConnected()) {
            error = "the host's connection is not up";
            return;
        }
        Host& h = *at->second->host;
        if (device.empty()) {
            sent = h.ask(group.c_str(), node.c_str(), true);
        } else {
            MqttSparkplugValue yes;
            yes.type = MqttSparkplugDataType::Boolean;
            yes.as.boolean = true;
            sent = h.library->commandDevice(group.c_str(), node.c_str(), device.c_str(), "Device Control/Rebirth", yes);
        }
        if (!sent) error = "it could not be sent (a group or node that is not a Sparkplug id?)";
    });
    return sent;
}

bool AppConnections::hostConnection(const std::string& host, std::string& connection) const {
    bool found = false;
    call([&] {
        auto spec = _hosts.find(host);
        if (spec == _hosts.end()) return;
        connection = spec->second.connection;
        found = true;
    });
    return found;
}

// --- Sparkplug edge nodes -------------------------------------------------------

namespace {

AppSparkplugEdgeNodeInfo edgeInfoOf(const AppSparkplugEdgeNodeSpec& spec, const AppConnections::EdgeNode* e,
                                    uint64_t history) {
    AppSparkplugEdgeNodeInfo info;
    info.spec = spec;
    info.history = history;
    if (e == nullptr) return info;
    const MqttSparkplugNode& node = *e->library;
    info.born = node.isBorn();
    info.bdSeq = node.birthDeathSequence();
    info.primaryHostOnline = node.primaryHostOnline();
    info.births = node.birthCount();
    info.historyLost = e->historyLost;
    for (const std::string& d : spec.devices) {
        if (node.deviceIsOnline(d.c_str())) info.devicesOnline.push_back(d);
    }
    return info;
}

} // namespace

bool AppConnections::putEdgeNode(const AppSparkplugEdgeNodeSpec& spec, AppSparkplugEdgeNodeInfo* info,
                                 std::string& error) {
    if (spec.id.empty() || spec.connection.empty()) {
        error = "an edge node needs an id and the connection it runs on";
        return false;
    }
    if (!mq_sparkplugValidId(spec.group.c_str()) || !mq_sparkplugValidId(spec.node.c_str())) {
        error = "group and node are Sparkplug ids (no '/', '+' or '#')";
        return false;
    }
    std::set<std::string> templates;
    for (const AppSparkplugTemplateDefinition& t : spec.templates) {
        bool isDefinition = false;
        const uint8_t* ref = nullptr;
        size_t refLength = 0;
        if (t.name.empty() || !templates.insert(t.name).second) {
            error = "template \"" + t.name + "\": each definition needs a name of its own";
            return false;
        }
        if (!mq_sparkplugTemplateHeader((const uint8_t*)t.encoded.data(), t.encoded.size(), isDefinition, ref,
                                        refLength) ||
            !isDefinition || ref != nullptr) {
            error = "template \"" + t.name + "\": a definition has no template_ref";
            return false;
        }
    }
    for (const AppSparkplugMetricDeclaration& m : spec.metrics) {
        if (!mq_sparkplugDataTypeSupported((MqttSparkplugDataType)m.datatype)) {
            error = "metric \"" + m.name + "\": a metric cannot be " +
                    std::string(mq_sparkplugDataTypeName((MqttSparkplugDataType)m.datatype));
            return false;
        }
        if (m.device.empty() && templates.count(m.name)) {
            error = "metric \"" + m.name + "\": a template definition has that name (both are NBIRTH metrics)";
            return false;
        }
        if (!m.device.empty() && std::find(spec.devices.begin(), spec.devices.end(), m.device) == spec.devices.end()) {
            error = "metric \"" + m.name + "\": its device \"" + m.device + "\" is not in devices";
            return false;
        }
    }
    bool ok = true;
    call([&] {
        for (const auto& one : _hosts) {
            if (one.second.connection == spec.connection) {
                error = "the connection \"" + spec.connection + "\" has a host on it: one Will each";
                ok = false;
                return;
            }
        }
        if (!stateUser(spec.connection).empty()) {
            error = "the connection \"" + spec.connection + "\" carries the STATE of \"" + stateUser(spec.connection) +
                    "\": one Will each";
            ok = false;
            return;
        }
        for (const auto& one : _edgeNodes) {
            if (one.first != spec.id && one.second.connection == spec.connection) {
                error = "the connection \"" + spec.connection + "\" has an edge node already: \"" + one.first + "\"";
                ok = false;
                return;
            }
        }
        auto had = _edgeNodes.find(spec.id);
        const std::string before = had == _edgeNodes.end() ? std::string() : had->second.connection;
        _edgeNodes[spec.id] = spec;
        saveHosts();
        // Born anew where it runs: a new session, a new bdSeq, its births from this declaration.
        for (const std::string& id : {before, spec.connection}) {
            auto at = _connections.find(id);
            if (at == _connections.end()) continue;
            Connection& c = *at->second;
            if (c.edge) {
                c.edge->library->disconnect();
                saveEdgeState(*c.edge, true);     // the new one moves on from its bdSeq
                c.edge.reset();
            }
            startEdgeNode(c);
        }
        auto at = _connections.find(spec.connection);
        if (info) {
            *info = edgeInfoOf(spec, at != _connections.end() ? at->second->edge.get() : nullptr,
                               _outbox.totals("sparkplug-node:" + spec.id).messages);
        }
    });
    return ok;
}

bool AppConnections::getEdgeNode(const std::string& id, AppSparkplugEdgeNodeInfo& info) const {
    bool found = false;
    call([&] {
        auto spec = _edgeNodes.find(id);
        if (spec == _edgeNodes.end()) return;
        auto at = _connections.find(spec->second.connection);
        info = edgeInfoOf(spec->second, at != _connections.end() ? at->second->edge.get() : nullptr,
                          const_cast<ConnectionOutbox&>(_outbox).totals("sparkplug-node:" + id).messages);
        found = true;
    });
    return found;
}

std::vector<AppSparkplugEdgeNodeInfo> AppConnections::edgeNodes() const {
    std::vector<AppSparkplugEdgeNodeInfo> out;
    call([&] {
        for (const auto& spec : _edgeNodes) {
            auto at = _connections.find(spec.second.connection);
            out.push_back(edgeInfoOf(spec.second, at != _connections.end() ? at->second->edge.get() : nullptr,
                                     const_cast<ConnectionOutbox&>(_outbox).totals("sparkplug-node:" + spec.first).messages));
        }
    });
    return out;
}

bool AppConnections::removeEdgeNode(const std::string& id) {
    bool found = false;
    call([&] {
        auto spec = _edgeNodes.find(id);
        if (spec == _edgeNodes.end()) return;
        auto at = _connections.find(spec->second.connection);
        if (at != _connections.end() && at->second->edge) {
            Connection& c = *at->second;
            c.edge->library->disconnect();
            for (int pass = 0; pass < 20 && c.client.isConnected(); pass++) {
                c.client.poll();
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            c.edge.reset();
        }
        _outbox.dropAll("sparkplug-node:" + id);
        if (!_directory.empty()) std::remove((_directory + "/" + edgeStateName(id)).c_str());
        _edgeNodes.erase(spec);
        saveHosts();
        found = true;
    });
    return found;
}

bool AppConnections::setValues(const std::string& id, const std::string& device, std::vector<AppSparkplugSet>& values,
                               uint32_t& sent, uint32_t& kept, std::vector<std::string>& unknown,
                               std::vector<std::string>& refused, std::string& error) {
    sent = kept = 0;
    bool found = false;
    call([&] {
        auto spec = _edgeNodes.find(id);
        if (spec == _edgeNodes.end()) {
            error = "no edge node \"" + id + "\"";
            return;
        }
        found = true;
        auto at = _connections.find(spec->second.connection);
        EdgeNode* e = at != _connections.end() ? at->second->edge.get() : nullptr;
        if (e == nullptr) {
            error = "its connection \"" + spec->second.connection + "\" does not exist";
            found = false;
            return;
        }
        MqttSparkplugNode& node = *e->library;
        // Born: sent now, live, even while what it kept is still going out as
        // history - that is filed at its own times, and never overwrites the
        // live picture, so the newest value must go as live data or the host
        // shows the older one until it next changes. (It waited behind the
        // history, and was kept as history itself.) A dead link the node has
        // not noticed yet is the replay window's (startEdgeNode).
        const bool born = node.isBorn();
        for (AppSparkplugSet& set : values) {
            const int index = node.indexOf(e->device(device), set.name.c_str());
            if (index < 0) {
                unknown.push_back(set.name);
                continue;
            }
            const MqttSparkplugPoint* point = node.pointAt((size_t)index);
            set.value.type = point->type;
            if (set.value.isSpan() && !set.value.isNull) {
                set.value.as.bytes.data = (const uint8_t*)set.text.data();
                set.value.as.bytes.length = (uint32_t)set.text.size();
            }
            // An instance of a definition this node publishes, and no other
            // ([tck-id-payloads-template-instance-ref]) - checked here, so
            // one is never kept as history it could not send.
            if (set.value.type == MqttSparkplugDataType::Template && !set.value.isNull) {
                const std::string ref = templateRefOf(set.value);
                const bool known = std::any_of(e->spec.templates.begin(), e->spec.templates.end(),
                                               [&](const AppSparkplugTemplateDefinition& t) { return t.name == ref; });
                if (!known) {
                    refused.push_back(set.name + ": its template_ref \"" + ref +
                                      "\" is not one of this edge node's templates");
                    continue;
                }
            }
            if (set.timestamp == 0) set.timestamp = (uint64_t)utcNowMs();
            if (born) {
                // The same value again is not news: nothing to send, and not
                // history either (it used to be kept, and sent as history).
                if (!set.value.differsFrom(node.valueAt((size_t)index))) {
                    sent++;
                    continue;
                }
                if (e->setCurrent((size_t)index, set.value, set.text, set.timestamp)) {
                    sent++;
                } else {
                    refused.push_back(set.name + ": " + mq_sparkplugErrorName(node.lastPayloadError()));
                }
                continue;
            }
            // Not born: its current value all the same - the NBIRTH carries the
            // newest, as current, where it carried the one from before the outage
            // and the newer ones only as history (which a host files and does not
            // show) - and kept, to go as history once it is born.
            e->setCurrent((size_t)index, set.value, set.text, set.timestamp);
            uint64_t ignored = 0;
            while (e->history >= e->spec.historyMax && e->spec.historyMax != 0) {
                if (!_outbox.dropOldest(e->key(), nullptr)) break;
                e->history--;
                e->historyLost++;
            }
            if (e->spec.historyMax != 0 && _outbox.push(e->key(), keptValue(device, set), ignored)) {
                e->history++;
                kept++;
            }
        }
        if (sent != 0) node.publishChanged();
    });
    return found;
}

bool AppConnections::deviceState(const std::string& id, const std::string& device, bool online, std::string& error) {
    bool done = false;
    call([&] {
        auto spec = _edgeNodes.find(id);
        auto at = spec == _edgeNodes.end() ? _connections.end() : _connections.find(spec->second.connection);
        if (at == _connections.end() || !at->second->edge) {
            error = "no edge node \"" + id + "\" running";
            return;
        }
        MqttSparkplugNode& node = *at->second->edge->library;
        done = online ? node.deviceOnline(device.c_str()) : node.deviceOffline(device.c_str());
        if (!done) error = "no device \"" + device + "\" on it";
    });
    return done;
}

bool AppConnections::rebirthEdgeNode(const std::string& id, std::string& error) {
    bool done = false;
    call([&] {
        auto spec = _edgeNodes.find(id);
        auto at = spec == _edgeNodes.end() ? _connections.end() : _connections.find(spec->second.connection);
        if (at == _connections.end() || !at->second->edge) {
            error = "no edge node \"" + id + "\" running";
            return;
        }
        done = at->second->edge->library->rebirth();
        if (!done) error = "it is not connected";
    });
    return done;
}

void AppConnections::saveHosts() {
    if (_directory.empty()) return;
    std::string text = "[";
    bool first = true;
    for (const auto& one : _hosts) {
        const AppSparkplugHostSpec& h = one.second;
        json::Object o;
        o.add("id", h.id).add("connection", h.connection).add("hostId", h.hostId).add("group", h.group);
        o.add("rebirthOnGap", h.rebirthOnGap).add("reorderTimeoutMs", (long long)h.reorderTimeoutMs);
        o.add("rebirthMinIntervalMs", (long long)h.rebirthMinIntervalMs);
        text += (first ? "\n" : ",\n") + o.str();
        first = false;
    }
    writePrivate(_directory + "/sparkplug-hosts.json", text + "\n]\n");
    text = "[";
    first = true;
    for (const auto& one : _edgeNodes) {
        const AppSparkplugEdgeNodeSpec& n = one.second;
        json::Object o;
        o.add("id", n.id).add("connection", n.connection).add("group", n.group).add("node", n.node);
        o.add("primaryHostId", n.primaryHostId).add("historyMax", (long long)n.historyMax);
        o.raw("devices", json::array(n.devices));
        std::string metrics = "[";
        for (size_t i = 0; i < n.metrics.size(); i++) {
            const AppSparkplugMetricDeclaration& m = n.metrics[i];
            json::Object d;
            d.add("name", m.name).add("device", m.device).add("datatype", (long long)m.datatype);
            d.add("writable", m.writable).add("units", m.units).add("hasRange", m.hasRange);
            d.add("low", std::to_string(m.low)).add("high", std::to_string(m.high));
            d.add("deadband", std::to_string(m.deadband)).add("deadbandPercent", std::to_string(m.deadbandPercent));
            metrics += (i ? "," : "") + d.str();
        }
        o.raw("metrics", metrics + "]");
        std::string defs = "[";
        for (size_t i = 0; i < n.templates.size(); i++) {
            json::Object d;
            d.add("name", n.templates[i].name).add("encoded", hexOf(n.templates[i].encoded));
            defs += (i ? "," : "") + d.str();
        }
        o.raw("templates", defs + "]");
        text += (first ? "\n" : ",\n") + o.str();
        first = false;
    }
    writePrivate(_directory + "/sparkplug-nodes.json", text + "\n]\n");
}

bool AppConnections::loadHosts(std::string& error) {
    std::ifstream nodesIn(_directory + "/sparkplug-nodes.json");
    if (nodesIn) {
        std::stringstream text;
        text << nodesIn.rdbuf();
        json::Value all;
        if (!json::parse(text.str(), all)) {
            error = _directory + "/sparkplug-nodes.json: not JSON";
            return false;
        }
        for (const json::Value& v : all.items) {
            AppSparkplugEdgeNodeSpec n;
            n.id = v.str("id");
            n.connection = v.str("connection");
            n.group = v.str("group");
            n.node = v.str("node");
            n.primaryHostId = v.str("primaryHostId");
            n.historyMax = (uint64_t)v.integer("historyMax", 100000);
            v.strings("devices", n.devices);
            for (const json::Value& d : v["metrics"].items) {
                AppSparkplugMetricDeclaration m;
                m.name = d.str("name");
                m.device = d.str("device");
                m.datatype = (uint32_t)d.integer("datatype");
                m.writable = d.flag("writable");
                m.units = d.str("units");
                m.hasRange = d.flag("hasRange");
                m.low = std::atof(d.str("low").c_str());
                m.high = std::atof(d.str("high").c_str());
                m.deadband = std::atof(d.str("deadband").c_str());
                m.deadbandPercent = std::atof(d.str("deadbandPercent").c_str());
                n.metrics.push_back(m);
            }
            for (const json::Value& d : v["templates"].items) {
                AppSparkplugTemplateDefinition t;
                t.name = d.str("name");
                t.encoded = bytesOfHex(d.str("encoded"));
                n.templates.push_back(t);
            }
            if (!n.id.empty()) _edgeNodes[n.id] = n;
        }
    }
    std::ifstream in(_directory + "/sparkplug-hosts.json");
    if (!in) return true;
    std::stringstream text;
    text << in.rdbuf();
    json::Value all;
    if (!json::parse(text.str(), all)) {
        error = _directory + "/sparkplug-hosts.json: not JSON";
        return false;
    }
    for (const json::Value& v : all.items) {
        AppSparkplugHostSpec h;
        h.id = v.str("id");
        h.connection = v.str("connection");
        h.hostId = v.str("hostId");
        h.group = v.str("group");
        h.rebirthOnGap = v.has("rebirthOnGap") ? v.flag("rebirthOnGap") : true;
        h.reorderTimeoutMs = (uint32_t)v.integer("reorderTimeoutMs", 2000);
        h.rebirthMinIntervalMs = (uint32_t)v.integer("rebirthMinIntervalMs", 60000);
        if (!h.id.empty()) _hosts[h.id] = h;
    }
    return true;
}

void AppConnections::groupsChanged(const std::string& connection) {
    post([this, connection] {
        auto at = _connections.find(connection);
        if (at != _connections.end()) at->second->filtersChanged = true;
    });
}

// --- Kept ---------------------------------------------------------------------

void AppConnections::save() {
    if (_directory.empty()) return;
    std::vector<std::string> all;
    for (const auto& one : _connections) all.push_back(toJson(one.second->spec));
    std::string text = "[";
    for (size_t i = 0; i < all.size(); i++) text += (i ? ",\n" : "\n") + all[i];
    text += "\n]\n";
    writePrivate(_directory + "/connections.json", text);
}

bool AppConnections::load(std::string& error) {
    std::ifstream in(_directory + "/connections.json", std::ios::binary);
    if (!in) return true;          // nothing kept yet
    std::ostringstream text;
    text << in.rdbuf();
    json::Value all;
    if (!json::parse(text.str(), all) || all.type != json::Value::Array) {
        error = _directory + "/connections.json is not a list of connections";
        return false;
    }
    for (const json::Value& one : all.items) {
        AppConnectionSpec spec;
        if (!fromJson(one, spec)) continue;
        if (!idIsName(spec.id)) {
            std::fprintf(stderr, "connections.json: \"%s\" is not a connection id any more (letters, digits and "
                                 ". _ - : @) - not opened; Put it again under another\n", spec.id.c_str());
            continue;
        }
        std::unique_ptr<Connection> c(new Connection());
        c->spec = spec;
        open(*c);
        _connections[spec.id] = std::move(c);
    }
    return true;
}

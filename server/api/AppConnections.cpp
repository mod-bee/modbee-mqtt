// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AppConnections.h"

#include "Json.h"
#include "MqttClient.h"
#include "sparkplug/MqttSparkplugHost.h"
#include "transport/MqttTcpTransport.h"
#if MQTT_TLS_SOCKETS
#include "transport/MqttTlsTransport.h"
#endif

#include <algorithm>
#include <set>
#include <chrono>
#include <cstdio>
#include <fstream>
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
    o.add("primaryCheck", (long long)s.primaryCheckSeconds);
    o.add("receiveMaximum", (long long)s.receiveMaximum);
    o.add("topicAliasMaximum", (long long)s.topicAliasMaximum);
    o.add("maximumPacketSize", (long long)s.maximumPacketSize);
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
    s.primaryCheckSeconds = (uint32_t)v.integer("primaryCheck", 60);
    s.receiveMaximum = (uint16_t)v.integer("receiveMaximum");
    s.topicAliasMaximum = (uint16_t)v.integer("topicAliasMaximum");
    s.maximumPacketSize = (uint32_t)v.integer("maximumPacketSize");
    if (v.has("store")) {
        const json::Value& k = v["store"];
        s.storeAndForward = true;
        s.storeMaxMessages = (uint64_t)k.integer("messages", 100000);
        s.storeMaxBytes = (uint64_t)k.integer("bytes", 64ll << 20);
        s.storeDropOldest = k.flag("dropOldest");
    }
    return !s.id.empty() && (!s.servers.empty() || s.thisServer);
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
    uint32_t backoffMs = 0;

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
        const bool lost = result == MqttResult::ConnectionLost || result == MqttResult::SessionLost ||
                          result == MqttResult::AckTimeout || result == MqttResult::NotConnected;
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

    // This server itself, as reachThisServer() says now; or one of `servers`.
    AppEndpoint here;
    bool hereTls = false;
    const AppEndpoint& endpoint(uint32_t index) const {
        return spec.thisServer || spec.servers.empty() ? here : spec.servers[index % spec.servers.size()];
    }
    bool usesTls() const { return spec.thisServer ? hereTls : spec.tls; }
    uint16_t portOf(const AppEndpoint& e) const { return e.port ? e.port : (usesTls() ? 8883 : 1883); }

    // Last, so it goes before the client it is attached to.
    std::unique_ptr<Host> host;

    void setState(AppConnectionState next, const std::string& error = std::string()) {
        if (next == state && error == lastError) return;
        state = next;
        if (!error.empty() || next == AppConnectionState::Connected) lastError = error;
        sinceMs = utcNowMs();
        owner->announce(*this);
    }
};

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
            if (where.hasLogin) {
                connection.userName = where.user.c_str();
                connection.hasUserName = true;
                connection.password.assign(where.password.begin(), where.password.end());
                connection.hasPassword = true;
            }
        }
    }
    applyWill(c.spec, connection);
    connection.receiveMaximum = c.spec.receiveMaximum;
    connection.topicAliasMaximum = c.spec.topicAliasMaximum;
    connection.maximumPacketSize = c.spec.maximumPacketSize;
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
    behaviour.autoReconnect = false;     // the manager decides: which server, and when
    behaviour.queueCapacity = 256;       // room for store and forward's window as well
    behaviour.receiveBufferSize = 65536;
    behaviour.transmitBufferSize = 65536;
    c.client.begin(*c.transport, connection, behaviour);
    c.client.onMessage(&Connection::received, &c);
    c.client.onSessionEstablished(&Connection::established, &c);
    c.state = AppConnectionState::Connecting;
    c.nextAttempt = Clock::now();
    c.filtersChanged = true;
    startHost(c);
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

void AppConnections::close(Connection& c) {
    // A host's STATE dies first, on purpose rather than by its Will.
    if (c.host) {
        c.host->library->disconnect();
        for (int pass = 0; pass < 20 && c.client.isConnected(); pass++) {
            c.client.poll();
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        c.host.reset();
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
    serviceHost(c);
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
        c.state != AppConnectionState::Waiting && c.state != AppConnectionState::Refused &&
        (c.asked || now >= c.nextAttempt)) {
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
        const AppEndpoint& e = c.endpoint(c.serverIndex);
        MqttConnectionConfig connection = c.client.connection();
        connection.host = e.host.c_str();
        connection.port = c.portOf(e);
        applyWill(c.spec, connection);
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
            c.backoffMs = 0;
            c.filtersChanged = true;
            c.subscribed.clear();
            c.nextProbe = now + std::chrono::seconds(c.spec.primaryCheckSeconds);
            c.setState(AppConnectionState::Connected);
        } else if (state == MqttState::Idle && (was != MqttState::Idle || takenBack) && c.attempting) {
            c.attempting = false;
            const uint8_t refused = c.client.status().connectReasonCode;
            const bool loginRefused = !c.reachedThisAttempt && refused != 0 && refused != 0x88 &&
                                      refused != 0x89 && refused != 0x03 && refused != 0x9C && refused != 0x9D;
            if (loginRefused) {
                char why[64];
                std::snprintf(why, sizeof why, "the server refused the connection (0x%02X)", refused);
                c.setState(AppConnectionState::Refused, why);
            } else if (c.reachedThisAttempt && c.spec.reconnectWhenAsked) {
                c.setState(AppConnectionState::Waiting, "the connection was lost");
            } else {
                // Failed to connect: the next server. Lost after connecting:
                // the same one first.
                if (!c.reachedThisAttempt && !c.spec.servers.empty()) {
                    c.serverIndex = (c.serverIndex + 1) % (uint32_t)c.spec.servers.size();
                }
                c.backoffMs = c.backoffMs ? std::min<uint32_t>(c.backoffMs * 2, 30000) : 500;
                c.nextAttempt = now + std::chrono::milliseconds(c.reachedThisAttempt ? 200 : c.backoffMs);
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
                c.client.disconnect();
                c.asked = true;
            } else if (c.probe->connectFailed()) {
                c.probe->end();
                c.probe.reset();
                c.nextProbe = now + std::chrono::seconds(c.spec.primaryCheckSeconds);
            }
        }
    }
}

// --- The API's side ----------------------------------------------------------

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
    if (spec.servers.empty() && !spec.thisServer) {
        error = "a connection needs at least one server (or this_server)";
        return false;
    }
    call([&] {
        std::unique_ptr<Connection>& slot = _connections[spec.id];
        if (!slot || !sameSpec(slot->spec, spec)) {
            if (slot) close(*slot);
            slot.reset(new Connection());
            slot->spec = spec;
            open(*slot);
            save();
        }
        if (info) *info = infoOf(*slot);
    });
    return true;
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
        close(*at->second);
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
    // Each one's state now, from the manager's thread.
    post([this, ids, changed] {
        for (auto& one : _connections) {
            if (ids.empty() || std::find(ids.begin(), ids.end(), one.first) != ids.end()) {
                changed(infoOf(*one.second));
            }
        }
    });
    return id;
}

void AppConnections::unwatch(uint64_t watcher) {
    std::lock_guard<std::mutex> guard(_lock);
    _watchers.erase(watcher);
}

void AppConnections::publish(const AppPublish& message, std::function<void(const AppPublishResult&)> done) {
    post([this, message, done] {
        auto at = _connections.find(message.connection);
        if (at != _connections.end() && at->second->spec.storeAndForward) {
            store(*at->second, message, done);
            return;
        }
        if (at == _connections.end() || !at->second->client.isConnected()) {
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
        if (queued != MqttResult::Queued && queued != MqttResult::Ok) break;   // the client is full: next pass
        c.inClient.insert(k.id);
        c.sent.push_back(std::move(sent));
        c.messagesOut++;
    }
}

// --- Sparkplug hosts ------------------------------------------------------------

namespace {

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
            const bool up = at != _connections.end() && at->second->client.isConnected();
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
        const bool up = at != _connections.end() && at->second->client.isConnected();
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
            const bool up = at != _connections.end() && at->second->client.isConnected();
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
                // Numbers and bools only: the library keeps text by reference to a buffer it reuses.
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
}

bool AppConnections::loadHosts(std::string& error) {
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
        std::unique_ptr<Connection> c(new Connection());
        c->spec = spec;
        open(*c);
        _connections[spec.id] = std::move(c);
    }
    return true;
}

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AppConnections.h"

#include "Json.h"
#include "MqttClient.h"
#include "transport/MqttTcpTransport.h"
#if MQTT_TLS_SOCKETS
#include "transport/MqttTlsTransport.h"
#endif

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <set>
#include <sstream>

#include "ServerOs.h"

namespace {

typedef std::chrono::steady_clock Clock;

int64_t utcNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// What a refusal by the remote broker means for the publish that asked.
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
        o.raw("will", w.str());
    }
    o.add("whenAsked", s.reconnectWhenAsked);
    o.add("primaryCheck", (long long)s.primaryCheckSeconds);
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
    }
    s.reconnectWhenAsked = v.flag("whenAsked");
    s.primaryCheckSeconds = (uint32_t)v.integer("primaryCheck", 60);
    return !s.id.empty() && !s.servers.empty();
}

bool sameSpec(const AppConnectionSpec& a, const AppConnectionSpec& b) { return toJson(a) == toJson(b); }

} // namespace

// =============================================================================
// ONE CONNECTION
// =============================================================================

struct AppConnections::Connection {
    AppConnections* owner = nullptr;
    AppConnectionSpec spec;
    std::unique_ptr<MqttTransport> transport;
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

    std::set<std::string> subscribed;          // what the remote broker has been asked for
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
        self.owner->_hub.connectionMessage(self.spec.id, m);
    }

    const AppEndpoint& endpoint(uint32_t index) const { return spec.servers[index % spec.servers.size()]; }
    uint16_t portOf(const AppEndpoint& e) const { return e.port ? e.port : (spec.tls ? 8883 : 1883); }

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

AppConnections::AppConnections(AppHub& hub, const std::string& stateDirectory)
    : _hub(hub), _directory(stateDirectory) {}

AppConnections::~AppConnections() { stop(); }

bool AppConnections::start(std::string& error) {
    if (!_directory.empty()) {
        server_os::makeDirectory(_directory, 0700);
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
    if (c.spec.hasWill) {
        connection.will.set(c.spec.will.topic.c_str(), (const uint8_t*)c.spec.will.payload.data(),
                            c.spec.will.payload.size(), c.spec.will.qos, c.spec.will.retain);
    }
    const AppEndpoint& first = c.endpoint(c.serverIndex);
    connection.host = first.host.c_str();
    connection.port = c.portOf(first);
#if MQTT_TLS_SOCKETS
    if (c.spec.tls) {
        MqttTlsTransport* tls = new MqttTlsTransport();
        if (!c.spec.caPem.empty()) tls->setCaCertificate(c.spec.caPem.c_str());
        if (!c.spec.certificatePem.empty() && !_directory.empty()) {
            const std::string base = _directory + "/" + c.spec.id;
            writePrivate(base + ".crt", c.spec.certificatePem);
            writePrivate(base + ".key", c.spec.keyPem);
            tls->setClientCertificate((base + ".crt").c_str(), (base + ".key").c_str());
        }
        c.transport.reset(tls);
    } else
#endif
    {
        c.transport.reset(new MqttTcpTransport());
    }
    MqttClientConfig behaviour;
    behaviour.autoReconnect = false;     // the manager decides: which server, and when
    behaviour.receiveBufferSize = 65536;
    behaviour.transmitBufferSize = 65536;
    c.client.begin(*c.transport, connection, behaviour);
    c.client.onMessage(&Connection::received, &c);
    c.client.onSessionEstablished(&Connection::established, &c);
    c.state = AppConnectionState::Connecting;
    c.nextAttempt = Clock::now();
    c.filtersChanged = true;
}

void AppConnections::close(Connection& c) {
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
}

void AppConnections::service(Connection& c) {
    const Clock::time_point now = Clock::now();
    c.client.poll();
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
        const AppEndpoint& e = c.endpoint(c.serverIndex);
        MqttConnectionConfig connection = c.client.connection();
        connection.host = e.host.c_str();
        connection.port = c.portOf(e);
        if (c.spec.hasWill) {
            connection.will.set(c.spec.will.topic.c_str(), (const uint8_t*)c.spec.will.payload.data(),
                                c.spec.will.payload.size(), c.spec.will.qos, c.spec.will.retain);
        } else {
            connection.will = MqttWillConfig();
        }
        c.client.reconfigure(connection);
#if MQTT_TLS_SOCKETS
        if (c.spec.tls) static_cast<MqttTlsTransport*>(c.transport.get())->configure(e.host.c_str(), c.portOf(e));
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
                if (!c.reachedThisAttempt) c.serverIndex = (c.serverIndex + 1) % (uint32_t)c.spec.servers.size();
                c.backoffMs = c.backoffMs ? std::min<uint32_t>(c.backoffMs * 2, 30000) : 500;
                c.nextAttempt = now + std::chrono::milliseconds(c.reachedThisAttempt ? 200 : c.backoffMs);
                c.setState(AppConnectionState::BackingOff,
                           c.reachedThisAttempt ? "the connection was lost" : "could not connect");
            }
        }
    }

    if (state != MqttState::Connected) return;

    // --- Subscriptions: what the groups on it want ---
    if (c.filtersChanged) {
        c.filtersChanged = false;
        const std::vector<std::string> wanted = _hub.filtersFor(c.spec.id);
        std::set<std::string> want(wanted.begin(), wanted.end());
        for (const std::string& filter : want) {
            if (!c.subscribed.count(filter)) c.client.subscribe(filter.c_str(), MQ_QOS_1);
        }
        for (const std::string& filter : c.subscribed) {
            if (!want.count(filter)) c.client.unsubscribe(filter.c_str());
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
    if (spec.servers.empty()) {
        error = "a connection needs at least one server";
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

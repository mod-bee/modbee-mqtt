// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// THE LIBRARY'S BROKER AND CLIENT, END TO END over real TCP on loopback:
// publish and subscribe at every QoS, retained messages, Wills, sessions that
// outlive a connection, shared subscriptions, logins, 3.1.1 - and a raw peer
// for what a well-behaved client never does, which is where the regressions
// live.

#include "TestClient.h"

#include <memory>
#include <vector>

using check::equal;
using check::ok;
using check::same;
using check::section;
using check::span;
using check::text;
using check::Client;
using check::Got;

namespace {

// --- A broker on an ephemeral port, and everything polled with it ---

struct Bench {
    MqttBrokerConfig config;
    MqttTcpListener listener;
    MqttBroker broker;
    MqttBrokerAccounts accounts;
    std::vector<std::function<void()>> others;
    bool started = false;

    explicit Bench(bool logins = false) {
        config.maxConnections = 32;
        config.maxSessions = 64;
        config.receiveBufferSize = 300000;
        config.transmitBufferSize = 300000;
        config.maximumPacketSize = 300000;
        listener.configure("127.0.0.1", 0, config.maxConnections);
        started = listener.begin();
        broker.begin(config);
        if (logins) broker.onAuthorise(&MqttBrokerAccounts::hook, &accounts);
        broker.addListener(listener);
    }
    ~Bench() { broker.end(); }
    uint16_t port() const { return listener.boundPort(); }
    void poll() {
        broker.poll();
        for (auto& other : others) other();
    }
    bool until(const std::function<bool()>& done, int ms = 5000) {
        return check::until(done, [this] { poll(); }, ms);
    }
};

// The bench's clients, polled with its broker.
struct Clients {
    check::Fleet fleet;
    Bench& bench;
    explicit Clients(Bench& b) : fleet(b.port()), bench(b) {
        b.others.push_back([this] {
            for (auto& c : fleet.all) c->mqtt.poll();
        });
    }
    ~Clients() { bench.others.clear(); }
    Client& make(const std::string& id, MqttVersion version = MqttVersion::V5) { return fleet.make(id, version); }
    bool connected(Client& c, int ms = 5000) {
        c.start();
        return bench.until([&] { return c.mqtt.isConnected(); }, ms);
    }
};

// --- A raw peer: packets by hand, for what a real client never does ---

struct Raw {
    MqttTcpTransport transport;
    std::vector<uint8_t> in;
    MqttCodecV5 codec;
    Bench& bench;

    struct Packet {
        uint8_t type = 0;
        uint16_t packetId = 0;
        bool dup = false;
        uint8_t qos = 0, reasonCode = 0;
        bool sessionPresent = false;
        std::string topic, payload;
    };

    explicit Raw(Bench& b) : bench(b) {}

    bool open() {
        in.clear();
        transport.configure("127.0.0.1", bench.port());
        transport.begin();
        return bench.until([&] { return transport.isConnected(); }, 3000);
    }
    void close() { transport.end(); }
    bool send(const MqttPacket& packet) {
        std::vector<uint8_t> bytes(codec.size(packet));
        size_t written = 0;
        if (codec.encode(bytes.data(), bytes.size(), packet, written) != MqttError::None) return false;
        size_t sent = 0;
        bench.until([&] {
            sent += transport.write(bytes.data() + sent, written - sent);
            return sent >= written;
        }, 3000);
        return sent == written;
    }
    void sendBytes(const std::vector<uint8_t>& bytes) {
        size_t sent = 0;
        bench.until([&] {
            sent += transport.write(bytes.data() + sent, bytes.size() - sent);
            return sent >= bytes.size();
        }, 3000);
    }
    // The next packet of `type`, or false.
    bool next(uint8_t type, Packet& out, int ms = 3000) {
        bool found = false;
        bench.until([&] {
            uint8_t chunk[4096];
            size_t n;
            while ((n = transport.read(chunk, sizeof chunk)) > 0) in.insert(in.end(), chunk, chunk + n);
            while (!in.empty()) {
                MqttFixedHeader header;
                if (mq_decodeFixedHeader(in.data(), in.size(), header) != MqttError::None) return false;
                if (in.size() < header.packetSize()) return false;
                MqttPacket p;
                const bool decoded =
                    codec.decode(header, in.data() + header.headerLength, header.remainingLength, p) == MqttError::None;
                Packet got;
                got.type = header.type;
                if (decoded && header.type == MQ_PKT_PUBLISH) {
                    got.packetId = p.publish.packetId;
                    got.dup = p.publish.dup;
                    got.qos = p.publish.qos;
                    got.topic = text(p.publish.topic);
                    got.payload = text(p.publish.payload);
                } else if (decoded && header.type == MQ_PKT_CONNACK) {
                    got.reasonCode = p.connack.reasonCode;
                    got.sessionPresent = p.connack.sessionPresent;
                } else if (decoded && header.type == MQ_PKT_DISCONNECT) {
                    got.reasonCode = p.disconnect.reasonCode;
                } else if (decoded && (header.type == MQ_PKT_PUBACK || header.type == MQ_PKT_SUBACK)) {
                    got.packetId = header.type == MQ_PKT_PUBACK ? p.ack.packetId : p.suback.packetId;
                    got.reasonCode = header.type == MQ_PKT_PUBACK ? p.ack.reasonCode : p.suback.reasons[0];
                }
                in.erase(in.begin(), in.begin() + header.packetSize());
                if (got.type == type) {
                    out = got;
                    found = true;
                    return true;
                }
            }
            return false;
        }, ms);
        return found;
    }
    // Whether the broker closed the connection within `ms`.
    bool closed(int ms) {
        return bench.until([&] {
            uint8_t chunk[256];
            while (transport.read(chunk, sizeof chunk) > 0) {}
            return transport.peerClosed() || !transport.isConnected();
        }, ms);
    }
    bool connect(const std::string& id, bool cleanStart, uint32_t expiry, uint16_t keepAlive, Packet& ack) {
        MqttPacket p;
        p.header.type = MQ_PKT_CONNECT;
        p.connect.version = MqttVersion::V5;
        p.connect.clientId = span(id);
        p.connect.cleanStart = cleanStart;
        p.connect.keepAlive = keepAlive;
        if (expiry) {
            p.connect.properties.sessionExpiryInterval = expiry;
            p.connect.properties.markPresent(MQ_PROP_SESSION_EXPIRY_INTERVAL);
        }
        return send(p) && next(MQ_PKT_CONNACK, ack);
    }
    bool subscribe(const std::string& filter, uint8_t qos, uint16_t id) {
        MqttPacket p;
        p.header.type = MQ_PKT_SUBSCRIBE;
        p.header.flags = 0x02;
        p.subscribe.packetId = id;
        p.subscribe.filters[0].filter = span(filter);
        p.subscribe.filters[0].options = qos;
        p.subscribe.count = 1;
        Packet ack;
        return send(p) && next(MQ_PKT_SUBACK, ack) && ack.reasonCode == qos;
    }
};

// =============================================================================

void publishAndSubscribe() {
    section("publish and subscribe, at every QoS");
    Bench bench;
    ok(bench.started, "a broker listens on an ephemeral port");
    Clients clients(bench);
    Client& sub = clients.make("sub");
    Client& pub = clients.make("pub");
    sub.subscribe("plant/#", MQ_QOS_2);
    ok(clients.connected(sub) && clients.connected(pub), "two clients connect");
    bench.until([&] { return sub.subscribed >= 1; }, 2000);
    for (uint8_t qos = 0; qos <= 2; qos++) pub.publish("plant/qos" + std::to_string(qos), "v" + std::to_string(qos), qos);
    ok(bench.until([&] { return sub.got.size() >= 3; }), "the subscriber receives all three");
    bool right = sub.got.size() == 3;
    for (size_t i = 0; right && i < 3; i++) {
        right = sub.got[i].qos == i && sub.got[i].payload == "v" + std::to_string(i) && !sub.got[i].retain;
    }
    ok(right, "  in order, each at the QoS it was published with");
    ok(bench.until([&] { return pub.completed >= 3; }), "the publisher's three complete (PUBACK, PUBCOMP)");
    equal<int>("  none failed", pub.failed, 0);

    // The subscription's QoS caps the delivery.
    Client& low = clients.make("low");
    low.subscribe("plant/#", MQ_QOS_0);
    clients.connected(low);
    bench.until([&] { return low.subscribed >= 1; }, 2000);
    pub.publish("plant/capped", "c", MQ_QOS_2);
    ok(bench.until([&] { return !low.got.empty(); }) && low.got[0].qos == 0, "a QoS 0 subscription gets QoS 2 as QoS 0");

    // Unsubscribed, nothing more arrives.
    bool unsubscribed = false;
    low.mqtt.unsubscribe("plant/#", [](void* c, MqttResult, uint16_t) { *static_cast<bool*>(c) = true; }, &unsubscribed);
    bench.until([&] { return unsubscribed; }, 2000);
    const size_t before = low.got.size();
    pub.publish("plant/after", "x", MQ_QOS_1);
    bench.until([&] { return pub.completed >= 5; }, 2000);
    equal<size_t>("after UNSUBSCRIBE, nothing", low.got.size(), before);
}

void retained() {
    section("retained messages");
    Bench bench;
    Clients clients(bench);
    Client& pub = clients.make("pub");
    clients.connected(pub);
    pub.publish("plant/state", "running", MQ_QOS_1, true);
    bench.until([&] { return pub.completed >= 1; });
    Client& late = clients.make("late");
    late.subscribe("plant/#", MQ_QOS_1);
    clients.connected(late);
    ok(bench.until([&] { return !late.got.empty(); }) && late.got[0].payload == "running" && late.got[0].retain,
       "a subscriber that comes later gets it, marked retained");
    pub.publish("plant/state", "", MQ_QOS_1, true);
    bench.until([&] { return pub.completed >= 2; });
    Client& later = clients.make("later");
    later.subscribe("plant/#", MQ_QOS_1);
    clients.connected(later);
    bench.until([&] { return false; }, 300);
    ok(later.got.empty(), "an empty retained message clears it");
}

void wills() {
    section("Wills");
    Bench bench;
    Clients clients(bench);
    Client& watch = clients.make("watch");
    watch.subscribe("status/#", MQ_QOS_1);
    clients.connected(watch);
    Client& gone = clients.make("gone");
    gone.will("status/gone", "offline", MQ_QOS_1, false);
    clients.connected(gone);
    gone.transport.end();                  // the socket closed, no DISCONNECT
    ok(bench.until([&] { return !watch.got.empty(); }) && watch.got[0].topic == "status/gone" &&
           watch.got[0].payload == "offline",
       "a client that vanishes has its Will published");
    Client& tidy = clients.make("tidy");
    tidy.will("status/tidy", "offline", MQ_QOS_1, false);
    clients.connected(tidy);
    tidy.mqtt.disconnect();
    bench.until([&] { return false; }, 400);
    bool any = false;
    for (const Got& g : watch.got) any = any || g.topic == "status/tidy";
    ok(!any, "one that sends DISCONNECT does not");
}

void sessions() {
    section("a session outlives its connection");
    Bench bench;
    Clients clients(bench);
    Client& pub = clients.make("pub");
    clients.connected(pub);
    {
        Client& away = clients.make("field-unit");
        away.persistent(300);
        away.subscribe("cmd/field-unit/#", MQ_QOS_1);
        ok(clients.connected(away), "a client with a session expiry connects and subscribes");
        bench.until([&] { return away.subscribed >= 1; }, 2000);
        away.mqtt.disconnect();
        bench.until([&] { return !away.mqtt.isConnected(); }, 2000);
    }
    for (int n = 0; n < 3; n++) pub.series("cmd/field-unit/set", "s" + std::to_string(n), MQ_QOS_1);
    bench.until([&] { return pub.completed >= 3; });
    Client& back = clients.make("field-unit");
    back.persistent(300);
    ok(clients.connected(back), "it comes back");
    ok(bench.until([&] { return back.got.size() >= 3; }), "  and is sent the three it missed, without subscribing again");
    ok(back.got.size() == 3 && back.got[0].payload == "s0" && back.got[2].payload == "s2", "  in order");
}

void shared() {
    section("shared subscriptions");
    Bench bench;
    Clients clients(bench);
    Client& a = clients.make("worker-a");
    Client& b = clients.make("worker-b");
    a.subscribe("$share/workers/jobs/#", MQ_QOS_1);
    b.subscribe("$share/workers/jobs/#", MQ_QOS_1);
    clients.connected(a);
    clients.connected(b);
    Client& pub = clients.make("pub");
    clients.connected(pub);
    bench.until([&] { return a.subscribed >= 1 && b.subscribed >= 1; }, 2000);
    for (int n = 0; n < 40; n++) pub.publish("jobs/" + std::to_string(n), "j", MQ_QOS_1);
    ok(bench.until([&] { return a.got.size() + b.got.size() >= 40; }), "40 jobs, each to one worker");
    equal<size_t>("  no job twice", a.got.size() + b.got.size(), 40u);
    ok(!a.got.empty() && !b.got.empty(), "  both workers had some");
}

void logins() {
    section("logins");
    Bench bench(true);
    bench.accounts.set("device-1", "right");
    Clients clients(bench);
    Client& wrong = clients.make("device-1");
    wrong.login("device-1", "wrong");
    ok(!clients.connected(wrong, 1500), "a wrong password is refused");
    equal<int>("  0x86 Bad user name or password", wrong.mqtt.status().connectReasonCode, 0x86);
    Client& right = clients.make("device-1b");
    right.login("device-1", "right");
    ok(clients.connected(right), "the right one connects");
}

void version311() {
    section("an MQTT 3.1.1 client");
    Bench bench;
    Clients clients(bench);
    Client& old = clients.make("legacy", MqttVersion::V311);
    old.subscribe("plant/#", MQ_QOS_1);
    ok(clients.connected(old), "connects at 3.1.1");
    Client& pub = clients.make("pub");
    clients.connected(pub);
    bench.until([&] { return old.subscribed >= 1; }, 2000);
    pub.publish("plant/x", "from 5.0", MQ_QOS_1);
    ok(bench.until([&] { return !old.got.empty(); }) && old.got[0].payload == "from 5.0",
       "receives what a 5.0 client published");
}

// --- Regressions: what a raw peer can provoke -----------------------------------

void redeliveredWithDup() {
    section("regression: a QoS 1 message resent after a reconnect carries DUP and its packet identifier");
    Bench bench;
    Clients clients(bench);
    Client& pub = clients.make("pub");
    clients.connected(pub);
    Raw raw(bench);
    Raw::Packet ack, first, again;
    ok(raw.open() && raw.connect("raw-1", true, 300, 60, ack) && ack.reasonCode == 0, "a raw client connects");
    ok(raw.subscribe("r/#", MQ_QOS_1, 1), "  subscribes at QoS 1");
    pub.publish("r/1", "once", MQ_QOS_1);
    ok(raw.next(MQ_PKT_PUBLISH, first) && !first.dup && first.packetId != 0, "it is sent the message, DUP 0");
    raw.close();                            // and never acknowledges it
    bench.until([&] { return false; }, 200);
    ok(raw.open() && raw.connect("raw-1", false, 300, 60, ack) && ack.sessionPresent, "back, its session present");
    ok(raw.next(MQ_PKT_PUBLISH, again), "  the message is sent again");
    ok(again.dup, "  with DUP set");
    equal<int>("  under the same packet identifier", again.packetId, first.packetId);
    same("  the same message", again.payload, std::string("once"));
}

void noMatchingSubscribers() {
    section("regression: PUBACK says 0x10 when nobody matched (5.0)");
    Bench bench;
    Raw raw(bench);
    Raw::Packet ack;
    raw.open();
    raw.connect("raw-2", true, 0, 60, ack);
    MqttPacket p;
    p.header.type = MQ_PKT_PUBLISH;
    const std::string topic = "nobody/listens", payload = "x";
    p.publish.topic = span(topic);
    p.publish.payload = span(payload);
    p.publish.qos = MQ_QOS_1;
    p.publish.packetId = 5;
    raw.send(p);
    Raw::Packet puback;
    ok(raw.next(MQ_PKT_PUBACK, puback) && puback.packetId == 5, "PUBACK for packet 5");
    equal<int>("  0x10 No matching subscribers", puback.reasonCode, 0x10);
}

void keepAlive() {
    section("regression: a client that falls silent is closed after one and a half keep alives");
    Bench bench;
    Raw raw(bench);
    Raw::Packet ack;
    raw.open();
    ok(raw.connect("silent", true, 0, 1, ack), "a client with a 1 s keep alive connects, then says nothing");
    const auto start = std::chrono::steady_clock::now();
    ok(raw.closed(4000), "it is closed");
    const long ms = (long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    ok(ms >= 1000 && ms < 3500, "  after " + std::to_string(ms) + " ms (1.5 s allowed, not before 1 s)");
}

void malformed() {
    section("regression: a malformed packet closes the connection");
    Bench bench;
    Raw raw(bench);
    Raw::Packet ack;
    raw.open();
    raw.connect("rude", true, 0, 60, ack);
    // SUBSCRIBE with reserved flags 0000 instead of 0010 [MQTT-3.8.1-1].
    raw.sendBytes({0x80, 0x08, 0x00, 0x01, 0x00, 0x00, 0x02, 'a', '/', 0x01});
    Raw::Packet bye;
    const bool told = raw.next(MQ_PKT_DISCONNECT, bye, 2000);
    ok(raw.closed(2000), "the broker closes it");
    if (told) ok(bye.reasonCode == 0x81, "  telling it 0x81 Malformed Packet first");
    Client* fine = nullptr;
    Clients clients(bench);
    fine = &clients.make("fine");
    ok(clients.connected(*fine), "and goes on serving others");
}

} // namespace

int main() {
    publishAndSubscribe();
    retained();
    wills();
    sessions();
    shared();
    logins();
    version311();
    redeliveredWithDup();
    noMatchingSubscribers();
    keepAlive();
    malformed();
    return check::report("broker_tests");
}

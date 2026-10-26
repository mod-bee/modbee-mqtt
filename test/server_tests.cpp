// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// THE SERVER (`modbee serve`'s engine), END TO END: one broker per core
// behind one port, messages crossing shards, a client id taken over from
// another shard, and what `sessions: store: file` and `retained: store: file`
// keep across a restart. Real TCP on loopback, files in the system's
// temporary directory.

#include "TestClient.h"

#include "ShardedBroker.h"
#include "SessionKeeper.h"
#include "RetainedKeeper.h"

#include <filesystem>
#include <memory>

using check::Client;
using check::Fleet;
using check::equal;
using check::ok;
using check::section;

namespace {

namespace fs = std::filesystem;

// A directory of its own under the system's temporary one, gone afterwards.
struct TempDir {
    fs::path path;
    explicit TempDir(const char* name) {
        path = fs::temp_directory_path() /
               ("modbee-server-tests-" + std::string(name) + "-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ignored;
        fs::remove_all(path, ignored);
    }
};

// A server: shards on threads of their own, extensions as `modbee serve` adds them.
struct Server {
    ShardedBroker broker;
    ShardExtensionChain chain;
    std::unique_ptr<SessionKeeper> sessions;
    std::unique_ptr<RetainedKeeper> retained;
    bool started = false;

    Server(size_t shards, const fs::path* keepIn = nullptr, uint16_t port = 0) {
        ShardedBrokerConfig c;
        c.address = "127.0.0.1";
        c.port = port;
        c.shards = shards;
        c.broker.maxConnections = 256;
        c.broker.maxSessions = 512;
        if (keepIn) {
            SessionKeeperConfig k;
            k.directory = (*keepIn / "sessions").string();
            fs::create_directories(k.directory);
            sessions.reset(new SessionKeeper(k));
            retained.reset(new RetainedKeeper(makeRetainedFile((*keepIn / "retained.db").string())));
            chain.add(retained.get());
            chain.add(sessions.get());
            c.extension = &chain;
        }
        started = broker.start(c);
    }
    ~Server() { broker.stop(); }
    uint16_t port() const { return broker.port(); }
};

void acrossShards() {
    section("one port, four shards: every client reaches every other");
    Server server(4);
    ok(server.started, "a server with four shards starts");
    equal<size_t>("  four shards", server.broker.shardCount(), 4u);
    Fleet fleet(server.port());
    std::vector<Client*> clients;
    for (int n = 0; n < 24; n++) {
        Client& c = fleet.make("unit-" + std::to_string(n));
        c.subscribe("units/" + std::to_string(n) + "/cmd", MQ_QOS_1);
        clients.push_back(&c);
    }
    bool all = true;
    for (Client* c : clients) all = fleet.connected(*c) && all;
    ok(all, "24 clients connect");
    ok(fleet.until([&] {
           for (Client* c : clients) if (c->subscribed < 1) return false;
           return true;
       }),
       "  each subscribes to its own topic");
    // The counters are a snapshot the shards refresh as they go.
    size_t busy = 0;
    fleet.until([&] {
        busy = 0;
        for (size_t n : server.broker.status().connectionsPerShard) busy += n > 0 ? 1 : 0;
        return busy >= 2;
    }, 3000);
    ok(busy >= 2, "  spread over " + std::to_string(busy) + " shards");
    Client& pub = fleet.make("controller");
    fleet.connected(pub);
    for (int n = 0; n < 24; n++) pub.publish("units/" + std::to_string(n) + "/cmd", "go-" + std::to_string(n), MQ_QOS_1);
    ok(fleet.until([&] {
           for (Client* c : clients) if (c->got.empty()) return false;
           return true;
       }),
       "a message to each, from one publisher, reaches every one whatever its shard");
    bool right = true;
    for (int n = 0; n < 24; n++) right = right && clients[n]->got.size() == 1 && clients[n]->got[0].payload == "go-" + std::to_string(n);
    ok(right, "  each its own, once");
    ok(fleet.until([&] { return pub.completed >= 24; }), "  and every PUBACK came back");
}

void takeover() {
    section("a client id that connects again takes the session over, from any shard");
    Server server(4);
    Fleet fleet(server.port());
    Client& first = fleet.make("same-id");
    fleet.connected(first);
    Client& second = fleet.make("same-id");
    ok(fleet.connected(second), "the second connection with the id is accepted");
    ok(fleet.until([&] { return !first.mqtt.isConnected(); }), "  the first is disconnected");
    equal<int>("  told 0x8E Session taken over", first.mqtt.status().lastReasonCode, 0x8E);
}

void keptAcrossRestart() {
    section("sessions: store: file and retained: store: file - a restart loses neither");
    TempDir dir("restart");
    uint16_t port = 0;
    {
        Server server(2, &dir.path);
        ok(server.started, "a server keeping sessions and retained messages in files starts");
        port = server.port();
        Fleet fleet(port);
        Client& unit = fleet.make("field-unit");
        unit.persistent(3600);
        unit.subscribe("cmd/field-unit/#", MQ_QOS_1);
        ok(fleet.connected(unit) && fleet.until([&] { return unit.subscribed >= 1; }), "a device with a session subscribes");
        unit.mqtt.disconnect();
        fleet.until([&] { return !unit.mqtt.isConnected(); });
        Client& pub = fleet.make("controller");
        fleet.connected(pub);
        pub.series("cmd/field-unit/set", "while-away", MQ_QOS_1);
        pub.publish("plant/state", "running", MQ_QOS_1, true);
        ok(fleet.until([&] { return pub.completed + pub.failed >= 2; }), "a command queued for it, and a retained state, both answered");
        equal<int>("  the command taken (queued for the session)", pub.completed, 1);
        // Nobody subscribes to plant/state: 0x10, and it is kept all the same.
        ok(pub.failed == 1 && pub.lastFailure == MqttResult::NoMatchingSubscribers,
           "  the retained state answered 0x10 No matching subscribers, and kept");
        pub.mqtt.disconnect();
        fleet.until([&] { return !pub.mqtt.isConnected(); });
    }
    {
        Server server(2, &dir.path, port);
        ok(server.started, "restarted on the same files");
        Fleet fleet(server.port());
        Client& unit = fleet.make("field-unit");
        unit.persistent(3600);
        ok(fleet.connected(unit), "the device comes back");
        ok(fleet.until([&] { return !unit.got.empty(); }) && unit.got[0].payload == "while-away",
           "  and is sent the command it missed, with no SUBSCRIBE of its own");
        Client& viewer = fleet.make("viewer");
        viewer.subscribe("plant/#", MQ_QOS_1);
        fleet.connected(viewer);
        ok(fleet.until([&] { return !viewer.got.empty(); }) && viewer.got[0].payload == "running" &&
               viewer.got[0].retain,
           "the retained state is still there");
    }
}

} // namespace

int main() {
    acrossShards();
    takeover();
    keptAcrossRestart();
    return check::report("server_tests");
}

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// MQTT CONNECTIONS THE SERVER KEEPS FOR THE APPLICATION (Application API:
// Connections)
// =============================================================================
//
// Another broker - a customer's, the platform's for a gateway's uplink, one a
// Sparkplug host or edge node lives on - reached by the server's own MQTT
// client, so the application needs no MQTT library and the connection stays
// up while the application restarts.
//
//   FAILOVER       `servers` in order of preference: an attempt that fails
//                  moves to the next; on any but the first, the first is
//                  checked every `primaryCheckSeconds` and gone back to once
//                  it answers
//   KEPT           the connections are written to a file in the server's data
//                  directory and opened again at start
//   IN             the messages it receives go to the consumer groups opened
//                  on it (AppHub), which subscribe it to their filters
//   OUT            a publish on it is answered when the remote broker has
//                  taken it - its PUBACK/PUBCOMP for CONFIRM_DELIVERED
//   WHEN ASKED     a connection that drops waits for reconnect() - with a new
//                  Will if it brings one (a Sparkplug edge node's NDEATH
//                  carries a new bdSeq each session)
//
// One thread owns every client (the library's MqttClient, single-threaded and
// non-blocking); calls from the API reach it through a queue.

#include "AppHub.h"

#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct AppEndpoint {
    std::string host;
    uint16_t port = 0;           // 0: 1883, or 8883 with TLS
};

struct AppConnectionSpec {
    std::string id;
    std::vector<AppEndpoint> servers;
    std::string clientId;
    bool hasUsername = false;
    std::string username;
    bool hasPassword = false;
    std::string password;        // never read back
    bool tls = false;
    std::string caPem;           // "" the system's trust store
    std::string certificatePem, keyPem;   // mutual TLS; the key never read back
    uint8_t protocol = 5;
    uint32_t keepaliveSeconds = 30;
    bool cleanStart = false;
    uint32_t sessionExpirySeconds = 0;
    bool hasWill = false;
    AppPublish will;
    bool reconnectWhenAsked = false;
    uint32_t primaryCheckSeconds = 60;
};

enum class AppConnectionState : uint8_t { Connecting, Connected, Waiting, BackingOff, Refused };

struct AppConnectionInfo {
    AppConnectionSpec spec;      // as kept, the password and key left out
    AppConnectionState state = AppConnectionState::Connecting;
    uint32_t serverIndex = 0;
    bool sessionPresent = false;
    std::string lastError;
    int64_t sinceMs = 0;
    uint64_t messagesIn = 0, messagesOut = 0;
};

class AppConnections {
public:
    // `stateDirectory` holds connections.json (and any TLS material given as
    // text), readable by the server's own user only. "" keeps nothing.
    AppConnections(AppHub& hub, const std::string& stateDirectory);
    ~AppConnections();

    // Opens what was kept, and starts the thread.
    bool start(std::string& error);
    void stop();

    // Create, or replace by id: a changed spec reconnects; an unchanged one
    // does nothing.
    bool put(const AppConnectionSpec& spec, AppConnectionInfo* info, std::string& error);
    bool get(const std::string& id, AppConnectionInfo& info) const;
    std::vector<AppConnectionInfo> list() const;
    bool remove(const std::string& id);
    // Connect again now - waiting (when asked) or not - with `will` from this
    // connect on, when given.
    bool reconnect(const std::string& id, const AppPublish* will, AppConnectionInfo* info, std::string& error);

    // Every change of state of these connections (all, when `ids` is empty),
    // starting with each one's state now. Called on the manager's thread.
    uint64_t watch(const std::vector<std::string>& ids, std::function<void(const AppConnectionInfo&)> changed);
    void unwatch(uint64_t watcher);

    // From AppHub: a publish on a connection; a group on one opened or closed.
    void publish(const AppPublish& message, std::function<void(const AppPublishResult&)> done);
    void groupsChanged(const std::string& connection);

    struct Connection;

private:
    AppHub& _hub;
    std::string _directory;
    mutable std::mutex _lock;                 // the maps and the queue
    std::condition_variable _wake;
    std::map<std::string, std::unique_ptr<Connection>> _connections;
    std::vector<std::function<void()>> _commands;   // run on the manager's thread
    struct Watcher {
        std::vector<std::string> ids;
        std::function<void(const AppConnectionInfo&)> changed;
    };
    std::map<uint64_t, Watcher> _watchers;
    uint64_t _nextWatcher = 1;
    bool _quit = false;
    std::thread _thread;

    void run();
    void open(Connection& connection);
    void close(Connection& connection);
    void service(Connection& connection);
    void announce(Connection& connection);
    void save();
    bool load(std::string& error);
    void post(std::function<void()> command);
    // Runs `command` on the manager's thread and waits for it: every
    // connection is touched there and nowhere else.
    void call(std::function<void()> command) const;
    AppConnectionInfo infoOf(const Connection& connection) const;
};

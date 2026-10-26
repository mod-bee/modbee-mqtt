// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// MESSAGES IN AND OUT OF THE APPLICATION (Application API: Messages)
// =============================================================================
//
// A ShardExtension, so it is offered every message once, on the shard it was
// published on, with who published it - the way the Kafka pipe is. It keeps
// what the application's CONSUMER GROUPS have to be sent, and sends it:
//
//   ONE PUBLISHER, ONE MEMBER     a message goes to the member its publisher
//                                 hashes to (rendezvous hashing over the
//                                 members), so one device's messages arrive in
//                                 order at one instance; a member joining or
//                                 leaving moves only its own share
//   CONNECTED MEMBERS FIRST       a durable group with no member keeps what it
//                                 gets until one comes (`replay`)
//   NOTHING STAYS WITH ONE THAT   what a member had not acknowledged when its
//   WENT                          stream ended goes to the others (`redelivered`)
//   THE GROUP MUST TAKE IT        a group at its queue limit refuses: a QoS 1/2
//                                 publisher is told 0x97 and keeps the message
//
// And Publish: a message from the application, routed on a shard's own thread.
// Nothing here knows gRPC (ApiServer.cpp is the wire), so it is tested without.

#include "ShardedBroker.h"
#include "MqttOwnedProperties.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// A client event, from the server's own $modbee/events/... (BrokerEvents).
struct AppClientEvent {
    enum Kind : uint8_t { Unknown, Connected, Disconnected, Subscribed, Unsubscribed, ServerStarted };
    Kind kind = Unknown;
    std::string clientId, login, filter, address;
    int64_t timeMs = 0;
    bool resumed = false;
    uint32_t reasonCode = 0;
    uint32_t qos = 0;
};

// One message as the application is given it (app.proto Delivery).
struct AppMessage {
    std::string topic;
    std::string payload;
    uint8_t qos = 0;
    bool retain = false;
    MqttOwnedProperties properties;
    std::string clientId, login, tag, origin;    // the publisher
    int64_t brokerTimeMs = 0;
    std::string connection;                      // a managed connection's id; "" for the broker's own clients
    bool isEvent = false;                        // a client event, not a message
    AppClientEvent event;
};

enum class AppPick : uint8_t { Publisher, RoundRobin };
enum class AppOnFull : uint8_t { Refuse, Drop };

struct AppConsumeOptions {
    std::string group;                           // "": a stream of its own, nothing outlives it
    std::vector<std::string> filters;
    std::string connection;
    uint32_t maxInFlight = 100;
    AppPick pick = AppPick::Publisher;
    bool events = false;
    uint32_t expirySeconds = 86400;
    AppOnFull onFull = AppOnFull::Refuse;
    std::vector<std::string> excludeOrigins;
};

// One member: a Consume stream. deliver() is called with the hub's lock held
// and must neither block nor call back into the hub.
class AppConsumer {
public:
    virtual ~AppConsumer() {}
    virtual void deliver(uint64_t id, const AppMessage& message, bool replay, bool redelivered) = 0;
};

struct AppPublish {
    std::string topic;
    std::string payload;
    uint8_t qos = 0;
    bool retain = false;
    MqttOwnedProperties properties;
    std::string connection;
    std::string origin;
};

enum class AppPublishOutcome : uint8_t { Accepted, Delivered, NoSubscribers, Partial, Refused, NotConnected };

struct AppPublishResult {
    AppPublishOutcome outcome = AppPublishOutcome::Accepted;
    uint32_t reasonCode = 0;
    uint32_t matched = 0, acknowledged = 0, pending = 0;
};

struct AppGroupInfo {
    std::string group, connection;
    std::vector<std::string> filters;
    uint32_t members = 0;
    uint64_t queued = 0, inFlight = 0, refused = 0;
};

struct AppHubConfig {
    size_t maxQueued = 100000;                   // per group: past it, the group refuses
    bool eventsEnabled = false;                  // events: enabled - $modbee/events/... exist
};

struct AppHubStatus {
    size_t groups = 0, members = 0;
    uint64_t taken = 0, delivered = 0, acknowledged = 0, redelivered = 0, refused = 0, dropped = 0;
    uint64_t published = 0;
};

class AppHub : public ShardExtension {
public:
    explicit AppHub(const AppHubConfig& config);
    ~AppHub() override;

    // --- ShardExtension ---
    bool begin(size_t shards, std::string& error) override;
    void prepare(uint32_t shard, MqttBroker& broker) override;
    int waitHandle(uint32_t shard) override;
    uint32_t offer(uint32_t shard, MqttBroker& broker, const MqttBrokerRouted& message,
                   uint64_t deferral) override;
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t) override { return false; }
    void stop(uint32_t shard, MqttBroker& broker) override;

    // --- The application's side, from any thread ---

    // A member joins `options.group` (made if it does not exist). Null, with
    // `error`, when it cannot: the group exists with other filters, say.
    struct Member;
    Member* open(const AppConsumeOptions& options, AppConsumer* consumer, std::string& error,
                 uint32_t* members, uint64_t* queued);
    void ack(Member* member, const std::vector<uint64_t>& ids);
    void retry(Member* member, const std::vector<uint64_t>& ids, uint32_t afterMs);
    // Its stream ended: what it held goes to the others. `member` is gone after.
    void close(Member* member);

    void publish(const AppPublish& message, std::function<void(const AppPublishResult&)> done);

    std::vector<AppGroupInfo> groups() const;
    // False, with `why`, when there is no such group or it has members;
    // `discarded` what it still held.
    bool deleteGroup(const std::string& group, const std::string& connection, uint64_t* discarded,
                     std::string* why);

    AppHubStatus status() const;

    struct Group;
    struct Entry;
    struct Shard;

private:
    AppHubConfig _config;
    std::vector<std::unique_ptr<Shard>> _shards;
    std::atomic<size_t> _nextShard{0};

    // The groups, and their members: one lock for them all - a member's stream,
    // a shard offering a message and the timer all go through it.
    mutable std::mutex _lock;
    std::map<std::string, std::unique_ptr<Group>> _groups;    // by "<connection>\0<group>"
    uint64_t _nextPrivate = 1;

    std::condition_variable _tick;
    bool _quit = false;
    std::thread _timer;

    std::atomic<uint64_t> _taken{0}, _delivered{0}, _acknowledged{0}, _redelivered{0}, _refused{0};
    std::atomic<uint64_t> _dropped{0}, _published{0};

    bool take(Group& group, const std::shared_ptr<const AppMessage>& message);
    void assign(Group& group, std::shared_ptr<Entry> entry);
    void pump(Group& group, Member& member);
    void leave(Group& group, Member& member);
    void timers();
};

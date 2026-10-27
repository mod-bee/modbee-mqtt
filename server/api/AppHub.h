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
//   KEPT (sessions: store: file)  a named group and what it holds are on disk
//                                 (GroupStore) before a QoS 1/2 publisher's
//                                 PUBACK, and back after a restart
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
    enum Kind : uint8_t { Unknown, Connected, Disconnected, Subscribed, Unsubscribed, ServerStarted,
                          PublishRefused, SubscribeRefused, ConnectRefused };
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
    // On a managed connection, how it subscribes to each filter on the remote
    // broker: the MQTT 5 subscription options byte (QoS, no local, retain as
    // published, retain handling), one per filter in order. Missing: QoS 1.
    std::vector<uint8_t> subscribeOptions;
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
    // CONFIRM_DELIVERED: answered once every subscriber it reached has
    // acknowledged it (QoS 1/2), or at the deadline with what is still owed.
    bool confirmDelivered = false;
    uint32_t deadlineMs = 10000;
    uint64_t confirmId = 0;          // the hub's own, as it routes it
};

enum class AppPublishOutcome : uint8_t { Accepted, Delivered, NoSubscribers, Partial, Refused, NotConnected };

struct AppPublishResult {
    AppPublishOutcome outcome = AppPublishOutcome::Accepted;
    uint32_t reasonCode = 0;
    uint32_t matched = 0, acknowledged = 0, pending = 0;
};

// Filter i's subscription options byte (AppConsumeOptions::subscribeOptions).
uint8_t appSubscribeOptions(const AppConsumeOptions& options, size_t i);

struct AppGroupInfo {
    std::string group, connection;
    std::vector<std::string> filters;
    std::vector<uint8_t> subscribeOptions;       // one per filter
    uint32_t members = 0;
    uint64_t queued = 0, inFlight = 0, refused = 0;
};

struct AppHubConfig {
    size_t maxQueued = 100000;                   // per group: past it, the group refuses
    bool eventsEnabled = false;                  // events: enabled - $modbee/events/... exist
    std::string directory;                       // set: named groups are kept in <directory>/groups.db
    bool syncFull = false;                       // and every commit reaches the disk first
};

struct AppHubStatus {
    size_t groups = 0, members = 0;
    uint64_t taken = 0, delivered = 0, acknowledged = 0, redelivered = 0, refused = 0, dropped = 0;
    uint64_t published = 0;
    uint64_t restoredGroups = 0, restoredMessages = 0;   // put back at start
};

class AppConnections;
class GroupStore;

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

    // --- Managed connections (AppConnections) ---
    void attachConnections(AppConnections* connections);
    // The filters every group opened on `connection` asks for, together, each
    // with its subscription options: where two groups ask for one filter, the
    // higher QoS, no local only if both, retain as published if either, and
    // the retained messages sent if either wants them.
    std::map<std::string, uint8_t> filtersFor(const std::string& connection) const;
    // A message a managed connection received, for its groups.
    void connectionMessage(const std::string& connection, const std::shared_ptr<const AppMessage>& message);

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
    std::unique_ptr<GroupStore> _store;          // with a directory
    uint64_t _nextStored = 1;                    // under _lock
    uint64_t _restoredGroups = 0, _restoredMessages = 0;
    std::atomic<AppConnections*> _connections{nullptr};
    void groupsChanged(const std::string& connection);
    std::atomic<size_t> _nextShard{0};

    // The groups, and their members: one lock for them all - a member's stream,
    // a shard offering a message and the timer all go through it.
    mutable std::mutex _lock;
    std::map<std::string, std::unique_ptr<Group>> _groups;    // by "<connection>\0<group>"
    uint64_t _nextPrivate = 1;

    std::condition_variable _tick;
    bool _quit = false;
    std::thread _timer;

    // --- Publishes followed to their subscribers (CONFIRM_DELIVERED) ---
    typedef std::chrono::steady_clock Clock;
    struct Followed {
        size_t reports = 0, expected = 0;     // shards that have routed it, of how many
        uint32_t matched = 0, awaited = 0, acknowledged = 0, lost = 0;
        // CONFIRM_ACCEPTED: answered once every shard has routed it (how many it
        // matched), without waiting for any acknowledgement.
        bool acceptedOnly = false;
        Clock::time_point deadline;
        std::function<void(const AppPublishResult&)> done;
    };
    std::mutex _followLock;
    std::unordered_map<uint64_t, Followed> _followed;
    std::atomic<uint64_t> _nextConfirm{1};
    static void confirmed(void* context, uint64_t confirm, MqttBrokerConfirm what, uint32_t matched,
                          uint32_t awaited);
    static AppPublishResult resultOf(const Followed& followed, bool timedOut);
    void expireFollowed();

    std::atomic<uint64_t> _taken{0}, _delivered{0}, _acknowledged{0}, _redelivered{0}, _refused{0};
    std::atomic<uint64_t> _dropped{0}, _published{0};

    // `change`: the GroupStore change that keeps it, 0 when not kept.
    bool take(Group& group, const std::shared_ptr<const AppMessage>& message, uint64_t* change = nullptr);
    void keepGroup(const Group& group);
    void forgetGroup(const Group& group);
    void assign(Group& group, std::shared_ptr<Entry> entry);
    void pump(Group& group, Member& member);
    void leave(Group& group, Member& member);
    void timers();
};

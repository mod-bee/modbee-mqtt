// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// SESSIONS THAT OUTLIVE A RESTART
// =============================================================================
//
// A broker keeps its sessions in memory. Restarted - upgraded, its host
// rebooted, or killed - it comes back with none: every persistent session's
// subscriptions and every QoS 1 and 2 message queued for a device that was
// away are gone. `sessions: store: file` keeps them as well:
//
//   sessions:
//     store: file
//     directory: /var/lib/modbee/sessions
//     sync: normal          # or full: every write reaches the disk first
//
// WHAT IS KEPT. Every session with a Session Expiry Interval (a 3.1.1 Clean
// Session 0): its owner, expiry, Will, subscriptions, the QoS 1 and 2 messages
// owed to it - with the packet identifier one went out under and whether its
// PUBREC came - and the QoS 2 identifiers it is waiting to see a PUBREL for.
// And the Will of every connected client, whatever its expiry: a server that
// is killed did not see a DISCONNECT, so those Wills are due when it is back.
// QoS 0 messages are not kept [V5:4.1 allows it; nobody relies on it].
//
// HOW. The broker tells the keeper of every change (MqttSessionJournal). Each
// shard collects its changes during a pass and hands them to its own writer
// thread, which commits them to the shard's SQLite file in one transaction, so
// a shard never waits on the disk. A QoS 1 or 2 PUBLISH whose message was
// queued for a kept session is acknowledged only once that transaction is
// committed - on every shard it reached - so a message the publisher was told
// is taken is on disk. A commit in WAL mode survives the process being killed;
// `sync: full` also survives the power going.
//
// BOUNDED by what the broker itself holds: sessions, subscriptions and queue
// bytes have their limits in memory, and the files hold no more than that. A
// writer that falls behind holds acknowledgements back, which slows the
// publishers through their own flow control rather than growing a backlog.
//
// AT START every file is read; sessions that expired while the server was
// down are dropped, the rest are put back offline on the shard that owns their
// client identifier, with their expiry still counting. A different number of
// shards from last time is handled: the sessions are dealt out again and the
// files rewritten before the first client is accepted.

#include "ShardedBroker.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct sqlite3;

struct SessionKeeperConfig {
    std::string directory;       // sessions-<shards>-<shard>.db, one per shard
    bool syncFull = false;       // fsync every commit (power-cut safe), not only WAL
};

struct SessionKeeperStatus {
    uint64_t restored = 0;       // sessions put back at start
    uint64_t expired = 0;        // found at start, expired while the server was down
    uint64_t refused = 0;        // found at start, no room for them (broker.max-sessions)
    uint64_t willsDue = 0;       // Wills of clients connected when the server stopped
    uint64_t kept = 0;           // sessions in the files now (with a Will-only record)
    uint64_t commits = 0;        // transactions written
    uint64_t changes = 0;        // rows they wrote or deleted
    uint64_t failures = 0;       // transactions that could not be written
    uint64_t behind = 0;         // batches handed to writers and not yet committed
};

class SessionKeeper : public ShardExtension {
public:
    explicit SessionKeeper(const SessionKeeperConfig& config);
    ~SessionKeeper() override;

    bool begin(size_t shards, std::string& error) override;
    void prepare(uint32_t shard, MqttBroker& broker) override;
    int waitHandle(uint32_t shard) override;
    uint32_t offer(uint32_t shard, MqttBroker& broker, const MqttBrokerRouted& message,
                   uint64_t deferral) override;
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t shard) override;
    void stop(uint32_t shard, MqttBroker& broker) override;
    uint64_t forwardedHold(uint32_t shard) override;
    bool forwardedSettled(uint32_t shard, uint64_t hold) override;

    SessionKeeperStatus status() const;
    const std::string& directory() const { return _config.directory; }

    // One change, as a writer applies it. Public for the shard's journal.
    struct Change;
    struct Shard;

private:
    SessionKeeperConfig _config;
    std::vector<std::unique_ptr<Shard>> _shards;
    std::atomic<uint64_t> _restored{0}, _expired{0}, _refused{0}, _willsDue{0};

    bool load(size_t shards, std::vector<std::vector<MqttBrokerSessionImage>>& into,
              std::string& error);
};

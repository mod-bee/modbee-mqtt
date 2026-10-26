// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// RETAINED MESSAGES THAT OUTLIVE A RESTART
// =============================================================================
//
// Every broker built on the library keeps its retained messages in memory,
// and MQTT allows exactly that. A server is restarted - upgraded, moved, its
// host rebooted - and a store that comes back empty means every dashboard is
// blank until each device happens to publish again. So `modbee serve` can
// keep them somewhere else AS WELL:
//
//   retained:
//     store: file      a snapshot file, written a few seconds after a change
//                      and at shutdown
//     store: kafka     a compacted Kafka topic, written as each change happens
//
// HOW. The keeper is a ShardExtension. It is offered every message at the
// shard it was published on - once, not once per shard - and records the
// retained ones: a payload stored, or an empty one clearing the topic. At
// start it loads what was kept and puts it into every shard's store with
// MqttBroker::restoreRetained(), before the first client is accepted. Memory
// stays where SUBSCRIBEs are answered from; the backend is only written to.
//
// A Message Expiry Interval is kept as the moment it expires, so a message
// restored after a day's outage is not handed out with a day's life left.

#include "ShardedBroker.h"

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// One retained message as it is kept outside the broker.
struct RetainedRecord {
    std::string topic;
    std::string payload;
    uint8_t qos = 0;

    bool hasFormat = false;
    uint8_t format = 0;
    std::string contentType;
    std::string responseTopic;
    bool hasCorrelation = false;
    std::string correlation;
    std::vector<std::pair<std::string, std::string>> userProperties;

    // UTC milliseconds since 1970 when it expires, or 0 for never. The
    // interval it was published with, for a backend that stores that instead.
    uint64_t expiresAt = 0;
    uint32_t expiryInterval = 0;

    static RetainedRecord from(const MqttBrokerRouted& message, uint64_t nowUtc);

    // The properties to restore it with, the expiry as what remains. False
    // once it has expired. `properties` points into this record.
    bool describeTo(MqttProperties& properties, uint64_t nowUtc) const;
};

// UTC milliseconds since 1970.
uint64_t retainedNowUtc();

typedef std::map<std::string, RetainedRecord> RetainedSet;

// Where they are kept.
class RetainedBackend {
public:
    virtual ~RetainedBackend() {}

    // Before the server starts: everything kept. False refuses to start.
    virtual bool load(RetainedSet& into, std::string& error) = 0;

    // From any shard's thread, never blocking.
    virtual void stored(const RetainedRecord& record) = 0;
    virtual void cleared(const std::string& topic) = 0;

    // At shutdown: write out what is pending, within a few seconds.
    virtual void flush() = 0;

    // For the start-up line: "file /var/lib/modbee/retained.db".
    virtual std::string where() const = 0;

    // Writes that failed, for the status line.
    virtual uint64_t failures() const = 0;
};

// The snapshot file. Its directory must exist and be writable.
std::unique_ptr<RetainedBackend> makeRetainedFile(const std::string& path);

class RetainedKeeper : public ShardExtension {
public:
    explicit RetainedKeeper(std::unique_ptr<RetainedBackend> backend);

    bool begin(size_t shards, std::string& error) override;
    void prepare(uint32_t shard, MqttBroker& broker) override;
    int waitHandle(uint32_t) override { return -1; }
    uint32_t offer(uint32_t shard, MqttBroker& broker, const MqttBrokerRouted& message,
                   uint64_t deferral) override;
    void service(uint32_t, MqttBroker&) override {}
    bool hasPendingWork(uint32_t) override { return false; }
    void stop(uint32_t shard, MqttBroker& broker) override;

    std::string where() const { return _backend->where(); }
    size_t loaded() const { return _loaded.size(); }
    // Loaded but not restored: expired while the server was down, or more
    // than broker.max-retained.
    uint64_t expired() const { return _expired; }
    uint64_t refused() const { return _refused.load(); }
    uint64_t failures() const { return _backend->failures(); }

private:
    std::unique_ptr<RetainedBackend> _backend;
    RetainedSet _loaded;
    uint64_t _expired = 0;
    std::atomic<uint64_t> _refused{0};
    size_t _shards = 0;
    std::atomic<size_t> _stopped{0};
};

// Several extensions on one server, in order: each is offered every message,
// and the PUBACK waits for every party any of them reports. Their wait handles
// are gathered into one epoll descriptor per shard, the shard's one slot; a
// forwarded message's report waits for every member's hold.
class ShardExtensionChain : public ShardExtension {
public:
    ~ShardExtensionChain() override;
    void add(ShardExtension* extension) { _chain.push_back(extension); }
    bool empty() const { return _chain.empty(); }

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

private:
    std::vector<ShardExtension*> _chain;
    // Per shard, touched only on that shard's thread once it runs.
    struct PerShard {
        int epoll = -1;
        uint64_t nextHold = 1;
        // A hold of the chain's own, and the members' holds behind it.
        std::vector<std::pair<uint64_t, std::vector<std::pair<size_t, uint64_t>>>> holds;
    };
    std::vector<PerShard> _perShard;
};

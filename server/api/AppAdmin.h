// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// AN ADMINISTRATOR'S REQUESTS, CARRIED OUT ON THE SHARDS (Application API: Admin)
// =============================================================================
//
// A broker is single-threaded: what an administrator asks of the sessions or
// the retained messages is done on each shard's own thread, between two passes
// of its loop, and gathered. A ShardExtension that offers nothing and holds
// nothing: a queue per shard and the eventfd that wakes it.

#include "ShardedBroker.h"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

class AppAdmin : public ShardExtension {
public:
    AppAdmin();
    ~AppAdmin() override;

    bool begin(size_t shards, std::string& error) override;
    void prepare(uint32_t shard, MqttBroker& broker) override;
    int waitHandle(uint32_t shard) override;
    uint32_t offer(uint32_t, MqttBroker&, const MqttBrokerRouted&, uint64_t) override { return 0; }
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t) override { return false; }
    void stop(uint32_t shard, MqttBroker& broker) override;

    size_t shards() const { return _shards.size(); }

    // `work` on every shard's thread, then `done` once, on the last one's.
    void everyShard(std::function<void(uint32_t shard, MqttBroker& broker)> work, std::function<void()> done);
    // `work` on one shard's thread.
    void oneShard(uint32_t shard, std::function<void(MqttBroker& broker)> work);

    struct Shard;

private:
    std::vector<std::unique_ptr<Shard>> _shards;
};

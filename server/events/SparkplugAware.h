// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// A SPARKPLUG AWARE MQTT SERVER (Sparkplug B 3.0, section 10)
// =============================================================================
//
// Every NBIRTH and DBIRTH a client publishes is kept, retained, on
//
//   $sparkplug/certificates/spBv1.0/<group>/NBIRTH/<node>
//   $sparkplug/certificates/spBv1.0/<group>/DBIRTH/<node>/<device>
//
// so a host that starts late reads every node's dictionary without asking for
// a single rebirth. A death clears its certificate - an NDEATH its node's and
// every one of its devices' - with an empty retained message. The bytes are
// the birth's own. Nobody publishes there but the server: a login reads them
// (the login database grants every login that; an application deciding logins
// names them in its Scope), and no rule lets one write them.
//
// One message in, at most a few out, on the shard it was published on - a
// node's client lives on one shard, so its devices are known there. Turned on
// and off while the server runs; off, it clears what it put there.

#include "ShardedBroker.h"

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

class SparkplugAware : public ShardExtension {
public:
    explicit SparkplugAware(bool enabled = false);
    ~SparkplugAware() override;

    void enable(bool on);
    bool enabled() const { return _enabled.load(); }
    uint64_t kept() const { return _kept.load(); }

    // "$sparkplug/certificates/..." - a topic only the server writes.
    static bool isCertificate(const std::string& topic);

    bool begin(size_t shards, std::string& error) override;
    int waitHandle(uint32_t) override { return -1; }
    uint32_t offer(uint32_t shard, MqttBroker& broker, const MqttBrokerRouted& message, uint64_t deferral) override;
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t shard) override;
    void stop(uint32_t shard, MqttBroker& broker) override { service(shard, broker); }

    struct Shard;

private:
    std::atomic<bool> _enabled;
    std::atomic<uint64_t> _kept{0};
    std::vector<std::unique_ptr<Shard>> _shards;
};

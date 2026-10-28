// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// THE KAFKA PIPE: standard MQTT to and from Apache Kafka, untouched.
//
// internal/CLOUD-PLAN.md section 6 is the design; docs/kafka.md is the manual.
// In one paragraph: every message that originates on a shard is matched
// against the mqtt-to-kafka mappings and produced by THAT shard's producer, so
// each message is produced exactly once and its delivery report concludes the
// held PUBACK on the thread that owns it. kafka-to-mqtt mappings each have a
// consumer, owned by one shard, whose records are published into the broker
// like any local message - and committed only once the broker has taken them.
//
// librdkafka's background threads never touch broker state: they only write to
// a WakeSignal in the shard's readiness set, and the shard runs every callback from
// its own loop.
//
// LIVE (apply()): the clusters and mappings are a PLAN, replaced whole. Each
// shard takes a new plan on its own thread, at its next message or service
// pass, and changes only the clients it touches: a producer is kept unless its
// cluster's settings, its idempotence, or a Kafka topic's acks or partitioner
// changed; a consumer unless its cluster, topics or group did. A producer that
// is replaced takes no more records and is destroyed once Kafka has answered
// every one it holds - so a PUBACK already waiting on Kafka is still sent, by
// the delivery report it was waiting for. A consumer is closed without
// blocking the shard, committing what it stored.

#include "KafkaControl.h"
#include "ShardedBroker.h"
#include "ServerConfig.h"

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// The pipe's counters, appended in the Prometheus text format, one series
// per mapping (label mapping="<id>").
void renderKafkaMetrics(const KafkaPipeStatus& status, std::string& out);

class KafkaPipe : public ShardExtension, public KafkaLive {
public:
    // `config`'s clusters and mappings are the first plan (they may be none).
    explicit KafkaPipe(const ServerConfig& config);
    ~KafkaPipe() override;

    KafkaPipe(const KafkaPipe&) = delete;
    KafkaPipe& operator=(const KafkaPipe&) = delete;

    // How long stop() waits for Kafka to acknowledge what is in flight.
    void setShutdownFlushMillis(int millis) { _flushMillis = millis; }

    bool begin(size_t shards, std::string& error) override;
    int waitHandle(uint32_t shard) override;
    uint32_t offer(uint32_t shard, MqttBroker& broker, const MqttBrokerRouted& message,
                   uint64_t deferral) override;
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t shard) override;
    void stop(uint32_t shard, MqttBroker& broker) override;

    // A new plan, checked already (KafkaControl). Every client it needs is
    // tried first, so a cluster a client cannot be made for (a TLS file that
    // is not there, a librdkafka setting it refuses) is an error, and the old
    // plan runs on.
    bool apply(const ServerConfig& section, std::string& error) override;
    KafkaPipeStatus status() const override;

    // librdkafka's version string, for a log line.
    static const char* libraryVersion();
    // What that librdkafka was built with ("ssl, sasl_scram, zstd, ..."):
    // without ssl it cannot reach a cloud cluster at all.
    static std::string libraryFeatures();

    struct Shard;
    struct ToKafka;
    struct FromKafka;
    struct Plan;

private:
    std::shared_ptr<const Plan> makePlan(const ServerConfig& section);
    std::shared_ptr<const Plan> currentPlan(uint64_t& generation) const;

    int _flushMillis = 5000;
    size_t _shardCount = 0;
    mutable std::mutex _planLock;
    std::shared_ptr<const Plan> _plan;                 // under _planLock
    std::atomic<uint64_t> _generation{1};
    // Each mapping's counters by id: a mapping changed keeps its own.
    std::map<std::string, std::shared_ptr<KafkaMappingCounters>> _counters;   // under _planLock
    std::vector<std::unique_ptr<Shard>> _shards;
    std::atomic<bool> _brokersDown{false};
};

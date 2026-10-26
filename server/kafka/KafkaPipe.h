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

#include "ShardedBroker.h"
#include "ServerConfig.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

// Counters, named as HiveMQ's Kafka extension names them so an operator who
// knows one knows the other. Running totals; read at any time from any thread.
struct KafkaMappingCounters {
    std::atomic<uint64_t> send{0};        // mqtt-to-kafka: handed to the producer
    std::atomic<uint64_t> success{0};     //   acknowledged by Kafka
    std::atomic<uint64_t> failed{0};      //   refused by Kafka or the producer
    std::atomic<uint64_t> tooLarge{0};    //   over max-record-bytes, never sent
    std::atomic<uint64_t> latencyMicros{0};   // sum, for success; / success = mean
    std::atomic<uint64_t> polled{0};      // kafka-to-mqtt: records read
    std::atomic<uint64_t> published{0};   //   published into the broker
    std::atomic<uint64_t> invalid{0};     //   could not be published (bad topic, ...)
    std::atomic<uint64_t> refused{0};     //   the broker could not take it (a queue or the retained store full)
};

struct KafkaPipeStatus {
    struct Mapping {
        std::string id;
        bool toKafka = true;
        uint64_t send = 0, success = 0, failed = 0, tooLarge = 0, latencyMicros = 0;
        uint64_t polled = 0, published = 0, invalid = 0, refused = 0;
    };
    std::vector<Mapping> mappings;
    uint64_t heldAcknowledgements = 0;    // PUBACK/PUBRECs waiting on Kafka now
    // False from librdkafka's "all brokers down" (or statistics showing no
    // broker UP) until a statistics report shows one UP again, or Kafka
    // acknowledges a record - what /readyz reports.
    bool kafkaReachable = true;
};

// The pipe's counters, appended in the Prometheus text format, one series
// per mapping (label mapping="<id>").
void renderKafkaMetrics(const KafkaPipeStatus& status, std::string& out);

class KafkaPipe : public ShardExtension {
public:
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

    KafkaPipeStatus status() const;

    // librdkafka's version string, for a log line.
    static const char* libraryVersion();
    // What that librdkafka was built with ("ssl, sasl_scram, zstd, ..."):
    // without ssl it cannot reach a cloud cluster at all.
    static std::string libraryFeatures();

    struct Shard;
    struct ToKafka;
    struct FromKafka;

private:
    ServerConfig _config;
    int _flushMillis = 5000;
    std::vector<std::unique_ptr<ToKafka>> _toKafka;
    std::vector<std::unique_ptr<FromKafka>> _fromKafka;
    std::vector<std::unique_ptr<Shard>> _shards;
    std::atomic<bool> _brokersDown{false};
};

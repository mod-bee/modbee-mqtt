// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// THE KAFKA SECTION, CHANGED WHILE THE SERVER RUNS.
//
// The clusters and mappings the pipe runs, as the Application API's Kafka
// service changes them: one at a time, each change all or nothing. A change
// is written back as text and read by the same loader as modbee.yaml
// (renderKafkaSection, loadKafkaSection), so it meets exactly the file's
// rules; then the pipe takes it (KafkaLive::apply), and only then is it kept,
// in the state directory as kafka.json.
//
// WHERE THEY START: kafka.json when there is one, else the file's kafka:. What
// the API changed wins over the file, as EMQX keeps what its API changed over
// its configuration file - a server started from flags alone keeps its Kafka
// mappings, and a file edited later does not silently undo a change made live.
//
// No librdkafka here: a build without Kafka has this too, and says so.

#include "ServerConfig.h"

#include <atomic>
#include <mutex>
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

// What runs a kafka: section: the pipe, where librdkafka is.
class KafkaLive {
public:
    virtual ~KafkaLive() {}
    // The clusters and mappings to run from now on, already checked. False,
    // with why, when a client for them cannot be made - and then the old ones
    // run on.
    virtual bool apply(const ServerConfig& section, std::string& error) = 0;
    virtual KafkaPipeStatus status() const = 0;
};

class KafkaControl {
public:
    // `file`: the configuration, whose kafka: is where the section starts.
    // `stateDirectory`: where kafka.json is kept ("" keeps nothing).
    // `retainedCluster`: the cluster the retained store writes to, which
    // cannot be deleted while it does. `live`: null in a build without Kafka.
    KafkaControl(const ServerConfig& file, const std::string& stateDirectory,
                 const std::string& retainedCluster, KafkaLive* live = nullptr);
    // The pipe, made from section() once load() has said where it starts.
    void attach(KafkaLive* live) { _live = live; }

    // kafka.json read, when there is one: the section the pipe should start
    // with. False, with why, when it cannot be read or is not valid.
    bool load(std::string& error);
    // The section, for the pipe at start (before apply is possible).
    ServerConfig section() const;

    bool available() const { return _live != nullptr; }

    enum class Result { Ok, NotFound, Invalid, InUse, Unavailable, Failed };

    // Create, or replace by id. A cluster given with no password keeps the
    // one it has. A mapping replaces one of either direction with its id.
    Result putCluster(const KafkaClusterConfig& cluster, KafkaClusterConfig* stored, std::string& error);
    Result putToKafka(const MqttToKafkaConfig& mapping, MqttToKafkaConfig* stored, std::string& error);
    Result putFromKafka(const KafkaToMqttConfig& mapping, KafkaToMqttConfig* stored, std::string& error);
    Result deleteCluster(const std::string& id, std::string& error);
    Result deleteMapping(const std::string& id, std::string& error);

    KafkaPipeStatus status() const;

private:
    Result change(ServerConfig next, std::string& error);
    bool save(const ServerConfig& section, std::string& error) const;

    mutable std::mutex _lock;
    ServerConfig _section;                 // clusters, toKafka and fromKafka only
    std::string _path;                     // <state>/kafka.json, or ""
    std::string _retainedCluster;
    KafkaLive* _live;
};

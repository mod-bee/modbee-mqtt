// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaPipe.h"
#include "KafkaCommon.h"
#include "KafkaEnvelope.h"

#include "MqttTopic.h"

#include <librdkafka/rdkafka.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <set>

#include "WakeSignal.h"

namespace {

// librdkafka wakes the shard when a queue has something: on Linux by
// writing to the shard's eventfd itself, elsewhere through a callback (its
// descriptor event needs a POSIX descriptor or, on Windows, its own kind of
// socket).
#if defined(_WIN32)
void signalShard(rd_kafka_t*, void* wake) { static_cast<WakeSignal*>(wake)->signal(); }
#endif

void wakeOnEvents(rd_kafka_queue_t* queue, WakeSignal& wake) {
#if defined(_WIN32)
    rd_kafka_queue_cb_event_enable(queue, &signalShard, &wake);
#else
    static const uint64_t kOne = 1;
    rd_kafka_queue_io_event_enable(queue, wake.handle(), &kOne, sizeof kOne);
#endif
}

// MQTT 5 reason codes the pipe answers a held PUBACK/PUBREC with.
const uint8_t kQuotaExceeded = 0x97;          // Kafka could not take it now
const uint8_t kImplementationError = 0x83;    // too large for this mapping, ever

// Records read from one consumer per pass, so a backlog cannot starve the
// shard's own connections. The rest wait a pass.
const int kConsumeBudget = 512;

// How often librdkafka's queues are served even with no wake-up: its own
// timers (retries, metadata refresh) post events that must not wait long.
const mq_time_t kServiceIntervalMs = 100;

// Where a message published from Kafka came from: carried through the broker
// as MqttBrokerRouted::origin, so an mqtt-to-kafka mapping never writes it
// back to the topic it was read from.
struct KafkaOrigin {
    uint32_t magic = 0x6B61666Bu;   // "kafk"
    const std::string* cluster = nullptr;   // its id
    const char* topic = nullptr;
};

std::string toText(const MqttSpan& span) {
    return std::string((const char*)span.data, span.length);
}

MqttSpan spanOf(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

// ${MQTT_TOPIC}, ${MQTT_CLIENTID}, ${MQTT_TOPIC_LEVEL_n}
std::string expandKey(const std::string& pattern, const MqttSpan& topic, const MqttSpan& clientId) {
    std::string out;
    size_t at = 0;
    while (at < pattern.size()) {
        const size_t open = pattern.find("${", at);
        if (open == std::string::npos) { out.append(pattern, at, std::string::npos); break; }
        out.append(pattern, at, open - at);
        const size_t close = pattern.find('}', open);
        // An unclosed "${" is text: without this, at = npos + 1 = 0 and the
        // loop began again from the start, for ever.
        if (close == std::string::npos) { out.append(pattern, open, std::string::npos); break; }
        const std::string name = pattern.substr(open + 2, close - open - 2);
        if (name == "MQTT_TOPIC") {
            out.append((const char*)topic.data, topic.length);
        } else if (name == "MQTT_CLIENTID") {
            out.append((const char*)clientId.data, clientId.length);
        } else if (name.compare(0, 17, "MQTT_TOPIC_LEVEL_") == 0) {
            const size_t want = (size_t)std::strtoul(name.c_str() + 17, nullptr, 10);
            size_t level = 0, start = 0;
            for (size_t i = 0; i <= topic.length; i++) {
                if (i == topic.length || topic.data[i] == '/') {
                    if (level == want) {
                        out.append((const char*)topic.data + start, i - start);
                        break;
                    }
                    level++;
                    start = i + 1;
                }
            }
        }
        at = close + 1;
    }
    return out;
}

// ${KAFKA_TOPIC}, ${KAFKA_KEY}
std::string expandTopic(const std::string& pattern, const char* kafkaTopic,
                        const void* key, size_t keyLength) {
    std::string out;
    size_t at = 0;
    while (at < pattern.size()) {
        const size_t open = pattern.find("${", at);
        if (open == std::string::npos) { out.append(pattern, at, std::string::npos); break; }
        out.append(pattern, at, open - at);
        const size_t close = pattern.find('}', open);
        // An unclosed "${" is text: without this, at = npos + 1 = 0 and the
        // loop began again from the start, for ever.
        if (close == std::string::npos) { out.append(pattern, open, std::string::npos); break; }
        const std::string name = pattern.substr(open + 2, close - open - 2);
        if (name == "KAFKA_TOPIC") out.append(kafkaTopic);
        else if (key != nullptr) out.append((const char*)key, keyLength);
        at = close + 1;
    }
    return out;
}

} // namespace

// --- The plan: the clusters and mappings, replaced whole ---------------------

struct KafkaPipe::ToKafka {
    MqttToKafkaConfig config;
    std::shared_ptr<KafkaMappingCounters> counters;
};

struct KafkaPipe::FromKafka {
    KafkaToMqttConfig config;
    std::shared_ptr<KafkaMappingCounters> counters;
    uint32_t owner = 0;            // the shard whose loop runs its consumer
};

struct KafkaPipe::Plan {
    std::vector<KafkaClusterConfig> clusters;
    std::vector<std::shared_ptr<ToKafka>> toKafka;
    std::vector<std::shared_ptr<FromKafka>> fromKafka;
    const KafkaClusterConfig* cluster(const std::string& id) const {
        for (const KafkaClusterConfig& c : clusters) if (c.id == id) return &c;
        return nullptr;
    }
};

namespace {

// Everything about a cluster a client is made from: two clusters with the
// same signature need no new client.
std::string clusterSignature(const KafkaClusterConfig& c) {
    std::string s = c.id + '\n' + c.bootstrapServers + '\n' + (c.tls ? "tls" : "plain") + '\n' + c.caFile + '\n' +
                    c.certificateFile + '\n' + c.keyFile + '\n' + (c.verifyHostname ? "verify" : "trust") + '\n' +
                    c.mechanism + '\n' + c.username + '\n' + c.password + '\n' + c.passwordFile + '\n';
    for (const auto& p : c.properties) s += p.first + '=' + p.second + '\n';
    return s;
}

// The shard owning a consumer: by its id, so a change to other mappings does
// not move it.
uint32_t ownerOf(const std::string& id, size_t shards) {
    return shards == 0 ? 0 : (uint32_t)(std::hash<std::string>()(id) % shards);
}

const char* acksText(int acks) { return acks == 0 ? "0" : acks == 1 ? "1" : "-1"; }

rd_kafka_conf_t* producerConf(const KafkaClusterConfig& cluster, bool allAcks, const std::string& clientId,
                              std::string& error) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    // Idempotence keeps a retried send from being written twice and keeps
    // order within a partition through retries. Kafka allows it only with
    // acks=all, so a cluster with any mapping on fewer acks goes without -
    // and docs/kafka.md says what that costs.
    const bool ok =
        kafkaClusterSettings(conf, cluster, clientId, error) &&
        kafkaSet(conf, "enable.idempotence", allAcks ? "true" : "false", error) &&
        kafkaSet(conf, "linger.ms", "5", error) &&
        kafkaSet(conf, "compression.type", "lz4", error) &&
        // The Java client's partitioner, so a key lands on the same partition
        // whether modbee or a Java producer wrote it.
        kafkaSet(conf, "partitioner", "murmur2_random", error) &&
        // How long a record may wait for Kafka before it is given up on - and
        // with it, how long a publisher's PUBACK can be held. librdkafka's
        // default is five minutes, far beyond the point where a device has
        // given up waiting (this library's client: 60 s). Thirty seconds rides
        // out a broker restart and still answers inside the client's
        // patience. The file can change it.
        kafkaSet(conf, "message.timeout.ms", "30000", error) &&
        // What may wait for Kafka, in memory. librdkafka's default is a
        // gigabyte, per producer - so a Kafka that stalled while devices
        // published large payloads took a broker in a 256 MB container past
        // its limit, and the kernel killed it. A full queue is answered, not
        // waited out (offer()). The file can change it.
        kafkaSet(conf, "queue.buffering.max.kbytes", "65536", error) &&
        // Broker state for /readyz (onStatistics). The file can change it.
        kafkaSet(conf, "statistics.interval.ms", "5000", error) &&
        kafkaPassThrough(conf, cluster, error);
    if (!ok) {
        rd_kafka_conf_destroy(conf);
        error = "kafka cluster " + cluster.id + ": " + error;
        return nullptr;
    }
    return conf;
}

rd_kafka_conf_t* consumerConf(const KafkaClusterConfig& cluster, const KafkaToMqttConfig& mapping,
                              std::string& error) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    // At-least-once: offsets are STORED only once the broker has taken the
    // record, and committed (automatically) from the store.
    const bool ok =
        kafkaClusterSettings(conf, cluster, "modbee-" + mapping.id, error) &&
        kafkaSet(conf, "group.id", mapping.group, error) &&
        kafkaSet(conf, "enable.auto.commit", "true", error) &&
        kafkaSet(conf, "enable.auto.offset.store", "false", error) &&
        kafkaSet(conf, "auto.offset.reset", "latest", error) &&
        // Consumer lag, for status (onConsumerStatistics). The file can change it.
        kafkaSet(conf, "statistics.interval.ms", "5000", error) &&
        kafkaPassThrough(conf, cluster, error);
    if (!ok) {
        rd_kafka_conf_destroy(conf);
        error = "kafka mapping " + mapping.id + ": " + error;
        return nullptr;
    }
    return conf;
}

// The topic-level settings a mapping asks for, from the producer's defaults:
// the topic-level settings the file passed through (message.timeout.ms,
// compression per topic) live there, and a fresh one would silently drop
// every one of them.
rd_kafka_topic_conf_t* topicConf(rd_kafka_t* producer, const MqttToKafkaConfig& mapping, std::string& error) {
    rd_kafka_topic_conf_t* conf = rd_kafka_default_topic_conf_dup(producer);
    char why[512];
    if (rd_kafka_topic_conf_set(conf, "acks", acksText(mapping.acks), why, sizeof why) != RD_KAFKA_CONF_OK ||
        rd_kafka_topic_conf_set(conf, "partitioner", mapping.uniform ? "random" : "murmur2_random", why,
                                sizeof why) != RD_KAFKA_CONF_OK) {
        rd_kafka_topic_conf_destroy(conf);
        error = "kafka mapping " + mapping.id + ": " + why;
        return nullptr;
    }
    if (mapping.messageTimeoutMs >= 0 &&
        rd_kafka_topic_conf_set(conf, "message.timeout.ms", std::to_string(mapping.messageTimeoutMs).c_str(), why,
                                sizeof why) != RD_KAFKA_CONF_OK) {
        rd_kafka_topic_conf_destroy(conf);
        error = "kafka mapping " + mapping.id + ": " + why;
        return nullptr;
    }
    return conf;
}

} // namespace

// --- One shard's clients -----------------------------------------------------

struct KafkaPipe::Shard {
    KafkaPipe* pipe = nullptr;
    uint32_t index = 0;
    WakeSignal wake;
    MqttBroker* broker = nullptr;          // set on every call from the shard's thread
    mq_time_t servedAt = 0;
    bool backlog = false;                  // a consumer had more than its budget
    std::atomic<uint64_t> held{0};         // acknowledgements waiting on Kafka

    // One Kafka topic on one producer. Its delivery reports count against the
    // mapping that writes to it, kept alive here while its records are out.
    struct Topic {
        std::string name;
        int acks = -1;
        bool uniform = false;
        rd_kafka_topic_t* handle = nullptr;
        std::shared_ptr<ToKafka> owner;
    };
    struct Producer {
        std::string clusterId;
        std::string signature;
        rd_kafka_t* handle = nullptr;
        std::vector<std::unique_ptr<Topic>> topics;
        bool retiring = false;             // takes no more; destroyed once Kafka has answered it all
        Topic* topic(const std::string& name) {
            for (std::unique_ptr<Topic>& t : topics) if (t->name == name) return t.get();
            return nullptr;
        }
    };
    struct Consumer {
        std::string signature;
        std::shared_ptr<FromKafka> from;
        rd_kafka_t* handle = nullptr;
        rd_kafka_queue_t* closing = nullptr;   // being closed, without blocking the shard
    };

    // The plan this shard runs, and what it made for it. route[i] is the
    // topic plan->toKafka[i] produces to (null: its cluster has no client).
    std::shared_ptr<const Plan> plan;
    uint64_t generation = 0;
    std::vector<std::unique_ptr<Producer>> producers;
    std::vector<Topic*> route;
    std::vector<Producer*> routeProducer;
    std::vector<std::unique_ptr<Consumer>> consumers;

    static void onDelivery(rd_kafka_t*, const rd_kafka_message_t* message, void* opaque);
    static void onError(rd_kafka_t*, int error, const char* reason, void* opaque);
    static int onStatistics(rd_kafka_t*, char* json, size_t length, void* opaque);
    static int onConsumerStatistics(rd_kafka_t*, char* json, size_t length, void* opaque);
    // Takes the pipe's current plan, if it is not the one running.
    bool reconcile(std::string* error);
    Producer* makeProducer(const KafkaClusterConfig& cluster, const std::string& signature, bool allAcks,
                           std::string& error);
    bool makeConsumer(Consumer& consumer, const KafkaClusterConfig& cluster, std::string& error);
    void beginClose(Consumer& consumer);
    void retire();
    void consume(Consumer& consumer);
    void publishRecord(FromKafka& mapping, const rd_kafka_message_t* record);
};

// librdkafka's own errors. Only "every broker is unreachable" changes what the
// pipe reports (/readyz): the rest it retries by itself.
void KafkaPipe::Shard::onError(rd_kafka_t*, int error, const char* reason, void* opaque) {
    Shard& shard = *static_cast<Shard*>(opaque);
    if (error == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
        if (!shard.pipe->_brokersDown.exchange(true)) {
            std::fprintf(stderr, "kafka: every broker is unreachable (%s) - publishers' acknowledgements wait\n",
                         reason ? reason : "");
        }
    }
}

// librdkafka's statistics, every few seconds: a broker in state "UP" means
// Kafka can be reached. This is what turns /readyz back to ready after an
// outage on a server with no traffic (a delivery would also, but there may be
// none). A substring test, not a parse: the document is librdkafka's own.
int KafkaPipe::Shard::onStatistics(rd_kafka_t*, char* json, size_t length, void* opaque) {
    Shard& shard = *static_cast<Shard*>(opaque);
    const std::string stats(json, length);
    const bool up = stats.find("\"state\":\"UP\"") != std::string::npos;
    shard.pipe->_brokersDown.store(!up, std::memory_order_relaxed);
    return 0;   // librdkafka frees the document
}

// A consumer's statistics, inside rd_kafka_consumer_poll() on the shard's own
// thread: its lag is the sum of every assigned partition's consumer_lag (-1,
// not known, for one it has no position in yet). A scan, not a parse, as above.
int KafkaPipe::Shard::onConsumerStatistics(rd_kafka_t*, char* json, size_t length, void* opaque) {
    const Consumer& consumer = *static_cast<const Consumer*>(opaque);
    const std::string stats(json, length);
    static const char kLag[] = "\"consumer_lag\":";
    int64_t lag = 0;
    bool known = false;
    for (size_t at = stats.find(kLag); at != std::string::npos; at = stats.find(kLag, at + 1)) {
        const long long one = std::strtoll(stats.c_str() + at + sizeof kLag - 1, nullptr, 10);
        if (one < 0) continue;
        lag += one;
        known = true;
    }
    consumer.from->counters->lag.store(known ? lag : -1, std::memory_order_relaxed);
    return 0;
}

// Runs inside rd_kafka_poll(), on the shard's own thread.
void KafkaPipe::Shard::onDelivery(rd_kafka_t*, const rd_kafka_message_t* message, void* opaque) {
    Shard& shard = *static_cast<Shard*>(opaque);
    const Topic& topic = *static_cast<const Topic*>(rd_kafka_topic_opaque(message->rkt));
    KafkaMappingCounters& counters = *topic.owner->counters;
    const uint64_t deferral = (uint64_t)(uintptr_t)message->_private;
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        shard.pipe->_brokersDown.store(false, std::memory_order_relaxed);
        counters.success.fetch_add(1, std::memory_order_relaxed);
        const int64_t latency = rd_kafka_message_latency(message);
        if (latency > 0) counters.latencyMicros.fetch_add((uint64_t)latency, std::memory_order_relaxed);
    } else {
        counters.failed.fetch_add(1, std::memory_order_relaxed);
    }
    if (deferral == 0) return;
    shard.held.fetch_sub(1, std::memory_order_relaxed);
    if (shard.broker == nullptr) return;    // shutting down: nobody to tell
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        shard.broker->concludeDeferred(deferral, 1, 0);
    } else {
        const uint8_t reason = message->err == RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE
                                   ? kImplementationError : kQuotaExceeded;
        shard.broker->concludeDeferred(deferral, 1, 1, reason);
    }
}

const char* KafkaPipe::libraryVersion() { return rd_kafka_version_str(); }

std::string KafkaPipe::libraryFeatures() {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char features[512] = "";
    size_t size = sizeof features;
    if (rd_kafka_conf_get(conf, "builtin.features", features, &size) != RD_KAFKA_CONF_OK) features[0] = 0;
    rd_kafka_conf_destroy(conf);
    std::string out(features);
    for (char& c : out) if (c == ',') c = ' ';
    return out;
}

// --- Construction ------------------------------------------------------------

KafkaPipe::KafkaPipe(const ServerConfig& config) {
    _plan = makePlan(config);
}

KafkaPipe::~KafkaPipe() {
    for (std::unique_ptr<Shard>& shard : _shards) {
        shard->broker = nullptr;
        for (auto& consumer : shard->consumers) {
            if (consumer->closing) rd_kafka_queue_destroy(consumer->closing);
            if (consumer->handle) rd_kafka_destroy(consumer->handle);
        }
        for (auto& producer : shard->producers) {
            for (auto& topic : producer->topics) rd_kafka_topic_destroy(topic->handle);
            rd_kafka_destroy(producer->handle);
        }
        shard->wake.close();
    }
}

std::shared_ptr<const KafkaPipe::Plan> KafkaPipe::makePlan(const ServerConfig& section) {
    // Under _planLock, or before anything else can see the pipe.
    std::shared_ptr<Plan> plan(new Plan());
    plan->clusters = section.clusters;
    std::set<std::string> ids;
    const auto countersFor = [&](const std::string& id) {
        ids.insert(id);
        std::shared_ptr<KafkaMappingCounters>& counters = _counters[id];
        if (!counters) counters.reset(new KafkaMappingCounters());
        return counters;
    };
    for (const MqttToKafkaConfig& m : section.toKafka) {
        std::shared_ptr<ToKafka> to(new ToKafka());
        to->config = m;
        to->counters = countersFor(m.id);
        plan->toKafka.push_back(to);
    }
    for (const KafkaToMqttConfig& m : section.fromKafka) {
        std::shared_ptr<FromKafka> from(new FromKafka());
        from->config = m;
        from->counters = countersFor(m.id);
        from->owner = ownerOf(m.id, _shardCount);
        plan->fromKafka.push_back(from);
    }
    // A mapping deleted takes its counters with it; made again, it counts
    // from zero.
    for (auto at = _counters.begin(); at != _counters.end();) {
        at = ids.count(at->first) ? std::next(at) : _counters.erase(at);
    }
    return plan;
}

std::shared_ptr<const KafkaPipe::Plan> KafkaPipe::currentPlan(uint64_t& generation) const {
    std::lock_guard<std::mutex> hold(_planLock);
    generation = _generation.load(std::memory_order_acquire);
    return _plan;
}

bool KafkaPipe::begin(size_t shards, std::string& error) {
    {
        std::lock_guard<std::mutex> hold(_planLock);
        _shardCount = shards;
        // The consumers' shards, now that there is a number of them.
        std::shared_ptr<Plan> plan(new Plan(*_plan));
        for (std::shared_ptr<FromKafka>& from : plan->fromKafka) {
            std::shared_ptr<FromKafka> placed(new FromKafka(*from));
            placed->owner = ownerOf(placed->config.id, shards);
            from = placed;
        }
        _plan = plan;
    }
    for (size_t s = 0; s < shards; s++) {
        std::unique_ptr<Shard> shard(new Shard());
        shard->pipe = this;
        shard->index = (uint32_t)s;
        if (!shard->wake.open()) {
            error = "kafka: no wake-up descriptor for shard " + std::to_string(s);
            return false;
        }
        // Its clients, here before its thread starts; later on its thread.
        if (!shard->reconcile(&error)) return false;
        _shards.push_back(std::move(shard));
    }
    return true;
}

bool KafkaPipe::apply(const ServerConfig& section, std::string& error) {
    // EVERY CLIENT IT NEEDS IS TRIED FIRST, here on the caller's thread and
    // then let go: a setting librdkafka refuses, a TLS file that is not there,
    // is a refused change and not a shard left without a producer.
    for (const KafkaClusterConfig& cluster : section.clusters) {
        bool produced = false, allAcks = true;
        for (const MqttToKafkaConfig& m : section.toKafka) {
            if (m.cluster != cluster.id) continue;
            produced = true;
            allAcks = allAcks && m.acks == -1;
        }
        if (produced) {
            rd_kafka_conf_t* conf = producerConf(cluster, allAcks, "modbee-check-" + cluster.id, error);
            if (conf == nullptr) return false;
            char reason[512];
            rd_kafka_t* probe = rd_kafka_new(RD_KAFKA_PRODUCER, conf, reason, sizeof reason);
            if (probe == nullptr) {
                rd_kafka_conf_destroy(conf);
                error = "kafka cluster " + cluster.id + ": " + reason;
                return false;
            }
            for (const MqttToKafkaConfig& m : section.toKafka) {
                if (m.cluster != cluster.id) continue;
                rd_kafka_topic_conf_t* tconf = topicConf(probe, m, error);
                if (tconf == nullptr) {
                    rd_kafka_destroy(probe);
                    return false;
                }
                rd_kafka_topic_conf_destroy(tconf);
            }
            rd_kafka_destroy(probe);
        }
    }
    for (const KafkaToMqttConfig& m : section.fromKafka) {
        const KafkaClusterConfig* cluster = section.cluster(m.cluster);
        if (cluster == nullptr) {
            error = "kafka mapping " + m.id + ": no cluster \"" + m.cluster + "\"";
            return false;
        }
        rd_kafka_conf_t* conf = consumerConf(*cluster, m, error);
        if (conf == nullptr) return false;
        rd_kafka_conf_destroy(conf);
    }
    {
        std::lock_guard<std::mutex> hold(_planLock);
        _plan = makePlan(section);
        _generation.fetch_add(1, std::memory_order_release);
    }
    // Each shard takes it at once, not at its next message.
    for (std::unique_ptr<Shard>& shard : _shards) shard->wake.signal();
    return true;
}

// --- A shard takes a plan, on its own thread ------------------------------------

KafkaPipe::Shard::Producer* KafkaPipe::Shard::makeProducer(const KafkaClusterConfig& cluster,
                                                           const std::string& signature, bool allAcks,
                                                           std::string& error) {
    const std::string clientId = "modbee-s" + std::to_string(index) + "-" + cluster.id;
    rd_kafka_conf_t* conf = producerConf(cluster, allAcks, clientId, error);
    if (conf == nullptr) return nullptr;
    rd_kafka_conf_set_dr_msg_cb(conf, &Shard::onDelivery);
    rd_kafka_conf_set_error_cb(conf, &Shard::onError);
    rd_kafka_conf_set_stats_cb(conf, &Shard::onStatistics);
    rd_kafka_conf_set_opaque(conf, this);
    char reason[512];
    rd_kafka_t* handle = rd_kafka_new(RD_KAFKA_PRODUCER, conf, reason, sizeof reason);
    if (handle == nullptr) {
        rd_kafka_conf_destroy(conf);
        error = "kafka cluster " + cluster.id + ": " + reason;
        return nullptr;
    }
    rd_kafka_queue_t* queue = rd_kafka_queue_get_main(handle);
    wakeOnEvents(queue, wake);
    rd_kafka_queue_destroy(queue);
    std::unique_ptr<Producer> producer(new Producer());
    producer->clusterId = cluster.id;
    producer->signature = signature;
    producer->handle = handle;
    producers.push_back(std::move(producer));
    return producers.back().get();
}

bool KafkaPipe::Shard::makeConsumer(Consumer& consumer, const KafkaClusterConfig& cluster, std::string& error) {
    const KafkaToMqttConfig& mapping = consumer.from->config;
    rd_kafka_conf_t* conf = consumerConf(cluster, mapping, error);
    if (conf == nullptr) return false;
    rd_kafka_conf_set_stats_cb(conf, &Shard::onConsumerStatistics);
    rd_kafka_conf_set_opaque(conf, &consumer);
    char reason[512];
    rd_kafka_t* handle = rd_kafka_new(RD_KAFKA_CONSUMER, conf, reason, sizeof reason);
    if (handle == nullptr) {
        rd_kafka_conf_destroy(conf);
        error = "kafka mapping " + mapping.id + ": " + reason;
        return false;
    }
    rd_kafka_poll_set_consumer(handle);
    rd_kafka_queue_t* queue = rd_kafka_queue_get_consumer(handle);
    wakeOnEvents(queue, wake);
    rd_kafka_queue_destroy(queue);

    const bool pattern = !mapping.kafkaTopicPattern.empty();
    rd_kafka_topic_partition_list_t* topics =
        rd_kafka_topic_partition_list_new(pattern ? 1 : (int)mapping.kafkaTopics.size());
    if (pattern) {
        rd_kafka_topic_partition_list_add(topics, mapping.kafkaTopicPattern.c_str(), RD_KAFKA_PARTITION_UA);
    } else {
        for (const std::string& t : mapping.kafkaTopics) {
            rd_kafka_topic_partition_list_add(topics, t.c_str(), RD_KAFKA_PARTITION_UA);
        }
    }
    const rd_kafka_resp_err_t subscribed = rd_kafka_subscribe(handle, topics);
    rd_kafka_topic_partition_list_destroy(topics);
    if (subscribed != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_destroy(handle);
        error = "kafka mapping " + mapping.id + ": " + rd_kafka_err2str(subscribed);
        return false;
    }
    consumer.handle = handle;
    return true;
}

// Leaves its group and commits what it stored, without blocking the shard:
// the close runs on its own queue, which retire() polls.
void KafkaPipe::Shard::beginClose(Consumer& consumer) {
    if (consumer.handle == nullptr || consumer.closing != nullptr) return;
    consumer.closing = rd_kafka_queue_new(consumer.handle);
    rd_kafka_error_t* failed = rd_kafka_consumer_close_queue(consumer.handle, consumer.closing);
    if (failed != nullptr) rd_kafka_error_destroy(failed);
}

// What a new plan replaced, let go once it is done: a producer once Kafka
// has answered every record it holds (each answer concludes a held PUBACK), a
// consumer once its close has finished.
void KafkaPipe::Shard::retire() {
    for (auto at = producers.begin(); at != producers.end();) {
        Producer& p = **at;
        if (!p.retiring) { ++at; continue; }
        for (int n = 0; n < 64 && rd_kafka_poll(p.handle, 0) > 0; n++) {
        }
        if (rd_kafka_outq_len(p.handle) > 0) { ++at; continue; }
        for (auto& topic : p.topics) rd_kafka_topic_destroy(topic->handle);
        rd_kafka_destroy(p.handle);
        at = producers.erase(at);
    }
    for (auto at = consumers.begin(); at != consumers.end();) {
        Consumer& c = **at;
        if (c.closing == nullptr) { ++at; continue; }
        // A close may hand back the group's revocation, which must be
        // answered (no rebalance callback is set) or the close never ends.
        while (rd_kafka_event_t* event = rd_kafka_queue_poll(c.closing, 0)) {
            if (rd_kafka_event_type(event) == RD_KAFKA_EVENT_REBALANCE &&
                rd_kafka_event_error(event) == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                const char* protocol = rd_kafka_rebalance_protocol(c.handle);
                if (protocol != nullptr && std::strcmp(protocol, "COOPERATIVE") == 0) {
                    rd_kafka_error_t* failed =
                        rd_kafka_incremental_unassign(c.handle, rd_kafka_event_topic_partition_list(event));
                    if (failed) rd_kafka_error_destroy(failed);
                } else {
                    rd_kafka_assign(c.handle, nullptr);
                }
            }
            rd_kafka_event_destroy(event);
        }
        if (!rd_kafka_consumer_closed(c.handle)) { ++at; continue; }
        rd_kafka_queue_destroy(c.closing);
        rd_kafka_destroy_flags(c.handle, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        at = consumers.erase(at);
    }
}

bool KafkaPipe::Shard::reconcile(std::string* error) {
    uint64_t now = 0;
    std::shared_ptr<const Plan> next = pipe->currentPlan(now);
    if (now == generation) return true;
    bool ok = true;
    std::string why;
    const auto failed = [&](const std::string& reason) {
        ok = false;
        if (error) *error = reason;
        else std::fprintf(stderr, "kafka: %s\n", reason.c_str());
    };

    // PRODUCERS: one per cluster that something is produced to.
    std::map<std::string, Producer*> live;
    for (const KafkaClusterConfig& cluster : next->clusters) {
        bool used = false, allAcks = true;
        for (const std::shared_ptr<ToKafka>& to : next->toKafka) {
            if (to->config.cluster != cluster.id) continue;
            used = true;
            allAcks = allAcks && to->config.acks == -1;
        }
        if (!used) continue;
        const std::string signature = clusterSignature(cluster) + (allAcks ? "idempotent" : "plain");
        Producer* keep = nullptr;
        for (std::unique_ptr<Producer>& p : producers) {
            if (!p->retiring && p->clusterId == cluster.id) keep = p.get();
        }
        // Kept, unless a topic it already writes to is asked for with other
        // acks or partitioning: librdkafka keeps one topic object per name, so
        // that takes a producer of its own.
        bool reusable = keep != nullptr && keep->signature == signature;
        for (const std::shared_ptr<ToKafka>& to : next->toKafka) {
            if (!reusable || to->config.cluster != cluster.id) continue;
            const Topic* t = keep->topic(to->config.kafkaTopic);
            if (t != nullptr && (t->acks != to->config.acks || t->uniform != to->config.uniform)) reusable = false;
        }
        if (!reusable) {
            if (keep != nullptr) keep->retiring = true;
            keep = makeProducer(cluster, signature, allAcks, why);
            if (keep == nullptr) failed(why);
        }
        if (keep != nullptr) live[cluster.id] = keep;
    }
    for (std::unique_ptr<Producer>& p : producers) {
        if (!live.count(p->clusterId) || live[p->clusterId] != p.get()) p->retiring = true;
    }

    // ROUTES: each mapping's topic on its cluster's producer.
    route.assign(next->toKafka.size(), nullptr);
    routeProducer.assign(next->toKafka.size(), nullptr);
    std::set<Topic*> owned;
    for (size_t m = 0; m < next->toKafka.size(); m++) {
        const std::shared_ptr<ToKafka>& to = next->toKafka[m];
        auto p = live.find(to->config.cluster);
        if (p == live.end()) continue;
        Producer& producer = *p->second;
        Topic* topic = producer.topic(to->config.kafkaTopic);
        if (topic == nullptr) {
            rd_kafka_topic_conf_t* tconf = topicConf(producer.handle, to->config, why);
            if (tconf == nullptr) {
                failed(why);
                continue;
            }
            std::unique_ptr<Topic> made(new Topic());
            made->name = to->config.kafkaTopic;
            made->acks = to->config.acks;
            made->uniform = to->config.uniform;
            rd_kafka_topic_conf_set_opaque(tconf, made.get());
            made->handle = rd_kafka_topic_new(producer.handle, made->name.c_str(), tconf);
            if (made->handle == nullptr) {
                failed("kafka mapping " + to->config.id + ": " + rd_kafka_err2str(rd_kafka_last_error()));
                continue;
            }
            producer.topics.push_back(std::move(made));
            topic = producer.topics.back().get();
        }
        // Its reports count against the first mapping writing to it.
        if (owned.insert(topic).second) topic->owner = to;
        route[m] = topic;
        routeProducer[m] = &producer;
    }

    // CONSUMERS: each kafka-to-mqtt mapping on the shard that owns it - kept
    // when only its MQTT side changed, made anew when its Kafka side did.
    std::set<Consumer*> kept;
    for (const std::shared_ptr<FromKafka>& from : next->fromKafka) {
        if (from->owner != index) continue;
        const KafkaClusterConfig* cluster = next->cluster(from->config.cluster);
        if (cluster == nullptr) continue;
        std::string signature = clusterSignature(*cluster) + from->config.group + '\n' +
                                from->config.kafkaTopicPattern + '\n';
        for (const std::string& t : from->config.kafkaTopics) signature += t + ',';
        Consumer* match = nullptr;
        for (std::unique_ptr<Consumer>& c : consumers) {
            if (c->closing == nullptr && c->from->config.id == from->config.id) match = c.get();
        }
        if (match != nullptr && match->signature == signature) {
            match->from = from;
            kept.insert(match);
            continue;
        }
        if (match != nullptr) beginClose(*match);
        std::unique_ptr<Consumer> made(new Consumer());
        made->signature = signature;
        made->from = from;
        if (!makeConsumer(*made, *cluster, why)) {
            failed(why);
            continue;
        }
        kept.insert(made.get());
        consumers.push_back(std::move(made));
    }
    for (std::unique_ptr<Consumer>& c : consumers) {
        if (!kept.count(c.get())) beginClose(*c);
    }

    plan = next;
    generation = now;
    return ok;
}

int KafkaPipe::waitHandle(uint32_t shard) {
    return shard < _shards.size() ? _shards[shard]->wake.handle() : -1;
}

uint32_t KafkaPipe::offer(uint32_t shardIndex, MqttBroker& broker, const MqttBrokerRouted& message,
                          uint64_t deferral) {
    Shard& shard = *_shards[shardIndex];
    shard.broker = &broker;
    // A plan changed since the last message: one atomic read to know.
    if (shard.generation != _generation.load(std::memory_order_acquire)) shard.reconcile(nullptr);
    const KafkaOrigin* origin = static_cast<const KafkaOrigin*>(message.origin);
    if (origin != nullptr && origin->magic != KafkaOrigin().magic) origin = nullptr;

    uint32_t parties = 0;
    const Plan& plan = *shard.plan;
    for (size_t m = 0; m < plan.toKafka.size(); m++) {
        ToKafka& to = *plan.toKafka[m];
        const MqttToKafkaConfig& mapping = to.config;
        bool matches = false;
        for (const std::string& filter : mapping.filters) {
            if (mq_topicMatches(spanOf(filter), message.topic)) { matches = true; break; }
        }
        for (const std::string& filter : mapping.excludeFilters) {
            if (matches && mq_topicMatches(spanOf(filter), message.topic)) matches = false;
        }
        if (!matches) continue;
        // Never back where it came from: that is a loop, not a mapping.
        if (origin != nullptr && origin->cluster != nullptr && *origin->cluster == mapping.cluster &&
            mapping.kafkaTopic == origin->topic) {
            continue;
        }
        if (shard.route[m] == nullptr) continue;
        rd_kafka_topic_t* topic = shard.route[m]->handle;

        // The acknowledgement waits for Kafka only for a client's QoS 1/2
        // publish on a mapping that asks for it. Everything else is sent and
        // its outcome counted.
        const bool wait = deferral != 0 && mapping.acknowledgeOnKafka;

        std::string key;
        bool keyed = true;
        switch (mapping.key) {
        case KafkaKeyMode::Topic: key = toText(message.topic); break;
        case KafkaKeyMode::ClientId: key = toText(message.clientId); keyed = !key.empty(); break;
        case KafkaKeyMode::None: keyed = false; break;
        case KafkaKeyMode::Template: key = expandKey(mapping.keyTemplate, message.topic, message.clientId); break;
        }

        // HEADERS: the envelope (KafkaEnvelope.h), or the placeholders asked
        // for; then the fixed ones.
        rd_kafka_headers_t* headers = nullptr;
        size_t headerBytes = 0;
        if (mapping.allHeaders || !mapping.headers.empty()) {
            headers = rd_kafka_headers_new(8);
            headerBytes = kafkaWriteEnvelope(headers, &mapping, message.topic, message.qos, message.retain,
                                             message.clientId, message.userName, message.properties);
        }

        // TOO LARGE FOR THIS MAPPING: never sent, never truncated. At MQTT 5
        // the publisher is told 0x83; at 3.1.1 it cannot be, and it is counted.
        const size_t recordBytes = message.payload.length + (keyed ? key.size() : 0) + headerBytes;
        if (recordBytes > mapping.maxRecordBytes) {
            if (headers) rd_kafka_headers_destroy(headers);
            to.counters->tooLarge.fetch_add(1, std::memory_order_relaxed);
            if (deferral != 0) {
                broker.concludeDeferred(deferral, 1, 1, kImplementationError);
                parties++;
            }
            continue;
        }

        rd_kafka_vu_t vu[6];
        size_t n = 0;
        vu[n].vtype = RD_KAFKA_VTYPE_RKT; vu[n].u.rkt = topic; n++;
        vu[n].vtype = RD_KAFKA_VTYPE_VALUE;
        vu[n].u.mem.ptr = (void*)message.payload.data;
        vu[n].u.mem.size = message.payload.length; n++;
        if (keyed) {
            vu[n].vtype = RD_KAFKA_VTYPE_KEY;
            vu[n].u.mem.ptr = (void*)key.data();
            vu[n].u.mem.size = key.size(); n++;
        }
        if (headers) { vu[n].vtype = RD_KAFKA_VTYPE_HEADERS; vu[n].u.headers = headers; n++; }
        vu[n].vtype = RD_KAFKA_VTYPE_MSGFLAGS; vu[n].u.i32 = RD_KAFKA_MSG_F_COPY; n++;
        vu[n].vtype = RD_KAFKA_VTYPE_OPAQUE;
        vu[n].u.ptr = wait ? (void*)(uintptr_t)deferral : nullptr; n++;

        // Never blocks: RD_KAFKA_MSG_F_BLOCK is not passed. A full queue is
        // the backpressure signal, and it is answered, not waited out.
        rd_kafka_error_t* failure = rd_kafka_produceva(shard.routeProducer[m]->handle, vu, n);
        if (failure != nullptr) {
            rd_kafka_error_destroy(failure);
            if (headers) rd_kafka_headers_destroy(headers);   // still ours when the produce failed
            to.counters->failed.fetch_add(1, std::memory_order_relaxed);
            if (deferral != 0) {
                broker.concludeDeferred(deferral, 1, 1, kQuotaExceeded);
                parties++;
            }
            continue;
        }
        to.counters->send.fetch_add(1, std::memory_order_relaxed);
        if (wait) {
            shard.held.fetch_add(1, std::memory_order_relaxed);
            parties++;
        } else if (deferral != 0) {
            // acknowledge: routed - the mapping took it, which is a match:
            // the publisher is told success, not 0x10.
            broker.concludeDeferred(deferral, 1, 0);
            parties++;
        }
    }
    return parties;
}

// --- Kafka -> MQTT -----------------------------------------------------------

void KafkaPipe::Shard::publishRecord(FromKafka& from, const rd_kafka_message_t* record) {
    const KafkaToMqttConfig& mapping = from.config;
    const char* kafkaTopic = rd_kafka_topic_name(record->rkt);

    // What the MQTT message is: from the mapping, or from the mqtt-* headers
    // a headers: all mapping wrote - so it republishes as the same message.
    std::vector<std::string> topics;
    uint8_t qos = mapping.qos;
    bool retain = mapping.retain;
    MqttProperties properties;
    std::string contentType = mapping.contentType, responseTopic = mapping.responseTopic;
    std::string correlation;
    bool hasCorrelation = false;
    bool hasFormat = mapping.hasPayloadFormat;
    uint8_t format = mapping.payloadFormat;
    bool hasExpiry = mapping.hasMessageExpiry;
    uint32_t expiry = mapping.messageExpiry;
    std::vector<std::pair<std::string, std::string>> users = mapping.userProperties;

    rd_kafka_headers_t* headers = nullptr;
    const bool hasHeaders = rd_kafka_message_headers(record, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR;
    KafkaEnvelope envelope;
    const bool readable = !hasHeaders || kafkaReadEnvelope(headers, envelope);
    // One this link wrote itself: Kafka has no No Local, so the link has it here.
    if (!mapping.skipLink.empty() && envelope.link == mapping.skipLink) return;
    if (mapping.fromHeaders && hasHeaders) {
        if (!readable) {
            // A later version of the envelope: not guessed at.
            from.counters->invalid.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        if (envelope.hasTopic) topics.assign(1, envelope.topic);
        if (envelope.hasQos) qos = envelope.qos;
        if (envelope.hasRetain) retain = envelope.retain;
        contentType = envelope.contentType;
        responseTopic = envelope.responseTopic;
        correlation = envelope.correlation;
        hasCorrelation = envelope.hasCorrelation;
        hasFormat = envelope.hasFormat;
        format = envelope.format;
        hasExpiry = envelope.hasExpiry;
        expiry = envelope.expiry;
        users = envelope.users;
    }
    if (topics.empty()) {
        for (const std::string& pattern : mapping.mqttTopics) {
            topics.push_back(expandTopic(pattern, kafkaTopic, record->key, record->key_len));
        }
    }

    if (!contentType.empty()) { properties.contentType = spanOf(contentType); properties.markPresent(MQ_PROP_CONTENT_TYPE); }
    // A Response Topic is a topic name [MQTT-3.3.2-13]: one from a header that
    // is not - a wildcard, bad UTF-8 - is left off rather than handed to every
    // subscriber, which would be right to refuse the whole PUBLISH for it.
    if (!responseTopic.empty() && mq_topicNameIsValid(spanOf(responseTopic))) {
        properties.responseTopic = spanOf(responseTopic);
        properties.markPresent(MQ_PROP_RESPONSE_TOPIC);
    }
    if (hasCorrelation) { properties.correlationData = spanOf(correlation); properties.markPresent(MQ_PROP_CORRELATION_DATA); }
    if (hasFormat) { properties.payloadFormatIndicator = format; properties.markPresent(MQ_PROP_PAYLOAD_FORMAT_INDICATOR); }
    if (hasExpiry) { properties.messageExpiryInterval = expiry; properties.markPresent(MQ_PROP_MESSAGE_EXPIRY_INTERVAL); }
    for (const auto& user : users) properties.addUserProperty(spanOf(user.first), spanOf(user.second));

    KafkaOrigin origin;
    origin.cluster = &from.config.cluster;
    origin.topic = kafkaTopic;
    for (const std::string& topic : topics) {
        MqttBrokerRouted message;
        message.topic = spanOf(topic);
        message.payload.data = (const uint8_t*)record->payload;
        message.payload.length = (uint32_t)record->len;
        message.qos = qos;
        message.retain = retain;
        message.properties = &properties;
        message.origin = &origin;
        // A $ topic is the server's own [MQTT-4.7.2-1]: $modbee/admin/request,
        // $modbee/events/... A record never speaks on one - whoever can write
        // to the Kafka topic must not be able to issue requests or forge events.
        if (!mq_topicNameIsValid(message.topic) || topic[0] == '$') {
            from.counters->invalid.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        // Committed either way (at-least-once is about reaching the broker, and a
        // refusal would refuse again on a re-read), but a refusal is COUNTED: a
        // subscriber's queue or the retained store was full. A record no session
        // subscribes to reaches nobody and is not counted here - with several
        // shards, this one cannot know whether another has a subscriber.
        if (!broker->publishMessage(message)) {
            from.counters->refused.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        from.counters->published.fetch_add(1, std::memory_order_relaxed);
    }
}

void KafkaPipe::Shard::consume(Consumer& consumer) {
    FromKafka& from = *consumer.from;
    for (int n = 0; n < kConsumeBudget; n++) {
        rd_kafka_message_t* record = rd_kafka_consumer_poll(consumer.handle, 0);
        if (record == nullptr) return;
        if (record->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            from.counters->polled.fetch_add(1, std::memory_order_relaxed);
            publishRecord(from, record);
            // TAKEN: now, and only now, may this offset be committed. A crash
            // before here re-reads the record - at-least-once - rather than
            // committing one the broker never saw.
            rd_kafka_error_t* stored = rd_kafka_offset_store_message(record);
            if (stored) rd_kafka_error_destroy(stored);
        } else if (record->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            std::fprintf(stderr, "kafka-to-mqtt %s: %s\n", from.config.id.c_str(),
                         rd_kafka_message_errstr(record));
        }
        rd_kafka_message_destroy(record);
    }
    backlog = true;
}

void KafkaPipe::service(uint32_t shardIndex, MqttBroker& broker) {
    Shard& shard = *_shards[shardIndex];
    shard.broker = &broker;
    const bool woken = shard.wake.drain();
    const mq_time_t now = mq_millis();
    if (!woken && !shard.backlog && mq_elapsed(shard.servedAt, now) < kServiceIntervalMs) return;
    shard.servedAt = now;
    shard.backlog = false;
    if (shard.generation != _generation.load(std::memory_order_acquire)) shard.reconcile(nullptr);
    for (std::unique_ptr<Shard::Producer>& producer : shard.producers) {
        if (producer->retiring) continue;
        // Every queued delivery report, not only the first.
        for (int n = 0; n < 64 && rd_kafka_poll(producer->handle, 0) > 0; n++) {
        }
    }
    for (std::unique_ptr<Shard::Consumer>& consumer : shard.consumers) {
        if (consumer->closing == nullptr) shard.consume(*consumer);
    }
    shard.retire();
}

bool KafkaPipe::hasPendingWork(uint32_t shardIndex) {
    return _shards[shardIndex]->backlog;
}

void KafkaPipe::stop(uint32_t shardIndex, MqttBroker& broker) {
    Shard& shard = *_shards[shardIndex];
    shard.broker = &broker;
    // Consumers first: closing commits the offsets already stored, and no
    // more records arrive to publish into a broker that is about to end.
    for (std::unique_ptr<Shard::Consumer>& consumer : shard.consumers) {
        if (consumer->closing == nullptr) rd_kafka_consumer_close(consumer->handle);
    }
    // Then what is in flight to Kafka - from replaced producers too - within a
    // bound: each delivery report concludes its held PUBACK while the client
    // is still connected.
    for (std::unique_ptr<Shard::Producer>& producer : shard.producers) {
        rd_kafka_flush(producer->handle, _flushMillis);
    }
    shard.broker = nullptr;
}

KafkaPipeStatus KafkaPipe::status() const {
    KafkaPipeStatus out;
    out.kafkaReachable = !_brokersDown.load(std::memory_order_relaxed);
    uint64_t ignored = 0;
    const std::shared_ptr<const Plan> plan = currentPlan(ignored);
    for (const std::shared_ptr<ToKafka>& to : plan->toKafka) {
        KafkaPipeStatus::Mapping m;
        m.id = to->config.id;
        m.toKafka = true;
        m.send = to->counters->send.load();
        m.success = to->counters->success.load();
        m.failed = to->counters->failed.load();
        m.tooLarge = to->counters->tooLarge.load();
        m.latencyMicros = to->counters->latencyMicros.load();
        out.mappings.push_back(m);
    }
    for (const std::shared_ptr<FromKafka>& from : plan->fromKafka) {
        KafkaPipeStatus::Mapping m;
        m.id = from->config.id;
        m.toKafka = false;
        m.polled = from->counters->polled.load();
        m.published = from->counters->published.load();
        m.invalid = from->counters->invalid.load();
        m.refused = from->counters->refused.load();
        m.lag = from->counters->lag.load();
        out.mappings.push_back(m);
    }
    for (const std::unique_ptr<Shard>& shard : _shards) out.heldAcknowledgements += shard->held.load();
    return out;
}

namespace {

std::string label(const std::string& id) {
    std::string out;
    for (char c : id) {
        if (c == '\\' || c == '"') out.push_back('\\');
        if (c == '\n') { out += "\\n"; continue; }
        out.push_back(c);
    }
    return out;
}

void series(std::string& out, const char* name, const char* type, const char* help,
            const KafkaPipeStatus& status, bool toKafka,
            uint64_t KafkaPipeStatus::Mapping::*field, double scale = 1.0) {
    out += std::string("# HELP ") + name + " " + help + "\n# TYPE " + name + " " + type + "\n";
    for (const KafkaPipeStatus::Mapping& m : status.mappings) {
        if (m.toKafka != toKafka) continue;
        char value[64];
        if (scale == 1.0) std::snprintf(value, sizeof value, "%llu", (unsigned long long)(m.*field));
        else std::snprintf(value, sizeof value, "%.6f", (double)(m.*field) * scale);
        out += std::string(name) + "{mapping=\"" + label(m.id) + "\"} " + value + "\n";
    }
}

} // namespace

void renderKafkaMetrics(const KafkaPipeStatus& status, std::string& out) {
    typedef KafkaPipeStatus::Mapping M;
    series(out, "modbee_kafka_mqtt_to_kafka_send_total", "counter",
           "Records handed to the Kafka producer.", status, true, &M::send);
    series(out, "modbee_kafka_mqtt_to_kafka_success_total", "counter",
           "Records Kafka acknowledged.", status, true, &M::success);
    series(out, "modbee_kafka_mqtt_to_kafka_failed_total", "counter",
           "Records Kafka or the producer refused.", status, true, &M::failed);
    series(out, "modbee_kafka_mqtt_to_kafka_too_large_total", "counter",
           "Messages over max-record-bytes, never sent.", status, true, &M::tooLarge);
    series(out, "modbee_kafka_mqtt_to_kafka_latency_seconds_sum", "counter",
           "Produce-to-acknowledgement time, summed; divide by success_total.", status, true,
           &M::latencyMicros, 1e-6);
    series(out, "modbee_kafka_kafka_to_mqtt_polled_total", "counter",
           "Records read from Kafka.", status, false, &M::polled);
    series(out, "modbee_kafka_kafka_to_mqtt_published_total", "counter",
           "MQTT messages published from Kafka records.", status, false, &M::published);
    series(out, "modbee_kafka_kafka_to_mqtt_invalid_total", "counter",
           "Records that made no valid MQTT message.", status, false, &M::invalid);
    series(out, "modbee_kafka_kafka_to_mqtt_refused_total", "counter",
           "Records the broker could not take: a subscriber's queue or the retained store was full.",
           status, false, &M::refused);
    out += "# HELP modbee_kafka_held_acknowledgements PUBACK/PUBRECs waiting on Kafka now.\n"
           "# TYPE modbee_kafka_held_acknowledgements gauge\n"
           "modbee_kafka_held_acknowledgements " + std::to_string(status.heldAcknowledgements) + "\n";
}

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaPipe.h"
#include "KafkaCommon.h"

#include "MqttTopic.h"

#include <librdkafka/rdkafka.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <sys/eventfd.h>
#include <unistd.h>

namespace {

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
    size_t cluster = 0;
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
        const std::string name = pattern.substr(open + 2, close - open - 2);
        if (name == "MQTT_TOPIC") {
            out.append((const char*)topic.data, topic.length);
        } else if (name == "MQTT_CLIENTID") {
            out.append((const char*)clientId.data, clientId.length);
        } else {
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
        const std::string name = pattern.substr(open + 2, close - open - 2);
        if (name == "KAFKA_TOPIC") out.append(kafkaTopic);
        else if (key != nullptr) out.append((const char*)key, keyLength);
        at = close + 1;
    }
    return out;
}

bool headerWanted(const MqttToKafkaConfig& mapping, const char* placeholder) {
    if (mapping.allHeaders) return true;
    for (const KafkaHeaderSpec& h : mapping.headers) {
        if (!h.fixed && h.name == placeholder) return true;
    }
    return false;
}

void addHeader(rd_kafka_headers_t* headers, const char* name, const void* value, size_t size) {
    rd_kafka_header_add(headers, name, -1, value, (ssize_t)size);
}

} // namespace

// --- The mappings, shared read-only by every shard after begin() ------------

struct KafkaPipe::ToKafka {
    MqttToKafkaConfig config;
    size_t cluster = 0;            // index into _config.clusters
    KafkaMappingCounters counters;
};

struct KafkaPipe::FromKafka {
    KafkaToMqttConfig config;
    size_t cluster = 0;
    uint32_t owner = 0;            // the shard whose loop runs its consumer
    KafkaMappingCounters counters;
};

// --- One shard's clients -----------------------------------------------------

struct KafkaPipe::Shard {
    KafkaPipe* pipe = nullptr;
    uint32_t index = 0;
    int wakeFd = -1;
    MqttBroker* broker = nullptr;          // set on every call from the shard's thread
    mq_time_t servedAt = 0;
    bool backlog = false;                  // a consumer had more than its budget
    std::atomic<uint64_t> held{0};         // acknowledgements waiting on Kafka

    // Producers, one per cluster that has mqtt-to-kafka mappings; and per
    // mapping, its topic handle on that producer (null: another cluster).
    std::vector<rd_kafka_t*> producers;
    std::vector<rd_kafka_topic_t*> topics;

    // Consumers this shard runs: (mapping index, handle).
    std::vector<std::pair<size_t, rd_kafka_t*>> consumers;

    static void onDelivery(rd_kafka_t*, const rd_kafka_message_t* message, void* opaque);
    static void onError(rd_kafka_t*, int error, const char* reason, void* opaque);
    static int onStatistics(rd_kafka_t*, char* json, size_t length, void* opaque);
    void consume(size_t mapping, rd_kafka_t* consumer);
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

// Runs inside rd_kafka_poll(), on the shard's own thread.
void KafkaPipe::Shard::onDelivery(rd_kafka_t*, const rd_kafka_message_t* message, void* opaque) {
    Shard& shard = *static_cast<Shard*>(opaque);
    const size_t mapping = (size_t)(uintptr_t)rd_kafka_topic_opaque(message->rkt);
    ToKafka& to = *shard.pipe->_toKafka[mapping];
    const uint64_t deferral = (uint64_t)(uintptr_t)message->_private;
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        shard.pipe->_brokersDown.store(false, std::memory_order_relaxed);
        to.counters.success.fetch_add(1, std::memory_order_relaxed);
        const int64_t latency = rd_kafka_message_latency(message);
        if (latency > 0) to.counters.latencyMicros.fetch_add((uint64_t)latency, std::memory_order_relaxed);
    } else {
        to.counters.failed.fetch_add(1, std::memory_order_relaxed);
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

// --- Construction ------------------------------------------------------------

KafkaPipe::KafkaPipe(const ServerConfig& config) : _config(config) {}

KafkaPipe::~KafkaPipe() {
    for (std::unique_ptr<Shard>& shard : _shards) {
        shard->broker = nullptr;
        for (auto& consumer : shard->consumers) rd_kafka_destroy(consumer.second);
        for (rd_kafka_topic_t* topic : shard->topics) if (topic) rd_kafka_topic_destroy(topic);
        for (rd_kafka_t* producer : shard->producers) if (producer) rd_kafka_destroy(producer);
        if (shard->wakeFd >= 0) ::close(shard->wakeFd);
    }
}

const char* KafkaPipe::libraryVersion() { return rd_kafka_version_str(); }

bool KafkaPipe::begin(size_t shards, std::string& error) {
    for (const MqttToKafkaConfig& m : _config.toKafka) {
        std::unique_ptr<ToKafka> to(new ToKafka());
        to->config = m;
        for (size_t c = 0; c < _config.clusters.size(); c++) {
            if (_config.clusters[c].id == m.cluster) to->cluster = c;
        }
        _toKafka.push_back(std::move(to));
    }
    for (size_t i = 0; i < _config.fromKafka.size(); i++) {
        std::unique_ptr<FromKafka> from(new FromKafka());
        from->config = _config.fromKafka[i];
        for (size_t c = 0; c < _config.clusters.size(); c++) {
            if (_config.clusters[c].id == from->config.cluster) from->cluster = c;
        }
        from->owner = (uint32_t)(i % shards);
        _fromKafka.push_back(std::move(from));
    }

    for (size_t s = 0; s < shards; s++) {
        std::unique_ptr<Shard> shard(new Shard());
        shard->pipe = this;
        shard->index = (uint32_t)s;
        shard->wakeFd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (shard->wakeFd < 0) {
            error = "kafka: no eventfd for shard " + std::to_string(s);
            return false;
        }
        static const uint64_t kOne = 1;

        // PRODUCERS: one per cluster that something is produced to.
        shard->producers.assign(_config.clusters.size(), nullptr);
        shard->topics.assign(_toKafka.size(), nullptr);
        for (size_t c = 0; c < _config.clusters.size(); c++) {
            bool used = false, allAcks = true;
            for (const std::unique_ptr<ToKafka>& to : _toKafka) {
                if (to->cluster != c) continue;
                used = true;
                allAcks = allAcks && to->config.acks == -1;
            }
            if (!used) continue;
            const KafkaClusterConfig& cluster = _config.clusters[c];
            rd_kafka_conf_t* conf = rd_kafka_conf_new();
            const std::string clientId = "modbee-s" + std::to_string(s) + "-" + cluster.id;
            // Idempotence keeps a retried send from being written twice and
            // keeps order within a partition through retries. Kafka allows it
            // only with acks=all, so a cluster with any mapping on fewer acks
            // goes without - and docs/kafka.md says what that costs.
            const bool ok =
                kafkaClusterSettings(conf, cluster, clientId, error) &&
                kafkaSet(conf, "enable.idempotence", allAcks ? "true" : "false", error) &&
                kafkaSet(conf, "linger.ms", "5", error) &&
                kafkaSet(conf, "compression.type", "lz4", error) &&
                // The Java client's partitioner, so a key lands on the same
                // partition whether modbee or a Java producer wrote it.
                kafkaSet(conf, "partitioner", "murmur2_random", error) &&
                // How long a record may wait for Kafka before it is given up
                // on - and with it, how long a publisher's PUBACK can be held.
                // librdkafka's default is five minutes, far beyond the point
                // where a device has given up waiting (this library's client:
                // 60 s). Thirty seconds rides out a broker restart and still
                // answers inside the client's patience. The file can change it.
                kafkaSet(conf, "message.timeout.ms", "30000", error) &&
                // Broker state for /readyz (onStatistics). The file can change it.
                kafkaSet(conf, "statistics.interval.ms", "5000", error) &&
                kafkaPassThrough(conf, cluster, error);
            if (!ok) {
                rd_kafka_conf_destroy(conf);
                error = "kafka cluster " + cluster.id + ": " + error;
                return false;
            }
            rd_kafka_conf_set_dr_msg_cb(conf, &Shard::onDelivery);
            rd_kafka_conf_set_error_cb(conf, &Shard::onError);
            rd_kafka_conf_set_stats_cb(conf, &Shard::onStatistics);
            rd_kafka_conf_set_opaque(conf, shard.get());
            char reason[512];
            rd_kafka_t* producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, reason, sizeof reason);
            if (producer == nullptr) {
                rd_kafka_conf_destroy(conf);
                error = "kafka cluster " + cluster.id + ": " + reason;
                return false;
            }
            rd_kafka_queue_t* queue = rd_kafka_queue_get_main(producer);
            rd_kafka_queue_io_event_enable(queue, shard->wakeFd, &kOne, sizeof kOne);
            rd_kafka_queue_destroy(queue);
            shard->producers[c] = producer;

            for (size_t m = 0; m < _toKafka.size(); m++) {
                if (_toKafka[m]->cluster != c) continue;
                const MqttToKafkaConfig& mc = _toKafka[m]->config;
                // FROM THE PRODUCER'S DEFAULTS, not a fresh topic config: the
                // topic-level settings the file passed through (message.timeout.ms,
                // compression per topic) live there, and a fresh one would
                // silently drop every one of them.
                rd_kafka_topic_conf_t* tconf = rd_kafka_default_topic_conf_dup(producer);
                char why[512];
                const char* acks = mc.acks == 0 ? "0" : mc.acks == 1 ? "1" : "-1";
                if (rd_kafka_topic_conf_set(tconf, "acks", acks, why, sizeof why) != RD_KAFKA_CONF_OK ||
                    rd_kafka_topic_conf_set(tconf, "partitioner",
                                            mc.uniform ? "random" : "murmur2_random", why,
                                            sizeof why) != RD_KAFKA_CONF_OK) {
                    rd_kafka_topic_conf_destroy(tconf);
                    error = "kafka mapping " + mc.id + ": " + why;
                    return false;
                }
                rd_kafka_topic_conf_set_opaque(tconf, (void*)(uintptr_t)m);
                rd_kafka_topic_t* topic = rd_kafka_topic_new(producer, mc.kafkaTopic.c_str(), tconf);
                if (topic == nullptr) {
                    error = "kafka mapping " + mc.id + ": " + rd_kafka_err2str(rd_kafka_last_error());
                    return false;
                }
                shard->topics[m] = topic;
            }
        }

        // CONSUMERS: each kafka-to-mqtt mapping on the shard that owns it.
        for (size_t m = 0; m < _fromKafka.size(); m++) {
            FromKafka& from = *_fromKafka[m];
            if (from.owner != s) continue;
            const KafkaClusterConfig& cluster = _config.clusters[from.cluster];
            rd_kafka_conf_t* conf = rd_kafka_conf_new();
            const std::string clientId = "modbee-" + from.config.id;
            // At-least-once: offsets are STORED only once the broker has
            // taken the record, and committed (automatically) from the store.
            const bool ok =
                kafkaClusterSettings(conf, cluster, clientId, error) &&
                kafkaSet(conf, "group.id", from.config.group, error) &&
                kafkaSet(conf, "enable.auto.commit", "true", error) &&
                kafkaSet(conf, "enable.auto.offset.store", "false", error) &&
                kafkaSet(conf, "auto.offset.reset", "latest", error) &&
                kafkaPassThrough(conf, cluster, error);
            if (!ok) {
                rd_kafka_conf_destroy(conf);
                error = "kafka mapping " + from.config.id + ": " + error;
                return false;
            }
            char reason[512];
            rd_kafka_t* consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, reason, sizeof reason);
            if (consumer == nullptr) {
                rd_kafka_conf_destroy(conf);
                error = "kafka mapping " + from.config.id + ": " + reason;
                return false;
            }
            rd_kafka_poll_set_consumer(consumer);
            rd_kafka_queue_t* queue = rd_kafka_queue_get_consumer(consumer);
            rd_kafka_queue_io_event_enable(queue, shard->wakeFd, &kOne, sizeof kOne);
            rd_kafka_queue_destroy(queue);

            const bool pattern = !from.config.kafkaTopicPattern.empty();
            rd_kafka_topic_partition_list_t* topics =
                rd_kafka_topic_partition_list_new(pattern ? 1 : (int)from.config.kafkaTopics.size());
            if (pattern) {
                rd_kafka_topic_partition_list_add(topics, from.config.kafkaTopicPattern.c_str(),
                                                  RD_KAFKA_PARTITION_UA);
            } else {
                for (const std::string& t : from.config.kafkaTopics) {
                    rd_kafka_topic_partition_list_add(topics, t.c_str(), RD_KAFKA_PARTITION_UA);
                }
            }
            const rd_kafka_resp_err_t subscribed = rd_kafka_subscribe(consumer, topics);
            rd_kafka_topic_partition_list_destroy(topics);
            if (subscribed != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_destroy(consumer);
                error = "kafka mapping " + from.config.id + ": " + rd_kafka_err2str(subscribed);
                return false;
            }
            shard->consumers.push_back(std::make_pair(m, consumer));
        }
        _shards.push_back(std::move(shard));
    }
    return true;
}

int KafkaPipe::waitHandle(uint32_t shard) {
    return shard < _shards.size() ? _shards[shard]->wakeFd : -1;
}

// --- MQTT -> Kafka -----------------------------------------------------------

uint32_t KafkaPipe::offer(uint32_t shardIndex, MqttBroker& broker, const MqttBrokerRouted& message,
                          uint64_t deferral) {
    Shard& shard = *_shards[shardIndex];
    shard.broker = &broker;
    const KafkaOrigin* origin = static_cast<const KafkaOrigin*>(message.origin);
    if (origin != nullptr && origin->magic != KafkaOrigin().magic) origin = nullptr;

    uint32_t parties = 0;
    for (size_t m = 0; m < _toKafka.size(); m++) {
        ToKafka& to = *_toKafka[m];
        const MqttToKafkaConfig& mapping = to.config;
        bool matches = false;
        for (const std::string& filter : mapping.filters) {
            if (mq_topicMatches(spanOf(filter), message.topic)) { matches = true; break; }
        }
        if (!matches) continue;
        // Never back where it came from: that is a loop, not a mapping.
        if (origin != nullptr && origin->cluster == to.cluster &&
            mapping.kafkaTopic == origin->topic) {
            continue;
        }
        rd_kafka_topic_t* topic = shard.topics[m];
        if (topic == nullptr) continue;

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

        // HEADERS: the placeholders asked for, then the fixed ones.
        rd_kafka_headers_t* headers = nullptr;
        size_t headerBytes = 0;
        const MqttProperties* p = message.properties;
        if (mapping.allHeaders || !mapping.headers.empty()) {
            headers = rd_kafka_headers_new(8);
            const auto add = [&](const char* name, const void* value, size_t size) {
                addHeader(headers, name, value, size);
                headerBytes += std::strlen(name) + size;
            };
            const auto addText = [&](const char* name, const std::string& value) {
                add(name, value.data(), value.size());
            };
            if (headerWanted(mapping, "mqtt-topic")) add("mqtt-topic", message.topic.data, message.topic.length);
            if (headerWanted(mapping, "mqtt-qos")) addText("mqtt-qos", std::to_string(message.qos));
            if (headerWanted(mapping, "mqtt-retain")) addText("mqtt-retain", message.retain ? "true" : "false");
            if (headerWanted(mapping, "mqtt-clientid") && message.clientId.length > 0) {
                add("mqtt-clientid", message.clientId.data, message.clientId.length);
            }
            // Who published it - the login the broker checked, so a reader knows whose message this is
            // without trusting anything in the payload or the topic. A Will carries its session's.
            if (headerWanted(mapping, "mqtt-username") && message.userName.length > 0) {
                add("mqtt-username", message.userName.data, message.userName.length);
            }
            if (p != nullptr) {
                if (headerWanted(mapping, "mqtt-content-type") && p->has(MQ_PROP_CONTENT_TYPE)) {
                    add("mqtt-content-type", p->contentType.data, p->contentType.length);
                }
                if (headerWanted(mapping, "mqtt-correlation-data") && p->has(MQ_PROP_CORRELATION_DATA)) {
                    add("mqtt-correlation-data", p->correlationData.data, p->correlationData.length);
                }
                if (headerWanted(mapping, "mqtt-payload-format-indicator") &&
                    p->has(MQ_PROP_PAYLOAD_FORMAT_INDICATOR)) {
                    addText("mqtt-payload-format-indicator", std::to_string(p->payloadFormatIndicator));
                }
                if (headerWanted(mapping, "mqtt-response-topic") && p->has(MQ_PROP_RESPONSE_TOPIC)) {
                    add("mqtt-response-topic", p->responseTopic.data, p->responseTopic.length);
                }
                if (headerWanted(mapping, "mqtt-message-expiry-interval") &&
                    p->has(MQ_PROP_MESSAGE_EXPIRY_INTERVAL)) {
                    addText("mqtt-message-expiry-interval", std::to_string(p->messageExpiryInterval));
                }
                if (headerWanted(mapping, "mqtt-user-properties")) {
                    for (uint8_t u = 0; u < p->userCount; u++) {
                        std::string name = toText(p->user[u].key);
                        if (name.compare(0, 5, "mqtt-") == 0) name = kUserPropertyEscape + name;
                        add(name.c_str(), p->user[u].value.data, p->user[u].value.length);
                    }
                }
            }
            for (const KafkaHeaderSpec& h : mapping.headers) {
                if (h.fixed) addText(h.name.c_str(), h.value);
            }
        }

        // TOO LARGE FOR THIS MAPPING: never sent, never truncated. At MQTT 5
        // the publisher is told 0x83; at 3.1.1 it cannot be, and it is counted.
        const size_t recordBytes = message.payload.length + (keyed ? key.size() : 0) + headerBytes;
        if (recordBytes > mapping.maxRecordBytes) {
            if (headers) rd_kafka_headers_destroy(headers);
            to.counters.tooLarge.fetch_add(1, std::memory_order_relaxed);
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
        rd_kafka_error_t* failure = rd_kafka_produceva(shard.producers[to.cluster], vu, n);
        if (failure != nullptr) {
            rd_kafka_error_destroy(failure);
            if (headers) rd_kafka_headers_destroy(headers);   // still ours when the produce failed
            to.counters.failed.fetch_add(1, std::memory_order_relaxed);
            if (deferral != 0) {
                broker.concludeDeferred(deferral, 1, 1, kQuotaExceeded);
                parties++;
            }
            continue;
        }
        to.counters.send.fetch_add(1, std::memory_order_relaxed);
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
    if (mapping.fromHeaders && rd_kafka_message_headers(record, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR) {
        users.clear();
        contentType.clear();
        responseTopic.clear();
        hasFormat = hasExpiry = false;
        const char* name;
        const void* value;
        size_t size;
        for (size_t i = 0; rd_kafka_header_get_all(headers, i, &name, &value, &size) ==
                           RD_KAFKA_RESP_ERR_NO_ERROR; i++) {
            const std::string v = value ? std::string((const char*)value, size) : std::string();
            if (std::strcmp(name, "mqtt-topic") == 0) topics.assign(1, v);
            else if (std::strcmp(name, "mqtt-qos") == 0) qos = (uint8_t)(v == "2" ? 2 : v == "1" ? 1 : 0);
            else if (std::strcmp(name, "mqtt-retain") == 0) retain = v == "true";
            else if (std::strcmp(name, "mqtt-content-type") == 0) contentType = v;
            else if (std::strcmp(name, "mqtt-response-topic") == 0) responseTopic = v;
            else if (std::strcmp(name, "mqtt-correlation-data") == 0) { correlation = v; hasCorrelation = true; }
            else if (std::strcmp(name, "mqtt-payload-format-indicator") == 0) { hasFormat = true; format = v == "1" ? 1 : 0; }
            else if (std::strcmp(name, "mqtt-message-expiry-interval") == 0) {
                hasExpiry = true;
                expiry = (uint32_t)std::strtoul(v.c_str(), nullptr, 10);
            }
            else if (std::strncmp(name, kUserPropertyEscape, sizeof kUserPropertyEscape - 1) == 0) {
                users.push_back(std::make_pair(std::string(name + sizeof kUserPropertyEscape - 1), v));
            }
            else if (std::strncmp(name, "mqtt-", 5) == 0) { /* mqtt-clientid and the like: informational */ }
            else users.push_back(std::make_pair(std::string(name), v));
        }
    }
    if (topics.empty()) {
        for (const std::string& pattern : mapping.mqttTopics) {
            topics.push_back(expandTopic(pattern, kafkaTopic, record->key, record->key_len));
        }
    }

    if (!contentType.empty()) { properties.contentType = spanOf(contentType); properties.markPresent(MQ_PROP_CONTENT_TYPE); }
    if (!responseTopic.empty()) { properties.responseTopic = spanOf(responseTopic); properties.markPresent(MQ_PROP_RESPONSE_TOPIC); }
    if (hasCorrelation) { properties.correlationData = spanOf(correlation); properties.markPresent(MQ_PROP_CORRELATION_DATA); }
    if (hasFormat) { properties.payloadFormatIndicator = format; properties.markPresent(MQ_PROP_PAYLOAD_FORMAT_INDICATOR); }
    if (hasExpiry) { properties.messageExpiryInterval = expiry; properties.markPresent(MQ_PROP_MESSAGE_EXPIRY_INTERVAL); }
    for (const auto& user : users) properties.addUserProperty(spanOf(user.first), spanOf(user.second));

    KafkaOrigin origin;
    origin.cluster = from.cluster;
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
        if (!mq_topicNameIsValid(message.topic)) {
            from.counters.invalid.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        // Committed either way (at-least-once is about reaching the broker, and a
        // refusal would refuse again on a re-read), but a refusal is COUNTED: a
        // subscriber's queue or the retained store was full. A record no session
        // subscribes to reaches nobody and is not counted here - with several
        // shards, this one cannot know whether another has a subscriber.
        if (!broker->publishMessage(message)) {
            from.counters.refused.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        from.counters.published.fetch_add(1, std::memory_order_relaxed);
    }
}

void KafkaPipe::Shard::consume(size_t mappingIndex, rd_kafka_t* consumer) {
    FromKafka& from = *pipe->_fromKafka[mappingIndex];
    for (int n = 0; n < kConsumeBudget; n++) {
        rd_kafka_message_t* record = rd_kafka_consumer_poll(consumer, 0);
        if (record == nullptr) return;
        if (record->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            from.counters.polled.fetch_add(1, std::memory_order_relaxed);
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
    uint64_t wakes = 0;
    const bool woken = ::read(shard.wakeFd, &wakes, sizeof wakes) == (ssize_t)sizeof wakes;
    const mq_time_t now = mq_millis();
    if (!woken && !shard.backlog && mq_elapsed(shard.servedAt, now) < kServiceIntervalMs) return;
    shard.servedAt = now;
    shard.backlog = false;
    for (rd_kafka_t* producer : shard.producers) {
        if (producer == nullptr) continue;
        // Every queued delivery report, not only the first.
        for (int n = 0; n < 64 && rd_kafka_poll(producer, 0) > 0; n++) {
        }
    }
    for (auto& consumer : shard.consumers) shard.consume(consumer.first, consumer.second);
}

bool KafkaPipe::hasPendingWork(uint32_t shardIndex) {
    return _shards[shardIndex]->backlog;
}

void KafkaPipe::stop(uint32_t shardIndex, MqttBroker& broker) {
    Shard& shard = *_shards[shardIndex];
    shard.broker = &broker;
    // Consumers first: closing commits the offsets already stored, and no
    // more records arrive to publish into a broker that is about to end.
    for (auto& consumer : shard.consumers) rd_kafka_consumer_close(consumer.second);
    // Then what is in flight to Kafka, within a bound: each delivery report
    // concludes its held PUBACK while the client is still connected.
    for (rd_kafka_t* producer : shard.producers) {
        if (producer != nullptr) rd_kafka_flush(producer, _flushMillis);
    }
    shard.broker = nullptr;
}

KafkaPipeStatus KafkaPipe::status() const {
    KafkaPipeStatus out;
    out.kafkaReachable = !_brokersDown.load(std::memory_order_relaxed);
    for (const std::unique_ptr<ToKafka>& to : _toKafka) {
        KafkaPipeStatus::Mapping m;
        m.id = to->config.id;
        m.toKafka = true;
        m.send = to->counters.send.load();
        m.success = to->counters.success.load();
        m.failed = to->counters.failed.load();
        m.tooLarge = to->counters.tooLarge.load();
        m.latencyMicros = to->counters.latencyMicros.load();
        out.mappings.push_back(m);
    }
    for (const std::unique_ptr<FromKafka>& from : _fromKafka) {
        KafkaPipeStatus::Mapping m;
        m.id = from->config.id;
        m.toKafka = false;
        m.polled = from->counters.polled.load();
        m.published = from->counters.published.load();
        m.invalid = from->counters.invalid.load();
        m.refused = from->counters.refused.load();
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

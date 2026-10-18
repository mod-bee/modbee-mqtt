// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaRetained.h"
#include "KafkaCommon.h"

#include <librdkafka/rdkafka.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

namespace {

// How long start-up may take to reach Kafka and read the topic.
const int kLoadTimeoutMs = 30000;

// How long shutdown waits for writes still in flight.
const int kFlushTimeoutMs = 5000;

class RetainedKafka : public RetainedBackend {
public:
    RetainedKafka(const KafkaClusterConfig& cluster, const std::string& topic)
        : _cluster(cluster), _topic(topic) {}

    ~RetainedKafka() override {
        _quit = true;
        if (_poller.joinable()) _poller.join();
        if (_producer) rd_kafka_destroy(_producer);
    }

    bool load(RetainedSet& into, std::string& error) override {
        if (!makeProducer(error)) return false;
        if (!ensureTopic(error)) return false;
        if (!readAll(into, error)) return false;
        _poller = std::thread([this] {
            while (!_quit) rd_kafka_poll(_producer, 200);
        });
        return true;
    }

    void stored(const RetainedRecord& r) override {
        rd_kafka_headers_t* headers = rd_kafka_headers_new(8);
        const auto add = [&](const char* name, const std::string& value) {
            rd_kafka_header_add(headers, name, -1, value.data(), (ssize_t)value.size());
        };
        add("mqtt-qos", std::to_string(r.qos));
        if (r.hasFormat) add("mqtt-payload-format-indicator", std::to_string(r.format));
        if (!r.contentType.empty()) add("mqtt-content-type", r.contentType);
        if (!r.responseTopic.empty()) add("mqtt-response-topic", r.responseTopic);
        if (r.hasCorrelation) add("mqtt-correlation-data", r.correlation);
        // The interval as published; the record's timestamp is when it was
        // stored, so the moment it expires is both together.
        if (r.expiresAt != 0) add("mqtt-message-expiry-interval", std::to_string(r.expiryInterval));
        for (const auto& user : r.userProperties) {
            std::string name = user.first;
            if (name.compare(0, 5, "mqtt-") == 0) name = kUserPropertyEscape + name;
            add(name.c_str(), user.second);
        }
        produce(r.topic, r.payload.data(), r.payload.size(), headers,
                r.expiresAt != 0 ? (int64_t)(r.expiresAt - (uint64_t)r.expiryInterval * 1000u) : 0);
    }

    void cleared(const std::string& topic) override {
        // A tombstone: no value at all, which compaction removes in time.
        produce(topic, nullptr, 0, nullptr, 0);
    }

    void flush() override {
        if (_producer) rd_kafka_flush(_producer, kFlushTimeoutMs);
        const int left = _producer ? rd_kafka_outq_len(_producer) : 0;
        if (left > 0) {
            std::fprintf(stderr, "modbee serve: %d retained change(s) were not written to Kafka "
                                 "topic %s before shutdown\n", left, _topic.c_str());
        }
    }

    std::string where() const override { return "Kafka topic " + _topic; }
    uint64_t failures() const override { return _failures.load(); }

private:
    bool makeProducer(std::string& error) {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        const bool ok = kafkaClusterSettings(conf, _cluster, "modbee-retained", error) &&
                        // In order per topic, never duplicated by a retry.
                        kafkaSet(conf, "enable.idempotence", "true", error) &&
                        kafkaSet(conf, "acks", "all", error) &&
                        kafkaSet(conf, "linger.ms", "5", error) &&
                        kafkaSet(conf, "message.timeout.ms", "30000", error) &&
                        // Not librdkafka's gigabyte: see KafkaPipe.cpp.
                        kafkaSet(conf, "queue.buffering.max.kbytes", "32768", error) &&
                        kafkaPassThrough(conf, _cluster, error);
        if (!ok) {
            rd_kafka_conf_destroy(conf);
            error = "retained store, cluster " + _cluster.id + ": " + error;
            return false;
        }
        rd_kafka_conf_set_opaque(conf, this);
        rd_kafka_conf_set_dr_msg_cb(conf, &RetainedKafka::onDelivery);
        char reason[512];
        _producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, reason, sizeof reason);
        if (_producer == nullptr) {
            error = std::string("retained store: ") + reason;
            return false;
        }
        return true;
    }

    static void onDelivery(rd_kafka_t*, const rd_kafka_message_t* message, void* opaque) {
        if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) return;
        RetainedKafka* self = static_cast<RetainedKafka*>(opaque);
        if (self->_failures.fetch_add(1) == 0) {
            std::fprintf(stderr, "modbee serve: a retained change was not written to Kafka "
                                 "topic %s: %s (counted in the status line)\n",
                         self->_topic.c_str(), rd_kafka_err2str(message->err));
        }
    }

    void produce(const std::string& key, const void* value, size_t length,
                 rd_kafka_headers_t* headers, int64_t timestamp) {
        rd_kafka_vu_t vu[6];
        size_t n = 0;
        vu[n].vtype = RD_KAFKA_VTYPE_TOPIC; vu[n].u.cstr = _topic.c_str(); n++;
        vu[n].vtype = RD_KAFKA_VTYPE_KEY;
        vu[n].u.mem.ptr = (void*)key.data();
        vu[n].u.mem.size = key.size(); n++;
        vu[n].vtype = RD_KAFKA_VTYPE_VALUE;
        vu[n].u.mem.ptr = (void*)value;
        vu[n].u.mem.size = length; n++;
        if (headers) { vu[n].vtype = RD_KAFKA_VTYPE_HEADERS; vu[n].u.headers = headers; n++; }
        if (timestamp > 0) { vu[n].vtype = RD_KAFKA_VTYPE_TIMESTAMP; vu[n].u.i64 = timestamp; n++; }
        vu[n].vtype = RD_KAFKA_VTYPE_MSGFLAGS; vu[n].u.i32 = RD_KAFKA_MSG_F_COPY; n++;
        rd_kafka_error_t* failure = rd_kafka_produceva(_producer, vu, n);
        if (failure != nullptr) {
            if (_failures.fetch_add(1) == 0) {
                std::fprintf(stderr, "modbee serve: a retained change was not written to Kafka "
                                     "topic %s: %s (counted in the status line)\n",
                             _topic.c_str(), rd_kafka_error_string(failure));
            }
            rd_kafka_error_destroy(failure);
            if (headers) rd_kafka_headers_destroy(headers);
        }
    }

    // --- The topic: created compacted, or checked ---

    bool ensureTopic(std::string& error) {
        rd_kafka_queue_t* queue = rd_kafka_queue_new(_producer);
        char reason[512];
        // One partition: a retained store is not a throughput problem, and
        // one partition keeps every change to it in one order.
        rd_kafka_NewTopic_t* topic = rd_kafka_NewTopic_new(_topic.c_str(), 1, -1, reason, sizeof reason);
        rd_kafka_NewTopic_set_config(topic, "cleanup.policy", "compact");
        rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(_producer, RD_KAFKA_ADMIN_OP_CREATETOPICS);
        rd_kafka_AdminOptions_set_request_timeout(options, kLoadTimeoutMs, reason, sizeof reason);
        rd_kafka_CreateTopics(_producer, &topic, 1, options, queue);
        rd_kafka_event_t* event = rd_kafka_queue_poll(queue, kLoadTimeoutMs + 1000);
        bool ok = false;
        bool existed = false;
        if (event == nullptr) {
            error = "could not reach Kafka (" + _cluster.bootstrapServers + ") to set up the "
                    "retained store topic " + _topic;
        } else if (rd_kafka_event_error(event) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            error = std::string("retained store topic ") + _topic + ": " + rd_kafka_event_error_string(event);
        } else {
            size_t count = 0;
            const rd_kafka_topic_result_t** results =
                rd_kafka_CreateTopics_result_topics(rd_kafka_event_CreateTopics_result(event), &count);
            const rd_kafka_resp_err_t err = count == 1 ? rd_kafka_topic_result_error(results[0])
                                                       : RD_KAFKA_RESP_ERR_UNKNOWN;
            if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                ok = true;
            } else if (err == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
                ok = existed = true;
            } else {
                error = std::string("could not create the retained store topic ") + _topic + ": " +
                        (count == 1 && rd_kafka_topic_result_error_string(results[0])
                             ? rd_kafka_topic_result_error_string(results[0]) : rd_kafka_err2str(err));
            }
        }
        if (event) rd_kafka_event_destroy(event);
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_NewTopic_destroy(topic);
        if (ok && existed) warnIfNotCompacted(queue);
        rd_kafka_queue_destroy(queue);
        return ok;
    }

    void warnIfNotCompacted(rd_kafka_queue_t* queue) {
        rd_kafka_ConfigResource_t* resource =
            rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, _topic.c_str());
        rd_kafka_DescribeConfigs(_producer, &resource, 1, nullptr, queue);
        rd_kafka_event_t* event = rd_kafka_queue_poll(queue, kLoadTimeoutMs);
        std::string policy;
        if (event != nullptr && rd_kafka_event_error(event) == RD_KAFKA_RESP_ERR_NO_ERROR) {
            size_t count = 0;
            const rd_kafka_ConfigResource_t** results = rd_kafka_DescribeConfigs_result_resources(
                rd_kafka_event_DescribeConfigs_result(event), &count);
            for (size_t r = 0; r < count; r++) {
                size_t entries = 0;
                const rd_kafka_ConfigEntry_t** configs = rd_kafka_ConfigResource_configs(results[r], &entries);
                for (size_t e = 0; e < entries; e++) {
                    if (std::strcmp(rd_kafka_ConfigEntry_name(configs[e]), "cleanup.policy") == 0 &&
                        rd_kafka_ConfigEntry_value(configs[e]) != nullptr) {
                        policy = rd_kafka_ConfigEntry_value(configs[e]);
                    }
                }
            }
        }
        if (event) rd_kafka_event_destroy(event);
        rd_kafka_ConfigResource_destroy(resource);
        if (!policy.empty() && policy.find("compact") == std::string::npos) {
            std::fprintf(stderr, "modbee serve: warning: Kafka topic %s keeps retained messages but "
                                 "is not compacted (cleanup.policy=%s) - Kafka will delete those not "
                                 "republished within its retention time. Set cleanup.policy=compact.\n",
                         _topic.c_str(), policy.c_str());
        }
    }

    // --- Reading it all back ---

    bool readAll(RetainedSet& into, std::string& error) {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        // A group id because librdkafka wants one; nothing is ever committed,
        // and every start reads from the beginning.
        const bool ok = kafkaClusterSettings(conf, _cluster, "modbee-retained-loader", error) &&
                        kafkaSet(conf, "group.id", "modbee-retained-loader", error) &&
                        kafkaSet(conf, "enable.auto.commit", "false", error) &&
                        kafkaSet(conf, "enable.partition.eof", "true", error) &&
                        kafkaPassThrough(conf, _cluster, error);
        if (!ok) {
            rd_kafka_conf_destroy(conf);
            return false;
        }
        char reason[512];
        rd_kafka_t* consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, reason, sizeof reason);
        if (consumer == nullptr) {
            error = std::string("retained store: ") + reason;
            return false;
        }

        // The partitions, and where each ends now.
        const rd_kafka_metadata_t* metadata = nullptr;
        rd_kafka_topic_t* handle = rd_kafka_topic_new(consumer, _topic.c_str(), nullptr);
        rd_kafka_resp_err_t err = rd_kafka_metadata(consumer, 0, handle, &metadata, kLoadTimeoutMs);
        rd_kafka_topic_destroy(handle);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR || metadata->topic_cnt != 1) {
            error = std::string("could not read the retained store topic ") + _topic + ": " + rd_kafka_err2str(err);
            if (metadata) rd_kafka_metadata_destroy(metadata);
            rd_kafka_destroy(consumer);
            return false;
        }
        rd_kafka_topic_partition_list_t* assignment = rd_kafka_topic_partition_list_new(1);
        size_t waiting = 0;
        for (int p = 0; p < metadata->topics[0].partition_cnt; p++) {
            const int32_t partition = metadata->topics[0].partitions[p].id;
            int64_t low = 0, high = 0;
            err = rd_kafka_query_watermark_offsets(consumer, _topic.c_str(), partition, &low, &high,
                                                   kLoadTimeoutMs);
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                error = std::string("could not read the retained store topic ") + _topic + ": " +
                        rd_kafka_err2str(err);
                rd_kafka_metadata_destroy(metadata);
                rd_kafka_topic_partition_list_destroy(assignment);
                rd_kafka_destroy(consumer);
                return false;
            }
            if (high > low) {
                rd_kafka_topic_partition_list_add(assignment, _topic.c_str(), partition)->offset =
                    RD_KAFKA_OFFSET_BEGINNING;
                waiting++;
            }
        }
        rd_kafka_metadata_destroy(metadata);

        bool complete = true;
        if (waiting > 0) {
            rd_kafka_assign(consumer, assignment);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kLoadTimeoutMs);
            while (waiting > 0) {
                if (std::chrono::steady_clock::now() > deadline) {
                    complete = false;
                    break;
                }
                rd_kafka_message_t* record = rd_kafka_consumer_poll(consumer, 500);
                if (record == nullptr) continue;
                if (record->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    waiting--;
                } else if (record->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    error = std::string("reading the retained store topic ") + _topic + ": " +
                            rd_kafka_message_errstr(record);
                    complete = false;
                    rd_kafka_message_destroy(record);
                    break;
                } else {
                    apply(record, into);
                }
                rd_kafka_message_destroy(record);
            }
            rd_kafka_assign(consumer, nullptr);
        }
        rd_kafka_topic_partition_list_destroy(assignment);
        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
        if (!complete && error.empty()) {
            error = "timed out reading the retained store topic " + _topic;
        }
        return complete;
    }

    static void apply(const rd_kafka_message_t* record, RetainedSet& into) {
        if (record->key == nullptr || record->key_len == 0) return;
        const std::string topic((const char*)record->key, record->key_len);
        if (record->payload == nullptr || record->len == 0) {
            into.erase(topic);   // a tombstone: cleared
            return;
        }
        RetainedRecord r;
        r.topic = topic;
        r.payload.assign((const char*)record->payload, record->len);
        rd_kafka_headers_t* headers = nullptr;
        if (rd_kafka_message_headers(record, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR) {
            const char* name;
            const void* value;
            size_t size;
            for (size_t i = 0; rd_kafka_header_get_all(headers, i, &name, &value, &size) ==
                               RD_KAFKA_RESP_ERR_NO_ERROR; i++) {
                const std::string v = value ? std::string((const char*)value, size) : std::string();
                if (std::strcmp(name, "mqtt-qos") == 0) r.qos = (uint8_t)(v == "2" ? 2 : v == "1" ? 1 : 0);
                else if (std::strcmp(name, "mqtt-payload-format-indicator") == 0) { r.hasFormat = true; r.format = v == "1" ? 1 : 0; }
                else if (std::strcmp(name, "mqtt-content-type") == 0) r.contentType = v;
                else if (std::strcmp(name, "mqtt-response-topic") == 0) r.responseTopic = v;
                else if (std::strcmp(name, "mqtt-correlation-data") == 0) { r.hasCorrelation = true; r.correlation = v; }
                else if (std::strcmp(name, "mqtt-message-expiry-interval") == 0) {
                    r.expiryInterval = (uint32_t)std::strtoul(v.c_str(), nullptr, 10);
                }
                else if (std::strncmp(name, kUserPropertyEscape, sizeof kUserPropertyEscape - 1) == 0) {
                    r.userProperties.push_back(std::make_pair(std::string(name + sizeof kUserPropertyEscape - 1), v));
                }
                else if (std::strncmp(name, "mqtt-", 5) == 0) { /* informational */ }
                else r.userProperties.push_back(std::make_pair(std::string(name), v));
            }
        }
        if (r.expiryInterval != 0) {
            rd_kafka_timestamp_type_t type;
            const int64_t at = rd_kafka_message_timestamp(record, &type);
            const uint64_t storedAt = at > 0 ? (uint64_t)at : retainedNowUtc();
            r.expiresAt = storedAt + (uint64_t)r.expiryInterval * 1000u;
        }
        into[topic] = std::move(r);
    }

    const KafkaClusterConfig _cluster;
    const std::string _topic;
    rd_kafka_t* _producer = nullptr;
    std::thread _poller;
    std::atomic<bool> _quit{false};
    std::atomic<uint64_t> _failures{0};
};

} // namespace

std::unique_ptr<RetainedBackend> makeRetainedKafka(const KafkaClusterConfig& cluster,
                                                   const std::string& topic) {
    return std::unique_ptr<RetainedBackend>(new RetainedKafka(cluster, topic));
}

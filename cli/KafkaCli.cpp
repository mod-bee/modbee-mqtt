// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaCli.h"
#include "Common.h"

#include <cstdio>
#include <iterator>

std::vector<ArgSpec> withKafkaSpecs(std::vector<ArgSpec> specs) {
    static const ArgSpec kafka[] = {
        {"kafka", 0, true, "Kafka instead of MQTT: its bootstrap servers, host:port[,host:port]"},
        {"kafka-topic", 0, true, "the Kafka topic: written to (pub, req), read from (sub; repeatable)"},
        {"kafka-reply-topic", 0, true, "req: the Kafka topic the reply comes on (default --kafka-topic)"},
        {"kafka-key", 0, true, "pub, req: the record key - the MQTT topic (default), none, or this text"},
        {"kafka-group", 0, true, "sub: read as a member of this consumer group (default: no group)"},
        {"kafka-from-start", 0, false, "sub: from each partition's start, not its end"},
        {"kafka-tls", 0, false, "Kafka over TLS"},
        {"kafka-ca", 0, true, "Kafka TLS: the CA file to trust (default the system's)"},
        {"kafka-cert", 0, true, "Kafka TLS: a client certificate file"},
        {"kafka-key-file", 0, true, "Kafka TLS: its key file"},
        {"kafka-insecure", 0, false, "Kafka TLS: do not check the broker's host name"},
        {"kafka-sasl", 0, true, "Kafka login: plain, scram-sha256 or scram-sha512"},
        {"kafka-user", 0, true, "Kafka login: user name"},
        {"kafka-password", 0, true, "Kafka login: password"},
        {"kafka-password-file", 0, true, "Kafka login: a file holding the password"},
        {"kafka-set", 0, true, "any librdkafka setting, name=value (repeatable)"},
    };
    specs.insert(specs.end(), std::begin(kafka), std::end(kafka));
    return specs;
}

bool kafkaAsked(const Args& args, const char* command, bool& refused) {
    refused = false;
    if (!args.has("kafka")) return false;
#if defined(MODBEE_HAVE_KAFKA)
    (void)command;
    return true;
#else
    std::fprintf(stderr, "modbee-mqtt %s: --kafka needs a build with Kafka (librdkafka was not found when this "
                         "one was built)\n", command);
    refused = true;
    return false;
#endif
}

#if defined(MODBEE_HAVE_KAFKA)

#include "KafkaCommon.h"
#include "KafkaEnvelope.h"

#include <librdkafka/rdkafka.h>

#include <fstream>
#include <sstream>

struct CliKafka::State {
    KafkaClusterConfig cluster;
    std::string key = "topic";
    std::string group;
    bool fromStart = false;
    rd_kafka_t* producer = nullptr;
    size_t outstanding = 0, failed = 0;
    rd_kafka_t* consumer = nullptr;
    rd_kafka_message_t* current = nullptr;
    KafkaEnvelope envelope;
    MqttProperties properties;
    std::string topic;

    static void delivered(rd_kafka_t*, const rd_kafka_message_t* record, void* opaque) {
        State& s = *static_cast<State*>(opaque);
        if (s.outstanding > 0) s.outstanding--;
        if (record->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            s.failed++;
            std::fprintf(stderr, "kafka: a record was refused: %s\n", rd_kafka_err2str(record->err));
        }
    }
};

namespace {

MqttSpan spanOf(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

} // namespace

CliKafka::CliKafka() : _s(new State()) {}

CliKafka::~CliKafka() {
    if (_s->current) rd_kafka_message_destroy(_s->current);
    if (_s->consumer) {
        rd_kafka_consumer_close(_s->consumer);
        rd_kafka_destroy(_s->consumer);
    }
    if (_s->producer) rd_kafka_destroy(_s->producer);
}

bool CliKafka::configure(const Args& args, std::string& error) {
    KafkaClusterConfig& c = _s->cluster;
    c.id = "cli";
    c.bootstrapServers = args.get("kafka");
    c.tls = args.has("kafka-tls") || args.has("kafka-ca") || args.has("kafka-cert");
    c.caFile = args.get("kafka-ca");
    c.certificateFile = args.get("kafka-cert");
    c.keyFile = args.get("kafka-key-file");
    c.verifyHostname = !args.has("kafka-insecure");
    if (!c.certificateFile.empty() && c.keyFile.empty()) {
        error = "--kafka-cert needs --kafka-key-file";
        return false;
    }
    c.mechanism = args.get("kafka-sasl", "none");
    if (c.mechanism != "none" && c.mechanism != "plain" && c.mechanism != "scram-sha256" &&
        c.mechanism != "scram-sha512") {
        error = "--kafka-sasl is plain, scram-sha256 or scram-sha512, not \"" + c.mechanism + "\"";
        return false;
    }
    c.username = args.get("kafka-user");
    c.password = args.get("kafka-password");
    if (args.has("kafka-password-file")) {
        std::ifstream in(args.get("kafka-password-file"));
        std::stringstream text;
        text << in.rdbuf();
        if (!in) {
            error = "cannot read " + args.get("kafka-password-file");
            return false;
        }
        c.password = text.str();
        while (!c.password.empty() && (c.password.back() == '\n' || c.password.back() == '\r')) c.password.pop_back();
    }
    for (const std::string& pair : args.all("kafka-set")) {
        const size_t equals = pair.find('=');
        if (equals == std::string::npos || equals == 0) {
            error = "--kafka-set wants name=value, not \"" + pair + "\"";
            return false;
        }
        c.properties.push_back(std::make_pair(pair.substr(0, equals), pair.substr(equals + 1)));
    }
    _s->key = args.get("kafka-key", "topic");
    _s->group = args.get("kafka-group");
    _s->fromStart = args.has("kafka-from-start");
    if (c.bootstrapServers.empty()) {
        error = "--kafka needs the bootstrap servers, host:port";
        return false;
    }
    return true;
}

bool CliKafka::openProducer(std::string& error) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    // Idempotent, as the pipe's: a retried record is written once, in order.
    if (!kafkaClusterSettings(conf, _s->cluster, "modbee-cli", error) ||
        !kafkaSet(conf, "enable.idempotence", "true", error) || !kafkaSet(conf, "linger.ms", "5", error) ||
        !kafkaPassThrough(conf, _s->cluster, error)) {
        rd_kafka_conf_destroy(conf);
        return false;
    }
    rd_kafka_conf_set_dr_msg_cb(conf, &State::delivered);
    rd_kafka_conf_set_opaque(conf, _s.get());
    char reason[512];
    _s->producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, reason, sizeof reason);
    if (_s->producer == nullptr) {
        rd_kafka_conf_destroy(conf);
        error = reason;
        return false;
    }
    return true;
}

bool CliKafka::produce(const std::string& kafkaTopic, const std::string& topic, const uint8_t* payload,
                       size_t length, uint8_t qos, bool retain, const MqttOwnedProperties& properties,
                       std::string& error) {
    MqttProperties p;
    properties.describeTo(p);
    rd_kafka_headers_t* headers = rd_kafka_headers_new(8);
    kafkaWriteEnvelope(headers, nullptr, spanOf(topic), qos, retain, MqttSpan(), MqttSpan(), &p);
    const std::string key = _s->key == "topic" ? topic : _s->key;
    rd_kafka_vu_t vu[6];
    size_t n = 0;
    vu[n].vtype = RD_KAFKA_VTYPE_TOPIC; vu[n].u.cstr = kafkaTopic.c_str(); n++;
    vu[n].vtype = RD_KAFKA_VTYPE_VALUE; vu[n].u.mem.ptr = (void*)payload; vu[n].u.mem.size = length; n++;
    if (_s->key != "none") {
        vu[n].vtype = RD_KAFKA_VTYPE_KEY; vu[n].u.mem.ptr = (void*)key.data(); vu[n].u.mem.size = key.size(); n++;
    }
    vu[n].vtype = RD_KAFKA_VTYPE_HEADERS; vu[n].u.headers = headers; n++;
    vu[n].vtype = RD_KAFKA_VTYPE_MSGFLAGS; vu[n].u.i32 = RD_KAFKA_MSG_F_COPY; n++;
    for (;;) {
        rd_kafka_error_t* failure = rd_kafka_produceva(_s->producer, vu, n);
        if (failure == nullptr) break;
        const rd_kafka_resp_err_t code = rd_kafka_error_code(failure);
        error = rd_kafka_error_string(failure);
        rd_kafka_error_destroy(failure);
        if (code != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            rd_kafka_headers_destroy(headers);
            return false;
        }
        rd_kafka_poll(_s->producer, 50);    // full: room is made as Kafka answers
    }
    _s->outstanding++;
    rd_kafka_poll(_s->producer, 0);
    return true;
}

bool CliKafka::flush(int ms, size_t& failed) {
    rd_kafka_flush(_s->producer, ms);
    rd_kafka_poll(_s->producer, 0);
    failed = _s->failed;
    return _s->outstanding == 0;
}

bool CliKafka::openConsumer(const std::vector<std::string>& topics, std::string& error) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    const bool grouped = !_s->group.empty();
    const std::string group = grouped ? _s->group : "modbee-cli-" + std::to_string(cliProcessId());
    if (!kafkaClusterSettings(conf, _s->cluster, "modbee-cli", error) || !kafkaSet(conf, "group.id", group, error) ||
        !kafkaSet(conf, "enable.auto.commit", grouped ? "true" : "false", error) ||
        !kafkaSet(conf, "auto.offset.reset", _s->fromStart ? "earliest" : "latest", error) ||
        !kafkaPassThrough(conf, _s->cluster, error)) {
        rd_kafka_conf_destroy(conf);
        return false;
    }
    char reason[512];
    _s->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, reason, sizeof reason);
    if (_s->consumer == nullptr) {
        rd_kafka_conf_destroy(conf);
        error = reason;
        return false;
    }
    rd_kafka_poll_set_consumer(_s->consumer);
    rd_kafka_topic_partition_list_t* list = rd_kafka_topic_partition_list_new((int)topics.size());
    rd_kafka_resp_err_t result = RD_KAFKA_RESP_ERR_NO_ERROR;
    if (grouped) {
        for (const std::string& t : topics) rd_kafka_topic_partition_list_add(list, t.c_str(), RD_KAFKA_PARTITION_UA);
        result = rd_kafka_subscribe(_s->consumer, list);
    } else {
        // Every partition, at the offset its end (or start) is now - asked,
        // not left to "end": a record written straight after this returns is
        // one this reader sees.
        for (const std::string& t : topics) {
            rd_kafka_topic_t* handle = rd_kafka_topic_new(_s->consumer, t.c_str(), nullptr);
            const rd_kafka_metadata_t* metadata = nullptr;
            result = rd_kafka_metadata(_s->consumer, 0, handle, &metadata, 10000);
            rd_kafka_topic_destroy(handle);
            if (result != RD_KAFKA_RESP_ERR_NO_ERROR) break;
            if (metadata->topic_cnt != 1 || metadata->topics[0].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                result = metadata->topic_cnt == 1 ? metadata->topics[0].err : RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
                rd_kafka_metadata_destroy(metadata);
                break;
            }
            for (int p = 0; p < metadata->topics[0].partition_cnt; p++) {
                const int32_t partition = metadata->topics[0].partitions[p].id;
                int64_t low = 0, high = 0;
                result = rd_kafka_query_watermark_offsets(_s->consumer, t.c_str(), partition, &low, &high, 10000);
                if (result != RD_KAFKA_RESP_ERR_NO_ERROR) break;
                rd_kafka_topic_partition_list_add(list, t.c_str(), partition)->offset = _s->fromStart ? low : high;
            }
            rd_kafka_metadata_destroy(metadata);
            if (result != RD_KAFKA_RESP_ERR_NO_ERROR) break;
        }
        if (result == RD_KAFKA_RESP_ERR_NO_ERROR) result = rd_kafka_assign(_s->consumer, list);
    }
    rd_kafka_topic_partition_list_destroy(list);
    if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
        error = std::string("cannot read ") + (topics.empty() ? "" : topics[0]) + ": " + rd_kafka_err2str(result);
        return false;
    }
    return true;
}

bool CliKafka::next(int ms, MqttMessage& out) {
    State& s = *_s;
    if (s.current) {
        rd_kafka_message_destroy(s.current);
        s.current = nullptr;
    }
    rd_kafka_message_t* record = rd_kafka_consumer_poll(s.consumer, ms);
    if (record == nullptr) return false;
    if (record->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (record->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            std::fprintf(stderr, "kafka: %s\n", rd_kafka_message_errstr(record));
        }
        rd_kafka_message_destroy(record);
        return false;
    }
    s.current = record;
    s.envelope = KafkaEnvelope();
    rd_kafka_headers_t* headers = nullptr;
    if (rd_kafka_message_headers(record, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR &&
        !kafkaReadEnvelope(headers, s.envelope)) {
        std::fprintf(stderr, "kafka: a record in envelope version %s, which this cannot read - skipped\n",
                     s.envelope.version.c_str());
        return false;
    }
    const KafkaEnvelope& e = s.envelope;
    s.topic = e.hasTopic ? e.topic : rd_kafka_topic_name(record->rkt);
    s.properties = MqttProperties();
    MqttProperties& p = s.properties;
    if (e.hasContentType) { p.contentType = spanOf(e.contentType); p.markPresent(MQ_PROP_CONTENT_TYPE); }
    if (e.hasResponseTopic) { p.responseTopic = spanOf(e.responseTopic); p.markPresent(MQ_PROP_RESPONSE_TOPIC); }
    if (e.hasCorrelation) { p.correlationData = spanOf(e.correlation); p.markPresent(MQ_PROP_CORRELATION_DATA); }
    if (e.hasFormat) { p.payloadFormatIndicator = e.format; p.markPresent(MQ_PROP_PAYLOAD_FORMAT_INDICATOR); }
    if (e.hasExpiry) { p.messageExpiryInterval = e.expiry; p.markPresent(MQ_PROP_MESSAGE_EXPIRY_INTERVAL); }
    for (const auto& user : e.users) p.addUserProperty(spanOf(user.first), spanOf(user.second));
    out = MqttMessage();
    out.topic = spanOf(s.topic);
    out.payload.data = (const uint8_t*)record->payload;
    out.payload.length = (uint32_t)record->len;
    out.qos = e.hasQos ? e.qos : 0;
    out.retain = e.hasRetain && e.retain;
    out.properties = &p;
    return true;
}

#endif

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaLinks.h"
#include "KafkaEnvelope.h"
#include "KafkaPipe.h"
#include "RetainedKeeper.h"
#include "ServerOs.h"
#include "ShardAccounts.h"
#include "ShardedBroker.h"

#include <cstdio>

namespace {

// The largest message a link carries either way: Kafka's own default
// record size, and room for the MQTT packet around it.
const size_t kMaxRecordBytes = 1048576;
const uint32_t kMaxPacket = (uint32_t)kMaxRecordBytes + 65536;

// Kafka's rule: 1 to 249 of [A-Za-z0-9._-], and not "." or "..".
bool kafkaTopicOk(const std::string& topic) {
    if (topic.empty() || topic.size() > 249 || topic == "." || topic == "..") return false;
    for (char c : topic) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' ||
                        c == '_' || c == '-';
        if (!ok) return false;
    }
    return true;
}

std::string randomText(size_t length) {
    static const char digits[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::vector<uint8_t> bytes(length);
    server_os::randomBytes(bytes.data(), bytes.size());
    std::string out;
    for (uint8_t b : bytes) out.push_back(digits[b % 62]);
    return out;
}

std::string groupOf(const std::string& id, const AppKafkaLinkSpec& link) {
    return link.consumerGroup.empty() ? "modbee-conn-" + id : link.consumerGroup;
}

// Everything a link's broker and pipe are made from: two links with the same
// signature are the same link.
std::string signatureOf(const ServerConfig& section) {
    std::string s;
    for (const KafkaClusterConfig& c : section.clusters) {
        s += c.id + '\n' + c.bootstrapServers + '\n' + (c.tls ? "tls" : "plain") + '\n' + c.caFile + '\n' +
             c.certificateFile + '\n' + c.keyFile + '\n' + (c.verifyHostname ? "v" : "t") + '\n' + c.mechanism +
             '\n' + c.username + '\n' + c.password + '\n';
        for (const auto& p : c.properties) s += p.first + '=' + p.second + '\n';
    }
    for (const MqttToKafkaConfig& m : section.toKafka) {
        s += "out " + m.kafkaTopic + ' ' + std::to_string((int)m.key) + ' ' + m.keyTemplate + '\n';
        for (const std::string& f : m.excludeFilters) s += "exclude " + f + '\n';
        for (const KafkaHeaderSpec& h : m.headers) s += "header " + h.name + '=' + h.value + '\n';
    }
    for (const KafkaToMqttConfig& m : section.fromKafka) {
        s += "in " + m.kafkaTopicPattern + ' ' + m.group + '\n';
        for (const std::string& t : m.kafkaTopics) s += t + ',';
    }
    return s;
}

} // namespace

// One link: its broker, the pipe and the login in front of it.
struct KafkaLinks::Link {
    std::string signature;
    ServerConfig section;                  // what its pipe runs
    std::string cluster;
    std::string user, password;
    ShardAccounts accounts;
    std::unique_ptr<KafkaPipe> pipe;
    ShardExtensionChain chain;
    ShardedBroker broker;

    ~Link() { broker.stop(); }
};

KafkaLinks::KafkaLinks(KafkaControl& clusters) {
    _clusters = clusters.section().clusters;
    clusters.share([this](const std::string& cluster) { return usedBy(cluster); },
                   [this](const ServerConfig& section) { clustersChanged(section); });
}

KafkaLinks::~KafkaLinks() { closeAll(); }

bool KafkaLinks::sectionFor(const std::string& id, const AppKafkaLinkSpec& link, ServerConfig& out,
                            std::string& error) const {
    // Under _lock.
    const KafkaClusterConfig* cluster = nullptr;
    if (link.cluster.empty()) {
        if (_clusters.size() == 1) cluster = &_clusters[0];
        else {
            error = _clusters.empty() ? "a kafka connection needs a Kafka cluster, and there is none (Kafka.PutCluster)"
                                      : "there are several Kafka clusters: say which (kafka.cluster)";
            return false;
        }
    } else {
        for (const KafkaClusterConfig& c : _clusters) if (c.id == link.cluster) cluster = &c;
        if (cluster == nullptr) {
            error = "no Kafka cluster \"" + link.cluster + "\" (Kafka.PutCluster)";
            return false;
        }
    }
    if (link.outTopic.empty() && link.inTopics.empty() && link.inPattern.empty()) {
        error = "a kafka connection needs a Kafka topic to write to (kafka_topic), to read from (kafka_topics or "
                "kafka_topic_pattern), or both";
        return false;
    }
    if (!link.inPattern.empty() && link.inPattern[0] != '^') {
        error = "kafka_topic_pattern is a regular expression starting with ^ (\"^plant-.*\"), as Kafka's clients take one";
        return false;
    }
    if (!link.inTopics.empty() && !link.inPattern.empty()) {
        error = "kafka_topics or kafka_topic_pattern, not both";
        return false;
    }
    if (!link.outTopic.empty() && !kafkaTopicOk(link.outTopic)) {
        error = "\"" + link.outTopic + "\" is not a valid Kafka topic name (1 to 249 of letters, digits, '.', '_' and '-')";
        return false;
    }
    for (const std::string& t : link.inTopics) {
        if (!kafkaTopicOk(t)) {
            error = "\"" + t + "\" is not a valid Kafka topic name (1 to 249 of letters, digits, '.', '_' and '-')";
            return false;
        }
    }
    const std::string group = groupOf(id, link);
    out = ServerConfig();
    out.clusters.push_back(*cluster);
    if (!link.outTopic.empty()) {
        MqttToKafkaConfig m;
        m.id = "conn-" + id + "-out";
        m.cluster = cluster->id;
        m.filters.push_back("#");
        m.kafkaTopic = link.outTopic;
        m.maxRecordBytes = kMaxRecordBytes;
        m.allHeaders = true;
        m.acknowledgeOnKafka = true;
        m.messageTimeoutMs = 0;              // until Kafka has it
        if (link.key.empty() || link.key == "topic") m.key = KafkaKeyMode::Topic;
        else if (link.key == "clientid") m.key = KafkaKeyMode::ClientId;
        else if (link.key == "none") m.key = KafkaKeyMode::None;
        else if (link.key.find("${") != std::string::npos) {
            m.key = KafkaKeyMode::Template;
            m.keyTemplate = link.key;
        } else {
            error = "key \"" + link.key + "\": topic, clientid, none, or a template with ${MQTT_TOPIC}, "
                    "${MQTT_CLIENTID} or ${MQTT_TOPIC_LEVEL_n}";
            return false;
        }
        KafkaHeaderSpec mark;
        mark.fixed = true;
        mark.name = kEnvelopeLinkHeader;
        mark.value = group;
        m.headers.push_back(mark);
        if (!link.stateConnection.empty()) m.excludeFilters.push_back("spBv1.0/STATE/#");
        out.toKafka.push_back(m);
    }
    if (!link.inTopics.empty() || !link.inPattern.empty()) {
        KafkaToMqttConfig m;
        m.id = "conn-" + id + "-in";
        m.cluster = cluster->id;
        m.kafkaTopics = link.inTopics;
        m.kafkaTopicPattern = link.inPattern;
        m.mqttTopics.push_back("${KAFKA_TOPIC}");   // a record with no mqtt-topic header
        m.fromHeaders = true;
        m.qos = 1;
        m.group = group;
        m.skipLink = group;
        out.fromKafka.push_back(m);
    }
    return true;
}

bool KafkaLinks::check(const std::string& id, const AppKafkaLinkSpec& link, std::string& error) {
    std::lock_guard<std::mutex> hold(_lock);
    ServerConfig section;
    return sectionFor(id, link, section, error);
}

bool KafkaLinks::open(const std::string& id, const AppKafkaLinkSpec& link, AppLocalAddress& where,
                      std::string& error) {
    std::lock_guard<std::mutex> hold(_lock);
    ServerConfig section;
    if (!sectionFor(id, link, section, error)) return false;
    const std::string signature = signatureOf(section);
    std::unique_ptr<Link>& slot = _links[id];
    if (!slot || slot->signature != signature) {
        // Made again: what the old one holds for Kafka is given a few seconds.
        if (slot) reap(std::move(slot));
        std::unique_ptr<Link> made(new Link());
        made->signature = signature;
        made->section = section;
        made->cluster = section.clusters[0].id;
        made->user = id;
        made->password = randomText(32);
        made->accounts.add(made->user, made->password);
        made->pipe.reset(new KafkaPipe(section));
        made->pipe->setShutdownFlushMillis(3000);
        made->chain.add(&made->accounts);
        made->chain.add(made->pipe.get());
        ShardedBrokerConfig c;
        c.address = "127.0.0.1";
        c.port = 0;
        c.shards = 1;
        c.broker.maxConnections = 4;
        c.broker.maxSessions = 4;
        c.broker.connectionCapacity = 4;
        c.broker.receiveBufferSize = kMaxPacket;
        c.broker.transmitBufferSize = kMaxPacket;
        c.broker.maximumPacketSize = kMaxPacket;
        c.broker.receiveMaximum = 256;
        c.broker.maxSubscriptionsPerSession = 4096;
        // What Kafka hands the connection faster than it takes it waits here.
        c.broker.maxQueuedPerSession = 50000;
        c.broker.maxQueuedBytesPerSession = 64u << 20;
        // Retained messages Kafka's records ask it to keep - each costs room
        // whether used or not, and a producer could ask for many.
        c.broker.maxRetained = 1000;
        c.broker.maximumSessionExpiry = 0xFFFFFFFFu;
        c.broker.allowAnonymous = false;
        // Grown as used: the limits above are what it may reach, not what an
        // idle link costs (reserved, they were hundreds of megabytes each).
        c.broker.reserveUpFront = false;
        c.extension = &made->chain;
        if (!made->broker.start(c)) {
            error = "the kafka connection's broker could not start: " + made->broker.lastError();
            _links.erase(id);
            return false;
        }
        slot = std::move(made);
    }
    where.host = "127.0.0.1";
    where.port = slot->broker.port();
    where.tls = false;
    where.hasLogin = true;
    where.user = slot->user;
    where.password = slot->password;
    return true;
}

void KafkaLinks::close(const std::string& id) {
    std::unique_ptr<Link> gone;
    {
        std::lock_guard<std::mutex> hold(_lock);
        auto at = _links.find(id);
        if (at == _links.end()) return;
        gone = std::move(at->second);
        _links.erase(at);
    }
    reap(std::move(gone));
}

void KafkaLinks::reap(std::unique_ptr<Link> link) {
    std::lock_guard<std::mutex> hold(_reaping);
    if (_reapers.size() >= 16) {
        // Long done, as a rule: joined, so the list does not grow.
        for (std::thread& t : _reapers) t.join();
        _reapers.clear();
    }
    std::shared_ptr<Link> owned(link.release());
    _reapers.emplace_back([owned]() mutable { owned.reset(); });
}

void KafkaLinks::closeAll() {
    std::map<std::string, std::unique_ptr<Link>> gone;
    {
        std::lock_guard<std::mutex> hold(_lock);
        gone.swap(_links);
    }
    for (auto& one : gone) reap(std::move(one.second));
    std::lock_guard<std::mutex> hold(_reaping);
    for (std::thread& t : _reapers) t.join();
    _reapers.clear();
}

bool KafkaLinks::status(const std::string& id, AppKafkaLinkStatus& out) const {
    std::lock_guard<std::mutex> hold(_lock);
    auto at = _links.find(id);
    if (at == _links.end()) return false;
    const KafkaPipeStatus pipe = at->second->pipe->status();
    out = AppKafkaLinkStatus();
    out.reachable = pipe.kafkaReachable;
    out.held = pipe.heldAcknowledgements;
    for (const KafkaPipeStatus::Mapping& m : pipe.mappings) {
        if (m.toKafka) {
            out.recordsOut += m.success;
            out.recordsOutFailed += m.failed + m.tooLarge;
        } else {
            out.recordsIn += m.published;
            out.recordsInInvalid += m.invalid + m.refused;
            out.lag = m.lag;
        }
    }
    return true;
}

std::string KafkaLinks::usedBy(const std::string& cluster) const {
    std::lock_guard<std::mutex> hold(_lock);
    for (const auto& one : _links) {
        if (one.second->cluster == cluster) return "connection \"" + one.first + "\"";
    }
    return std::string();
}

// A cluster changed live: each link on it takes the new settings live too -
// its pipe replaces only the clients the change touched.
void KafkaLinks::clustersChanged(const ServerConfig& section) {
    std::lock_guard<std::mutex> hold(_lock);
    _clusters = section.clusters;
    for (auto& one : _links) {
        Link& link = *one.second;
        const KafkaClusterConfig* now = section.cluster(link.cluster);
        if (now == nullptr) continue;
        // The mappings as they run, on the cluster as it is now.
        ServerConfig next = link.section;
        next.clusters.assign(1, *now);
        const std::string signature = signatureOf(next);
        if (signature == link.signature) continue;
        std::string why;
        if (!link.pipe->apply(next, why)) {
            std::fprintf(stderr, "kafka connection %s: the cluster's change was not taken: %s\n", one.first.c_str(),
                         why.c_str());
            continue;
        }
        link.section = next;
        link.signature = signature;
    }
}

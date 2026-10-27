// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "SparkplugAware.h"

#include <cstring>

namespace {

const char kPrefix[] = "$sparkplug/certificates/";

MqttSpan spanOf(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

std::vector<std::string> partsOf(const std::string& topic) {
    std::vector<std::string> parts;
    size_t start = 0;
    for (size_t i = 0; i <= topic.size(); i++) {
        if (i == topic.size() || topic[i] == '/') {
            parts.push_back(topic.substr(start, i - start));
            start = i + 1;
        }
    }
    return parts;
}

} // namespace

struct SparkplugAware::Shard {
    struct Out {
        std::string topic, payload;     // an empty payload clears it
    };
    std::vector<Out> pending;
    // What this shard has put there: a node's birth, and its devices'.
    std::map<std::string, std::set<std::string>> nodes;   // "group/node" -> devices
    std::atomic<bool> clearAll{false};
};

SparkplugAware::SparkplugAware(bool enabled) : _enabled(enabled) {}

SparkplugAware::~SparkplugAware() {}

bool SparkplugAware::isCertificate(const std::string& topic) {
    return topic.compare(0, sizeof kPrefix - 1, kPrefix) == 0;
}

bool SparkplugAware::begin(size_t shards, std::string&) {
    _shards.clear();
    for (size_t i = 0; i < shards; i++) _shards.emplace_back(new Shard());
    return true;
}

void SparkplugAware::enable(bool on) {
    const bool was = _enabled.exchange(on);
    // Off: what it put there is cleared, by each shard, on its own thread.
    if (was && !on) {
        for (auto& shard : _shards) shard->clearAll = true;
    }
}

uint32_t SparkplugAware::offer(uint32_t shard, MqttBroker&, const MqttBrokerRouted& message, uint64_t) {
    if (!_enabled.load(std::memory_order_relaxed) || shard >= _shards.size()) return 0;
    if (message.topic.length < 8 || std::memcmp(message.topic.data, "spBv1.0/", 8) != 0) return 0;
    const std::string topic((const char*)message.topic.data, message.topic.length);
    const std::vector<std::string> p = partsOf(topic);
    if (p.size() < 4 || p[1] == "STATE") return 0;
    const std::string node = p[1] + "/" + p[3];
    Shard& s = *_shards[shard];
    const std::string base = std::string(kPrefix) + "spBv1.0/" + p[1];
    if (p[2] == "NBIRTH" && p.size() == 4) {
        s.pending.push_back({base + "/NBIRTH/" + p[3],
                             std::string((const char*)message.payload.data, message.payload.length)});
        s.nodes[node];
    } else if (p[2] == "DBIRTH" && p.size() == 5) {
        s.pending.push_back({base + "/DBIRTH/" + p[3] + "/" + p[4],
                             std::string((const char*)message.payload.data, message.payload.length)});
        s.nodes[node].insert(p[4]);
    } else if (p[2] == "NDEATH" && p.size() == 4) {
        s.pending.push_back({base + "/NBIRTH/" + p[3], std::string()});
        for (const std::string& device : s.nodes[node]) {
            s.pending.push_back({base + "/DBIRTH/" + p[3] + "/" + device, std::string()});
        }
        s.nodes.erase(node);
    } else if (p[2] == "DDEATH" && p.size() == 5) {
        s.pending.push_back({base + "/DBIRTH/" + p[3] + "/" + p[4], std::string()});
        s.nodes[node].erase(p[4]);
    }
    return 0;
}

bool SparkplugAware::hasPendingWork(uint32_t shard) {
    return shard < _shards.size() && (!_shards[shard]->pending.empty() || _shards[shard]->clearAll.load());
}

void SparkplugAware::service(uint32_t shard, MqttBroker& broker) {
    if (shard >= _shards.size()) return;
    Shard& s = *_shards[shard];
    if (s.clearAll.exchange(false)) {
        for (const auto& node : s.nodes) {
            const size_t slash = node.first.find('/');
            const std::string base = std::string(kPrefix) + "spBv1.0/" + node.first.substr(0, slash);
            const std::string id = node.first.substr(slash + 1);
            s.pending.push_back({base + "/NBIRTH/" + id, std::string()});
            for (const std::string& device : node.second) {
                s.pending.push_back({base + "/DBIRTH/" + id + "/" + device, std::string()});
            }
        }
        s.nodes.clear();
    }
    if (s.pending.empty()) return;
    std::vector<Shard::Out> batch;
    batch.swap(s.pending);
    for (const Shard::Out& out : batch) {
        MqttBrokerRouted routed;
        routed.topic = spanOf(out.topic);
        routed.payload = spanOf(out.payload);
        routed.qos = MQ_QOS_0;
        routed.retain = true;
        broker.publishMessage(routed);
        if (!out.payload.empty()) _kept++;
    }
}

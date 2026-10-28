// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaControl.h"

#include "ServerOs.h"

#include <algorithm>
#include <fstream>
#include <sstream>

KafkaControl::KafkaControl(const ServerConfig& file, const std::string& stateDirectory,
                           const std::string& retainedCluster, KafkaLive* live)
    : _path(stateDirectory.empty() ? std::string() : stateDirectory + "/kafka.json"),
      _retainedCluster(retainedCluster), _live(live) {
    _section.clusters = file.clusters;
    _section.toKafka = file.toKafka;
    _section.fromKafka = file.fromKafka;
}

bool KafkaControl::load(std::string& error) {
    if (_path.empty()) return true;
    std::ifstream in(_path);
    if (!in) return true;                 // never changed over the API: the file's stands
    std::stringstream text;
    text << in.rdbuf();
    ServerConfig loaded;
    if (!loadKafkaSection(text.str(), loaded, error)) {
        error = _path + ": " + error;
        return false;
    }
    std::lock_guard<std::mutex> hold(_lock);
    _section = loaded;
    return true;
}

ServerConfig KafkaControl::section() const {
    std::lock_guard<std::mutex> hold(_lock);
    return _section;
}

bool KafkaControl::save(const ServerConfig& section, std::string& error) const {
    if (_path.empty()) return true;
    // Readable by the server's own user only: it holds the clusters' passwords.
    return server_os::writeFileAtomically(
        _path, renderKafkaSection(section.clusters, section.toKafka, section.fromKafka), error, 0600);
}

KafkaControl::Result KafkaControl::change(ServerConfig next, std::string& error) {
    if (_live == nullptr) {
        error = "this build has no Kafka (librdkafka was not found when it was built)";
        return Result::Unavailable;
    }
    // Through the loader, as the file goes: one set of rules, and the
    // defaults it fills in (the only cluster, the consumer group).
    ServerConfig checked;
    if (!loadKafkaSection(renderKafkaSection(next.clusters, next.toKafka, next.fromKafka), checked, error)) {
        return Result::Invalid;
    }
    if (!_live->apply(checked, error)) return Result::Failed;
    std::string why;
    if (!save(checked, why)) {
        // Running, but not kept: said, not undone - undoing it would be a
        // second live change nobody asked for.
        error = "applied, but not kept: " + why;
        _section = checked;
        if (_changed) _changed(_section);
        return Result::Failed;
    }
    _section = checked;
    if (_changed) _changed(_section);
    return Result::Ok;
}

KafkaControl::Result KafkaControl::putCluster(const KafkaClusterConfig& cluster, KafkaClusterConfig* stored,
                                              std::string& error) {
    std::lock_guard<std::mutex> hold(_lock);
    if (cluster.id.empty()) {
        error = "a cluster needs an id";
        return Result::Invalid;
    }
    ServerConfig next = _section;
    KafkaClusterConfig given = cluster;
    auto at = std::find_if(next.clusters.begin(), next.clusters.end(),
                           [&](const KafkaClusterConfig& c) { return c.id == cluster.id; });
    if (at != next.clusters.end()) {
        // No password given: the one it has (an answer never shows it).
        if (given.password.empty() && given.passwordFile.empty() && given.username == at->username) {
            given.password = at->password;
            given.passwordFile = at->passwordFile;
        }
        *at = given;
    } else {
        next.clusters.push_back(given);
    }
    const Result result = change(next, error);
    if (result == Result::Ok && stored) *stored = *_section.cluster(cluster.id);
    return result;
}

KafkaControl::Result KafkaControl::putToKafka(const MqttToKafkaConfig& mapping, MqttToKafkaConfig* stored,
                                              std::string& error) {
    std::lock_guard<std::mutex> hold(_lock);
    if (mapping.id.empty()) {
        error = "a mapping needs an id";
        return Result::Invalid;
    }
    ServerConfig next = _section;
    // Replaced where it is, so the order of mappings - and the order they
    // are matched in - does not move.
    next.fromKafka.erase(std::remove_if(next.fromKafka.begin(), next.fromKafka.end(),
                                        [&](const KafkaToMqttConfig& m) { return m.id == mapping.id; }),
                         next.fromKafka.end());
    auto at = std::find_if(next.toKafka.begin(), next.toKafka.end(),
                           [&](const MqttToKafkaConfig& m) { return m.id == mapping.id; });
    if (at != next.toKafka.end()) *at = mapping;
    else next.toKafka.push_back(mapping);
    const Result result = change(next, error);
    if (result == Result::Ok && stored) {
        for (const MqttToKafkaConfig& m : _section.toKafka) {
            if (m.id == mapping.id) *stored = m;
        }
    }
    return result;
}

KafkaControl::Result KafkaControl::putFromKafka(const KafkaToMqttConfig& mapping, KafkaToMqttConfig* stored,
                                                std::string& error) {
    std::lock_guard<std::mutex> hold(_lock);
    if (mapping.id.empty()) {
        error = "a mapping needs an id";
        return Result::Invalid;
    }
    ServerConfig next = _section;
    next.toKafka.erase(std::remove_if(next.toKafka.begin(), next.toKafka.end(),
                                      [&](const MqttToKafkaConfig& m) { return m.id == mapping.id; }),
                       next.toKafka.end());
    auto at = std::find_if(next.fromKafka.begin(), next.fromKafka.end(),
                           [&](const KafkaToMqttConfig& m) { return m.id == mapping.id; });
    if (at != next.fromKafka.end()) *at = mapping;
    else next.fromKafka.push_back(mapping);
    const Result result = change(next, error);
    if (result == Result::Ok && stored) {
        for (const KafkaToMqttConfig& m : _section.fromKafka) {
            if (m.id == mapping.id) *stored = m;
        }
    }
    return result;
}

KafkaControl::Result KafkaControl::deleteCluster(const std::string& id, std::string& error) {
    std::lock_guard<std::mutex> hold(_lock);
    if (_section.cluster(id) == nullptr) {
        error = "no Kafka cluster \"" + id + "\"";
        return Result::NotFound;
    }
    if (!_retainedCluster.empty() && id == _retainedCluster) {
        error = "the retained store writes to cluster \"" + id + "\" (retained: store: kafka)";
        return Result::InUse;
    }
    const std::string user = _usedBy ? _usedBy(id) : std::string();
    if (!user.empty()) {
        error = user + " uses cluster \"" + id + "\"; delete it first";
        return Result::InUse;
    }
    for (const MqttToKafkaConfig& m : _section.toKafka) {
        if (m.cluster == id) {
            error = "mapping \"" + m.id + "\" uses cluster \"" + id + "\"; delete it first";
            return Result::InUse;
        }
    }
    for (const KafkaToMqttConfig& m : _section.fromKafka) {
        if (m.cluster == id) {
            error = "mapping \"" + m.id + "\" uses cluster \"" + id + "\"; delete it first";
            return Result::InUse;
        }
    }
    ServerConfig next = _section;
    next.clusters.erase(std::remove_if(next.clusters.begin(), next.clusters.end(),
                                       [&](const KafkaClusterConfig& c) { return c.id == id; }),
                        next.clusters.end());
    return change(next, error);
}

KafkaControl::Result KafkaControl::deleteMapping(const std::string& id, std::string& error) {
    std::lock_guard<std::mutex> hold(_lock);
    ServerConfig next = _section;
    const size_t before = next.toKafka.size() + next.fromKafka.size();
    next.toKafka.erase(std::remove_if(next.toKafka.begin(), next.toKafka.end(),
                                      [&](const MqttToKafkaConfig& m) { return m.id == id; }),
                       next.toKafka.end());
    next.fromKafka.erase(std::remove_if(next.fromKafka.begin(), next.fromKafka.end(),
                                        [&](const KafkaToMqttConfig& m) { return m.id == id; }),
                         next.fromKafka.end());
    if (next.toKafka.size() + next.fromKafka.size() == before) {
        error = "no Kafka mapping \"" + id + "\"";
        return Result::NotFound;
    }
    return change(next, error);
}

KafkaPipeStatus KafkaControl::status() const {
    if (_live == nullptr) {
        KafkaPipeStatus none;
        none.kafkaReachable = false;
        return none;
    }
    return _live->status();
}

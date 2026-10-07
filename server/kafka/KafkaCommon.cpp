// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaCommon.h"

#include <cstdio>

// librdkafka's own log lines, in one line each and the server's voice. Called
// from librdkafka's threads; fprintf to stderr is line-atomic, which is all
// this needs. Its default level is warnings and worse; `log_level` in a
// cluster's properties changes it (7 is debug).
void kafkaOnLog(const rd_kafka_t* handle, int level, const char* facility, const char* text) {
    static const char* const kLevels[] = {"emerg", "alert", "crit", "error",
                                          "warning", "notice", "info", "debug"};
    std::fprintf(stderr, "kafka %s [%s] %s: %s\n", (level >= 0 && level < 8) ? kLevels[level] : "log",
                 rd_kafka_name(handle), facility, text);
}

bool kafkaSet(rd_kafka_conf_t* conf, const char* name, const std::string& value, std::string& error) {
    char reason[512];
    if (rd_kafka_conf_set(conf, name, value.c_str(), reason, sizeof reason) != RD_KAFKA_CONF_OK) {
        error = std::string(name) + "=" + value + ": " + reason;
        return false;
    }
    return true;
}

// The settings every client of one cluster shares: where, and how securely.
bool kafkaClusterSettings(rd_kafka_conf_t* conf, const KafkaClusterConfig& cluster,
                     const std::string& clientId, std::string& error) {
    rd_kafka_conf_set_log_cb(conf, &kafkaOnLog);
    if (!kafkaSet(conf, "log_level", "4", error)) return false;
    if (!kafkaSet(conf, "bootstrap.servers", cluster.bootstrapServers, error)) return false;
    if (!kafkaSet(conf, "client.id", clientId, error)) return false;
    const bool sasl = cluster.mechanism != "none";
    const char* protocol = cluster.tls ? (sasl ? "sasl_ssl" : "ssl")
                                       : (sasl ? "sasl_plaintext" : "plaintext");
    if (!kafkaSet(conf, "security.protocol", protocol, error)) return false;
    if (cluster.tls) {
        if (!cluster.caFile.empty() && !kafkaSet(conf, "ssl.ca.location", cluster.caFile, error)) return false;
        if (!cluster.certificateFile.empty() &&
            (!kafkaSet(conf, "ssl.certificate.location", cluster.certificateFile, error) ||
             !kafkaSet(conf, "ssl.key.location", cluster.keyFile, error))) {
            return false;
        }
        if (!kafkaSet(conf, "ssl.endpoint.identification.algorithm",
                 cluster.verifyHostname ? "https" : "none", error)) {
            return false;
        }
    }
    if (sasl) {
        const std::string mechanism = cluster.mechanism == "plain" ? "PLAIN"
                                    : cluster.mechanism == "scram-sha256" ? "SCRAM-SHA-256"
                                    : cluster.mechanism == "scram-sha512" ? "SCRAM-SHA-512"
                                    : "OAUTHBEARER";
        if (!kafkaSet(conf, "sasl.mechanisms", mechanism, error)) return false;
        if (!cluster.username.empty() && !kafkaSet(conf, "sasl.username", cluster.username, error)) return false;
        if (!cluster.password.empty() && !kafkaSet(conf, "sasl.password", cluster.password, error)) return false;
    }
    return true;
}

// Anything the file passes straight through, last, so it can override ours.
bool kafkaPassThrough(rd_kafka_conf_t* conf, const KafkaClusterConfig& cluster, std::string& error) {
    for (const auto& property : cluster.properties) {
        if (!kafkaSet(conf, property.first.c_str(), property.second, error)) return false;
    }
    return true;
}


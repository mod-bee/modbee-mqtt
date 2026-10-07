// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// What every Kafka client in the server shares - the pipe's producers and
// consumers, and the retained store's: how to reach a cluster, how librdkafka
// logs, and how MQTT user properties become Kafka headers. One copy, so the
// two cannot disagree about a password or a header name.

#include "ServerConfig.h"

#include <librdkafka/rdkafka.h>

#include <string>

// Header names for user properties that would collide with the reserved
// mqtt-* names are escaped with this prefix, and unescaped on the way back -
// so the round trip is lossless whatever the user called their properties.
inline constexpr char kUserPropertyEscape[] = "mqtt-user-property-";

// librdkafka's own log lines, one line each, in the server's voice.
void kafkaOnLog(const rd_kafka_t* handle, int level, const char* facility, const char* text);

// One setting, with librdkafka's reason in `error` when it is refused.
bool kafkaSet(rd_kafka_conf_t* conf, const char* name, const std::string& value,
              std::string& error);

// The settings every client of one cluster shares: where, and how securely.
bool kafkaClusterSettings(rd_kafka_conf_t* conf, const KafkaClusterConfig& cluster,
                          const std::string& clientId, std::string& error);

// Anything the file passes straight through, last, so it can override ours.
bool kafkaPassThrough(rd_kafka_conf_t* conf, const KafkaClusterConfig& cluster,
                      std::string& error);

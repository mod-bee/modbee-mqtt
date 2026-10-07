// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// Retained messages kept in a COMPACTED Kafka topic (retained: store: kafka).
//
// One record per change: the key is the MQTT topic, the value the payload,
// and the headers the rest, named as the pipe names them (mqtt-qos,
// mqtt-content-type, mqtt-message-expiry-interval, user properties...). A
// cleared topic is a tombstone - a record with no value. Compaction keeps the
// newest record for each key and drops tombstones in time, so the topic is
// exactly the retained set, readable by anything that reads Kafka.
//
// The topic is created, compacted, if it does not exist. One that exists and
// is NOT compacted is used, with a warning: Kafka would delete a topic's
// unchanged retained messages after its retention time.
//
// At start the whole topic is read, from the beginning to its end at that
// moment, before the server accepts a client. A Kafka that cannot be reached
// then is a server that does not start - restarting it is better than serving
// an empty store as though it were the real one.

#include "RetainedKeeper.h"
#include "ServerConfig.h"

#include <memory>
#include <string>

std::unique_ptr<RetainedBackend> makeRetainedKafka(const KafkaClusterConfig& cluster,
                                                   const std::string& topic);

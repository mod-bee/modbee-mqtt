// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// THE ENVELOPE: ONE MQTT MESSAGE AS ONE KAFKA RECORD.
//
// The contract (docs/kafka.md, "The envelope") written once, so the pipe, a
// connection whose link is Kafka and the command-line tool cannot disagree:
//
//   value      the payload, untouched - binary, empty, whatever it was
//   key        what the mapping says (the topic by default)
//   headers    mqtt-envelope: 1               the contract's version
//              mqtt-topic, mqtt-qos, mqtt-retain
//              mqtt-clientid, mqtt-username   who published it (read back: informational)
//              mqtt-content-type, mqtt-response-topic,
//              mqtt-correlation-data (raw bytes), mqtt-payload-format-indicator,
//              mqtt-message-expiry-interval
//              every user property, in order, repeats kept; a name starting
//              "mqtt-" written as mqtt-user-property-<name>
//              mqtt-link: <group>             written by a connection whose link is
//                                             Kafka; that link does not read its own back
//
// NOT IN IT, deliberately - each belongs to one hop, not to the message:
// the packet identifier, DUP, a Topic Alias, Subscription Identifiers, and
// the Message Expiry already spent (the interval is the one published).
//
// A reader refuses a version it does not know (a record with mqtt-envelope
// other than 1); a record with no mqtt-envelope header at all - written by
// someone else's producer, or before the header existed - is read as 1.

#include "ServerConfig.h"
#include "MqttProperties.h"

#include <librdkafka/rdkafka.h>

#include <string>
#include <utility>
#include <vector>

inline constexpr char kEnvelopeVersionHeader[] = "mqtt-envelope";
inline constexpr char kEnvelopeVersion[] = "1";
inline constexpr char kEnvelopeLinkHeader[] = "mqtt-link";

// The headers for one message: every mqtt-* one when `selection` is null or
// asks for all (headers: all), else only the placeholders it names - then its
// fixed ones. The version header goes with the whole envelope only. Returns
// the bytes they add to the record (names and values), for max-record-bytes.
size_t kafkaWriteEnvelope(rd_kafka_headers_t* headers, const MqttToKafkaConfig* selection,
                          const MqttSpan& topic, uint8_t qos, bool retain, const MqttSpan& clientId,
                          const MqttSpan& userName, const MqttProperties* properties);

// One record's headers, read back.
struct KafkaEnvelope {
    bool hasTopic = false;
    std::string topic;
    bool hasQos = false, hasRetain = false;
    uint8_t qos = 0;
    bool retain = false;
    std::string clientId, userName, link;
    bool hasContentType = false, hasResponseTopic = false, hasCorrelation = false;
    std::string contentType, responseTopic, correlation;
    bool hasFormat = false;
    uint8_t format = 0;
    bool hasExpiry = false;
    uint32_t expiry = 0;
    // The user properties, and any header that is not the envelope's own.
    std::vector<std::pair<std::string, std::string>> users;
    std::string version;                 // "" when the record has none
};

// False when the record is in a version of the envelope this cannot read.
bool kafkaReadEnvelope(rd_kafka_headers_t* headers, KafkaEnvelope& out);

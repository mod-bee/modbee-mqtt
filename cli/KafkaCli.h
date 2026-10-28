// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// pub, sub and req WITH KAFKA IN THE MIDDLE: --kafka <bootstrap>.
//
// For a person testing a link whose other end is Kafka. `pub` writes a record
// as the pipe would, in the envelope (server/kafka/KafkaEnvelope.h); `sub`
// reads records and prints them as the MQTT messages they are, exactly as it
// prints messages from a broker (--json, --sparkplug and all); `req` writes a
// request and waits for the record answering it.
//
// The flags exist in every build; one without librdkafka refuses them, saying
// so. No consumer group unless --kafka-group: the partitions are assigned
// directly at their end (or start), so nothing waits for a group to form.

#include "Args.h"

#include <string>
#include <vector>

// The --kafka flags, after a command's own.
std::vector<ArgSpec> withKafkaSpecs(std::vector<ArgSpec> specs);

// --kafka given; and when it is in a build without Kafka, says so (false).
bool kafkaAsked(const Args& args, const char* command, bool& refused);

#if defined(MODBEE_HAVE_KAFKA)

#include "MqttClient.h"
#include "MqttOwnedProperties.h"

#include <memory>

class CliKafka {
public:
    CliKafka();
    ~CliKafka();
    CliKafka(const CliKafka&) = delete;
    CliKafka& operator=(const CliKafka&) = delete;

    // The cluster, from the flags: where, TLS, SASL, anything passed through.
    bool configure(const Args& args, std::string& error);

    // --- Writing ---
    bool openProducer(std::string& error);
    // One record in the envelope, its key the MQTT topic (or --kafka-key).
    bool produce(const std::string& kafkaTopic, const std::string& topic, const uint8_t* payload, size_t length,
                 uint8_t qos, bool retain, const MqttOwnedProperties& properties, std::string& error);
    // Until Kafka has answered every record, or `ms`: how many it refused.
    bool flush(int ms, size_t& failed);

    // --- Reading ---
    // Every partition of `topics` from its end (or its start), or as the
    // group --kafka-group's member.
    bool openConsumer(const std::vector<std::string>& topics, std::string& error);
    // The next record as an MQTT message (the envelope's topic, QoS, retain
    // and properties; the Kafka topic's name when it has no mqtt-topic),
    // valid until the next call. False when none came within `ms`.
    bool next(int ms, MqttMessage& out);

    struct State;

private:
    std::unique_ptr<State> _s;
};

#endif

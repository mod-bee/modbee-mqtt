// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// THE SPARKPLUG TOPIC NAMESPACE
// =============================================================================
//
// [tck-id-topic-structure] All MQTT clients using the Sparkplug specification
// MUST use the following topic namespace structure:
//
//     namespace/group_id/message_type/edge_node_id/[device_id]
//
// Four elements, sometimes five. The namespace is always "spBv1.0"
// [tck-id-topic-structure-namespace-a]. The device_id is present for exactly
// the four device-level message types and absent for the rest -
// [tck-id-topic-structure-namespace-device-id-associated-message-types] and
// -non-associated-message-types - which is one of the few places Sparkplug
// forbids something outright rather than leaving it optional.
//
// A Host Application's own topic is the odd one out: spBv1.0/STATE/{host_id},
// which is three elements with the literal "STATE" where a group_id would be
// [tck-id-payloads-state-will-message].
//
// EVERY ELEMENT IS CASE SENSITIVE. [tck-id-case-sensitivity-sparkplug-ids].
// A group called "Plant" and one called "plant" are two groups. Nothing here
// folds case, and the comparison functions say so at the point of use, because
// the instinct to "normalise" an identifier is strong and wrong.

#include "MqttSparkplugTypes.h"

#include <string.h>

// Longest topic this build will construct or accept.
//
// The specification sets no limit - MQTT's own is 65,535 - but a fixed buffer is
// what lets a node build a topic inside a scan cycle without allocating. 128 is
// four generous identifiers plus the namespace and separators, and anything
// longer is refused by name rather than truncated: a truncated topic publishes
// to the wrong place, which is worse than not publishing.
#ifndef MQ_SP_MAX_TOPIC
#define MQ_SP_MAX_TOPIC 128
#endif

// One identifier - group, node or device.
#ifndef MQ_SP_MAX_ID
#define MQ_SP_MAX_ID 48
#endif

// A parsed topic. The elements are copied rather than spanned, because a topic
// arrives in a receive buffer that the next poll() reuses, and a node holds the
// group and node id for the life of the connection.
struct MqttSparkplugTopicParts {
    MqttSparkplugType type = MqttSparkplugType::NBirth;

    char group[MQ_SP_MAX_ID] = {0};
    char node[MQ_SP_MAX_ID] = {0};
    char device[MQ_SP_MAX_ID] = {0};

    bool hasDevice = false;

    // For a STATE topic the host id lands in `group`, because that is the
    // element position it occupies, and `node` is empty. Said here rather than
    // adding a fourth field that is empty eight times out of nine.
    bool isState() const { return type == MqttSparkplugType::State; }
};

// --- Validating an identifier ---
//
// [tck-id-topic-structure-namespace-valid-group-id] The format of the Group ID
// MUST be a valid UTF-8 string with the exception of the reserved characters of
// + (plus), / (forward slash), and # (number sign).
// -valid-edge-node-id and -valid-device-id say the same for the other two.
//
// Those three characters are MQTT's wildcards and separator: a group id
// containing one would either split into two topic elements or turn a
// subscription into a wildcard, so they are refused rather than escaped. MQTT
// has no escaping mechanism, so escaping is not on the table.
//
// An empty identifier is also refused. The specification does not say so
// explicitly, but an empty element makes "a/b//d" - which parses as five
// elements, one of them blank, and no Host Application will match it.
bool mq_sparkplugValidId(const char* id);

// --- Building ---
//
// Writes into `out` and returns the length, or 0 if it would not fit or an
// identifier is invalid. Never truncates.
//
// `device` is required for the four device-level types and MUST be null for the
// rest - passing one for an NDATA is an error, not something quietly dropped,
// because a topic with a device element where the specification forbids one is
// a topic a conformant host discards.
size_t mq_sparkplugBuildTopic(char* out, size_t capacity,
                              MqttSparkplugType type,
                              const char* group, const char* node,
                              const char* device = nullptr);

// spBv1.0/STATE/{host_id}. Separate because its shape is different, and
// because a Host Application has no group or node id to pass.
size_t mq_sparkplugBuildStateTopic(char* out, size_t capacity,
                                   const char* hostId);

// --- Parsing ---
//
// True on a topic this build understands. False for a topic in another
// namespace, an unknown message type, a device element where there must not be
// one, or a missing one where there must be.
//
// Lengths are taken rather than assuming a terminator, because this is called
// on a span from the receive buffer.
bool mq_sparkplugParseTopic(const char* topic, size_t length,
                            MqttSparkplugTopicParts& out);

inline bool mq_sparkplugParseTopic(const char* topic,
                                   MqttSparkplugTopicParts& out) {
    return mq_sparkplugParseTopic(topic, topic ? strlen(topic) : 0, out);
}

// --- Subscription filters ---
//
// The two an edge node must register.
// [tck-id-message-flow-edge-node-ncmd-subscribe] ... MUST subscribe to a topic
// of the form spBv1.0/group_id/NCMD/edge_node_id ... with a QoS of 1.
// [tck-id-message-flow-device-dcmd-subscribe] ... spBv1.0/group_id/DCMD/
// edge_node_id/device_id ... with a QoS of 1.
//
// Note the QoS: 1, not the library's QoS 0 default, and mandatory in both
// cases. A node that subscribed at QoS 0 could miss a rebirth request, which is
// the one command it MUST be able to answer.
size_t mq_sparkplugNodeCommandFilter(char* out, size_t capacity,
                                     const char* group, const char* node);

// One filter covering every device under this node, rather than one per device:
// spBv1.0/{group}/DCMD/{node}/+. A node with twelve devices would otherwise
// hold twelve subscriptions for no benefit, and MQTT charges per subscription
// in the SUBSCRIBE packet and in the broker's matching.
size_t mq_sparkplugDeviceCommandFilter(char* out, size_t capacity,
                                       const char* group, const char* node);

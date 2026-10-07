// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// =============================================================================
// PROPERTIES  -  5.0 only
// =============================================================================
//
// V5:2.2.2 and Table 2-4. Twenty-three identifiers, each with a data type and
// a list of the packets it may appear in.
//
// The packet list is ENFORCED, not documentation. V5:2.2.2.2:
//
//   "A Control Packet which contains an Identifier which is not valid for its
//    packet type, or contains a value not of the specified data type, is a
//    Malformed Packet."
//
// So the table below is the decoder, and a property in the wrong packet is
// refused rather than skipped. Skipping would mean silently accepting packets
// a conforming peer would reject, and the two ends would then disagree about
// what had been said.
//
// 3.1.1 has no properties at all. That is why this file is reached only from
// the 5.0 codec and why nothing here is shared.

#include "MqttWire.h"

// --- Identifiers, Table 2-4 ---
#define MQ_PROP_PAYLOAD_FORMAT_INDICATOR          0x01
#define MQ_PROP_MESSAGE_EXPIRY_INTERVAL           0x02
#define MQ_PROP_CONTENT_TYPE                      0x03
#define MQ_PROP_RESPONSE_TOPIC                    0x08
#define MQ_PROP_CORRELATION_DATA                  0x09
#define MQ_PROP_SUBSCRIPTION_IDENTIFIER           0x0B
#define MQ_PROP_SESSION_EXPIRY_INTERVAL           0x11
#define MQ_PROP_ASSIGNED_CLIENT_IDENTIFIER        0x12
#define MQ_PROP_SERVER_KEEP_ALIVE                 0x13
#define MQ_PROP_AUTHENTICATION_METHOD             0x15
#define MQ_PROP_AUTHENTICATION_DATA               0x16
#define MQ_PROP_REQUEST_PROBLEM_INFORMATION       0x17
#define MQ_PROP_WILL_DELAY_INTERVAL               0x18
#define MQ_PROP_REQUEST_RESPONSE_INFORMATION      0x19
#define MQ_PROP_RESPONSE_INFORMATION              0x1A
#define MQ_PROP_SERVER_REFERENCE                  0x1C
#define MQ_PROP_REASON_STRING                     0x1F
#define MQ_PROP_RECEIVE_MAXIMUM                   0x21
#define MQ_PROP_TOPIC_ALIAS_MAXIMUM               0x22
#define MQ_PROP_TOPIC_ALIAS                       0x23
#define MQ_PROP_MAXIMUM_QOS                       0x24
#define MQ_PROP_RETAIN_AVAILABLE                  0x25
#define MQ_PROP_USER_PROPERTY                     0x26
#define MQ_PROP_MAXIMUM_PACKET_SIZE               0x27
#define MQ_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE   0x28
#define MQ_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE 0x29
#define MQ_PROP_SHARED_SUBSCRIPTION_AVAILABLE     0x2A

// --- Where the Will's properties live ---
//
// V5:3.1.3.2. The Will has its own property set inside the CONNECT payload,
// and its identifiers are a different subset from CONNECT's own. Given a
// pseudo packet type so one table covers both without a second code path.
#define MQ_PROP_SCOPE_WILL 0x10

// --- How many repeatable properties a build holds ---
//
// Only two identifiers may legally repeat: User Property, and Subscription
// Identifier inside a PUBLISH. Everything else is a Protocol Error the second
// time. Bounded rather than grown, because this decodes attacker-supplied
// bytes on a device with no spare heap - and a peer that sends ten thousand
// User Properties must cost us nothing.
#ifndef MQTT_MAX_USER_PROPERTIES
#define MQTT_MAX_USER_PROPERTIES 8
#endif

// V5:3.3.2.3.8 - "Multiple Subscription Identifiers will be included if the
// publication is the result of a match to more than one subscription".
#ifndef MQTT_MAX_SUBSCRIPTION_IDS
#define MQTT_MAX_SUBSCRIPTION_IDS 4
#endif

struct MqttUserProperty {
    MqttSpan key;
    MqttSpan value;
};

// --- A decoded or to-be-encoded property set ---
//
// A struct with a field per property rather than a list of identifier/value
// pairs. Fixed size, no allocation, and access is a named field rather than a
// search - so a caller cannot read the Session Expiry Interval as though it
// were the Message Expiry Interval, which a generic list makes easy.
//
// Spans point into the buffer being decoded and are valid only as long as it
// is. Nothing here copies a payload.

// A BOUND THAT DOES NOT FIT ITS COUNTER CORRUPTS RATHER THAN REFUSES.
//
// These are build flags. Raised past what the field holding the count can
// express, the count wraps silently - and the symptom is not "the bound was
// too big", it is messages going missing. Checked here, at compile time,
// rather than by whoever picked the number.
static_assert(MQTT_MAX_USER_PROPERTIES <= 255,
              "MQTT_MAX_USER_PROPERTIES must fit the uint8_t userCount");
static_assert(MQTT_MAX_SUBSCRIPTION_IDS <= 255,
              "MQTT_MAX_SUBSCRIPTION_IDS must fit the uint8_t subscriptionIdCount");

struct MqttProperties {
    // Which fields below mean anything. A property that is absent and one that
    // is present and zero are different things: Session Expiry Interval absent
    // means "the default", present and 0 means "end the session at once".
    uint32_t present = 0;

    uint8_t  payloadFormatIndicator = 0;
    uint32_t messageExpiryInterval = 0;
    MqttSpan contentType;
    MqttSpan responseTopic;
    MqttSpan correlationData;
    uint32_t sessionExpiryInterval = 0;
    MqttSpan assignedClientIdentifier;
    uint16_t serverKeepAlive = 0;
    MqttSpan authenticationMethod;
    MqttSpan authenticationData;
    uint8_t  requestProblemInformation = 1;   // V5:3.1.2.11.7, default 1
    uint32_t willDelayInterval = 0;           // V5:3.1.3.2.2, default 0
    uint8_t  requestResponseInformation = 0;  // V5:3.1.2.11.6, default 0
    MqttSpan responseInformation;
    MqttSpan serverReference;
    MqttSpan reasonString;
    uint16_t receiveMaximum = 65535;          // V5:3.1.2.11.3, default 65535
    uint16_t topicAliasMaximum = 0;           // V5:3.1.2.11.5, default 0
    uint16_t topicAlias = 0;
    uint8_t  maximumQos = 2;                  // V5:3.2.2.3.4, default 2
    uint8_t  retainAvailable = 1;             // V5:3.2.2.3.5, default 1
    uint32_t maximumPacketSize = 0;           // 0 means "no limit stated"
    uint8_t  wildcardSubscriptionAvailable = 1;
    uint8_t  subscriptionIdentifiersAvailable = 1;
    uint8_t  sharedSubscriptionAvailable = 1;

    MqttUserProperty user[MQTT_MAX_USER_PROPERTIES];
    uint8_t userCount = 0;

    uint32_t subscriptionIds[MQTT_MAX_SUBSCRIPTION_IDS] = {0};
    uint8_t  subscriptionIdCount = 0;

    bool has(uint8_t identifier) const;
    void markPresent(uint8_t identifier);
    void clear() { *this = MqttProperties(); }

    bool addUserProperty(const MqttSpan& key, const MqttSpan& value);
};

// --- The table ---

// Whether the identifier exists at all.
bool mq_propertyIsKnown(uint8_t identifier);

// Whether it may appear in this packet type, or in the Will property set when
// `packetType` is MQ_PROP_SCOPE_WILL.
bool mq_propertyAllowedIn(uint8_t identifier, uint8_t packetType);

const char* mq_propertyName(uint8_t identifier);

// --- Codec ---

// Reads a whole property set: the Property Length, then that many bytes of
// properties. An absent property set is still a Property Length of zero
// [V5:MQTT-2.2.2-1], never a missing field.
//
// The reader is bounded to the declared length before any property is read, so
// a Property Length larger than the packet cannot walk off the end - and a
// property that runs past the declared length is caught even when the packet
// as a whole has bytes to spare.
MqttError mq_decodeProperties(MqttReader& reader, uint8_t packetType,
                              MqttProperties& out);

// Bytes the set will occupy, INCLUDING the Property Length prefix.
size_t mq_propertiesSize(const MqttProperties& properties, uint8_t packetType);

void mq_encodeProperties(MqttWriter& writer, const MqttProperties& properties,
                         uint8_t packetType);

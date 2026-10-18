// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttProperties.h"
#include "MqttNoExcept.h"

namespace {

// The data type of a property's value. V5 Table 2-4, third column.
enum class Kind : uint8_t {
    Byte, TwoByte, FourByte, VarInt, Utf8, Binary, StringPair
};

typedef unsigned PacketMask;

// constexpr, and every mask below with it, so TABLE and the identifier index
// are constants the compiler lays out in flash rather than tables a static
// constructor builds in RAM at boot - see MqttTypes.cpp.
constexpr PacketMask bit(uint8_t packetType) { return 1u << packetType; }

constexpr PacketMask CONNECT     = bit(MQ_PKT_CONNECT);
constexpr PacketMask CONNACK     = bit(MQ_PKT_CONNACK);
constexpr PacketMask PUBLISH     = bit(MQ_PKT_PUBLISH);
constexpr PacketMask PUBACK      = bit(MQ_PKT_PUBACK);
constexpr PacketMask PUBREC      = bit(MQ_PKT_PUBREC);
constexpr PacketMask PUBREL      = bit(MQ_PKT_PUBREL);
constexpr PacketMask PUBCOMP     = bit(MQ_PKT_PUBCOMP);
constexpr PacketMask SUBSCRIBE   = bit(MQ_PKT_SUBSCRIBE);
constexpr PacketMask SUBACK      = bit(MQ_PKT_SUBACK);
constexpr PacketMask UNSUBSCRIBE = bit(MQ_PKT_UNSUBSCRIBE);
constexpr PacketMask UNSUBACK    = bit(MQ_PKT_UNSUBACK);
constexpr PacketMask DISCONNECT  = bit(MQ_PKT_DISCONNECT);
constexpr PacketMask AUTH        = bit(MQ_PKT_AUTH);
constexpr PacketMask WILL        = bit(MQ_PROP_SCOPE_WILL);

// Every acknowledgement that may carry a Reason String or a User Property.
constexpr PacketMask ACKS = PUBACK | PUBREC | PUBREL | PUBCOMP | SUBACK | UNSUBACK;

struct Entry {
    uint8_t identifier;
    Kind kind;
    PacketMask packets;
    bool repeatable;
    const char* name;
};

// V5 Table 2-4, transcribed row for row. The order here is the order
// properties are encoded in; V5:2.2.2.2 says order is not significant for
// differing identifiers, so any order is legal - fixing one makes our own
// output reproducible, which is what lets a test compare bytes.
constexpr Entry TABLE[] = {
    { MQ_PROP_PAYLOAD_FORMAT_INDICATOR,          Kind::Byte,       PUBLISH | WILL,            false, "Payload Format Indicator" },
    { MQ_PROP_MESSAGE_EXPIRY_INTERVAL,           Kind::FourByte,   PUBLISH | WILL,            false, "Message Expiry Interval" },
    { MQ_PROP_CONTENT_TYPE,                      Kind::Utf8,       PUBLISH | WILL,            false, "Content Type" },
    { MQ_PROP_RESPONSE_TOPIC,                    Kind::Utf8,       PUBLISH | WILL,            false, "Response Topic" },
    { MQ_PROP_CORRELATION_DATA,                  Kind::Binary,     PUBLISH | WILL,            false, "Correlation Data" },
    // The only identifier besides User Property that may repeat, and only in a
    // PUBLISH: one arrives per subscription the message matched.
    { MQ_PROP_SUBSCRIPTION_IDENTIFIER,           Kind::VarInt,     PUBLISH | SUBSCRIBE,       true,  "Subscription Identifier" },
    { MQ_PROP_SESSION_EXPIRY_INTERVAL,           Kind::FourByte,   CONNECT | CONNACK | DISCONNECT, false, "Session Expiry Interval" },
    { MQ_PROP_ASSIGNED_CLIENT_IDENTIFIER,        Kind::Utf8,       CONNACK,                   false, "Assigned Client Identifier" },
    { MQ_PROP_SERVER_KEEP_ALIVE,                 Kind::TwoByte,    CONNACK,                   false, "Server Keep Alive" },
    { MQ_PROP_AUTHENTICATION_METHOD,             Kind::Utf8,       CONNECT | CONNACK | AUTH,  false, "Authentication Method" },
    { MQ_PROP_AUTHENTICATION_DATA,               Kind::Binary,     CONNECT | CONNACK | AUTH,  false, "Authentication Data" },
    { MQ_PROP_REQUEST_PROBLEM_INFORMATION,       Kind::Byte,       CONNECT,                   false, "Request Problem Information" },
    { MQ_PROP_WILL_DELAY_INTERVAL,               Kind::FourByte,   WILL,                      false, "Will Delay Interval" },
    { MQ_PROP_REQUEST_RESPONSE_INFORMATION,      Kind::Byte,       CONNECT,                   false, "Request Response Information" },
    { MQ_PROP_RESPONSE_INFORMATION,              Kind::Utf8,       CONNACK,                   false, "Response Information" },
    { MQ_PROP_SERVER_REFERENCE,                  Kind::Utf8,       CONNACK | DISCONNECT,      false, "Server Reference" },
    { MQ_PROP_REASON_STRING,                     Kind::Utf8,       CONNACK | ACKS | DISCONNECT | AUTH, false, "Reason String" },
    { MQ_PROP_RECEIVE_MAXIMUM,                   Kind::TwoByte,    CONNECT | CONNACK,         false, "Receive Maximum" },
    { MQ_PROP_TOPIC_ALIAS_MAXIMUM,               Kind::TwoByte,    CONNECT | CONNACK,         false, "Topic Alias Maximum" },
    { MQ_PROP_TOPIC_ALIAS,                       Kind::TwoByte,    PUBLISH,                   false, "Topic Alias" },
    { MQ_PROP_MAXIMUM_QOS,                       Kind::Byte,       CONNACK,                   false, "Maximum QoS" },
    { MQ_PROP_RETAIN_AVAILABLE,                  Kind::Byte,       CONNACK,                   false, "Retain Available" },
    { MQ_PROP_USER_PROPERTY,                     Kind::StringPair, CONNECT | CONNACK | PUBLISH | WILL | ACKS | SUBSCRIBE | UNSUBSCRIBE | DISCONNECT | AUTH, true, "User Property" },
    { MQ_PROP_MAXIMUM_PACKET_SIZE,               Kind::FourByte,   CONNECT | CONNACK,         false, "Maximum Packet Size" },
    { MQ_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE,   Kind::Byte,       CONNACK,                   false, "Wildcard Subscription Available" },
    { MQ_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE, Kind::Byte,       CONNACK,                   false, "Subscription Identifier Available" },
    { MQ_PROP_SHARED_SUBSCRIPTION_AVAILABLE,     Kind::Byte,       CONNACK,                   false, "Shared Subscription Available" },
};

constexpr size_t TABLE_SIZE = sizeof(TABLE) / sizeof(TABLE[0]);

// The table index is the bit position in MqttProperties::present, so the two
// can never drift apart. 27 entries, which fits a uint32_t with room spare.
static_assert(TABLE_SIZE <= 32, "present is a uint32_t bitmask over this table");

// identifier -> table index, as a lookup rather than a search.
//
// has() and markPresent() are on the per-message path several times over -
// copyForForwarding() alone asks five times per subscriber, and decoding asks
// twice per property - and each ask used to walk all 27 table entries. The
// identifiers are single bytes, so 256 bytes of table answers in one load.
//
// Computed from TABLE by the compiler rather than written out by hand, because
// a table written out by hand is a second copy of TABLE that can disagree with
// it. It used to be built on first use, in 256 bytes of RAM; as a constant it
// is in flash, and there is no first use to race between two tasks.
struct IdentifierIndex {
    int8_t at[256];
};

constexpr IdentifierIndex buildIdentifierIndex() {
    IdentifierIndex index{};
    for (size_t i = 0; i < 256; i++) index.at[i] = -1;
    for (size_t i = 0; i < TABLE_SIZE; i++) index.at[TABLE[i].identifier] = (int8_t)i;
    return index;
}

constexpr IdentifierIndex IDENTIFIER_INDEX = buildIdentifierIndex();

int indexOf(uint8_t identifier) { return IDENTIFIER_INDEX.at[identifier]; }

// V5:3.2.2.3.4, 3.2.2.3.5, 3.1.2.11.6, 3.1.2.11.7 and the availability flags
// all say the same thing: a value other than 0 or 1 is a Protocol Error.
inline bool isBooleanByte(uint8_t value) { return value <= 1; }

} // namespace

bool MqttProperties::has(uint8_t identifier) const {
    const int index = indexOf(identifier);
    return index >= 0 && (present & (1u << index)) != 0;
}

void MqttProperties::markPresent(uint8_t identifier) {
    const int index = indexOf(identifier);
    if (index >= 0) present |= (1u << index);
}

bool MqttProperties::addUserProperty(const MqttSpan& key, const MqttSpan& value) {
    if (userCount >= MQTT_MAX_USER_PROPERTIES) return false;
    user[userCount].key = key;
    user[userCount].value = value;
    userCount++;
    markPresent(MQ_PROP_USER_PROPERTY);
    return true;
}

bool mq_propertyIsKnown(uint8_t identifier) {
    return indexOf(identifier) >= 0;
}

bool mq_propertyAllowedIn(uint8_t identifier, uint8_t packetType) {
    const int index = indexOf(identifier);
    if (index < 0) return false;
    if (packetType > MQ_PKT_AUTH && packetType != MQ_PROP_SCOPE_WILL) return false;
    return (TABLE[index].packets & bit(packetType)) != 0;
}

const char* mq_propertyName(uint8_t identifier) {
    const int index = indexOf(identifier);
    return index >= 0 ? TABLE[index].name : "unknown property";
}

// =============================================================================
// DECODE
// =============================================================================

MqttError mq_decodeProperties(MqttReader& reader, uint8_t packetType,
                              MqttProperties& out) {
    out.clear();

    // [V5:MQTT-2.2.2-1] "If there are no properties, this MUST be indicated by
    // including a Property Length of zero." So the length is always read - an
    // absent property set is a zero, never a missing field.
    const uint32_t length = reader.vbi();
    if (!reader.ok()) return reader.error();

    if (length > reader.remaining()) {
        // The declared length runs past the packet. Caught here rather than
        // discovered field by field, because a decoder that walks a lying
        // length one property at a time is one bounds check away from reading
        // whatever follows the buffer.
        reader.fail(MqttError::Truncated);
        return MqttError::Truncated;
    }

    // Bounded to the declared length before a single property is read. A
    // property that runs past the property block is then caught even when the
    // packet as a whole has bytes to spare - which is the case a single
    // packet-level bound would miss.
    const MqttSpan block = reader.bytes(length);
    if (!reader.ok()) return reader.error();

    MqttReader properties(block.data, block.length);

    while (!properties.atEnd()) {
        const uint32_t rawIdentifier = properties.vbi();
        if (!properties.ok()) {
            reader.fail(properties.error());
            return properties.error();
        }

        // An identifier is a Variable Byte Integer, so it could in principle
        // be large. None is, so anything above a byte is unknown by definition.
        const uint8_t identifier = rawIdentifier <= 0xFFu ? (uint8_t)rawIdentifier : 0xFFu;
        const int index = indexOf(identifier);

        if (index < 0 || (TABLE[index].packets & bit(packetType)) == 0) {
            // V5:2.2.2.2 - a Malformed Packet. Not skipped: skipping would
            // accept packets a conforming peer rejects, and then the two ends
            // disagree about what was said.
            reader.fail(MqttError::PropertyNotAllowed);
            return MqttError::PropertyNotAllowed;
        }

        const Entry& entry = TABLE[index];

        if (!entry.repeatable && out.has(identifier)) {
            // "It is a Protocol Error to include the X more than once", said
            // once per property throughout section 3. One rule, one check.
            reader.fail(MqttError::PropertyRepeated);
            return MqttError::PropertyRepeated;
        }

        MqttError valueError = MqttError::None;

        switch (entry.kind) {
            case Kind::Byte: {
                const uint8_t value = properties.u8();
                if (!properties.ok()) break;
                switch (identifier) {
                    case MQ_PROP_PAYLOAD_FORMAT_INDICATOR:
                        // V5:3.1.3.2.3 - 0 is unspecified bytes, 1 is UTF-8.
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.payloadFormatIndicator = value;
                        break;
                    case MQ_PROP_REQUEST_PROBLEM_INFORMATION:
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.requestProblemInformation = value;
                        break;
                    case MQ_PROP_REQUEST_RESPONSE_INFORMATION:
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.requestResponseInformation = value;
                        break;
                    case MQ_PROP_MAXIMUM_QOS:
                        // V5:3.2.2.3.4 - "It is a Protocol Error to include
                        // Maximum QoS more than once, or to have a value other
                        // than 0 or 1." Note 2 is NOT a legal value here: a
                        // peer that supports QoS 2 omits the property.
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.maximumQos = value;
                        break;
                    case MQ_PROP_RETAIN_AVAILABLE:
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.retainAvailable = value;
                        break;
                    case MQ_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE:
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.wildcardSubscriptionAvailable = value;
                        break;
                    case MQ_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE:
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.subscriptionIdentifiersAvailable = value;
                        break;
                    case MQ_PROP_SHARED_SUBSCRIPTION_AVAILABLE:
                        if (!isBooleanByte(value)) valueError = MqttError::PropertyValueInvalid;
                        out.sharedSubscriptionAvailable = value;
                        break;
                    default: break;
                }
                break;
            }

            case Kind::TwoByte: {
                const uint16_t value = properties.u16();
                if (!properties.ok()) break;
                switch (identifier) {
                    case MQ_PROP_SERVER_KEEP_ALIVE:
                        out.serverKeepAlive = value;
                        break;
                    case MQ_PROP_RECEIVE_MAXIMUM:
                        // V5:3.1.2.11.3 - "It is a Protocol Error to include
                        // the Receive Maximum value more than once or for it
                        // to have the value 0." A quota of zero would mean no
                        // QoS>0 publish could ever be sent.
                        if (value == 0) valueError = MqttError::PropertyValueInvalid;
                        out.receiveMaximum = value;
                        break;
                    case MQ_PROP_TOPIC_ALIAS_MAXIMUM:
                        out.topicAliasMaximum = value;
                        break;
                    case MQ_PROP_TOPIC_ALIAS:
                        // V5:3.3.2.3.4 - "A Topic Alias of 0 is not permitted."
                        if (value == 0) valueError = MqttError::PropertyValueInvalid;
                        out.topicAlias = value;
                        break;
                    default: break;
                }
                break;
            }

            case Kind::FourByte: {
                const uint32_t value = properties.u32();
                if (!properties.ok()) break;
                switch (identifier) {
                    case MQ_PROP_MESSAGE_EXPIRY_INTERVAL:
                        out.messageExpiryInterval = value;
                        break;
                    case MQ_PROP_SESSION_EXPIRY_INTERVAL:
                        out.sessionExpiryInterval = value;
                        break;
                    case MQ_PROP_WILL_DELAY_INTERVAL:
                        out.willDelayInterval = value;
                        break;
                    case MQ_PROP_MAXIMUM_PACKET_SIZE:
                        // V5:3.1.2.11.4 - "It is a Protocol Error to include
                        // the Maximum Packet Size more than once, or for the
                        // value to be set to zero."
                        if (value == 0) valueError = MqttError::PropertyValueInvalid;
                        out.maximumPacketSize = value;
                        break;
                    default: break;
                }
                break;
            }

            case Kind::VarInt: {
                const uint32_t value = properties.vbi();
                if (!properties.ok()) break;
                if (identifier == MQ_PROP_SUBSCRIPTION_IDENTIFIER) {
                    // V5:3.3.2.3.8 - "It is a Protocol Error if the
                    // Subscription Identifier has a value of 0."
                    if (value == 0) {
                        valueError = MqttError::PropertyValueInvalid;
                        break;
                    }
                    if (out.subscriptionIdCount >= MQTT_MAX_SUBSCRIPTION_IDS) {
                        valueError = MqttError::TooManyProperties;
                        break;
                    }
                    out.subscriptionIds[out.subscriptionIdCount++] = value;
                }
                break;
            }

            case Kind::Utf8: {
                const MqttSpan value = properties.utf8();
                if (!properties.ok()) break;
                switch (identifier) {
                    case MQ_PROP_CONTENT_TYPE:               out.contentType = value; break;
                    case MQ_PROP_RESPONSE_TOPIC:             out.responseTopic = value; break;
                    case MQ_PROP_ASSIGNED_CLIENT_IDENTIFIER: out.assignedClientIdentifier = value; break;
                    case MQ_PROP_AUTHENTICATION_METHOD:      out.authenticationMethod = value; break;
                    case MQ_PROP_RESPONSE_INFORMATION:       out.responseInformation = value; break;
                    case MQ_PROP_SERVER_REFERENCE:           out.serverReference = value; break;
                    case MQ_PROP_REASON_STRING:              out.reasonString = value; break;
                    default: break;
                }
                break;
            }

            case Kind::Binary: {
                const MqttSpan value = properties.binary();
                if (!properties.ok()) break;
                switch (identifier) {
                    case MQ_PROP_CORRELATION_DATA:      out.correlationData = value; break;
                    case MQ_PROP_AUTHENTICATION_DATA:   out.authenticationData = value; break;
                    default: break;
                }
                break;
            }

            case Kind::StringPair: {
                const MqttSpan key = properties.utf8();
                const MqttSpan value = properties.utf8();
                if (!properties.ok()) break;
                if (!out.addUserProperty(key, value)) {
                    valueError = MqttError::TooManyProperties;
                }
                break;
            }
        }

        if (!properties.ok()) {
            reader.fail(properties.error());
            return properties.error();
        }
        if (valueError != MqttError::None) {
            reader.fail(valueError);
            return valueError;
        }
        out.markPresent(identifier);
    }

    return MqttError::None;
}

// =============================================================================
// ENCODE
// =============================================================================

namespace {

// The bytes a fixed-width property value occupies.
//
// VarInt, Utf8, Binary and StringPair are not here: the two repeatable ones
// are handled before this is reached, and the variable-width ones need the
// value to measure. Falling through to 0 for those would understate a packet
// and truncate it, so they are measured at their own case above.
size_t sizeOfFixedValue(const Entry& entry) {
    switch (entry.kind) {
        case Kind::Byte:     return 1;
        case Kind::TwoByte:  return 2;
        case Kind::FourByte: return 4;
        default:             return 0;
    }
}

// The property block without its Property Length prefix. The prefix is the
// VBI of THIS number; deriving it from the total instead writes a length one
// short at every VBI width boundary (a 127-byte block said 126).
size_t bodySize(const MqttProperties& properties, uint8_t packetType) {
    size_t body = 0;

    for (size_t i = 0; i < TABLE_SIZE; i++) {
        const Entry& entry = TABLE[i];
        if ((entry.packets & bit(packetType)) == 0) continue;
        if ((properties.present & (1u << i)) == 0) continue;

        if (entry.identifier == MQ_PROP_USER_PROPERTY) {
            for (uint8_t u = 0; u < properties.userCount; u++) {
                body += 1 + mq_utf8Size(properties.user[u].key.length)
                          + mq_utf8Size(properties.user[u].value.length);
            }
            continue;
        }
        if (entry.identifier == MQ_PROP_SUBSCRIPTION_IDENTIFIER) {
            for (uint8_t s = 0; s < properties.subscriptionIdCount; s++) {
                body += 1 + mq_vbiSize(properties.subscriptionIds[s]);
            }
            continue;
        }

        body += 1;   // the identifier

        switch (entry.kind) {
            case Kind::Utf8:
                switch (entry.identifier) {
                    case MQ_PROP_CONTENT_TYPE:               body += mq_utf8Size(properties.contentType.length); break;
                    case MQ_PROP_RESPONSE_TOPIC:             body += mq_utf8Size(properties.responseTopic.length); break;
                    case MQ_PROP_ASSIGNED_CLIENT_IDENTIFIER: body += mq_utf8Size(properties.assignedClientIdentifier.length); break;
                    case MQ_PROP_AUTHENTICATION_METHOD:      body += mq_utf8Size(properties.authenticationMethod.length); break;
                    case MQ_PROP_RESPONSE_INFORMATION:       body += mq_utf8Size(properties.responseInformation.length); break;
                    case MQ_PROP_SERVER_REFERENCE:           body += mq_utf8Size(properties.serverReference.length); break;
                    case MQ_PROP_REASON_STRING:              body += mq_utf8Size(properties.reasonString.length); break;
                    default: break;
                }
                break;
            case Kind::Binary:
                switch (entry.identifier) {
                    case MQ_PROP_CORRELATION_DATA:    body += mq_binarySize(properties.correlationData.length); break;
                    case MQ_PROP_AUTHENTICATION_DATA: body += mq_binarySize(properties.authenticationData.length); break;
                    default: break;
                }
                break;
            default:
                body += sizeOfFixedValue(entry);
                break;
        }
    }

    return body;
}

} // namespace

size_t mq_propertiesSize(const MqttProperties& properties, uint8_t packetType) {
    const size_t body = bodySize(properties, packetType);
    return mq_vbiSize((uint32_t)body) + body;
}

void mq_encodeProperties(MqttWriter& writer, const MqttProperties& properties,
                         uint8_t packetType) {
    // The length is computed first and written once. Writing properties and
    // then going back to fill in a length would mean either a second pass or a
    // reserved gap, and a reserved gap that turns out to be the wrong width is
    // a packet nothing can parse.
    const size_t body = bodySize(properties, packetType);

    writer.vbi((uint32_t)body);

    for (size_t i = 0; i < TABLE_SIZE; i++) {
        const Entry& entry = TABLE[i];
        if ((entry.packets & bit(packetType)) == 0) continue;
        if ((properties.present & (1u << i)) == 0) continue;

        if (entry.identifier == MQ_PROP_USER_PROPERTY) {
            for (uint8_t u = 0; u < properties.userCount; u++) {
                writer.u8(MQ_PROP_USER_PROPERTY);
                writer.utf8(properties.user[u].key.data, properties.user[u].key.length);
                writer.utf8(properties.user[u].value.data, properties.user[u].value.length);
            }
            continue;
        }
        if (entry.identifier == MQ_PROP_SUBSCRIPTION_IDENTIFIER) {
            for (uint8_t s = 0; s < properties.subscriptionIdCount; s++) {
                writer.u8(MQ_PROP_SUBSCRIPTION_IDENTIFIER);
                writer.vbi(properties.subscriptionIds[s]);
            }
            continue;
        }

        writer.u8(entry.identifier);

        switch (entry.identifier) {
            case MQ_PROP_PAYLOAD_FORMAT_INDICATOR:          writer.u8(properties.payloadFormatIndicator); break;
            case MQ_PROP_REQUEST_PROBLEM_INFORMATION:       writer.u8(properties.requestProblemInformation); break;
            case MQ_PROP_REQUEST_RESPONSE_INFORMATION:      writer.u8(properties.requestResponseInformation); break;
            case MQ_PROP_MAXIMUM_QOS:                       writer.u8(properties.maximumQos); break;
            case MQ_PROP_RETAIN_AVAILABLE:                  writer.u8(properties.retainAvailable); break;
            case MQ_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE:   writer.u8(properties.wildcardSubscriptionAvailable); break;
            case MQ_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE: writer.u8(properties.subscriptionIdentifiersAvailable); break;
            case MQ_PROP_SHARED_SUBSCRIPTION_AVAILABLE:     writer.u8(properties.sharedSubscriptionAvailable); break;

            case MQ_PROP_SERVER_KEEP_ALIVE:                 writer.u16(properties.serverKeepAlive); break;
            case MQ_PROP_RECEIVE_MAXIMUM:                   writer.u16(properties.receiveMaximum); break;
            case MQ_PROP_TOPIC_ALIAS_MAXIMUM:               writer.u16(properties.topicAliasMaximum); break;
            case MQ_PROP_TOPIC_ALIAS:                       writer.u16(properties.topicAlias); break;

            case MQ_PROP_MESSAGE_EXPIRY_INTERVAL:           writer.u32(properties.messageExpiryInterval); break;
            case MQ_PROP_SESSION_EXPIRY_INTERVAL:           writer.u32(properties.sessionExpiryInterval); break;
            case MQ_PROP_WILL_DELAY_INTERVAL:               writer.u32(properties.willDelayInterval); break;
            case MQ_PROP_MAXIMUM_PACKET_SIZE:               writer.u32(properties.maximumPacketSize); break;

            case MQ_PROP_CONTENT_TYPE:                      writer.utf8(properties.contentType.data, properties.contentType.length); break;
            case MQ_PROP_RESPONSE_TOPIC:                    writer.utf8(properties.responseTopic.data, properties.responseTopic.length); break;
            case MQ_PROP_ASSIGNED_CLIENT_IDENTIFIER:        writer.utf8(properties.assignedClientIdentifier.data, properties.assignedClientIdentifier.length); break;
            case MQ_PROP_AUTHENTICATION_METHOD:             writer.utf8(properties.authenticationMethod.data, properties.authenticationMethod.length); break;
            case MQ_PROP_RESPONSE_INFORMATION:              writer.utf8(properties.responseInformation.data, properties.responseInformation.length); break;
            case MQ_PROP_SERVER_REFERENCE:                  writer.utf8(properties.serverReference.data, properties.serverReference.length); break;
            case MQ_PROP_REASON_STRING:                     writer.utf8(properties.reasonString.data, properties.reasonString.length); break;

            case MQ_PROP_CORRELATION_DATA:                  writer.binary(properties.correlationData.data, properties.correlationData.length); break;
            case MQ_PROP_AUTHENTICATION_DATA:               writer.binary(properties.authenticationData.data, properties.authenticationData.length); break;

            default: break;
        }
    }
}

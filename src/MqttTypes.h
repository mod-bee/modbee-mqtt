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
// THE WIRE
// =============================================================================
//
// Every number the two standards fix: packet types, the flag bits that go with
// them, the reason codes of 5.0 and the return codes of 3.1.1, and the limits.
//
// These are #define rather than enum class because they are values the
// specification pins, not a closed type this library invents. A byte arriving
// off the wire may hold any of 256 values, and casting one into an enum class
// that does not list it is undefined behaviour - which is the last thing
// wanted in the one place hostile input lands.

#include "MqttPlatform.h"
#include "MqttVersion.h"

// --- Control packet types ---
//
// Byte 1, bits 7-4. V5: Table 2-1. V311: Table 2.1.
// Identical in both versions except AUTH, which 3.1.1 does not define.
#define MQ_PKT_RESERVED       0
#define MQ_PKT_CONNECT        1    // Client to Server: connection request
#define MQ_PKT_CONNACK        2    // Server to Client: connect acknowledgement
#define MQ_PKT_PUBLISH        3    // either direction
#define MQ_PKT_PUBACK         4    // QoS 1 acknowledgement
#define MQ_PKT_PUBREC         5    // QoS 2 part 1
#define MQ_PKT_PUBREL         6    // QoS 2 part 2
#define MQ_PKT_PUBCOMP        7    // QoS 2 part 3
#define MQ_PKT_SUBSCRIBE      8
#define MQ_PKT_SUBACK         9
#define MQ_PKT_UNSUBSCRIBE   10
#define MQ_PKT_UNSUBACK      11
#define MQ_PKT_PINGREQ       12
#define MQ_PKT_PINGRESP      13
#define MQ_PKT_DISCONNECT    14
#define MQ_PKT_AUTH          15    // 5.0 only; reserved in 3.1.1

const char* mq_packetName(uint8_t type);

// Whether this version defines the packet type at all. Type 0 is Forbidden in
// both; AUTH exists only in 5.0, so an AUTH arriving on a 3.1.1 connection is
// a Malformed Packet rather than an unsupported feature.
bool mq_packetTypeIsDefined(uint8_t type, MqttVersion version);

// --- Fixed header flags ---
//
// Byte 1, bits 3-0. V5: Table 2-2, V311: Table 2.2.
//
// Where a flag bit is Reserved it MUST be set to the value listed, and an
// invalid flag is a Malformed Packet [V5:MQTT-2.1.3-1] [V311:MQTT-2.2.2-1].
//
// Three packets reserve 0b0010 rather than 0b0000 - PUBREL, SUBSCRIBE and
// UNSUBSCRIBE - and a codec that assumes all reserved flags are zero produces
// packets every conforming peer rejects. Encoded here as a table rather than
// as scattered literals so the three cannot be forgotten in one place and
// remembered in another.
#define MQ_FLAGS_MASK        0x0Fu

// The required flag value for a packet type, or 0xFF where the type carries
// real flags (PUBLISH) or is not defined.
uint8_t mq_reservedFlags(uint8_t type);

// PUBLISH flags. V5:3.3.1, V311:3.3.1.
#define MQ_PUBLISH_RETAIN    0x01u
#define MQ_PUBLISH_QOS_MASK  0x06u
#define MQ_PUBLISH_QOS_SHIFT 1
#define MQ_PUBLISH_DUP       0x08u

// --- Quality of service ---
//
// A PUBLISH packet MUST NOT have both QoS bits set to 1; a receiver that gets
// one MUST close the connection [V5:MQTT-3.3.1-4] [V311:MQTT-3.3.1-4].
#define MQ_QOS_0             0
#define MQ_QOS_1             1
#define MQ_QOS_2             2
#define MQ_QOS_INVALID       3

// --- Limits fixed by the standards ---

// A Variable Byte Integer is at most four bytes. V5:1.5.5, V311:1.5.2.
#define MQ_VBI_MAX_BYTES     4
#define MQ_VBI_MAX_VALUE     268435455u   // 0xFF 0xFF 0xFF 0x7F

// A UTF-8 Encoded String is length-prefixed by a Two Byte Integer, so it
// cannot exceed 65,535 bytes. V5:1.5.4, V311:1.5.3.
#define MQ_STRING_MAX        65535u

// Binary Data is length-prefixed the same way. V5:1.5.6.
#define MQ_BINARY_MAX        65535u

// The largest a packet can be: a five-byte fixed header plus the maximum
// Remaining Length. Nothing allocates this - it is the ceiling a declared
// length is checked against.
#define MQ_PACKET_MAX        (1u + MQ_VBI_MAX_BYTES + MQ_VBI_MAX_VALUE)

// A Packet Identifier is a Two Byte Integer and MUST be non-zero where one is
// required [V5:MQTT-2.2.1-3].
#define MQ_PACKET_ID_NONE    0u
#define MQ_PACKET_ID_MAX     65535u

// Conventional ports. Not in either standard - they are IANA assignments - but
// nothing else in the library should carry a bare 1883.
#define MQ_PORT_TCP          1883
#define MQ_PORT_TLS          8883

// --- Reason codes, 5.0 only ---
//
// V5: Table 2-6. One byte. Values below 0x80 indicate success and 0x80 or
// above indicate failure (V5:2.4), which is the whole of how a caller decides
// whether an operation worked.
//
// The same numeric value means different things on different packets: 0x00 is
// Success on CONNACK, Normal disconnection on DISCONNECT and Granted QoS 0 on
// SUBACK. So a reason code is never interpreted without knowing which packet
// carried it, and mq_reasonName() below takes the packet type for that reason.
#define MQ_REASON_SUCCESS                        0x00
#define MQ_REASON_NORMAL_DISCONNECTION           0x00
#define MQ_REASON_GRANTED_QOS_0                  0x00
#define MQ_REASON_GRANTED_QOS_1                  0x01
#define MQ_REASON_GRANTED_QOS_2                  0x02
#define MQ_REASON_DISCONNECT_WITH_WILL           0x04
#define MQ_REASON_NO_MATCHING_SUBSCRIBERS        0x10
#define MQ_REASON_NO_SUBSCRIPTION_EXISTED        0x11
#define MQ_REASON_CONTINUE_AUTHENTICATION        0x18
#define MQ_REASON_REAUTHENTICATE                 0x19
#define MQ_REASON_UNSPECIFIED_ERROR              0x80
#define MQ_REASON_MALFORMED_PACKET               0x81
#define MQ_REASON_PROTOCOL_ERROR                 0x82
#define MQ_REASON_IMPLEMENTATION_SPECIFIC_ERROR  0x83
#define MQ_REASON_UNSUPPORTED_PROTOCOL_VERSION   0x84
#define MQ_REASON_CLIENT_IDENTIFIER_NOT_VALID    0x85
#define MQ_REASON_BAD_USER_NAME_OR_PASSWORD      0x86
#define MQ_REASON_NOT_AUTHORIZED                 0x87
#define MQ_REASON_SERVER_UNAVAILABLE             0x88
#define MQ_REASON_SERVER_BUSY                    0x89
#define MQ_REASON_BANNED                         0x8A
#define MQ_REASON_SERVER_SHUTTING_DOWN           0x8B
#define MQ_REASON_BAD_AUTHENTICATION_METHOD      0x8C
#define MQ_REASON_KEEP_ALIVE_TIMEOUT             0x8D
#define MQ_REASON_SESSION_TAKEN_OVER             0x8E
#define MQ_REASON_TOPIC_FILTER_INVALID           0x8F
#define MQ_REASON_TOPIC_NAME_INVALID             0x90
#define MQ_REASON_PACKET_IDENTIFIER_IN_USE       0x91
#define MQ_REASON_PACKET_IDENTIFIER_NOT_FOUND    0x92
#define MQ_REASON_RECEIVE_MAXIMUM_EXCEEDED       0x93
#define MQ_REASON_TOPIC_ALIAS_INVALID            0x94
#define MQ_REASON_PACKET_TOO_LARGE               0x95
#define MQ_REASON_MESSAGE_RATE_TOO_HIGH          0x96
#define MQ_REASON_QUOTA_EXCEEDED                 0x97
#define MQ_REASON_ADMINISTRATIVE_ACTION          0x98
#define MQ_REASON_PAYLOAD_FORMAT_INVALID         0x99
#define MQ_REASON_RETAIN_NOT_SUPPORTED           0x9A
#define MQ_REASON_QOS_NOT_SUPPORTED              0x9B
#define MQ_REASON_USE_ANOTHER_SERVER             0x9C
#define MQ_REASON_SERVER_MOVED                   0x9D
#define MQ_REASON_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED 0x9E
#define MQ_REASON_CONNECTION_RATE_EXCEEDED       0x9F
#define MQ_REASON_MAXIMUM_CONNECT_TIME           0xA0
#define MQ_REASON_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED 0xA1
#define MQ_REASON_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED   0xA2

// V5:2.4 - "Reason Codes less than 0x80 indicate successful completion of an
// operation. The normal Reason Code for success is 0. Reason Code values of
// 0x80 or greater indicate failure."
inline bool mq_reasonIsFailure(uint8_t reason) { return reason >= 0x80; }

// Whether the reason code is one Table 2-6 permits on this packet type. A code
// outside the table for its packet is a Protocol Error, and a peer that sends
// one is telling us something we cannot act on.
bool mq_reasonIsValidFor(uint8_t reason, uint8_t packetType);

const char* mq_reasonName(uint8_t reason, uint8_t packetType);

// --- Connect return codes, 3.1.1 only ---
//
// V311: Table 3.1. A DIFFERENT VALUE SPACE from the reason codes above, and
// the reason both are named here rather than merged: 0x01 means "unacceptable
// protocol level" in 3.1.1 and "Granted QoS 1" in 5.0.
//
// Values 6 to 255 are Reserved for future use, so a CONNACK carrying one is
// malformed rather than an unknown failure.
#define MQ_RETURN_ACCEPTED                0x00
#define MQ_RETURN_UNACCEPTABLE_PROTOCOL   0x01
#define MQ_RETURN_IDENTIFIER_REJECTED     0x02
#define MQ_RETURN_SERVER_UNAVAILABLE      0x03
#define MQ_RETURN_BAD_CREDENTIALS         0x04
#define MQ_RETURN_NOT_AUTHORIZED          0x05
#define MQ_RETURN_MAX                     0x05

const char* mq_returnCodeName(uint8_t code);

// --- SUBACK return codes, 3.1.1 only ---
//
// V311:3.9.3. "SUBACK return codes other than 0x00, 0x01, 0x02 and 0x80 are
// reserved and MUST NOT be used" [V311:MQTT-3.9.3-2]. 5.0 replaces this with
// the full reason-code table, which is why the two codecs cannot share a
// validator here.
#define MQ_SUBACK_MAX_QOS_0   0x00
#define MQ_SUBACK_MAX_QOS_1   0x01
#define MQ_SUBACK_MAX_QOS_2   0x02
#define MQ_SUBACK_FAILURE     0x80

bool mq_subackCodeIsValidV311(uint8_t code);

// --- Big-endian integers ---
//
// "Two Byte Integer data values are 16-bit unsigned integers in big-endian
// order" (V5:1.5.2). Written as explicit byte arithmetic rather than a cast
// over a struct, so the result does not depend on the host's endianness or on
// the compiler's alignment choices.
inline void mq_writeU16(uint8_t* out, uint16_t value) {
    out[0] = (uint8_t)(value >> 8);
    out[1] = (uint8_t)(value & 0xFFu);
}

inline uint16_t mq_readU16(const uint8_t* in) {
    return (uint16_t)(((uint16_t)in[0] << 8) | (uint16_t)in[1]);
}

inline void mq_writeU32(uint8_t* out, uint32_t value) {
    out[0] = (uint8_t)(value >> 24);
    out[1] = (uint8_t)(value >> 16);
    out[2] = (uint8_t)(value >> 8);
    out[3] = (uint8_t)(value & 0xFFu);
}

inline uint32_t mq_readU32(const uint8_t* in) {
    return ((uint32_t)in[0] << 24) | ((uint32_t)in[1] << 16) |
           ((uint32_t)in[2] << 8)  | (uint32_t)in[3];
}

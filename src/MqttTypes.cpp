// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTypes.h"
#include "MqttNoExcept.h"

namespace {

// A packet type as a bit, so "which packets may carry this reason code" is one
// word rather than a list to walk.
//
// `unsigned` rather than uint16_t: an OR of two uint16_t promotes to int, so a
// narrower type here would need a cast on every row of the table below - and a
// table with a cast on every row is a table nobody proofreads.
typedef unsigned PacketMask;

// constexpr, and every mask below with it, so REASONS is a constant the
// compiler lays out in flash. With an inline function here the table was built
// in RAM by a static constructor at boot - about half a kilobyte of a board's
// internal RAM, for a table that never changes.
constexpr PacketMask bit(uint8_t packetType) { return 1u << packetType; }

constexpr PacketMask CONNACK    = bit(MQ_PKT_CONNACK);
constexpr PacketMask PUBACK     = bit(MQ_PKT_PUBACK);
constexpr PacketMask PUBREC     = bit(MQ_PKT_PUBREC);
constexpr PacketMask PUBREL     = bit(MQ_PKT_PUBREL);
constexpr PacketMask PUBCOMP    = bit(MQ_PKT_PUBCOMP);
constexpr PacketMask SUBACK     = bit(MQ_PKT_SUBACK);
constexpr PacketMask UNSUBACK   = bit(MQ_PKT_UNSUBACK);
constexpr PacketMask DISCONNECT = bit(MQ_PKT_DISCONNECT);
constexpr PacketMask AUTH       = bit(MQ_PKT_AUTH);

// V5: Table 2-6, transcribed. The packet mask is the table's own third column.
//
// It is enforced rather than documented because a reason code outside its
// table for the packet that carried it is a Protocol Error: the peer is
// telling us something the standard gives us no way to act on, and guessing
// what it meant is worse than refusing.
struct ReasonEntry {
    uint8_t code;
    PacketMask packets;
    const char* name;
};

constexpr ReasonEntry REASONS[] = {
    // 0x00 has three names; which one applies is decided by the packet, so it
    // appears three times rather than once with a name that is wrong twice.
    { 0x00, CONNACK | PUBACK | PUBREC | PUBREL | PUBCOMP | UNSUBACK | AUTH, "Success" },
    { 0x00, DISCONNECT,                          "Normal disconnection" },
    { 0x00, SUBACK,                              "Granted QoS 0" },
    { 0x01, SUBACK,                              "Granted QoS 1" },
    { 0x02, SUBACK,                              "Granted QoS 2" },
    { 0x04, DISCONNECT,                          "Disconnect with Will Message" },
    { 0x10, PUBACK | PUBREC,                     "No matching subscribers" },
    { 0x11, UNSUBACK,                            "No subscription existed" },
    { 0x18, AUTH,                                "Continue authentication" },
    { 0x19, AUTH,                                "Re-authenticate" },
    { 0x80, CONNACK | PUBACK | PUBREC | SUBACK | UNSUBACK | DISCONNECT, "Unspecified error" },
    { 0x81, CONNACK | DISCONNECT,                "Malformed Packet" },
    { 0x82, CONNACK | DISCONNECT,                "Protocol Error" },
    { 0x83, CONNACK | PUBACK | PUBREC | SUBACK | UNSUBACK | DISCONNECT, "Implementation specific error" },
    { 0x84, CONNACK,                             "Unsupported Protocol Version" },
    { 0x85, CONNACK,                             "Client Identifier not valid" },
    { 0x86, CONNACK,                             "Bad User Name or Password" },
    { 0x87, CONNACK | PUBACK | PUBREC | SUBACK | UNSUBACK | DISCONNECT, "Not authorized" },
    { 0x88, CONNACK,                             "Server unavailable" },
    { 0x89, CONNACK | DISCONNECT,                "Server busy" },
    { 0x8A, CONNACK,                             "Banned" },
    { 0x8B, DISCONNECT,                          "Server shutting down" },
    { 0x8C, CONNACK | DISCONNECT,                "Bad authentication method" },
    { 0x8D, DISCONNECT,                          "Keep Alive timeout" },
    { 0x8E, DISCONNECT,                          "Session taken over" },
    { 0x8F, SUBACK | UNSUBACK | DISCONNECT,      "Topic Filter invalid" },
    { 0x90, CONNACK | PUBACK | PUBREC | DISCONNECT, "Topic Name invalid" },
    { 0x91, PUBACK | PUBREC | SUBACK | UNSUBACK, "Packet Identifier in use" },
    { 0x92, PUBREL | PUBCOMP,                    "Packet Identifier not found" },
    { 0x93, DISCONNECT,                          "Receive Maximum exceeded" },
    { 0x94, DISCONNECT,                          "Topic Alias invalid" },
    { 0x95, CONNACK | DISCONNECT,                "Packet too large" },
    { 0x96, DISCONNECT,                          "Message rate too high" },
    { 0x97, CONNACK | PUBACK | PUBREC | SUBACK | DISCONNECT, "Quota exceeded" },
    { 0x98, DISCONNECT,                          "Administrative action" },
    { 0x99, CONNACK | PUBACK | PUBREC | DISCONNECT, "Payload format invalid" },
    { 0x9A, CONNACK | DISCONNECT,                "Retain not supported" },
    { 0x9B, CONNACK | DISCONNECT,                "QoS not supported" },
    { 0x9C, CONNACK | DISCONNECT,                "Use another server" },
    { 0x9D, CONNACK | DISCONNECT,                "Server moved" },
    { 0x9E, SUBACK | DISCONNECT,                 "Shared Subscriptions not supported" },
    { 0x9F, CONNACK | DISCONNECT,                "Connection rate exceeded" },
    { 0xA0, DISCONNECT,                          "Maximum connect time" },
    { 0xA1, SUBACK | DISCONNECT,                 "Subscription Identifiers not supported" },
    { 0xA2, SUBACK | DISCONNECT,                 "Wildcard Subscriptions not supported" },
};

constexpr size_t REASON_COUNT = sizeof(REASONS) / sizeof(REASONS[0]);

} // namespace

const char* mq_packetName(uint8_t type) {
    switch (type) {
        case MQ_PKT_RESERVED:    return "RESERVED";
        case MQ_PKT_CONNECT:     return "CONNECT";
        case MQ_PKT_CONNACK:     return "CONNACK";
        case MQ_PKT_PUBLISH:     return "PUBLISH";
        case MQ_PKT_PUBACK:      return "PUBACK";
        case MQ_PKT_PUBREC:      return "PUBREC";
        case MQ_PKT_PUBREL:      return "PUBREL";
        case MQ_PKT_PUBCOMP:     return "PUBCOMP";
        case MQ_PKT_SUBSCRIBE:   return "SUBSCRIBE";
        case MQ_PKT_SUBACK:      return "SUBACK";
        case MQ_PKT_UNSUBSCRIBE: return "UNSUBSCRIBE";
        case MQ_PKT_UNSUBACK:    return "UNSUBACK";
        case MQ_PKT_PINGREQ:     return "PINGREQ";
        case MQ_PKT_PINGRESP:    return "PINGRESP";
        case MQ_PKT_DISCONNECT:  return "DISCONNECT";
        case MQ_PKT_AUTH:        return "AUTH";
    }
    return "?";
}

bool mq_packetTypeIsDefined(uint8_t type, MqttVersion version) {
    if (type == MQ_PKT_RESERVED || type > MQ_PKT_AUTH) return false;
    // AUTH and the whole enhanced authentication exchange arrived with 5.0, so
    // one on a 3.1.1 connection is a packet type that version does not define.
    if (type == MQ_PKT_AUTH) return version == MqttVersion::V5;
    return true;
}

uint8_t mq_reservedFlags(uint8_t type) {
    switch (type) {
        // The three that are not zero. Getting one of these wrong produces
        // packets every conforming peer rejects, and the symptom is a broker
        // that closes the connection with no explanation.
        case MQ_PKT_PUBREL:
        case MQ_PKT_SUBSCRIBE:
        case MQ_PKT_UNSUBSCRIBE:
            return 0x02u;

        case MQ_PKT_CONNECT:
        case MQ_PKT_CONNACK:
        case MQ_PKT_PUBACK:
        case MQ_PKT_PUBREC:
        case MQ_PKT_PUBCOMP:
        case MQ_PKT_SUBACK:
        case MQ_PKT_UNSUBACK:
        case MQ_PKT_PINGREQ:
        case MQ_PKT_PINGRESP:
        case MQ_PKT_DISCONNECT:
        case MQ_PKT_AUTH:
            return 0x00u;

        // PUBLISH carries DUP, QoS and RETAIN, so there is no single required
        // value. 0xFF says "ask the PUBLISH rules", not "any flags will do".
        case MQ_PKT_PUBLISH:
        default:
            return 0xFFu;
    }
}

bool mq_reasonIsValidFor(uint8_t reason, uint8_t packetType) {
    if (packetType > MQ_PKT_AUTH) return false;
    const PacketMask wanted = bit(packetType);
    for (size_t i = 0; i < REASON_COUNT; i++) {
        if (REASONS[i].code == reason && (REASONS[i].packets & wanted) != 0) {
            return true;
        }
    }
    return false;
}

const char* mq_reasonName(uint8_t reason, uint8_t packetType) {
    if (packetType <= MQ_PKT_AUTH) {
        const PacketMask wanted = bit(packetType);
        for (size_t i = 0; i < REASON_COUNT; i++) {
            if (REASONS[i].code == reason && (REASONS[i].packets & wanted) != 0) {
                return REASONS[i].name;
            }
        }
    }
    // Not valid for this packet, but naming it anyway helps whoever is reading
    // a trace of a peer that sent it.
    for (size_t i = 0; i < REASON_COUNT; i++) {
        if (REASONS[i].code == reason) return REASONS[i].name;
    }
    return "unknown reason code";
}

const char* mq_returnCodeName(uint8_t code) {
    switch (code) {
        case MQ_RETURN_ACCEPTED:              return "Connection Accepted";
        case MQ_RETURN_UNACCEPTABLE_PROTOCOL: return "Connection Refused, unacceptable protocol version";
        case MQ_RETURN_IDENTIFIER_REJECTED:   return "Connection Refused, identifier rejected";
        case MQ_RETURN_SERVER_UNAVAILABLE:    return "Connection Refused, Server unavailable";
        case MQ_RETURN_BAD_CREDENTIALS:       return "Connection Refused, bad user name or password";
        case MQ_RETURN_NOT_AUTHORIZED:        return "Connection Refused, not authorized";
    }
    return "reserved return code";
}

bool mq_subackCodeIsValidV311(uint8_t code) {
    return code == MQ_SUBACK_MAX_QOS_0 || code == MQ_SUBACK_MAX_QOS_1 ||
           code == MQ_SUBACK_MAX_QOS_2 || code == MQ_SUBACK_FAILURE;
}

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
// THE PACKET MODEL
// =============================================================================
//
// One set of structures for both protocol versions.
//
// The fields are 5.0's, because 5.0 is a superset of 3.1.1 in everything but
// the value spaces of CONNACK and SUBACK. A 3.1.1 packet decodes into the same
// structure with the 5.0-only fields left absent, so everything above the
// codec is written once and the version is a property of the connection rather
// than a branch in the application.
//
// SPANS POINT INTO THE BUFFER BEING DECODED. Nothing here copies a payload: a
// 200 kB PUBLISH should not be copied on its way past, least of all twice. A
// decoded packet is therefore valid only while the bytes it came from are, and
// anything that must outlive them - a queued publish, a stored session entry -
// copies what it needs at that point and says so.
//
// There is no role in this file. A CONNECT is a CONNECT whichever end builds
// it, which is what lets a broker be a different L3 over this same code.

#include "MqttFeatures.h"
#include "MqttProperties.h"

// --- Where a packet's properties are ---
//
// On a host every packet kind below carries its own property set. On a board
// (MQTT_LEAN, MqttFeatures.h) the packet carries ONE, shared by whichever kind
// it is: eleven sets were 3 kB of every packet on the stack, and the code that
// initialised them was repeated wherever a packet was built. A Will keeps its
// own, because it travels inside a CONNECT that has properties of its own.
//
// Code reaches a kind's set through MQ_PROPERTIES(packet, kind), where `kind`
// is that packet's member or a reference to it:
//
//     const MqttPublish& publish = packet.publish;
//     mq_encodeProperties(writer, MQ_PROPERTIES(packet, publish), MQ_PKT_PUBLISH);
//
// On a host it is `publish.properties`, the very same tokens as before.
#if MQTT_LEAN
    #define MQ_PROPERTIES(packet, kind) (packet).properties
#else
    #define MQ_PROPERTIES(packet, kind) kind.properties
#endif

// Topic filters carried in one SUBSCRIBE or UNSUBSCRIBE, and reason codes in
// one SUBACK or UNSUBACK.
//
// Bounded rather than grown. A peer may send a SUBACK with as many reason
// codes as it likes, and on a device with no spare heap the answer to that has
// to be a limit rather than an allocation. Raise it at build time where an
// application really does subscribe to more in one packet.
//
// SIXTEEN, NOT EIGHT. A batched subscribe is what every mainstream client
// library does - Paho's subscribe(list) is one call - and eight is low enough
// that ordinary code reaches it. Exceeding it can only end the connection,
// because SUBACK owes exactly one reason code per filter in the request
// [V5:MQTT-3.9.3-1] and a truncated answer would be a protocol violation of
// its own; so the limit is set where real clients do not meet it, and a peer
// that does meet it is now told 0x97 Quota Exceeded rather than 0x80.
#ifndef MQTT_MAX_FILTERS_PER_PACKET
#define MQTT_MAX_FILTERS_PER_PACKET 16
#endif

// --- Fixed header ---
//
// V5:2.1, V311:2.2. Present on every packet: one byte of type and flags, then
// a Variable Byte Integer Remaining Length.
struct MqttFixedHeader {
    uint8_t type = MQ_PKT_RESERVED;
    uint8_t flags = 0;
    uint32_t remainingLength = 0;

    // Bytes the header itself occupies: 1 plus the Remaining Length's own
    // encoding, so 2 to 5. The reassembly above needs this to know where the
    // body starts without decoding twice.
    uint8_t headerLength = 0;

    // Total bytes of the packet. V5:2.1.4 - "The packet size is the total
    // number of bytes in an MQTT Control Packet, this is equal to the length
    // of the Fixed Header plus the Remaining Length."
    uint32_t packetSize() const { return headerLength + remainingLength; }
};

// Reads only the fixed header, from however many bytes have arrived so far.
//
// This is what a stream reassembler calls to find out how much more to wait
// for, so `Truncated` here means "not yet" rather than "malformed" - the only
// place in the library where that is true, and why it is a separate function
// rather than part of a packet decode.
MqttError mq_decodeFixedHeader(const uint8_t* data, size_t available,
                               MqttFixedHeader& out);

size_t mq_fixedHeaderSize(uint32_t remainingLength);

// The Protocol Level byte inside a CONNECT's variable header, read without
// decoding anything else.
//
// An ACCEPTING end cannot know which codec to use until it has read this, and
// it cannot get there by decoding: each codec refuses a level it does not
// implement, which is exactly right and exactly unhelpful here. So the one
// byte that decides is read on its own.
//
// `body` is the packet body - the bytes after the fixed header. Returns false
// when those bytes are not a plausible CONNECT variable header at all, in
// which case nothing about them should be trusted.
//
// Role-neutral: a proxy, a recorder or a test peer needs exactly this too, and
// none of them is a server.
bool mq_peekProtocolLevel(const uint8_t* body, size_t length, uint8_t& level);
void mq_encodeFixedHeader(MqttWriter& writer, uint8_t type, uint8_t flags,
                          uint32_t remainingLength);

// --- Will ---
//
// V5:3.1.3.2, V311:3.1.3.2. Carried inside the CONNECT payload, with its own
// property set whose identifiers are a different subset from CONNECT's own.
struct MqttWill {
    MqttSpan topic;
    MqttSpan payload;          // bytes, never a string: a Will may be protobuf
    uint8_t qos = MQ_QOS_0;
    bool retain = false;
    MqttProperties properties; // 5.0 only; ignored when encoding 3.1.1
};

// --- CONNECT ---
struct MqttConnect {
    MqttVersion version = MqttVersion::V5;
    MqttSpan clientId;

    // V5 calls this Clean Start and pairs it with Session Expiry Interval;
    // 3.1.1 calls it Clean Session and it alone decides whether state is kept.
    // One flag bit either way, so one field.
    bool cleanStart = true;

    uint16_t keepAlive = 0;

    bool hasWill = false;
    MqttWill will;

    bool hasUserName = false;
    MqttSpan userName;

    // Binary Data, not a string. V5:3.1.3.6 makes the Password Binary Data;
    // 3.1.1 does too. Treating it as text would corrupt any password with a
    // byte that is not valid UTF-8.
    bool hasPassword = false;
    MqttSpan password;

#if !MQTT_LEAN
    MqttProperties properties;
#endif
};

// --- CONNACK ---
struct MqttConnAck {
    bool sessionPresent = false;

    // A 5.0 Reason Code or a 3.1.1 Connect Return Code. THE TWO VALUE SPACES
    // ARE DIFFERENT and only the connection's version says which this is:
    // 0x01 means "Granted QoS 1" in 5.0 and "unacceptable protocol version" in
    // 3.1.1. Whoever reads this field must already know the version, which is
    // why the decoder records it below.
    uint8_t reasonCode = 0;

    // Which value space `reasonCode` is in. Set by the decoder from the shape
    // of the packet, not from what was requested - a 3.1.1-only peer answers a
    // 5.0 CONNECT with a 3.1.1 CONNACK, and that is exactly how the fallback
    // finds out.
    MqttVersion version = MqttVersion::V5;

#if !MQTT_LEAN
    MqttProperties properties;
#endif
};

// --- PUBLISH ---
struct MqttPublish {
    bool dup = false;
    bool retain = false;
    uint8_t qos = MQ_QOS_0;

    MqttSpan topic;

    // Zero when QoS is 0. V5:2.2.1 - a Packet Identifier is present only
    // when QoS > 0.
    uint16_t packetId = MQ_PACKET_ID_NONE;

#if !MQTT_LEAN
    MqttProperties properties;
#endif

    // Whatever the Remaining Length leaves over. May legitimately be empty:
    // a zero-length payload to a retained topic is how a retained message is
    // cleared.
    MqttSpan payload;
};

// --- SUBSCRIBE ---

// V5:3.8.3.1. 3.1.1 has only the QoS bits and reserves the rest.
#define MQ_SUBOPT_QOS_MASK        0x03u
#define MQ_SUBOPT_NO_LOCAL        0x04u
#define MQ_SUBOPT_RETAIN_AS_PUB   0x08u
#define MQ_SUBOPT_RETAIN_HANDLING_MASK  0x30u
#define MQ_SUBOPT_RETAIN_HANDLING_SHIFT 4
#define MQ_SUBOPT_RESERVED_MASK   0xC0u

// V5:3.8.3.1 Retain Handling values.
#define MQ_RETAIN_SEND_ALWAYS      0
#define MQ_RETAIN_SEND_IF_NEW      1
#define MQ_RETAIN_SEND_NEVER       2

struct MqttSubscription {
    MqttSpan filter;
    uint8_t options = 0;

    uint8_t qos() const { return options & MQ_SUBOPT_QOS_MASK; }
    bool noLocal() const { return (options & MQ_SUBOPT_NO_LOCAL) != 0; }
    bool retainAsPublished() const { return (options & MQ_SUBOPT_RETAIN_AS_PUB) != 0; }
    uint8_t retainHandling() const {
        return (uint8_t)((options & MQ_SUBOPT_RETAIN_HANDLING_MASK)
                         >> MQ_SUBOPT_RETAIN_HANDLING_SHIFT);
    }
};


// A BOUND THAT DOES NOT FIT ITS COUNTER CORRUPTS RATHER THAN REFUSES.
//
// These are build flags. Raised past what the field holding the count can
// express, the count wraps silently - and the symptom is not "the bound was
// too big", it is messages going missing. Checked here, at compile time,
// rather than by whoever picked the number.
static_assert(MQTT_MAX_FILTERS_PER_PACKET <= 255,
              "MQTT_MAX_FILTERS_PER_PACKET must fit the uint8_t counts that "
              "SUBSCRIBE, UNSUBSCRIBE and SUBACK each keep");

struct MqttSubscribe {
    uint16_t packetId = MQ_PACKET_ID_NONE;
#if !MQTT_LEAN
    MqttProperties properties;
#endif
    MqttSubscription filters[MQTT_MAX_FILTERS_PER_PACKET];
    uint8_t count = 0;
};

struct MqttUnsubscribe {
    uint16_t packetId = MQ_PACKET_ID_NONE;
#if !MQTT_LEAN
    MqttProperties properties;
#endif
    MqttSpan filters[MQTT_MAX_FILTERS_PER_PACKET];
    uint8_t count = 0;
};

// --- SUBACK and UNSUBACK ---
//
// One reason code per filter, in the order the request listed them
// [V5:MQTT-3.9.3-1] [V311:MQTT-3.9.3-1]. In 3.1.1 these are the four SUBACK
// return codes; in 5.0 they are reason codes from Table 2-6.
struct MqttSubAck {
    uint16_t packetId = MQ_PACKET_ID_NONE;
#if !MQTT_LEAN
    MqttProperties properties;
#endif
    uint8_t reasons[MQTT_MAX_FILTERS_PER_PACKET] = {0};
    uint8_t count = 0;
};

// --- PUBACK, PUBREC, PUBREL, PUBCOMP ---
//
// One shape for all four. They differ only in their packet type and in which
// reason codes Table 2-6 permits, and both of those are checked by the codec
// rather than by the structure.
struct MqttPubAck {
    uint16_t packetId = MQ_PACKET_ID_NONE;

    // V5:3.4.2.1 - the Reason Code and Property Length may both be omitted
    // when the reason is Success and there are no properties, which makes the
    // packet two bytes rather than four. Both forms are legal and both are
    // produced by real brokers, so both are decoded and the shorter is what we
    // emit when we can.
    uint8_t reasonCode = MQ_REASON_SUCCESS;
#if !MQTT_LEAN
    MqttProperties properties;
#endif
};

// --- DISCONNECT and AUTH ---
struct MqttDisconnect {
    // V5:3.14.2.1 - may be omitted, meaning Normal disconnection. 3.1.1 has no
    // variable header at all, so there is nothing here to encode for it.
    uint8_t reasonCode = MQ_REASON_NORMAL_DISCONNECTION;
#if !MQTT_LEAN
    MqttProperties properties;
#endif
};

struct MqttAuth {
    uint8_t reasonCode = MQ_REASON_SUCCESS;
#if !MQTT_LEAN
    MqttProperties properties;
#endif
};

// --- One packet, whatever it is ---
//
// A struct rather than a union: the members with spans and property sets are
// not trivially copyable in a way a union would make pleasant, and the whole
// thing is one stack object per packet decoded rather than one per connection.
//
// A single decode entry point matters more than the few hundred bytes. It is
// what lets the fuzzer throw arbitrary bytes at every packet type through one
// call, and what keeps a future broker from needing a second dispatch.
struct MqttPacket {
    MqttFixedHeader header;

    MqttConnect connect;
    MqttConnAck connack;
    MqttPublish publish;
    MqttPubAck ack;              // PUBACK, PUBREC, PUBREL, PUBCOMP
    MqttSubscribe subscribe;
    MqttSubAck suback;           // SUBACK and UNSUBACK
    MqttUnsubscribe unsubscribe;
    MqttDisconnect disconnect;
    MqttAuth auth;

#if MQTT_LEAN
    MqttProperties properties;   // whichever kind it is (see MQ_PROPERTIES)
#endif

    uint8_t type() const { return header.type; }
    void clear() { *this = MqttPacket(); }
};

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttCodecV5.h"

namespace {

// --- CONNECT flag bits, V5:3.1.2.3 ---
const uint8_t CONNECT_RESERVED     = 0x01;
const uint8_t CONNECT_CLEAN_START  = 0x02;
const uint8_t CONNECT_WILL_FLAG    = 0x04;
const uint8_t CONNECT_WILL_QOS     = 0x18;
const uint8_t CONNECT_WILL_RETAIN  = 0x20;
const uint8_t CONNECT_PASSWORD     = 0x40;
const uint8_t CONNECT_USER_NAME    = 0x80;
const uint8_t CONNECT_WILL_QOS_SHIFT = 3;

// --- CONNACK acknowledge flags, V5:3.2.2.1 ---
const uint8_t CONNACK_SESSION_PRESENT = 0x01;
const uint8_t CONNACK_RESERVED        = 0xFE;

// Whether the fixed header's flags are what this packet type requires.
//
// [V5:MQTT-2.1.3-1] "Where a flag bit is marked as Reserved, it is reserved
// for future use and MUST be set to the value listed", and an invalid flag is
// a Malformed Packet. Three types reserve 0b0010 rather than zero, which is
// the part most often got wrong.
MqttError checkFlags(const MqttFixedHeader& header) {
    if (header.type == MQ_PKT_PUBLISH) {
        // PUBLISH carries real flags, checked where the QoS is read.
        return MqttError::None;
    }
    const uint8_t required = mq_reservedFlags(header.type);
    if (required == 0xFFu) return MqttError::PropertyNotAllowed;  // undefined type
    return header.flags == required ? MqttError::None : MqttError::InvalidUtf8;
}

// A reason code is refused when Table 2-6 does not list it for this packet.
// The peer is telling us something the standard gives us no way to act on, and
// guessing what it meant is worse than refusing.
MqttError checkReason(uint8_t reason, uint8_t packetType) {
    return mq_reasonIsValidFor(reason, packetType)
               ? MqttError::None : MqttError::PropertyValueInvalid;
}

} // namespace

// =============================================================================
// DECODE
// =============================================================================

MqttError MqttCodecV5::decode(const MqttFixedHeader& header,
                              const uint8_t* body, size_t length,
                              MqttPacket& out) const {
    out.clear();
    out.header = header;

    if (!mq_packetTypeIsDefined(header.type, MqttVersion::V5)) {
        return MqttError::PropertyNotAllowed;
    }

    // Flags first, because a wrong reserved bit makes the packet malformed
    // whatever its body says, and reading the body first would mean acting on
    // a packet a conforming peer would have rejected outright.
    if (header.type != MQ_PKT_PUBLISH) {
        const MqttError flagError = checkFlags(header);
        if (flagError != MqttError::None) return flagError;
    }

    MqttReader reader(body, length);

    switch (header.type) {

    case MQ_PKT_CONNECT: {
        MqttConnect& connect = out.connect;

        const MqttSpan name = reader.utf8();
        if (!reader.ok()) return reader.error();
        if (!mq_spanEquals(name, MQ_PROTOCOL_NAME)) {
            // V5:3.1.2.1 - anything else is not MQTT at this version.
            return MqttError::InvalidUtf8;
        }

        const uint8_t level = reader.u8();
        if (!reader.ok()) return reader.error();
        if (level != (uint8_t)MqttVersion::V5) return MqttError::PropertyValueInvalid;
        connect.version = MqttVersion::V5;

        const uint8_t flags = reader.u8();
        if (!reader.ok()) return reader.error();

        // [V5:MQTT-3.1.2-3] The Server MUST validate that the reserved flag is
        // set to 0. Checked here rather than ignored, because a peer that sets
        // it is a peer whose idea of the packet differs from ours.
        if ((flags & CONNECT_RESERVED) != 0) return MqttError::PropertyValueInvalid;

        connect.cleanStart = (flags & CONNECT_CLEAN_START) != 0;
        connect.hasWill = (flags & CONNECT_WILL_FLAG) != 0;
        connect.will.qos = (uint8_t)((flags & CONNECT_WILL_QOS) >> CONNECT_WILL_QOS_SHIFT);
        connect.will.retain = (flags & CONNECT_WILL_RETAIN) != 0;
        connect.hasUserName = (flags & CONNECT_USER_NAME) != 0;
        connect.hasPassword = (flags & CONNECT_PASSWORD) != 0;

        if (!connect.hasWill) {
            // [V5:MQTT-3.1.2-11] If the Will Flag is 0, Will QoS MUST be 0 and
            // Will Retain MUST be 0. A peer that sets them without a Will has
            // built the packet from a structure it did not clear.
            if (connect.will.qos != MQ_QOS_0 || connect.will.retain) {
                return MqttError::PropertyValueInvalid;
            }
        } else if (connect.will.qos == MQ_QOS_INVALID) {
            // [V5:MQTT-3.1.2-12] Will QoS MUST NOT be 3.
            return MqttError::PropertyValueInvalid;
        }

        connect.keepAlive = reader.u16();
        if (!reader.ok()) return reader.error();

        const MqttError propertyError =
            mq_decodeProperties(reader, MQ_PKT_CONNECT, connect.properties);
        if (propertyError != MqttError::None) return propertyError;

        connect.clientId = reader.utf8();
        if (!reader.ok()) return reader.error();

        if (connect.hasWill) {
            const MqttError willProperties =
                mq_decodeProperties(reader, MQ_PROP_SCOPE_WILL, connect.will.properties);
            if (willProperties != MqttError::None) return willProperties;
            connect.will.topic = reader.utf8();
            connect.will.payload = reader.binary();
            if (!reader.ok()) return reader.error();
        }
        if (connect.hasUserName) {
            connect.userName = reader.utf8();
            if (!reader.ok()) return reader.error();
        }
        if (connect.hasPassword) {
            // Binary Data, not a string: a password with a byte that is not
            // valid UTF-8 is legal and must survive.
            connect.password = reader.binary();
            if (!reader.ok()) return reader.error();
        }
        break;
    }

    case MQ_PKT_CONNACK: {
        MqttConnAck& connack = out.connack;
        connack.version = MqttVersion::V5;

        const uint8_t flags = reader.u8();
        if (!reader.ok()) return reader.error();

        // [V5:MQTT-3.2.2-1] Bits 7-1 are reserved and MUST be set to 0.
        if ((flags & CONNACK_RESERVED) != 0) return MqttError::PropertyValueInvalid;
        connack.sessionPresent = (flags & CONNACK_SESSION_PRESENT) != 0;

        connack.reasonCode = reader.u8();
        if (!reader.ok()) return reader.error();
        const MqttError reasonError = checkReason(connack.reasonCode, MQ_PKT_CONNACK);
        if (reasonError != MqttError::None) return reasonError;

        const MqttError propertyError =
            mq_decodeProperties(reader, MQ_PKT_CONNACK, connack.properties);
        if (propertyError != MqttError::None) return propertyError;
        break;
    }

    case MQ_PKT_PUBLISH: {
        MqttPublish& publish = out.publish;
        publish.dup = (header.flags & MQ_PUBLISH_DUP) != 0;
        publish.retain = (header.flags & MQ_PUBLISH_RETAIN) != 0;
        publish.qos = (uint8_t)((header.flags & MQ_PUBLISH_QOS_MASK) >> MQ_PUBLISH_QOS_SHIFT);

        // [V5:MQTT-3.3.1-4] A PUBLISH packet MUST NOT have both QoS bits set
        // to 1. A receiver that gets one MUST close the Network Connection.
        if (publish.qos == MQ_QOS_INVALID) return MqttError::PropertyValueInvalid;

        // [V5:MQTT-3.3.1-2] The DUP flag MUST be set to 0 for all QoS 0
        // messages. There is nothing to redeliver at QoS 0, so a DUP here
        // means the two ends disagree about what was sent.
        if (publish.qos == MQ_QOS_0 && publish.dup) return MqttError::PropertyValueInvalid;

        publish.topic = reader.utf8();
        if (!reader.ok()) return reader.error();

        if (publish.qos > MQ_QOS_0) {
            publish.packetId = reader.u16();
            if (!reader.ok()) return reader.error();
            // [V5:MQTT-2.2.1-3] a Packet Identifier MUST be non-zero.
            if (publish.packetId == MQ_PACKET_ID_NONE) {
                return MqttError::PropertyValueInvalid;
            }
        }

        const MqttError propertyError =
            mq_decodeProperties(reader, MQ_PKT_PUBLISH, publish.properties);
        if (propertyError != MqttError::None) return propertyError;

        // Whatever is left. The payload's length is not a field - it is what
        // the Remaining Length leaves over, which is why every field before it
        // has to have been read exactly.
        publish.payload = reader.rest();
        if (!reader.ok()) return reader.error();
        return MqttError::None;   // rest() consumed everything; expectEnd would be a no-op
    }

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP: {
        MqttPubAck& ack = out.ack;
        ack.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (ack.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;

        // V5:3.4.2.1 - "If the Remaining Length is 2, then there is no Reason
        // Code and the value of 0x00 (Success) is used." Both the two-byte and
        // the longer form are legal and real brokers send both, so both decode.
        if (header.remainingLength == 2) {
            ack.reasonCode = MQ_REASON_SUCCESS;
            break;
        }

        ack.reasonCode = reader.u8();
        if (!reader.ok()) return reader.error();
        const MqttError reasonError = checkReason(ack.reasonCode, header.type);
        if (reasonError != MqttError::None) return reasonError;

        // V5:3.4.2.2.1 - "If the Remaining Length is less than 4 there is no
        // Property Length and the value of 0 is used."
        if (header.remainingLength < 4) break;

        const MqttError propertyError =
            mq_decodeProperties(reader, header.type, ack.properties);
        if (propertyError != MqttError::None) return propertyError;
        break;
    }

    case MQ_PKT_SUBSCRIBE: {
        MqttSubscribe& subscribe = out.subscribe;
        subscribe.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (subscribe.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;

        const MqttError propertyError =
            mq_decodeProperties(reader, MQ_PKT_SUBSCRIBE, subscribe.properties);
        if (propertyError != MqttError::None) return propertyError;

        while (!reader.atEnd()) {
            if (subscribe.count >= MQTT_MAX_FILTERS_PER_PACKET) {
                return MqttError::TooManyProperties;
            }
            MqttSubscription& filter = subscribe.filters[subscribe.count];
            filter.filter = reader.utf8();
            filter.options = reader.u8();
            if (!reader.ok()) return reader.error();

            // V5:3.8.3.1 - Maximum QoS of 3 is a Protocol Error, Retain
            // Handling of 3 is a Protocol Error, and [V5:MQTT-3.8.3-5] makes
            // any non-zero reserved bit malformed.
            if ((filter.options & MQ_SUBOPT_RESERVED_MASK) != 0) {
                return MqttError::PropertyValueInvalid;
            }
            if (filter.qos() == MQ_QOS_INVALID) return MqttError::PropertyValueInvalid;
            if (filter.retainHandling() == 3) return MqttError::PropertyValueInvalid;

            subscribe.count++;
        }
        // [V5:MQTT-3.8.3-2] A SUBSCRIBE packet MUST contain at least one
        // Topic Filter / Subscription Options pair.
        if (subscribe.count == 0) return MqttError::PropertyValueInvalid;
        break;
    }

    case MQ_PKT_SUBACK:
    case MQ_PKT_UNSUBACK: {
        MqttSubAck& suback = out.suback;
        suback.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (suback.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;

        const MqttError propertyError =
            mq_decodeProperties(reader, header.type, suback.properties);
        if (propertyError != MqttError::None) return propertyError;

        while (!reader.atEnd()) {
            if (suback.count >= MQTT_MAX_FILTERS_PER_PACKET) {
                return MqttError::TooManyProperties;
            }
            const uint8_t reason = reader.u8();
            if (!reader.ok()) return reader.error();
            const MqttError reasonError = checkReason(reason, header.type);
            if (reasonError != MqttError::None) return reasonError;
            suback.reasons[suback.count++] = reason;
        }
        if (suback.count == 0) return MqttError::PropertyValueInvalid;
        break;
    }

    case MQ_PKT_UNSUBSCRIBE: {
        MqttUnsubscribe& unsubscribe = out.unsubscribe;
        unsubscribe.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (unsubscribe.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;

        const MqttError propertyError =
            mq_decodeProperties(reader, MQ_PKT_UNSUBSCRIBE, unsubscribe.properties);
        if (propertyError != MqttError::None) return propertyError;

        while (!reader.atEnd()) {
            if (unsubscribe.count >= MQTT_MAX_FILTERS_PER_PACKET) {
                return MqttError::TooManyProperties;
            }
            unsubscribe.filters[unsubscribe.count] = reader.utf8();
            if (!reader.ok()) return reader.error();
            unsubscribe.count++;
        }
        if (unsubscribe.count == 0) return MqttError::PropertyValueInvalid;
        break;
    }

    case MQ_PKT_PINGREQ:
    case MQ_PKT_PINGRESP:
        // V5:3.12, 3.13 - no variable header and no payload. Anything at all
        // here means the two ends disagree about the packet.
        break;

    case MQ_PKT_DISCONNECT: {
        MqttDisconnect& disconnect = out.disconnect;

        // V5:3.14.2.1 - "If the Remaining Length is less than 1 the value of
        // 0x00 (Normal disconnection) is used." A clean close is two bytes on
        // the wire and nothing more.
        if (header.remainingLength < 1) {
            disconnect.reasonCode = MQ_REASON_NORMAL_DISCONNECTION;
            break;
        }
        disconnect.reasonCode = reader.u8();
        if (!reader.ok()) return reader.error();
        const MqttError reasonError = checkReason(disconnect.reasonCode, MQ_PKT_DISCONNECT);
        if (reasonError != MqttError::None) return reasonError;

        // V5:3.14.2.2.1 - "If the Remaining Length is less than 2, a value of
        // 0 is used."
        if (header.remainingLength < 2) break;

        const MqttError propertyError =
            mq_decodeProperties(reader, MQ_PKT_DISCONNECT, disconnect.properties);
        if (propertyError != MqttError::None) return propertyError;
        break;
    }

    case MQ_PKT_AUTH: {
        MqttAuth& auth = out.auth;

        // V5:3.15.2.1 - the Reason Code and Property Length may both be
        // omitted when the reason is Success and there are no properties, in
        // which case the Remaining Length is 0.
        if (header.remainingLength == 0) {
            auth.reasonCode = MQ_REASON_SUCCESS;
            break;
        }
        auth.reasonCode = reader.u8();
        if (!reader.ok()) return reader.error();
        const MqttError reasonError = checkReason(auth.reasonCode, MQ_PKT_AUTH);
        if (reasonError != MqttError::None) return reasonError;

        if (header.remainingLength < 2) break;

        const MqttError propertyError =
            mq_decodeProperties(reader, MQ_PKT_AUTH, auth.properties);
        if (propertyError != MqttError::None) return propertyError;
        break;
    }

    default:
        return MqttError::PropertyNotAllowed;
    }

    // Both standards define every packet's layout exactly, so a decode that
    // leaves bytes has misread something. Carrying on would mean acting on a
    // packet the two ends disagree about.
    reader.expectEnd();
    return reader.error();
}

// =============================================================================
// ENCODE
// =============================================================================
//
// Every packet is measured before it is written, and written only into a
// buffer already known to hold it. Nothing here grows a buffer, and nothing
// writes a length it has to go back and correct.

namespace {

// The Remaining Length of a packet: everything after the fixed header.
// Returns 0 for a packet this version cannot carry, which the caller turns
// into a refusal rather than a short packet.
size_t bodySizeV5(const MqttPacket& packet, bool& encodable) {
    encodable = true;
    const uint8_t type = packet.header.type;

    switch (type) {

    case MQ_PKT_CONNECT: {
        const MqttConnect& c = packet.connect;
        size_t size = mq_cstringSize(MQ_PROTOCOL_NAME)  // Protocol Name
                    + 1                                  // Protocol Version
                    + 1                                  // Connect Flags
                    + 2;                                 // Keep Alive
        size += mq_propertiesSize(c.properties, MQ_PKT_CONNECT);
        size += mq_utf8Size(c.clientId.length);
        if (c.hasWill) {
            size += mq_propertiesSize(c.will.properties, MQ_PROP_SCOPE_WILL);
            size += mq_utf8Size(c.will.topic.length);
            size += mq_binarySize(c.will.payload.length);
        }
        if (c.hasUserName) size += mq_utf8Size(c.userName.length);
        if (c.hasPassword) size += mq_binarySize(c.password.length);
        return size;
    }

    case MQ_PKT_CONNACK:
        return 1 + 1 + mq_propertiesSize(packet.connack.properties, MQ_PKT_CONNACK);

    case MQ_PKT_PUBLISH: {
        const MqttPublish& p = packet.publish;
        size_t size = mq_utf8Size(p.topic.length);
        if (p.qos > MQ_QOS_0) size += 2;
        size += mq_propertiesSize(p.properties, MQ_PKT_PUBLISH);
        size += p.payload.length;
        return size;
    }

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP: {
        const MqttPubAck& a = packet.ack;
        const size_t properties = mq_propertiesSize(a.properties, type);

        // V5:3.4.2.1 lets both the Reason Code and the Property Length be
        // omitted when the reason is Success and there are no properties. Two
        // bytes rather than four, on every acknowledgement of every QoS 1
        // publish - which on a busy link is worth having.
        if (a.reasonCode == MQ_REASON_SUCCESS && properties == 1) return 2;
        return 2 + 1 + properties;
    }

    case MQ_PKT_SUBSCRIBE: {
        const MqttSubscribe& s = packet.subscribe;
        size_t size = 2 + mq_propertiesSize(s.properties, MQ_PKT_SUBSCRIBE);
        for (uint8_t i = 0; i < s.count; i++) {
            size += mq_utf8Size(s.filters[i].filter.length) + 1;
        }
        return size;
    }

    case MQ_PKT_SUBACK:
    case MQ_PKT_UNSUBACK:
        return 2 + mq_propertiesSize(packet.suback.properties, type)
                 + packet.suback.count;

    case MQ_PKT_UNSUBSCRIBE: {
        const MqttUnsubscribe& u = packet.unsubscribe;
        size_t size = 2 + mq_propertiesSize(u.properties, MQ_PKT_UNSUBSCRIBE);
        for (uint8_t i = 0; i < u.count; i++) {
            size += mq_utf8Size(u.filters[i].length);
        }
        return size;
    }

    case MQ_PKT_PINGREQ:
    case MQ_PKT_PINGRESP:
        return 0;

    case MQ_PKT_DISCONNECT: {
        const MqttDisconnect& d = packet.disconnect;
        const size_t properties = mq_propertiesSize(d.properties, MQ_PKT_DISCONNECT);
        // V5:3.14.2.1 - a clean close with nothing to add is two bytes total.
        if (d.reasonCode == MQ_REASON_NORMAL_DISCONNECTION && properties == 1) return 0;
        return 1 + properties;
    }

    case MQ_PKT_AUTH: {
        const MqttAuth& a = packet.auth;
        const size_t properties = mq_propertiesSize(a.properties, MQ_PKT_AUTH);
        if (a.reasonCode == MQ_REASON_SUCCESS && properties == 1) return 0;
        return 1 + properties;
    }

    default:
        encodable = false;
        return 0;
    }
}

// The fixed header's flag nibble for a packet being encoded.
uint8_t flagsFor(const MqttPacket& packet) {
    if (packet.header.type != MQ_PKT_PUBLISH) {
        return mq_reservedFlags(packet.header.type);
    }
    const MqttPublish& p = packet.publish;
    uint8_t flags = (uint8_t)((p.qos << MQ_PUBLISH_QOS_SHIFT) & MQ_PUBLISH_QOS_MASK);
    if (p.dup) flags |= MQ_PUBLISH_DUP;
    if (p.retain) flags |= MQ_PUBLISH_RETAIN;
    return flags;
}

} // namespace

size_t MqttCodecV5::size(const MqttPacket& packet) const {
    if (!mq_packetTypeIsDefined(packet.header.type, MqttVersion::V5)) return 0;

    bool encodable = true;
    const size_t body = bodySizeV5(packet, encodable);
    if (!encodable) return 0;

    const size_t header = mq_fixedHeaderSize((uint32_t)body);
    if (header == 0) return 0;   // body above 268,435,455; nothing can carry it
    return header + body;
}

MqttError MqttCodecV5::encode(uint8_t* out, size_t capacity,
                              const MqttPacket& packet, size_t& written) const {
    written = 0;

    // MEASURED ONCE. This called size(), which calls bodySizeV5(), and then
    // called bodySizeV5() again - so every outbound packet walked the whole
    // 27-entry property table and its user-property list one more time than it
    // needed to. size() is also virtual, so none of it was ever folded away.
    if (!mq_packetTypeIsDefined(packet.header.type, MqttVersion::V5)) {
        return MqttError::PropertyNotAllowed;
    }

    bool encodable = true;
    const size_t body = bodySizeV5(packet, encodable);
    if (!encodable) return MqttError::PropertyNotAllowed;

    const size_t headerSize = mq_fixedHeaderSize((uint32_t)body);
    if (headerSize == 0) return MqttError::PropertyNotAllowed;
    const size_t total = headerSize + body;
    if (total > capacity) return MqttError::Overflow;

    MqttWriter writer(out, capacity);
    mq_encodeFixedHeader(writer, packet.header.type, flagsFor(packet), (uint32_t)body);

    const uint8_t type = packet.header.type;

    switch (type) {

    case MQ_PKT_CONNECT: {
        const MqttConnect& c = packet.connect;
        writer.utf8(MQ_PROTOCOL_NAME);
        writer.u8((uint8_t)MqttVersion::V5);

        uint8_t flags = 0;
        if (c.cleanStart) flags |= CONNECT_CLEAN_START;
        if (c.hasWill) {
            flags |= CONNECT_WILL_FLAG;
            flags |= (uint8_t)((c.will.qos << CONNECT_WILL_QOS_SHIFT) & CONNECT_WILL_QOS);
            if (c.will.retain) flags |= CONNECT_WILL_RETAIN;
        }
        if (c.hasUserName) flags |= CONNECT_USER_NAME;
        if (c.hasPassword) flags |= CONNECT_PASSWORD;
        writer.u8(flags);

        writer.u16(c.keepAlive);
        mq_encodeProperties(writer, c.properties, MQ_PKT_CONNECT);

        writer.utf8(c.clientId.data, c.clientId.length);
        if (c.hasWill) {
            mq_encodeProperties(writer, c.will.properties, MQ_PROP_SCOPE_WILL);
            writer.utf8(c.will.topic.data, c.will.topic.length);
            writer.binary(c.will.payload.data, c.will.payload.length);
        }
        if (c.hasUserName) writer.utf8(c.userName.data, c.userName.length);
        if (c.hasPassword) writer.binary(c.password.data, c.password.length);
        break;
    }

    case MQ_PKT_CONNACK: {
        const MqttConnAck& a = packet.connack;
        writer.u8(a.sessionPresent ? CONNACK_SESSION_PRESENT : 0u);
        writer.u8(a.reasonCode);
        mq_encodeProperties(writer, a.properties, MQ_PKT_CONNACK);
        break;
    }

    case MQ_PKT_PUBLISH: {
        const MqttPublish& p = packet.publish;
        writer.utf8(p.topic.data, p.topic.length);
        if (p.qos > MQ_QOS_0) writer.u16(p.packetId);
        mq_encodeProperties(writer, p.properties, MQ_PKT_PUBLISH);
        writer.raw(p.payload.data, p.payload.length);
        break;
    }

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP: {
        const MqttPubAck& a = packet.ack;
        writer.u16(a.packetId);
        if (body > 2) {
            writer.u8(a.reasonCode);
            mq_encodeProperties(writer, a.properties, type);
        }
        break;
    }

    case MQ_PKT_SUBSCRIBE: {
        const MqttSubscribe& s = packet.subscribe;
        writer.u16(s.packetId);
        mq_encodeProperties(writer, s.properties, MQ_PKT_SUBSCRIBE);
        for (uint8_t i = 0; i < s.count; i++) {
            writer.utf8(s.filters[i].filter.data, s.filters[i].filter.length);
            writer.u8(s.filters[i].options);
        }
        break;
    }

    case MQ_PKT_SUBACK:
    case MQ_PKT_UNSUBACK: {
        const MqttSubAck& s = packet.suback;
        writer.u16(s.packetId);
        mq_encodeProperties(writer, s.properties, type);
        for (uint8_t i = 0; i < s.count; i++) writer.u8(s.reasons[i]);
        break;
    }

    case MQ_PKT_UNSUBSCRIBE: {
        const MqttUnsubscribe& u = packet.unsubscribe;
        writer.u16(u.packetId);
        mq_encodeProperties(writer, u.properties, MQ_PKT_UNSUBSCRIBE);
        for (uint8_t i = 0; i < u.count; i++) {
            writer.utf8(u.filters[i].data, u.filters[i].length);
        }
        break;
    }

    case MQ_PKT_PINGREQ:
    case MQ_PKT_PINGRESP:
        break;

    case MQ_PKT_DISCONNECT: {
        if (body > 0) {
            writer.u8(packet.disconnect.reasonCode);
            mq_encodeProperties(writer, packet.disconnect.properties, MQ_PKT_DISCONNECT);
        }
        break;
    }

    case MQ_PKT_AUTH: {
        if (body > 0) {
            writer.u8(packet.auth.reasonCode);
            mq_encodeProperties(writer, packet.auth.properties, MQ_PKT_AUTH);
        }
        break;
    }

    default:
        return MqttError::PropertyNotAllowed;
    }

    if (!writer.ok()) return writer.error();

    // The size computed before writing and the bytes actually written must
    // agree. If they ever do not, every packet on the wire is either truncated
    // or padded, and the symptom is a broker that closes the connection with
    // no explanation - so it is checked here rather than assumed.
    if (writer.written() != total) return MqttError::Overflow;

    written = writer.written();
    return MqttError::None;
}

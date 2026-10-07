// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttCodecV311.h"

namespace {

// --- CONNECT flag bits, V311:3.1.2.3. Same layout as 5.0. ---
const uint8_t CONNECT_RESERVED     = 0x01;
const uint8_t CONNECT_CLEAN_SESSION = 0x02;
const uint8_t CONNECT_WILL_FLAG    = 0x04;
const uint8_t CONNECT_WILL_QOS     = 0x18;
const uint8_t CONNECT_WILL_RETAIN  = 0x20;
const uint8_t CONNECT_PASSWORD     = 0x40;
const uint8_t CONNECT_USER_NAME    = 0x80;
const uint8_t CONNECT_WILL_QOS_SHIFT = 3;

const uint8_t CONNACK_SESSION_PRESENT = 0x01;
const uint8_t CONNACK_RESERVED        = 0xFE;

// V311:3.8.3 - "The upper 6 bits of the Requested QoS byte are not used in the
// current version of the protocol. They are reserved for future use."
//
// 5.0 gave those bits meaning: No Local, Retain As Published and Retain
// Handling. A subscription carrying any of them cannot be expressed at 3.1.1,
// and sending it with the bits stripped would quietly subscribe on terms the
// caller did not ask for.
const uint8_t SUBSCRIBE_QOS_MASK     = 0x03;
const uint8_t SUBSCRIBE_RESERVED_MASK = 0xFC;

MqttError checkFlags(const MqttFixedHeader& header) {
    if (header.type == MQ_PKT_PUBLISH) return MqttError::None;
    const uint8_t required = mq_reservedFlags(header.type);
    if (required == 0xFFu) return MqttError::PropertyNotAllowed;
    return header.flags == required ? MqttError::None : MqttError::InvalidUtf8;
}

// Whether a property set holds anything at all.
//
// 3.1.1 has no property block, so a caller that set one is asking for
// something this version cannot carry. Refused rather than dropped: the whole
// point of having two results is that the caller finds out.
inline bool hasAnyProperty(const MqttProperties& properties) {
    return properties.present != 0;
}

} // namespace

// =============================================================================
// DECODE
// =============================================================================

MqttError MqttCodecV311::decode(const MqttFixedHeader& header,
                                const uint8_t* body, size_t length,
                                MqttPacket& out) const {
    out.clear();
    out.header = header;

    // AUTH does not exist at this version, so type 15 is reserved and a packet
    // carrying it is malformed rather than an unsupported feature.
    if (!mq_packetTypeIsDefined(header.type, MqttVersion::V311)) {
        return MqttError::PropertyNotAllowed;
    }

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
        if (!mq_spanEquals(name, MQ_PROTOCOL_NAME)) return MqttError::InvalidUtf8;

        const uint8_t level = reader.u8();
        if (!reader.ok()) return reader.error();
        if (level != (uint8_t)MqttVersion::V311) return MqttError::PropertyValueInvalid;
        connect.version = MqttVersion::V311;

        const uint8_t flags = reader.u8();
        if (!reader.ok()) return reader.error();

        // [V311:MQTT-3.1.2-3] The Server MUST validate that the reserved flag
        // is set to zero and disconnect the Client if it is not zero.
        if ((flags & CONNECT_RESERVED) != 0) return MqttError::PropertyValueInvalid;

        connect.cleanStart = (flags & CONNECT_CLEAN_SESSION) != 0;
        connect.hasWill = (flags & CONNECT_WILL_FLAG) != 0;
        connect.will.qos = (uint8_t)((flags & CONNECT_WILL_QOS) >> CONNECT_WILL_QOS_SHIFT);
        connect.will.retain = (flags & CONNECT_WILL_RETAIN) != 0;
        connect.hasUserName = (flags & CONNECT_USER_NAME) != 0;
        connect.hasPassword = (flags & CONNECT_PASSWORD) != 0;

        // [V311:MQTT-3.1.2-11] and [V311:MQTT-3.1.2-13]
        if (!connect.hasWill) {
            if (connect.will.qos != MQ_QOS_0 || connect.will.retain) {
                return MqttError::PropertyValueInvalid;
            }
        } else if (connect.will.qos == MQ_QOS_INVALID) {
            // [V311:MQTT-3.1.2-14] Will QoS MUST NOT be 3.
            return MqttError::PropertyValueInvalid;
        }

        connect.keepAlive = reader.u16();
        if (!reader.ok()) return reader.error();

        // No property block. The payload begins immediately.
        connect.clientId = reader.utf8();
        if (!reader.ok()) return reader.error();

        if (connect.hasWill) {
            // And no Will properties either: a topic and a payload, nothing
            // more. There is no Will Delay Interval at this version, which is
            // why a Sparkplug death certificate is delivered the moment the
            // connection drops rather than after a grace period.
            connect.will.topic = reader.utf8();
            connect.will.payload = reader.binary();
            if (!reader.ok()) return reader.error();
        }
        if (connect.hasUserName) {
            connect.userName = reader.utf8();
            if (!reader.ok()) return reader.error();
        }
        if (connect.hasPassword) {
            connect.password = reader.binary();
            if (!reader.ok()) return reader.error();
        }
        break;
    }

    case MQ_PKT_CONNACK: {
        MqttConnAck& connack = out.connack;

        // THE FIELD BELOW IS A CONNECT RETURN CODE, NOT A REASON CODE. The
        // version is recorded so whoever reads it knows which table to look
        // in: 0x01 is "unacceptable protocol version" here and "Granted QoS 1"
        // at 5.0.
        connack.version = MqttVersion::V311;

        const uint8_t flags = reader.u8();
        if (!reader.ok()) return reader.error();

        // V311:3.2.2.1 - "Bits 7-1 are reserved and MUST be set to 0."
        if ((flags & CONNACK_RESERVED) != 0) return MqttError::PropertyValueInvalid;
        connack.sessionPresent = (flags & CONNACK_SESSION_PRESENT) != 0;

        connack.reasonCode = reader.u8();
        if (!reader.ok()) return reader.error();

        // V311 Table 3.1 - values 6 to 255 are Reserved for future use, so a
        // CONNACK carrying one is malformed rather than an unknown failure.
        if (connack.reasonCode > MQ_RETURN_MAX) return MqttError::PropertyValueInvalid;
        break;
    }

    case MQ_PKT_PUBLISH: {
        MqttPublish& publish = out.publish;
        publish.dup = (header.flags & MQ_PUBLISH_DUP) != 0;
        publish.retain = (header.flags & MQ_PUBLISH_RETAIN) != 0;
        publish.qos = (uint8_t)((header.flags & MQ_PUBLISH_QOS_MASK) >> MQ_PUBLISH_QOS_SHIFT);

        // [V311:MQTT-3.3.1-4] and [V311:MQTT-3.3.1-2]
        if (publish.qos == MQ_QOS_INVALID) return MqttError::PropertyValueInvalid;
        if (publish.qos == MQ_QOS_0 && publish.dup) return MqttError::PropertyValueInvalid;

        publish.topic = reader.utf8();
        if (!reader.ok()) return reader.error();

        if (publish.qos > MQ_QOS_0) {
            publish.packetId = reader.u16();
            if (!reader.ok()) return reader.error();
            if (publish.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;
        }

        publish.payload = reader.rest();
        if (!reader.ok()) return reader.error();
        return MqttError::None;
    }

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP: {
        // A Packet Identifier and nothing else. There is no reason code at
        // this version, so a QoS 1 publish that the peer refused is
        // indistinguishable from one it accepted - which is exactly what 5.0's
        // reason codes were added to fix.
        MqttPubAck& ack = out.ack;
        ack.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (ack.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;
        ack.reasonCode = MQ_REASON_SUCCESS;
        break;
    }

    case MQ_PKT_SUBSCRIBE: {
        MqttSubscribe& subscribe = out.subscribe;
        subscribe.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (subscribe.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;

        while (!reader.atEnd()) {
            if (subscribe.count >= MQTT_MAX_FILTERS_PER_PACKET) {
                return MqttError::TooManyProperties;
            }
            MqttSubscription& filter = subscribe.filters[subscribe.count];
            filter.filter = reader.utf8();
            const uint8_t requested = reader.u8();
            if (!reader.ok()) return reader.error();

            // [V311:MQTT-3-8.3-4] The Server MUST treat a SUBSCRIBE packet as
            // malformed if any of the reserved bits are non-zero.
            if ((requested & SUBSCRIBE_RESERVED_MASK) != 0) {
                return MqttError::PropertyValueInvalid;
            }
            if ((requested & SUBSCRIBE_QOS_MASK) == MQ_QOS_INVALID) {
                return MqttError::PropertyValueInvalid;
            }
            filter.options = requested;
            subscribe.count++;
        }
        // [V311:MQTT-3.8.3-3] at least one Topic Filter / QoS pair.
        if (subscribe.count == 0) return MqttError::PropertyValueInvalid;
        break;
    }

    case MQ_PKT_SUBACK: {
        MqttSubAck& suback = out.suback;
        suback.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (suback.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;

        while (!reader.atEnd()) {
            if (suback.count >= MQTT_MAX_FILTERS_PER_PACKET) {
                return MqttError::TooManyProperties;
            }
            const uint8_t code = reader.u8();
            if (!reader.ok()) return reader.error();

            // [V311:MQTT-3.9.3-2] "SUBACK return codes other than 0x00, 0x01,
            // 0x02 and 0x80 are reserved and MUST NOT be used." A DIFFERENT
            // value space from 5.0's reason codes, which is why the two codecs
            // cannot share this check.
            if (!mq_subackCodeIsValidV311(code)) return MqttError::PropertyValueInvalid;
            suback.reasons[suback.count++] = code;
        }
        if (suback.count == 0) return MqttError::PropertyValueInvalid;
        break;
    }

    case MQ_PKT_UNSUBSCRIBE: {
        MqttUnsubscribe& unsubscribe = out.unsubscribe;
        unsubscribe.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (unsubscribe.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;

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

    case MQ_PKT_UNSUBACK: {
        // V311:3.11.1 - "For the UNSUBACK Packet this has the value 2." A
        // Packet Identifier and nothing else: no per-filter result at all, so
        // an unsubscribe from a filter that was never subscribed looks exactly
        // like one that was.
        MqttSubAck& suback = out.suback;
        suback.packetId = reader.u16();
        if (!reader.ok()) return reader.error();
        if (suback.packetId == MQ_PACKET_ID_NONE) return MqttError::PropertyValueInvalid;
        suback.count = 0;
        break;
    }

    case MQ_PKT_PINGREQ:
    case MQ_PKT_PINGRESP:
        break;

    case MQ_PKT_DISCONNECT:
        // V311:3.14.2, 3.14.3 - no variable header and no payload. Two bytes
        // on the wire and nothing else; there is no way to say why.
        out.disconnect.reasonCode = MQ_REASON_NORMAL_DISCONNECTION;
        break;

    default:
        return MqttError::PropertyNotAllowed;
    }

    reader.expectEnd();
    return reader.error();
}

// =============================================================================
// ENCODE
// =============================================================================

namespace {

// Whether this packet can be said at all in 3.1.1.
//
// Checked before anything is measured, so a caller asking for a topic alias or
// a message expiry interval is told rather than sent a packet with the request
// quietly removed.
MqttError encodableAtV311(const MqttPacket& packet) {
    const uint8_t type = packet.header.type;

    if (!mq_packetTypeIsDefined(type, MqttVersion::V311)) {
        // AUTH, and the whole enhanced authentication exchange.
        return MqttError::NotSupportedByProtocol;
    }

    switch (type) {
    case MQ_PKT_CONNECT:
        if (hasAnyProperty(packet.connect.properties) ||
            hasAnyProperty(packet.connect.will.properties)) {
            return MqttError::NotSupportedByProtocol;
        }
        break;
    case MQ_PKT_CONNACK:
        if (hasAnyProperty(packet.connack.properties)) return MqttError::NotSupportedByProtocol;
        break;
    case MQ_PKT_PUBLISH:
        // Topic Alias, Message Expiry Interval, Content Type, Response Topic,
        // Correlation Data, Payload Format Indicator, Subscription Identifier
        // and User Property all live here at 5.0 and nowhere at 3.1.1.
        if (hasAnyProperty(packet.publish.properties)) return MqttError::NotSupportedByProtocol;
        break;
    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP:
        if (hasAnyProperty(packet.ack.properties)) return MqttError::NotSupportedByProtocol;
        // There is no reason code, so a caller trying to refuse a publish
        // cannot say so. Sending the acknowledgement anyway would report
        // success for something that failed.
        if (packet.ack.reasonCode != MQ_REASON_SUCCESS) {
            return MqttError::NotSupportedByProtocol;
        }
        break;
    case MQ_PKT_SUBSCRIBE:
        if (hasAnyProperty(packet.subscribe.properties)) return MqttError::NotSupportedByProtocol;
        for (uint8_t i = 0; i < packet.subscribe.count; i++) {
            // No Local, Retain As Published and Retain Handling are 5.0 only.
            // Stripping them would subscribe on terms nobody asked for.
            if ((packet.subscribe.filters[i].options & SUBSCRIBE_RESERVED_MASK) != 0) {
                return MqttError::NotSupportedByProtocol;
            }
        }
        break;
    case MQ_PKT_SUBACK:
        if (hasAnyProperty(packet.suback.properties)) return MqttError::NotSupportedByProtocol;
        for (uint8_t i = 0; i < packet.suback.count; i++) {
            if (!mq_subackCodeIsValidV311(packet.suback.reasons[i])) {
                return MqttError::NotSupportedByProtocol;
            }
        }
        break;
    case MQ_PKT_UNSUBACK:
        if (hasAnyProperty(packet.suback.properties)) return MqttError::NotSupportedByProtocol;
        break;
    case MQ_PKT_UNSUBSCRIBE:
        if (hasAnyProperty(packet.unsubscribe.properties)) return MqttError::NotSupportedByProtocol;
        break;
    case MQ_PKT_DISCONNECT:
        if (hasAnyProperty(packet.disconnect.properties) ||
            packet.disconnect.reasonCode != MQ_REASON_NORMAL_DISCONNECTION) {
            // No way to say why, and no Session Expiry Interval to revise on
            // the way out.
            return MqttError::NotSupportedByProtocol;
        }
        break;
    default:
        break;
    }
    return MqttError::None;
}

size_t bodySizeV311(const MqttPacket& packet) {
    switch (packet.header.type) {

    case MQ_PKT_CONNECT: {
        const MqttConnect& c = packet.connect;
        size_t size = mq_cstringSize(MQ_PROTOCOL_NAME) + 1 + 1 + 2;
        size += mq_utf8Size(c.clientId.length);
        if (c.hasWill) {
            size += mq_utf8Size(c.will.topic.length);
            size += mq_binarySize(c.will.payload.length);
        }
        if (c.hasUserName) size += mq_utf8Size(c.userName.length);
        if (c.hasPassword) size += mq_binarySize(c.password.length);
        return size;
    }

    case MQ_PKT_CONNACK:
        return 2;

    case MQ_PKT_PUBLISH: {
        const MqttPublish& p = packet.publish;
        size_t size = mq_utf8Size(p.topic.length);
        if (p.qos > MQ_QOS_0) size += 2;
        return size + p.payload.length;
    }

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP:
    case MQ_PKT_UNSUBACK:
        return 2;

    case MQ_PKT_SUBSCRIBE: {
        const MqttSubscribe& s = packet.subscribe;
        size_t size = 2;
        for (uint8_t i = 0; i < s.count; i++) {
            size += mq_utf8Size(s.filters[i].filter.length) + 1;
        }
        return size;
    }

    case MQ_PKT_SUBACK:
        return (size_t)2 + packet.suback.count;

    case MQ_PKT_UNSUBSCRIBE: {
        const MqttUnsubscribe& u = packet.unsubscribe;
        size_t size = 2;
        for (uint8_t i = 0; i < u.count; i++) size += mq_utf8Size(u.filters[i].length);
        return size;
    }

    case MQ_PKT_PINGREQ:
    case MQ_PKT_PINGRESP:
    case MQ_PKT_DISCONNECT:
        return 0;

    default:
        return 0;
    }
}

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

size_t MqttCodecV311::size(const MqttPacket& packet) const {
    if (encodableAtV311(packet) != MqttError::None) return 0;
    const size_t body = bodySizeV311(packet);
    const size_t header = mq_fixedHeaderSize((uint32_t)body);
    return header == 0 ? 0 : header + body;
}

MqttError MqttCodecV311::encode(uint8_t* out, size_t capacity,
                                const MqttPacket& packet, size_t& written) const {
    written = 0;

    // Refused before anything is measured, and the reason is specific: the
    // caller asked for something 3.1.1 has no way to say.
    const MqttError supported = encodableAtV311(packet);
    if (supported != MqttError::None) return supported;

    const size_t body = bodySizeV311(packet);
    const size_t total = mq_fixedHeaderSize((uint32_t)body) + body;
    if (total > capacity) return MqttError::Overflow;

    MqttWriter writer(out, capacity);
    mq_encodeFixedHeader(writer, packet.header.type, flagsFor(packet), (uint32_t)body);

    switch (packet.header.type) {

    case MQ_PKT_CONNECT: {
        const MqttConnect& c = packet.connect;
        writer.utf8(MQ_PROTOCOL_NAME);
        writer.u8((uint8_t)MqttVersion::V311);

        uint8_t flags = 0;
        if (c.cleanStart) flags |= CONNECT_CLEAN_SESSION;
        if (c.hasWill) {
            flags |= CONNECT_WILL_FLAG;
            flags |= (uint8_t)((c.will.qos << CONNECT_WILL_QOS_SHIFT) & CONNECT_WILL_QOS);
            if (c.will.retain) flags |= CONNECT_WILL_RETAIN;
        }
        if (c.hasUserName) flags |= CONNECT_USER_NAME;
        if (c.hasPassword) flags |= CONNECT_PASSWORD;
        writer.u8(flags);
        writer.u16(c.keepAlive);

        // [V311:MQTT-3.1.3-1] The fields, if present, MUST appear in the order
        // Client Identifier, Will Topic, Will Message, User Name, Password.
        // No Will Properties: that field arrived with 5.0.
        writer.utf8(c.clientId.data, c.clientId.length);
        if (c.hasWill) {
            writer.utf8(c.will.topic.data, c.will.topic.length);
            writer.binary(c.will.payload.data, c.will.payload.length);
        }
        if (c.hasUserName) writer.utf8(c.userName.data, c.userName.length);
        if (c.hasPassword) writer.binary(c.password.data, c.password.length);
        break;
    }

    case MQ_PKT_CONNACK:
        writer.u8(packet.connack.sessionPresent ? CONNACK_SESSION_PRESENT : 0u);
        writer.u8(packet.connack.reasonCode);
        break;

    case MQ_PKT_PUBLISH: {
        const MqttPublish& p = packet.publish;
        writer.utf8(p.topic.data, p.topic.length);
        if (p.qos > MQ_QOS_0) writer.u16(p.packetId);
        writer.raw(p.payload.data, p.payload.length);
        break;
    }

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP:
        writer.u16(packet.ack.packetId);
        break;

    case MQ_PKT_UNSUBACK:
        writer.u16(packet.suback.packetId);
        break;

    case MQ_PKT_SUBSCRIBE: {
        const MqttSubscribe& s = packet.subscribe;
        writer.u16(s.packetId);
        for (uint8_t i = 0; i < s.count; i++) {
            writer.utf8(s.filters[i].filter.data, s.filters[i].filter.length);
            writer.u8((uint8_t)(s.filters[i].options & SUBSCRIBE_QOS_MASK));
        }
        break;
    }

    case MQ_PKT_SUBACK: {
        const MqttSubAck& s = packet.suback;
        writer.u16(s.packetId);
        for (uint8_t i = 0; i < s.count; i++) writer.u8(s.reasons[i]);
        break;
    }

    case MQ_PKT_UNSUBSCRIBE: {
        const MqttUnsubscribe& u = packet.unsubscribe;
        writer.u16(u.packetId);
        for (uint8_t i = 0; i < u.count; i++) {
            writer.utf8(u.filters[i].data, u.filters[i].length);
        }
        break;
    }

    case MQ_PKT_PINGREQ:
    case MQ_PKT_PINGRESP:
    case MQ_PKT_DISCONNECT:
        break;

    default:
        return MqttError::PropertyNotAllowed;
    }

    if (!writer.ok()) return writer.error();
    if (writer.written() != total) return MqttError::Overflow;

    written = writer.written();
    return MqttError::None;
}

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttPacket.h"
#include "MqttNoExcept.h"

MqttError mq_decodeFixedHeader(const uint8_t* data, size_t available,
                               MqttFixedHeader& out) {
    out = MqttFixedHeader();
    if (!data || available < 1) return MqttError::Truncated;

    out.type = (uint8_t)(data[0] >> 4);
    out.flags = (uint8_t)(data[0] & MQ_FLAGS_MASK);

    uint32_t remaining = 0;
    uint8_t consumed = 0;
    const MqttError error = mq_decodeVbi(data + 1, available - 1, remaining, consumed);
    if (error != MqttError::None) return error;

    out.remainingLength = remaining;
    out.headerLength = (uint8_t)(1 + consumed);
    return MqttError::None;
}

size_t mq_fixedHeaderSize(uint32_t remainingLength) {
    const uint8_t vbi = mq_vbiSize(remainingLength);
    return vbi == 0 ? 0 : (size_t)1 + vbi;
}

void mq_encodeFixedHeader(MqttWriter& writer, uint8_t type, uint8_t flags,
                          uint32_t remainingLength) {
    writer.u8((uint8_t)((type << 4) | (flags & MQ_FLAGS_MASK)));
    writer.vbi(remainingLength);
}

bool mq_peekProtocolLevel(const uint8_t* body, size_t length, uint8_t& level) {
    level = 0;
    if (body == nullptr || length < 3) return false;

    // V5:3.1.2.1 / V311:3.1.2.1 - the variable header opens with the Protocol
    // Name as a UTF-8 Encoded String, so a two-byte length then that many
    // bytes, and the Protocol Level is the byte immediately after it.
    //
    // The name is NOT checked against "MQTT" here. 3.1 used "MQIsdp" and some
    // deployed equipment still does; the caller wants to answer that with
    // "unacceptable protocol level" rather than with silence, and it can only
    // do that if it gets the level out.
    const uint32_t nameLength = (uint32_t)((body[0] << 8) | body[1]);
    if ((size_t)nameLength + 3 > length) return false;

    level = body[2 + nameLength];
    return true;
}

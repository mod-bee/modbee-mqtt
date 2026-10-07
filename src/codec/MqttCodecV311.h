// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// MQTT 3.1.1, Protocol Level 4.
//
// What is NOT here is the point of the file. 3.1.1 has:
//
//   no properties anywhere              so no property block on any packet
//   no reason codes                     CONNACK carries a Connect Return Code
//                                       from an entirely different value space
//   no AUTH packet                      type 15 is reserved
//   no DISCONNECT body                  a clean close is two bytes and nothing
//   no UNSUBACK reason codes            UNSUBACK is a Packet Identifier alone
//   only four SUBACK return codes       V311:MQTT-3.9.3-2 reserves the rest
//   no Will properties                  the Will is a topic and a payload
//
// Anything the caller asks for that 3.1.1 cannot express is REFUSED while
// encoding rather than dropped. See MqttError::NotSupportedByProtocol.

#include "../MqttCodec.h"

class MqttCodecV311 : public MqttCodec {
public:
    MqttVersion version() const override { return MqttVersion::V311; }

    MqttError decode(const MqttFixedHeader& header,
                     const uint8_t* body, size_t length,
                     MqttPacket& out) const override;

    size_t size(const MqttPacket& packet) const override;

    MqttError encode(uint8_t* out, size_t capacity,
                     const MqttPacket& packet, size_t& written) const override;
};

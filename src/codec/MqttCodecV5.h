// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// MQTT 5.0. The full wire format: properties everywhere the standard allows
// them, reason codes from Table 2-6, and the AUTH packet.
//
// Everything here cites V5. Where 3.1.1 differs the difference is in
// MqttCodecV311, never in a branch below.

#include "../MqttCodec.h"

class MqttCodecV5 : public MqttCodec {
public:
    MqttVersion version() const override { return MqttVersion::V5; }

    MqttError decode(const MqttFixedHeader& header,
                     const uint8_t* body, size_t length,
                     MqttPacket& out) const override;

    size_t size(const MqttPacket& packet) const override;

    MqttError encode(uint8_t* out, size_t capacity,
                     const MqttPacket& packet, size_t& written) const override;
};

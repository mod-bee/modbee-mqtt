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
// PROTOCOL VERSION
// =============================================================================
//
// 3.1.1 and 5.0 are not one protocol with a version byte. CONNACK carries a
// one-byte Connect Return Code in 3.1.1 and a Reason Code from a 44-value
// table in 5.0 - different value spaces entirely. SUBACK in 3.1.1 permits only
// four values and reserves the rest. DISCONNECT in 3.1.1 has no variable
// header. There are no properties anywhere in 3.1.1, and no AUTH packet.
//
// So the version is carried explicitly, one codec per wire format, and this
// enum is what selects between them.
//
// The value IS the Protocol Level byte that travels in CONNECT, so no
// translation table is needed and no translation table can be wrong.
//   V311:3.1.2.2 - "the value of the Protocol Level field for the version
//                   3.1.1 of the protocol is 4 (0x04)"
//   V5:3.1.2.2   - "The value of the Protocol Version field for version 5.0
//                   of the protocol is 5 (0x05)"

#include <stdint.h>

enum class MqttVersion : uint8_t {
    V311 = 4,
    V5   = 5,
};

// The Protocol Name that accompanies the level. Both versions use "MQTT";
// only the pre-OASIS 3.1 used "MQIsdp", and that version is out of scope.
//   V311:3.1.2.1, V5:3.1.2.1
#define MQ_PROTOCOL_NAME     "MQTT"
#define MQ_PROTOCOL_NAME_LEN 4

inline uint8_t mq_protocolLevel(MqttVersion version) {
    return (uint8_t)version;
}

inline const char* mq_versionName(MqttVersion version) {
    switch (version) {
        case MqttVersion::V311: return "3.1.1";
        case MqttVersion::V5:   return "5.0";
    }
    return "?";
}

// Whether a level byte read off the wire is one this library speaks. Stated as
// a function rather than a cast, because a cast of an arbitrary byte into the
// enum is undefined behaviour and this is exactly where an arbitrary byte

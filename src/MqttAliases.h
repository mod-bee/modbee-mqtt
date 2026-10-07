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
// TOPIC ALIASES  -  5.0 only
// =============================================================================
//
// V5:3.3.2.3.4. An integer standing in for a topic name, so a long name is
// sent once and referred to by two bytes thereafter. On a metered radio link
// carrying "plant/line3/extruder/zone2/temperature" ten times a second, that
// is most of the traffic.
//
// TWO INDEPENDENT TABLES, because the two directions negotiate separately and
// neither end may assume the other's limit:
//
//   OUTBOUND  bounded by the Topic Alias Maximum the PEER sent in its
//             acknowledgement. Zero - the common default - means the peer
//             accepts no aliases at all, and that is the case a branch tends
//             to forget.
//
//   INBOUND   bounded by the Topic Alias Maximum WE sent. We cannot receive an
//             alias we did not offer to accept, and one that arrives anyway is
//             reason code 0x94 and a DISCONNECT.
//
// NEITHER IS SESSION STATE. V5:3.3.2.3.4 - "Topic Alias mappings exist only
// within a Network Connection and last only for the lifetime of that Network
// Connection." Carrying one across a reconnect means publishing to whatever
// topic the old alias used to mean, which is the worst kind of bug: it works,
// and it puts the data somewhere else.

#include "MqttTypes.h"

#include <string>
#include <vector>

#ifndef MQTT_MAX_ALIASES
#define MQTT_MAX_ALIASES 8
#endif

class MqttAliases {
public:
    // Called on EVERY new connection, never on a resumed session.
    void reset(uint16_t outboundMaximum, uint16_t inboundMaximum);

    // --- Outbound ---

    // The alias already agreed for this topic, or 0 if there is none.
    uint16_t lookupOutbound(const uint8_t* topic, uint16_t length) const;

    // Takes the next free alias for this topic, or 0 when the table is full or
    // the peer offered none. The caller then sends the topic AND the alias
    // once; later publishes may send the alias with an empty topic.
    // `searchFirst` false when the caller has already looked and missed.
    uint16_t assignOutbound(const uint8_t* topic, uint16_t length,
                            bool searchFirst = true);

    // --- Inbound ---

    // Records what an alias means. False when the alias is outside what we
    // offered, which is a Protocol Error the caller turns into 0x94.
    bool rememberInbound(uint16_t alias, const uint8_t* topic, uint16_t length);

    // What an alias means, or an empty span if it means nothing yet.
    //
    // V5:3.3.2.3.4 - a PUBLISH with an alias and a zero-length topic before
    // any mapping exists is a Protocol Error, and this is how the caller
    // finds out.
    bool lookupInbound(uint16_t alias, const uint8_t*& topic, uint16_t& length) const;

    bool inboundAllowed(uint16_t alias) const {
        return alias != 0 && alias <= _inboundMaximum;
    }

    uint16_t outboundMaximum() const { return _outboundMaximum; }
    uint16_t inboundMaximum() const { return _inboundMaximum; }

private:
    struct Entry {
        std::vector<uint8_t> topic;
        uint16_t alias = 0;
        bool inUse() const { return alias != 0; }
    };

    Entry _outbound[MQTT_MAX_ALIASES];
    Entry _inbound[MQTT_MAX_ALIASES];

    uint16_t _outboundMaximum = 0;
    uint16_t _inboundMaximum = 0;
    uint16_t _nextOutbound = 1;
};

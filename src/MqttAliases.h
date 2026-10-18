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
#include "MqttFixed.h"

// A board's alias topics are fixed (MqttFixed.h), as long as a PLC block's
// topic pin (STRING(128)).
#if MQTT_LEAN && !defined(MQTT_ALIAS_TOPIC)
#define MQTT_ALIAS_TOPIC 128
#endif

#ifndef MQTT_MAX_ALIASES
#define MQTT_MAX_ALIASES 8
#endif

class MqttAliases {
public:
    // Called on EVERY new connection, never on a resumed session.
    void reset(uint16_t outboundMaximum, uint16_t inboundMaximum);

    // THE TOPIC ALIAS MAXIMUM A CONNECT MAY ADVERTISE for a configuration
    // that asks for `wanted`: no more than this build's table holds.
    //
    // reset() has always capped the table at MQTT_MAX_ALIASES, but the CONNECT
    // went on advertising whatever the configuration said. A client that
    // offered 16 and kept 8 told the broker aliases 9 to 16 were fine, and
    // then closed the connection with 0x94 the first time the broker used
    // one - a protocol error of its own making, repeated on every reconnect.
    // What is advertised and what reset() is given are now the same number.
    static uint16_t offerable(uint16_t wanted) {
        return wanted > MQTT_MAX_ALIASES ? (uint16_t)MQTT_MAX_ALIASES : wanted;
    }

    // Whether a topic of this length can be held in an entry. Always on a
    // host; on a board, up to MQTT_ALIAS_TOPIC bytes. A topic that does not
    // fit is never stored cut short - see rememberInbound().
    static bool fits(uint16_t length) {
#if MQTT_LEAN
        return length <= MQTT_ALIAS_TOPIC;
#else
        (void)length;
        return true;
#endif
    }

    // --- Outbound ---

    // The alias already agreed for this topic, or 0 if there is none.
    uint16_t lookupOutbound(const uint8_t* topic, uint16_t length) const;

    // Takes the next free alias for this topic, or 0 when the table is full,
    // the peer offered none, or the topic is longer than an entry holds (the
    // topic then simply goes in full). The caller then sends the topic AND the alias
    // once; later publishes may send the alias with an empty topic.
    // `searchFirst` false when the caller has already looked and missed.
    uint16_t assignOutbound(const uint8_t* topic, uint16_t length,
                            bool searchFirst = true);

    // --- Inbound ---

    // Records what an alias means. False when the alias is outside what we
    // offered, which is a Protocol Error the caller turns into 0x94.
    //
    // Also false when the topic is longer than an entry holds (a board's
    // MQTT_ALIAS_TOPIC), and then the alias is left meaning NOTHING - not a
    // shortened topic, and not whatever it meant before. The PUBLISH carrying
    // the full topic is still delivered under it; a later PUBLISH that sends
    // the alias alone finds no mapping, which the caller turns into 0x94. A
    // message is never delivered under a topic other than the one it was
    // sent to.
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
        MQ_BYTES(MQTT_ALIAS_TOPIC) topic;
        uint16_t alias = 0;
        bool inUse() const { return alias != 0; }
    };

    Entry _outbound[MQTT_MAX_ALIASES];
    Entry _inbound[MQTT_MAX_ALIASES];

    uint16_t _outboundMaximum = 0;
    uint16_t _inboundMaximum = 0;
    uint16_t _nextOutbound = 1;
};

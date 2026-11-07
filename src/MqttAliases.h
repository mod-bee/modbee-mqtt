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

// HOW MANY ALIASES EACH DIRECTION HOLDS. A board's tables are fixed, so 8; a
// host's grow as aliases are used, up to MQTT_MAX_ALIASES (MqttFeatures.h).
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
    //
    // CHOSEN, THEN CONFIRMED. chooseOutbound() says what the next PUBLISH to a
    // topic should carry and changes nothing; confirmOutbound() records it once
    // that PUBLISH is in the transmit queue. A packet that could not be queued
    // - the buffer busy for a moment, the packet too large - must not leave a
    // mapping behind: the retry would find it, send the alias alone, and the
    // peer, which never saw the topic go with it, would close the connection
    // with 0x94.

    struct Choice {
        uint16_t alias = 0;      // 0: the topic goes in full, with no alias
        bool aliasOnly = false;  // true: the alias alone, a zero-length topic
    };

    // What a PUBLISH to this topic carries. The alias alone when the peer has
    // already been sent this topic with it on this connection; otherwise the
    // topic AND an alias - the next unused one, or, when every alias the peer
    // allows is taken and `replace` is set, the one least recently sent
    // (V5:3.3.2.3.4 lets a sender give an alias a new topic at any time, and
    // the publish that does so carries the topic). No alias at all when the
    // peer offered none, the topic is longer than an entry holds, or the table
    // is full and `replace` is not set.
    Choice chooseOutbound(const uint8_t* topic, uint16_t length, bool replace) const;

    // The PUBLISH that carried `choice` is queued for the wire.
    void confirmOutbound(const Choice& choice, const uint8_t* topic, uint16_t length);

    // The alias already agreed for this topic, or 0 if there is none.
    uint16_t lookupOutbound(const uint8_t* topic, uint16_t length) const;

    // chooseOutbound() and confirmOutbound() in one, without replacement: the
    // alias for this topic, a new one taken for it, or 0.
    uint16_t assignOutbound(const uint8_t* topic, uint16_t length);

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
    // ENTRY n IS ALIAS n + 1, in both tables. Outbound aliases are handed out
    // in order and a replaced one keeps its number, so the entry for an alias
    // is found by index - and an inbound alias, which the peer numbers, is
    // within what we offered, so the same holds there. An empty topic is an
    // unused entry: no mapping is ever made to an empty topic.
    struct Entry {
        MQ_BYTES(MQTT_ALIAS_TOPIC) topic;
        uint32_t lastSent = 0;   // outbound only: what replacement goes by
    };

#if MQTT_LEAN
    Entry _outbound[MQTT_MAX_ALIASES];
    Entry _inbound[MQTT_MAX_ALIASES];
    uint16_t _outboundUsed = 0;
    size_t inboundSize() const { return MQTT_MAX_ALIASES; }
#else
    // Grown as aliases are used, so a connection that never uses one - most
    // of a broker's - holds two empty vectors, whatever the maximum.
    std::vector<Entry> _outbound;
    std::vector<Entry> _inbound;
    size_t inboundSize() const { return _inbound.size(); }
#endif

    uint16_t outboundUsed() const {
#if MQTT_LEAN
        return _outboundUsed;
#else
        return (uint16_t)_outbound.size();
#endif
    }

    uint16_t _outboundMaximum = 0;
    uint16_t _inboundMaximum = 0;

    // A count of confirmed sends, which orders the entries for replacement.
    // Wrapping after four billion publishes misorders them once, for a
    // moment: it decides which alias is replaced, never what one means.
    uint32_t _sends = 0;
};

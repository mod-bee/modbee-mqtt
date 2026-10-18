// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttAliases.h"

#include <string.h>
#include "MqttNoExcept.h"

namespace {

bool sameTopic(const MQ_BYTES_REF& held, const uint8_t* topic, uint16_t length) {
    return held.size() == length && memcmp(held.data(), topic, length) == 0;
}

} // namespace

void MqttAliases::reset(uint16_t outboundMaximum, uint16_t inboundMaximum) {
    for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
        _outbound[i] = Entry();
        _inbound[i] = Entry();
    }
    // Never more than this build holds, whatever the peer offered. A peer
    // offering 65535 aliases is not an instruction to allocate 65535 entries.
    //
    // The inbound cap is offerable(), the same function the client's CONNECT
    // uses, so what it advertised and what it accepts cannot drift apart.
    _outboundMaximum = outboundMaximum > MQTT_MAX_ALIASES
                           ? (uint16_t)MQTT_MAX_ALIASES : outboundMaximum;
    _inboundMaximum = offerable(inboundMaximum);
    _nextOutbound = 1;
}

uint16_t MqttAliases::lookupOutbound(const uint8_t* topic, uint16_t length) const {
    if (!topic || length == 0) return 0;
    for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
        if (_outbound[i].inUse() && sameTopic(_outbound[i].topic, topic, length)) {
            return _outbound[i].alias;
        }
    }
    return 0;
}

uint16_t MqttAliases::assignOutbound(const uint8_t* topic, uint16_t length,
                                     bool searchFirst) {
    if (!topic || length == 0) return 0;

    // The peer accepts none. The common default, and the branch worth being
    // explicit about rather than discovering through a rejected publish.
    if (_outboundMaximum == 0) return 0;

    // NO SECOND LOOKUP FROM deliverQueued().
    //
    // The broker's only caller reaches this having JUST had lookupOutbound()
    // return zero for the same topic - so repeating the search here walked all
    // MQTT_MAX_ALIASES slots a second time, with a vector compare on each, per
    // delivered message. It is kept for any other caller, which is why it is a
    // parameter rather than a deletion: the broker passes false because it
    // already knows the answer.
    if (searchFirst) {
        const uint16_t existing = lookupOutbound(topic, length);
        if (existing != 0) return existing;
    }

    // A topic too long for an entry is not aliased at all. Stored cut short
    // it would be worse than useless: a later publish to a topic that IS the
    // first MQTT_ALIAS_TOPIC bytes of this one would find it, send the alias
    // alone, and the broker would deliver that message to the long topic.
    if (!fits(length)) return 0;

    if (_nextOutbound > _outboundMaximum) {
        // Full. Evicting an entry would be worse than not aliasing: the peer
        // would keep the old meaning until it saw the new mapping, and any
        // publish in between would land on the wrong topic.
        return 0;
    }

    for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
        if (_outbound[i].inUse()) continue;
        _outbound[i].topic.assign(topic, topic + length);
        _outbound[i].alias = _nextOutbound++;
        return _outbound[i].alias;
    }
    return 0;
}

bool MqttAliases::rememberInbound(uint16_t alias, const uint8_t* topic, uint16_t length) {
    if (!inboundAllowed(alias)) return false;
    if (!topic || length == 0) return false;

    // A TOPIC THE ENTRY CANNOT HOLD IS NOT HELD AT ALL.
    //
    // A board's entries are fixed (MqttFixed.h), and MqttFixedBytes cuts what
    // it is given to its capacity, silently - which is right for a PLC's
    // STRING pin and wrong here. A 200-byte topic was kept as its first 128
    // bytes, and every later alias-only message was delivered under that
    // shortened name: a topic nobody published to, matched against the
    // subscriptions as if it were real. So the mapping is refused instead,
    // and any earlier meaning of this alias is dropped with it - the peer has
    // replaced it, and keeping the old one would deliver the next alias-only
    // message to the previous topic. The alias now means nothing, and a
    // PUBLISH that relies on it is 0x94, which is the truth: this end cannot
    // honour that mapping.
    if (!fits(length)) {
        for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
            if (_inbound[i].alias == alias) _inbound[i] = Entry();
        }
        return false;
    }

    // A peer may reassign an alias mid-connection: V5:3.3.2.3.4 - "A sender
    // can modify the Topic Alias mapping by sending another PUBLISH in the
    // same Network Connection with the same Topic Alias value and a different
    // non-zero length Topic Name."
    for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
        if (_inbound[i].alias == alias) {
            _inbound[i].topic.assign(topic, topic + length);
            return true;
        }
    }
    for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
        if (_inbound[i].inUse()) continue;
        _inbound[i].alias = alias;
        _inbound[i].topic.assign(topic, topic + length);
        return true;
    }
    return false;
}

bool MqttAliases::lookupInbound(uint16_t alias, const uint8_t*& topic,
                                uint16_t& length) const {
    topic = nullptr;
    length = 0;
    if (!inboundAllowed(alias)) return false;

    for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
        if (_inbound[i].alias != alias) continue;
        if (_inbound[i].topic.empty()) return false;
        topic = _inbound[i].topic.data();
        length = (uint16_t)_inbound[i].topic.size();
        return true;
    }
    return false;
}

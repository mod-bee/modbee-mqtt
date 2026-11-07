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
#if MQTT_LEAN
    for (int i = 0; i < MQTT_MAX_ALIASES; i++) {
        _outbound[i] = Entry();
        _inbound[i] = Entry();
    }
    _outboundUsed = 0;
#else
    // Released, not just emptied: a broker's connection keeps this object
    // for its whole life, and most of its connections never use an alias.
    std::vector<Entry>().swap(_outbound);
    std::vector<Entry>().swap(_inbound);
#endif
    // Never more than this build holds, whatever the peer offered. A peer
    // offering 65535 aliases is not an instruction to allocate 65535 entries.
    //
    // The inbound cap is offerable(), the same function the client's CONNECT
    // uses, so what it advertised and what it accepts cannot drift apart.
    _outboundMaximum = outboundMaximum > MQTT_MAX_ALIASES
                           ? (uint16_t)MQTT_MAX_ALIASES : outboundMaximum;
    _inboundMaximum = offerable(inboundMaximum);
    _sends = 0;
}

uint16_t MqttAliases::lookupOutbound(const uint8_t* topic, uint16_t length) const {
    if (!topic || length == 0) return 0;
    const uint16_t used = outboundUsed();
    for (uint16_t i = 0; i < used; i++) {
        if (sameTopic(_outbound[i].topic, topic, length)) return (uint16_t)(i + 1);
    }
    return 0;
}

MqttAliases::Choice MqttAliases::chooseOutbound(const uint8_t* topic, uint16_t length,
                                                bool replace) const {
    Choice choice;
    if (!topic || length == 0) return choice;

    // The peer accepts none. The common default, and the branch worth being
    // explicit about rather than discovering through a rejected publish.
    if (_outboundMaximum == 0) return choice;

    const uint16_t existing = lookupOutbound(topic, length);
    if (existing != 0) {
        choice.alias = existing;
        choice.aliasOnly = true;
        return choice;
    }

    // A topic too long for an entry is not aliased at all. Stored cut short
    // it would be worse than useless: a later publish to a topic that IS the
    // first MQTT_ALIAS_TOPIC bytes of this one would find it, send the alias
    // alone, and the broker would deliver that message to the long topic.
    if (!fits(length)) return choice;

    const uint16_t used = outboundUsed();
    if (used < _outboundMaximum) {
        choice.alias = (uint16_t)(used + 1);
        return choice;
    }

    // FULL. Replacing is safe because a connection is one ordered stream: the
    // publish that gives the alias its new topic carries that topic, and the
    // peer reads it before anything sent after it. What makes it safe is that
    // nothing is confirmed until it is queued (see the header) - and that a
    // copy kept for resending never carries an alias at all (MqttClient).
    if (!replace) return choice;
    uint16_t oldest = 0;
    for (uint16_t i = 1; i < used; i++) {
        if (_outbound[i].lastSent < _outbound[oldest].lastSent) oldest = i;
    }
    choice.alias = (uint16_t)(oldest + 1);
    return choice;
}

void MqttAliases::confirmOutbound(const Choice& choice, const uint8_t* topic,
                                  uint16_t length) {
    if (choice.alias == 0 || choice.alias > _outboundMaximum) return;
    const uint16_t index = (uint16_t)(choice.alias - 1);
    const uint16_t used = outboundUsed();

    if (!choice.aliasOnly) {
        if (!topic || length == 0 || !fits(length) || index > used) return;
        if (index == used) {
#if MQTT_LEAN
            _outboundUsed++;
#else
            _outbound.emplace_back();
#endif
        }
        _outbound[index].topic.assign(topic, topic + length);
    } else if (index >= used) {
        return;
    }
    _outbound[index].lastSent = ++_sends;
}

uint16_t MqttAliases::assignOutbound(const uint8_t* topic, uint16_t length) {
    const Choice choice = chooseOutbound(topic, length, false);
    confirmOutbound(choice, topic, length);
    return choice.alias;
}

bool MqttAliases::rememberInbound(uint16_t alias, const uint8_t* topic, uint16_t length) {
    if (!inboundAllowed(alias)) return false;
    if (!topic || length == 0) return false;
    const size_t index = (size_t)(alias - 1);

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
        if (index < inboundSize()) _inbound[index] = Entry();
        return false;
    }

#if !MQTT_LEAN
    // Within what we offered (inboundAllowed), so bounded by it.
    if (index >= _inbound.size()) _inbound.resize(index + 1);
#endif

    // A peer may reassign an alias mid-connection: V5:3.3.2.3.4 - "A sender
    // can modify the Topic Alias mapping by sending another PUBLISH in the
    // same Network Connection with the same Topic Alias value and a different
    // non-zero length Topic Name."
    _inbound[index].topic.assign(topic, topic + length);
    return true;
}

bool MqttAliases::lookupInbound(uint16_t alias, const uint8_t*& topic,
                                uint16_t& length) const {
    topic = nullptr;
    length = 0;
    if (!inboundAllowed(alias)) return false;

    const size_t index = (size_t)(alias - 1);
    if (index >= inboundSize() || _inbound[index].topic.empty()) return false;
    topic = _inbound[index].topic.data();
    length = (uint16_t)_inbound[index].topic.size();
    return true;
}

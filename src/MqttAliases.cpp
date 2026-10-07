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

namespace {

bool sameTopic(const std::vector<uint8_t>& held, const uint8_t* topic, uint16_t length) {
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
    _outboundMaximum = outboundMaximum > MQTT_MAX_ALIASES
                           ? (uint16_t)MQTT_MAX_ALIASES : outboundMaximum;
    _inboundMaximum = inboundMaximum > MQTT_MAX_ALIASES
                          ? (uint16_t)MQTT_MAX_ALIASES : inboundMaximum;
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

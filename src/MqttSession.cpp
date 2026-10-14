// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttSession.h"

#include <string.h>
#include "MqttNoExcept.h"

// The subscription table: as many slots as it has grown to.
#define MQ_SUBSCRIPTION_SLOTS ((int)_subscriptions.capacity())

void MqttSession::clear() {
    resetProtocolState();
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        _subscriptions[i] = MqttStoredSubscription();
    }
    _outOfMemory = false;
    for (size_t i = 0; i < kMaxControlIds; i++) {
        _controlIds[i] = MQ_PACKET_ID_NONE;
    }
}

void MqttSession::resetProtocolState() {
    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) {
        _outbound[i] = MqttStoredPublish();
    }
    _inboundCount = 0;
    _nextPacketId = 1;

    // AND THE IDENTIFIERS SUBSCRIBE AND UNSUBSCRIBE WERE HOLDING. With no
    // session there is nothing left to answer them, so no SUBACK for an old
    // identifier can ever arrive. Keeping them meant every connection that
    // died with a SUBSCRIBE unanswered used up one of eight slots for good -
    // until a client that had lost its link eight times at the wrong moment
    // connected, could not send its SUBSCRIBE, and with it at the head of the
    // queue sent nothing at all. Found by the field soak.
    for (size_t i = 0; i < kMaxControlIds; i++) {
        _controlIds[i] = MQ_PACKET_ID_NONE;
    }

    // The subscriptions stay, but none of them is established any more: the
    // peer either never knew about them or has just forgotten them.
    markAllSubscriptionsUnestablished();
}

// =============================================================================
// PACKET IDENTIFIERS
// =============================================================================

bool MqttSession::packetIdInUse(uint16_t packetId) const {
    if (indexOfOutbound(packetId) >= 0) return true;
    // ...and the ones SUBSCRIBE/UNSUBSCRIBE are holding, which used to be
    // invisible here. See the declaration for what that cost.
    for (size_t i = 0; i < kMaxControlIds; i++) {
        if (_controlIds[i] == packetId && packetId != MQ_PACKET_ID_NONE) return true;
    }
    return false;
}

bool MqttSession::reserveControlId(uint16_t packetId) {
    if (packetId == MQ_PACKET_ID_NONE) return false;
    for (size_t i = 0; i < kMaxControlIds; i++) {
        if (_controlIds[i] == MQ_PACKET_ID_NONE) { _controlIds[i] = packetId; return true; }
    }
    return false;
}

void MqttSession::releaseControlId(uint16_t packetId) {
    for (size_t i = 0; i < kMaxControlIds; i++) {
        if (_controlIds[i] == packetId) _controlIds[i] = MQ_PACKET_ID_NONE;
    }
}

uint16_t MqttSession::allocatePacketId() {
    if (outboundFull()) return MQ_PACKET_ID_NONE;

    // [V5:MQTT-2.2.1-3] non-zero and currently unused. The search is bounded
    // by the in-flight limit rather than by the identifier space, because at
    // most that many can be taken - so this cannot spin.
    for (uint32_t tried = 0; tried <= MQTT_SESSION_MAX_INFLIGHT; tried++) {
        const uint16_t candidate = _nextPacketId;

        // Wraps to 1, never to 0: zero is reserved and means "no identifier".
        _nextPacketId = (uint16_t)(_nextPacketId == MQ_PACKET_ID_MAX
                                       ? 1 : _nextPacketId + 1);

        if (candidate != MQ_PACKET_ID_NONE && !packetIdInUse(candidate)) {
            return candidate;
        }
    }
    return MQ_PACKET_ID_NONE;
}

void MqttSession::releasePacketId(uint16_t packetId) {
    releaseOutbound(packetId);
}

// =============================================================================
// OUTBOUND
// =============================================================================

int MqttSession::indexOfOutbound(uint16_t packetId) const {
    if (packetId == MQ_PACKET_ID_NONE) return -1;
    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) {
        if (_outbound[i].packetId == packetId) return i;
    }
    return -1;
}

bool MqttSession::outboundFull() const {
    return outboundCount() >= MQTT_SESSION_MAX_INFLIGHT;
}

size_t MqttSession::outboundCount() const {
    size_t count = 0;
    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) {
        if (_outbound[i].inUse()) count++;
    }
    return count;
}

bool MqttSession::storeOutbound(uint16_t packetId, uint8_t qos,
                                const uint8_t* encoded, size_t length) {
    if (packetId == MQ_PACKET_ID_NONE || !encoded || length == 0) return false;
    if (indexOfOutbound(packetId) >= 0) return false;   // already stored

    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) {
        if (_outbound[i].inUse()) continue;
        _outbound[i].packetId = packetId;
        _outbound[i].qos = qos;
        _outbound[i].awaitingPubcomp = false;
        _outbound[i].sent = false;
        _outbound[i].encoded.assign(encoded, encoded + length);
#if MQTT_LEAN
        if (_outbound[i].encoded.failed()) {   // the byte area is full
            _outbound[i] = MqttStoredPublish();
            return false;
        }
#endif
        return true;
    }
    return false;
}

MqttStoredPublish* MqttSession::findOutbound(uint16_t packetId) {
    const int index = indexOfOutbound(packetId);
    return index >= 0 ? &_outbound[index] : nullptr;
}

void MqttSession::releaseOutbound(uint16_t packetId) {
    const int index = indexOfOutbound(packetId);
    if (index < 0) return;

    // The vector is cleared rather than left holding a payload nobody will
    // send. A QoS 2 exchange over a 200 kB publish would otherwise keep those
    // bytes for the life of the session.
    _outbound[index] = MqttStoredPublish();
}

bool MqttSession::promoteToPubrel(uint16_t packetId,
                                  const uint8_t* encoded, size_t length) {
    MqttStoredPublish* stored = findOutbound(packetId);
    if (!stored || !encoded || length == 0) return false;

    // The PUBLISH has been acknowledged by a PUBREC, so what a reconnect must
    // resend from here is the PUBREL - and resending the PUBLISH instead would
    // deliver the message a second time, which is exactly what QoS 2 exists to
    // prevent. The publish's bytes are released with the assignment.
    stored->awaitingPubcomp = true;
    stored->sent = false;
    stored->encoded.assign(encoded, encoded + length);
    return true;
}

void MqttSession::markAllUnsent() {
    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) {
        if (_outbound[i].inUse()) _outbound[i].sent = false;
    }
}

MqttStoredPublish* MqttSession::nextUnsent() {
    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) {
        if (_outbound[i].inUse() && !_outbound[i].sent) return &_outbound[i];
    }
    return nullptr;
}

// =============================================================================
// INBOUND QoS 2
// =============================================================================

bool MqttSession::inboundKnown(uint16_t packetId) const {
    for (uint8_t i = 0; i < _inboundCount; i++) {
        if (_inbound[i] == packetId) return true;
    }
    return false;
}

bool MqttSession::rememberInbound(uint16_t packetId) {
    // [V5:MQTT-4.3.3-10] "Until it has received the corresponding PUBREL
    // packet, the receiver MUST acknowledge any subsequent PUBLISH packet with
    // the same Packet Identifier by sending a PUBREC. It MUST NOT cause
    // duplicate messages to be delivered to any onward recipients."
    //
    // So false here does not mean failure - it means "already delivered, do
    // not deliver again", and the caller still sends the PUBREC.
    if (inboundKnown(packetId)) return false;

    if (_inboundCount >= MQTT_SESSION_MAX_INBOUND_QOS2) {
        // Full. Refusing the oldest would be worse than refusing this one:
        // forgetting an identifier still awaiting its PUBREL is how a message
        // gets delivered twice.
        return false;
    }
    _inbound[_inboundCount++] = packetId;
    return true;
}

void MqttSession::forgetInbound(uint16_t packetId) {
    for (uint8_t i = 0; i < _inboundCount; i++) {
        if (_inbound[i] != packetId) continue;
        _inbound[i] = _inbound[_inboundCount - 1];
        _inboundCount--;
        return;
    }
}

// =============================================================================
// SUBSCRIPTIONS
// =============================================================================

int MqttSession::indexOfSubscription(const uint8_t* filter, uint16_t length) const {
    if (!filter || length == 0) return -1;
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        const MqttStoredSubscription& entry = _subscriptions[i];
        if (entry.filter.size() == length &&
            memcmp(entry.filter.data(), filter, length) == 0) {
            return i;
        }
    }
    return -1;
}

bool MqttSession::addSubscription(const uint8_t* filter, uint16_t length,
                                  uint8_t options, MqttCompletionFn onComplete,
                                  void* context) {
    if (!filter || length == 0) return false;

    // Re-subscribing to a filter already held replaces its options rather than
    // adding a second entry. V5:3.8.4 - "If a Server receives a SUBSCRIBE
    // packet containing a Topic Filter that is identical to a Non-shared
    // Subscription's Topic Filter ... then it MUST replace that existing
    // Subscription with a new Subscription."
    const int existing = indexOfSubscription(filter, length);
    if (existing >= 0) {
        _subscriptions[existing].options = options;
        _subscriptions[existing].established = false;
        _subscriptions[existing].issued = false;
        _subscriptions[existing].onComplete = onComplete;
        _subscriptions[existing].context = context;
        return true;
    }

    // Every slot taken: one more, as far as memory goes.
    if (subscriptionCount() == _subscriptions.capacity() &&
        !_subscriptions.reserve(_subscriptions.capacity() + 1)) {
        _outOfMemory = true;
        return false;
    }
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        if (_subscriptions[i].inUse()) continue;
        _subscriptions[i].filter.assign(filter, filter + length);
#if MQTT_LEAN
        if (_subscriptions[i].filter.failed()) {   // no memory for the filter
            _outOfMemory = true;
            return false;
        }
#endif
        _subscriptions[i].options = options;
        _subscriptions[i].established = false;
        _subscriptions[i].issued = false;
        _subscriptions[i].onComplete = onComplete;
        _subscriptions[i].context = context;
        return true;
    }
    return false;
}

void MqttSession::removeSubscription(const uint8_t* filter, uint16_t length) {
    const int index = indexOfSubscription(filter, length);
    if (index >= 0) _subscriptions[index] = MqttStoredSubscription();
}

void MqttSession::markSubscriptionEstablished(const uint8_t* filter, uint16_t length) {
    const int index = indexOfSubscription(filter, length);
    if (index >= 0) _subscriptions[index].established = true;
}

void MqttSession::markSubscriptionIssued(const uint8_t* filter, uint16_t length) {
    const int index = indexOfSubscription(filter, length);
    if (index >= 0) _subscriptions[index].issued = true;
}

void MqttSession::clearSubscriptionsIssued() {
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        _subscriptions[i].issued = false;
    }
}

void MqttSession::markAllSubscriptionsUnestablished() {
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        _subscriptions[i].established = false;
    }
}

size_t MqttSession::subscriptionCount() const {
    size_t count = 0;
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        if (_subscriptions[i].inUse()) count++;
    }
    return count;
}

const MqttStoredSubscription* MqttSession::subscriptionAt(size_t index) const {
    size_t seen = 0;
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        if (!_subscriptions[i].inUse()) continue;
        if (seen == index) return &_subscriptions[i];
        seen++;
    }
    return nullptr;
}

uint32_t MqttSession::storedBytes() const {
    uint32_t bytes = 0;
    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) {
        bytes += (uint32_t)_outbound[i].encoded.size();
    }
    for (int i = 0; i < MQ_SUBSCRIPTION_SLOTS; i++) {
        bytes += (uint32_t)_subscriptions[i].filter.size();
    }
    return bytes;
}

#if MQTT_LEAN
void MqttSession::bind(MqttByteArea& area) {
    for (int i = 0; i < MQTT_SESSION_MAX_INFLIGHT; i++) _outbound[i].encoded.bind(area);
}
#endif

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttBrokerSession.h"


#include <string.h>
#include "../MqttNoExcept.h"

namespace {

std::string toString(const MqttSpan& span) {
    if (span.data == nullptr || span.length == 0) return std::string();
    return std::string((const char*)span.data, (size_t)span.length);
}

bool spanEqualsString(const MqttSpan& span, const std::string& text) {
    if (span.length != text.size()) return false;
    if (span.length == 0) return true;
    return memcmp(span.data, text.data(), text.size()) == 0;
}

} // namespace

// =============================================================================
// LIFETIME
// =============================================================================

void MqttBrokerSession::configure(size_t maxSubscriptions, size_t maxQueued,
                                  bool reserveNow) {
    if (maxSubscriptions == 0) maxSubscriptions = 1;
    if (maxQueued == 0) maxQueued = 1;
    _maxSubscriptions = maxSubscriptions;
    _maxQueued = maxQueued;

    _subscriptions.clear();
    _subscriptionsUsed = 0;
    _queue.clear();
    _queueExtent = 0;
    _inbound.clear();
    if (reserveNow) {
        _subscriptions.resize(maxSubscriptions);
        _queue.resize(maxQueued);
        _inbound.reserve(maxQueued);
    } else {
        // Truly empty, not merely cleared: a reconfigure must give the memory
        // back, which is the point of not reserving.
        std::vector<MqttBrokerSubscription>().swap(_subscriptions);
        std::vector<MqttBrokerDelivery>().swap(_queue);
        std::vector<uint16_t>().swap(_inbound);
    }
}

void MqttBrokerSession::adopt(const MqttSpan& clientId) {
#if defined(MQTT_PLATFORM_HOST)
    // An identity being replaced leaves the index first, or the old name
    // would go on finding this slot.
    if (_inUse && _index) _index->erase(_self);
#endif
    _inUse = true;
    _clientId = toString(clientId);
#if defined(MQTT_PLATFORM_HOST)
    if (_index) {
        _index->markTaken(_self);
        _index->insert(_self);
    }
#endif
    _nextPacketId = 1;
    _nextSequence = 1;
    connected = false;
    expiryInterval = 0;
    offlineSince = 0;
    messagesDelivered = 0;
    messagesDropped = 0;
    will.clear();
    clearSubscriptions();
    clearInbound();
    for (MqttBrokerDelivery& entry : _queue) entry.release();
    _queueExtent = 0;
}

void MqttBrokerSession::release() {
#if defined(MQTT_PLATFORM_HOST)
    // While the identifier is still here to be found and removed.
    if (_inUse && _index) {
        _index->erase(_self);
        _index->giveBack(_self);
    }
#endif
    _inUse = false;
    _clientId.clear();
    connected = false;
    userName.clear();
    connectionSlot = kNoConnection;
    will.clear();
    clearSubscriptions();
    clearInbound();
    for (MqttBrokerDelivery& entry : _queue) entry.release();
    _queueExtent = 0;
}

bool MqttBrokerSession::matchesClientId(const MqttSpan& clientId) const {
    return _inUse && spanEqualsString(clientId, _clientId);
}

bool MqttBrokerSession::hasExpired(mq_time_t now) const {
    if (!_inUse || connected) return false;

    // [V5:3.1.2.11.2] 0xFFFFFFFF means the Session does not expire.
    if (expiryInterval == 0xFFFFFFFFu) return false;

    // Seconds on the wire, milliseconds on the clock. The elapsed time is
    // divided down rather than the interval multiplied up, because the latter
    // overflows a 32-bit millisecond count at 49.7 days and a plant's session
    // expiry is very often a week.
    return (mq_elapsed(offlineSince, now) / 1000u) >= expiryInterval;
}

// =============================================================================
// SUBSCRIPTIONS
// =============================================================================

int MqttBrokerSession::indexOfSubscription(const MqttSpan& filter) const {
    for (size_t index = 0; index < _subscriptionsUsed; index++) {
        if (spanEqualsString(filter, _subscriptions[index].filter)) return (int)index;
    }
    return -1;
}

bool MqttBrokerSession::addSubscription(const MqttSpan& filter, uint8_t options,
                                        uint32_t subscriptionId,
                                        bool& replacedExisting) {
    replacedExisting = false;

    int slot = indexOfSubscription(filter);
    if (slot >= 0) {
        // [V5:MQTT-3.8.4-3] An identical Topic Filter REPLACES the existing
        // subscription; the message queue is NOT emptied by that replacement.
        replacedExisting = true;
    } else {
        if (_subscriptionsUsed >= _maxSubscriptions) return false;   // no room
        reserveSubscriptions();
        slot = (int)_subscriptionsUsed++;
    }

    bumpRevision();
    MqttBrokerSubscription& entry = _subscriptions[(size_t)slot];
    if (entry.retainedOwed) _retainedOwed--;    // a replacement starts over
    entry = MqttBrokerSubscription();
    entry.filter = toString(filter);
    entry.options = options;
    entry.subscriptionId = subscriptionId;

    // A shared subscription routes on the filter INSIDE the "$share/name/"
    // prefix. Matching on the whole thing would mean no message ever matched,
    // which is a failure that looks exactly like a misconfigured client.
    MqttSpan shareName;
    MqttSpan inner;
    if (mq_isSharedFilter(filter) && mq_splitSharedFilter(filter, shareName, inner)) {
        entry.shareName = toString(shareName);
        entry.effective = toString(inner);
    } else {
        entry.effective = entry.filter;
    }

#if defined(MQTT_PLATFORM_HOST)
    // A replacement keeps its filter text, so its place in the routing index
    // is unchanged; only a new filter is added.
    if (_routes && !replacedExisting) {
        _routes->add(_self, entry.effective);
        if (entry.shared()) _routes->shareMembership(entry.shareName, entry.effective, +1);
    }
#endif
    return true;
}

bool MqttBrokerSession::removeSubscription(const MqttSpan& filter) {
    const int index = indexOfSubscription(filter);
    if (index < 0) return false;

    // Close the gap by moving the last live entry into it, so the front of
    // the array stays dense. Removing the last one is the same statement
    // with the move as a self-assignment, which is why there is no branch.
    bumpRevision();
#if defined(MQTT_PLATFORM_HOST)
    if (_routes) {
        const MqttBrokerSubscription& leaving = _subscriptions[(size_t)index];
        _routes->remove(_self, leaving.effective);
        if (leaving.shared()) _routes->shareMembership(leaving.shareName, leaving.effective, -1);
    }
#endif
    if (_subscriptions[(size_t)index].retainedOwed) _retainedOwed--;
    const size_t last = --_subscriptionsUsed;
    _subscriptions[(size_t)index] = _subscriptions[last];
    _subscriptions[last] = MqttBrokerSubscription();
    return true;
}

size_t MqttBrokerSession::subscriptionCount() const {
    return _subscriptionsUsed;
}

const MqttBrokerSubscription* MqttBrokerSession::subscriptionAt(size_t index) const {
    if (index >= _subscriptionsUsed) return nullptr;
    return &_subscriptions[index];
}

void MqttBrokerSession::clearSubscriptions() {
    bumpRevision();
#if defined(MQTT_PLATFORM_HOST)
    if (_routes) {
        for (size_t index = 0; index < _subscriptionsUsed; index++) {
            const MqttBrokerSubscription& leaving = _subscriptions[index];
            _routes->remove(_self, leaving.effective);
            if (leaving.shared()) _routes->shareMembership(leaving.shareName, leaving.effective, -1);
        }
    }
#endif
    for (MqttBrokerSubscription& entry : _subscriptions) {
        entry = MqttBrokerSubscription();
    }
    _subscriptionsUsed = 0;
    _retainedOwed = 0;
}

void MqttBrokerSession::oweRetained(size_t index) {
    if (index >= _subscriptionsUsed) return;
    MqttBrokerSubscription& entry = _subscriptions[index];
    if (!entry.retainedOwed) _retainedOwed++;
    entry.retainedOwed = true;
    entry.retainedNext = 0;
}

void MqttBrokerSession::retainedProgress(size_t index, size_t next, bool finished) {
    if (index >= _subscriptionsUsed) return;
    MqttBrokerSubscription& entry = _subscriptions[index];
    if (!entry.retainedOwed) return;
    if (finished) {
        entry.retainedOwed = false;
        _retainedOwed--;
    } else {
        entry.retainedNext = (uint32_t)next;
    }
}

// =============================================================================
// THE DELIVERY QUEUE
// =============================================================================

MqttBrokerDelivery* MqttBrokerSession::reserveDelivery() {
    reserveQueue();
    for (size_t index = 0; index < _queue.size(); index++) {
        MqttBrokerDelivery& entry = _queue[index];
        if (entry.inUse()) continue;
        entry.release();      // clean, and keeps the buffers it already grew
        if (index + 1 > _queueExtent) _queueExtent = index + 1;
        return &entry;
    }
    // Full. Counted rather than ignored, exactly as enqueue() does.
    messagesDropped++;
    return nullptr;
}

void MqttBrokerSession::commitDelivery(MqttBrokerDelivery& entry) {
    entry.sequence = _nextSequence++;
}

bool MqttBrokerSession::enqueue(const MqttBrokerDelivery& message) {
    reserveQueue();
    for (size_t index = 0; index < _queue.size(); index++) {
        MqttBrokerDelivery& entry = _queue[index];
        if (!entry.inUse()) {
            entry = message;
            entry.sequence = _nextSequence++;
            if (index + 1 > _queueExtent) _queueExtent = index + 1;
            return true;
        }
    }
    // Full. Counted rather than ignored: a queue that silently drops is a
    // queue whose depth nobody ever discovers is wrong.
    messagesDropped++;
    return false;
}

size_t MqttBrokerSession::queued() const {
    size_t used = 0;
    for (size_t index = 0; index < _queueExtent; index++) {
        if (_queue[index].inUse()) used++;
    }
    return used;
}

bool MqttBrokerSession::queueFull() const {
    // Still the full array: "is there any room anywhere" is a question about
    // the whole queue, not about how deep it currently goes. A queue not yet
    // reserved has all of its room.
    if (_queue.empty()) return false;
    for (const MqttBrokerDelivery& entry : _queue) {
        if (!entry.inUse()) return false;
    }
    return true;
}

MqttBrokerDelivery* MqttBrokerSession::nextUnsent(bool allowQos12) {
    // The one place the extent is pulled back in, because this runs on the
    // delivery path and so is the scan that would otherwise keep paying for a
    // queue that was deep once and is not any more.
    while (_queueExtent > 0 && !_queue[_queueExtent - 1].inUse()) _queueExtent--;

    // Oldest first. [V5:4.6] requires messages on one topic to be delivered in
    // the order they were received, and the queue is filled in that order, so
    // "the earliest unsent entry" is the whole of the ordering rule.
    MqttBrokerDelivery* oldest = nullptr;
    for (size_t index = 0; index < _queueExtent; index++) {
        MqttBrokerDelivery& entry = _queue[index];
        if (!entry.inUse() || entry.sent) continue;
        if (!allowQos12 && entry.qos > MQ_QOS_0) continue;
        if (oldest == nullptr || entry.sequence < oldest->sequence) oldest = &entry;
    }
    return oldest;
}

MqttBrokerDelivery* MqttBrokerSession::findByPacketId(uint16_t packetId) {
    if (packetId == MQ_PACKET_ID_NONE) return nullptr;
    for (size_t index = 0; index < _queueExtent; index++) {
        MqttBrokerDelivery& entry = _queue[index];
        if (entry.inUse() && entry.packetId == packetId) return &entry;
    }
    return nullptr;
}

MqttBrokerDelivery* MqttBrokerSession::deliveryAt(size_t index) {
    if (index >= _queue.size()) return nullptr;
    return &_queue[index];
}

void MqttBrokerSession::releaseDelivery(uint16_t packetId) {
    MqttBrokerDelivery* entry = findByPacketId(packetId);
    if (entry) entry->release();
}

void MqttBrokerSession::markAllUnsent() {
    // After a reconnect everything in flight goes out again [V5:4.4]. A
    // half-finished QoS 2 exchange resumes at its PUBREL rather than its
    // PUBLISH, so those keep their `sent` flag - re-sending the PUBLISH there
    // would restart an exchange the peer has already half completed.
    for (size_t index = 0; index < _queueExtent; index++) {
        MqttBrokerDelivery& entry = _queue[index];
        if (!entry.inUse()) continue;
        if (entry.awaitingPubcomp) entry.pubrelOwed = true;
        else entry.sent = false;
    }
}

// =============================================================================
// PACKET IDENTIFIERS
// =============================================================================

bool MqttBrokerSession::packetIdInUse(uint16_t packetId) const {
    if (packetId == MQ_PACKET_ID_NONE) return true;
    for (size_t index = 0; index < _queueExtent; index++) {
        if (_queue[index].inUse() && _queue[index].packetId == packetId) return true;
    }
    return false;
}

uint16_t MqttBrokerSession::allocatePacketId() {
    // [V5:MQTT-2.2.1-3] Non-zero and currently unused. The search is bounded
    // by the queue depth rather than by the 65,535-value space, because at
    // most that many identifiers can be taken - so this cannot spin.
    for (size_t tried = 0; tried <= _queue.size(); tried++) {
        const uint16_t candidate = _nextPacketId;
        _nextPacketId = (uint16_t)(_nextPacketId == MQ_PACKET_ID_MAX
                                       ? 1 : _nextPacketId + 1);
        if (candidate != MQ_PACKET_ID_NONE && !packetIdInUse(candidate)) {
            return candidate;
        }
    }
    return MQ_PACKET_ID_NONE;
}

void MqttBrokerSession::releasePacketId(uint16_t packetId) {
    releaseDelivery(packetId);
}

// =============================================================================
// INBOUND QoS 2
// =============================================================================

bool MqttBrokerSession::rememberInbound(uint16_t packetId) {
    if (inboundKnown(packetId)) return false;
    if (_inbound.size() >= _maxQueued) return false;
    _inbound.push_back(packetId);
    return true;
}

bool MqttBrokerSession::inboundKnown(uint16_t packetId) const {
    for (uint16_t entry : _inbound) {
        if (entry == packetId) return true;
    }
    return false;
}

void MqttBrokerSession::forgetInbound(uint16_t packetId) {
    for (size_t index = 0; index < _inbound.size(); index++) {
        if (_inbound[index] == packetId) {
            _inbound.erase(_inbound.begin() + (long)index);
            return;
        }
    }
}

void MqttBrokerSession::clearInbound() { _inbound.clear(); }


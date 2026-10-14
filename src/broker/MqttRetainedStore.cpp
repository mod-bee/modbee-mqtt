// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttRetainedStore.h"


#include <string.h>
#include "../MqttNoExcept.h"

namespace {

std::string toString(const MqttSpan& span) {
    if (span.data == nullptr || span.length == 0) return std::string();
    return std::string((const char*)span.data, (size_t)span.length);
}

MqttSpan asSpan(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

} // namespace

void MqttRetainedStore::configure(size_t maximum) {
    _messages.clear();
    _messages.resize(maximum);
#if MQTT_RETAINED_INDEX
    _indexed = maximum >= kIndexFrom;
    _index.clear();
    _free.clear();
    if (_indexed) {
        _index.reserve(maximum);
        _free.reserve(maximum);
        // Highest first, so the lowest slot is handed out first - the order
        // the walk would have used.
        for (size_t index = maximum; index > 0; index--) _free.push_back((uint32_t)(index - 1));
    }
#endif
}

void MqttRetainedStore::release(size_t index) {
#if MQTT_RETAINED_INDEX
    if (_indexed && _messages[index].inUse()) {
        _index.erase(_messages[index].topic);
        _free.push_back((uint32_t)index);
    }
#endif
    _messages[index] = MqttRetainedMessage();
}

void MqttRetainedStore::occupied(size_t index) {
#if MQTT_RETAINED_INDEX
    if (_indexed) _index[_messages[index].topic] = (uint32_t)index;
#else
    (void)index;
#endif
}

int MqttRetainedStore::indexOf(const MqttSpan& topic) const {
#if MQTT_RETAINED_INDEX
    if (_indexed) {
        if (topic.length == 0) return -1;
        const auto found = _index.find(toString(topic));
        return found == _index.end() ? -1 : (int)found->second;
    }
#endif
    for (size_t index = 0; index < _messages.size(); index++) {
        const MqttRetainedMessage& message = _messages[index];
        if (!message.inUse()) continue;
        if (message.topic.size() != topic.length) continue;
        if (topic.length == 0) continue;
        if (memcmp(message.topic.data(), topic.data, topic.length) == 0) {
            return (int)index;
        }
    }
    return -1;
}

int MqttRetainedStore::freeSlot() {
#if MQTT_RETAINED_INDEX
    if (_indexed) {
        if (_free.empty()) return -1;
        const uint32_t slot = _free.back();
        _free.pop_back();
        return (int)slot;
    }
#endif
    for (size_t index = 0; index < _messages.size(); index++) {
        if (!_messages[index].inUse()) return (int)index;
    }
    return -1;
}

bool MqttRetainedStore::hasExpired(const MqttRetainedMessage& message, mq_time_t now) {
    if (!message.properties.hasExpiry) return false;
    // The interval is in SECONDS [V5:3.3.2.3.3] and our clock is in
    // milliseconds. Multiplying the interval up would overflow a 32-bit
    // millisecond count at 49.7 days, so the elapsed time is divided down
    // instead - which cannot overflow and loses at most a second.
    const mq_time_t elapsedSeconds = mq_elapsed(message.storedAt, now) / 1000u;
    return elapsedSeconds >= message.properties.expiryInterval;
}

bool MqttRetainedStore::store(const MqttSpan& topic, const MqttSpan& payload,
                              uint8_t qos, const MqttProperties& properties,
                              mq_time_t now) {
    if (topic.data == nullptr || topic.length == 0) return false;

    const int existing = indexOf(topic);

    // [V5:MQTT-3.3.1-6] A retained PUBLISH with a zero-length payload REMOVES
    // the retained message and is not stored. The message itself is still
    // delivered to whoever is subscribed right now [V5:MQTT-3.3.1-7] - that is
    // the caller's business, not the store's.
    if (payload.length == 0) {
        if (existing >= 0) release((size_t)existing);
        return true;
    }

    int slot = existing;
    if (slot < 0) {
        slot = freeSlot();
        if (slot < 0) return false;    // full, and this is a new topic
    }

    MqttRetainedMessage& message = _messages[slot];
    message = MqttRetainedMessage();
    message.topic = toString(topic);
    message.payload.assign(payload.data, payload.data + payload.length);
    message.qos = qos;
    message.storedAt = now;

    // One call, and it keeps everything a forwarded PUBLISH must carry -
    // including User Property, which a hand-written list here forgot.
    //
    // Topic Alias and Subscription Identifier are deliberately NOT kept: an
    // alias is per-connection [V5:3.3.2.3.4] and an identifier belongs to the
    // subscriber that asked for it [V5:3.8.2.1.2]. copyForForwarding leaves
    // both out, for both stores, in one place.
    message.properties.copyForForwarding(properties);

    if (existing < 0) occupied((size_t)slot);
    return true;
}

void MqttRetainedStore::clear(const MqttSpan& topic) {
    const int index = indexOf(topic);
    if (index >= 0) release((size_t)index);
}

void MqttRetainedStore::clearAll() {
    configure(_messages.size());
}

size_t MqttRetainedStore::count() const {
    size_t used = 0;
    for (const MqttRetainedMessage& message : _messages) {
        if (message.inUse()) used++;
    }
    return used;
}

size_t MqttRetainedStore::sweep(mq_time_t now, size_t* liveOut) {
    size_t removed = 0;
    size_t live = 0;
    for (size_t index = 0; index < _messages.size(); index++) {
        const MqttRetainedMessage& message = _messages[index];
        if (!message.inUse()) continue;
        if (hasExpired(message, now)) {
            release(index);
            removed++;
        } else {
            live++;
        }
    }
    if (liveOut) *liveOut = live;
    return removed;
}

namespace {

// [V5:3.3.2.3.3] "The PUBLISH packet sent to a Client by the Server MUST
// contain a Message Expiry Interval set to the received value minus the time
// that the Application Message has been waiting in the Server."
bool remainingExpiryOf(const MqttRetainedMessage& message, mq_time_t now,
                       uint32_t& remaining) {
    if (!message.properties.hasExpiry) return false;
    const uint32_t waited = (uint32_t)(mq_elapsed(message.storedAt, now) / 1000u);
    remaining = waited >= message.properties.expiryInterval
                    ? 0 : message.properties.expiryInterval - waited;
    return true;
}

bool hasWildcard(const MqttSpan& filter) {
    return filter.data != nullptr &&
           (memchr(filter.data, '+', filter.length) != nullptr ||
            memchr(filter.data, '#', filter.length) != nullptr);
}

} // namespace

size_t MqttRetainedStore::visitMatchingFrom(const MqttSpan& filter, mq_time_t now,
                                            size_t from, StepFn visit, void* context) {
    const size_t end = _messages.size();
    if (visit == nullptr || from >= end) return end;

    size_t first = from, last = end;
#if MQTT_RETAINED_INDEX
    // One topic, found directly. A resume that finds it already taken - or
    // gone - is complete.
    if (_indexed && !hasWildcard(filter)) {
        const int found = indexOf(filter);
        if (found < 0 || (size_t)found < from) return end;
        first = (size_t)found;
        last = first + 1;
    }
#endif

    for (size_t index = first; index < last; index++) {
        const MqttRetainedMessage& message = _messages[index];
        if (!message.inUse()) continue;
        if (hasExpired(message, now)) {
            release(index);
            continue;
        }
        if (!mq_topicMatches(filter, asSpan(message.topic))) continue;

        uint32_t remaining = 0;
        const bool hasRemaining = remainingExpiryOf(message, now, remaining);
        if (!visit(context, message, remaining, hasRemaining)) return index;
    }
    return end;
}

void MqttRetainedStore::visitMatching(const MqttSpan& filter, mq_time_t now,
                                      VisitFn visit, void* context) {
    if (visit == nullptr) return;

    // A filter with no wildcard names one topic, and the index finds it
    // without walking the store.
    size_t first = 0, last = _messages.size();
#if MQTT_RETAINED_INDEX
    if (_indexed && !hasWildcard(filter)) {
        const int found = indexOf(filter);
        if (found < 0) return;
        first = (size_t)found;
        last = first + 1;
    }
#endif

    for (size_t index = first; index < last; index++) {
        const MqttRetainedMessage& message = _messages[index];
        if (!message.inUse()) continue;

        if (hasExpired(message, now)) {
            // Found on the way past, removed on the way past. A store that
            // only expired on a timer would hand an expired message to a
            // subscriber that arrived between two sweeps.
            release(index);
            continue;
        }

        if (!mq_topicMatches(filter, asSpan(message.topic))) continue;

        uint32_t remaining = 0;
        const bool hasRemaining = remainingExpiryOf(message, now, remaining);
        visit(context, message, remaining, hasRemaining);
    }
}


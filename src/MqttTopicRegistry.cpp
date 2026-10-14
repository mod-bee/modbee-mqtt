// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTopicRegistry.h"

#include <string.h>
#include "MqttNoExcept.h"

MqttTopicHandle MqttTopicRegistry::add(const char* topic) {
    if (topic == nullptr) return MQTT_TOPIC_HANDLE_NONE;

    const size_t length = strlen(topic);
    if (length == 0 || length >= MQTT_HANDLE_TOPIC_MAX) {
        return MQTT_TOPIC_HANDLE_NONE;
    }

    // The same text twice is the same handle, a free slot is
    // reused, and otherwise one more entry is allocated - limited by memory
    // and by what a handle can number, nothing else.
    size_t free = _entries.count;
    for (size_t index = 0; index < _entries.count; index++) {
        const Entry& entry = _entries[index];
        if (!entry.inUse) {
            if (free == _entries.count) free = index;
            continue;
        }
        if (entry.length == length && memcmp(entry.topic, topic, length) == 0) {
            return (MqttTopicHandle)index;
        }
    }
    if (free == _entries.count) {
        if (free >= 32767 || !_entries.slots.reserve(free + 1)) {
            _outOfMemory = free < 32767;
            return MQTT_TOPIC_HANDLE_NONE;
        }
        void* raw = mq_allocLarge(sizeof(Entry));
        if (raw == nullptr) {
            _outOfMemory = true;
            return MQTT_TOPIC_HANDLE_NONE;
        }
        _entries.slots[free] = new (raw) Entry();
        _entries.count++;
    }
    Entry& entry = _entries[free];
    memcpy(entry.topic, topic, length);
    entry.topic[length] = '\0';
    entry.length = (uint8_t)length;
    entry.inUse = true;
    entry.pending = false;
    entry.state = MqttTopicState();
    return (MqttTopicHandle)free;
}

MqttTopicRegistry::Entries::~Entries() noexcept {
    for (size_t index = 0; index < count; index++) {
        slots[index]->~Entry();
        mq_freeLarge(slots[index]);
    }
}

size_t MqttTopicRegistry::count() const {
    size_t used = 0;
    for (size_t index = 0; index < _entries.count; index++) {
        if (_entries[index].inUse) used++;
    }
    return used;
}

// The entries stay allocated for the next registrations.
void MqttTopicRegistry::clear() {
    for (size_t index = 0; index < _entries.count; index++) _entries[index] = Entry();
    _outOfMemory = false;
}

const char* MqttTopicRegistry::topicOf(MqttTopicHandle handle) const {
    return valid(handle) ? _entries[handle].topic : nullptr;
}

const MqttTopicState& MqttTopicRegistry::stateOf(MqttTopicHandle handle) const {
    return valid(handle) ? _entries[handle].state : _noSuchHandle;
}

MqttTopicState* MqttTopicRegistry::mutableStateOf(MqttTopicHandle handle) {
    return valid(handle) ? &_entries[handle].state : nullptr;
}


void MqttTopicRegistry::noteOutcome(void* context, MqttResult result, uint16_t) {
    MqttTopicState* state = (MqttTopicState*)context;
    if (state == nullptr) return;

    state->lastResult = result;

    // 0x10 No matching subscribers is a SUCCESS: the message was delivered to
    // the broker and accepted. Counting it as a failure would show a healthy
    // link as broken every time nobody happened to be subscribed.
    if (mq_succeeded(result) || result == MqttResult::NoMatchingSubscribers) {
        state->published++;
    } else if (result != MqttResult::Queued) {
        state->failed++;
    }
}

void MqttTopicRegistry::noteMessage(const MqttSpan& topic, const MqttSpan& payload) {
    for (size_t index = 0; index < _entries.count; index++) {
        Entry& entry = _entries[index];
        if (!entry.inUse) continue;

        MqttSpan filter;
        filter.data = (const uint8_t*)entry.topic;
        filter.length = entry.length;
        if (!mq_topicMatches(filter, topic)) continue;

        entry.state.received++;

        size_t copied = payload.length;
        entry.state.truncated = copied > MQTT_HANDLE_PAYLOAD_MAX;
        if (entry.state.truncated) copied = MQTT_HANDLE_PAYLOAD_MAX;
        if (payload.data != nullptr && copied > 0) {
            memcpy(entry.state.payload, payload.data, copied);
        }
        entry.state.payloadLength = (uint16_t)copied;
        entry.pending = true;
    }
}

bool MqttTopicRegistry::hasMessage(MqttTopicHandle handle) const {
    return valid(handle) && _entries[handle].pending;
}

bool MqttTopicRegistry::takeMessage(MqttTopicHandle handle, uint8_t* buffer,
                                    size_t capacity, size_t& length) {
    length = 0;
    if (!valid(handle)) return false;

    Entry& entry = _entries[handle];
    if (!entry.pending) return false;

    size_t copied = entry.state.payloadLength;
    if (copied > capacity) copied = capacity;
    if (buffer != nullptr && copied > 0) {
        memcpy(buffer, entry.state.payload, copied);
    }
    length = copied;

    // Cleared, so a block can tell a new message from the same one read twice.
    // The COUNTER is not cleared: that is a running total, and two readers must
    // not take each other's edges.
    entry.pending = false;
    return true;
}

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

MqttTopicHandle MqttTopicRegistry::add(const char* topic) {
    if (topic == nullptr) return MQTT_TOPIC_HANDLE_NONE;

    const size_t length = strlen(topic);
    if (length == 0 || length >= MQTT_HANDLE_TOPIC_MAX) {
        return MQTT_TOPIC_HANDLE_NONE;
    }

    // The same text twice is the same handle.
    for (int index = 0; index < MQTT_MAX_TOPIC_HANDLES; index++) {
        const Entry& entry = _entries[index];
        if (entry.inUse && entry.length == length &&
            memcmp(entry.topic, topic, length) == 0) {
            return (MqttTopicHandle)index;
        }
    }

    for (int index = 0; index < MQTT_MAX_TOPIC_HANDLES; index++) {
        Entry& entry = _entries[index];
        if (entry.inUse) continue;
        memcpy(entry.topic, topic, length);
        entry.topic[length] = '\0';
        entry.length = (uint8_t)length;
        entry.inUse = true;
        entry.pending = false;
        entry.state = MqttTopicState();
        return (MqttTopicHandle)index;
    }
    return MQTT_TOPIC_HANDLE_NONE;
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

size_t MqttTopicRegistry::count() const {
    size_t used = 0;
    for (const Entry& entry : _entries) {
        if (entry.inUse) used++;
    }
    return used;
}

void MqttTopicRegistry::clear() {
    for (Entry& entry : _entries) entry = Entry();
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
    for (Entry& entry : _entries) {
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

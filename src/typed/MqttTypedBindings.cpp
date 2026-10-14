// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTypedBindings.h"


#include "../MqttTopic.h"

#include <math.h>
#include <string.h>
#include "../MqttNoExcept.h"

void MqttTypedBindings::begin(MqttClient& client) {
    _client = &client;
    for (MqttBinding& binding : _bindings) binding = MqttBinding();
    _published = 0;
    _failed = 0;
}

bool MqttTypedBindings::add(const char* topic, const void* source,
                            MqttValueType type, double deadband,
                            mq_time_t maxIntervalMs, uint8_t qos, bool retain,
                            MqttEncoding encoding) {
    if (topic == nullptr || source == nullptr) return false;

    // Validated here rather than at the first publish. A binding with a
    // wildcard in its topic would be accepted now and refused for ever after,
    // once per scan, with nothing to say which of thirty bindings was wrong.
    if (!mq_topicNameIsValid(mq_topicSpan(topic))) return false;

    for (MqttBinding& binding : _bindings) {
        if (binding.inUse() && binding.topic == topic) {
            // Rebinding the same topic REPLACES. Two bindings on one topic
            // would publish two values to it every scan, and which one a
            // subscriber saw last would depend on the order of an array.
            binding = MqttBinding();
        }
    }

    for (MqttBinding& binding : _bindings) {
        if (binding.inUse()) continue;
        binding.topic = topic;
        binding.source = source;
        binding.type = type;
        binding.encoding = encoding;
        binding.deadband = deadband < 0.0 ? -deadband : deadband;
        binding.maxIntervalMs = maxIntervalMs;
        binding.qos = qos;
        binding.retain = retain;
        return true;
    }
    return false;   // no room; never a silent drop
}

#define MQ_BIND_NUMERIC(ctype, valuetype)                                      \
    bool MqttTypedBindings::bind(const char* topic, const ctype* source,       \
                                 double deadband, mq_time_t maxIntervalMs,     \
                                 uint8_t qos, bool retain,                     \
                                 MqttEncoding encoding) {                      \
        return add(topic, source, valuetype, deadband, maxIntervalMs, qos,     \
                   retain, encoding);                                          \
    }

MQ_BIND_NUMERIC(int8_t,   MqttValueType::Int8)
MQ_BIND_NUMERIC(uint8_t,  MqttValueType::UInt8)
MQ_BIND_NUMERIC(int16_t,  MqttValueType::Int16)
MQ_BIND_NUMERIC(uint16_t, MqttValueType::UInt16)
MQ_BIND_NUMERIC(int32_t,  MqttValueType::Int32)
MQ_BIND_NUMERIC(uint32_t, MqttValueType::UInt32)
MQ_BIND_NUMERIC(int64_t,  MqttValueType::Int64)
MQ_BIND_NUMERIC(uint64_t, MqttValueType::UInt64)
MQ_BIND_NUMERIC(float,    MqttValueType::Float32)
MQ_BIND_NUMERIC(double,   MqttValueType::Float64)

#undef MQ_BIND_NUMERIC

bool MqttTypedBindings::bind(const char* topic, const bool* source,
                             mq_time_t maxIntervalMs, uint8_t qos, bool retain,
                             MqttEncoding encoding) {
    // No deadband for a boolean. "Publish when it moves by more than 0.5" is
    // not a thing anybody means about a contact closing.
    return add(topic, source, MqttValueType::Bool, 0.0, maxIntervalMs, qos,
               retain, encoding);
}

bool MqttTypedBindings::unbind(const char* topic) {
    if (topic == nullptr) return false;
    for (MqttBinding& binding : _bindings) {
        if (binding.inUse() && binding.topic == topic) {
            binding = MqttBinding();
            return true;
        }
    }
    return false;
}

size_t MqttTypedBindings::count() const {
    size_t used = 0;
    for (const MqttBinding& binding : _bindings) {
        if (binding.inUse()) used++;
    }
    return used;
}

const MqttBinding* MqttTypedBindings::at(size_t index) const {
    return index < MQTT_MAX_BINDINGS ? &_bindings[index] : nullptr;
}

const MqttBinding* MqttTypedBindings::find(const char* topic) const {
    if (topic == nullptr) return nullptr;
    for (const MqttBinding& binding : _bindings) {
        if (binding.inUse() && binding.topic == topic) return &binding;
    }
    return nullptr;
}

MqttValue MqttTypedBindings::read(const MqttBinding& binding) {
    MqttValue value;
    value.type = binding.type;

    // memcpy rather than a dereferenced cast. The source may be any alignment
    // the application chose - a packed structure mapped onto a fieldbus image
    // is exactly the case here - and an unaligned load is undefined behaviour
    // that the sanitizer reports and some targets fault on.
    switch (binding.type) {
    case MqttValueType::Bool: {
        bool raw = false;
        memcpy(&raw, binding.source, sizeof raw);
        value.asBool = raw;
        break;
    }
    case MqttValueType::Int8: {
        int8_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asSigned = raw; break;
    }
    case MqttValueType::UInt8: {
        uint8_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asUnsigned = raw; break;
    }
    case MqttValueType::Int16: {
        int16_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asSigned = raw; break;
    }
    case MqttValueType::UInt16: {
        uint16_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asUnsigned = raw; break;
    }
    case MqttValueType::Int32: {
        int32_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asSigned = raw; break;
    }
    case MqttValueType::UInt32: {
        uint32_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asUnsigned = raw; break;
    }
    case MqttValueType::Int64: {
        int64_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asSigned = raw; break;
    }
    case MqttValueType::UInt64: {
        uint64_t raw = 0; memcpy(&raw, binding.source, sizeof raw);
        value.asUnsigned = raw; break;
    }
    case MqttValueType::Float32: {
        float raw = 0.0f; memcpy(&raw, binding.source, sizeof raw);
        value.asReal = (double)raw; break;
    }
    case MqttValueType::Float64: {
        double raw = 0.0; memcpy(&raw, binding.source, sizeof raw);
        value.asReal = raw; break;
    }
    case MqttValueType::Text:
        break;   // not bindable
    }
    return value;
}

void MqttTypedBindings::publishOne(MqttBinding& binding, mq_time_t now, bool force) {
    const MqttValue value = read(binding);

    uint8_t payload[64];
    const size_t length = mq_encodeValue(value, binding.encoding, payload, sizeof payload);
    if (length == 0) {
        binding.failures++;
        _failed++;
        return;
    }

    const MqttResult result =
        _client->publish(binding.topic.c_str(), payload, length,
                         binding.qos, binding.retain);

    if (mq_accepted(result)) {
        binding.lastValue = value.number();
        binding.lastPublishedAt = now;
        binding.everPublished = true;
        binding.publishes++;
        _published++;
    } else {
        // COUNTED, never ignored. The queue being full or the link being down
        // is something an operator should see as a number rather than infer
        // from a flat line on a trend.
        binding.failures++;
        _failed++;
    }
    (void)force;
}

void MqttTypedBindings::poll() {
    if (_client == nullptr) return;

    // Nothing is published while the link is down. The client would queue it,
    // and a queue of setpoints from forty minutes ago arriving all at once on
    // reconnect is worse than the gap - the value that matters is the current
    // one, and the max-interval rule sends that within a minute anyway.
    if (!_client->isConnected()) return;

    const mq_time_t now = mq_millis();

    for (MqttBinding& binding : _bindings) {
        if (!binding.inUse()) continue;

        const MqttValue value = read(binding);
        const double current = value.number();

        bool due = false;

        if (!binding.everPublished) {
            // A subscriber that has just arrived knows nothing. The first
            // reading always goes.
            due = true;
        } else if (binding.type == MqttValueType::Bool) {
            due = (current != 0.0) != (binding.lastValue != 0.0);
        } else {
            const double moved = fabs(current - binding.lastValue);
            due = moved > binding.deadband ||
                  (binding.deadband == 0.0 && moved != 0.0);
        }

        if (!due && binding.maxIntervalMs != 0 &&
            mq_elapsed(binding.lastPublishedAt, now) >= binding.maxIntervalMs) {
            // Unchanged, but a subscriber has to be able to tell that apart
            // from a dead publisher - and it cannot, unless something arrives.
            due = true;
        }

        if (due) publishOne(binding, now, false);
    }
}

void MqttTypedBindings::publishAll() {
    if (_client == nullptr || !_client->isConnected()) return;
    const mq_time_t now = mq_millis();
    for (MqttBinding& binding : _bindings) {
        if (binding.inUse()) publishOne(binding, now, true);
    }
}


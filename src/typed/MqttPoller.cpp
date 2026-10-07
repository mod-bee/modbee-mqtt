// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttPoller.h"


#include "../MqttTopic.h"

#include <string.h>

namespace {

// A span over a std::string WITHOUT going through c_str().
//
// mq_topicSpan(const char*) has to strlen() what it is given, and these calls
// are inside a loop over every tracked entry, run once per received message -
// so the length of a string that already knows its length was being recomputed
// per entry per message. Local to this file: it is a convenience, not an
// addition to the topic API.
MqttSpan spanOfText(const std::string& text) {
    MqttSpan span;
    if (text.empty()) return span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

} // namespace

const char* mq_qualityName(MqttQuality quality) {
    switch (quality) {
        case MqttQuality::Unknown:   return "unknown";
        case MqttQuality::Bad:       return "bad";
        case MqttQuality::Uncertain: return "uncertain";
        case MqttQuality::Good:      return "good";
    }
    return "?";
}

void MqttPoller::begin(MqttClient& client) {
    _client = &client;
    for (MqttTracked& entry : _tracked) entry = MqttTracked();

    // The application's handler is kept and called afterwards. Replacing it
    // would mean an application that wanted tracked values silently stopped
    // receiving everything else, which is the kind of fault that gets blamed
    // on the broker for a week.
    _previous = nullptr;
    _previousContext = nullptr;

    client.onMessage(&MqttPoller::onMessage, this);
}

bool MqttPoller::track(const char* filter, MqttValueType type,
                       mq_time_t maxIntervalMs, uint8_t qos,
                       MqttEncoding encoding) {
    if (_client == nullptr || filter == nullptr) return false;
    if (!mq_topicFilterIsValid(mq_topicSpan(filter))) return false;

    for (MqttTracked& entry : _tracked) {
        if (entry.inUse() && entry.topic == filter) {
            // Re-tracking replaces the type and interval but KEEPS the value.
            // A commissioning change should not throw away a reading that
            // arrived a second ago.
            entry.type = type;
            entry.maxIntervalMs = maxIntervalMs;
            entry.encoding = encoding;
            return true;
        }
    }

    for (MqttTracked& entry : _tracked) {
        if (entry.inUse()) continue;
        entry = MqttTracked();
        entry.topic = filter;
        entry.type = type;
        entry.maxIntervalMs = maxIntervalMs;
        entry.encoding = encoding;

        // Subscribing here rather than leaving it to the caller: a tracked
        // topic nobody subscribed to reads as Unknown for ever, and works out
        // to be the commonest way to spend an afternoon.
        _client->subscribe(filter, qos);
        return true;
    }
    return false;
}

bool MqttPoller::trackLiveness(const char* filter, const char* livenessTopic) {
    if (filter == nullptr || livenessTopic == nullptr) return false;
    for (MqttTracked& entry : _tracked) {
        if (entry.inUse() && entry.topic == filter) {
            entry.livenessTopic = livenessTopic;
            entry.liveByTopic = true;
            _client->subscribe(livenessTopic, MQ_QOS_1);
            return true;
        }
    }
    return false;
}

void MqttPoller::onMessage(void* context, const MqttMessage& message) {
    MqttPoller& poller = *(MqttPoller*)context;
    poller.accept(message);
    if (poller._previous) poller._previous(poller._previousContext, message);
}

void MqttPoller::accept(const MqttMessage& message) {
    const mq_time_t now = mq_millis();

    for (MqttTracked& entry : _tracked) {
        if (!entry.inUse()) continue;

        // A liveness topic first, because one message can be both - a Will
        // topic that is also tracked as a value is unusual but legal.
        if (!entry.livenessTopic.empty() &&
            mq_topicMatches(spanOfText(entry.livenessTopic), message.topic)) {
            // Empty, "0", "false" or "offline" means the publisher is gone.
            // Anything else means it is here. Deliberately generous: these are
            // the four things the field actually publishes, and guessing
            // narrower would read a perfectly good birth certificate as a
            // death.
            const size_t length = message.payload.length;
            const char* data = (const char*)message.payload.data;
            entry.liveByTopic =
                !(length == 0 ||
                  (length == 1 && data[0] == '0') ||
                  (length == 5 && memcmp(data, "false", 5) == 0) ||
                  (length == 7 && memcmp(data, "offline", 7) == 0));
        }

        if (!mq_topicMatches(spanOfText(entry.topic), message.topic)) {
            continue;
        }

        MqttValue decoded;
        if (!mq_decodeValue(message.payload.data, message.payload.length,
                            entry.type, entry.encoding, decoded)) {
            // COUNTED, and the previous value is left alone with its previous
            // timestamp - so it goes stale on schedule rather than appearing
            // fresh because something unreadable arrived.
            entry.undecodable++;
            continue;
        }

        entry.value = decoded;
        entry.payloadLength =
            message.payload.length > MQTT_TRACKED_PAYLOAD_MAX
                ? MQTT_TRACKED_PAYLOAD_MAX : (uint16_t)message.payload.length;
        if (entry.payloadLength > 0) {
            memcpy(entry.payload, message.payload.data, entry.payloadLength);
        }

        // The spans in `decoded` point into the receive buffer, which is gone
        // the moment this returns. Text is re-pointed at our own copy; a
        // number needs no fixing because it was decoded, not referenced.
        if (entry.type == MqttValueType::Text) {
            entry.value.asText.data = entry.payload;
            entry.value.asText.length = entry.payloadLength;
        }

        entry.lastReceivedAt = now;
        entry.everReceived = true;
        entry.received++;
    }
}

void MqttPoller::poll() {
    // Nothing to do. Quality is computed when it is asked for, from the clock,
    // so a caller that polls once a minute gets the same answer as one that
    // polls every scan - and there is no timer to drift.
}

MqttQuality MqttPoller::qualityOf(const MqttTracked& entry, mq_time_t now) const {
    if (!entry.everReceived) return MqttQuality::Unknown;

    // A publisher its own Will says is gone. Whatever it last told us, it is
    // not telling us that now.
    if (!entry.livenessTopic.empty() && !entry.liveByTopic) return MqttQuality::Bad;

    // Our own link. Not the publisher's - we cannot see that - but a value we
    // could not have received an update for is not one to act on.
    if (_client == nullptr || !_client->isConnected()) return MqttQuality::Bad;

    if (entry.maxIntervalMs == 0) return MqttQuality::Good;

    const mq_time_t age = mq_elapsed(entry.lastReceivedAt, now);
    if (age <= entry.maxIntervalMs) return MqttQuality::Good;
    if (age <= entry.maxIntervalMs * 2) return MqttQuality::Uncertain;
    return MqttQuality::Bad;
}

const MqttTracked* MqttPoller::find(const char* filter) const {
    if (filter == nullptr) return nullptr;
    for (const MqttTracked& entry : _tracked) {
        if (entry.inUse() && entry.topic == filter) return &entry;
    }
    return nullptr;
}

const MqttTracked* MqttPoller::at(size_t index) const {
    return index < MQTT_MAX_TRACKED ? &_tracked[index] : nullptr;
}

size_t MqttPoller::count() const {
    size_t used = 0;
    for (const MqttTracked& entry : _tracked) {
        if (entry.inUse()) used++;
    }
    return used;
}

MqttQuality MqttPoller::quality(const char* filter) const {
    const MqttTracked* entry = find(filter);
    return entry ? qualityOf(*entry, mq_millis()) : MqttQuality::Unknown;
}

bool MqttPoller::read(const char* filter, MqttValue& value,
                      MqttQuality& quality) const {
    const MqttTracked* entry = find(filter);
    if (entry == nullptr) {
        quality = MqttQuality::Unknown;
        return false;
    }
    quality = qualityOf(*entry, mq_millis());
    value = entry->value;
    return entry->everReceived;
}

double MqttPoller::number(const char* filter, double fallback) const {
    MqttValue value;
    MqttQuality quality = MqttQuality::Unknown;
    if (!read(filter, value, quality)) return fallback;

    // ONLY a Good value. A control path that took an Uncertain one would be
    // acting on a reading the library has already said it does not trust, and
    // the fallback is the caller's own choice of what to do instead.
    return quality == MqttQuality::Good ? value.number() : fallback;
}

mq_time_t MqttPoller::ageOf(const char* filter) const {
    const MqttTracked* entry = find(filter);
    if (entry == nullptr || !entry->everReceived) return 0;
    return mq_elapsed(entry->lastReceivedAt, mq_millis());
}


// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// =============================================================================
// A VALUE YOU CAN ACT ON
// =============================================================================
//
// A number on its own is not enough to act on. A pressure of 3.2 bar might be
// the current reading, or it might be what the sensor said before the cable
// was cut on Tuesday - and a control system that cannot tell those apart will
// eventually act on the second one.
//
// So every tracked value carries a QUALITY, and the quality is judged against
// THE INTERVAL THE CALLER ASKED FOR rather than a constant:
//
//     never received                 -> Unknown
//     the broker is not connected    -> Bad
//     maxInterval is 0               -> Good      (nothing was promised)
//     age <= maxInterval             -> Good
//     maxInterval < age <= 2x        -> Uncertain
//     age > 2x                       -> Bad
//
// A hard-coded "stale after five seconds" is wrong for a temperature sampled
// hourly and wrong for a pressure sampled ten times a second. The publisher
// said how often it would speak; that is what its silence is measured against.
//
// ONE HONEST LIMIT, DOCUMENTED RATHER THAN PAPERED OVER. A connected broker
// will happily serve a retained message from a device that died last Tuesday,
// and nothing in MQTT lets a subscriber tell that from a live one. So this
// reports what it knows - when WE last received something, and whether OUR
// link is up - and it does not pretend to know whether the publisher is alive.
// For that, wire a Will or a birth topic to liveness() and the plant's own
// knowledge becomes part of the answer.
//
// Unknown and Bad stay distinct because they send somebody to different
// places: Unknown is a commissioning error - a topic nobody is publishing -
// and Bad is a cable.

#include "../MqttFeatures.h"


#include "MqttValue.h"
#include "../MqttClient.h"

#include <string>

#ifndef MQTT_MAX_TRACKED
#define MQTT_MAX_TRACKED 32
#endif

// The largest payload a tracked topic keeps. Text numbers are short; this is
// generous for them and deliberately not generous enough to hold a file.
#ifndef MQTT_TRACKED_PAYLOAD_MAX
#define MQTT_TRACKED_PAYLOAD_MAX 64
#endif

enum class MqttQuality : uint8_t {
    Unknown = 0,   // nothing has ever arrived
    Bad,           // the link is down, or the value is more than twice overdue
    Uncertain,     // overdue, but not by much
    Good,
};

#define MQ_QUALITY_COUNT 4

const char* mq_qualityName(MqttQuality quality);


// A BOUND THAT DOES NOT FIT ITS COUNTER CORRUPTS RATHER THAN REFUSES.
//
// These are build flags. Raised past what the field holding the count can
// express, the count wraps silently - and the symptom is not "the bound was
// too big", it is messages going missing. Checked here, at compile time,
// rather than by whoever picked the number.
static_assert(MQTT_TRACKED_PAYLOAD_MAX <= 65535,
              "MQTT_TRACKED_PAYLOAD_MAX must fit the uint16_t payloadLength");

struct MqttTracked {
    std::string topic;             // the filter that was subscribed to
    MqttValueType type = MqttValueType::Float64;
    MqttEncoding encoding = MqttEncoding::Text;

    // How often the publisher said it would speak. Zero means it made no
    // promise, and then silence means nothing and quality stays Good.
    mq_time_t maxIntervalMs = 0;

    // Optional: a topic whose last message says whether the publisher is
    // alive - a Will topic, or a birth certificate. Empty when unused.
    std::string livenessTopic;
    bool liveByTopic = true;

    MqttValue value;
    uint8_t payload[MQTT_TRACKED_PAYLOAD_MAX] = {0};
    uint16_t payloadLength = 0;

    mq_time_t lastReceivedAt = 0;
    bool everReceived = false;

    // Counted rather than hidden: a payload that would not decode as the type
    // it was tracked as is a real disagreement between two ends, and a tracker
    // that silently kept the previous value would hide it for ever.
    uint32_t received = 0;
    uint32_t undecodable = 0;

    bool inUse() const { return !topic.empty(); }
};

class MqttPoller {
public:
    // The client is the caller's and must outlive this object.
    //
    // This installs its own message handler on the client. A handler the
    // application had already installed is CHAINED, not replaced - an
    // application that wants both tracked values and raw messages should not
    // have to choose, and silently dropping its handler would be the worst
    // possible way to tell it otherwise.
    void begin(MqttClient& client);

    // Tracks a topic and subscribes to it.
    //
    // `maxIntervalMs` is how often the publisher promises to speak, which is
    // what staleness is measured against. Pass the same number the publisher's
    // binding was given.
    bool track(const char* filter, MqttValueType type,
               mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
               MqttEncoding encoding = MqttEncoding::Text);

    // A topic that says whether the publisher of `filter` is alive - typically
    // its Will topic. A payload of "0", "false", "offline" or empty means it
    // is not, and the tracked value's quality becomes Bad however fresh it is.
    bool trackLiveness(const char* filter, const char* livenessTopic);

    void poll();

    // --- Reading ---

    MqttQuality quality(const char* filter) const;

    // The value, and whether it is worth using. `quality` is filled in either
    // way, so a caller can act on a stale reading knowingly.
    bool read(const char* filter, MqttValue& value, MqttQuality& quality) const;

    // Convenience for the common case. Returns the fallback when the quality
    // is not Good, so a control path cannot use a stale number by accident.
    double number(const char* filter, double fallback = 0.0) const;

    mq_time_t ageOf(const char* filter) const;

    size_t count() const;
    const MqttTracked* at(size_t index) const;
    const MqttTracked* find(const char* filter) const;

private:
    MqttClient* _client = nullptr;

    // The handler that was there before, so an application keeps its own.
    MqttMessageFn _previous = nullptr;
    void* _previousContext = nullptr;

    MqttTracked _tracked[MQTT_MAX_TRACKED];

    static void onMessage(void* context, const MqttMessage& message);
    void accept(const MqttMessage& message);

    MqttQuality qualityOf(const MqttTracked& entry, mq_time_t now) const;
};


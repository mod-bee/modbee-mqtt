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
// PUBLISHING A VARIABLE
// =============================================================================
//
// Point the library at a variable, say how much it has to move before anybody
// cares, and how often to say so anyway. The library does the rest on poll().
//
//     float pressure = 0.0f;
//     bindings.bind("plant/p1/pressure", &pressure, 0.5f, 60000);
//
// That publishes when the pressure moves by half a bar, and at least once a
// minute regardless - which is what a plant actually wants: a change-driven
// stream that a subscriber can still tell apart from a dead sensor.
//
// STRICTLY ABOVE THE RAW API (R3). Everything here calls MqttClient::publish
// and nothing here reaches into it. An application can use both, and a layer
// that needs something this does not offer is not blocked by it.
//
// -----------------------------------------------------------------------------
// WHY THE DEFAULT IS TEXT
// -----------------------------------------------------------------------------
//
// See MqttValue.h. Node-RED and Ignition both read decimal text and neither
// reads a big-endian binary payload without being configured to, so text is
// what interoperates. It has a second advantage that matters on this library's
// targets: TEXT IS WIDTH-INDEPENDENT. `int` is 16 bits on AVR and 32 on ESP32,
// so a binary binding to an `int` puts different bytes on the wire depending
// on which board was compiled - and the same binding in text puts "70" on the
// wire from both. The overloads below cover only the fixed-width types for the
// same reason, though C++ cannot tell `int` from `int32_t` on a machine where
// they are the same type, so this is a hazard that is documented rather than
// prevented.

#include "../MqttFeatures.h"


#include "MqttValue.h"
#include "../MqttClient.h"

#include <string>
#include <vector>

#ifndef MQTT_MAX_BINDINGS
#define MQTT_MAX_BINDINGS 32
#endif

struct MqttBinding {
    std::string topic;
    const void* source = nullptr;
    MqttValueType type = MqttValueType::Float64;
    MqttEncoding encoding = MqttEncoding::Text;

    // How far the value must move before it is worth saying. Zero publishes
    // every change, however small - which for a floating-point reading from a
    // real sensor is every single scan, and is almost never what was meant.
    double deadband = 0.0;

    // Publish at least this often even if nothing moved. Zero means never.
    //
    // This is what lets a SUBSCRIBER tell a steady value from a dead
    // publisher, which it otherwise cannot do at all - and it is the interval
    // MqttPoller judges staleness against.
    mq_time_t maxIntervalMs = 0;

    uint8_t qos = MQ_QOS_0;
    bool retain = false;

    // What was last sent, and when.
    double lastValue = 0.0;
    mq_time_t lastPublishedAt = 0;
    bool everPublished = false;

    uint32_t publishes = 0;
    uint32_t failures = 0;

    bool inUse() const { return source != nullptr; }
};

class MqttTypedBindings {
public:
    // The client is the caller's and must outlive this object.
    void begin(MqttClient& client);

    // Each returns false when there is no room or the topic is not a valid
    // Topic Name - never silently.
    bool bind(const char* topic, const bool* source,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);

    bool bind(const char* topic, const int8_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const uint8_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const int16_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const uint16_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const int32_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const uint32_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const int64_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const uint64_t* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const float* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);
    bool bind(const char* topic, const double* source, double deadband = 0.0,
              mq_time_t maxIntervalMs = 0, uint8_t qos = MQ_QOS_0,
              bool retain = false, MqttEncoding encoding = MqttEncoding::Text);

    // Anything else fails to compile rather than binding the wrong number of
    // bytes. A `long double`, a pointer, a struct - all of them are mistakes
    // that would otherwise be found on the wire.
    template <typename T>
    bool bind(const char*, const T*, double = 0.0, mq_time_t = 0,
              uint8_t = MQ_QOS_0, bool = false,
              MqttEncoding = MqttEncoding::Text) = delete;

    bool unbind(const char* topic);

    // Reads every bound variable and publishes what has changed enough or
    // waited long enough. Call it once per scan, after poll().
    void poll();

    // Publishes everything immediately, whatever the deadband says. For the
    // session-established hook: a subscriber that has just arrived has no idea
    // what any value is, and waiting for the next change could be hours.
    void publishAll();

    size_t count() const;
    uint32_t published() const { return _published; }
    uint32_t failed() const { return _failed; }

    const MqttBinding* at(size_t index) const;
    const MqttBinding* find(const char* topic) const;

private:
    MqttClient* _client = nullptr;
    MqttBinding _bindings[MQTT_MAX_BINDINGS];
    uint32_t _published = 0;
    uint32_t _failed = 0;

    bool add(const char* topic, const void* source, MqttValueType type,
             double deadband, mq_time_t maxIntervalMs, uint8_t qos, bool retain,
             MqttEncoding encoding);

    // Reads the variable through the type it was bound as.
    static MqttValue read(const MqttBinding& binding);

    void publishOne(MqttBinding& binding, mq_time_t now, bool force);
};


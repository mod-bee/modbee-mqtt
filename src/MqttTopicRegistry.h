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
// TOPICS BY NUMBER, AND STATE YOU CAN POLL
// =============================================================================
//
// A topic registered once and referred to by an integer thereafter, with a
// small block of counters beside it that anybody can read without registering
// a handler.
//
// THIS EXISTS FOR THE WRAPPER. Topics are strings, and an IEC 61131 function
// block passing a STRING[255] by value on every scan is both a performance
// problem and an ergonomic one - the block grows a 255-byte input nobody wants
// to wire. Registering once and publishing by handle turns that into an INT.
//
// The string API stays and is the primary one. This is the same machinery
// reached by a different name, and it lives in its own file because it is a
// self-contained idea: a registry of topics with state attached. It was part
// of MqttClient until the API review, where "is MqttClient doing too much"
// had an obvious answer.
//
// COUNTERS ARE RUNNING TOTALS, NEVER FLAGS. A block compares one with what it
// saw last scan, so nothing is missed between scans and two blocks reading the
// same handle cannot take each other's edges. The only thing that IS consumed
// is the payload, through takeMessage - because a reader has to be able to
// tell a new message from the same one read twice.
//
// Role-neutral: nothing here knows which end of a connection it is on.

#include "MqttResult.h"
#include "MqttTopic.h"
#include "MqttFixed.h"   // MqttGrowList

// HOW MANY TOPICS MAY BE REGISTERED: no fixed number, on a host or a board.
// The table grows with what the program registers, each entry allocated once
// (in PSRAM where a board has some), and only memory ends it - add() then
// refuses and outOfMemory() says why - or the 32767 handles an int16_t
// MqttTopicHandle can number. (It was MQTT_MAX_TOPIC_HANDLES, 16, shared by
// every block on a client.)

// The largest inbound payload a handle keeps for polling.
//
// A polled reader has to keep the message somewhere until it is collected, and
// on a device with no spare heap that somewhere is bounded. A message larger
// than this is still delivered to the CALLBACK - it is only the polled copy
// that is limited, and truncation is reported rather than hidden.
#ifndef MQTT_HANDLE_PAYLOAD_MAX
#define MQTT_HANDLE_PAYLOAD_MAX 256
#endif

// The longest topic text a handle can hold.
#ifndef MQTT_HANDLE_TOPIC_MAX
#define MQTT_HANDLE_TOPIC_MAX 128
#endif

// THE TWO SIZES ABOVE ARE BUILD FLAGS, AND RAISING ONE TOO FAR WOULD CORRUPT
// RATHER THAN REFUSE.
//
// A topic's length is stored in a uint8_t and a payload's in a uint16_t, so a
// build that raised either bound past what its counter can hold would truncate
// the stored length silently - a topic that compares equal to a different one,
// or a payload reported shorter than it is. Neither would look like a build
// flag; both would look like the library losing messages.
//
// Caught here, at compile time, rather than by whoever chose the number.
static_assert(MQTT_HANDLE_TOPIC_MAX <= 256,
              "MQTT_HANDLE_TOPIC_MAX must fit the uint8_t that stores a "
              "topic's length - raise the field before raising the bound");
static_assert(MQTT_HANDLE_PAYLOAD_MAX <= 65535,
              "MQTT_HANDLE_PAYLOAD_MAX must fit the uint16_t that stores a "
              "payload's length - raise the field before raising the bound");

typedef int16_t MqttTopicHandle;
#define MQTT_TOPIC_HANDLE_NONE ((MqttTopicHandle)-1)

// Everything a function block reads instead of registering a handler.
struct MqttTopicState {
    uint32_t published = 0;     // publishes on this topic that succeeded
    uint32_t failed = 0;
    uint32_t received = 0;      // messages delivered matching it
    MqttResult lastResult = MqttResult::Ok;

    // The most recent payload, for a reader that polls. `truncated` says the
    // message was larger than the buffer, so a block never mistakes a partial
    // reading for a complete one.
    uint8_t payload[MQTT_HANDLE_PAYLOAD_MAX] = {0};
    uint16_t payloadLength = 0;
    bool truncated = false;
};

class MqttTopicRegistry {
public:
    // Registers a topic or filter and returns its handle, or
    // MQTT_TOPIC_HANDLE_NONE when memory for one more is not there (or all
    // 32767 handles are taken) or the text will not fit.
    //
    // Registering the same text twice returns the SAME handle rather than
    // consuming a second slot: a function block that registers in its own
    // first-scan branch would otherwise use a slot per instance for a topic
    // they share.
    MqttTopicHandle add(const char* topic);

    const char* topicOf(MqttTopicHandle handle) const;

    // Never null for any handle, valid or not - a caller polling every scan
    // should not have to null-check something that cannot usefully fail.
    const MqttTopicState& stateOf(MqttTopicHandle handle) const;

    // The mutable state, for the completion handler to write into. Null for an
    // invalid handle.
    MqttTopicState* mutableStateOf(MqttTopicHandle handle);

    // Records an inbound message against every handle whose text MATCHES it.
    //
    // Matched rather than compared, because a handle may hold a FILTER as well
    // as a name: a block subscribed to "plant/+/pressure" must see what
    // arrives on "plant/line3/pressure".
    void noteMessage(const MqttSpan& topic, const MqttSpan& payload);

    // Copies the most recent payload out and clears the pending flag.
    bool takeMessage(MqttTopicHandle handle, uint8_t* buffer, size_t capacity,
                     size_t& length);

    // Whether a message is waiting to be taken.
    bool hasMessage(MqttTopicHandle handle) const;

    size_t count() const;
    void clear();

    // The table ran out of memory: a registration was refused for want of
    // RAM rather than for anything about the topic. Cleared by clear().
    bool outOfMemory() const { return _outOfMemory; }

    // The completion handler the client installs for a handle-based publish
    // or subscribe. The context is the MqttTopicState, so there is no
    // allocation, no capture, and nothing for a generated wrapper to express.
    static void noteOutcome(void* context, MqttResult result, uint16_t packetId);

private:
    struct Entry {
        char topic[MQTT_HANDLE_TOPIC_MAX] = {0};
        uint8_t length = 0;
        bool inUse = false;
        bool pending = false;       // a message is waiting to be taken
        MqttTopicState state;
    };
    // The entries, one allocation each, so an entry never moves: its
    // MqttTopicState is the context of a publish still in flight. Indexed as
    // an array is.
    struct Entries {
        MqttGrowList<Entry*> slots;
        size_t count = 0;
        Entries() noexcept = default;
        Entries(const Entries&) = delete;
        Entries& operator=(const Entries&) = delete;
        ~Entries() noexcept;
        Entry& operator[](size_t index) const noexcept { return *slots[index]; }
    };
    Entries _entries;
    bool _outOfMemory = false;

    // Returned for a handle that does not exist, so stateOf() never returns
    // null and a polled reader needs no special case.
    MqttTopicState _noSuchHandle;

    bool valid(MqttTopicHandle handle) const {
        return handle >= 0 && (size_t)handle < _entries.count &&
               _entries[handle].inUse;
    }
};

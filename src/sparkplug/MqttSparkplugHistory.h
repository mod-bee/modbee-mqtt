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
// AN OUTAGE BUFFER: EVENTS KEPT WHILE THE NODE CANNOT PUBLISH
// =============================================================================
//
// Sparkplug B has no store-and-forward of its own. While a node is not born -
// the cellular link is down, the broker is away, the primary host is offline -
// every change simply is not sent, and after the next birth the host sees only
// current values. What happened in between, and when, is gone.
//
// The specification's answer is the metric flag is_historical: "it may be
// desirable to send metrics after they were acquired ... for batching, store
// and forward, or sending local backup data during network communication
// losses. This flag denotes that the message should not be considered a real
// time/current value." A host stores such a metric in its history at the
// metric's OWN timestamp and does not let it update the live tag.
//
// This is the buffer that makes that possible on a controller: a bounded ring
// of small timestamped events, {time, source, code, value}, filled while the
// node cannot publish and drained after it births again, oldest first. When it
// is full the OLDEST event is dropped and counted, never the newest - the end
// of an outage is what an operator asks about first.
//
// It is deliberately not a recording of every metric. An analog that moves
// every scan would fill any buffer in minutes; the things worth keeping are
// events (a run started and why it stopped, an alarm raised and cleared, a
// deferral and its reason, a day's total), and monotonic counters reconcile
// the rest.
//
// MEMORY: 24 bytes an event (8 time, 4 value, 4 seq, 2+2 source and code, flags), allocated once with mq_allocLarge() - external
// RAM first on an ESP32 with PSRAM - when begin() is first called, which is on
// a scan, long after the PSRAM has joined the heap. 256 events are 6 KB of
// PSRAM and nothing of the internal heap. Nothing here is static.

#include <stddef.h>
#include <stdint.h>

struct MqttSparkplugEvent {
    // UTC milliseconds - or, with MQ_SP_EVENT_UPTIME set, mq_millis() at the
    // moment it happened, because the clock was not set yet. Corrected when
    // the event is sent if the clock is set by then.
    uint64_t timestamp = 0;
    float value = 0.0f;
    // The caller's own count, so an event sent twice is recognisably one.
    uint32_t seq = 0;
    uint16_t source = 0;
    uint16_t code = 0;
    uint8_t flags = 0;
};

#define MQ_SP_EVENT_UPTIME  0x01   // timestamp is uptime, not UTC
#define MQ_SP_EVENT_OFFLINE 0x02   // captured while the node was not born

class MqttSparkplugHistory {
public:
    MqttSparkplugHistory() = default;
    ~MqttSparkplugHistory();
    MqttSparkplugHistory(const MqttSparkplugHistory&) = delete;
    MqttSparkplugHistory& operator=(const MqttSparkplugHistory&) = delete;

    // Allocates room for `depth` events. Once: a second call is ignored and
    // answers whether the first succeeded. False when there is no memory, and
    // the buffer then holds nothing (add() counts everything as lost).
    bool begin(size_t depth);
    bool ready() const { return _events != nullptr; }

    size_t depth() const { return _depth; }
    size_t count() const { return _count; }
    bool empty() const { return _count == 0; }

    // Events dropped because the buffer was full (or never allocated): the
    // oldest went, the newest was kept. Never reset.
    uint32_t lost() const { return _lost; }

    // Keeps one event. Full: the oldest is dropped and counted.
    void add(const MqttSparkplugEvent& event);

    // The i-th oldest event still held, or null.
    const MqttSparkplugEvent* peek(size_t index) const;

    // Forgets the `n` oldest - once they have been sent.
    void drop(size_t n);

    // Puts an uptime timestamp on UTC: `utcNow` is the wall clock now and
    // `uptimeNow` mq_millis() now. Every event still marked UPTIME is moved.
    // An event older than the uptime counter's range (49 days) cannot be
    // placed and keeps its mark.
    void placeOnClock(uint64_t utcNow, uint32_t uptimeNow);

private:
    MqttSparkplugEvent* _events = nullptr;
    size_t _depth = 0;
    size_t _first = 0;
    size_t _count = 0;
    uint32_t _lost = 0;
    bool _begun = false;
};

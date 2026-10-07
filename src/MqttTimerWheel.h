// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// Timers by the thousand, without visiting the ones that are not due.
//
// WHY THIS EXISTS. A broker owes deadlines to every session it holds: a Will
// that goes out after its delay, a session that ends when its expiry passes.
// It used to find them by walking every session on every poll, which costs
// nothing at a plant's sixty-four sessions and was most of the idle cost of a
// broker holding a hundred and forty thousand (internal/STATE.md, 2026-10-04).
// A wheel finds the due ones by looking only where they are.
//
// HOW. A hashed wheel: 1024 slots of 16 ms each, so one revolution is about
// sixteen seconds. A timer sits in the slot its deadline falls in; advance()
// visits only the slots the clock has moved through since last time, and in
// each fires the timers whose deadline has actually arrived - a timer due a
// revolution or more later shares the slot and is simply left there. Arming
// and cancelling are O(1) and allocate nothing: the timer is INTRUSIVE, a
// node embedded in whatever owns it, linked into its slot's list.
//
// THE CLOCK WRAPS every 49.7 days and this is written for it. Deadlines are
// compared with mq_isFuture(), and the slot arithmetic stays continuous across
// the wrap because 2^32 ms is a whole number of revolutions (2^32 / 16 / 1024
// = 2^18). A deadline more than 2^31 ms ahead cannot be expressed at all, so
// callers arm long intervals in capped steps and re-arm when one fires early -
// which is also why firing never assumes the deadline is the owner's real one.
//
// NOT THREAD-SAFE, and nothing here is: one wheel belongs to one broker on one
// thread (R5).

#include "MqttPlatform.h"

#include <stddef.h>
#include <stdint.h>
#include <vector>

struct MqttTimer {
    MqttTimer() = default;

    // A copy is an UNLINKED timer. Copying the links would put a second node
    // into a list that only knows about the first, and the first unlink would
    // leave the copy pointing into freed or foreign memory.
    MqttTimer(const MqttTimer& other) : due(other.due), owner(other.owner), kind(other.kind) {}
    MqttTimer& operator=(const MqttTimer& other) {
        if (this != &other) { due = other.due; owner = other.owner; kind = other.kind; }
        return *this;
    }

    bool armed() const { return next != nullptr; }

    MqttTimer* prev = nullptr;
    MqttTimer* next = nullptr;
    mq_time_t due = 0;
    // Free for the owner: which thing this timer belongs to, and which of its
    // timers it is. The wheel never reads them.
    uint32_t owner = 0;
    uint8_t kind = 0;
};

typedef void (*MqttTimerFn)(void* context, MqttTimer& timer, mq_time_t now);

class MqttTimerWheel {
public:
    static const size_t kSlots = 1024;     // a power of two; see the header note
    static const mq_time_t kTickMs = 16;   // likewise
    // The furthest ahead one arm() may reach. Anything later is armed at this
    // and re-armed by its owner when it fires.
    static const mq_time_t kMaxAheadMs = 0x40000000u;   // about 12 days

    MqttTimerWheel();

    // Empties the wheel. Every timer that was armed must be considered
    // unlinked afterwards - which is only true if the owners were reset too,
    // so call it where they are (MqttBroker::begin).
    void reset(mq_time_t now);

    // Arms or re-arms. A deadline already past fires on the next advance().
    void arm(MqttTimer& timer, mq_time_t due);
    void cancel(MqttTimer& timer);

    // Fires every timer due at or before `now`, unlinked before its callback
    // so the callback may re-arm it, cancel others, or arm new ones.
    void advance(mq_time_t now, MqttTimerFn fire, void* context);

    size_t armedCount() const { return _armed; }

private:
    std::vector<MqttTimer> _heads;   // one sentinel per slot, circular lists
    uint32_t _lastTick = 0;          // the tick advance() last reached
    mq_time_t _lastTime = 0;
    size_t _armed = 0;

    static uint32_t tickOf(mq_time_t time) { return (uint32_t)(time / kTickMs); }
    void link(MqttTimer& timer, size_t slot);
    static void unlink(MqttTimer& timer);
};

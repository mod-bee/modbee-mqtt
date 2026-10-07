// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTimerWheel.h"

namespace {
const uint32_t kTickMask = 0x0FFFFFFFu;   // 2^32 ms / 16 ms per tick = 2^28 ticks
}

MqttTimerWheel::MqttTimerWheel() { reset(0); }

void MqttTimerWheel::reset(mq_time_t now) {
    _heads.assign(kSlots, MqttTimer());
    for (MqttTimer& head : _heads) {
        head.prev = &head;
        head.next = &head;
    }
    _lastTime = now;
    _lastTick = tickOf(now);
    _armed = 0;
}

void MqttTimerWheel::link(MqttTimer& timer, size_t slot) {
    MqttTimer& head = _heads[slot];
    timer.prev = head.prev;
    timer.next = &head;
    head.prev->next = &timer;
    head.prev = &timer;
    _armed++;
}

void MqttTimerWheel::unlink(MqttTimer& timer) {
    timer.prev->next = timer.next;
    timer.next->prev = timer.prev;
    timer.prev = nullptr;
    timer.next = nullptr;
}

void MqttTimerWheel::arm(MqttTimer& timer, mq_time_t due) {
    cancel(timer);
    timer.due = due;
    // A deadline before the last tick advance() reached goes in THAT tick's
    // slot, which advance() always revisits - otherwise it would land in a
    // slot already passed this revolution and wait sixteen seconds for the
    // next one.
    const uint32_t tick = mq_isFuture(_lastTime, due) ? tickOf(due) : _lastTick;
    link(timer, tick & (kSlots - 1));
}

void MqttTimerWheel::cancel(MqttTimer& timer) {
    if (!timer.armed()) return;
    unlink(timer);
    if (_armed > 0) _armed--;
}

void MqttTimerWheel::advance(mq_time_t now, MqttTimerFn fire, void* context) {
    if (_armed == 0) {
        // Nothing to find, so nothing to visit - but the position still moves,
        // or the first timer armed after an idle hour would be compared with
        // an hour-old tick.
        _lastTime = now;
        _lastTick = tickOf(now);
        return;
    }

    const uint32_t target = tickOf(now);
    const uint32_t from = _lastTick;
    // How many ticks to walk, the last one INCLUDED again: timers armed into
    // it since the previous advance may be due by now.
    uint32_t steps = (uint32_t)((target - from) & kTickMask) + 1;
    if (steps > kSlots) steps = kSlots;      // a whole revolution covers every slot

    // THE POSITION MOVES FIRST. A callback below may arm a timer whose
    // deadline has already passed, and arm() puts those in the slot of the
    // last tick reached - which must be the one the NEXT advance starts from,
    // not one this walk has already left behind, or the timer would wait a
    // whole revolution.
    _lastTick = target;
    _lastTime = now;

    for (uint32_t step = 0; step < steps; step++) {
        MqttTimer& head = _heads[(from + step) & (kSlots - 1)];
        // Detach the due ones first, then fire them. Firing while walking
        // would let a callback re-arm a timer into this same slot behind the
        // cursor and be fired twice, or unlink the node the walk stands on.
        MqttTimer due;
        due.prev = &due;
        due.next = &due;
        for (MqttTimer* node = head.next; node != &head;) {
            MqttTimer* following = node->next;
            if (!mq_isFuture(now, node->due)) {
                unlink(*node);
                node->prev = due.prev;
                node->next = &due;
                due.prev->next = node;
                due.prev = node;
            }
            node = following;
        }
        while (due.next != &due) {
            MqttTimer* node = due.next;
            unlink(*node);
            if (_armed > 0) _armed--;
            fire(context, *node, now);
        }
    }
}

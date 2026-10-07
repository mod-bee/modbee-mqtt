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
// PLATFORM
// =============================================================================
//
// The only file in the library that knows whether it is on an Arduino or a
// host. Everything above it asks for the time and yields; nothing above it
// includes a platform header.
//
// Two things are settled here and they are settled once: how wide a time value
// is, and where it comes from.

#include <stdint.h>
#include <stddef.h>

#if defined(ARDUINO)
    #define MQTT_PLATFORM_ARDUINO 1
#else
    #define MQTT_PLATFORM_HOST 1
#endif

// --- Time ---

// Fixed at 32 bits on every target.
//
// `unsigned long` is 32-bit on ESP32 and MSVC but 64-bit on Linux and macOS,
// so a library that used it would roll over after 49.7 days on the board and
// never on the host that tests it. Pinning the width keeps every timeout
// calculation identical everywhere, which means a rollover defect shows up on
// every target or on none - and the one that shows up nowhere is the one that
// takes a plant down in seven weeks.
typedef uint32_t mq_time_t;

// Milliseconds since start. Wraps about every 49.7 days.
mq_time_t mq_millis();

// Microseconds since start. Wraps about every 71.6 minutes.
//
// That wrap is short enough to hit in an afternoon, so every subtraction on
// this clock goes through mq_elapsed() below.
mq_time_t mq_micros();

// --- UTC, which is a different question from elapsed time ---
//
// Milliseconds since 1970-01-01 UTC, or ZERO where the platform has no such
// clock.
//
// mq_millis() above is monotonic from an arbitrary zero - correct for every
// deadline in this library and useless as a date. Some payload formats need a
// real one: Sparkplug's [tck-id-payloads-timestamp-in-UTC] requires every
// message's timestamp to be UTC, and a boot-relative counter there is wrong by
// however long the device has been running.
//
// ZERO IS AN HONEST ANSWER, and the reason this is not just "the time". An
// ESP32 with no RTC and no NTP knows only how long it has been on; returning a
// made-up date would be worse than saying nothing, because a layer above can
// be given the epoch from somewhere else - an NTP block, a value the SCADA
// system wrote down - and cannot correct a number it was told was real.
//
// This is here rather than behind a guard in the layer that needs it because
// R9 is right: a platform's habits belong in the platform layer, and a wall
// clock is a platform capability like any other.
uint64_t mq_epochMillis();

// How long has passed between two readings of the same clock.
//
// Unsigned arithmetic wraps, so this is correct across the rollover for any
// interval shorter than the clock's full range. Prefer it over a bare
// subtraction everywhere: writing it this way says the wrap was considered.
inline mq_time_t mq_elapsed(mq_time_t since, mq_time_t now) {
    return (mq_time_t)(now - since);
}

// Whether a deadline is still ahead of us.
//
// Half the range is treated as the future and half as the past, which is the
// only test that works on a clock that wraps. A plain `now < deadline` reads a
// deadline just past the wrap as long expired and fires everything at once.
inline bool mq_isFuture(mq_time_t now, mq_time_t deadline) {
    const mq_time_t remaining = mq_elapsed(now, deadline);
    return remaining != 0 && remaining < 0x80000000u;
}

// --- Clock source ---

// The clock is replaceable, for two reasons.
//
// PORTING. A target this library has never seen supplies its own two functions
// and needs no #ifdef here.
//
// TESTING. A test drives time by hand, so a keepalive interval or a session
// expiry measured in hours costs no wall clock, and so the 49.7 day rollover
// can be exercised in milliseconds rather than never being exercised at all.
typedef mq_time_t (*mq_clock_fn)();

// Either argument may be null, which restores the platform clock for that one.
//
// A replacement must be monotonic within its width and must WRAP rather than
// saturate, matching the platform clocks it stands in for. A source that
// saturates makes mq_elapsed() return a constant and every timeout stops.
void mq_setClockSource(mq_clock_fn millisSource, mq_clock_fn microsSource);

void mq_resetClockSource();

// --- Yielding ---

// Give the rest of the system a turn.
//
// Called where the library would otherwise spin without making progress. There
// is exactly one such place - MqttBroker::end(), draining every connection's
// last DISCONNECT before the sockets close - and it is a bounded shutdown
// loop, not a steady-state one.
//
// IT IS NOT THE PARTIAL-WRITE PATH, though this comment used to say it was.
// MqttLink::serviceTransmit() does not retry until the medium has room: a
// write that takes nothing RETURNS, and the rest goes out on the next poll
// from the cursor where it stopped. Nothing in the send path spins, so nothing
// in the send path yields - and it must not, because yield() on an ESP8266
// runs the WiFi stack and calling it per successful write would cost more than
// it could ever save.
//
// On Arduino this is the core's own yield(), which feeds the watchdog and runs
// the network stack on an ESP8266. Without it, a full transmit buffer on that
// part is a reset rather than a wait.
void mq_yield();

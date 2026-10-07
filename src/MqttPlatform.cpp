// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttPlatform.h"

#if defined(MQTT_PLATFORM_HOST)
// gettimeofday, for mq_epochMillis(). A board has no such header, which is
// exactly why that function answers zero there.
#include <sys/time.h>
#endif

#if defined(MQTT_PLATFORM_ARDUINO)

#include <Arduino.h>

namespace {

mq_time_t platformMillis() { return (mq_time_t)millis(); }
mq_time_t platformMicros() { return (mq_time_t)micros(); }

} // namespace

void mq_yield() { yield(); }

#else

#include <chrono>
#include <thread>

namespace {

// Anchored at first use, so both clocks start near zero the way Arduino's do.
//
// std::chrono::steady_clock counts from an arbitrary point that is typically
// system uptime - a large number that is nowhere near the 32-bit wrap. Left
// alone it would put the host permanently in a different part of the range
// from the board, which hides rollover defects on the machine that runs the
// tests and exposes them only on the one in the field.
std::chrono::steady_clock::time_point startupTime() {
    static const std::chrono::steady_clock::time_point t0 =
        std::chrono::steady_clock::now();
    return t0;
}

mq_time_t platformMillis() {
    const auto since = std::chrono::steady_clock::now() - startupTime();
    return (mq_time_t)std::chrono::duration_cast<std::chrono::milliseconds>(
        since).count();
}

mq_time_t platformMicros() {
    const auto since = std::chrono::steady_clock::now() - startupTime();
    return (mq_time_t)std::chrono::duration_cast<std::chrono::microseconds>(
        since).count();
}

} // namespace

void mq_yield() { std::this_thread::yield(); }

#endif

namespace {

// Constant-initialised with the platform clocks, so there is no static
// initialisation order to reason about and no null check on a path that runs
// on every poll().
mq_clock_fn g_millisSource = &platformMillis;
mq_clock_fn g_microsSource = &platformMicros;

} // namespace

mq_time_t mq_millis() { return g_millisSource(); }
mq_time_t mq_micros() { return g_microsSource(); }

void mq_setClockSource(mq_clock_fn millisSource, mq_clock_fn microsSource) {
    g_millisSource = millisSource ? millisSource : &platformMillis;
    g_microsSource = microsSource ? microsSource : &platformMicros;
}

void mq_resetClockSource() {
    g_millisSource = &platformMillis;
    g_microsSource = &platformMicros;
}

// --- UTC ---
//
// See the comment on mq_epochMillis() in MqttPlatform.h for why zero is a
// legitimate answer rather than a failure.
uint64_t mq_epochMillis() {
#if defined(MQTT_PLATFORM_HOST)
    struct timeval now;
    if (gettimeofday(&now, nullptr) != 0) return 0;
    return (uint64_t)now.tv_sec * 1000ull + (uint64_t)(now.tv_usec / 1000);
#else
    // A board. There may be an RTC and there may be NTP, and this layer cannot
    // know which - so it says nothing and the caller supplies the epoch.
    return 0;
#endif
}

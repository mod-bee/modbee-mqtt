// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttPlatform.h"

#include <stdlib.h>

// The boards with a system clock: newlib's gettimeofday()/settimeofday(), kept
// by the core across the life of the program. Everything else answers zero in
// mq_epochMillis() and false in mq_setEpochMillis().
#if defined(ESP32) || defined(ESP8266) || defined(ARDUINO_ARCH_RP2040)
    #define MQ_BOARD_HAS_SYSTEM_CLOCK 1
#else
    #define MQ_BOARD_HAS_SYSTEM_CLOCK 0
#endif

// 2024-01-01T00:00:00Z. A system clock reading earlier than this on a board is
// its count since boot - nobody has set it - and not a date.
#define MQ_EPOCH_PLAUSIBLE_SECONDS 1704067200ull

// The ESP32 heap allocator, which can be asked for external RAM by name.
#if defined(ESP32)
    #include <esp_heap_caps.h>
    #define MQ_BOARD_HAS_PSRAM_HEAP 1
#else
    #define MQ_BOARD_HAS_PSRAM_HEAP 0
#endif

#if defined(MQTT_PLATFORM_HOST) || MQ_BOARD_HAS_SYSTEM_CLOCK
// gettimeofday, for mq_epochMillis(); settimeofday on a board.
#include <sys/time.h>
#include <time.h>
#endif

#if defined(MQTT_PLATFORM_ARDUINO)

#include <Arduino.h>
#include "MqttNoExcept.h"

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
#elif MQ_BOARD_HAS_SYSTEM_CLOCK
    // The board's own clock, which counts from boot until somebody sets it -
    // SNTP, a modem's network time, an RTC. A reading before 2024 is that boot
    // count, not a date, and is answered as none rather than as 1970.
    struct timeval now;
    if (gettimeofday(&now, nullptr) != 0) return 0;
    if ((uint64_t)now.tv_sec < MQ_EPOCH_PLAUSIBLE_SECONDS) return 0;
    return (uint64_t)now.tv_sec * 1000ull + (uint64_t)(now.tv_usec / 1000);
#else
    // A board with no system clock. There may be an RTC and there may be NTP,
    // and this layer cannot know which - so it says nothing and the caller
    // supplies the epoch.
    return 0;
#endif
}

bool mq_setEpochMillis(uint64_t millisecondsSinceEpoch) {
#if !defined(MQTT_PLATFORM_HOST) && MQ_BOARD_HAS_SYSTEM_CLOCK
    if (millisecondsSinceEpoch / 1000ull < MQ_EPOCH_PLAUSIBLE_SECONDS) return false;
    struct timeval at;
    at.tv_sec = (time_t)(millisecondsSinceEpoch / 1000ull);
    at.tv_usec = (suseconds_t)((millisecondsSinceEpoch % 1000ull) * 1000ull);
    return settimeofday(&at, nullptr) == 0;
#else
    (void)millisecondsSinceEpoch;
    return false;
#endif
}

// --- Large tables ---

void* mq_allocLarge(size_t bytes) {
    if (bytes == 0) return nullptr;
#if MQ_BOARD_HAS_PSRAM_HEAP
    // External RAM first, explicitly. The core puts a block of 4 kB or more
    // there by itself (CONFIG_SPIRAM_MALLOC_ALWAYSINTERNAL), but a registry
    // that grows from a few points crosses that line late, and the part below
    // it is exactly the internal RAM the Wi-Fi stack wants. Internal RAM is
    // the fallback, so a board without PSRAM still works.
    return heap_caps_malloc_prefer(bytes, 2, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT,
                                   MALLOC_CAP_DEFAULT);
#else
    return malloc(bytes);
#endif
}

void mq_freeLarge(void* pointer) {
    if (pointer == nullptr) return;
#if MQ_BOARD_HAS_PSRAM_HEAP
    heap_caps_free(pointer);
#else
    free(pointer);
#endif
}

bool mq_hasLargeMemory() {
#if defined(MQTT_PLATFORM_HOST)
    return true;
#elif MQ_BOARD_HAS_PSRAM_HEAP
    // Asked at run time, not decided by the build: the same ESP32-S3 image
    // runs on a module with PSRAM and on one without, and only the first has
    // the room.
    return heap_caps_get_total_size(MALLOC_CAP_SPIRAM) > 0;
#else
    return false;
#endif
}

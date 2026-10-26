// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// THE PUBLISHED TEST SUITE'S HARNESS
// =============================================================================
//
// Each test program prints what it checked, one line each, and exits 1 if
// anything failed. Nothing here needs more than the standard library and the
// library itself: it builds and runs on Linux, macOS and Windows alike, on a
// GitHub runner or anywhere else.

#include <ModBeeMqtt.h>

#include <chrono>
#include <cstdio>
#include <functional>
#include <string>
#include <thread>

namespace check {

inline int& failures() {
    static int count = 0;
    return count;
}

inline int& passes() {
    static int count = 0;
    return count;
}

inline void section(const char* name) {
    std::printf("\n%s\n", name);
    std::fflush(stdout);
}

inline bool ok(bool condition, const std::string& what) {
    std::printf("  %s  %s\n", condition ? "pass" : "FAIL", what.c_str());
    std::fflush(stdout);
    (condition ? passes() : failures())++;
    return condition;
}

template <typename T>
bool equal(const std::string& what, const T& got, const T& expected) {
    if (got == expected) return ok(true, what);
    std::printf("  FAIL  %s (got %lld, expected %lld)\n", what.c_str(), (long long)got, (long long)expected);
    std::fflush(stdout);
    failures()++;
    return false;
}

inline bool same(const std::string& what, const std::string& got, const std::string& expected) {
    if (got == expected) return ok(true, what);
    std::printf("  FAIL  %s (got \"%s\", expected \"%s\")\n", what.c_str(), got.c_str(), expected.c_str());
    std::fflush(stdout);
    failures()++;
    return false;
}

inline int report(const char* program) {
    std::printf("\n%s: %d passed, %d failed\n", program, passes(), failures());
    return failures() == 0 ? 0 : 1;
}

// Runs `poll` until `done`, or `ms` pass. Everything in these tests is polled
// on the one thread - the library never blocks - so a test is deterministic
// in what it does, if not in exactly when.
inline bool until(const std::function<bool()>& done, const std::function<void()>& poll, int ms = 5000) {
    const auto start = std::chrono::steady_clock::now();
    while (!done()) {
        if (std::chrono::steady_clock::now() - start > std::chrono::milliseconds(ms)) return false;
        poll();
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return true;
}

inline std::string text(const MqttSpan& span) {
    return span.data ? std::string((const char*)span.data, span.length) : std::string();
}

inline MqttSpan span(const std::string& value) {
    MqttSpan out;
    out.data = (const uint8_t*)value.data();
    out.length = (uint32_t)value.size();
    return out;
}

} // namespace check

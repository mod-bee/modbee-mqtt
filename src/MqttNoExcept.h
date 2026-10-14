#pragma once

// Included last by each library source file, after every other include.
//
// The library throws nothing and catches nothing, so on a board its own
// functions need no exception tables or unwind code: about 7 kB of flash.
// Placed after the standard headers so they keep their own settings; an
// application handler that throws through the library ends the program, which
// on a controller is what an escaped exception does anyway.
//
// A board only (MQTT_LEAN, MqttFeatures.h). The host build keeps its
// exception tables and compiles exactly as before.
#include "MqttFeatures.h"

#if MQTT_LEAN && defined(__GNUC__) && !defined(__clang__)
#pragma GCC optimize("no-exceptions")
#endif

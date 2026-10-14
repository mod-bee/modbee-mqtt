cmake_minimum_required(VERSION 3.16)

project(ModBeeMqtt
    VERSION 0.1.0
    DESCRIPTION "MQTT 3.1.1 and 5.0 client, broker and bridge for Arduino and Linux - nothing waits on the network"
    LANGUAGES CXX)

# C++17 for THIS build - the host examples and the test suite use it.
#
# THE LIBRARY ITSELF NEEDS NOTHING NEWER THAN C++14, and that is deliberate.
# The floor is set by what consumes it: the mbed-based Arduino cores hard-code
# -std=gnu++14, and OpenPLC's STruC++ compiles its generated code and runtime
# as C++14. A library that needed C++17 would not build there.
# scripts/check-cxx14.sh enforces it, because one `if constexpr` written on a
# host would break those builds silently.
#
# It was C++11 until 2026-10-04. SAMD was the only listed core still at
# gnu++11, and it was dropped from library.properties rather than holding the
# whole library at C++11 - a decision, recorded in internal/CLOUD-PLAN.md §12.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MQTT_BUILD_EXAMPLES  "Build the plain C++ examples" ON)
option(MQTT_BUILD_TESTS     "Build the test suite" ON)
option(MQTT_SANITIZE        "Build with the address and undefined behaviour sanitizers" OFF)
option(MQTT_SANITIZE_THREAD "Build with the thread sanitizer (excludes MQTT_SANITIZE)" OFF)
option(MQTT_NO_EXCEPTIONS   "Build without exceptions or RTTI, as a wrapper would" OFF)

# The `modbee` command-line tool (cli/) and the server tier (server/: one
# broker per core behind one port, and the Kafka pipe). Host-only: they live
# outside src/, so no Arduino build ever sees them, and they are not part of
# the library a project links. On by default only when this repository is
# what is being built - a project that pulls the library in with
# add_subdirectory() gets the library alone. Server: Linux only.
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(_mqtt_top_level ON)
else()
    set(_mqtt_top_level OFF)
endif()

if(_mqtt_top_level AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    option(MQTT_BUILD_SERVER "Build the multi-core server tier" ON)
else()
    option(MQTT_BUILD_SERVER "Build the multi-core server tier" OFF)
endif()

if(_mqtt_top_level AND UNIX)
    option(MQTT_BUILD_CLI "Build the modbee command-line tool" ON)
else()
    option(MQTT_BUILD_CLI "Build the modbee command-line tool" OFF)
endif()

# NO FEATURE OPTIONS. Everything this library can do, it compiles.
#
# The broker, the bridge, shared subscriptions and the typed bindings used to
# be options here and #if guards in the sources. That cost a consumer with no
# build system - the Arduino IDE, the OpenPLC runtime - any way to reach them,
# which meant the broker compiled to its stub in every OpenPLC project ever
# built. See src/MqttFeatures.h for the whole reasoning.
#
# TLS is the one part that needs something this library does not ship. It is
# DETECTED rather than configured: if <openssl/ssl.h> is there, the transport
# is compiled and verifies certificates by default; if it is not, asking for TLS reports
# on a pin. Nobody has to pass a flag either way.

# The library parses bytes it did not produce, and MQTT gives it four
# independently declared lengths to get wrong - remaining length, property
# length, each string prefix, and the residual payload. An out-of-bounds read
# is therefore the failure mode that matters most and the one least likely to
# surface as a wrong answer. CI runs the suite under these on every push.
if(MQTT_SANITIZE)
    if(MSVC)
        add_compile_options(/fsanitize=address)
    else()
        # vptr is the one undefined-behaviour check left out. It needs type
        # information for every polymorphic class, and the server and CLI are
        # built without RTTI (R8) - so it reported a correct call through a
        # server class as "invalid vptr". A check that cannot tell right from
        # wrong here is noise that hides the reports that matter.
        add_compile_options(-fsanitize=address,undefined -fno-sanitize=vptr -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=address,undefined -fno-sanitize=vptr)
    endif()
endif()

# A host runtime may drive one client from two threads: an OpenPLC task
# servicing the link while application code publishes from a slower one. The
# address sanitizer says nothing about that race; only the thread sanitizer
# does, and the two cannot be built together.
if(MQTT_SANITIZE_THREAD)
    if(MQTT_SANITIZE)
        message(FATAL_ERROR "MQTT_SANITIZE and MQTT_SANITIZE_THREAD cannot be combined")
    endif()
    if(NOT MSVC)
        add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=thread)
    endif()
endif()

# Generated IEC 61131 function-block code, and several MCU toolchains, build
# with both of these off. Proving the library compiles that way is cheaper than
# discovering it does not once a wrapper exists.
if(MQTT_NO_EXCEPTIONS AND NOT MSVC)
    add_compile_options(-fno-exceptions -fno-rtti)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# =============================================================================
# LIBRARY
# =============================================================================

# Listed explicitly rather than globbed, so adding a file is a deliberate act
# and CMake never silently misses one.
set(MQTT_SOURCES
    src/MqttPlatform.cpp
    src/MqttTypes.cpp
    src/MqttResult.cpp
    src/MqttCapabilities.cpp
    src/MqttSession.cpp
    src/MqttOperations.cpp
    src/MqttAliases.cpp
    src/MqttLink.cpp
    src/MqttTopicRegistry.cpp
    src/MqttClient.cpp
    src/MqttDevice.cpp
    src/MqttWire.cpp
    src/MqttProperties.cpp
    src/MqttOwnedProperties.cpp
    src/MqttPacket.cpp
    src/MqttTopic.cpp
    src/MqttTimerWheel.cpp
    src/MqttFixed.cpp
    src/transport/MqttTcpTransport.cpp
    src/transport/MqttTlsTransport.cpp
    src/transport/MqttIdentity.cpp
    src/transport/MqttCertificate.cpp
    # A board's TLS over its own channel, by the ESP32 core's mbedTLS. Empty
    # on a host (MQTT_CHANNEL_TLS is 0 there); the test suite builds it
    # against an ESP32-shaped mbedTLS - see test/CMakeLists.txt.
    src/transport/MqttChannelTls.cpp
    # A board's connect on a task of its own (ESP32). Empty on a host unless a
    # test asks for it: test/wire/connect_task.sh.
    src/transport/MqttConnectTask.cpp
    src/codec/MqttCodecV5.cpp
    src/codec/MqttCodecV311.cpp
)

# Every role, always. What a client-only build costs is a linker question, not
# a compile one: the broker and bridge are unreferenced classes there and
# --gc-sections drops them. docs/footprint.md carries the measured figures.
list(APPEND MQTT_SOURCES
    src/sparkplug/MqttSparkplugTypes.cpp
    src/sparkplug/MqttSparkplugValue.cpp
    src/sparkplug/MqttSparkplugPayload.cpp
    src/sparkplug/MqttSparkplugTopic.cpp
    src/sparkplug/MqttSparkplugNode.cpp
    src/sparkplug/MqttSparkplugHistory.cpp
    src/sparkplug/MqttSparkplugHost.cpp
    src/broker/MqttBroker.cpp
    src/broker/MqttBrokerAccounts.cpp
    src/broker/MqttBrokerSession.cpp
    src/broker/MqttRetainedStore.cpp
    src/broker/MqttRouteIndex.cpp
    src/transport/MqttTcpListener.cpp
    src/transport/MqttTlsListener.cpp
    src/transport/MqttPollSet.cpp
    src/transport/MqttTlsAccept.cpp
    src/bridge/MqttBridge.cpp
    src/typed/MqttValue.cpp
    src/typed/MqttTypedBindings.cpp
    src/typed/MqttPoller.cpp
)

add_library(modbee-mqtt STATIC ${MQTT_SOURCES})

# The minimum a consumer needs, stated on the target rather than only in a
# comment. Anything linking this may be built at a newer standard; it may not
# be built at an older one.
target_compile_features(modbee-mqtt PUBLIC cxx_std_14)
add_library(ModBee::mqtt ALIAS modbee-mqtt)

target_include_directories(modbee-mqtt PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include/modbee>
)

if(WIN32)
    target_link_libraries(modbee-mqtt PUBLIC ws2_32)
else()
    target_link_libraries(modbee-mqtt PUBLIC Threads::Threads)
endif()

# The TLS engine, whichever the machine has. QUIET and not REQUIRED for both:
# their absence is a smaller build, not a failure - src/MqttFeatures.h detects
# the header and the TLS code compiles to nothing without either.
#
# find_package still matters even though the guard is __has_include: it is what
# puts a non-standard prefix on the include path, so the detection sees the
# same header the link will use.
#
# WOLFSSL FIRST, and the order here has to agree with MqttFeatures.h or the
# compiler and the linker will pick different engines - which shows up as a
# page of undefined references naming the engine that was NOT chosen. Point
# CMake at one with -Dwolfssl_DIR=<prefix>/lib/cmake/wolfssl, or at neither to
# get OpenSSL.
find_package(wolfssl QUIET CONFIG)
if(wolfssl_FOUND)
    target_link_libraries(modbee-mqtt PUBLIC wolfssl::wolfssl)
    message(STATUS "modbee-mqtt: wolfSSL found - it is the TLS engine")
else()
    find_package(OpenSSL QUIET)
    if(OpenSSL_FOUND)
        target_link_libraries(modbee-mqtt PUBLIC OpenSSL::SSL OpenSSL::Crypto)
        message(STATUS "modbee-mqtt: OpenSSL found - it is the TLS engine")
    else()
        message(STATUS "modbee-mqtt: no TLS engine - TLS reports rather than connects")
    endif()
endif()

if(MSVC)
    target_compile_options(modbee-mqtt PRIVATE /W4)
else()
    target_compile_options(modbee-mqtt PRIVATE -Wall -Wextra)
endif()

# =============================================================================
# EXAMPLES
# =============================================================================

if(MQTT_BUILD_EXAMPLES AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/examples/cpp/CMakeLists.txt)
    add_subdirectory(examples/cpp)
endif()

# =============================================================================
# THE COMMAND-LINE TOOL
# =============================================================================

if(MQTT_BUILD_SERVER AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/server/CMakeLists.txt)
    add_subdirectory(server)
endif()

if(MQTT_BUILD_CLI AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/cli/CMakeLists.txt)
    add_subdirectory(cli)
endif()

# =============================================================================
# TESTS
# =============================================================================

if(MQTT_BUILD_TESTS AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/test/CMakeLists.txt)
    enable_testing()
    add_subdirectory(test)
endif()

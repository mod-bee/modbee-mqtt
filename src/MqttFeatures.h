// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

#include "MqttPlatform.h"

// =============================================================================
// WHAT IS COMPILED IN
// =============================================================================
//
// EVERYTHING, AND NOTHING IS BEHIND A FEATURE FLAG.
//
// This library used to gate the broker, the bridge, shared subscriptions and
// the typed bindings behind MQTT_WITH_* defines, so a board that would never
// run a broker did not carry one. The reasoning was sound and the result was
// not: a consumer with no build system of its own - the Arduino IDE, and the
// OpenPLC runtime, which compiles a program with a fixed CXXFLAGS - has no way
// to define anything. A feature nobody can reach is not a feature; it is a
// trap with documentation. It cost a day discovering that the broker compiled
// to its stub in every OpenPLC project ever built, and then discovering the
// same thing again for TLS.
//
// So the rule is now the one modbee-modbus has always had, and it has never
// had this problem: DETECT THE PLATFORM, COMPILE EVERYTHING THE PLATFORM CAN
// CARRY, AND CHOOSE AT RUN TIME ON AN ENUMERATION. MqttSecurity picks plain or
// TLS, MqttTransport picks the medium, and a role - client, broker, bridge -
// is a class you instantiate or do not. None of that needs the preprocessor.
//
// The cost is honest and it is measured: docs/footprint.md carries the figures
// from scripts/measure-footprint.sh. On a board that only ever runs a client,
// the broker and bridge are unreferenced classes and the linker drops them -
// which is what --gc-sections is for, and what the footprint numbers show.
//
// --- The one thing that cannot simply be compiled ---
//
// TLS on a host needs OpenSSL, and OpenSSL is a library rather than a piece of
// this one. It cannot be "always compiled" on a machine that does not have it.
//
// So it is DETECTED, NOT CONFIGURED - the same shape as the platform check
// above, and not a flag anybody has to pass:
//
//   header present  -> MqttTlsTransport is compiled and verifies by default
//
// (This line named MQTT_TLS_VERIFY, a macro that is defined nowhere in this
// repository and never was - so the headline TLS behaviour was described in
// terms of a symbol that does not exist. Verification is not a build switch:
// MqttTlsTransport validates the chain unless the caller asks it not to with
// setInsecureNoCertificateValidation(), which is spelt at that length on
// purpose.)
//   header absent   -> it is not, and asking for TLS reports on a pin
//
// __has_include is safe here in a way it is NOT for an Arduino library:
// openssl/ssl.h is a system header, so there is no dependency scanner to fool.
// The trap documented in modbee-modbus is about hiding a USER library behind
// __has_include, where arduino-cli then never puts it on the include path.
//
// On Arduino there is nothing to detect. WiFiClientSecure IS an Arduino
// network client, so TLS there is MqttStreamTransport over a secure channel
// and costs no define at all.
// THE HEADER AND THE LINK LINE MUST AGREE. This is the one sharp edge that
// detection creates, and it is worth stating before somebody meets it: the
// COMPILER decides from <openssl/ssl.h>, and the LINKER is told by whoever
// wrote the link line. A machine with the header but no -lssl -lcrypto
// compiles MqttTlsTransport and then fails with a page of undefined
// references to SSL_new and friends.
//
// So: install the development headers AND link the libraries, or do neither.
// CMake gets this right by itself - find_package(OpenSSL) does both. A raw
// g++ line has to say it. The OpenPLC runtime image is the case to watch: it
// carries libssl.so.3 and no headers today, so detection correctly yields 0
// and every project builds. Adding libssl-dev WITHOUT adding -lssl -lcrypto
// to scripts/Makefile.strucpp would break every project on that runtime.
//
// An explicit value wins, and detection only fills the gap. That is not a
// feature flag coming back in through the side door: nothing has to pass it,
// every supported target is handled by the detection, and it exists so a host
// that HAS OpenSSL but does not want the transport can say so - and so the
// absent case is testable on a machine that does have the header.
// --- WHICH ENGINE, and why there are two ---
//
// wolfSSL first, OpenSSL second, and the order is the decision.
//
// wolfSSL is the only engine that can do the whole job on a BOARD: it builds
// for every Arduino architecture, it serves TLS, and - the part that decides
// it - it can WRITE an X.509 certificate. BearSSL cannot; it ships three
// certificate decoders and no writer at all, so a controller running BearSSL
// could never mint its own identity, and a broker on a board with no identity
// cannot serve TLS to anybody. Choosing wolfSSL on the host as well means a
// runtime and a board run the same code, fail the same way and print the same
// words, rather than being two stories to document and two to debug.
//
// OpenSSL stays because it is already here, already proven against three
// public brokers, and because a host that would rather use the system library
// it already patches and audits should be able to. Where both are present
// wolfSSL wins; where only one is, that one is used; where neither is, TLS
// reports on a pin rather than connecting.
//
// Detected, never configured - the same rule as everything else in this file.
#if !defined(MQTT_TLS_ENGINE_WOLFSSL) && !defined(MQTT_TLS_ENGINE_OPENSSL)
    #if defined(__has_include)
        // <wolfssl/ssl.h>, NOT <wolfssl/options.h>. options.h exists only in an
        // autotools or CMake install; the VENDORED tree - upstream's own
        // Arduino packaging, which is what travels inside a .stlib - has no
        // options.h at all and configures itself from user_settings.h instead.
        // Probing for options.h would have found the engine on a developer's
        // machine and missed it on every controller, which is the worst of the
        // two ways to be wrong.
        #if __has_include(<wolfssl/ssl.h>)
            #define MQTT_TLS_ENGINE_WOLFSSL 1
        #elif defined(MQTT_PLATFORM_HOST) && __has_include(<openssl/ssl.h>)
            #define MQTT_TLS_ENGINE_OPENSSL 1
        #endif
    #endif
#endif

#if !defined(MQTT_TLS_ENGINE_WOLFSSL)
    #define MQTT_TLS_ENGINE_WOLFSSL 0
#endif
#if !defined(MQTT_TLS_ENGINE_OPENSSL)
    #define MQTT_TLS_ENGINE_OPENSSL 0
#endif

// Naming one engine explicitly must not leave the other one also claiming to
// be selected - a build that defined MQTT_TLS_ENGINE_WOLFSSL by hand on a
// machine that also has OpenSSL would otherwise get both, and the first #if in
// MqttTlsSupport.h would silently decide.
#if MQTT_TLS_ENGINE_WOLFSSL
    #undef MQTT_TLS_ENGINE_OPENSSL
    #define MQTT_TLS_ENGINE_OPENSSL 0
#endif

#if !defined(MQTT_TLS_AVAILABLE)
    #if MQTT_TLS_ENGINE_WOLFSSL || MQTT_TLS_ENGINE_OPENSSL
        #define MQTT_TLS_AVAILABLE 1
    #endif
#endif

#if !defined(MQTT_TLS_AVAILABLE)
    #define MQTT_TLS_AVAILABLE 0
#endif

// And the reverse: MQTT_TLS_AVAILABLE=0 forced by hand means no engine, whatever
// headers happen to be installed.
#if !MQTT_TLS_AVAILABLE
    #undef MQTT_TLS_ENGINE_WOLFSSL
    #undef MQTT_TLS_ENGINE_OPENSSL
    #define MQTT_TLS_ENGINE_WOLFSSL 0
    #define MQTT_TLS_ENGINE_OPENSSL 0
#endif

// --- An engine is not the same thing as a socket ---
//
// MQTT_TLS_AVAILABLE says an engine is compiled in. It says NOTHING about how
// bytes reach the network, and the distinction only appeared once wolfSSL did:
// OpenSSL was detected on a host and nowhere else, so "there is an engine" and
// "there are BSD sockets" were the same statement by accident. wolfSSL is
// detected on a BOARD too, and a board has no <poll.h>, no <netdb.h> and no
// getaddrinfo.
//
// So MqttTlsTransport and MqttTlsListener - which dial and accept over BSD
// sockets - are compiled only where those exist. MqttIdentity is NOT: it is
// pure cryptography, it is exactly what a board needs to mint its own
// certificate, and it works anywhere the engine does.
//
// On a board, DIALLING TLS is MqttChannelTlsTransport over a plain Arduino
// Client, with the core's mbedTLS (MQTT_CHANNEL_TLS, below) - and what wolfSSL
// adds there is the SERVING side: an identity, a certificate, and
// MqttStreamTlsListener. A sketch may still hand MqttStreamTransport a secure
// Client of its own (WiFiClientSecure, SSLClient); the library no longer
// needs one.
//
// BUFFERS THAT START SMALL AND GROW (MqttLink) are a host feature: a broker
// holding a million mostly-idle connections needs them, and nothing on a
// board does - a board allocates its buffers once, at begin(), and never
// again. So on a board the growth code is not merely unused but ABSENT: the
// compiler removes every path behind it, and the field it reads is not
// there. Measured: about 1 kB of flash in a client build, for code no board
// ever runs. Define MQTT_LINK_GROWS yourself to override either way.
#ifndef MQTT_LINK_GROWS
    #if defined(MQTT_PLATFORM_HOST)
        #define MQTT_LINK_GROWS 1
    #else
        #define MQTT_LINK_GROWS 0
    #endif
#endif

// A HASH INDEX OVER THE BROKER'S RETAINED MESSAGES is a host feature for the
// same reason. A board's store is a few hundred slots, searched in a line,
// which is the fastest thing at that size and costs no flash. A server's is
// tens of thousands, on every shard, and a retained publish searching all of
// them on every shard is what would limit it. On a board the index is absent,
// not merely unused. Define MQTT_RETAINED_INDEX yourself to override.
#ifndef MQTT_RETAINED_INDEX
    #if defined(MQTT_PLATFORM_HOST)
        #define MQTT_RETAINED_INDEX 1
    #else
        #define MQTT_RETAINED_INDEX 0
    #endif
#endif

// THE BOARD BUILD'S LEAN CHOICES - one switch for the memory-saving changes
// that a board needs and a host must not get. The host build (the broker, the
// Kafka pipe, the Docker image, the CLI and its bench) is tuned for speed and
// compiles exactly as it did before any of them: with the switch off, not one
// preprocessed line of it changes, which was checked token for token.
//
// What it turns on, each where it is used:
//   - no exception tables or unwind code for the library's own functions
//     (MqttNoExcept.h): about 5 kB of flash. The library throws and catches
//     nothing, so nothing changes but the size.
//   - one property set per packet instead of one per packet kind
//     (MqttPacket.h, MQ_PROPERTIES): a packet is 1 kB on the stack instead of
//     3 kB, and about 2.7 kB less code.
//
// The tests define it on a host to run a board's code there. Define
// MQTT_LEAN yourself to override either way.
#ifndef MQTT_LEAN
    #if defined(MQTT_PLATFORM_HOST)
        #define MQTT_LEAN 0
    #else
        #define MQTT_LEAN 1
    #endif
#endif

// A board's constructors are noexcept: MqttNoExcept.h's pragma does not reach
// them, and they would keep their unwind tables. Nothing on a host.
#if MQTT_LEAN
    #define MQ_NOEXCEPT noexcept
#else
    #define MQ_NOEXCEPT
#endif

// SPARKPLUG'S PER-MESSAGE LIMITS on a host. A board's are sized for a board -
// 34 metrics a message (MqttSparkplugTypes.h) - because every payload object
// holds that many. A host application watching real plant nodes meets births
// of hundreds of metrics, and a message over the limit cannot be read at all,
// so a host defaults larger: about 70 kB a payload. Define either yourself to
// override.
#if defined(MQTT_PLATFORM_HOST)
    #ifndef MQ_SP_MAX_METRICS
        #define MQ_SP_MAX_METRICS 256
    #endif
    #ifndef MQ_SP_NAME_BYTES
        #define MQ_SP_NAME_BYTES 16384
    #endif
#endif

// THE INBOUND QoS 2 TABLE on a host (MqttSession.h, MQTT_SESSION_MAX_INBOUND_QOS2):
// 255, stated as the client's Receive Maximum at 5.0 - a consumer on a WAN link
// with only a board's 16 in flight is held to 16 round trips' worth.
#if defined(MQTT_PLATFORM_HOST)
    #ifndef MQTT_SESSION_MAX_INBOUND_QOS2
        #define MQTT_SESSION_MAX_INBOUND_QOS2 255
    #endif
#endif

#if MQTT_TLS_AVAILABLE && defined(MQTT_PLATFORM_HOST)
    #define MQTT_TLS_SOCKETS 1
#else
    #define MQTT_TLS_SOCKETS 0
#endif

// --- TLS OVER A BOARD'S OWN CHANNEL, by the core's mbedTLS ---
//
// A board dials TLS through MqttChannelTlsTransport (transport/), which runs
// the handshake on the controller over ANY Arduino channel - WiFiClient,
// EthernetClient, a modem's plain socket - and checks the broker the same way
// on all three. It is a separate question from MQTT_TLS_AVAILABLE above:
// that is wolfSSL or OpenSSL, for host sockets and for SERVING TLS; this is
// the engine the ESP32 Arduino core already links for its own Wi-Fi, so
// dialling costs the board no second TLS library.
//
// DETECTED, never configured, like everything else here: every ESP32 Arduino
// core ships mbedTLS, so ARDUINO_ARCH_ESP32 is the whole answer. Another
// board has no mbedTLS to reuse and gets 0 - MqttChannelTls.cpp then compiles
// to nothing.
//
// On a host it is 0 unless asked for, and only the test suite asks: the host
// has the socket transports, and the test builds this one against an mbedTLS
// shaped like the ESP32 core's (scripts/host-mbedtls.sh) to prove it off the
// board. The same header-and-link-line rule as OpenSSL applies: define it,
// put mbedTLS on the include path AND the link line, or do neither.
#if !defined(MQTT_CHANNEL_TLS)
    #if defined(ARDUINO_ARCH_ESP32) && defined(__has_include)
        #if __has_include(<mbedtls/ssl.h>)
            #define MQTT_CHANNEL_TLS 1
        #endif
    #endif
#endif
#if !defined(MQTT_CHANNEL_TLS)
    #define MQTT_CHANNEL_TLS 0
#endif

// --- A BOARD'S CONNECT, OUTSIDE THE SCAN ---
//
// Arduino's Client::connect() is synchronous - seconds when the broker cannot
// be reached - and a TLS handshake is heavy arithmetic for a controller.
// MqttConnectTask (transport/) runs both on a short-lived task of their own, so
// the PLC task that polls the client never waits for either. It needs threads:
// every ESP32 core runs FreeRTOS, so ARDUINO_ARCH_ESP32 is again the whole
// answer. A board without threads gets 0 and connects as it always has: inside
// begin(), which stalls its caller for as long as the medium takes.
//
// It needs MQTT_LEAN too, for MqttTransport::openFailed(), which carries an
// open that failed back to the client and exists only there. The test suite
// turns it on for a host (std::thread) to prove it off the board.
#if !defined(MQTT_CONNECT_TASK)
    #if defined(ARDUINO_ARCH_ESP32) && MQTT_LEAN
        #define MQTT_CONNECT_TASK 1
    #else
        #define MQTT_CONNECT_TASK 0
    #endif
#endif
#if MQTT_CONNECT_TASK && !MQTT_LEAN
    #error "MQTT_CONNECT_TASK needs MQTT_LEAN (MqttTransport::openFailed)"
#endif

// The name for a log line or a status pin. One place, so a runtime and a board
// cannot disagree about what they are running.
#if MQTT_TLS_ENGINE_WOLFSSL
    #define MQTT_TLS_ENGINE_NAME "wolfSSL"
#elif MQTT_TLS_ENGINE_OPENSSL
    #define MQTT_TLS_ENGINE_NAME "OpenSSL"
#else
    #define MQTT_TLS_ENGINE_NAME "none"
#endif

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
// THE TLS SESSION A BOARD DIALS WITH
// =============================================================================
//
// The engine half of MqttChannelTlsTransport: one TLS session, driven a step
// at a time, over an MqttByteChannel. The engine is the mbedTLS the ESP32
// Arduino core already links for its own Wi-Fi - see MQTT_CHANNEL_TLS in
// MqttFeatures.h - so a board that dials TLS carries no second TLS library.
//
// NO ENGINE TYPES IN THIS HEADER, for the reason MqttTlsAccept.h gives: a
// public header that pulls a TLS engine in collides with whatever else the
// sketch includes (wolfSSL's SHA_CTX against the ESP32 ROM's is the one that
// has already happened). The session is a pointer to state the .cpp owns.
//
// ALL OF ITS STATE IS ON THE HEAP AND ONLY WHILE IT IS UP. begin() allocates,
// end() frees, so a link that is down holds no TLS memory at all. While it is
// up that is about 40 kB on an ESP32 - two 16 kB record buffers, which the
// core compiles in - and on a board with PSRAM it is placed there; see
// mq_channelTlsUseExternalRam() below.
//
// What it checks is what MqttTlsTransport - the host TLS transport - checks,
// so a board and a Runtime refuse the same broker for the same reason:
//
//   validate (default)  the chain, against the CA given, and the host name
//   a pinned key        SHA-256 of the broker's SubjectPublicKeyInfo, and
//                       nothing else: no chain, no name, no dates
//   insecure            nothing - encrypted to whoever answered
//
// Dates are never judged. The core's mbedTLS is built without
// MBEDTLS_HAVE_TIME_DATE, because a controller with no clock cannot judge
// one, and the host transport's pinned mode skips them for the same reason.
//
// WHY IT FAILED is a value, MqttTlsError below, plus the engine's own code -
// not the engine's words. mbedtls_strerror() carries a text for every code in
// every mbedTLS module, about 11.7 kB of a board's flash, for sentences such
// as "SSL - A fatal alert message was received from our peer" that tell an
// engineer no more than "TLS: broker refused us -0x7780" does.

#include "../MqttFeatures.h"
#include "MqttByteChannel.h"

#include <stddef.h>
#include <stdint.h>

#if MQTT_CHANNEL_TLS

// Why a session failed: one value per thing an engineer can act on. The
// numbers are fixed - a value may be added at the end, never moved - so a
// status page or a log that stored one keeps reading it the same way.
// mq_tlsErrorText() is the short sentence for each (30 characters at most);
// docs/security.md lists them with what to do.
enum class MqttTlsError : uint8_t {
    None = 0,
    NoHost = 1,            // no host or port configured
    Unreachable = 2,       // the channel under it would not open
    NoMemory = 3,          // the engine ran out of heap
    NoCa = 4,              // validating, and no CA was given
    BadCa = 5,             // the CA would not parse
    BadClientCertificate = 6,   // the client certificate or key would not load
    EngineSetup = 7,       // the engine would not start (random seed, config)
    Untrusted = 8,         // the broker's chain does not lead to the CA given
    WrongHost = 9,         // the broker's certificate is for another name
    BadCertificate = 10,   // the broker's certificate failed another check
    NoPeerCertificate = 11,     // pinned, and the broker presented none
    PinMismatch = 12,      // not the pinned key
    Refused = 13,          // the broker ended the handshake with an alert
    NoCommonProtocol = 14, // no TLS version or cipher suite in common
    Handshake = 15,        // the handshake failed some other way
    Timeout = 16,          // the handshake was given up before it finished
    Closed = 17,           // the broker closed the connection
    Dropped = 18,          // the connection went in the middle of a record
    ReadFailed = 19,
    WriteFailed = 20,
};

// The short sentence for a value: "" for None, never null.
const char* mq_tlsErrorText(MqttTlsError error);

// A mbedTLS code (negative, high- and low-level parts OR-ed) as the value it
// means, or `otherwise` where it means nothing more specific. Public so a test
// can hold the mapping to what docs/security.md says.
MqttTlsError mq_tlsErrorFromEngine(int code, MqttTlsError otherwise);

class MqttChannelTlsSession {
public:
    MqttChannelTlsSession() { _lastError[0] = 0; _peerKey[0] = 0; }
    ~MqttChannelTlsSession() { end(); }

    MqttChannelTlsSession(const MqttChannelTlsSession&) = delete;
    MqttChannelTlsSession& operator=(const MqttChannelTlsSession&) = delete;

    // What to check. Pointers are KEPT, not copied: a board's certificates
    // are string literals in flash, and copying one into RAM per connection
    // would spend a kilobyte to hold what is already held.
    struct Trust {
        bool validate = true;              // false: setInsecureNoCertificateValidation()
        const char* caPem = nullptr;       // one or more PEM certificates
        const char* pin = nullptr;         // 64 hex; non-empty means pinned
        const char* certificatePem = nullptr;   // mutual TLS: both or neither
        const char* keyPem = nullptr;
    };

    // Allocates the engine and queues the first flight. `host` is both the
    // SNI and the name the certificate must carry when validating, and must
    // outlive the session; so must `channel`.
    bool begin(const Trust& trust, const char* host, MqttByteChannel* channel);

    // One step of the handshake: 1 established, 0 under way, -1 failed.
    // Never waits. MqttChannelTlsTransport calls it from isConnected(), which
    // is what MqttClient asks once per poll while it is Connecting.
    int pump();

    // > 0 bytes moved, 0 nothing now, -1 the session is over.
    int read(uint8_t* into, size_t length);
    int write(const uint8_t* data, size_t length);

    // Decrypted bytes already inside the engine.
    size_t pending() const;

    // The channel under it would not open. Frees the engine and records why
    // (MqttTlsError::Unreachable).
    void unreachable(const char* host, uint16_t port);

    // close_notify if the session was up, then free everything. Keeps the
    // last error and the peer's key, so a caller can still read why. Ending a
    // handshake that had not finished records MqttTlsError::Timeout, because
    // that is what it almost always is: MqttClient giving up at its connect
    // timeout (an application ending the client mid-handshake is the other).
    void end();

    bool established() const { return _state == kUp; }
    bool failed() const { return _state == kFailed; }
    bool certificateVerified() const { return _verified; }

    // Why the last attempt failed: the value, the engine's code where the
    // engine gave one (negative; 0 otherwise), and the two as one short line
    // for a LAST_ERROR pin - mq_tlsErrorText() and, with a code, " -0x7780".
    // All three are cleared by begin().
    MqttTlsError lastErrorCode() const { return _error; }
    int lastEngineCode() const { return _engineCode; }
    const char* lastError() const { return _lastError; }

    // SHA-256 over the DER SubjectPublicKeyInfo of the broker's certificate,
    // 64 lowercase hex characters - the same value MqttTlsTransport reports
    // and MQTT_TLS_PINNED pins. Empty until a handshake has completed.
    const char* peerKeyFingerprint() const { return _peerKey; }

private:
    struct Engine;

    static const int kIdle = 0;
    static const int kUp = 1;
    static const int kFailed = -1;

    void fail(MqttTlsError error, int engineCode = 0);
    bool finish();

    Engine* _engine = nullptr;
    int _state = kIdle;
    bool _verified = false;
    // After a write the engine could not finish, it wants the SAME call again;
    // see write().
    size_t _retryLength = 0;
    MqttTlsError _error = MqttTlsError::None;
    int _engineCode = 0;
    // 30 characters of sentence, " -0x" and four digits, and the terminator.
    char _lastError[40];
    char _peerKey[65];
};

// Places mbedTLS's allocations in PSRAM, when the board has any.
//
// GLOBAL, and that is the point to understand before calling it: mbedTLS has
// ONE allocator, so this moves every mbedTLS user on the board - this
// transport, and anything else on the core that speaks TLS or runs WPA's
// cryptography through mbedTLS. That is the same arrangement ESP-IDF offers
// as CONFIG_MBEDTLS_EXTERNAL_MEM_ALLOC, which the Arduino core's own build
// leaves at "internal RAM" only because it cannot know a board has PSRAM.
//
// SAFE AT ANY MOMENT, including with sessions open elsewhere: both the old
// and the new allocator release through heap_caps_free(), which frees a block
// from either memory, so nothing allocated before the switch is freed by the
// wrong function after it.
//
// What moves: blocks of 1 kB or more - the two 16 kB record buffers,
// certificates, the handshake's working state - which are what made a TLS
// session cost 40 kB of internal RAM. Smaller blocks stay internal, where the
// engine touches them most. Either kind falls back to the other memory when
// its own is full.
//
// MqttChannelTlsSession::begin() calls it once by itself. Returns whether
// PSRAM is in use; false on a board without it (nothing changed) and on any
// target that is not an ESP32. Define MQTT_CHANNEL_TLS_INTERNAL_RAM to leave
// the core's allocator alone.
bool mq_channelTlsUseExternalRam();

#endif  // MQTT_CHANNEL_TLS

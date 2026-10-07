// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// The engine half of accepting TLS connections, with no engine types here.
//
// Named "accept" rather than "server" because R1 - scripts/check-layering.py -
// forbids a layer this far down from naming a role, and "server" is a role
// word even when it means the end of a TLS handshake that presents a
// certificate. The rule is worth more than the obvious name.
//
// WHY THE HANDLES ARE void*. wolfSSL's OpenSSL compatibility layer defines
// SHA_CTX, and so does the ESP32 ROM by way of WiFiClientSecure.h. A public
// header that names SSL* or SSL_CTX* therefore pulls in a typedef that
// collides with the platform's own, and every ESP32 sketch using TLS as a
// client stops compiling - with an error pointing at neither. MqttTlsTransport
// already carries its SSL as a void* for exactly this reason; this is the same
// decision, applied to the serving side.
//
// So MqttStreamTlsListener can be a template - which is what makes it testable
// on a host - without the engine following it into every sketch.

#include "../MqttFeatures.h"

#if MQTT_TLS_AVAILABLE

#include "MqttByteChannel.h"
#include "MqttIdentity.h"

#include <string>

// The certificate and the server context: one per listener, made once.
class MqttTlsAcceptContext {
public:
    MqttTlsAcceptContext() = default;
    ~MqttTlsAcceptContext();

    MqttTlsAcceptContext(const MqttTlsAcceptContext&) = delete;
    MqttTlsAcceptContext& operator=(const MqttTlsAcceptContext&) = delete;

    // Generates a self-signed identity if it has none, then builds the
    // context. On a board the generation is wc_MakeCert over an EC P-256 key,
    // about a second; RSA-2048 would be minutes, which is why the key is EC.
    bool begin();
    void end();

    bool ready() const { return _context != nullptr; }
    void* handle() const { return _context; }

    MqttIdentity& identity() { return _identity; }
    const std::string& fingerprint() const { return _identity.fingerprint(); }
    const std::string& lastError() const { return _lastError; }

private:
    bool loadIdentity();

    MqttIdentity _identity;
    void* _context = nullptr;      // SSL_CTX*
    std::string _lastError;
};

// One accepted connection's TLS session.
class MqttTlsAcceptedSession {
public:
    MqttTlsAcceptedSession() = default;
    ~MqttTlsAcceptedSession();

    MqttTlsAcceptedSession(const MqttTlsAcceptedSession&) = delete;
    MqttTlsAcceptedSession& operator=(const MqttTlsAcceptedSession&) = delete;

    // `channel` is where the engine's bytes go; it must outlive the session.
    bool begin(MqttTlsAcceptContext& context, MqttByteChannel* channel);
    void end();

    bool live() const { return _ssl != nullptr && !_failed; }
    bool failed() const { return _failed; }

    // Drives the handshake a little further. True once it has completed.
    //
    // Pumped rather than waited for, because nothing here blocks and a PLC
    // scan cannot stop for a handshake. Until it completes, read() and write()
    // report zero - which the link reads as "nothing yet" and retries.
    bool pump();

    size_t read(uint8_t* into, size_t length);
    size_t write(const uint8_t* data, size_t length);
    size_t pending() const;        // decrypted bytes already inside the engine

private:
    void noteIfFatal(int result);

    void* _ssl = nullptr;          // SSL*
    bool _handshaking = false;
    bool _failed = false;
};

#endif  // MQTT_TLS_AVAILABLE

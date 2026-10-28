// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// TLS on a host, over OpenSSL. Compiles to nothing on Arduino, and to nothing
// at all when <openssl/ssl.h> is absent - OpenSSL is not a dependency this
// library imposes on anyone who does not want it.
//
// On Arduino none of this is needed: WiFiClientSecure IS an Arduino network
// channel, so MqttStreamTransport covers TLS there with no extra code at all.
//
// CERTIFICATE VALIDATION IS ON BY DEFAULT AND THE OPT-OUT IS NAMED SO IT
// CANNOT BE TYPED BY ACCIDENT. A transport that quietly accepted any
// certificate would make every "we use TLS" claim in a plant false, and the
// person who typed setInsecure() six months ago will not be the one reading
// the incident report.

#include "../MqttFeatures.h"   // MQTT_TLS_AVAILABLE, before anything tests it
#include "../MqttTransport.h"
#include "MqttSocketSupport.h"   // mq_socket::AddressList

#if MQTT_TLS_SOCKETS

#include <string>

// The engine's handles are held as void*, not as SSL* and SSL_CTX*.
//
// This header used to forward-declare OpenSSL's own struct tags so a consumer
// did not need OpenSSL on their include path to read this type. That worked
// for exactly one engine: wolfSSL's are WOLFSSL and WOLFSSL_CTX, so the
// forward declaration was not merely unnecessary there, it was WRONG - every
// assignment became a conversion between two unrelated struct types and the
// file would not compile at all.
//
// void* costs one cast in the .cpp, which is the only place that knows which
// engine it is talking to. MqttTlsListener has always been written this way.

class MqttTlsTransport : public MqttTransport {
public:
    MqttTlsTransport() = default;
    ~MqttTlsTransport() override;

    MqttTlsTransport(const MqttTlsTransport&) = delete;
    MqttTlsTransport& operator=(const MqttTlsTransport&) = delete;

    void configure(const char* host, uint16_t port);

    // The same two as MqttTcpTransport: how long the host name's last known
    // addresses may be dialled when DNS does not answer (seconds, 24 hours
    // unless set, 0 never), and the kernel's give-up time for unacknowledged
    // data (milliseconds, two minutes unless set, 0 the kernel's default).
    void setStaleAddressLimit(uint32_t seconds) { _addresses.setStaleAddressLimit(seconds); }
    void setDeadLinkTimeout(uint32_t milliseconds) { _addresses.setDeadLinkTimeout(milliseconds); }

    // The certificate authority to trust. Either a PEM file, or the system
    // store when neither is set.
    void setCaFile(const char* path);
    void setCaCertificate(const char* pem);

    // A client certificate, for mutual TLS.
    void setClientCertificate(const char* certificatePath, const char* keyPath);

    // Turns off certificate and hostname validation.
    //
    // Named at length on purpose. This is the single call that turns a TLS
    // connection into an encrypted conversation with whoever answered, and a
    // short name for it - setInsecure(), as several libraries use - reads in a
    // diff as a configuration detail rather than as the security decision it is.
    void setInsecureNoCertificateValidation();

    // Trust one specific key, instead of trusting an authority that vouches
    // for it. 64 hexadecimal characters; colons, spaces and capitals are
    // tolerated because every tool prints them differently.
    //
    // THIS IS FOR THE SELF-SIGNED CASE, where there is no authority to
    // validate against - a ModBee broker, or a site's own Mosquitto. It is not
    // for a cloud broker: public brokers renew every ninety days and the key
    // usually renews with the certificate, so a pinned cloud link would break
    // quarterly, in the field, at whatever hour the renewal ran.
    //
    // Pinning REPLACES chain and hostname validation rather than adding to it.
    // That is deliberate, and the expiry date is the reason: a controller with
    // no clock cannot judge one, and a link that drops because a PLC booted
    // believing it is 1970 is worse than one that trusts a key it recognises.
    void setPinnedKeyFingerprint(const char* fingerprint);

    // What the peer actually presented, once the handshake has completed.
    // Empty before that.
    //
    // This is how a connection is commissioned: bring it up once with no pin,
    // read this, and store it. Everything after that is compared against it.
    const std::string& peerKeyFingerprint() const { return _peerFingerprint; }

    bool begin() override;
    void end() override;

    size_t readable() override;
    size_t read(uint8_t* into, size_t length) override;
    size_t write(const uint8_t* buffer, size_t length) override;
    bool isConnected() const override;
    int waitHandle() const override { return _socket >= 0 ? _socket : _addresses.waitHandle(); }
    // Our own buffer OR the engine's. SSL_pending() is the half a readiness
    // set cannot see: a TLS record arrives whole, is decrypted whole, and the
    // plaintext of several MQTT packets can be sitting inside the SSL object
    // with the socket reporting nothing left to read.
    bool hasBufferedInput() const override;
    bool peerClosed() const override;
    bool connectFailed() const override { return _failed; }

    const std::string& lastError() const { return _lastError; }

    // Whether the peer's certificate was verified. False after an insecure
    // connection, so an application can report what it actually got rather
    // than what it asked for.
    bool certificateVerified() const { return _verified; }

private:
    std::string _host;
    uint16_t _port = 0;

    std::string _caFile;
    std::string _caPem;
    std::string _certFile;
    std::string _keyFile;
    bool _validate = true;
    bool _verified = false;
    std::string _pin;
    std::string _peerFingerprint;

    // Pinning and validating are alternatives, not a scale. Asked in one place
    // so the three sites that care cannot disagree about it.
    bool pinning() const { return !_pin.empty(); }

    int _socket = -1;
    void* _context = nullptr;   // SSL_CTX* / WOLFSSL_CTX*
    void* _ssl = nullptr;       // SSL*     / WOLFSSL*

    bool _connecting = false;   // TCP handshake outstanding
    bool _handshaking = false;  // TLS handshake outstanding
    bool _connected = false;
    bool _peerClosed = false;
    bool _failed = false;       // the attempt begin() started is over, unconnected

    std::string _lastError;
    int _connectError = 0;      // the socket's error from the last failed connect (SO_ERROR)

    // Set when the engine would not take the hostname before the handshake,
    // so the check runs against the peer certificate afterwards instead. Only
    // wolfSSL needs it, and only for a name with no dot in it.
    bool _checkHostAfterHandshake = false;

    // See MQTT_SOCKET_READ_SIZE in MqttSocketSupport.h - one definition
    // for all four transports, which is why this is not a number.
    static const size_t kReceiveBufferSize = MQTT_SOCKET_READ_SIZE;
    uint8_t _receive[kReceiveBufferSize];
    size_t _receiveHead = 0;
    size_t _receiveTail = 0;

    void closeAll();
    void fail(const std::string& what);
    // Ready, Resolving or Unresolvable - see mq_socket::AddressList::start().
    mq_socket::AddressList::Resolution openSocket();
    bool startSession();

    // The addresses the host resolved to, and the move to the next one. Held
    // across polls because a non-blocking connect completes later, and only
    // then is it known whether this candidate worked.
    mq_socket::AddressList _addresses;
    void exhausted();
    bool finishTcp();
    bool finishHandshake();
    bool fill();
    std::string opensslError() const;
};

#endif // MQTT_TLS_SOCKETS

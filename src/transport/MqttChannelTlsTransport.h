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
// TLS OVER ANY ARDUINO CHANNEL
// =============================================================================
//
// A board dialling a TLS broker. The channel is a PLAIN Arduino Client -
// WiFiClient, EthernetClient, a modem's GsmClient - and the TLS is done here,
// on the controller, by the mbedTLS the ESP32 core already carries
// (MQTT_CHANNEL_TLS in MqttFeatures.h). So the same broker is checked the same
// way whatever the medium, which a secure Client of each medium's own never
// managed:
//
//   WiFiClientSecure   verifies by CA, cannot pin a key, does not report the
//                      key it was shown
//   EthernetClient     has no TLS at all - it needed SSLClient
//   a modem's own TLS  encrypts and checks nothing
//
// THE CHECKS ARE MqttTlsTransport's, the host TLS transport: the same
// setters, the same defaults (validate, refuse without a CA) and the same
// pinning. A refusal is reported as a value, MqttTlsError, with a short line
// for a status pin. See MqttChannelTls.h for what is checked.
//
// NOTHING HERE WAITS FOR TLS. begin() opens the channel - Client::connect()
// blocks, as it always has, because Arduino's interface demands a verdict -
// and queues the first flight. Every later round trip is driven from
// isConnected(), which is the one call MqttClient makes per poll while it is
// Connecting, so a handshake over a modem's half-second round trips costs the
// scan nothing and CONNECT_TIMEOUT bounds it exactly as it bounds TCP.
//
// A STEP IS STILL ARITHMETIC. The key exchange and the certificate check are
// heavy work for a controller, done inside whichever isConnected() call
// reaches them. Wrapped in an MqttConnectTask (MqttConnectTask.h) the
// open and the whole handshake run on a task of their own instead, and the
// transport is handed back up; MQTT_CLIENT does that on an ESP32.
//
// A TEMPLATE, for the reason MqttStreamTlsListener is one: on a board the
// channel is a WiFiClient; in the test suite it is a socket shaped like one,
// so the handshake, every refusal and the data path are proven off the board.
//
// Usage, in a sketch:
//
//     WiFiClient wifi;
//     MqttChannelTlsTransport<WiFiClient> tls(wifi, "broker.example.com", 8883);
//     tls.setCaCertificate(MQTT_CA_CERT_PEM);       // or pin, or insecure
//     client.begin(tls, connection);

#include "../MqttFeatures.h"
#include "../MqttTransport.h"
#include "MqttByteChannel.h"
#include "MqttChannelTls.h"
#include "MqttStreamTransport.h"   // mq_stream: a channel's connect timeout, its room to write

#if MQTT_CHANNEL_TLS

template <typename ChannelType>
class MqttChannelTlsTransport : public MqttTransport, public MqttByteChannel {
public:
    // The channel is the caller's and must outlive this transport.
    explicit MqttChannelTlsTransport(ChannelType& channel) : _channel(channel) {}
    MqttChannelTlsTransport(ChannelType& channel, const char* host, uint16_t port)
        : _channel(channel), _host(host), _port(port) {}

    ~MqttChannelTlsTransport() override { _session.end(); }

    MqttChannelTlsTransport(const MqttChannelTlsTransport&) = delete;
    MqttChannelTlsTransport& operator=(const MqttChannelTlsTransport&) = delete;

    // Where to dial. The host is kept as a POINTER, as MqttStreamTransport
    // keeps it, and is also the name the certificate must carry.
    void configure(const char* host, uint16_t port) { _host = host; _port = port; }
    // MqttStreamTransport's spelling, so MqttOwnedTransport can retarget
    // either.
    void target(const char* host, uint16_t port) { configure(host, port); }
    const char* host() const { return _host; }
    uint16_t port() const { return _port; }

    // --- What to trust: MqttTlsTransport's setters ---
    //
    // Every PEM is KEPT AS A POINTER and must outlive the transport. On a
    // board it is a literal in flash (MQTT_CA_CERT_PEM), and a copy would
    // spend RAM holding what flash already holds.

    // One or more PEM certificates. Validation is the default and refuses to
    // connect without one: a board has no system store to fall back on.
    void setCaCertificate(const char* pem) { _trust.caPem = pem; }

    // Mutual TLS: this device's certificate and key, PEM, both or neither.
    void setClientCertificate(const char* certificatePem, const char* keyPem) {
        _trust.certificatePem = certificatePem;
        _trust.keyPem = keyPem;
    }

    // Named at length on purpose - see MqttTlsTransport.h.
    void setInsecureNoCertificateValidation() { _trust.validate = false; }

    // Trust one key instead of an authority. 64 hexadecimal characters;
    // colons, spaces and capitals are tolerated. Empty stops pinning. Takes
    // effect at the next begin(), which every reconnect is - so a key learnt
    // on first use is enforced from the next connection on. Copied.
    void setPinnedKeyFingerprint(const char* fingerprint) {
        size_t used = 0;
        if (fingerprint != nullptr) {
            while (fingerprint[used] != '\0' && used < sizeof _pin - 1) {
                _pin[used] = fingerprint[used];
                used++;
            }
        }
        _pin[used] = '\0';
        _trust.pin = _pin;
    }

    // What the broker presented, once a handshake has completed; empty
    // before. How a pinned link is commissioned (BROKER_KEY, TRUST_NEW).
    const char* peerKeyFingerprint() const { return _session.peerKeyFingerprint(); }
    bool certificateVerified() const { return _session.certificateVerified(); }
    const char* lastError() const { return _session.lastError(); }
    MqttTlsError lastErrorCode() const { return _session.lastErrorCode(); }
    int lastEngineCode() const { return _session.lastEngineCode(); }

    // --- MqttTransport ---

    bool begin() override {
        _session.end();
        // The session first: a configuration that can never connect - no CA
        // to validate against, a certificate without its key - is refused
        // with its reason before anything is dialled, not after a modem has
        // spent seconds opening a socket for it.
        if (!_session.begin(_trust, _host, this)) {
            _channel.stop();
            return false;
        }
        // Bounded by the caller's connect timeout where the channel can be
        // told one - see MqttStreamTransport::begin().
        if (_port == 0 ||
            (!_channel.connected() &&
             mq_stream::open(_channel, _host, _port, _within,
                             std::integral_constant<bool, mq_stream::IsTinyGsm<ChannelType>::value>()) != 1)) {
            _session.unreachable(_host, _port);
            return false;
        }
        _session.pump();   // ClientHello now, not a scan later
        return true;
    }

    void end() override {
        _session.end();
        _channel.stop();
    }

    void connectWithin(uint32_t milliseconds) override { _within = milliseconds; }

    bool isConnected() const override {
        if (_session.pump() != 1) return false;
        return const_cast<ChannelType&>(_channel).connected() != 0 ||
               _session.pending() > 0;
    }

    // A refused certificate is a verdict, not a "not yet": MqttClient closes
    // at once instead of waiting out its connect timeout.
    bool connectFailed() const override { return _session.failed(); }

    size_t readable() override {
        if (_session.pump() != 1) return 0;
        const size_t waiting = _session.pending();
        if (waiting > 0) return waiting;
        // Encrypted bytes are waiting; whether they make a whole record is
        // the engine's to say, and read() returns 0 if they do not yet.
        return _channel.available() > 0 ? 1u : 0u;
    }

    size_t read(uint8_t* into, size_t length) override {
        const int got = _session.read(into, length);
        return got > 0 ? (size_t)got : 0;
    }

    size_t write(const uint8_t* buffer, size_t length) override {
        const int sent = _session.write(buffer, length);
        return sent > 0 ? (size_t)sent : 0;
    }

    bool hasBufferedInput() const override { return _session.pending() > 0; }

    bool peerClosed() const override {
        if (_session.failed()) return true;
        ChannelType& channel = const_cast<ChannelType&>(_channel);
        return channel.connected() == 0 && channel.available() == 0 &&
               _session.pending() == 0;
    }

    // --- MqttByteChannel: where the engine's records go ---

    // NEVER INTO A FULL SOCKET, and a chunk at a time - for the reason
    // MqttStreamTransport::write() gives: an ESP32 channel's write() waits up
    // to ten seconds for room, inside poll(). "Nothing taken" is the engine's
    // WANT_WRITE, and it resends the rest of the record on a later poll.
    int channelSend(const uint8_t* data, int length) override {
        if (!mq_stream::hasRoom(_channel,
                                std::integral_constant<bool, mq_stream::HasSocket<ChannelType>::value>())) {
            return _channel.connected() ? 0 : -1;
        }
        if (length > MQTT_STREAM_WRITE_CHUNK) length = MQTT_STREAM_WRITE_CHUNK;
        const int sent = (int)_channel.write(data, (size_t)length);
        if (sent > 0) return sent;
        return _channel.connected() ? 0 : -1;
    }

    int channelRecv(uint8_t* into, int length) override {
        // Client::read(buf, n) after available(), never readBytes(): see
        // MqttStreamTransport::read() for why the blocking one is not usable.
        const int have = _channel.available();
        if (have <= 0) return _channel.connected() ? 0 : -1;
        const int want = have < length ? have : length;
        const int got = (int)_channel.read(into, (size_t)want);
        if (got > 0) return got;
        return _channel.connected() ? 0 : -1;
    }

    bool channelOpen() const override {
        return const_cast<ChannelType&>(_channel).connected() != 0;
    }

private:
    ChannelType& _channel;
    const char* _host = nullptr;
    uint16_t _port = 0;
    uint32_t _within = 0;      // connectWithin(); 0 is the channel's own default
    MqttChannelTlsSession::Trust _trust;
    char _pin[160] = {0};
    // Mutable because the handshake advances from isConnected(), which the
    // interface makes const; MqttTlsTransport does the same with a cast.
    mutable MqttChannelTlsSession _session;
};

#endif  // MQTT_CHANNEL_TLS

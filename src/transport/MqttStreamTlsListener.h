// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// A BOARD SERVING TLS.
//
// MqttBroker has never been host-only - it carries no platform guard and runs
// on an ESP32 today. What kept a board from being a TLS broker was this class
// not existing: MqttTlsListener accepts BSD sockets, and a board has none.
//
// Everything else was already there and already neutral:
//
//   the engine          wolfSSL, vendored, on every Arduino architecture
//   the certificate     MqttIdentity mints a self-signed EC P-256 one with
//                       wc_MakeCert, on the board, on first start
//   the framing         MqttLink, which does not know what it is reading from
//
// So this is the accept half and nothing more: take a connection from an
// Arduino server, wrap it in a TLS session, and hand MqttBroker a transport.
//
// A TEMPLATE, AND THAT IS WHY IT IS TESTED. On a board you pass a WiFiServer
// and a WiFiClient, or an EthernetServer and an EthernetClient - but they are
// only required to have the handful of methods used below, so the same class
// instantiates on a host against a test double and the TLS serving path is
// exercised in CI rather than only on hardware nobody can run there.
//
// `AcceptorType` rather than `ServerType` because R1 - see
// scripts/check-layering.py - forbids a layer this far down from naming a
// role, and "server" is a role word even when it means a listening socket.
// The rule is worth more than the familiar name.

#include "../MqttFeatures.h"
#include "../MqttListener.h"
#include "MqttByteChannel.h"
#include "MqttAcceptorCall.h"
#include "MqttTlsAccept.h"

// NOTE WHAT IS NOT INCLUDED: MqttTlsSupport.h, and so no engine types.
//
// wolfSSL's OpenSSL compatibility layer defines SHA_CTX and so does the ESP32
// ROM, through WiFiClientSecure.h. A public header that pulls the engine in
// therefore breaks every ESP32 sketch that also speaks TLS as a client, with a
// conflicting-typedef error pointing at neither. The ESP32 toolchain caught
// exactly that when this class first included MqttTlsSupport.h directly.
//
// The engine lives behind MqttTlsAccept.cpp; this header sees only void* and
// two small classes.

#if MQTT_TLS_AVAILABLE

#include <string>
#include <vector>

// One accepted connection: an Arduino channel with a TLS session over it.
template <typename ChannelType>
class MqttStreamTlsTransport : public MqttTransport, public MqttByteChannel {
public:
    ~MqttStreamTlsTransport() override { release(); }

    bool adopt(const ChannelType& channel, MqttTlsAcceptContext& context) {
        _channel = channel;
        _live = true;
        if (!_session.begin(context, this)) { release(); return false; }
        return true;
    }

    bool inUse() const { return _live; }

    void release() {
        _session.end();
        if (_live) _channel.stop();
        _live = false;
    }

    // --- MqttTransport ---

    bool begin() override { return _live; }
    void end() override { release(); }

    // THE HAZARD THIS CLASS MUST NOT WALK INTO.
    //
    // MqttLink::peerGone() is `peerClosed() || !isConnected()`, and
    // MqttBroker::serviceConnection turns that straight into a close WITH THE
    // WILL PUBLISHED. A transport reporting itself disconnected while its
    // handshake was still running would be torn down on its first poll - and
    // would publish a Will for a session that never connected. So this stays
    // true from the moment the channel is adopted until the session fails.
    bool isConnected() const override { return _live && !_session.failed(); }
    bool peerClosed() const override { return !_live || _session.failed(); }

    size_t readable() override {
        const size_t waiting = _session.pending();
        if (waiting > 0) return waiting;
        return _channel.available() > 0 ? 1u : 0u;
    }

    size_t read(uint8_t* into, size_t length) override {
        return _session.read(into, length);
    }

    size_t write(const uint8_t* buffer, size_t length) override {
        return _session.write(buffer, length);
    }

    bool hasBufferedInput() const override { return _session.pending() > 0; }

    // --- MqttByteChannel: how the engine reaches the medium ---

    int channelSend(const uint8_t* data, int length) override {
        if (!_live) return -1;
        const int sent = (int)_channel.write(data, (size_t)length);
        if (sent > 0) return sent;
        return _channel.connected() ? 0 : -1;
    }

    int channelRecv(uint8_t* into, int length) override {
        if (!_live) return -1;
        const int available = _channel.available();
        if (available <= 0) return _channel.connected() ? 0 : -1;
        const int want = available < length ? available : length;
        const int got = (int)_channel.read(into, (size_t)want);
        if (got > 0) return got;
        return _channel.connected() ? 0 : -1;
    }

    bool channelOpen() const override { return _live; }

private:
    ChannelType _channel;
    MqttTlsAcceptedSession _session;
    bool _live = false;
};

// The listener: an Arduino acceptor, a TLS context, and a fixed pool.
template <typename AcceptorType, typename ChannelType>
class MqttStreamTlsListener : public MqttListener {
public:
    explicit MqttStreamTlsListener(AcceptorType& acceptor) : _acceptor(acceptor) {}

    ~MqttStreamTlsListener() override {
        end();
        for (MqttStreamTlsTransport<ChannelType>* entry : _pool) delete entry;
        _pool.clear();
    }

    // Sized once, like everything else. One more than asked for: the extra
    // never serves, it is where a connection arriving at a full broker is
    // accepted and told 0x89 Server Busy rather than dropped without a word.
    void configure(size_t maximum = 4) {
        if (maximum == 0) maximum = 1;
        for (MqttStreamTlsTransport<ChannelType>* entry : _pool) delete entry;
        _pool.clear();
        _pool.reserve(maximum + 1);
        for (size_t index = 0; index < maximum + 1; index++) {
            _pool.push_back(new MqttStreamTlsTransport<ChannelType>());
        }
    }

    // The certificate this broker serves, generated on the board if it has
    // none. MqttIdentity uses wc_MakeCert, which is the whole reason wolfSSL
    // is the engine here: BearSSL can read an X.509 and cannot write one.
    MqttIdentity& identity() { return _context.identity(); }

    // What a client pins, and what an engineer reads off the panel: 64
    // lowercase hex characters over the DER SubjectPublicKeyInfo.
    const std::string& fingerprint() const { return _context.fingerprint(); }

    bool begin() override {
        if (_pool.empty()) configure();
        if (!_context.begin()) { _lastError = _context.lastError(); return false; }
        _acceptor.begin();
        _listening = true;
        return true;
    }

    void end() override {
        for (MqttStreamTlsTransport<ChannelType>* entry : _pool) entry->release();
        _context.end();
        _listening = false;
    }

    bool isListening() const override { return _listening; }

    MqttTransport* accept() override {
        if (!_listening || !_context.ready()) return nullptr;

        MqttStreamTlsTransport<ChannelType>* slot = nullptr;
        for (MqttStreamTlsTransport<ChannelType>* entry : _pool) {
            if (!entry->inUse()) { slot = entry; break; }
        }
        if (slot == nullptr) return nullptr;

        ChannelType channel = mq_acceptor::accept(_acceptor);
        if (!channel) return nullptr;

        if (!slot->adopt(channel, _context)) return nullptr;
        return slot;
    }

    void release(MqttTransport* transport) override {
        for (MqttStreamTlsTransport<ChannelType>* entry : _pool) {
            if (entry == transport) { entry->release(); return; }
        }
    }

    size_t capacityRemaining() const override {
        size_t free = 0;
        for (const MqttStreamTlsTransport<ChannelType>* entry : _pool) {
            if (!entry->inUse()) free++;
        }
        return free > 0 ? free - 1 : 0;   // the reserve is not capacity
    }

    const std::string& lastError() const { return _lastError; }

private:
    AcceptorType& _acceptor;
    MqttTlsAcceptContext _context;
    std::vector<MqttStreamTlsTransport<ChannelType>*> _pool;
    bool _listening = false;
    std::string _lastError;
};

#endif  // MQTT_TLS_AVAILABLE

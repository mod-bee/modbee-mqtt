// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// A BOARD SERVING PLAIN MQTT.
//
// This is MqttStreamTlsListener's unencrypted sibling, and it was written
// second, which was the wrong order. A board could serve TLS on 8883 before it
// could serve clear text on 1883 - because the TLS listener was the one
// somebody asked for, and the easy half was never noticed missing. A panel
// broker on an isolated line, talking to twenty sensors that have no
// certificate store between them, is the ordinary case.
//
// Nothing here is new work. The framing is MqttLink's, the routing is
// MqttBroker's, and neither knows what it is reading from. This is the accept
// half: take a connection from an Arduino server and hand MqttBroker a
// transport over it.
//
// A TEMPLATE, AND THAT IS WHY IT IS TESTED. On a board you pass a WiFiServer
// and a WiFiClient, or an EthernetServer and an EthernetClient - but they are
// only required to have the handful of methods used below, so the same class
// instantiates on a host against a test double and the accept path is
// exercised in CI rather than only on hardware nobody can run there.
//
// `AcceptorType` rather than `ServerType` because R1 forbids a layer this far
// down from naming a role, and "server" is a role word even when it means a
// listening socket.
//
// NO ENGINE, NO GUARD. Unlike its TLS sibling this file needs nothing
// installed, so it is not wrapped in MQTT_TLS_AVAILABLE and a board that has
// no TLS at all can still be a broker.

#include "../MqttListener.h"
#include "../MqttTransport.h"
#include "MqttAcceptorCall.h"

#include <vector>

// One accepted connection: an Arduino channel, held by value because the
// acceptor gave it to us and nobody else owns it.
template <typename ChannelType>
class MqttStreamAcceptedTransport : public MqttTransport {
public:
    bool adopt(const ChannelType& channel) {
        _channel = channel;
        _live = true;
        return true;
    }

    bool inUse() const { return _live; }

    void release() {
        if (_live) _channel.stop();
        _live = false;
    }

    // --- MqttTransport ---

    bool begin() override { return _live; }
    void end() override { release(); }

    // A CHANNEL WITH BYTES LEFT IS NOT GONE, and this is the difference
    // between this class and a naive `return _channel.connected()`.
    //
    // Arduino cores report connected() false as soon as the far end sends FIN,
    // while the bytes it sent before that are still in the channel's own
    // buffer. MqttBroker turns peerGone() into a close with the Will
    // published, so a transport that forgot the buffered bytes would discard a
    // clean DISCONNECT that had already arrived and publish a Will for a
    // client that said goodbye properly.
    bool isConnected() const override {
        if (!_live) return false;
        return _channel.connected() || _channel.available() > 0;
    }

    bool peerClosed() const override {
        if (!_live) return true;
        return !_channel.connected() && _channel.available() <= 0;
    }

    size_t readable() override {
        if (!_live) return 0;
        const int waiting = _channel.available();
        return waiting > 0 ? (size_t)waiting : 0u;
    }

    // Client::read(buf, size) takes only what has already arrived.
    // Stream::readBytes() BLOCKS up to _timeout - one second by default - and
    // is never used anywhere in this library.
    size_t read(uint8_t* into, size_t length) override {
        if (!_live || into == nullptr || length == 0) return 0;
        const int waiting = _channel.available();
        if (waiting <= 0) return 0;
        const size_t want = (size_t)waiting < length ? (size_t)waiting : length;
        const int got = _channel.read(into, want);
        return got > 0 ? (size_t)got : 0u;
    }

    size_t write(const uint8_t* buffer, size_t length) override {
        if (!_live || buffer == nullptr || length == 0) return 0;
        return _channel.write(buffer, length);
    }

    bool hasBufferedInput() const override {
        return _live && _channel.available() > 0;
    }

private:
    // MUTABLE, AND THE REASON IS ARDUINO'S OWN SIGNATURES.
    //
    // Client::available() and Client::connected() are NOT const - they may
    // service the stack and refill a buffer - while MqttTransport's
    // isConnected(), peerClosed() and hasBufferedInput() are, because asking a
    // transport a question must not be a way to change one. Both are right for
    // their own side, so the mismatch is absorbed here, once, rather than by
    // dropping const from the interface every implementation has to honour.
    //
    // The fake channel in test/test_stream_listener.cpp declares those two
    // methods non-const for exactly this reason: a fake that is tidier than
    // the real thing is a test that cannot fail the way a board does. This
    // sketch-level error was found by the Arduino shim and not by CI, the
    // first time round.
    mutable ChannelType _channel;
    bool _live = false;
};

// The listener: an Arduino acceptor and a fixed pool.
template <typename AcceptorType, typename ChannelType>
class MqttStreamListener : public MqttListener {
public:
    explicit MqttStreamListener(AcceptorType& acceptor) : _acceptor(acceptor) {}

    ~MqttStreamListener() override {
        end();
        for (MqttStreamAcceptedTransport<ChannelType>* entry : _pool) delete entry;
        _pool.clear();
    }

    // Sized once, like everything else. One more than asked for: the extra
    // never serves, it is where a connection arriving at a full broker is
    // accepted and told 0x89 Server Busy rather than dropped without a word.
    void configure(size_t maximum = 4) {
        if (maximum == 0) maximum = 1;
        for (MqttStreamAcceptedTransport<ChannelType>* entry : _pool) delete entry;
        _pool.clear();
        _pool.reserve(maximum + 1);
        for (size_t index = 0; index < maximum + 1; index++) {
            _pool.push_back(new MqttStreamAcceptedTransport<ChannelType>());
        }
    }

    bool begin() override {
        if (_pool.empty()) configure();
        _acceptor.begin();
        _listening = true;
        return true;
    }

    void end() override {
        for (MqttStreamAcceptedTransport<ChannelType>* entry : _pool) entry->release();
        _listening = false;
    }

    bool isListening() const override { return _listening; }

    MqttTransport* accept() override {
        if (!_listening) return nullptr;

        MqttStreamAcceptedTransport<ChannelType>* slot = nullptr;
        for (MqttStreamAcceptedTransport<ChannelType>* entry : _pool) {
            if (!entry->inUse()) { slot = entry; break; }
        }
        // NO SLOT MEANS NO accept(), deliberately. The channel stays queued in
        // the acceptor rather than being taken and dropped, which from the far
        // end looks exactly like a broker that crashed. The reserve slot above
        // is what turns the last one into a refusal with a reason instead.
        if (slot == nullptr) return nullptr;

        ChannelType channel = mq_acceptor::accept(_acceptor);
        if (!channel) return nullptr;

        if (!slot->adopt(channel)) return nullptr;
        return slot;
    }

    void release(MqttTransport* transport) override {
        for (MqttStreamAcceptedTransport<ChannelType>* entry : _pool) {
            if (entry == transport) { entry->release(); return; }
        }
    }

    size_t capacityRemaining() const override {
        size_t free = 0;
        for (const MqttStreamAcceptedTransport<ChannelType>* entry : _pool) {
            if (!entry->inUse()) free++;
        }
        return free > 0 ? free - 1 : 0;   // the reserve is not capacity
    }

private:
    AcceptorType& _acceptor;
    std::vector<MqttStreamAcceptedTransport<ChannelType>*> _pool;
    bool _listening = false;
};

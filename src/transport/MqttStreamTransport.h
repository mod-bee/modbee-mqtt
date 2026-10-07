// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// Arduino. Header-only, and compiles to nothing anywhere else.
//
// A template over the core's network channel type rather than a class per
// board, so WiFiClient, EthernetClient and WiFiClientSecure all work with the
// same code. That is also why TLS costs nothing extra here: a secure channel
// on Arduino IS an ordinary channel, so the same template covers it.
//
// The template parameter is named ChannelType rather than after the Arduino
// class, because from this library's side it is a byte channel and nothing
// more - which end of a conversation it is at is decided two layers up.

#include "../MqttTransport.h"

#if defined(MQTT_PLATFORM_ARDUINO)

#include <Arduino.h>

template <typename ChannelType>
class MqttStreamTransport : public MqttTransport {
public:
    // The channel is the caller's. It must outlive this transport, because the
    // sketch owns the WiFi stack and this does not.
    MqttStreamTransport(ChannelType& channel, const char* host, uint16_t port)
        : _channel(channel), _host(host), _port(port) {}

    // Without a target yet. MqttDevice builds the transport in its constructor
    // and learns the host later, at connect(), so there has to be a way to say
    // it afterwards.
    explicit MqttStreamTransport(ChannelType& channel)
        : _channel(channel), _host(nullptr), _port(0) {}

    // The host is kept as a POINTER, not a copy - so whatever it points at
    // must outlive this transport. A literal always does; a std::string that
    // went out of scope is a dangling pointer at the exact moment a reconnect
    // needs it, which is why MqttDevice owns the string it passes here.
    void target(const char* host, uint16_t port) {
        _host = host;
        _port = port;
    }

    const char* host() const { return _host; }
    uint16_t port() const { return _port; }

    // THIS IS THE ONE CALL IN THE LIBRARY THAT CAN STALL, AND IT IS NOT OURS.
    //
    // Arduino's Client::connect() is synchronous on every core that implements
    // it. On WiFi that is usually milliseconds; on Ethernet it is a DNS lookup
    // and a TCP handshake; on a cellular modem through TinyGSM it can be
    // SECONDS, because the AT command underneath it is.
    //
    // The library's "nothing blocks" guarantee is about the library. It cannot
    // make somebody else's network stack asynchronous, and pretending
    // otherwise would be worse than saying so.
    //
    // What it does instead is confine the stall to ONE function, called from
    // exactly two places: MqttClient::connect(), and a reconnect attempt
    // inside poll(). Nothing else in the library can stall - not publishing,
    // not receiving, not the keepalive.
    //
    // The reconnect one is the one that matters, because it is inside loop().
    // A scan cycle that cannot afford a cellular modem's connect time turns
    // auto-reconnect off and calls connect() where it can afford it:
    //
    //     mqtt.autoReconnect(false);      // and call connect() yourself
    //
    // docs/transports.md gives the measured shape of this per transport.
    bool begin() override {
        if (_channel.connected()) return true;
        if (_host == nullptr || _port == 0) return false;
        return _channel.connect(_host, _port) == 1;
    }

    void end() override { _channel.stop(); }

    size_t readable() override {
        const int have = _channel.available();
        return have > 0 ? (size_t)have : 0;
    }

    // Client::read(buf, size), NEVER Stream::readBytes().
    //
    // Arduino offers two bulk reads and only one of them is usable here:
    //
    //   Client::read(uint8_t*, size_t)  takes what has already arrived and
    //                                   returns immediately.
    //   Stream::readBytes(buf, len)     BLOCKS until it has `len` bytes or
    //                                   _timeout elapses - one second by
    //                                   default.
    //
    // Nothing in this library may block: MqttClient::poll() is called once per
    // PLC scan, and a one-second stall inside it is a scan cycle that missed
    // its deadline. Clamping to available() first means this cannot wait even
    // if some future ChannelType implements read() in terms of the blocking
    // one.
    //
    // This is also where Arduino's byte-wise Stream idiom now lives, and the
    // only place it does. It used to be the shape of MqttTransport itself, so
    // every host socket paid for it too.
    size_t read(uint8_t* into, size_t length) override {
        if (into == nullptr || length == 0) return 0;
        const int have = _channel.available();
        if (have <= 0) return 0;
        const size_t want = (size_t)have < length ? (size_t)have : length;
        const int got = _channel.read(into, want);
        return got > 0 ? (size_t)got : 0;
    }

    size_t write(const uint8_t* buffer, size_t length) override {
        return _channel.write(buffer, length);
    }

    bool isConnected() const override {
        // connected() is not const on the Arduino cores, and the constness
        // here is ours to keep rather than theirs to break.
        return const_cast<ChannelType&>(_channel).connected() != 0;
    }

    bool peerClosed() const override {
        // The Arduino cores report a closed peer by connected() going false
        // while bytes may still be buffered. Data still readable means the
        // peer went away after sending; no data means it simply went.
        ChannelType& channel = const_cast<ChannelType&>(_channel);
        return channel.connected() == 0 && channel.available() == 0;
    }

private:
    ChannelType& _channel;
    const char* _host;
    uint16_t _port;
};

#endif // MQTT_PLATFORM_ARDUINO

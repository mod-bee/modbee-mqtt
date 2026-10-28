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

#include <type_traits>
#include <utility>

#if defined(ARDUINO_ARCH_ESP32)
// select(): whether a channel's socket has room. The C library's, which
// ESP-IDF routes to lwIP for a socket descriptor - not lwIP's own header,
// whose IPADDR_* macros collide with names already in scope by then.
#include <sys/select.h>
#include <sys/time.h>
#endif

// HOW LONG ONE POLL MAY SPEND WRITING, and the most handed to the channel in
// one call. See write() below: an Arduino channel's write() blocks for as long
// as it takes, so these are what bound the time poll() can spend inside it.
#ifndef MQTT_STREAM_WRITE_BUDGET_MS
#define MQTT_STREAM_WRITE_BUDGET_MS 20
#endif
#ifndef MQTT_STREAM_WRITE_CHUNK
#define MQTT_STREAM_WRITE_CHUNK 1024
#endif

// =============================================================================
// WHAT A CHANNEL'S connect() CAN BE TOLD ABOUT TIME
// =============================================================================
//
// Arduino's Client::connect(host, port) takes no timeout, and each channel
// waits as long as its own default says. Two of the common ones can be told,
// and are found here by what they offer rather than by name, so a sketch that
// never includes them pays nothing:
//
//   TinyGSM      connect(host, port, timeout_s) - SECONDS, default 75. Found by
//                TinyGsmStringFromIp, a static every TinyGSM client inherits.
//   ESP32 TLS    setHandshakeTimeout(seconds) on NetworkClientSecure, default
//                120 - the handshake, after its TCP connect.
//
// Both are only ever SHORTENED to the caller's connect timeout, never
// lengthened. The ESP32's plain TCP connect is left alone on purpose: its
// default is 3 seconds, already inside any budget, and setConnectionTimeout()
// also sets the socket's read and write timeouts.
namespace mq_stream {

template <typename T>
struct IsTinyGsm {
    template <typename U> static auto probe(int) -> decltype(&U::TinyGsmStringFromIp, std::true_type());
    template <typename U> static std::false_type probe(...);
    static const bool value = decltype(probe<T>(0))::value;
};

template <typename T>
struct HasHandshakeTimeout {
    template <typename U>
    static auto probe(int) -> decltype(std::declval<U&>().setHandshakeTimeout((unsigned long)1),
                                       std::true_type());
    template <typename U> static std::false_type probe(...);
    static const bool value = decltype(probe<T>(0))::value;
};

// Whole seconds, at least one: zero means "no timeout" to some channels.
inline unsigned long secondsWithin(uint32_t milliseconds, unsigned long ceiling) {
    unsigned long seconds = (unsigned long)((milliseconds + 999u) / 1000u);
    if (seconds == 0) seconds = 1;
    return seconds < ceiling ? seconds : ceiling;
}

template <typename ChannelType>
int open(ChannelType& channel, const char* host, uint16_t port, uint32_t within,
         std::true_type /* TinyGSM */) {
    if (within == 0) return channel.connect(host, port);
    return channel.connect(host, port, (int)secondsWithin(within, 75));
}

template <typename ChannelType>
void limitHandshake(ChannelType& channel, uint32_t within, std::true_type) {
    if (within != 0) channel.setHandshakeTimeout(secondsWithin(within, 120));
}
template <typename ChannelType>
void limitHandshake(ChannelType&, uint32_t, std::false_type) {}

template <typename ChannelType>
int open(ChannelType& channel, const char* host, uint16_t port, uint32_t within,
         std::false_type) {
    limitHandshake(channel, within,
                   std::integral_constant<bool, HasHandshakeTimeout<ChannelType>::value>());
    return channel.connect(host, port);
}

// A socket underneath the channel - the ESP32 core's NetworkClient and
// NetworkClientSecure both say which, through fd().
template <typename T>
struct HasSocket {
    template <typename U> static auto probe(int) -> decltype(std::declval<const U&>().fd(), std::true_type());
    template <typename U> static std::false_type probe(...);
    static const bool value = decltype(probe<T>(0))::value;
};

// Whether a write now would be taken without waiting. Only a channel with a
// socket can be asked; any other is assumed to have room, as before.
template <typename ChannelType>
bool hasRoom(const ChannelType& channel, std::true_type) {
#if defined(ARDUINO_ARCH_ESP32)
    const int handle = channel.fd();
    if (handle < 0) return true;           // closed: its own write() says so
    fd_set writable;
    FD_ZERO(&writable);
    FD_SET(handle, &writable);
    struct timeval now = {0, 0};
    return select(handle + 1, nullptr, &writable, nullptr, &now) > 0;
#else
    (void)channel;
    return true;
#endif
}
template <typename ChannelType>
bool hasRoom(const ChannelType&, std::false_type) { return true; }

} // namespace mq_stream

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
    // On an ESP32 there is a better answer, and it is now the default: the
    // channel MqttDevice and the Sparkplug classes are given is wrapped in an
    // MqttConnectTask (MqttConnectTask.h), so this call runs on a short task
    // of its own while the client polls on. MQTT_CLIENT does the same.
    //
    // AND IT IS BOUNDED BY THE CALLER'S CONNECT TIMEOUT where the channel can
    // be told one (connectWithin, mq_stream above): TinyGSM's own default is
    // 75 seconds, which outlived the client's 20 and held the modem - so the
    // next attempt could not start - for most of a minute after the client
    // had given up on this one.
    //
    // docs/transports.md gives the measured shape of this per transport.
    bool begin() override {
        if (_channel.connected()) return true;
        if (_host == nullptr || _port == 0) return false;
        return mq_stream::open(_channel, _host, _port, _within,
                               std::integral_constant<bool, mq_stream::IsTinyGsm<ChannelType>::value>()) == 1;
    }

    void connectWithin(uint32_t milliseconds) override { _within = milliseconds; }

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

    // BOUNDED, because the channel's write() is not.
    //
    // An Arduino channel's write() returns when it has taken everything or
    // given up, and on a link that has stopped moving it takes a long time to
    // give up: the ESP32 core's NetworkClient waits up to ten seconds for room
    // (ten one-second selects), its TLS client until the socket timeout, and
    // a TinyGSM modem holds the call for the whole AT exchange of however
    // many bytes it was handed. The link drains its queue by calling write()
    // until it is refused - so one dead Wi-Fi link stopped poll() for ten
    // seconds at a time, and one 200 kB publish through a modem stopped it for
    // the whole transfer.
    //
    // So: never more than MQTT_STREAM_WRITE_CHUNK per call; nothing at all
    // when the channel's socket has no room (ESP32 - a socket with room takes
    // a chunk at once); and once a poll has spent MQTT_STREAM_WRITE_BUDGET_MS
    // writing, a refusal, so the rest goes on the next poll. The link already
    // resumes from where a refused write stopped, so nothing above notices
    // except that loop() keeps its pace.
    size_t write(const uint8_t* buffer, size_t length) override {
        if (buffer == nullptr || length == 0) return 0;
        const mq_time_t now = mq_millis();
        // A write more than a millisecond after the last one, or the first
        // after a refusal, starts a new poll's allowance.
        if (_refused || mq_elapsed(_lastWriteAt, now) > 1) {
            _refused = false;
            _writingSince = now;
        } else if (mq_elapsed(_writingSince, now) >= (mq_time_t)MQTT_STREAM_WRITE_BUDGET_MS) {
            _refused = true;
            return 0;
        }
        if (!mq_stream::hasRoom(_channel,
                                std::integral_constant<bool, mq_stream::HasSocket<ChannelType>::value>())) {
            _refused = true;
            return 0;
        }
        if (length > (size_t)MQTT_STREAM_WRITE_CHUNK) length = (size_t)MQTT_STREAM_WRITE_CHUNK;
        const size_t sent = _channel.write(buffer, length);
        _lastWriteAt = mq_millis();
        return sent;
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
    uint32_t _within = 0;       // connectWithin(); 0 is the channel's own default

    // write()'s allowance for the poll it is in.
    mq_time_t _writingSince = 0;
    mq_time_t _lastWriteAt = 0;
    bool _refused = false;
};

#endif // MQTT_PLATFORM_ARDUINO

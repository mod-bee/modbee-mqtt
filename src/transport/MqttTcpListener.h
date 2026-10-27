// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// Accepting TCP connections on a host: POSIX or Win32. Compiles to nothing on
// Arduino, where a board listens through its own server class instead.
//
// A board that will never run a broker should not carry a listening socket, so
// this file is self-guarding rather than only excluded by CMake - the Arduino
// IDE compiles every .cpp under src/ and has no way to leave one out.
//
// Non-blocking throughout. accept() returns null when nobody is waiting, which
// is nearly always.

#include "../MqttFeatures.h"
#include "MqttSocketSupport.h"   // MQTT_SOCKET_READ_SIZE, and the socket shim
#include "../MqttListener.h"

#if defined(MQTT_PLATFORM_HOST)

#include <string>
#include <unordered_map>
#include <vector>

// One accepted connection. An MqttTransport like any other - the layers above
// cannot tell it from a transport that dialled out, which is the point.
class MqttAcceptedTransport : public MqttTransport {
public:
    MqttAcceptedTransport() = default;
    ~MqttAcceptedTransport() override;

    MqttAcceptedTransport(const MqttAcceptedTransport&) = delete;
    MqttAcceptedTransport& operator=(const MqttAcceptedTransport&) = delete;

    // Takes ownership of an already-accepted descriptor.
    void adopt(int socket, const char* peerName);

    bool begin() override;
    void end() override;
    size_t readable() override;
    size_t read(uint8_t* into, size_t length) override;
    size_t write(const uint8_t* buffer, size_t length) override;
    bool isConnected() const override;
    int waitHandle() const override { return _socket; }
    bool hasBufferedInput() const override { return _receiveHead < _receiveTail; }
    bool peerClosed() const override;

    // Where the connection came from, for a log line and for an
    // authorisation hook that wants to know. "203.0.113.7:51544".
    const char* peerName() const override { return _peerName.c_str(); }

    bool inUse() const { return _socket >= 0; }

private:
    int _socket = -1;
    bool _peerClosed = false;
    std::string _peerName;

    // A receive buffer, for callers that ask readable() or read a byte at a
    // time - a syscall per byte would make a 200 kB payload 200,000 of them.
    // See MQTT_SOCKET_READ_SIZE in MqttSocketSupport.h - one definition
    // for all four transports, which is why this is not a number.
    //
    // ALLOCATED ON FIRST USE, and the broker never uses it. MqttLink reads in
    // bulk, straight into its own buffer, so for every connection a broker
    // serves this was 4 kB copied through and never needed - 4 GB at a
    // million connections. read() now recv()s
    // directly into the caller's buffer whenever nothing is held here.
    static const size_t kReceiveBufferSize = MQTT_SOCKET_READ_SIZE;
    std::vector<uint8_t> _receive;
    size_t _receiveHead = 0;
    size_t _receiveTail = 0;

    void closeSocket();
    bool fill();
};

class MqttTcpListener : public MqttListener {
public:
    MqttTcpListener() = default;
    ~MqttTcpListener() override;

    MqttTcpListener(const MqttTcpListener&) = delete;
    MqttTcpListener& operator=(const MqttTcpListener&) = delete;

    // `address` may be null or empty for every interface. `maximum` is how
    // many connections this listener will hold at once - a fixed pool, sized
    // once, because a broker on a gateway with 320 kB of RAM must fail to
    // accept the 33rd connection rather than fail to allocate during the 33rd
    // connection.
    void configure(const char* address, uint16_t port, size_t maximum = 32);

    // Several listeners on the same port, one per broker of a multi-core
    // server, with the KERNEL spreading arrivals between them (SO_REUSEPORT,
    // Linux). Set before begin(). Where the option does not exist the second
    // begin() on the port simply fails, and says so.
    void setReusePort(bool reuse) { _reusePort = reuse; }

    bool begin() override;
    void end() override;

    // Accepts on `socket` from now on (mq_socket::openListeningSocket), in
    // place of the listening socket it had - the port changed while the
    // server runs. The connections already handed out are untouched. -1 stops
    // accepting, as end() does. A pool never configured is sized `maximum`.
    void useSocket(int socket, uint16_t port, size_t maximum);

    // Holds `maximum` connections from now on, if that is more than it was
    // configured for: the broker's limit was raised. Never shrinks.
    void grow(size_t maximum);

    MqttTransport* accept() override;
    void release(MqttTransport* transport) override;
    bool isListening() const override { return _socket >= 0; }
    int waitHandle() const override { return _socket; }
    size_t capacityRemaining() const override;

    uint16_t port() const { return _port; }

    // The port actually bound, which differs from the one asked for whenever
    // port 0 was requested - the usual way a test gets a free port without
    // racing another test for a fixed one.
    uint16_t boundPort() const { return _boundPort; }

    const std::string& lastError() const { return _lastError; }

    // Connections handed out and not yet released.
    size_t active() const;

private:
    std::string _address;
    uint16_t _port = 0;
    uint16_t _boundPort = 0;
    bool _reusePort = false;
    size_t _servingCapacity = 0;
    int _socket = -1;
    int _spare = -1;            // given up to refuse a connection when descriptors run out
    std::string _lastError;

    // Deque of pointers rather than a vector of objects: the transports are
    // handed out by address and must not move when the pool is touched.
    std::vector<MqttAcceptedTransport*> _pool;

    // Free pool entries, lowest on top, and each entry's place in the pool -
    // what accept() and release() used to find by walking it, once per
    // connection, which at 200,000 connections was most of a broker's poll.
    // Both built in configure() and never grown.
    std::vector<uint32_t> _free;
    std::vector<uint8_t> _isFree;
    std::unordered_map<const MqttTransport*, uint32_t> _poolIndex;
    void giveBack(uint32_t index);

    void fail(const std::string& what);
};

#endif // MQTT_PLATFORM_HOST

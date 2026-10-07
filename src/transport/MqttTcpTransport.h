// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// TCP on a host: POSIX or Win32. Compiles to nothing on Arduino.
//
// Non-blocking throughout, including the connect. A DNS lookup and a TCP
// handshake both take time nobody can bound, and a library that blocked on
// either would stall a scan cycle at exactly the moment a plant is least able
// to tolerate it - the moment the network came back.

#include "../MqttTransport.h"
#include "MqttSocketSupport.h"   // mq_socket::AddressList

#if defined(MQTT_PLATFORM_HOST)

#include <string>
#include <vector>

class MqttTcpTransport : public MqttTransport {
public:
    MqttTcpTransport() = default;
    ~MqttTcpTransport() override;

    // Non-copyable: it owns a socket, and two objects closing one descriptor
    // is a bug that shows up as somebody else's connection dropping.
    MqttTcpTransport(const MqttTcpTransport&) = delete;
    MqttTcpTransport& operator=(const MqttTcpTransport&) = delete;

    void configure(const char* host, uint16_t port);

    // Dial from this numeric source address. For a load generator spreading
    // connections over several addresses; nothing else needs it. Takes effect
    // at the next begin().
    void setLocalAddress(const char* address) { _addresses.setLocalAddress(address); }

    // Starts the connection and returns. Whether it completed is what
    // isConnected() answers on a later poll.
    bool begin() override;
    void end() override;

    size_t readable() override;
    size_t read(uint8_t* into, size_t length) override;
    size_t write(const uint8_t* buffer, size_t length) override;
    bool isConnected() const override;
    int waitHandle() const override { return _socket; }
    bool hasBufferedInput() const override { return _receiveHead < _receiveTail; }
    bool peerClosed() const override;
    bool connectFailed() const override { return _failed; }

    // Why begin() returned false, or why the link dropped. In an operator's
    // words rather than an errno, because the person reading it is usually
    // standing in a plant room rather than sitting with the source.
    const std::string& lastError() const { return _lastError; }

private:
    std::string _host;
    uint16_t _port = 0;
    int _socket = -1;

    // A non-blocking connect() returns EINPROGRESS and finishes later, so
    // "have we connected" is a question with three answers and this is the
    // third one.
    bool _connecting = false;
    bool _connected = false;
    bool _failed = false;       // the attempt begin() started is over, unconnected
    bool _peerClosed = false;

    std::string _lastError;

    // A receive buffer, because the interface above is byte-at-a-time and a
    // syscall per byte would make a 200 kB payload 200,000 of them. One recv
    // fills this and read() serves from it.
    //
    // It is also how a closed peer is detected: recv returning zero on a
    // readable socket is the only signal, and that discovery cannot be undone,
    // so there has to be somewhere to keep what came with it.
    // See MQTT_SOCKET_READ_SIZE in MqttSocketSupport.h - one definition
    // for all four transports, which is why this is not a number.
    static const size_t kReceiveBufferSize = MQTT_SOCKET_READ_SIZE;
    // ALLOCATED ON FIRST USE, like the accepted side's (MqttTcpListener.h):
    // MqttLink reads in bulk straight into its own buffer, so a client never
    // needed this 4 kB copy - only a caller that asks readable() first does.
    std::vector<uint8_t> _receive;
    size_t _receiveHead = 0;
    size_t _receiveTail = 0;

    void fail(const std::string& what);
    void closeSocket();
    bool finishConnecting();

    // The addresses the host resolved to, and the move to the next one. Held
    // across polls: a non-blocking connect completes later, and only then is
    // it known whether this candidate worked.
    mq_socket::AddressList _addresses;
    bool nextAddress();

    // Pulls whatever the socket has into the buffer. False when nothing came.
    bool fill();
};

#endif // MQTT_PLATFORM_HOST

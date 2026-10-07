// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTcpTransport.h"

#if defined(MQTT_PLATFORM_HOST)

#include <string.h>
#include <errno.h>

#include "MqttSocketSupport.h"   // the Win32/POSIX shim, once

namespace {


// Closes a socket WITHOUT throwing away what we just wrote.
//
// close() on a socket with UNREAD INCOMING data sends an RST rather than a
// FIN, and an RST discards whatever is still in our send buffer. A client that
// sends DISCONNECT and closes immediately can therefore have the DISCONNECT
// thrown away - and the broker, seeing a reset, treats the disconnection as
// ungraceful and publishes the Will. A device that shut down cleanly would
// raise an alarm every time.
//
// Bounded, so a peer that keeps sending cannot hold the socket open.
void drainBeforeClose(int socketHandle) {
    ::shutdown(socketHandle, SHUT_WR);

    uint8_t discard[1024];
    for (int pass = 0; pass < 16; pass++) {
        const int got = (int)recv(socketHandle, (char*)discard, (int)sizeof discard, 0);
        if (got <= 0) break;
    }
}

} // namespace

MqttTcpTransport::~MqttTcpTransport() { closeSocket(); }

void MqttTcpTransport::configure(const char* host, uint16_t port) {
    _host = host ? host : "";
    _port = port;
}

void MqttTcpTransport::fail(const std::string& what) {
    _lastError = what;
    closeSocket();
}

void MqttTcpTransport::closeSocket() {
    _addresses.reset();          // candidates belong to one attempt
    if (_socket >= 0) {
        drainBeforeClose(_socket);
        MQ_CLOSE(_socket);
        _socket = -1;
    }
    _connecting = false;
    _connected = false;
    _failed = false;
    _peerClosed = false;
    _receiveHead = 0;
    _receiveTail = 0;
}

bool MqttTcpTransport::begin() {
    mq_socket::ensureWinsock();
    closeSocket();
    _lastError.clear();

    if (_host.empty() || _port == 0) {
        _lastError = "no host or port configured";
        return false;
    }

    // Every address the name has. See mq_socket::AddressList: a non-blocking
    // connect returns EINPROGRESS for any address, so the loop this replaced
    // committed to the first one and freed the rest before anybody could find
    // out whether it worked.
    //
    // And looked up WITHOUT WAITING - see mq_socket::NameLookup. A name still
    // being resolved is "connecting" exactly as a connect in progress is, and
    // finishConnecting() collects it on a later poll.
    switch (_addresses.start(_host.c_str(), _port)) {
    case mq_socket::AddressList::Resolved:
        return nextAddress();
    case mq_socket::AddressList::Resolving:
        _connecting = true;
        _connected = false;
        return true;
    default:
        _lastError = "cannot resolve " + _host + ": " + _addresses.why();
        return false;
    }
}

// Start connecting to the next candidate, or give up having said how many were
// tried. Called from begin() and again from finishConnecting() each time an
// attempt turns out to have failed.
bool MqttTcpTransport::nextAddress() {
    int handle = -1;
    switch (_addresses.connectNext(handle)) {
    case mq_socket::AddressList::Connected:
        _socket = handle;
        _connected = true;
        _connecting = false;
        _addresses.reset();
        return true;
    case mq_socket::AddressList::InProgress:
        // The normal case, and the reason this is not a blocking call: the
        // handshake finishes on its own and finishConnecting() collects it on
        // a later poll.
        _socket = handle;
        _connecting = true;
        _connected = false;
        return true;
    default:
        break;
    }

    char note[96];
    snprintf(note, sizeof note, ": none of %u address(es) answered",
             _addresses.tried());
    _lastError = "cannot connect to " + _host + note;
    _addresses.reset();
    _socket = -1;
    _connecting = false;
    _connected = false;
    _failed = true;
    return false;
}

bool MqttTcpTransport::finishConnecting() {
    if (_connecting && _addresses.resolving()) {
        switch (_addresses.collect()) {
        case mq_socket::AddressList::Resolving:
            return false;                         // still looking; ask again
        case mq_socket::AddressList::Resolved:
            if (!nextAddress()) return false;
            if (_connected) return true;
            break;                                // connect now in progress
        default:
            _lastError = "cannot resolve " + _host + ": " + _addresses.why();
            _addresses.reset();
            _connecting = false;
            _connected = false;
            _failed = true;
            return false;
        }
    }
    if (!_connecting || _socket < 0) return _connected;

#if defined(_WIN32)
    fd_set writable;
    fd_set failed;
    FD_ZERO(&writable); FD_SET((SOCKET)_socket, &writable);
    FD_ZERO(&failed);   FD_SET((SOCKET)_socket, &failed);
    timeval immediately = { 0, 0 };
    const int ready = select(0, nullptr, &writable, &failed, &immediately);
    if (ready <= 0) return false;
    const bool bad = FD_ISSET((SOCKET)_socket, &failed) != 0;
#else
    struct pollfd watch;
    watch.fd = _socket;
    watch.events = POLLOUT;
    watch.revents = 0;
    const int ready = poll(&watch, 1, 0);      // zero timeout: never waits
    if (ready <= 0) return false;
    const bool bad = (watch.revents & (POLLERR | POLLHUP)) != 0;
#endif

    // A socket becomes writable both when it connects and when it fails, so
    // the error queue is what actually says which. Skipping this reports a
    // refused connection as an established one, and the first write then fails
    // for a reason that looks nothing like the cause.
    int error = 0;
    socklen_t length = sizeof error;
    if (bad || getsockopt(_socket, SOL_SOCKET, SO_ERROR, (char*)&error, &length) != 0
            || error != 0) {
        // THIS ADDRESS failed, not the host. Take the next one; nextAddress()
        // is what finally gives up, and only once every candidate is spent.
        MQ_CLOSE(_socket);
        _socket = -1;
        return nextAddress() && _connected;
    }

    _connecting = false;
    _connected = true;
    _addresses.reset();          // connected: the rest are not needed
    return true;
}

void MqttTcpTransport::end() { closeSocket(); }

bool MqttTcpTransport::isConnected() const {
    // const in the interface, and finishing a handshake is not a change the
    // caller can observe as one - the socket was already on its way.
    MqttTcpTransport* self = const_cast<MqttTcpTransport*>(this);
    if (self->_connecting) self->finishConnecting();
    return self->_connected && self->_socket >= 0;
}

bool MqttTcpTransport::peerClosed() const { return _peerClosed; }

bool MqttTcpTransport::fill() {
    if (_receiveHead < _receiveTail) return true;     // still holding data

    _receiveHead = 0;
    _receiveTail = 0;
    if (!isConnected()) return false;

    if (_receive.empty()) _receive.resize(kReceiveBufferSize);
    const int got = (int)recv(_socket, (char*)_receive.data(), (int)kReceiveBufferSize, 0);
    if (got > 0) {
        _receiveTail = (size_t)got;
        return true;
    }
    if (got == 0) {
        // Zero bytes from a readable socket is FIN: the peer has sent all it
        // is going to. The only way to learn it, and it cannot be undone -
        // which is why the buffer above exists rather than a peek.
        _peerClosed = true;
        return false;
    }
    if (!MQ_WOULD_BLOCK) {
        _peerClosed = true;
        fail("connection lost");
    }
    return false;
}

size_t MqttTcpTransport::readable() {
    if (_receiveHead < _receiveTail) return _receiveTail - _receiveHead;
    if (!fill()) return 0;
    return _receiveTail - _receiveHead;
}

size_t MqttTcpTransport::read(uint8_t* into, size_t length) {
    if (into == nullptr || length == 0) return 0;

    // STRAIGHT OUT OF THE BUFFER THIS TRANSPORT ALREADY FILLED. The bytes are
    // here because fill() did one bulk recv() for them; handing them back one
    // at a time through a virtual call - which is what the old interface
    // required - was pure ceremony.
    //
    // AND STRAIGHT INTO THE CALLER'S BUFFER WHEN THIS ONE IS EMPTY, with
    // fill()'s outcomes recorded the same way: a FIN learnt here is just as
    // permanent.
    if (_receiveHead >= _receiveTail) {
        if (!isConnected()) return 0;
        const int got = (int)recv(_socket, (char*)into, (int)length, 0);
        if (got > 0) return (size_t)got;
        if (got == 0) {
            _peerClosed = true;
        } else if (!MQ_WOULD_BLOCK) {
            _peerClosed = true;
            fail("connection lost");
        }
        return 0;
    }

    const size_t have = _receiveTail - _receiveHead;
    const size_t taken = have < length ? have : length;
    if (taken == 0) return 0;
    memcpy(into, _receive.data() + _receiveHead, taken);
    _receiveHead += taken;
    return taken;
}

size_t MqttTcpTransport::write(const uint8_t* buffer, size_t length) {
    if (!buffer || length == 0 || !isConnected()) return 0;

#if defined(MSG_NOSIGNAL)
    // Without this, writing to a socket whose peer has gone raises SIGPIPE and
    // kills the process. A library that can take down its host because a
    // broker restarted is not one to put in a plant.
    const int flags = MSG_NOSIGNAL;
#else
    const int flags = 0;
#endif

    const int sent = (int)send(_socket, (const char*)buffer, (int)length, flags);
    if (sent > 0) return (size_t)sent;

    if (sent < 0 && !MQ_WOULD_BLOCK) {
        _peerClosed = true;
        fail("connection lost while sending");
        return 0;
    }
    // A full transmit buffer. Nothing was taken, and the caller resumes from
    // its cursor on the next poll rather than blocking here.
    return 0;
}

#endif // MQTT_PLATFORM_HOST

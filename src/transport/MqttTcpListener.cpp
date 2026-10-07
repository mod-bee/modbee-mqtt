// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTcpListener.h"

#if defined(MQTT_PLATFORM_HOST)

#include <string.h>
#include <errno.h>
#include <stdio.h>

#include "MqttSocketSupport.h"   // the Win32/POSIX shim, once

namespace {



void drainBeforeClose(int socketHandle) {
    // Tell the peer we are done writing. Our own outbound data is flushed by
    // the stack first, so anything already queued still goes.
    ::shutdown(socketHandle, SHUT_WR);

    // Then swallow whatever is still coming, so close() has no unread data to
    // reset over. A few kilobytes and a few passes: enough for the packets a
    // peer had in flight, nowhere near enough to be held open by a flood.
    uint8_t discard[1024];
    for (int pass = 0; pass < 16; pass++) {
        const int got = (int)recv(socketHandle, (char*)discard, (int)sizeof discard, 0);
        if (got <= 0) break;   // EOF, or nothing more right now
    }
}

} // namespace

// =============================================================================
// ONE ACCEPTED CONNECTION
// =============================================================================

MqttAcceptedTransport::~MqttAcceptedTransport() { closeSocket(); }

void MqttAcceptedTransport::adopt(int socket, const char* peerName) {
    closeSocket();
    _socket = socket;
    _peerClosed = false;
    _receiveHead = 0;
    _receiveTail = 0;
    _peerName = peerName ? peerName : "";

    mq_socket::setNonBlocking(_socket);
    mq_socket::suppressSigPipe(_socket);

    // Nagle off. MQTT packets are small and self-contained, and a broker that
    // waited 40 ms to coalesce a PUBACK with whatever came next would add that
    // delay to every QoS 1 round trip in the plant.
    const int one = 1;
    setsockopt(_socket, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof one);
}

bool MqttAcceptedTransport::begin() {
    // Already open: it was accepted, not dialled. Present so that the layers
    // above cannot tell an accepted connection from an outbound one.
    return _socket >= 0;
}

void MqttAcceptedTransport::end() { closeSocket(); }

// Closes a socket WITHOUT throwing away what we just wrote.
//
// This is not fussiness. On Linux - and on every other Berkeley-sockets stack -
// close() on a socket that still has UNREAD INCOMING data sends an RST rather
// than a FIN, and an RST discards whatever is still in our send buffer. So a
// broker that answers a flood with "DISCONNECT, reason 0x93" and then closes
// sends the client a connection reset instead: the client sees EPIPE, never
// learns why it was disconnected, and reports a broken broker.
//
// Paho's flow-control test found exactly that, as a BrokenPipeError raised
// while it was still sending. The fix is to read and discard what is pending
// first, so the kernel has nothing left to be rude about and sends a FIN.
//
// Bounded, because a peer that keeps sending must not be able to hold the
// socket open by doing so - at which point an RST is the right answer anyway.
void MqttAcceptedTransport::closeSocket() {
    if (_socket >= 0) {
        drainBeforeClose(_socket);
        MQ_CLOSE(_socket);
        _socket = -1;
    }
    _receiveHead = 0;
    _receiveTail = 0;
}

bool MqttAcceptedTransport::fill() {
    if (_socket < 0) return false;
    if (_receiveHead < _receiveTail) return true;

    _receiveHead = 0;
    _receiveTail = 0;
    if (_receive.empty()) _receive.resize(kReceiveBufferSize);

    const int got = (int)recv(_socket, (char*)_receive.data(), (int)kReceiveBufferSize, 0);
    if (got > 0) {
        _receiveTail = (size_t)got;
        return true;
    }
    if (got == 0) {
        // The peer has half-closed. THIS DISCOVERY CANNOT BE UNDONE - there is
        // no second recv that reports it again - so it is recorded rather than
        // returned, and peerClosed() answers from the record afterwards.
        _peerClosed = true;
        return false;
    }
    if (!MQ_WOULD_BLOCK) _peerClosed = true;
    return false;
}

size_t MqttAcceptedTransport::readable() {
    if (_receiveHead < _receiveTail) return _receiveTail - _receiveHead;
    if (!fill()) return 0;
    return _receiveTail - _receiveHead;
}

size_t MqttAcceptedTransport::read(uint8_t* into, size_t length) {
    if (into == nullptr || length == 0) return 0;

    // STRAIGHT OUT OF THE BUFFER THIS TRANSPORT ALREADY FILLED. The bytes are
    // here because fill() did one bulk recv() for them; handing them back one
    // at a time through a virtual call - which is what the old interface
    // required - was pure ceremony.
    //
    // AND STRAIGHT INTO THE CALLER'S BUFFER WHEN THIS ONE IS EMPTY. A bulk
    // reader gains nothing from a copy through here, and every connection a
    // broker serves is a bulk reader. Same outcomes as fill(), recorded the
    // same way, because a half-close discovered here is just as permanent.
    if (_receiveHead >= _receiveTail) {
        if (_socket < 0) return 0;
        const int got = (int)recv(_socket, (char*)into, (int)length, 0);
        if (got > 0) return (size_t)got;
        if (got == 0 || !MQ_WOULD_BLOCK) _peerClosed = true;
        return 0;
    }

    const size_t have = _receiveTail - _receiveHead;
    const size_t taken = have < length ? have : length;
    if (taken == 0) return 0;
    memcpy(into, _receive.data() + _receiveHead, taken);
    _receiveHead += taken;
    return taken;
}

size_t MqttAcceptedTransport::write(const uint8_t* buffer, size_t length) {
    if (_socket < 0 || buffer == nullptr || length == 0) return 0;

#if defined(MSG_NOSIGNAL)
    // Without this, writing to a socket whose peer has gone raises SIGPIPE and
    // kills the process - and this is the write every plain-TCP client of this
    // broker goes through. MqttTcpTransport has had the flag since the
    // beginning; the accepted side was missed, so the CLIENT half of the
    // library was safe and the BROKER half was not.
    const int flags = MSG_NOSIGNAL;
#else
    // macOS and the BSDs have no MSG_NOSIGNAL; adopt() sets SO_NOSIGPIPE on the
    // socket instead. Windows never raises SIGPIPE.
    const int flags = 0;
#endif

    const int sent = (int)send(_socket, (const char*)buffer, (int)length, flags);
    if (sent > 0) return (size_t)sent;

    // A PARTIAL WRITE IS ORDINARY AND SO IS A ZERO. The send buffer is full
    // and the caller resumes from its cursor on the next poll. Only a real
    // error means the connection has gone.
    if (sent < 0 && !MQ_WOULD_BLOCK) _peerClosed = true;
    return 0;
}

bool MqttAcceptedTransport::isConnected() const {
    return _socket >= 0 && !_peerClosed;
}

bool MqttAcceptedTransport::peerClosed() const {
    // Bytes still buffered mean there is work left whatever the socket did.
    // Reporting the peer as gone while its last PUBLISH is sitting unread
    // would throw away a message that arrived perfectly well.
    if (_receiveHead < _receiveTail) return false;
    return _peerClosed;
}

// =============================================================================
// THE LISTENER
// =============================================================================

MqttTcpListener::~MqttTcpListener() {
    end();
    for (MqttAcceptedTransport* entry : _pool) delete entry;
    _pool.clear();
}

void MqttTcpListener::configure(const char* address, uint16_t port, size_t maximum) {
    _address = address ? address : "";
    _port = port;

    if (maximum == 0) maximum = 1;

    // The pool is built once, here, and never grows. A broker that allocated
    // per connection would do its allocating exactly when it is busiest, and
    // on a gateway with no spare heap that is the afternoon it falls over.
    for (MqttAcceptedTransport* entry : _pool) delete entry;
    _pool.clear();
    // ONE MORE THAN ASKED FOR, and the extra one is not for serving.
    //
    // When every serving slot is taken, a connection that arrives has to be
    // refused - and refusing it IN MQTT, with a reason the client can read,
    // needs somewhere to read its CONNECT and write a CONNACK. Without a spare
    // there is nowhere, which is why this used to decline to accept() at all
    // and leave the client with a dropped SYN and no idea why.
    _servingCapacity = maximum;
    _pool.reserve(maximum + 1);
    for (size_t index = 0; index < maximum + 1; index++) {
        _pool.push_back(new MqttAcceptedTransport());
    }

    _poolIndex.clear();
    _poolIndex.reserve(_pool.size());
    _free.clear();
    _free.reserve(_pool.size());
    _isFree.assign(_pool.size(), 1);
    for (size_t index = _pool.size(); index > 0; index--) {
        _free.push_back((uint32_t)(index - 1));
        _poolIndex[_pool[index - 1]] = (uint32_t)(index - 1);
    }
}

void MqttTcpListener::giveBack(uint32_t index) {
    if (index >= _isFree.size() || _isFree[index]) return;   // never twice
    _isFree[index] = 1;
    _free.push_back(index);
}

void MqttTcpListener::fail(const std::string& what) {
    _lastError = what;
}

bool MqttTcpListener::begin() {
    mq_socket::ensureWinsock();
    end();

    if (_pool.empty()) configure(_address.c_str(), _port, 32);

    _socket = (int)::socket(AF_INET, SOCK_STREAM, 0);
    if (_socket < 0) {
        fail("could not create a listening socket");
        return false;
    }

    // Without this a broker cannot restart until the kernel's TIME_WAIT
    // expires - two minutes during which every reconnecting device fails, for
    // no reason a person watching would ever guess.
    const int one = 1;
    setsockopt(_socket, SOL_SOCKET, SO_REUSEADDR, (const char*)&one, sizeof one);
#if defined(SO_REUSEPORT)
    if (_reusePort) {
        setsockopt(_socket, SOL_SOCKET, SO_REUSEPORT, (const char*)&one, sizeof one);
    }
#endif

    struct sockaddr_in endpoint;
    memset(&endpoint, 0, sizeof endpoint);
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(_port);
    if (_address.empty() || _address == "0.0.0.0" || _address == "*") {
        endpoint.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, _address.c_str(), &endpoint.sin_addr) != 1) {
        fail("the listen address is not a dotted-quad IPv4 address: " + _address);
        MQ_CLOSE(_socket);
        _socket = -1;
        return false;
    }

    if (::bind(_socket, (struct sockaddr*)&endpoint, sizeof endpoint) != 0) {
        char note[96];
        std::snprintf(note, sizeof note, "port %u is already in use", (unsigned)_port);
        fail(note);
        MQ_CLOSE(_socket);
        _socket = -1;
        return false;
    }

    // The backlog is the kernel's queue of connections it has completed for us
    // but we have not taken yet. It is not the connection limit - that is the
    // pool - it is how many may pile up between two poll() calls.
    // THE BACKLOG IS THE POOL, not sixteen.
    //
    // This was a fixed 16, which is the number of connections the KERNEL will
    // hold between one accept() and the next. Everything beyond it is dropped
    // before this process ever sees it - so a broker sized for a thousand
    // clients silently refused all but sixteen of them whenever they arrived
    // together, and the clients saw a connection that failed for no stated
    // reason. Measured with mqtt-benchmark: 1000 clients at once left ~300
    // unconnected, and the same thousand ramped over ten seconds connected
    // perfectly.
    //
    // The pool is the right size because it is exactly how many this broker is
    // willing to serve; anything the pool cannot hold is refused deliberately,
    // in MQTT, with a reason - which is a different and far better thing than
    // a TCP connection the kernel dropped. SOMAXCONN caps it, and the kernel
    // clamps silently anyway, so asking for more than it allows is harmless.
    int backlog = (int)_pool.size();
    if (backlog < 16) backlog = 16;
#if defined(SOMAXCONN)
    if (backlog > SOMAXCONN) backlog = SOMAXCONN;
#endif
    if (::listen(_socket, backlog) != 0) {
        fail("the socket would not listen");
        MQ_CLOSE(_socket);
        _socket = -1;
        return false;
    }

    mq_socket::setNonBlocking(_socket);

    // What port was actually bound. Asking for 0 and reading back what the
    // kernel chose is how a test gets a free port without racing another test
    // for a fixed one.
    struct sockaddr_in actual;
    socklen_t actualLength = sizeof actual;
    if (::getsockname(_socket, (struct sockaddr*)&actual, &actualLength) == 0) {
        _boundPort = ntohs(actual.sin_port);
    } else {
        _boundPort = _port;
    }

    _lastError.clear();
    return true;
}

void MqttTcpListener::end() {
    if (_socket >= 0) {
        MQ_CLOSE(_socket);
        _socket = -1;
    }
    // Connections already handed out are NOT closed. They belong to whoever
    // took them, and ending a plant's sessions because somebody closed a
    // listening socket would be a surprising way to lose a shift's data.
}

MqttTransport* MqttTcpListener::accept() {
    if (_socket < 0) return nullptr;

    // A free slot first - and the pool has one more than the caller asked for,
    // kept back so that a connection arriving at a full broker can be accepted
    // and told 0x89 Server Busy rather than closed on sight. Closing it
    // without a word looks to the far end exactly like a broker that crashed,
    // which is what this did before the reserve existed.
    MqttAcceptedTransport* slot = nullptr;
    uint32_t slotIndex = 0;
    while (!_free.empty()) {
        slotIndex = _free.back();
        if (slotIndex < _pool.size() && !_pool[slotIndex]->inUse()) {
            slot = _pool[slotIndex];
            break;
        }
        // In use after all - released by end() on the transport rather than
        // through release(). Dropped from the stack; release() restores it.
        _free.pop_back();
        if (slotIndex < _isFree.size()) _isFree[slotIndex] = 0;
    }
    if (slot == nullptr) {
        // Empty stack. Before reporting the pool full, look once for entries
        // that came free without release() - a transport ended directly by
        // its holder - so a missed hand-back costs one walk, not a slot lost
        // for good.
        for (size_t index = _pool.size(); index > 0; index--) {
            if (!_pool[index - 1]->inUse()) giveBack((uint32_t)(index - 1));
        }
        if (_free.empty()) return nullptr;
        slotIndex = _free.back();
        slot = _pool[slotIndex];
    }

    struct sockaddr_in from;
    socklen_t fromLength = sizeof from;
    const int accepted = (int)::accept(_socket, (struct sockaddr*)&from, &fromLength);
    if (accepted < 0) {
        // NOT EVERY FAILURE HERE IS "nobody waiting".
        //
        // EWOULDBLOCK is, and is the usual case. EMFILE and ENFILE are file
        // descriptors running out, which is very reachable at a thousand
        // sockets against a default RLIMIT_NOFILE of 1024 - and when it
        // happens the listening socket stays readable for ever, so this is
        // retried on every poll, nothing is counted, and the broker looks
        // perfectly healthy while accepting nobody. Recording it is the
        // difference between a diagnosable fault and a mystery.
        if (!MQ_WOULD_BLOCK) {
            fail("accept failed");
        }
        return nullptr;
    }

    char name[64];
    char host[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &from.sin_addr, host, sizeof host);
    std::snprintf(name, sizeof name, "%s:%u", host, (unsigned)ntohs(from.sin_port));

    slot->adopt(accepted, name);
    // Taken only now: most calls find nobody waiting and the slot stays free.
    _free.pop_back();
    _isFree[slotIndex] = 0;
    return slot;
}

void MqttTcpListener::release(MqttTransport* transport) {
    if (transport == nullptr) return;

    // Only a transport from this pool. A caller unwinding an error path should
    // never have to work out which listener produced what, so anything else is
    // ignored rather than treated as a fault.
    const auto found = _poolIndex.find(transport);
    if (found == _poolIndex.end()) return;
    _pool[found->second]->end();
    giveBack(found->second);
}

size_t MqttTcpListener::capacityRemaining() const {
    size_t free = 0;
    for (const auto* entry : _pool) {
        if (!entry->inUse()) free++;
    }
    // The reserve is not capacity: it exists to refuse, not to serve. Reporting
    // it would tell a caller it had room when the next client will be told no.
    return free > 0 ? free - 1 : 0;
}

size_t MqttTcpListener::active() const {
    size_t used = 0;
    for (const MqttAcceptedTransport* entry : _pool) {
        if (entry->inUse()) used++;
    }
    return used;
}

#endif // MQTT_PLATFORM_HOST

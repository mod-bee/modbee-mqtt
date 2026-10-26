// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "WakeSignal.h"

#if defined(__linux__)

#include <sys/eventfd.h>
#include <unistd.h>

#include <cstdint>

bool WakeSignal::open() {
    close();
    _handle = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    return _handle >= 0;
}

void WakeSignal::close() {
    if (_handle >= 0) ::close(_handle);
    _handle = -1;
}

void WakeSignal::signal() {
    const uint64_t one = 1;
    const ssize_t ignored = ::write(_handle, &one, sizeof one);
    (void)ignored;
}

bool WakeSignal::drain() {
    uint64_t counter = 0;
    return ::read(_handle, &counter, sizeof counter) == (ssize_t)sizeof counter;   // non-blocking
}

#else

#include "transport/MqttSocketSupport.h"

#include <cstring>

bool WakeSignal::open() {
    close();
    mq_socket::ensureWinsock();
    const int s = (int)::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s < 0) return false;
    sockaddr_in at;
    std::memset(&at, 0, sizeof at);
    at.sin_family = AF_INET;
    at.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    at.sin_port = 0;
    socklen_t length = sizeof at;
    // Bound to an ephemeral loopback port, and connected to itself: a datagram
    // sent is a datagram this same socket reads.
    if (::bind(s, (sockaddr*)&at, sizeof at) != 0 || ::getsockname(s, (sockaddr*)&at, &length) != 0 ||
        ::connect(s, (sockaddr*)&at, sizeof at) != 0 || !mq_socket::setNonBlocking(s)) {
        MQ_CLOSE(s);
        return false;
    }
    _handle = s;
    return true;
}

void WakeSignal::close() {
    if (_handle >= 0) MQ_CLOSE(_handle);
    _handle = -1;
}

void WakeSignal::signal() {
    const char one = 1;
    ::send(_handle, &one, 1, 0);
}

bool WakeSignal::drain() {
    char discard[64];
    bool signalled = false;
    while (::recv(_handle, discard, (int)sizeof discard, 0) > 0) signalled = true;
    return signalled;
}

#endif

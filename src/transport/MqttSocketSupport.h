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
// THE SOCKET BITS EVERY HOST TRANSPORT NEEDS
// =============================================================================
//
// One copy of the Win32/POSIX shim, the Winsock guard and the non-blocking
// call, because there were three.
//
// THE THREE COPIES WERE NOT IDENTICAL, WHICH IS THE ARGUMENT FOR THIS FILE.
// MqttTcpTransport and MqttTcpListener each carried a Win32-aware
// setNonBlocking; MqttTlsTransport carried a POSIX-only one that called fcntl
// unconditionally, so a TLS client on Windows would have failed to go
// non-blocking and the transport's whole never-block contract with it. Nobody
// noticed because nobody builds the TLS transport on Windows - which is the
// kind of latent difference duplication produces and review does not catch.
//
// Header-only and `inline`: this is included by several translation units in
// one library, and a role-free helper is not worth a .cpp.
//
// R1 SAFE. Nothing here knows whether it is serving a client or a broker. A
// listener accepting a connection and a client dialling one want exactly the
// same three things from the operating system.

#include "../MqttPlatform.h"

// How much a transport reads from its socket in one go.
//
// ONE DEFINITION, because there were four and they had already drifted: three
// transports said 1460 and MqttTlsTransport said 4096, for the same SSL_read
// against the same record size, with nothing anywhere saying why. This file
// exists because the same thing happened to setNonBlocking - its own header
// says so - and the lesson was applied to that function and not to the buffer
// sitting in the same four classes.
//
// 4096, not 1460. A TLS record can be up to 16 kB and arrives whole, so a
// 1460-byte buffer takes four reads to clear one record; and now that
// MqttLink reads in bulk, a bigger buffer is simply fewer syscalls for the
// same bytes. It costs 2.6 kB per connection more than the old figure, which
// is the trade a gateway wants and a board sets down with MQTT_SOCKET_READ_SIZE.
#ifndef MQTT_SOCKET_READ_SIZE
#define MQTT_SOCKET_READ_SIZE 4096
#endif

#if defined(MQTT_PLATFORM_HOST)

#include <errno.h>
#include <stdio.h>    // snprintf, for the port as a service name
#include <string.h>   // memset, for the resolver hints
#include <atomic>     // a name lookup's hand-over from its worker
#include <chrono>     // the age of a remembered answer
#include <mutex>      // the table of lookup workers
#include <new>        // std::nothrow
#include <string>

#if defined(_WIN32)
    #include <winsock2.h>
    #include <ws2tcpip.h>
    typedef int socklen_t;
    #define MQ_CLOSE closesocket
    #define MQ_WOULD_BLOCK (WSAGetLastError() == WSAEWOULDBLOCK)
    // A non-blocking connect() that has not finished yet. Not an error: the
    // caller comes back and asks again rather than waiting.
    #define MQ_IN_PROGRESS (WSAGetLastError() == WSAEWOULDBLOCK)
    #define MQ_SHUT_WR SD_SEND
#else
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <netinet/tcp.h>
    #include <arpa/inet.h>
    #include <netdb.h>          // getaddrinfo, for a transport that dials out
    #include <poll.h>           // finishing a connect, and a handshake
    #include <pthread.h>        // a name lookup, off the polling thread
    #include <unistd.h>
    #include <fcntl.h>
    #define MQ_CLOSE ::close
    #define MQ_WOULD_BLOCK (errno == EAGAIN || errno == EWOULDBLOCK)
    #define MQ_IN_PROGRESS (errno == EINPROGRESS)
    #define MQ_SHUT_WR SHUT_WR
#endif

namespace mq_socket {

/** How a non-blocking connect() stands, without waiting: still going, done,
 *  or failed. A socket becomes writable both when it connects and when it
 *  fails, so the error queue is what actually says which - skipping it reports
 *  a refused connection as an established one, and the first write then fails
 *  for a reason that looks nothing like the cause. */
enum class ConnectOutcome { Pending, Connected, Failed };

// `errorOut`: the socket's own error when the connect failed (SO_ERROR), for saying why.
inline ConnectOutcome connectOutcome(int socketHandle, int* errorOut = nullptr) {
#if defined(_WIN32)
    fd_set writable;
    fd_set failed;
    FD_ZERO(&writable); FD_SET((SOCKET)socketHandle, &writable);
    FD_ZERO(&failed);   FD_SET((SOCKET)socketHandle, &failed);
    timeval immediately = { 0, 0 };
    const int ready = select(0, nullptr, &writable, &failed, &immediately);
    if (ready <= 0) return ConnectOutcome::Pending;
    const bool bad = FD_ISSET((SOCKET)socketHandle, &failed) != 0;
#else
    struct pollfd watch;
    watch.fd = socketHandle;
    watch.events = POLLOUT;
    watch.revents = 0;
    const int ready = ::poll(&watch, 1, 0);    // zero timeout: never waits
    if (ready <= 0) return ConnectOutcome::Pending;
    const bool bad = (watch.revents & (POLLERR | POLLHUP)) != 0;
#endif
    int error = 0;
    socklen_t length = sizeof error;
    const bool queried = getsockopt(socketHandle, SOL_SOCKET, SO_ERROR, (char*)&error, &length) == 0;
    if (bad || !queried || error != 0) {
        if (errorOut) *errorOut = queried ? error : 0;
        return ConnectOutcome::Failed;
    }
    return ConnectOutcome::Connected;
}

// Why a connect failed, in the words a person reading a status line needs.
inline std::string connectErrorWords(int error) {
    switch (error) {
    case 0: return "";
#if defined(_WIN32)
    case WSAECONNREFUSED: return "connection refused";
    case WSAETIMEDOUT: return "timed out";
    case WSAEHOSTUNREACH: return "no route to host";
    case WSAENETUNREACH: return "network unreachable";
    default: return "error " + std::to_string(error);
#else
    case ECONNREFUSED: return "connection refused";
    case ETIMEDOUT: return "timed out";
    case EHOSTUNREACH: return "no route to host";
    case ENETUNREACH: return "network unreachable";
    default: return strerror(error);
#endif
    }
}

#if defined(_WIN32)
struct WinsockGuard {
    WinsockGuard() { WSADATA data; WSAStartup(MAKEWORD(2, 2), &data); }
    ~WinsockGuard() { WSACleanup(); }
};
/** Winsock started once per process. A function-local static is initialised
 *  exactly once and is thread-safe since C++11. */
inline void ensureWinsock() { static WinsockGuard guard; (void)guard; }
#else
inline void ensureWinsock() {}
#endif

/** RUNNING OUT OF DESCRIPTORS (EMFILE, ENFILE) leaves the connection that
 *  could not be accepted in the backlog, and the listening socket readable for
 *  as long as it waits: a level-triggered poll returns at once, every time,
 *  and the loop spins a whole core while accepting nobody. A descriptor kept
 *  spare for this lets that connection be taken and closed at once - refused
 *  promptly instead of left hanging - and the spare is opened again. A
 *  listener opens one in begin() and closes it in end(); shed() runs right
 *  after the accept() that failed, while errno still says why. POSIX only:
 *  elsewhere there is no spare and shed() does nothing. */
inline int openSpareDescriptor() {
#if defined(_WIN32)
    return -1;
#else
    return ::open("/dev/null", O_RDONLY | O_CLOEXEC);
#endif
}

inline void shedWaitingConnection(int listenSocket, int& spare) {
#if defined(_WIN32)
    (void)listenSocket;
    (void)spare;
#else
    if (spare < 0 || (errno != EMFILE && errno != ENFILE)) return;
    ::close(spare);
    const int taken = ::accept(listenSocket, nullptr, nullptr);
    if (taken >= 0) ::close(taken);
    spare = openSpareDescriptor();
#endif
}

inline void closeSpareDescriptor(int& spare) {
#if !defined(_WIN32)
    if (spare >= 0) ::close(spare);
#endif
    spare = -1;
}

/** Put a socket into non-blocking mode. False if the call failed, which every
 *  caller treats as "this socket is unusable" rather than carrying on. */
inline bool setNonBlocking(int socketHandle) {
#if defined(_WIN32)
    u_long mode = 1;
    return ioctlsocket(socketHandle, FIONBIO, &mode) == 0;
#else
    const int flags = fcntl(socketHandle, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(socketHandle, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

/** A listening socket on `address` ("", "0.0.0.0" or "*": every interface)
 *  and `port` (0: one the kernel picks), non-blocking, with `backlog`.
 *  `reusePort`: several sockets share the port (SO_REUSEPORT, Linux). The
 *  socket, with the port it got in `bound`; -1, with `error`, when the port is
 *  taken or the address is not an IPv4 one. For a listener changed while it
 *  runs (useSocket): opened where a failure can still be answered. */
inline int openListeningSocket(const std::string& address, uint16_t port, bool reusePort, int backlog,
                               uint16_t& bound, std::string& error) {
    ensureWinsock();
    struct sockaddr_in endpoint;
    memset(&endpoint, 0, sizeof endpoint);
    endpoint.sin_family = AF_INET;
    endpoint.sin_port = htons(port);
    if (address.empty() || address == "0.0.0.0" || address == "*") {
        endpoint.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, address.c_str(), &endpoint.sin_addr) != 1) {
        error = "the listen address is not a dotted-quad IPv4 address: " + address;
        return -1;
    }
    const int handle = (int)::socket(AF_INET, SOCK_STREAM, 0);
    if (handle < 0) {
        error = "could not create a listening socket";
        return -1;
    }
    const int one = 1;
    setsockopt(handle, SOL_SOCKET, SO_REUSEADDR, (const char*)&one, sizeof one);
#if defined(SO_REUSEPORT)
    if (reusePort) setsockopt(handle, SOL_SOCKET, SO_REUSEPORT, (const char*)&one, sizeof one);
#else
    (void)reusePort;
#endif
    if (::bind(handle, (struct sockaddr*)&endpoint, sizeof endpoint) != 0) {
        error = "port " + std::to_string((unsigned)port) + " is already in use";
        MQ_CLOSE(handle);
        return -1;
    }
#if defined(SOMAXCONN)
    if (backlog > SOMAXCONN) backlog = SOMAXCONN;
#endif
    if (backlog < 16) backlog = 16;
    if (::listen(handle, backlog) != 0 || !setNonBlocking(handle)) {
        error = "the socket would not listen";
        MQ_CLOSE(handle);
        return -1;
    }
    struct sockaddr_in actual;
    socklen_t length = sizeof actual;
    bound = ::getsockname(handle, (struct sockaddr*)&actual, &length) == 0 ? ntohs(actual.sin_port) : port;
    return handle;
}

// Stop a write to a departed peer from killing the process.
//
// WRITING TO A SOCKET WHOSE PEER HAS GONE RAISES SIGPIPE, and its default
// disposition terminates the process. That is not rare and it is not the
// peer misbehaving: it is every reset connection, every port scanner, every
// device that loses power while connected. A library that can take down its
// host because a client vanished is not one to put in a plant.
//
// THERE ARE TWO MECHANISMS AND NO PLATFORM HAS BOTH.
//
//   Linux    MSG_NOSIGNAL, per send() call.
//   macOS,   SO_NOSIGPIPE, once per socket - there is no MSG_NOSIGNAL, so a
//   BSD      per-call flag cannot be asked for at all.
//   Windows  never raises SIGPIPE.
//
// Both are used. The per-call flag is at every send site; this is the other
// half, and without it macOS had neither - MqttTlsSupport.h said macOS "is
// covered by SO_NOSIGPIPE on the socket itself" while nothing in the library
// ever set it.
//
// NOT signal(SIGPIPE, SIG_IGN): that is a process-wide change made by a
// library the application merely linked, and a controller whose own code
// relies on SIGPIPE would have it taken away silently by something it asked
// only to speak MQTT.
inline void suppressSigPipe(int socketHandle) {
#if defined(SO_NOSIGPIPE)
    const int on = 1;
    setsockopt(socketHandle, SOL_SOCKET, SO_NOSIGPIPE, (const char*)&on, sizeof on);
#else
    (void)socketHandle;   // Linux uses MSG_NOSIGNAL; Windows has no signal.
#endif
}


// =============================================================================
// A HOST'S ADDRESSES, TRIED IN TURN
// =============================================================================
//
// WHY THIS IS A CLASS AND NOT A LOOP. Both transports used to resolve the host
// and loop over the results like this:
//
//     for (addrinfo* at = results; at; at = at->ai_next) {
//         ...
//         if (errno == EINPROGRESS) { _socket = handle; freeaddrinfo(results);
//                                     return true; }
//     }
//
// which looks like it tries every address and does not. A NON-BLOCKING
// connect() returns EINPROGRESS for EVERY address - including one that will
// never complete - so the first candidate always wins, and freeaddrinfo throws
// the others away before anybody finds out whether it worked. The loop had one
// iteration.
//
// That is not academic. A public broker is multi-homed:
//
//     broker.hivemq.com    six addresses, three of them IPv6
//     broker.emqx.io       three
//     test.mosquitto.org   two
//
// so a client that can only use the first one fails whenever that particular
// address is unhealthy, or is IPv6 on a machine with no IPv6 route - and it
// fails INTERMITTENTLY, because DNS rotates the order. It also fails silently:
// the connect never errors, it just never completes, so the transport has no
// error to report and the timeout above it prints an empty reason.
//
// The list therefore has to OUTLIVE the first attempt. The connect completes
// asynchronously, on a later poll, and only then is it known whether to move
// on - which is what this holds.
//
// AND THE LIST IS RACED, NOT WALKED - see "A RACE, NOT A QUEUE" below.

// =============================================================================
// THE SETTINGS, IN ONE PLACE
// =============================================================================

// Happy Eyeballs' Connection Attempt Delay [RFC 8305 section 5]: how long one
// address is given on its own before the next is started beside it. 250 ms is
// the RFC's recommended default, and nothing is lost by it on a slow link: the
// earlier attempt is NOT abandoned when the next starts, so a cellular link
// with a two-second round trip still connects on its first address.
#ifndef MQTT_CONNECT_ATTEMPT_DELAY_MS
#define MQTT_CONNECT_ATTEMPT_DELAY_MS 250
#endif

// How long a fresh name lookup is waited for before the last addresses the
// name resolved to are used instead - when there are any. RFC 8767's "client
// response timer", whose suggested value this is. A lookup that answers
// sooner always wins; one that answers later still refreshes what is
// remembered, because the lookup runs on and is not thrown away.
#ifndef MQTT_DNS_STALE_AFTER_MS
#define MQTT_DNS_STALE_AFTER_MS 1800
#endif

// The longest the last good answer for a name is kept, in seconds. 24 hours
// by default: long enough to ride out a carrier's DNS outage, short enough
// that a server which has moved is not chased for days. getaddrinfo() does not
// say what TTL the answer carried, so the RFC 8767 bound is all there is to
// go on. Per transport: setStaleAddressLimit(). Zero turns the memory off.
#ifndef MQTT_DNS_STALE_LIMIT_S
#define MQTT_DNS_STALE_LIMIT_S 86400
#endif

// THE KERNEL'S OWN BACKSTOP, under the MQTT keepalive. See tuneDialledSocket.
// Milliseconds of unacknowledged data before the kernel gives up on the
// connection (TCP_USER_TIMEOUT, Linux). Zero leaves the kernel's default -
// which, with tcp_retries2 at 15, is the best part of fifteen minutes.
#ifndef MQTT_TCP_DEAD_LINK_MS
#define MQTT_TCP_DEAD_LINK_MS 120000
#endif

// NOT TCP_NOTSENT_LOWAT. It was set here, at 16 kB, so that "the bytes have
// left the link" meant "on the wire" - and a host client publishing quickly
// fell from 44,000 messages a second to under 700, losing most of a burst
// (test_app_api's throughput section; 256 kB was no better). The
// acknowledgement deadline starts when the bytes leave this library's buffer,
// which on a host is a little early; the managed connections' deadline (twice
// the keepalive, at least 120 s) has room for what the kernel holds.

// =============================================================================
// ONE ADDRESS TO DIAL
// =============================================================================
//
// A copy of the address, not a pointer into getaddrinfo()'s list. The list is
// freed the moment it has been read, which means a candidate can come from a
// fresh lookup or from the memory of an old one and be dialled the same way.

struct Endpoint {
    union {
        struct sockaddr any;
        struct sockaddr_in v4;
        struct sockaddr_in6 v6;
    } address;
    socklen_t length;
    int family;
    // From the last-known-good memory rather than a fresh answer. A stale
    // address that is refused is forgotten at once - see condemns().
    bool stale;

    Endpoint() : length(0), family(0), stale(false) { memset(&address, 0, sizeof address); }

    /** Only IPv4 and IPv6 stream addresses are dialled; anything else is
     *  skipped, as a socket that could not be created always was. */
    static bool from(const struct addrinfo* info, Endpoint& out) {
        if (info == nullptr || info->ai_addr == nullptr) return false;
        if (info->ai_family != AF_INET && info->ai_family != AF_INET6) return false;
        if ((size_t)info->ai_addrlen > sizeof out.address) return false;
        out = Endpoint();
        memcpy(&out.address, info->ai_addr, (size_t)info->ai_addrlen);
        out.length = (socklen_t)info->ai_addrlen;
        out.family = info->ai_family;
        return true;
    }

    void setPort(uint16_t port) {
        if (family == AF_INET) address.v4.sin_port = htons(port);
        else if (family == AF_INET6) address.v6.sin6_port = htons(port);
    }

    /** The same host address, whatever the port. */
    bool sameAddress(const Endpoint& other) const {
        if (family != other.family) return false;
        if (family == AF_INET) {
            return memcmp(&address.v4.sin_addr, &other.address.v4.sin_addr,
                          sizeof address.v4.sin_addr) == 0;
        }
        if (family == AF_INET6) {
            return memcmp(&address.v6.sin6_addr, &other.address.v6.sin6_addr,
                          sizeof address.v6.sin6_addr) == 0;
        }
        return false;
    }
};

// =============================================================================
// THE LAST GOOD ANSWER, PER NAME
// =============================================================================
//
// A CARRIER'S DNS GOES DOWN MORE OFTEN THAN ITS ROUTING DOES. On a cellular
// modem the resolver is the carrier's, reached over the same bearer, and it is
// the piece that fails first: the APN is up, the broker is up, its address has
// not changed - and every reconnect attempt re-resolved the name, got nothing,
// and reported "cannot resolve" for as long as the outage lasted. A connection
// that worked an hour ago was refused for want of a lookup whose answer was
// already known.
//
// So the last answer that RESOLVED is remembered, per host name, and used when
// a fresh lookup fails or does not answer in time. The rules, which are what a
// server that moves or fails over needs - it is the reason the memory must
// never become a pin:
//
//   1. EVERY ATTEMPT LOOKS UP FIRST. The memory is used only when the lookup
//      fails, or has not answered within MQTT_DNS_STALE_AFTER_MS (RFC 8767's
//      client response timer, 1.8 s) - never in preference to an answer.
//   2. A FRESH ANSWER REPLACES WHAT IS REMEMBERED AT ONCE - including one that
//      arrives after the attempt stopped waiting for it, because the lookup is
//      left to finish on its worker and stores its answer when it does.
//   3. IT EXPIRES. getaddrinfo() does not report the TTL, so the bound is RFC
//      8767's serve-stale window: 24 hours by default (MQTT_DNS_STALE_LIMIT_S,
//      or setStaleAddressLimit() per transport), counted from when the answer
//      was received - using it does not make it younger. After that it is
//      dropped and the device simply keeps retrying DNS.
//   4. A STALE ADDRESS THAT IS REFUSED, OR TIMES OUT, IS FORGOTTEN AT ONCE.
//      Not one that fails because THIS machine has no route at all (network
//      unreachable, network down): that says nothing about the address, and
//      forgetting every address while the modem re-registers would throw away
//      exactly what the memory is for.
//   5. PER HOST NAME, so a second or failover server under another name is
//      looked up and remembered separately.
//   6. A connection made on a stale address changes nothing about the next
//      attempt, which looks the name up afresh like any other.
//
// The clock is a 64-bit steady one, NOT mq_millis(): that wraps every 49.7
// days, and an answer remembered fifty days ago would look a few hours old.
// Replaceable, like the resolver, so a test can age an answer without waiting.

typedef uint64_t (*SteadyClockFn)();
inline uint64_t steadyMillis() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}
inline SteadyClockFn& steadyClockSlot() {
    static SteadyClockFn clock = &steadyMillis;
    return clock;
}

class KnownAddresses {
public:
    static const unsigned kHosts = 16;      // names remembered, oldest replaced
    static const unsigned kPerHost = 8;     // addresses kept for each

    /** A fresh answer, which replaces whatever was remembered for the name.
     *  Called from a lookup's worker thread as well as the polling one. */
    void remember(const char* host, const struct addrinfo* results) {
        if (host == nullptr || *host == '\0') return;
        const uint64_t now = steadyClockSlot()();
        std::lock_guard<std::mutex> guard(_lock);
        Row* row = find(host);
        if (row == nullptr) row = claim();
        row->host = host;
        row->count = 0;
        for (const struct addrinfo* at = results; at != nullptr && row->count < kPerHost;
             at = at->ai_next) {
            Endpoint candidate;
            if (!Endpoint::from(at, candidate)) continue;
            bool duplicate = false;
            for (unsigned i = 0; i < row->count; i++) {
                if (row->addresses[i].sameAddress(candidate)) duplicate = true;
            }
            if (!duplicate) row->addresses[row->count++] = candidate;
        }
        row->storedAt = now;
        if (row->count == 0) erase(*row);   // an answer with nothing dialable
    }

    /** What was remembered for `host`, if it is younger than `limitMs`; marked
     *  stale. An entry past its age is dropped here. */
    unsigned recall(const char* host, uint64_t limitMs, Endpoint* into, unsigned room) {
        if (host == nullptr || into == nullptr) return 0;
        const uint64_t now = steadyClockSlot()();
        std::lock_guard<std::mutex> guard(_lock);
        Row* row = find(host);
        if (row == nullptr) return 0;
        if (limitMs == 0 || now - row->storedAt > limitMs) {
            erase(*row);
            return 0;
        }
        unsigned n = 0;
        for (; n < row->count && n < room; n++) {
            into[n] = row->addresses[n];
            into[n].stale = true;
        }
        return n;
    }

    /** A stale address that was refused: never offered again. */
    void forget(const char* host, const Endpoint& address) {
        if (host == nullptr) return;
        std::lock_guard<std::mutex> guard(_lock);
        Row* row = find(host);
        if (row == nullptr) return;
        unsigned kept = 0;
        for (unsigned i = 0; i < row->count; i++) {
            if (!row->addresses[i].sameAddress(address)) row->addresses[kept++] = row->addresses[i];
        }
        row->count = kept;
        if (kept == 0) erase(*row);
    }

    /** How many addresses are remembered for `host`, whatever their age. */
    unsigned count(const char* host) {
        std::lock_guard<std::mutex> guard(_lock);
        Row* row = host != nullptr ? find(host) : nullptr;
        return row != nullptr ? row->count : 0;
    }

    /** Everything forgotten. For a test, and for an owner that knows better. */
    void forgetAll() {
        std::lock_guard<std::mutex> guard(_lock);
        for (unsigned i = 0; i < kHosts; i++) erase(_rows[i]);
    }

private:
    struct Row {
        std::string host;
        Endpoint addresses[kPerHost];
        unsigned count = 0;
        uint64_t storedAt = 0;
    };

    std::mutex _lock;
    Row _rows[kHosts];

    Row* find(const char* host) {
        for (unsigned i = 0; i < kHosts; i++) {
            if (_rows[i].count != 0 && _rows[i].host == host) return &_rows[i];
        }
        return nullptr;
    }

    // An empty row, or the one remembered longest ago.
    Row* claim() {
        Row* oldest = &_rows[0];
        for (unsigned i = 0; i < kHosts; i++) {
            if (_rows[i].count == 0) return &_rows[i];
            if (_rows[i].storedAt < oldest->storedAt) oldest = &_rows[i];
        }
        return oldest;
    }

    static void erase(Row& row) {
        row.host.clear();
        row.count = 0;
        row.storedAt = 0;
    }
};

inline KnownAddresses& knownAddresses() {
    static KnownAddresses known;
    return known;
}

// =============================================================================
// A NAME LOOKUP THAT DOES NOT HOLD UP THE SCAN
// =============================================================================
//
// getaddrinfo() BLOCKS, AND FOR AS LONG AS THE RESOLVER LIKES. With a name and
// a DNS server that does not answer, that is five seconds per attempt per
// server in resolv.conf's defaults - on every reconnect. Everything else on the
// dialling path is non-blocking (the connect, the TLS handshake), so this was
// the one call that could stop a PLC scan, and on a site whose DNS is down it
// would stop it every time the link was retried.
//
// So a NAME is looked up on a short-lived worker thread and collected on a
// later poll, exactly the way a non-blocking connect is. A NUMERIC address is
// not a lookup at all - AI_NUMERICHOST answers it on the spot or refuses it on
// the spot - so it never starts a thread, and a plant that configures its
// broker by IP address pays nothing for any of this.
//
// THE WORKER OWNS NOTHING THE CALLER CAN FREE. The lookup's state is shared and
// counted: the transport may give up on an attempt (end(), a connect timeout,
// a destructor) while getaddrinfo() is still inside the resolver, and the
// worker then finds itself the last holder and frees the result itself. There
// is no join, deliberately - joining would be the very wait this exists to
// remove. AND IT STILL REMEMBERS WHAT IT FOUND: an abandoned lookup that
// answers late refreshes the last-known-good memory above, so the next attempt
// is not left with an older answer than the network has already given.
//
// BOUNDED. A dead DNS server and a client retrying every second would otherwise
// leave a thread per attempt parked in the resolver. Past kMaxLookupsInFlight
// a new lookup is refused with a reason rather than started - and that refusal
// is a failed lookup like any other, so the remembered answer is used if there
// is one. The retry after the backoff tries again; nothing waits, nothing
// leaks, and the table empties as the resolver's own timeouts run out.
//
// JOINED, NEVER DETACHED - AND THIS IS WHAT STOPS A CRASH. A PLC runtime loads
// its program as a shared object and dlclose()s it on every stop and every
// re-download. A detached worker still inside getaddrinfo() at that moment -
// which is exactly where it sits while DNS is down - would return into code
// that has been unmapped, and take the whole runtime with it. So every worker
// is held in a fixed table: finished ones are joined on the next lookup, and
// the table's destructor joins whatever is left. Static destructors run inside
// dlclose() BEFORE the code is unmapped, so nothing can outlive it. The price
// is that stopping a program while DNS is hung waits for the resolver's own
// timeout - bounded, and visible, where the alternative was a crash.
//
// Windows resolves synchronously, as before: it has no pthreads, and no
// Windows host is a controller this library is deployed on.

/** The resolver, replaceable so a test can make it slow on purpose: the only
 *  way to prove a lookup does not hold up poll() is a lookup that takes a
 *  measurable time. A function-local static in an inline function, so every
 *  translation unit shares the one slot. */
typedef int (*ResolverFn)(const char*, const char*, const struct addrinfo*,
                          struct addrinfo**);
inline ResolverFn& resolverSlot() {
    static ResolverFn resolver = &::getaddrinfo;
    return resolver;
}

struct NameLookup {
#if !defined(_WIN32)
    std::atomic<int> holders{2};            // the AddressList and the worker
    std::atomic<bool> finished{false};
#endif
    int status = 0;
    struct addrinfo* results = nullptr;
    std::string host;
    char service[8] = {0};
    int slot = -1;                           // its worker's row in the table
    // Taken when the lookup starts, on the caller's thread, so the worker
    // reads nothing shared.
    ResolverFn resolver = nullptr;
};

#if !defined(_WIN32)
const int kMaxLookupsInFlight = 8;

// The workers, every one of which is joined. See the header comment.
struct LookupWorkers {
    std::mutex lock;
    pthread_t thread[kMaxLookupsInFlight];
    bool used[kMaxLookupsInFlight] = {};
    std::atomic<bool> done[kMaxLookupsInFlight];

    LookupWorkers() {
        for (int i = 0; i < kMaxLookupsInFlight; i++) done[i].store(false);
    }

    // Runs inside dlclose(), before the code it would return into is unmapped.
    ~LookupWorkers() {
        for (int i = 0; i < kMaxLookupsInFlight; i++) {
            if (used[i]) pthread_join(thread[i], nullptr);
        }
    }

    // Join every worker that has finished, freeing its row. Never waits: a
    // finished worker has already returned or is one instruction from it.
    void reapLocked() {
        for (int i = 0; i < kMaxLookupsInFlight; i++) {
            if (used[i] && done[i].load(std::memory_order_acquire)) {
                pthread_join(thread[i], nullptr);
                used[i] = false;
            }
        }
    }

    int runningLocked() const {
        int n = 0;
        for (int i = 0; i < kMaxLookupsInFlight; i++) {
            if (used[i] && !done[i].load(std::memory_order_acquire)) n++;
        }
        return n;
    }
};

inline LookupWorkers& lookupWorkers() {
    // THE MEMORY IS BUILT FIRST, SO IT IS DESTROYED LAST. Static objects are
    // destroyed in the reverse order their construction finished, and a
    // worker still inside the resolver writes to the memory when it returns -
    // so the workers have to be joined (their table's destructor) before the
    // memory is gone.
    knownAddresses();
    static LookupWorkers workers;
    return workers;
}

/** How many lookups are still inside the resolver. */
inline int lookupsInFlight() {
    LookupWorkers& workers = lookupWorkers();
    std::lock_guard<std::mutex> guard(workers.lock);
    return workers.runningLocked();
}

inline void releaseLookup(NameLookup* lookup) {
    if (lookup->holders.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (lookup->results != nullptr) freeaddrinfo(lookup->results);
        delete lookup;
    }
}

inline void* runLookup(void* argument) {
    NameLookup* lookup = static_cast<NameLookup*>(argument);
    const int slot = lookup->slot;           // read before the lookup can go
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    lookup->status = lookup->resolver(lookup->host.c_str(), lookup->service,
                                      &hints, &lookup->results);
    // Remembered whether or not anybody is still waiting for it: rule 2.
    if (lookup->status == 0 && lookup->results != nullptr) {
        knownAddresses().remember(lookup->host.c_str(), lookup->results);
    }
    lookup->finished.store(true, std::memory_order_release);
    releaseLookup(lookup);
    lookupWorkers().done[slot].store(true, std::memory_order_release);
    return nullptr;
}
#endif

// =============================================================================
// WHAT A DIALLED SOCKET IS GIVEN
// =============================================================================
//
// TCP_NODELAY, as it always had: MQTT packets are small and a PINGREQ held
// back by Nagle is a keepalive that arrives late.
//
// AND A BACKSTOP UNDER THE MQTT KEEPALIVE, because the keepalive can be off
// (Keep Alive 0 is the application's to choose) or long (300 s is ordinary on
// a metered cellular link), and the kernel's own give-up time for data that is
// never acknowledged is the best part of fifteen minutes:
//
//   SO_KEEPALIVE      120 s idle, then a probe every 30 s, four of them. It
//                     only ever runs on a link the MQTT keepalive leaves idle
//                     for two minutes, so a keepalive of 60 s never sees it.
//   TCP_USER_TIMEOUT  MQTT_TCP_DEAD_LINK_MS (120 s) of data unacknowledged and
//                     the connection is over. Two minutes with no ACK at all
//                     is a dead path, not a slow one: a slow link acknowledges
//                     slowly, it does not stop.
//
// Each where the platform has it, and silently not where it does not: a
// refused option leaves the socket exactly as it was before this existed.
inline void tuneDialledSocket(int handle, uint32_t deadLinkMs) {
    int one = 1;
    setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof one);
    setsockopt(handle, SOL_SOCKET, SO_KEEPALIVE, (const char*)&one, sizeof one);
#if defined(TCP_KEEPIDLE)
    int idle = 120;
    setsockopt(handle, IPPROTO_TCP, TCP_KEEPIDLE, (const char*)&idle, sizeof idle);
#endif
#if defined(TCP_KEEPINTVL)
    int interval = 30;
    setsockopt(handle, IPPROTO_TCP, TCP_KEEPINTVL, (const char*)&interval, sizeof interval);
#endif
#if defined(TCP_KEEPCNT)
    int probes = 4;
    setsockopt(handle, IPPROTO_TCP, TCP_KEEPCNT, (const char*)&probes, sizeof probes);
#endif
#if defined(TCP_USER_TIMEOUT)
    if (deadLinkMs != 0) {
        unsigned int timeout = (unsigned int)deadLinkMs;
        setsockopt(handle, IPPROTO_TCP, TCP_USER_TIMEOUT, (const char*)&timeout, sizeof timeout);
    }
#else
    (void)deadLinkMs;
#endif
}

/** Whether a failed connect says the ADDRESS is bad - refused, timed out,
 *  unreachable host - rather than that this machine has no network at all.
 *  Only the first condemns a remembered address (rule 4 above). */
inline bool condemns(int error) {
#if defined(_WIN32)
    return error != WSAENETUNREACH && error != WSAENETDOWN;
#else
    return error != ENETUNREACH && error != ENETDOWN;
#endif
}

// =============================================================================
// A RACE, NOT A QUEUE
// =============================================================================
//
// ONE CONNECT BUDGET USED TO COVER EVERY ADDRESS, AND THE FIRST ATE IT. The
// walk above moved on only when an address FAILED - and a blackholed address
// does not fail, it just never answers: an IPv6 address on a network whose
// IPv6 is broken, an A record for a server that was switched off. The connect
// sat on it until the kernel's SYN retries ran out, two minutes later, long
// after the client's twenty-second connect timeout had ended the attempt - so
// the second address, which would have answered in forty milliseconds, was
// never tried at all, on that attempt or any other.
//
// So the addresses are RACED, the way RFC 8305 (Happy Eyeballs v2) races them:
//
//   - In an order that INTERLEAVES the families: the resolver's first choice,
//     then the first of the other family, then alternating. A host whose IPv6
//     is broken reaches its first IPv4 address on the second step, not after
//     every IPv6 address has had its turn.
//   - Each address is given MQTT_CONNECT_ATTEMPT_DELAY_MS (250 ms) alone, and
//     then the next is started BESIDE it. The earlier attempt is not given up
//     on: on a cellular link with a two-second round trip the first address
//     still wins, and the later SYNs cost a few dozen bytes.
//   - An attempt that fails starts the next at once.
//   - The first to connect wins and every other is closed.
//
// Up to kMaxRacing at once; past that the oldest is closed to make room, so a
// list longer than the table is still worked through rather than stuck behind
// eight blackholes.

class AddressList {
public:
    static const unsigned kMaxCandidates = 16;
    static const unsigned kMaxRacing = 8;

    AddressList() = default;
    ~AddressList() { reset(); }

    AddressList(const AddressList&) = delete;
    AddressList& operator=(const AddressList&) = delete;

    void reset() {
        closeRacing();
        _count = 0;
        _next = 0;
        _tried = 0;
        _stale = false;
#if !defined(_WIN32)
        // Abandoned, not waited for: the worker frees it when it returns.
        if (_lookup != nullptr) releaseLookup(_lookup);
#endif
        _lookup = nullptr;
    }

    enum Resolution { Resolved, Resolving, Unresolvable };

    /** Start resolving a host to dial, WITHOUT WAITING. Resolved means the
     *  candidates are ready now (a numeric address always is); Resolving means
     *  ask collect() on a later poll; Unresolvable means stop, and why() says
     *  what to tell the operator. */
    Resolution start(const char* host, uint16_t port) {
        reset();
        _why = nullptr;
        _host = host != nullptr ? host : "";
        _port = port;
        if (_host.empty()) {
            _why = "no host";
            return Unresolvable;
        }

        char service[8];
        snprintf(service, sizeof service, "%u", (unsigned)port);

        // A numeric address answers here and now, either way.
        struct addrinfo hints;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_NUMERICHOST;
        struct addrinfo* numeric = nullptr;
        if (getaddrinfo(host, service, &hints, &numeric) == 0 && numeric != nullptr) {
            adopt(numeric);
            freeaddrinfo(numeric);
            return _count != 0 ? Resolved : Unresolvable;
        }

#if defined(_WIN32)
        struct addrinfo* found = nullptr;
        hints.ai_flags = 0;
        if (getaddrinfo(host, service, &hints, &found) == 0 && found != nullptr) {
            knownAddresses().remember(host, found);
            adopt(found);
            freeaddrinfo(found);
            if (_count != 0) return Resolved;
        }
        _why = "does not resolve";
        return recallStale() ? Resolved : Unresolvable;
#else
        LookupWorkers& workers = lookupWorkers();
        std::lock_guard<std::mutex> guard(workers.lock);
        workers.reapLocked();
        int slot = -1;
        for (int i = 0; i < kMaxLookupsInFlight; i++) {
            if (!workers.used[i]) { slot = i; break; }
        }
        if (slot < 0) {
            _why = "DNS is not answering - earlier lookups are still waiting";
            return recallStale() ? Resolved : Unresolvable;
        }

        NameLookup* lookup = new (std::nothrow) NameLookup();
        if (lookup == nullptr) {
            _why = "no memory for a name lookup";
            return recallStale() ? Resolved : Unresolvable;
        }
        lookup->host = host;
        lookup->slot = slot;
        lookup->resolver = resolverSlot();
        memcpy(lookup->service, service, sizeof service);

        workers.done[slot].store(false, std::memory_order_release);
        if (pthread_create(&workers.thread[slot], nullptr, &runLookup, lookup) != 0) {
            // Not falling back to a blocking lookup: that would put back the
            // very stall this exists to remove. The next retry tries again.
            delete lookup;
            _why = "cannot start a name lookup";
            return recallStale() ? Resolved : Unresolvable;
        }
        workers.used[slot] = true;
        _lookup = lookup;
        _lookupSince = mq_millis();
        return Resolving;
#endif
    }

    /** Collect a lookup start() left running. Never waits. A lookup that has
     *  not answered within MQTT_DNS_STALE_AFTER_MS, or that failed, gives way
     *  to the last good answer when there is one. */
    Resolution collect() {
#if defined(_WIN32)
        return _count != 0 ? Resolved : Unresolvable;
#else
        if (_lookup == nullptr) return _count != 0 ? Resolved : Unresolvable;
        if (!_lookup->finished.load(std::memory_order_acquire)) {
            if (mq_elapsed(_lookupSince, mq_millis()) >= (mq_time_t)MQTT_DNS_STALE_AFTER_MS &&
                recallStale()) {
                // The lookup carries on without us and refreshes the memory
                // when it answers; this attempt goes ahead on what is known.
                releaseLookup(_lookup);
                _lookup = nullptr;
                return Resolved;
            }
            return Resolving;
        }

        NameLookup* lookup = _lookup;
        _lookup = nullptr;
        const bool found = lookup->status == 0 && lookup->results != nullptr;
        if (found) adopt(lookup->results);
        releaseLookup(lookup);
        if (found && _count != 0) return Resolved;
        _why = "does not resolve";
        return recallStale() ? Resolved : Unresolvable;
#endif
    }

    bool resolving() const { return _lookup != nullptr; }

    /** Why start() or collect() said Unresolvable, for an operator. */
    const char* why() const { return _why != nullptr ? _why : "does not resolve"; }

    /** The candidates came from the last-known-good memory, not a fresh
     *  answer - for an operator's log, and for a test. */
    bool servingStale() const { return _stale; }

    /** Resolve a host once, synchronously. False if the name does not resolve
     *  at all. Not on any polling path - a test's, and nothing else's.
     *
     *  A NULL host asks for THIS MACHINE'S WILDCARD ADDRESSES - what a
     *  listener would bind to. It is the one multi-address list available
     *  with no network and no DNS, which is what test_transport uses to walk
     *  the list: a single-address host cannot tell a cursor that advances once
     *  from one that advances twice, and advancing twice is what crashed. */
    bool resolve(const char* host, uint16_t port) {
        reset();
        _host = host != nullptr ? host : "";
        _port = port;

        char service[8];
        snprintf(service, sizeof service, "%u", (unsigned)port);

        struct addrinfo hints;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;      // IPv4 and IPv6, in the order the
        hints.ai_socktype = SOCK_STREAM;  // resolver recommends (RFC 6724)
        if (host == nullptr) hints.ai_flags = AI_PASSIVE;

        struct addrinfo* results = nullptr;
        if (getaddrinfo(host, service, &hints, &results) != 0 || results == nullptr) {
            return false;
        }
        adopt(results);
        freeaddrinfo(results);
        return _count != 0;
    }

    /** How many candidates there are to dial. */
    unsigned size() const { return _count; }

    /** The candidate at `index`, in the order they are dialled. */
    const Endpoint* candidate(unsigned index) const {
        return index < _count ? &_candidates[index] : nullptr;
    }

    enum Attempt { Connected, InProgress, Exhausted };

    /** Open a socket on the next candidate and start connecting to it - ONE
     *  AT A TIME, with no race. Kept for a caller that walks the list itself;
     *  the transports race (race(), below).
     *
     *  `socketOut` is set on Connected and InProgress, and left alone on
     *  Exhausted. A candidate whose socket cannot even be created is skipped
     *  rather than reported: it is this machine's limit, not that address's
     *  fault, and the next one may still work. */
    Attempt connectNext(int& socketOut) {
        // The cursor is advanced EXACTLY ONCE per candidate.
        //
        // The first version of this was a `for (; _next; _next = _next->ai_next)`
        // that also advanced inside the body - so it skipped every other
        // address, and on the last one it advanced a null pointer and
        // segfaulted. It only showed up on hosts with IPv6 addresses, because
        // those fail immediately on a machine with no IPv6 route and so reach
        // the fall-through; a host whose addresses all return EINPROGRESS
        // returns early and never gets there.
        while (_next < _count) {
            const unsigned index = _next++;     // consumed, whatever happens below
            int handle = -1;
            int error = 0;
            const Attempt attempt = open(index, handle, error);
            if (handle < 0) continue;           // no socket of that family here
            if (attempt == Exhausted) {
                // Refused outright - which is what an IPv6 address does on a
                // machine with no IPv6 route. Close it and take the next.
                MQ_CLOSE(handle);
                continue;
            }
            socketOut = handle;
            return attempt;
        }
        return Exhausted;
    }

    /** One step of the race: never waits. Connected hands over the winning
     *  socket in `socketOut`, every other attempt closed; InProgress means ask
     *  again on a later poll; Exhausted means every candidate has failed.
     *  `errorOut` is the socket error of the last attempt that failed. */
    Attempt race(int& socketOut, int& errorOut) {
        const mq_time_t now = mq_millis();
        bool failed = false;

        // Every attempt under way, oldest first.
        for (unsigned i = 0; i < _racers;) {
            int error = 0;
            const ConnectOutcome outcome = connectOutcome(_racing[i].handle, &error);
            if (outcome == ConnectOutcome::Pending) { i++; continue; }
            if (outcome == ConnectOutcome::Connected) {
                socketOut = _racing[i].handle;
                dropRacer(i, false);
                closeRacing();
                return Connected;
            }
            if (error != 0) errorOut = error;
            judge(_racing[i].index, error);
            dropRacer(i, true);
            failed = true;
        }

        // The next candidate: when nothing is racing, when the newest attempt
        // has had its head start, or at once when one has just failed.
        const bool startNow = _racers == 0 || failed ||
            mq_elapsed(_lastStart, now) >= (mq_time_t)MQTT_CONNECT_ATTEMPT_DELAY_MS;
        while (startNow && _next < _count) {
            if (_racers == kMaxRacing) dropRacer(0, true);   // the oldest makes room
            const unsigned index = _next++;
            int handle = -1;
            int error = 0;
            const Attempt attempt = open(index, handle, error);
            if (handle < 0) continue;
            if (attempt == Connected) {
                socketOut = handle;
                closeRacing();
                return Connected;
            }
            if (attempt == InProgress) {
                _racing[_racers].handle = handle;
                _racing[_racers].index = index;
                _racers++;
                _lastStart = now;
                break;
            }
            // Refused on the spot: the next one, now.
            if (error != 0) errorOut = error;
            judge(index, error);
            MQ_CLOSE(handle);
        }

        if (_racers == 0 && _next >= _count) return Exhausted;
        return InProgress;
    }

    /** The newest attempt's socket, for a caller that waits on one; -1 when
     *  nothing is racing. */
    int waitHandle() const { return _racers > 0 ? _racing[_racers - 1].handle : -1; }

    /** How many attempts are under way at once. */
    unsigned racing() const { return _racers; }

    /** Dial FROM this numeric address rather than whichever the routing
     *  table picks. Empty restores the default. A load generator uses it to
     *  spread connections over several source addresses: the kernel has only
     *  so many ephemeral ports per (source, destination) pair - about 28,000
     *  with Linux's default range - so one source address cannot open more
     *  connections than that to one broker port. */
    void setLocalAddress(const char* address) { _local = address ? address : ""; }

    /** How long a remembered answer may be used, in seconds; 0 never uses one.
     *  See KnownAddresses. */
    void setStaleAddressLimit(uint32_t seconds) { _staleLimitMs = (uint64_t)seconds * 1000u; }

    /** Milliseconds of unacknowledged data before the kernel ends the
     *  connection; 0 leaves the kernel's default. See tuneDialledSocket. */
    void setDeadLinkTimeout(uint32_t milliseconds) { _deadLinkMs = milliseconds; }

    /** How many were actually attempted, for an error an operator can read:
     *  "none of 6 addresses answered" says something "connection refused"
     *  does not. */
    unsigned tried() const { return _tried; }

private:
    std::string _local;
    std::string _host;
    uint16_t _port = 0;
    uint64_t _staleLimitMs = (uint64_t)MQTT_DNS_STALE_LIMIT_S * 1000u;
    uint32_t _deadLinkMs = MQTT_TCP_DEAD_LINK_MS;

    Endpoint _candidates[kMaxCandidates];
    unsigned _count = 0;
    unsigned _next = 0;
    unsigned _tried = 0;
    bool _stale = false;

    struct Racer {
        int handle;
        unsigned index;
    };
    Racer _racing[kMaxRacing];
    unsigned _racers = 0;
    mq_time_t _lastStart = 0;

    NameLookup* _lookup = nullptr;
    mq_time_t _lookupSince = 0;
    const char* _why = nullptr;

    // The candidates from a list, families interleaved [RFC 8305 section 4]:
    // the resolver's first choice, the first of the other family, and so on
    // alternately. A list of one family is left in the resolver's order.
    void adoptEndpoints(const Endpoint* list, unsigned n) {
        Endpoint first[kMaxCandidates];
        Endpoint second[kMaxCandidates];
        unsigned firstCount = 0;
        unsigned secondCount = 0;
        const int leading = n > 0 ? list[0].family : 0;
        for (unsigned i = 0; i < n; i++) {
            if (list[i].family == leading) {
                if (firstCount < kMaxCandidates) first[firstCount++] = list[i];
            } else if (secondCount < kMaxCandidates) {
                second[secondCount++] = list[i];
            }
        }
        _count = 0;
        _next = 0;
        for (unsigned i = 0; _count < kMaxCandidates && (i < firstCount || i < secondCount); i++) {
            if (i < firstCount && _count < kMaxCandidates) _candidates[_count++] = first[i];
            if (i < secondCount && _count < kMaxCandidates) _candidates[_count++] = second[i];
        }
        for (unsigned i = 0; i < _count; i++) _candidates[i].setPort(_port);
    }

    void adopt(const struct addrinfo* results) {
        Endpoint list[kMaxCandidates];
        unsigned n = 0;
        for (const struct addrinfo* at = results; at != nullptr && n < kMaxCandidates;
             at = at->ai_next) {
            if (Endpoint::from(at, list[n])) n++;
        }
        _stale = false;
        adoptEndpoints(list, n);
    }

    // The last good answer for this name, when there is one young enough.
    bool recallStale() {
        Endpoint list[KnownAddresses::kPerHost];
        const unsigned n = knownAddresses().recall(_host.c_str(), _staleLimitMs, list,
                                                   KnownAddresses::kPerHost);
        if (n == 0) return false;
        adoptEndpoints(list, n);
        _stale = true;
        return _count != 0;
    }

    // A failed attempt: a remembered address that was refused is forgotten.
    void judge(unsigned index, int error) {
        if (index >= _count || !_candidates[index].stale) return;
        if (error != 0 && !condemns(error)) return;
        knownAddresses().forget(_host.c_str(), _candidates[index]);
    }

    // Open a socket for one candidate and start its connect. Connected or
    // InProgress, or Exhausted for a connect refused on the spot (with the
    // socket still open, for the caller to close). `handle` stays -1 when no
    // socket could be created at all.
    Attempt open(unsigned index, int& handle, int& error) {
        const Endpoint& endpoint = _candidates[index];
        handle = (int)::socket(endpoint.family, SOCK_STREAM, IPPROTO_TCP);
        if (handle < 0) return Exhausted;
        if (!setNonBlocking(handle)) { MQ_CLOSE(handle); handle = -1; return Exhausted; }
        suppressSigPipe(handle);

        // A chosen source address, when one was asked for. A candidate of
        // another family (an IPv6 address for an IPv4 source) cannot use it
        // and is skipped, the same as one this host cannot reach.
        if (!_local.empty() && !bindLocal(handle, endpoint.family)) {
            MQ_CLOSE(handle);
            handle = -1;
            return Exhausted;
        }

        tuneDialledSocket(handle, _deadLinkMs);

        const int result = ::connect(handle, &endpoint.address.any, endpoint.length);
        _tried++;
        if (result == 0) return Connected;
        if (MQ_IN_PROGRESS) return InProgress;
#if defined(_WIN32)
        error = WSAGetLastError();
#else
        error = errno;
#endif
        return Exhausted;
    }

    void dropRacer(unsigned position, bool close) {
        if (position >= _racers) return;
        if (close) MQ_CLOSE(_racing[position].handle);
        for (unsigned i = position + 1; i < _racers; i++) _racing[i - 1] = _racing[i];
        _racers--;
    }

    void closeRacing() {
        while (_racers > 0) dropRacer(_racers - 1, true);
    }

    bool bindLocal(int handle, int family) {
        struct addrinfo hints;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = family;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_NUMERICHOST;       // never a lookup: this is poll()'s path
        struct addrinfo* local = nullptr;
        if (getaddrinfo(_local.c_str(), nullptr, &hints, &local) != 0 || local == nullptr) {
            return false;
        }
#if defined(IP_BIND_ADDRESS_NO_PORT)
        // Linux: choose the port at connect(), not bind(), so the same port
        // can be reused towards different destinations. Without it a bound
        // socket reserves a port outright, and a generator runs out of them
        // long before the four-tuple space is used.
        int one = 1;
        setsockopt(handle, IPPROTO_IP, IP_BIND_ADDRESS_NO_PORT, (const char*)&one, sizeof one);
#endif
        const bool bound = ::bind(handle, local->ai_addr, (socklen_t)local->ai_addrlen) == 0;
        freeaddrinfo(local);
        return bound;
    }
};

} // namespace mq_socket

#endif // MQTT_PLATFORM_HOST

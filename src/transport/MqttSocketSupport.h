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

inline ConnectOutcome connectOutcome(int socketHandle) {
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
    if (bad || getsockopt(socketHandle, SOL_SOCKET, SO_ERROR, (char*)&error, &length) != 0 || error != 0) {
        return ConnectOutcome::Failed;
    }
    return ConnectOutcome::Connected;
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
// remove.
//
// BOUNDED. A dead DNS server and a client retrying every second would otherwise
// leave a thread per attempt parked in the resolver. Past kMaxLookupsInFlight
// a new lookup is refused with a reason rather than started, and the retry
// after the backoff tries again.
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
    lookup->finished.store(true, std::memory_order_release);
    releaseLookup(lookup);
    lookupWorkers().done[slot].store(true, std::memory_order_release);
    return nullptr;
}
#endif

class AddressList {
public:
    AddressList() = default;
    ~AddressList() { reset(); }

    AddressList(const AddressList&) = delete;
    AddressList& operator=(const AddressList&) = delete;

    void reset() {
        if (_results != nullptr) freeaddrinfo(_results);
        _results = nullptr;
        _next = nullptr;
        _tried = 0;
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
        if (host == nullptr || *host == '\0') {
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
        if (getaddrinfo(host, service, &hints, &_results) == 0 && _results != nullptr) {
            _next = _results;
            return Resolved;
        }
        _results = nullptr;

#if defined(_WIN32)
        if (resolve(host, port)) return Resolved;
        _why = "does not resolve";
        return Unresolvable;
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
            return Unresolvable;
        }

        NameLookup* lookup = new (std::nothrow) NameLookup();
        if (lookup == nullptr) {
            _why = "no memory for a name lookup";
            return Unresolvable;
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
            return Unresolvable;
        }
        workers.used[slot] = true;
        _lookup = lookup;
        return Resolving;
#endif
    }

    /** Collect a lookup start() left running. Never waits. */
    Resolution collect() {
#if defined(_WIN32)
        return _results != nullptr ? Resolved : Unresolvable;
#else
        if (_lookup == nullptr) return _results != nullptr ? Resolved : Unresolvable;
        if (!_lookup->finished.load(std::memory_order_acquire)) return Resolving;

        NameLookup* lookup = _lookup;
        _lookup = nullptr;
        const bool found = lookup->status == 0 && lookup->results != nullptr;
        if (found) {
            _results = lookup->results;     // ours now
            lookup->results = nullptr;
            _next = _results;
        } else {
            _why = "does not resolve";
        }
        releaseLookup(lookup);
        return found ? Resolved : Unresolvable;
#endif
    }

    bool resolving() const { return _lookup != nullptr; }

    /** Why start() or collect() said Unresolvable, for an operator. */
    const char* why() const { return _why != nullptr ? _why : "does not resolve"; }

    /** Resolve a host once. False if the name does not resolve at all.
     *
     *  A NULL host asks for THIS MACHINE'S WILDCARD ADDRESSES - what a
     *  listener would bind to. It is the one multi-address list available
     *  with no network and no DNS, which is what test_transport uses to walk
     *  the list: a single-address host cannot tell a cursor that advances once
     *  from one that advances twice, and advancing twice is what crashed. */
    bool resolve(const char* host, uint16_t port) {
        reset();

        char service[8];
        snprintf(service, sizeof service, "%u", (unsigned)port);

        struct addrinfo hints;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;      // IPv4 and IPv6, in the order the
        hints.ai_socktype = SOCK_STREAM;  // resolver recommends (RFC 6724)
        if (host == nullptr) hints.ai_flags = AI_PASSIVE;

        if (getaddrinfo(host, service, &hints, &_results) != 0 || _results == nullptr) {
            _results = nullptr;
            return false;
        }
        _next = _results;
        return true;
    }

    /** How many candidates the resolver returned. */
    unsigned size() const {
        unsigned n = 0;
        for (const struct addrinfo* a = _results; a != nullptr; a = a->ai_next) n++;
        return n;
    }

    enum Attempt { Connected, InProgress, Exhausted };

    /** Open a socket on the next candidate and start connecting to it.
     *
     *  `socketOut` is set on Connected and InProgress, and left alone on
     *  Exhausted. A candidate whose socket cannot even be created is skipped
     *  rather than reported: it is this machine's limit, not that address's
     *  fault, and the next one may still work. */
    Attempt connectNext(int& socketOut) {
        // A `while`, and the cursor is advanced EXACTLY ONCE per candidate.
        //
        // The first version of this was a `for (; _next; _next = _next->ai_next)`
        // that also advanced inside the body - so it skipped every other
        // address, and on the last one it advanced a null pointer and
        // segfaulted. It only showed up on hosts with IPv6 addresses, because
        // those fail immediately on a machine with no IPv6 route and so reach
        // the fall-through; a host whose addresses all return EINPROGRESS
        // returns early and never gets there.
        while (_next != nullptr) {
            struct addrinfo* candidate = _next;
            _next = _next->ai_next;     // consumed, whatever happens below

            const int handle = (int)::socket(candidate->ai_family,
                                             candidate->ai_socktype,
                                             candidate->ai_protocol);
            if (handle < 0) continue;   // no socket of that family on this host
            if (!setNonBlocking(handle)) { MQ_CLOSE(handle); continue; }
            suppressSigPipe(handle);

            // A chosen source address, when one was asked for. A candidate of
            // another family (an IPv6 address for an IPv4 source) cannot use
            // it and is skipped, the same as one this host cannot reach.
            if (!_local.empty() && !bindLocal(handle, candidate->ai_family)) {
                MQ_CLOSE(handle);
                continue;
            }

            int one = 1;
            setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof one);

            const int result = ::connect(handle, candidate->ai_addr,
                                         (socklen_t)candidate->ai_addrlen);
            _tried++;
            socketOut = handle;
            if (result == 0) return Connected;
            if (MQ_IN_PROGRESS) return InProgress;

            // Refused outright - which is what an IPv6 address does on a
            // machine with no IPv6 route. Close it and take the next.
            MQ_CLOSE(handle);
        }
        return Exhausted;
    }

    /** Dial FROM this numeric address rather than whichever the routing
     *  table picks. Empty restores the default. A load generator uses it to
     *  spread connections over several source addresses: the kernel has only
     *  so many ephemeral ports per (source, destination) pair - about 28,000
     *  with Linux's default range - so one source address cannot open more
     *  connections than that to one broker port. */
    void setLocalAddress(const char* address) { _local = address ? address : ""; }

    /** How many were actually attempted, for an error an operator can read:
     *  "none of 6 addresses answered" says something "connection refused"
     *  does not. */
    unsigned tried() const { return _tried; }

private:
    std::string _local;

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

    struct addrinfo* _results = nullptr;
    struct addrinfo* _next = nullptr;
    unsigned _tried = 0;
    NameLookup* _lookup = nullptr;
    const char* _why = nullptr;
};

} // namespace mq_socket

#endif // MQTT_PLATFORM_HOST

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// Asking the kernel which connections have something to say.
//
// WHY THIS EXISTS. The broker used to ask every connection in turn whether it
// had anything, which costs a recv() per connection per poll whether or not
// there is anything to get. Measured on this broker with 200 clients and
// 20,000 messages: 41,173 recvfrom calls of which 20,373 returned EAGAIN, and
// 2,613 accept calls of which 2,413 returned EAGAIN. Half of every read was a
// syscall spent learning nothing, and 54% of all CPU time was in the kernel.
//
// A readiness set turns that around: the kernel already knows which
// descriptors are ready, and asking it once is cheaper than asking every
// socket separately.
//
// WHAT IT IS NOT. It does not read, write, or own anything. It is handed
// descriptors, it is asked which of them are ready, and that is all - so the
// transports keep their sockets and the broker keeps its abstraction.
//
// HOST ONLY, AND DELIBERATELY. A board has a handful of connections, no epoll
// to ask, and a scan cycle that must not block; MqttStreamTransport returns -1
// from waitHandle(), the broker finds no readiness source, and it polls
// exactly as it always has. None of this file is compiled there.

#include "../MqttPlatform.h"

#if defined(MQTT_PLATFORM_HOST)

#include <cstddef>
#include <cstdint>
#include <vector>

class MqttPollSet {
public:
    MqttPollSet();
    ~MqttPollSet();

    MqttPollSet(const MqttPollSet&) = delete;
    MqttPollSet& operator=(const MqttPollSet&) = delete;

    // Ready for its own sake: a set that could not be created is not an error,
    // it is a broker that carries on polling. Every caller must cope with
    // false, because that is also what a board gets.
    bool begin();
    void end();
    bool isOpen() const;

    // =========================================================================
    // PERSISTENT MODE - epoll, Linux only, and only when asked for
    // =========================================================================
    //
    // The mode above rebuilds the whole list every round, which is right for
    // a plant broker and costs O(connections) per poll at any scale - poll(2)
    // walks the list in the kernel and isReady() walks it again here. Past a
    // few thousand connections that walk IS the broker's time (measured: 81 ms
    // per poll at 21,000 connections; internal/CLOUD-PLAN.md, L4).
    //
    // In this mode a descriptor is registered ONCE, with a 64-bit token the
    // caller chooses, and a wait reports the tokens that became ready - so the
    // cost of a poll is the number of connections with something to say, not
    // the number connected.
    //
    // THE HAZARD THIS FILE WAS WRITTEN TO AVOID is a stale registration: a
    // descriptor closed and reopened as somebody else, still answering to the
    // old token. Two rules close it, and the caller owns both:
    //   - remove() BEFORE the descriptor is closed, so the kernel's interest
    //     list never holds a closed descriptor; and
    //   - put a GENERATION in the token, bumped every time a slot is reused,
    //     so an event already collected for the previous occupant is
    //     recognised and ignored.
    // MqttBroker does both (MqttBrokerConnection::generation).
    //
    // False where there is no epoll; the caller then uses begin() instead.
    bool beginPersistent();
    bool persistent() const { return _epoll >= 0; }

    // Level-triggered: a descriptor that still has data is reported again on
    // the next wait, so a caller that reads only some of it loses nothing.
    bool add(int handle, uint64_t token, bool wantWrite);
    bool update(int handle, uint64_t token, bool wantWrite);
    void remove(int handle);

    // After wait(), in persistent mode: the tokens that were ready.
    size_t readyCount() const { return _ready.size(); }
    uint64_t readyToken(size_t index) const { return _ready[index]; }

    // True once per wait(): whether the ready list was collected by a wait()
    // nobody has consumed yet. A daemon that sleeps on the set between polls
    // has just asked the kernel; the broker uses that answer instead of
    // asking again a microsecond later - which, at a thousand busy
    // connections, was a second epoll_wait per pass and several percent of
    // throughput.
    bool takeFresh() { const bool fresh = _fresh; _fresh = false; return fresh; }

    // Descriptors are re-declared every round rather than added and removed as
    // connections come and go. A broker's connection pool is walked once per
    // poll anyway, so building the list costs a pass it was already making -
    // and it cannot drift out of step with the pool, which a long-lived
    // registration can and does the first time a slot is recycled.
    void clear();
    void watch(int handle, bool forWriting);

    // Waits up to timeoutMs for any watched descriptor to become ready, and
    // returns how many did. Zero means the timeout elapsed with nothing ready.
    //
    // ONLY A STANDALONE DAEMON MAY PASS A NON-ZERO TIMEOUT. MqttBroker::poll()
    // promises never to wait for anything because a PLC calls it once per
    // scan; a timeout of 0 makes this a readiness QUERY, which is what the
    // broker uses it for.
    int wait(int timeoutMs);

    // True if that descriptor was ready in the last wait().
    bool isReady(int handle) const;

private:
    struct Watched {
        int handle;
        bool forWriting;
        bool ready;
    };
    std::vector<Watched> _watched;

    // The pollfd array, kept between calls so a thousand connections do not
    // allocate a thousand-entry vector every wait.
    //
    // RAW BYTES, SIZED IN THE .cpp, so this header does not drag <poll.h> (or
    // winsock) into everything that includes the broker. It must be a byte
    // buffer rather than an array of some stand-in struct: poll() is handed a
    // PACKED array and walks it by sizeof(struct pollfd), so a stand-in of the
    // wrong size gives every entry after the first a wrong stride. That is
    // exactly what happened here - 16-byte slots against an 8-byte pollfd, so
    // the kernel read garbage for every descriptor but one and the broker
    // stalled on its second connection.
    std::vector<unsigned char> _fds;
    bool _open;

    // Persistent mode. The event array is raw bytes for the same reason _fds
    // is: so this header does not pull <sys/epoll.h> into everything that
    // includes the broker.
    int _epoll = -1;
    std::vector<unsigned char> _events;
    std::vector<uint64_t> _ready;
    bool _fresh = false;
};

#endif  // MQTT_PLATFORM_HOST

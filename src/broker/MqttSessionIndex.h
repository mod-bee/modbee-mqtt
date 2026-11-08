// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// Sessions by Client Identifier, and free session slots, without a walk.
//
// WHY THIS EXISTS. Every CONNECT asked two questions of the session table -
// "is there a session for this identifier?" and "which slot is free?" - and
// answered both by walking it. Filling a broker therefore cost O(N^2): a test
// that built 70,000 sessions spent four minutes in those two loops under the
// sanitizers, and a broker accepting connections at 40,000 spent most of each
// poll there.
//
// WHAT IT IS. An open-addressing table of session indices, keyed by the hash
// of the identifier, with linear probing and backward-shift deletion - no
// tombstones, so it never degrades however many sessions come and go. And a
// stack of free slots. Both are sized once, in configure(), and never grow.
//
// WHO KEEPS IT RIGHT. The sessions themselves: MqttBrokerSession::adopt() and
// release() report to it. Keeping it from the broker's side would mean every
// place that releases a session remembering to say so, and there are half a
// dozen of them; the session has exactly two places where its identity
// changes.
//
// HOST ONLY. A board holds a dozen sessions, where the walk is cheaper than
// the table would be large.

#include "../MqttPlatform.h"

#if defined(MQTT_PLATFORM_HOST)

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>
#if defined(__linux__)
#include <sys/random.h>
#endif

// WHERE THE INDEX HASHES START, chosen once per process. FNV-1a from its
// published offset was a hash anybody could run ahead of time: client
// identifiers, or topic filters, made to share a bucket turn every lookup into
// a walk. One value for the whole process, because the routing index's keys
// are compared between shards.
inline uint32_t mq_indexHashSeed() {
    static const uint32_t seed = [] {
        uint32_t value = 0;
#if defined(__linux__)
        if (getrandom(&value, sizeof value, GRND_NONBLOCK) != (ssize_t)sizeof value) value = 0;
#endif
        // Elsewhere, or if the kernel had nothing: weaker, still not public.
        if (value == 0) value = ((uint32_t)(uintptr_t)&value ^ (uint32_t)mq_millis()) * 2654435761u;
        return value;
    }();
    return seed;
}

class MqttSessionIndex {
public:
    // An enumerator, not a static const member: assign() binds its argument
    // by reference, which before C++17 needs an out-of-line definition, and
    // this header has nowhere to put one.
    enum : uint32_t { kNone = 0xFFFFFFFFu };

    // `identifierOf(i)` gives session i's identifier; the index stores
    // indices, not strings, so there is one copy of each identifier.
    typedef const std::string& (*IdentifierFn)(const void* sessions, uint32_t index);

    // GROWN AS SESSIONS ARRIVE. `sessions` is the limit; the table starts at
    // 16 slots and doubles when half full, and the per-session arrays grow to
    // the highest session handed out. Everything here was sized to the limit
    // at start - about 40 bytes for each session a server might ever hold.
    void configure(size_t sessions, const void* table, IdentifierFn identifierOf) {
        _table = table;
        _identifierOf = identifierOf;
        _limit = sessions;
        _count = 0;
        _fresh = 0;
        _slots.assign(16, kNone);
        _mask = 15;
        std::vector<uint32_t>().swap(_hashes);
        std::vector<uint32_t>().swap(_free);
        std::vector<uint8_t>().swap(_inFree);
    }

    bool configured() const { return !_slots.empty(); }

    // --- The free list ---

    // Free slots handed out lowest first, as the walk did, so a broker's
    // behaviour does not change with the index underneath it: one given back
    // first, then the next never used.
    uint32_t takeFree() {
        while (!_free.empty()) {
            const uint32_t index = _free.back();
            _free.pop_back();
            if (index < _inFree.size() && _inFree[index]) {
                _inFree[index] = 0;
                return index;
            }
        }
        if (_fresh >= _limit) return kNone;
        const uint32_t index = (uint32_t)_fresh++;
        sizeFor(index);
        _inFree[index] = 0;
        return index;
    }

    void giveBack(uint32_t index) {
        if (index >= _inFree.size() || _inFree[index]) return;   // never twice
        _inFree[index] = 1;
        _free.push_back(index);
    }

    // A slot taken without takeFree() - adopt() on a slot the caller chose.
    void markTaken(uint32_t index) {
        if (index >= _limit) return;
        // Past the ones handed out: those below it are free, and listed.
        while (_fresh < index) {
            sizeFor((uint32_t)_fresh);
            _inFree[_fresh] = 1;
            _free.push_back((uint32_t)_fresh++);
        }
        if (_fresh == index) _fresh++;
        sizeFor(index);
        _inFree[index] = 0;
    }

    // --- The identifier table ---

    void insert(uint32_t index) {
        if (_slots.empty() || index >= _limit) return;
        sizeFor(index);
        // At most half full, so a free slot always exists.
        if ((_count + 1) * 2 > _slots.size()) grow();
        const std::string& id = _identifierOf(_table, index);
        const uint32_t hash = hashOf((const uint8_t*)id.data(), id.size());
        _hashes[index] = hash;
        if (place(index, hash)) _count++;
    }

    void erase(uint32_t index) {
        if (_slots.empty() || index >= _hashes.size()) return;   // never inserted
        size_t position = _hashes[index] & _mask;
        size_t probes = 0;
        while (_slots[position] != index) {
            if (_slots[position] == kNone || ++probes > _mask) return;   // not in
            position = (position + 1) & _mask;
        }
        // BACKWARD-SHIFT DELETION. Emptying the slot would cut the probe
        // chain of any entry that had to step past it, and that entry would
        // become unfindable - a session the broker could no longer see,
        // holding an identifier it would then hand out a second time.
        _slots[position] = kNone;
        if (_count > 0) _count--;
        size_t hole = position;
        size_t probe = position;
        for (;;) {
            probe = (probe + 1) & _mask;
            const uint32_t entry = _slots[probe];
            if (entry == kNone) return;
            const size_t home = _hashes[entry] & _mask;
            // Move the entry back into the hole unless its home lies
            // cyclically within (hole, probe] - in which case the hole is not
            // on its chain and moving it would put it before its own home.
            const bool homeBetween = (hole <= probe)
                ? (home > hole && home <= probe)
                : (home > hole || home <= probe);
            if (!homeBetween) {
                _slots[hole] = entry;
                _slots[probe] = kNone;
                hole = probe;
            }
        }
    }

    uint32_t find(const uint8_t* id, size_t length) const {
        if (_slots.empty()) return kNone;
        const uint32_t hash = hashOf(id, length);
        size_t position = hash & _mask;
        for (size_t probes = 0; probes <= _mask && _slots[position] != kNone; probes++) {
            const uint32_t index = _slots[position];
            if (_hashes[index] == hash) {
                const std::string& candidate = _identifierOf(_table, index);
                if (candidate.size() == length &&
                    (length == 0 || memcmp(candidate.data(), id, length) == 0)) {
                    return index;
                }
            }
            position = (position + 1) & _mask;
        }
        return kNone;
    }

private:
    // BOUNDED, here and in every loop of the index. The table is kept at most
    // half full, so a free slot always exists while its invariants hold - but
    // if they ever did not, an unbounded probe would hang the broker, and a
    // miss is the failure that can at least be seen. False when already there.
    bool place(uint32_t index, uint32_t hash) {
        size_t position = hash & _mask;
        for (size_t probes = 0; probes <= _mask; probes++) {
            if (_slots[position] == kNone) { _slots[position] = index; return true; }
            if (_slots[position] == index) return false;
            position = (position + 1) & _mask;
        }
        return false;
    }

    // Twice the slots, every entry placed again by the hash it was given.
    void grow() {
        std::vector<uint32_t> old;
        old.swap(_slots);
        _slots.assign(old.size() * 2, kNone);
        _mask = _slots.size() - 1;
        for (uint32_t index : old) {
            if (index != kNone) place(index, _hashes[index]);
        }
    }

    // The per-session arrays long enough to hold `index`.
    void sizeFor(uint32_t index) {
        if (index < _hashes.size()) return;
        size_t size = _hashes.empty() ? 64 : _hashes.size() * 2;
        if (size < (size_t)index + 1) size = (size_t)index + 1;
        if (size > _limit) size = _limit;
        _hashes.resize(size, 0);
        _inFree.resize(size, 0);
    }

    // FNV-1a. Identifiers are short and often differ only at the end
    // ("pump-0001", "pump-0002"), which it spreads well enough here.
    static uint32_t hashOf(const uint8_t* data, size_t length) {
        uint32_t hash = 2166136261u ^ mq_indexHashSeed();
        for (size_t index = 0; index < length; index++) {
            hash ^= data[index];
            hash *= 16777619u;
        }
        return hash;
    }

    const void* _table = nullptr;
    IdentifierFn _identifierOf = nullptr;
    std::vector<uint32_t> _slots;
    size_t _mask = 0;
    std::vector<uint32_t> _hashes;     // per session, its identifier's hash
    std::vector<uint32_t> _free;       // a stack of free session slots
    std::vector<uint8_t> _inFree;      // so a slot is never on it twice
    size_t _limit = 0;                 // sessions it may ever hold
    size_t _count = 0;                 // entries in _slots
    size_t _fresh = 0;                 // every slot below this has been handed out
};

#endif  // MQTT_PLATFORM_HOST

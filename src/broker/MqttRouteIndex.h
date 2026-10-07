// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// Which sessions might want a message on this topic - kept up to date one
// subscription at a time.
//
// WHY THIS EXISTS. The routing index the broker had was rebuilt from scratch
// whenever any subscription anywhere changed, so a reconnect storm - a
// hundred thousand devices each subscribing once, between publishes - rebuilt
// it a hundred thousand times. And every filter containing a wildcard sat in
// one list that EVERY publish walked, so a fleet in which each device
// subscribes to its own "devices/<id>/cmd/#" made every message visit every
// device (internal/CLOUD-PLAN.md, L5 and L6).
//
// THE IDEA. An MQTT filter can only match a topic that begins with the
// filter's LITERAL PREFIX - its levels before the first wildcard. So:
//
//   a filter with no wildcard     is filed under its whole text      (literal)
//   a filter with a wildcard      is filed under its literal prefix  (prefix)
//
//   "plant/tank-01/level"   literal  "plant/tank-01/level"
//   "devices/42/cmd/#"      prefix   "devices/42/cmd"
//   "plant/+/level"         prefix   "plant"
//   "#", "+/status"         prefix   ""  (every topic is a candidate)
//
// and a publish to "a/b/c" looks up the literal "a/b/c" and the prefixes "",
// "a", "a/b", "a/b/c" - one lookup per level, whatever the number of
// subscriptions. "devices/42/cmd/#" is offered for device 42's commands and
// for nothing else.
//
// IT ONLY HAS TO BE A SUPERSET. Every session it offers is still checked
// against the topic with mq_topicMatches(), exactly as before, so an index
// that offers a session too many costs a comparison and an index that offers
// too FEW would lose a message. Hash collisions and empty topic levels only
// ever add candidates; nothing here can remove one that belongs.
//
// INCREMENTAL. Subscribing adds one entry, unsubscribing removes one, and the
// sessions themselves report both (MqttBrokerSession), so there is no rebuild.
//
// HOST ONLY, like the other indexes: a board's few dozen subscriptions are
// cheaper to walk.

#include "../MqttPlatform.h"

#if defined(MQTT_PLATFORM_HOST)

#include "../MqttWire.h"

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

class MqttRouteIndex {
public:
    enum : uint32_t { kNone = 0xFFFFFFFFu };

    // `buckets` is rounded up to a power of two. `reserveEntries` is how many
    // subscriptions to make room for now; more are added as they come.
    void configure(size_t buckets, size_t reserveEntries);
    bool configured() const { return !_heads.empty(); }

    void add(uint32_t session, const std::string& filter);
    void remove(uint32_t session, const std::string& filter);

    // Appends every session that MIGHT have a filter matching the topic.
    // May repeat a session; the caller removes repeats.
    void candidates(const MqttSpan& topic, std::vector<uint32_t>& out) const;

    size_t entries() const { return _entries; }

    // --- For a server running one broker per core ---
    //
    // Every key a subscription is filed under, as it is added (+1) and
    // removed (-1). A multi-core server keeps a count per key per broker, so
    // another broker can ask "might anybody on you want this topic?" without
    // touching this index - and send a message only where the answer is yes.
    typedef void (*KeyObserver)(void* context, uint32_t key, int delta);
    void observe(KeyObserver observer, void* context) { _observer = observer; _observerContext = context; }

    // Shared-subscription membership: a session joined (+1) or left (-1) the
    // group `name` on `filter` (the filter inside "$share/name/"). A server
    // running several brokers counts members per broker, so the broker a
    // message is published on can choose, for the whole server, which broker
    // delivers each group's one copy. Reported by the sessions, which know
    // the share name; this index only carries the observer.
    typedef void (*ShareObserver)(void* context, const std::string& name,
                                  const std::string& filter, int delta);
    void observeShares(ShareObserver observer, void* context) {
        _shareObserver = observer;
        _shareObserverContext = context;
    }
    void shareMembership(const std::string& name, const std::string& filter, int delta) const {
        if (_shareObserver) _shareObserver(_shareObserverContext, name, filter, delta);
    }

    // Every key a topic is looked up under: the same keys candidates() uses,
    // so a broker whose counts for all of them are zero cannot have a match.
    typedef void (*KeyVisitor)(void* context, uint32_t key);
    static void forEachTopicKey(const MqttSpan& topic, KeyVisitor visit, void* context);

private:
    struct Entry {
        uint32_t session = 0;
        uint32_t key = 0;        // the full hash, so a bucket shared by another key is skipped
        uint32_t next = kNone;
    };

    std::vector<uint32_t> _heads;    // bucket -> first entry
    std::vector<Entry> _pool;
    uint32_t _free = kNone;          // the free entries, linked through `next`
    size_t _mask = 0;
    size_t _entries = 0;
    KeyObserver _observer = nullptr;
    void* _observerContext = nullptr;
    ShareObserver _shareObserver = nullptr;
    void* _shareObserverContext = nullptr;

    // The two kinds hash differently, so the literal "a/b" and the prefix
    // "a/b" (of "a/b/#") are different keys.
    static uint32_t keyOf(bool prefix, const uint8_t* data, size_t length);
    static uint32_t keyOfFilter(const std::string& filter);
    void appendBucket(uint32_t key, std::vector<uint32_t>& out) const;
};

#endif  // MQTT_PLATFORM_HOST

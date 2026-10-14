// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttRouteIndex.h"
#include "../MqttNoExcept.h"

#if defined(MQTT_PLATFORM_HOST)

void MqttRouteIndex::configure(size_t buckets, size_t reserveEntries) {
    size_t size = 16;
    while (size < buckets) size *= 2;
    _heads.assign(size, kNone);
    _mask = size - 1;
    _pool.clear();
    _pool.reserve(reserveEntries);
    _free = kNone;
    _entries = 0;
}

uint32_t MqttRouteIndex::keyOf(bool prefix, const uint8_t* data, size_t length) {
    // FNV-1a, seeded differently for the two kinds.
    uint32_t hash = prefix ? 0x811C9DC5u : 0x050C5D1Fu;
    for (size_t index = 0; index < length; index++) {
        hash ^= data[index];
        hash *= 16777619u;
    }
    return hash;
}

// A filter's key: its literal prefix if it has a wildcard, its whole text if
// not. The prefix is everything before the first level that IS a wildcard -
// in a valid filter a level containing '+' or '#' is exactly that character
// [V5:4.7.1], so finding the first '+' or '#' finds that level.
uint32_t MqttRouteIndex::keyOfFilter(const std::string& filter) {
    const uint8_t* data = (const uint8_t*)filter.data();
    for (size_t index = 0; index < filter.size(); index++) {
        if (data[index] == '+' || data[index] == '#') {
            // The levels before it, without the separator that ends them:
            // "a/b/+" -> "a/b", "+/x" -> "".
            const size_t prefix = index > 0 ? index - 1 : 0;
            return keyOf(true, data, prefix);
        }
    }
    return keyOf(false, data, filter.size());
}

void MqttRouteIndex::add(uint32_t session, const std::string& filter) {
    if (_heads.empty()) return;
    uint32_t entry;
    if (_free != kNone) {
        entry = _free;
        _free = _pool[entry].next;
    } else {
        entry = (uint32_t)_pool.size();
        _pool.push_back(Entry());
    }
    const uint32_t key = keyOfFilter(filter);
    const size_t bucket = key & _mask;
    _pool[entry].session = session;
    _pool[entry].key = key;
    _pool[entry].next = _heads[bucket];
    _heads[bucket] = entry;
    _entries++;
    if (_observer) _observer(_observerContext, key, +1);
}

void MqttRouteIndex::remove(uint32_t session, const std::string& filter) {
    if (_heads.empty()) return;
    const uint32_t key = keyOfFilter(filter);
    const size_t bucket = key & _mask;
    // Any one entry for this session under this key. A session holding two
    // filters with the same prefix ("a/+/x", "a/+/y") has two identical
    // entries, and removing either leaves the right count behind.
    uint32_t* link = &_heads[bucket];
    while (*link != kNone) {
        Entry& candidate = _pool[*link];
        if (candidate.session == session && candidate.key == key) {
            const uint32_t gone = *link;
            *link = candidate.next;
            _pool[gone].next = _free;
            _free = gone;
            if (_entries > 0) _entries--;
            if (_observer) _observer(_observerContext, key, -1);
            return;
        }
        link = &candidate.next;
    }
}

void MqttRouteIndex::appendBucket(uint32_t key, std::vector<uint32_t>& out) const {
    for (uint32_t entry = _heads[key & _mask]; entry != kNone; entry = _pool[entry].next) {
        if (_pool[entry].key == key) out.push_back(_pool[entry].session);
    }
}

void MqttRouteIndex::forEachTopicKey(const MqttSpan& topic, KeyVisitor visit, void* context) {
    const uint8_t* data = topic.data;
    const size_t length = topic.length;

    // Filters with no wildcard: the whole topic.
    visit(context, keyOf(false, data, length));

    // Filters with one: every prefix of whole levels, from none to all of
    // them - "a/#" matches "a" itself [V5:4.7.1.2], so the full topic is a
    // prefix too.
    visit(context, keyOf(true, data, 0));
    for (size_t index = 0; index < length; index++) {
        if (data[index] == '/') visit(context, keyOf(true, data, index));
    }
    if (length > 0) visit(context, keyOf(true, data, length));
}

namespace {
struct Collect {
    const MqttRouteIndex* index;
    std::vector<uint32_t>* out;
};
}

void MqttRouteIndex::candidates(const MqttSpan& topic, std::vector<uint32_t>& out) const {
    if (_heads.empty()) return;
    Collect collect;
    collect.index = this;
    collect.out = &out;
    forEachTopicKey(topic, [](void* context, uint32_t key) {
        Collect& c = *static_cast<Collect*>(context);
        c.index->appendBucket(key, *c.out);
    }, &collect);
}

#endif  // MQTT_PLATFORM_HOST

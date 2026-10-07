// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// A bounded queue between exactly one producer thread and one consumer
// thread, with no lock.
//
// The shards of a multi-core broker talk only through these: one ring for
// every ordered pair of shards, so each ring has one writer and one reader by
// construction and needs nothing heavier than two atomic indices.
//
// BOUNDED, AND IT SAYS WHEN IT IS FULL. push() returns false rather than
// growing or waiting; the caller decides what a full ring means - which for a
// message is "that shard could not take it", reported, and never "it was
// delivered".

#include <atomic>
#include <cstddef>
#include <vector>

template <typename T>
class SpscRing {
public:
    explicit SpscRing(size_t capacity) {
        size_t size = 2;
        while (size < capacity) size *= 2;
        _slots.resize(size);
        _mask = size - 1;
    }

    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    // Producer side.
    bool push(const T& value) {
        const size_t tail = _tail.load(std::memory_order_relaxed);
        const size_t head = _head.load(std::memory_order_acquire);
        if (tail - head > _mask) return false;
        _slots[tail & _mask] = value;
        // Release: whatever the producer wrote before this - including into
        // objects `value` points at - is visible to the consumer that sees it.
        _tail.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Consumer side.
    bool pop(T& out) {
        const size_t head = _head.load(std::memory_order_relaxed);
        const size_t tail = _tail.load(std::memory_order_acquire);
        if (head == tail) return false;
        out = _slots[head & _mask];
        _head.store(head + 1, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return _head.load(std::memory_order_acquire) == _tail.load(std::memory_order_acquire);
    }

private:
    std::vector<T> _slots;
    size_t _mask = 0;
    // On their own cache lines, so the producer and consumer do not slow each
    // other down by writing to the same one.
    alignas(64) std::atomic<size_t> _head{0};
    alignas(64) std::atomic<size_t> _tail{0};
};

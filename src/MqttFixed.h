#pragma once

// =============================================================================
// FIXED STORAGE FOR A BOARD
// =============================================================================
//
// On a host the library holds its text and bytes in std::string and
// std::vector, and that does not change: the broker, the Kafka pipe and the
// CLI are tuned around them. On a board (MQTT_LEAN, MqttFeatures.h) the same
// members are fixed storage, sized once, so nothing is allocated while the
// controller runs and no standard-library container code is linked:
//
//   MQ_STRING(n)   text of up to n characters          std::string on a host
//   MQ_BYTES(n)    up to n bytes                       std::vector<uint8_t>
//   MQ_LIST(T, n)  up to n elements                    std::vector<T>
//   MQ_AREA_BYTES  bytes kept in a shared MqttByteArea std::vector<uint8_t>
//   MQ_HEAP_BYTES  a buffer allocated once, at begin() std::vector<uint8_t>
//   MQ_HEAP_LIST(T) an array allocated once, when sized std::vector<T>
//   MQ_STRING_REF / MQ_BYTES_REF   a parameter that takes any of them
//
// On a host each macro is the very type it replaces, token for token, so the
// host build compiles exactly as before (test/wire/host_same.sh proves it).
// The board types offer the part of the std interface the library uses.
//
// What a board gives up: text longer than its field is cut to the field (the
// PLC blocks' own STRING pins are shorter than the fields), and a byte area
// that is full refuses the next message ("queue full") rather than growing.
//
// One type is the same on both: MqttGrowList, at the end, for the tables a
// program fills as it starts (topics, subscriptions, devices, accounts, the
// send queue's slots). Nothing caps those but memory, on a host or a board.

#include "MqttFeatures.h"

#if !MQTT_LEAN

#define MQ_STRING(capacity) std::string
#define MQ_BYTES(capacity) std::vector<uint8_t>
#define MQ_LIST(type, capacity) std::vector<type>
#define MQ_AREA_BYTES std::vector<uint8_t>
#define MQ_OWNED_BYTES std::vector<uint8_t>
#define MQ_HEAP_BYTES std::vector<uint8_t>
#define MQ_HEAP_LIST(type) std::vector<type>
#define MQ_STRING_REF std::string
#define MQ_BYTES_REF std::vector<uint8_t>

#else

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <new>

#define MQ_STRING(capacity) MqttFixedText<capacity>
#define MQ_BYTES(capacity) MqttFixedBytes<capacity>
#define MQ_LIST(type, capacity) MqttFixedList<type, capacity>
#define MQ_AREA_BYTES MqttAreaBytes
#define MQ_OWNED_BYTES MqttOwnedBytes
#define MQ_HEAP_BYTES MqttHeapBytes
#define MQ_HEAP_LIST(type) MqttHeapList<type>
#define MQ_STRING_REF MqttText
#define MQ_BYTES_REF MqttBytes

// --- Text ---
//
// The base holds the size and a pointer to the derived class's characters,
// so a function can take any field by `const MqttText&`. Always terminated.
class MqttText {
public:
    const char* c_str() const noexcept { return _data; }
    const char* data() const noexcept { return _data; }
    size_t size() const noexcept { return _size; }
    size_t length() const noexcept { return _size; }
    size_t capacity() const noexcept { return _capacity; }
    bool empty() const noexcept { return _size == 0; }
    void clear() noexcept { _size = 0; _data[0] = 0; }

    // Cut to the capacity.
    void assign(const char* text, size_t length) noexcept;
    MqttText& operator=(const char* text) noexcept {
        assign(text, text ? strlen(text) : 0);
        return *this;
    }
    MqttText& operator=(const MqttText& other) noexcept {
        if (this != &other) assign(other._data, other._size);
        return *this;
    }
    // From anything with data() and size(): a std::string a caller holds.
    template <typename Text>
    auto operator=(const Text& text) noexcept -> decltype((void)text.data(), (void)text.size(), *this) {
        assign(text.data(), text.size());
        return *this;
    }

    // Appends, cut to the capacity.
    void append(const char* text, size_t length) noexcept;
    MqttText& operator+=(const char* text) noexcept {
        append(text, text ? strlen(text) : 0);
        return *this;
    }
    template <typename Text>
    auto operator+=(const Text& text) noexcept -> decltype((void)text.data(), (void)text.size(), *this) {
        append(text.data(), text.size());
        return *this;
    }

    char operator[](size_t index) const noexcept { return _data[index]; }
    bool operator==(const MqttText& other) const noexcept {
        return _size == other._size && memcmp(_data, other._data, _size) == 0;
    }
    bool operator==(const char* text) const noexcept {
        const size_t length = text ? strlen(text) : 0;
        return _size == length && memcmp(_data, text, length) == 0;
    }
    bool operator!=(const MqttText& other) const noexcept { return !(*this == other); }
    bool operator!=(const char* text) const noexcept { return !(*this == text); }

protected:
    MqttText(char* storage, size_t capacity) noexcept
        : _data(storage), _capacity((uint16_t)capacity) {
        storage[0] = 0;
    }
    MqttText(const MqttText&) = delete;

private:
    char* _data;
    uint16_t _size = 0;
    uint16_t _capacity;
};

template <size_t N>
class MqttFixedText : public MqttText {
public:
    MqttFixedText() noexcept : MqttText(_text, N) {}
    MqttFixedText(const char* text) noexcept : MqttText(_text, N) { MqttText::operator=(text); }
    MqttFixedText(const MqttFixedText& other) noexcept : MqttText(_text, N) {
        assign(other.data(), other.size());
    }
    MqttFixedText& operator=(const MqttFixedText& other) noexcept {
        MqttText::operator=(other);
        return *this;
    }
    using MqttText::operator=;

private:
    char _text[N + 1];
};

// --- Bytes ---
class MqttBytes {
public:
    const uint8_t* data() const noexcept { return _data; }
    uint8_t* data() noexcept { return _data; }
    size_t size() const noexcept { return _size; }
    size_t capacity() const noexcept { return _capacity; }
    bool empty() const noexcept { return _size == 0; }
    void clear() noexcept { _size = 0; }
    // Sets the size, up to the capacity; new bytes are left as they are.
    void resize(size_t size) noexcept { _size = (uint16_t)(size < _capacity ? size : _capacity); }
    const uint8_t* begin() const noexcept { return _data; }
    const uint8_t* end() const noexcept { return _data + _size; }

    // Cut to the capacity.
    void assign(const uint8_t* first, const uint8_t* last) noexcept;
    // From any other iterators: a std::string a caller holds.
    template <typename Iterator>
    void assign(Iterator first, Iterator last) noexcept {
        _size = 0;
        for (; first != last && _size < _capacity; ++first) _data[_size++] = (uint8_t)*first;
    }
    MqttBytes& operator=(const MqttBytes& other) noexcept {
        if (this != &other) assign(other.begin(), other.end());
        return *this;
    }

    uint8_t operator[](size_t index) const noexcept { return _data[index]; }
    uint8_t& operator[](size_t index) noexcept { return _data[index]; }
    bool operator==(const MqttBytes& other) const noexcept {
        return _size == other._size && memcmp(_data, other._data, _size) == 0;
    }
    bool operator!=(const MqttBytes& other) const noexcept { return !(*this == other); }

protected:
    MqttBytes(uint8_t* storage, size_t capacity) noexcept
        : _data(storage), _capacity((uint16_t)capacity) {}
    MqttBytes(const MqttBytes&) = delete;

private:
    uint8_t* _data;
    uint16_t _size = 0;
    uint16_t _capacity;
};

template <size_t N>
class MqttFixedBytes : public MqttBytes {
public:
    MqttFixedBytes() noexcept : MqttBytes(_bytes, N) {}
    MqttFixedBytes(const MqttFixedBytes& other) noexcept : MqttBytes(_bytes, N) {
        assign(other.begin(), other.end());
    }
    MqttFixedBytes& operator=(const MqttFixedBytes& other) noexcept {
        MqttBytes::operator=(other);
        return *this;
    }
    using MqttBytes::operator=;

private:
    uint8_t _bytes[N];
};

// --- A list of up to N elements ---
template <typename T, size_t N>
class MqttFixedList {
public:
    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }
    bool full() const noexcept { return _size == N; }
    void clear() noexcept { resize(0); }

    // Returns false when full, where a std::vector would have grown.
    bool push_back(const T& item) noexcept {
        if (_size == N) return false;
        _items[_size++] = item;
        return true;
    }
    void pop_back() noexcept {
        if (_size) _items[--_size] = T();
    }
    void resize(size_t size) noexcept {
        if (size > N) size = N;
        for (size_t i = size; i < _size; i++) _items[i] = T();
        _size = size;
    }
    void erase(T* at) noexcept { erase(at, at + 1); }
    void erase(T* first, T* last) noexcept {
        if (first < begin()) first = begin();
        if (last > end()) last = end();
        if (first >= last) return;
        const size_t gone = (size_t)(last - first);
        for (T* next = last; next < end(); ++next) *(next - gone) = *next;
        resize(_size - gone);
    }
    void swap(MqttFixedList& other) noexcept {
        const size_t most = _size > other._size ? _size : other._size;
        for (size_t i = 0; i < most; i++) {
            T held = _items[i];
            _items[i] = other._items[i];
            other._items[i] = held;
        }
        const size_t size = _size; _size = other._size; other._size = size;
    }
    void reserve(size_t) {}   // fixed already

    T& operator[](size_t index) noexcept { return _items[index]; }
    const T& operator[](size_t index) const noexcept { return _items[index]; }
    T& back() noexcept { return _items[_size - 1]; }
    T* begin() noexcept { return _items; }
    T* end() noexcept { return _items + _size; }
    const T* begin() const noexcept { return _items; }
    const T* end() const noexcept { return _items + _size; }

private:
    T _items[N];
    size_t _size = 0;
};

// --- A buffer allocated once, when it is sized ---
//
// MqttLink's buffers: sized at begin(), allocated then and only again if the
// size changes.
class MqttHeapBytes {
public:
    MqttHeapBytes() noexcept = default;
    explicit MqttHeapBytes(size_t size) noexcept { resize(size); }
    ~MqttHeapBytes() noexcept { delete[] _data; }
    MqttHeapBytes(const MqttHeapBytes&) = delete;
    MqttHeapBytes& operator=(const MqttHeapBytes&) = delete;
    // Moved as a std::vector is: the buffer goes with it.
    MqttHeapBytes(MqttHeapBytes&& other) noexcept { swap(other); }
    MqttHeapBytes& operator=(MqttHeapBytes&& other) noexcept {
        swap(other);
        return *this;
    }

    // The contents are not kept: the link resizes an empty buffer.
    void resize(size_t size) noexcept {
        if (size == _size) return;
        delete[] _data;
        _data = size ? new (std::nothrow) uint8_t[size] : nullptr;
        _size = _data ? size : 0;
    }
    void swap(MqttHeapBytes& other) noexcept {
        uint8_t* data = _data; _data = other._data; other._data = data;
        size_t size = _size; _size = other._size; other._size = size;
    }
    uint8_t* data() noexcept { return _data; }
    const uint8_t* data() const noexcept { return _data; }
    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }
    uint8_t& operator[](size_t index) noexcept { return _data[index]; }
    uint8_t operator[](size_t index) const noexcept { return _data[index]; }

private:
    uint8_t* _data = nullptr;
    size_t _size = 0;
};

// --- An array allocated once, when it is sized ---
//
// A Sparkplug metric's write queue: sized to the queue depth when the node
// connects, and never again.
template <typename T>
class MqttHeapList {
public:
    MqttHeapList() noexcept = default;
    ~MqttHeapList() noexcept { delete[] _items; }
    MqttHeapList(const MqttHeapList&) = delete;
    MqttHeapList& operator=(const MqttHeapList&) = delete;
    MqttHeapList(MqttHeapList&& other) noexcept : _items(other._items), _size(other._size) {
        other._items = nullptr;
        other._size = 0;
    }

    void resize(size_t size) noexcept {
        if (size == _size) return;
        delete[] _items;
        _items = size ? new (std::nothrow) T[size] : nullptr;
        _size = _items ? size : 0;
    }
    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }
    T& operator[](size_t index) noexcept { return _items[index]; }
    const T& operator[](size_t index) const noexcept { return _items[index]; }

private:
    T* _items = nullptr;
    size_t _size = 0;
};

// --- A large block, in PSRAM where the board has it (mq_allocLarge) ---
class MqttLargeBytes {
public:
    MqttLargeBytes() noexcept = default;
    ~MqttLargeBytes() { mq_freeLarge(_data); }
    MqttLargeBytes(const MqttLargeBytes&) = delete;
    MqttLargeBytes& operator=(const MqttLargeBytes&) = delete;

    // A new block of `size` bytes, or false and the old one kept.
    bool replace(size_t size) noexcept {
        uint8_t* data = (uint8_t*)mq_allocLarge(size);
        if (!data) return false;
        _old = _data;
        _data = data;
        _size = size;
        return true;
    }
    // Frees the block replace() took the place of, once its bytes are moved.
    void releaseOld() noexcept {
        mq_freeLarge(_old);
        _old = nullptr;
    }
    uint8_t* data() noexcept { return _data; }
    size_t size() const noexcept { return _size; }

private:
    uint8_t* _data = nullptr;
    uint8_t* _old = nullptr;
    size_t _size = 0;
};

// --- One byte area for many variable-length entries ---
//
// A client keeps every queued topic and payload and every resend copy here,
// in one block sized when the client is begun - from its queue size, so a
// queue of any size has the room it asks for (MqttClient::begin). Entries stay
// packed: a released entry's bytes are closed up at once, so the area never
// fragments and "full" means full. Room can be RESERVED ahead of use (a queued
// QoS 1/2 publish reserves its resend copy), so what was promised is always
// there.
//
// The entry table is sized the same way (entries()): MQTT_AREA_ENTRIES for
// the default queue of MQTT_QUEUE_CAPACITY, in proportion above it.
#ifndef MQTT_AREA_ENTRIES
#define MQTT_AREA_ENTRIES 128
#endif

class MqttByteArea {
public:
    typedef uint16_t Entry;
    static const Entry kNone = 0xFFFF;     // no entry
    static const Entry kFailed = 0xFFFE;   // an entry the area could not take
    static const size_t kMaxEntries = 0xFFFE;

    MqttByteArea() noexcept = default;
    ~MqttByteArea() noexcept;
    MqttByteArea(const MqttByteArea&) = delete;
    MqttByteArea& operator=(const MqttByteArea&) = delete;

    void begin(uint8_t* bytes, size_t capacity) noexcept {
        _bytes = bytes;
        _capacity = capacity;
    }
    // To a larger block, bytes and entries kept (they are offsets).
    void move(uint8_t* bytes, size_t capacity) noexcept {
        if (_used) memcpy(bytes, _bytes, _used);
        begin(bytes, capacity);
    }
    // Room for at least `count` entries, those in use kept. False, and the
    // table as it was, when the memory is not there.
    bool entries(size_t count) noexcept;
    size_t entries() const noexcept { return _entries; }

    size_t capacity() const noexcept { return _capacity; }
    size_t used() const noexcept { return _used; }
    size_t reserved() const noexcept { return _reserved; }
    size_t available() const noexcept { return _capacity - _used - _reserved; }

    bool reserve(size_t length) noexcept {
        if (length > available()) return false;
        _reserved += length;
        return true;
    }
    void unreserve(size_t length) noexcept {
        _reserved -= length < _reserved ? length : _reserved;
    }

    // A new entry holding a copy of `length` bytes, or kFailed when there is
    // no room or no free entry.
    Entry put(const uint8_t* bytes, size_t length) noexcept ;
    void release(Entry entry) noexcept;

    bool contains(const uint8_t* bytes) const noexcept {
        return _bytes && bytes >= _bytes && bytes < _bytes + _capacity;
    }

    uint8_t* at(Entry entry) noexcept { return _bytes + _offset[entry]; }
    const uint8_t* at(Entry entry) const noexcept { return _bytes + _offset[entry]; }

private:
    static const uint16_t kFree = 0xFFFF;
    uint8_t* _bytes = nullptr;
    size_t _capacity = 0;
    size_t _used = 0;
    size_t _reserved = 0;
    // Offsets are 32-bit: an area for a large queue, or for two messages of
    // more than 32 kB, is past what 16 bits can address.
    uint32_t* _offset = nullptr;
    uint16_t* _length = nullptr;
    size_t _entries = 0;
};

// --- Bytes held in an area ---
//
// BOUND to an area (bind()), a member owns a copy of its bytes there: what is
// assigned to it is copied in, and the copy is released when it is cleared,
// reassigned or destroyed. If the area cannot take it, it holds nothing and
// failed() says so - the queue checks for room first, so that is never seen.
//
// NOT BOUND, it is a view of the caller's bytes and copies nothing: that is
// what an operation being built on the stack holds until the queue copies it
// into a bound slot.
class MqttAreaBytes {
public:
    MqttAreaBytes() noexcept = default;
    template <typename Byte>
    MqttAreaBytes(const Byte* first, const Byte* last) noexcept
        : _view((const uint8_t*)first), _size((uint16_t)(last - first)) {
        static_assert(sizeof(Byte) == 1, "bytes or characters");
    }
    MqttAreaBytes(const MqttAreaBytes& other) noexcept
        : _view(other.data()), _size(other._size) {}
    // Moved: the entry goes with it, nothing is copied (a queue's slots move
    // when the queue grows).
    MqttAreaBytes(MqttAreaBytes&& other) noexcept
        : _area(other._area), _view(other._view), _size(other._size),
          _entry(other._entry) {
        other._view = nullptr;
        other._size = 0;
        other._entry = MqttByteArea::kNone;
    }
    ~MqttAreaBytes() noexcept { drop(); }

    void bind(MqttByteArea& area) noexcept {
        drop();
        _area = &area;
    }
    bool bound() const noexcept { return _area != nullptr; }
    bool failed() const noexcept { return _entry == MqttByteArea::kFailed; }

    MqttAreaBytes& operator=(const MqttAreaBytes& other) noexcept;

    void assign(const uint8_t* first, const uint8_t* last) noexcept;
    void clear() noexcept { drop(); }

    const uint8_t* data() const noexcept {
        return held() ? _area->at(_entry) : _view;
    }
    // A view's bytes are the caller's; only a slot (only in a test) is ever
    // one, and nothing writes through it.
    uint8_t* data() noexcept {
        return held() ? _area->at(_entry) : const_cast<uint8_t*>(_view);
    }
    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }
    const uint8_t* begin() const noexcept { return data(); }
    const uint8_t* end() const noexcept { return data() + _size; }

    uint8_t operator[](size_t index) const noexcept { return data()[index]; }
    uint8_t& operator[](size_t index) noexcept { return data()[index]; }
    bool operator==(const MqttAreaBytes& other) const noexcept {
        return _size == other._size &&
               (_size == 0 || memcmp(data(), other.data(), _size) == 0);
    }
    bool operator!=(const MqttAreaBytes& other) const noexcept { return !(*this == other); }

private:
    void drop() noexcept;
    bool held() const noexcept { return _entry < MqttByteArea::kFailed; }

    MqttByteArea* _area = nullptr;
    const uint8_t* _view = nullptr;
    uint16_t _size = 0;
    MqttByteArea::Entry _entry = MqttByteArea::kNone;   // or kFailed
};

// --- Bytes of its own, on the heap ---
//
// A session's subscription filter: one allocation the size of the filter, so
// a program's subscriptions are limited by memory and not by the byte area,
// which is sized for the queue. Moves, never copies. Read as a view of the
// area kind where a queued operation wants one.
class MqttOwnedBytes {
public:
    MqttOwnedBytes() noexcept = default;
    ~MqttOwnedBytes() noexcept { delete[] _data; }
    MqttOwnedBytes(const MqttOwnedBytes&) = delete;
    MqttOwnedBytes& operator=(const MqttOwnedBytes&) = delete;
    MqttOwnedBytes(MqttOwnedBytes&& other) noexcept { swap(other); }
    MqttOwnedBytes& operator=(MqttOwnedBytes&& other) noexcept {
        swap(other);
        return *this;
    }

    // A copy of [first, last), or nothing and failed() when memory is out.
    void assign(const uint8_t* first, const uint8_t* last) noexcept {
        const size_t size = (size_t)(last - first);
        delete[] _data;
        _data = size ? new (std::nothrow) uint8_t[size] : nullptr;
        _failed = size && !_data;
        _size = _data ? size : 0;
        if (_size) memcpy(_data, first, _size);
    }
    bool failed() const noexcept { return _failed; }
    const uint8_t* data() const noexcept { return _data; }
    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }
    operator MqttAreaBytes() const noexcept { return MqttAreaBytes(_data, _data + _size); }

private:
    void swap(MqttOwnedBytes& other) noexcept {
        uint8_t* data = _data; _data = other._data; other._data = data;
        size_t size = _size; _size = other._size; other._size = size;
        bool failed = _failed; _failed = other._failed; other._failed = failed;
    }
    uint8_t* _data = nullptr;
    size_t _size = 0;
    bool _failed = false;
};

#endif  // MQTT_LEAN

// =============================================================================
// ON A HOST AND A BOARD ALIKE
// =============================================================================

#include <stddef.h>
#include <new>

// --- A table that grows with what the program declares ---
//
// Topics, subscriptions, devices, broker accounts, a host's tracked nodes: the
// tables a PLC program fills as its blocks start. They have no compile-time
// ceiling - an OpenPLC project cannot pass -D, so a ceiling there is one nobody
// can raise - and are limited by memory alone, on a board in PSRAM where it
// has some (mq_allocLarge). The same on a host (the Runtime, the broker): none
// of these tables is on a per-message path, and each grows only when
// something is declared, subscribed or connected. Doubles from four, or
// exactly what is asked (reserveExactly). A growth that fails keeps what it
// had and returns false, for the owner to report; nothing throws. Items keep
// their values when it grows (moved), not their addresses.
template <typename T>
class MqttGrowList {
public:
    MqttGrowList() noexcept = default;
    ~MqttGrowList() noexcept { release(); }
    MqttGrowList(const MqttGrowList&) = delete;
    MqttGrowList& operator=(const MqttGrowList&) = delete;
    // Moved, so a table of tables can grow too.
    MqttGrowList(MqttGrowList&& other) noexcept { swap(other); }
    MqttGrowList& operator=(MqttGrowList&& other) noexcept {
        swap(other);
        return *this;
    }

    size_t capacity() const noexcept { return _capacity; }

    // Room for at least `wanted` items, the new ones default-constructed.
    bool reserve(size_t wanted) noexcept { return grow(wanted, false); }
    // Room for `wanted` items and no more, where the owner walks every one.
    bool reserveExactly(size_t wanted) noexcept { return grow(wanted, true); }

    T& operator[](size_t index) noexcept { return _items[index]; }
    const T& operator[](size_t index) const noexcept { return _items[index]; }

private:
    bool grow(size_t wanted, bool exact) noexcept {
        if (wanted <= _capacity) return true;
        size_t capacity = exact ? wanted : (_capacity ? _capacity * 2 : 4);
        if (capacity < wanted) capacity = wanted;
        if (capacity > (size_t)-1 / sizeof(T)) return false;
        T* grown = (T*)mq_allocLarge(capacity * sizeof(T));
        if (!grown) return false;
        for (size_t i = 0; i < capacity; i++) {
            if (i < _capacity) new (&grown[i]) T(static_cast<T&&>(_items[i]));
            else new (&grown[i]) T();
        }
        release();
        _items = grown;
        _capacity = capacity;
        return true;
    }
    void swap(MqttGrowList& other) noexcept {
        T* items = _items; _items = other._items; other._items = items;
        size_t capacity = _capacity; _capacity = other._capacity; other._capacity = capacity;
    }
    void release() noexcept {
        for (size_t i = 0; i < _capacity; i++) _items[i].~T();
        mq_freeLarge(_items);
        _items = nullptr;
        _capacity = 0;
    }
    T* _items = nullptr;
    size_t _capacity = 0;
};

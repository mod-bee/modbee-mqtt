// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

// A board's fixed storage (MqttFixed.h). Out of line so each piece is in the
// firmware once rather than wherever it is used. Nothing here on a host.

// No code on a host; a board (or a host test that asks for MQTT_LEAN) gets it.
#include "MqttFixed.h"

#if MQTT_LEAN

#include "MqttNoExcept.h"

// --- Text and bytes ---

void MqttText::assign(const char* text, size_t length) noexcept {
    if (!text) length = 0;
    if (length > _capacity) length = _capacity;
    if (length) memmove(_data, text, length);
    _size = (uint16_t)length;
    _data[length] = 0;
}

void MqttText::append(const char* text, size_t length) noexcept {
    if (!text) return;
    if (length > (size_t)(_capacity - _size)) length = _capacity - _size;
    if (length) memmove(_data + _size, text, length);
    _size = (uint16_t)(_size + length);
    _data[_size] = 0;
}

void MqttBytes::assign(const uint8_t* first, const uint8_t* last) noexcept {
    size_t length = (first && last > first) ? (size_t)(last - first) : 0;
    if (length > _capacity) length = _capacity;
    if (length) memmove(_data, first, length);
    _size = (uint16_t)length;
}

// --- The byte area ---

MqttByteArea::~MqttByteArea() noexcept {
    mq_freeLarge(_offset);
    mq_freeLarge(_length);
}

bool MqttByteArea::entries(size_t count) noexcept {
    if (count <= _entries) return true;
    if (count > kMaxEntries) return false;
    uint32_t* offset = (uint32_t*)mq_allocLarge(count * sizeof(uint32_t));
    uint32_t* length = (uint32_t*)mq_allocLarge(count * sizeof(uint32_t));
    if (!offset || !length) {
        mq_freeLarge(offset);
        mq_freeLarge(length);
        return false;
    }
    // The entries in use keep their numbers: a held MqttAreaBytes names one.
    for (size_t i = 0; i < count; i++) {
        offset[i] = i < _entries ? _offset[i] : 0;
        length[i] = i < _entries ? _length[i] : kFree;
    }
    mq_freeLarge(_offset);
    mq_freeLarge(_length);
    _offset = offset;
    _length = length;
    _entries = count;
    return true;
}

MqttByteArea::Entry MqttByteArea::put(const uint8_t* bytes, size_t length) noexcept {
    if (length > available() || length >= kFree) return kFailed;
    for (size_t entry = 0; entry < _entries; entry++) {
        if (_length[entry] != kFree) continue;
        _offset[entry] = (uint32_t)_used;
        _length[entry] = (uint32_t)length;
        if (length) memcpy(_bytes + _used, bytes, length);
        _used += length;
        return (Entry)entry;
    }
    return kFailed;
}

// The bytes above a released entry close up at once, so the area never
// fragments; the entries above it are told where their bytes went.
void MqttByteArea::release(Entry entry) noexcept {
    if (entry >= _entries || _length[entry] == kFree) return;
    const size_t offset = _offset[entry];
    const size_t length = _length[entry];
    _length[entry] = kFree;
    if (length == 0) return;
    memmove(_bytes + offset, _bytes + offset + length, _used - offset - length);
    _used -= length;
    for (size_t other = 0; other < _entries; other++) {
        if (_length[other] != kFree && _offset[other] > offset) {
            _offset[other] = (uint32_t)(_offset[other] - length);
        }
    }
}

// --- Bytes held in an area ---

MqttAreaBytes& MqttAreaBytes::operator=(const MqttAreaBytes& other) noexcept {
    if (this != &other) assign(other.data(), other.data() + other._size);
    return *this;
}

void MqttAreaBytes::assign(const uint8_t* first, const uint8_t* last) noexcept {
    const size_t length = (first && last > first) ? (size_t)(last - first) : 0;
    if (!_area) {
        drop();
        _view = first;
        _size = (uint32_t)length;
        return;
    }
    // Bytes from the same area move when an entry is released, so they are
    // copied before the old entry goes; anything else after.
    const bool fromArea = length && _area->contains(first);
    const MqttByteArea::Entry old = _entry;
    if (!fromArea) drop();
    _entry = length ? _area->put(first, length) : MqttByteArea::kNone;
    if (fromArea && old < MqttByteArea::kFailed) _area->release(old);
    _view = nullptr;
    _size = held() ? (uint32_t)length : 0;
}

void MqttAreaBytes::drop() noexcept {
    if (held()) _area->release(_entry);
    _entry = MqttByteArea::kNone;
    _view = nullptr;
    _size = 0;
}

#endif  // MQTT_LEAN

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugHistory.h"

#include "../MqttPlatform.h"

#include <new>
#include <string.h>
#include "../MqttNoExcept.h"

MqttSparkplugHistory::~MqttSparkplugHistory() {
    if (_events != nullptr) {
        for (size_t i = 0; i < _depth; i++) _events[i].~MqttSparkplugEvent();
        mq_freeLarge(_events);
    }
}

bool MqttSparkplugHistory::begin(size_t depth) {
    if (_begun) return _events != nullptr;
    _begun = true;
    if (depth == 0) return false;
    void* raw = mq_allocLarge(depth * sizeof(MqttSparkplugEvent));
    if (raw == nullptr) return false;
    _events = static_cast<MqttSparkplugEvent*>(raw);
    for (size_t i = 0; i < depth; i++) new (&_events[i]) MqttSparkplugEvent();
    _depth = depth;
    return true;
}

void MqttSparkplugHistory::add(const MqttSparkplugEvent& event) {
    if (_events == nullptr) {
        _lost++;
        return;
    }
    if (_count == _depth) {
        // Full: the oldest goes, and the newest is kept.
        _first = (_first + 1) % _depth;
        _count--;
        _lost++;
    }
    _events[(_first + _count) % _depth] = event;
    _count++;
}

const MqttSparkplugEvent* MqttSparkplugHistory::peek(size_t index) const {
    if (_events == nullptr || index >= _count) return nullptr;
    return &_events[(_first + index) % _depth];
}

void MqttSparkplugHistory::drop(size_t n) {
    if (n > _count) n = _count;
    if (_depth == 0) return;
    _first = (_first + n) % _depth;
    _count -= n;
}

void MqttSparkplugHistory::placeOnClock(uint64_t utcNow, uint32_t uptimeNow) {
    for (size_t i = 0; i < _count; i++) {
        MqttSparkplugEvent& event = _events[(_first + i) % _depth];
        if ((event.flags & MQ_SP_EVENT_UPTIME) == 0) continue;
        const uint32_t ago = uptimeNow - (uint32_t)event.timestamp;   // wraps correctly
        if ((uint64_t)ago > utcNow) continue;
        event.timestamp = utcNow - ago;
        event.flags &= (uint8_t)~MQ_SP_EVENT_UPTIME;
    }
}

size_t MqttSparkplugHistory::save(MqttSparkplugEvent* out,
                                  size_t capacity) const {
    if (out == nullptr) return 0;
    size_t copied = 0;
    for (; copied < _count && copied < capacity; copied++) {
        out[copied] = _events[(_first + copied) % _depth];
    }
    return copied;
}

size_t MqttSparkplugHistory::restore(const MqttSparkplugEvent* events,
                                     size_t count, uint32_t lost) {
    _lost = lost > 0xFFFFFFFFu - _lost ? 0xFFFFFFFFu : _lost + lost;
    if (events == nullptr) return 0;
    size_t kept = 0;
    for (size_t index = 0; index < count; index++) {
        if (events[index].flags & MQ_SP_EVENT_UPTIME) {
            // The previous boot's uptime: no clock can place it now.
            _lost++;
            continue;
        }
        add(events[index]);   // full: the oldest goes, counted
        kept++;
    }
    return kept < _count ? kept : _count;
}

void mq_spEventWrite(uint8_t* out, const MqttSparkplugEvent& event) {
    if (out == nullptr) return;
    uint32_t valueBits = 0;
    memcpy(&valueBits, &event.value, sizeof valueBits);
    for (int i = 0; i < 8; i++) out[i] = (uint8_t)(event.timestamp >> (8 * i));
    for (int i = 0; i < 4; i++) out[8 + i] = (uint8_t)(valueBits >> (8 * i));
    for (int i = 0; i < 4; i++) out[12 + i] = (uint8_t)(event.seq >> (8 * i));
    out[16] = (uint8_t)event.source;
    out[17] = (uint8_t)(event.source >> 8);
    out[18] = (uint8_t)event.code;
    out[19] = (uint8_t)(event.code >> 8);
    out[20] = event.flags;
    out[21] = 1;   // the layout's version
    out[22] = 0;
    out[23] = 0;
}

bool mq_spEventRead(const uint8_t* in, MqttSparkplugEvent& event) {
    if (in == nullptr || in[21] != 1) return false;
    uint64_t timestamp = 0;
    for (int i = 0; i < 8; i++) timestamp |= (uint64_t)in[i] << (8 * i);
    uint32_t valueBits = 0;
    for (int i = 0; i < 4; i++) valueBits |= (uint32_t)in[8 + i] << (8 * i);
    uint32_t seq = 0;
    for (int i = 0; i < 4; i++) seq |= (uint32_t)in[12 + i] << (8 * i);
    event.timestamp = timestamp;
    memcpy(&event.value, &valueBits, sizeof valueBits);
    event.seq = seq;
    event.source = (uint16_t)(in[16] | (in[17] << 8));
    event.code = (uint16_t)(in[18] | (in[19] << 8));
    event.flags = in[20];
    return true;
}

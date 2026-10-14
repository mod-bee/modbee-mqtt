// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugHistory.h"

#include "../MqttPlatform.h"

#include <new>
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

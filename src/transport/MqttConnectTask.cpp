// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttConnectTask.h"

#if MQTT_CONNECT_TASK

#if defined(ARDUINO_ARCH_ESP32)
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

namespace {
// One lock for every instance: a critical section is a few loads and stores
// here, and a portMUX must sit in internal RAM, which a static does whatever
// heap the instance itself came from.
portMUX_TYPE g_connectLock = portMUX_INITIALIZER_UNLOCKED;
struct Locked {
    Locked() { portENTER_CRITICAL(&g_connectLock); }
    ~Locked() { portEXIT_CRITICAL(&g_connectLock); }
};
bool startTask(void (*body)(void*), void* argument, uint32_t stackBytes) {
    // ESP-IDF counts a stack in bytes. Any core: the priority is what keeps
    // it out of the PLC's way.
    TaskHandle_t handle = nullptr;
    return xTaskCreatePinnedToCore(body, "mqtt-connect", stackBytes, argument,
                                   MQTT_CONNECT_PRIORITY, &handle, tskNO_AFFINITY) == pdPASS;
}
void waitStep() {
    const TickType_t ticks = pdMS_TO_TICKS(MQTT_CONNECT_STEP_MS);
    vTaskDelay(ticks > 0 ? ticks : 1);
}
uint32_t stackUnused() { return (uint32_t)uxTaskGetStackHighWaterMark(nullptr); }   // bytes
void finishTask() { vTaskDelete(nullptr); }
}  // namespace

#else  // a host, for the test suite only
#include <chrono>
#include <mutex>
#include <thread>

namespace {
std::mutex g_connectLock;
struct Locked {
    Locked() { g_connectLock.lock(); }
    ~Locked() { g_connectLock.unlock(); }
};
bool startTask(void (*body)(void*), void* argument, uint32_t) {
    std::thread(body, argument).detach();
    return true;
}
void waitStep() { std::this_thread::sleep_for(std::chrono::milliseconds(MQTT_CONNECT_STEP_MS)); }
uint32_t stackUnused() { return 0; }
void finishTask() {}
}  // namespace
#endif

MqttConnectTask::Phase MqttConnectTask::phase() const {
    Locked locked;
    return _phase;
}

bool MqttConnectTask::busy() const { return phase() == Running; }

uint32_t MqttConnectTask::stackLeft() const {
    Locked locked;
    return _stackLeft;
}

bool MqttConnectTask::retire(void (*destroy)(void*), void* object) {
    Locked locked;
    if (_phase != Running) return false;
    _destroy = destroy;
    _object = object;
    _cancel = true;
    return true;
}

bool MqttConnectTask::begin() {
    if (_medium == nullptr) return false;
    {
        Locked locked;
        if (_phase == Running) {
            // The last attempt was given up on and its connect has not
            // returned yet. This one fails to open, and the backoff decides
            // when to try again.
            _error = "the last connect is still finishing";
            return false;
        }
        _phase = Running;
        _cancel = false;
    }
    _error = "";
    if (!startTask(&MqttConnectTask::run, this, _stackBytes)) {
        Locked locked;
        _phase = Idle;
        _error = "no memory for the connect task";
        return false;
    }
    return true;
}

void MqttConnectTask::run(void* argument) {
    MqttConnectTask* self = (MqttConnectTask*)argument;
    MqttTransport* medium = self->_medium;

    // The one call that blocks, then the handshake a step at a time.
    Phase outcome = OpenFailed;
    if (medium->begin()) {
        for (;;) {
            {
                Locked locked;
                if (self->_cancel) break;
            }
            if (medium->isConnected()) { outcome = Idle; break; }
            if (medium->connectFailed()) { outcome = Failed; break; }
            waitStep();
        }
    }

    // Handing back. An end() asked for while the medium was ours is done here,
    // and the hand-over is decided under the lock, so an end() can never fall
    // between the task's last look and the caller's first.
    const uint32_t left = stackUnused();   // outside the lock: it reads the stack
    bool ended = false;
    void (*destroy)(void*) = nullptr;
    void* object = nullptr;
    for (;;) {
        bool endNow = false;
        {
            Locked locked;
            if (self->_cancel && !ended) {
                endNow = true;
            } else {
                if (left != 0 && (self->_stackLeft == 0 || left < self->_stackLeft)) {
                    self->_stackLeft = left;
                }
                self->_phase = ended ? Idle : outcome;
                destroy = self->_destroy;
                object = self->_object;
                self->_destroy = nullptr;
                self->_object = nullptr;
            }
        }
        if (!endNow) break;
        medium->end();
        ended = true;
    }
    // `self` is the caller's from here on, and may be freed by the line below.
    if (destroy != nullptr) destroy(object);
    finishTask();
}

void MqttConnectTask::end() {
    if (_medium == nullptr) return;
    {
        Locked locked;
        if (_phase == Running) {
            _cancel = true;   // the task ends it when the connect returns
            return;
        }
        _phase = Idle;
    }
    _medium->end();
}

bool MqttConnectTask::isConnected() const {
    return _medium != nullptr && phase() == Idle && _medium->isConnected();
}

bool MqttConnectTask::connectFailed() const {
    const Phase now = _medium != nullptr ? phase() : Idle;
    if (now == Failed || now == OpenFailed) return true;
    return now == Idle && _medium != nullptr && _medium->connectFailed();
}

bool MqttConnectTask::openFailed() const {
    return _medium != nullptr && phase() == OpenFailed;
}

size_t MqttConnectTask::readable() { return ours() ? _medium->readable() : 0; }

size_t MqttConnectTask::read(uint8_t* into, size_t length) {
    return ours() ? _medium->read(into, length) : 0;
}

size_t MqttConnectTask::write(const uint8_t* buffer, size_t length) {
    return ours() ? _medium->write(buffer, length) : 0;
}

int MqttConnectTask::waitHandle() const { return ours() ? _medium->waitHandle() : -1; }

bool MqttConnectTask::hasBufferedInput() const {
    return ours() && _medium->hasBufferedInput();
}

bool MqttConnectTask::peerClosed() const { return ours() && _medium->peerClosed(); }

const char* MqttConnectTask::peerName() const { return ours() ? _medium->peerName() : ""; }

const char* MqttConnectTask::peerCertificateName() const {
    return ours() ? _medium->peerCertificateName() : "";
}

const char* MqttConnectTask::peerCertificateFingerprint() const {
    return ours() ? _medium->peerCertificateFingerprint() : "";
}

#endif  // MQTT_CONNECT_TASK

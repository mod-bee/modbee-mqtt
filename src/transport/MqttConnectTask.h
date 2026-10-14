// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// =============================================================================
// A BOARD'S CONNECT, ON A TASK OF ITS OWN
// =============================================================================
//
// Arduino's Client::connect() is synchronous: up to three seconds on an ESP32's
// Wi-Fi when the broker cannot be reached, longer on a modem. And a TLS
// handshake is heavy arithmetic for a controller, a step at a time.
// Both used to run inside MqttClient::poll() - inside a PLC scan - on every
// reconnect attempt, so the task holding the client stood still for as long
// as the network took.
//
// This transport wraps the one that opens the medium and does the opening on
// a short-lived task: begin() starts the task and returns at once, the task
// calls the medium's begin() and then drives isConnected() until the handshake
// is done or has failed, and hands the medium back. Meanwhile the client sees
// "not connected yet" and stays Connecting, which is what it already does for
// a host socket whose connect is under way - so CONNECT_TIMEOUT bounds the
// attempt and the reconnect backoff is the client's, unchanged.
//
// WHO OWNS THE MEDIUM. While an attempt is running the task does, entirely:
// nothing here touches the medium from the caller's side - every call answers
// "nothing yet" - and an end() asked for meanwhile is carried out by the task
// when the medium's own connect returns, which cannot be interrupted. Once the
// attempt is over the medium is the caller's again, and every call goes
// straight through to it.
//
// THE MEDIUM MAY NOT BE FREED WHILE IT IS IN USE. Freeing it is the owner's,
// and an owner that wants to while busy() is true hands it over instead -
// retire(): the task frees it when the attempt is over. Never destroy this
// object while busy().
//
// HOW AN ATTEMPT ENDS:
//   connected      isConnected() is the medium's again
//   refused        connectFailed(): a handshake or a certificate said no, and
//                  the client closes as it always has
//   never opened   openFailed(): the medium's begin() returned false - no
//                  route, no answer, a modem not registered - and the client
//                  answers it as it answered a false begin(): the backoff,
//                  nothing concluded
//
// One task per attempt, deleted when it ends: no RAM is held while the link
// is up. Its stack is internal RAM: MQTT_CONNECT_STACK for a socket over Wi-Fi
// or Ethernet, MQTT_CONNECT_TLS_STACK where a TLS handshake or a modem's AT
// exchange runs on it (the owner says which, stackBytes()); stackLeft() is the
// least any attempt left unused. The task runs at MQTT_CONNECT_PRIORITY,
// below every PLC task, so a handshake's arithmetic only ever uses time the
// PLC does not.
//
// FreeRTOS on an ESP32. A host builds it only when a test asks
// (MQTT_CONNECT_TASK=1), on std::thread; see MqttFeatures.h.

#include "../MqttFeatures.h"
#include "../MqttTransport.h"

#if MQTT_CONNECT_TASK

// Bytes. A Wi-Fi socket's open - the name lookup and the connect, with this
// task's own frames - is at most about 1.9 kB on an ESP32-S3, read off the
// built firmware's frames; a TLS handshake measured 3.3 kB at most off the
// board, against an mbedTLS configured as the ESP32 core's. Each with room
// for an interrupt's context and the difference between the two machines.
#ifndef MQTT_CONNECT_STACK
#define MQTT_CONNECT_STACK 3072
#endif
#ifndef MQTT_CONNECT_TLS_STACK
#define MQTT_CONNECT_TLS_STACK 6144
#endif
// FreeRTOS priority. 1 is the lowest above idle; the PLC's tasks are 4 and up.
#ifndef MQTT_CONNECT_PRIORITY
#define MQTT_CONNECT_PRIORITY 1
#endif
// How often the task drives a handshake that is waiting for the far end.
#ifndef MQTT_CONNECT_STEP_MS
#define MQTT_CONNECT_STEP_MS 2
#endif

class MqttConnectTask : public MqttTransport {
public:
    explicit MqttConnectTask(uint32_t stackBytes = MQTT_CONNECT_STACK)
        : _stackBytes(stackBytes) {}
    ~MqttConnectTask() override = default;   // never while busy(): see retire()

    MqttConnectTask(const MqttConnectTask&) = delete;
    MqttConnectTask& operator=(const MqttConnectTask&) = delete;

    // The transport that opens the medium. Before the first begin().
    void wrap(MqttTransport& medium) { _medium = &medium; }
    void stackBytes(uint32_t bytes) { _stackBytes = bytes; }

    // An attempt is running and the medium is the task's.
    bool busy() const;

    // Free `object` - which holds the medium - once the attempt is over, and
    // end the medium first. True when the task has taken it; false when
    // nothing is running, and the caller frees it now.
    bool retire(void (*destroy)(void* object), void* object);

    // The least stack, in bytes, any attempt left unused; 0 before the first.
    uint32_t stackLeft() const;

    // Why begin() refused without starting anything; empty otherwise.
    const char* lastError() const { return _error; }

    // --- MqttTransport ---
    bool begin() override;
    void end() override;
    size_t readable() override;
    size_t read(uint8_t* into, size_t length) override;
    size_t write(const uint8_t* buffer, size_t length) override;
    bool isConnected() const override;
    int waitHandle() const override;
    bool hasBufferedInput() const override;
    bool peerClosed() const override;
    bool connectFailed() const override;
    bool openFailed() const override;
    const char* peerName() const override;
    const char* peerCertificateName() const override;
    const char* peerCertificateFingerprint() const override;

private:
    enum Phase : uint8_t { Idle, Running, Failed, OpenFailed };

    static void run(void* self);
    Phase phase() const;
    bool ours() const { return _medium != nullptr && phase() != Running; }

    MqttTransport* _medium = nullptr;
    uint32_t _stackBytes;
    const char* _error = "";

    // Shared with the task; read and written only under the lock.
    Phase _phase = Idle;
    bool _cancel = false;
    void (*_destroy)(void*) = nullptr;
    void* _object = nullptr;
    uint32_t _stackLeft = 0;
};

#endif  // MQTT_CONNECT_TASK

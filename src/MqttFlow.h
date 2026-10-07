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
// FLOW CONTROL
// =============================================================================
//
// V5:4.9. The send quota: how many QoS>0 publishes may be outstanding.
//
// Symmetric on purpose. The same arithmetic runs on both sides of a
// connection, so a broker built on this layer uses it unchanged - it simply
// has a quota per connection rather than one.
//
// The rules are short, exact, and one of them is a trap:
//
//   [V5:MQTT-4.9.0-1] the initial quota is non-zero and does not exceed the
//                     peer's Receive Maximum
//   [V5:MQTT-4.9.0-2] at zero, no more QoS>0 PUBLISH may be sent
//   [V5:MQTT-4.9.0-3] EVERYTHING ELSE IS STILL PROCESSED AND ANSWERED
//
// The third is the trap. A naive "the queue is full, stop the world" also
// stops acknowledging inbound publishes, so the peer's quota drains too and
// neither end can make progress. The deadlock looks like a dead link and it is
// entirely self-inflicted.
//
// The quota is NOT session state. V5:4.9 - "The send quota and Receive Maximum
// value are not preserved across Network Connections, and are re-initialized
// with each new Network Connection." Carrying one across a reconnect is a
// silent interop bug that only shows after the first drop.

#include "MqttTypes.h"

class MqttFlow {
public:
    // Called on every new connection, never on a resumed session.
    void reset(uint16_t receiveMaximum) {
        // [V5:MQTT-4.9.0-1] non-zero, not exceeding the peer's Receive
        // Maximum. A Receive Maximum of 0 is already refused as a Protocol
        // Error while decoding, so this only guards a caller's own mistake.
        _initial = receiveMaximum == 0 ? 1 : receiveMaximum;
        _quota = _initial;
    }

    uint16_t initial() const { return _initial; }
    uint16_t available() const { return _quota; }
    uint16_t inFlight() const { return (uint16_t)(_initial - _quota); }

    // [V5:MQTT-4.9.0-2] Whether another QoS>0 PUBLISH may be sent.
    //
    // QoS 0 is deliberately not asked about here: the standard permits it at
    // zero quota, and this library sends it, because a telemetry stream should
    // not stop because an acknowledged setpoint is outstanding.
    bool canSendQos12() const { return _quota > 0; }

    void takeOne() {
        if (_quota > 0) _quota--;
    }

    // Returned on PUBACK or PUBCOMP received, "regardless of whether the
    // PUBACK or PUBCOMP carried an error code", and on a PUBREC with a reason
    // of 0x80 or greater - because that PUBREC ends the exchange and no PUBREL
    // will follow.
    void returnOne() {
        // "The send quota is not incremented if it is already equal to the
        // initial send quota." A replayed PUBREL after a reconnect would
        // otherwise inflate it, and the peer would then be sent more in flight
        // than it agreed to accept.
        if (_quota < _initial) _quota++;
    }

private:
    uint16_t _initial = 65535;
    uint16_t _quota = 65535;
};

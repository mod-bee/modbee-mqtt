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
// KEEPALIVE
// =============================================================================
//
// V5:3.1.2.10 and 3.12, V311:3.1.2.10 and 3.12.
//
// This is the ONLY watchdog in the library, and it is the only one there can
// be. [V5:MQTT-4.4.0-1] forbids retransmitting a publish on a timer, so a peer
// that accepts a TCP connection and then goes silent - a half-open socket, a
// firewall that dropped the state, a broker wedged on its own disk - is
// invisible to everything else. A missing PINGRESP is what proves it.
//
// The interval is in SECONDS on the wire and may be overridden by the peer
// (V5:3.2.2.3.14). Zero turns the whole mechanism off at both versions.

#include "MqttPlatform.h"
#include "MqttTypes.h"

class MqttKeepAlive {
public:
    // `seconds` is the value in force - the peer's Server Keep Alive where it
    // sent one, ours otherwise. Resolved by MqttCapabilities, not here.
    void begin(uint16_t seconds, mq_time_t now) {
        _intervalMs = (mq_time_t)seconds * 1000u;
        _lastSentAt = now;
        _lastReceivedAt = now;
        _pingSentAt = 0;
        _awaitingResponse = false;
    }

    void stop() {
        _intervalMs = 0;
        _awaitingResponse = false;
    }

    bool enabled() const { return _intervalMs != 0; }

    // Anything we send counts towards OUR obligation. V5:3.1.2.10 - the Keep
    // Alive is "the maximum time interval that is permitted to elapse between
    // the point at which the Client finishes transmitting one MQTT Control
    // Packet and the point it starts sending the next".
    void noteSent(mq_time_t now) { _lastSentAt = now; }

    // A PINGRESP, and anything else the peer sends that proves it is alive.
    void noteReceived(mq_time_t now) {
        _awaitingResponse = false;
        _lastReceivedAt = now;
    }

    // Whether a PINGREQ is due now.
    //
    // TWO REASONS, and the second is the one a field unit depends on.
    //
    //   - We have SENT nothing for an interval: the protocol's own obligation.
    //   - We have HEARD nothing for an interval. Sending proves nothing about
    //     the far end. A node publishing at QoS 0 every few seconds used to
    //     satisfy the first rule for ever, never ping, never wait for an
    //     answer - and so never noticed a broker that had silently gone, until
    //     the kernel's TCP retransmissions gave up some fifteen minutes later.
    //     A PINGREQ may be sent at any time [V5:3.12.4], so asking costs one
    //     two-byte packet per interval on a link that is otherwise one-way.
    bool pingDue(mq_time_t now) const {
        if (!enabled() || _awaitingResponse) return false;
        return mq_elapsed(_lastSentAt, now) >= _intervalMs ||
               mq_elapsed(_lastReceivedAt, now) >= _intervalMs;
    }

    void notePingSent(mq_time_t now) {
        _pingSentAt = now;
        _lastSentAt = now;
        _awaitingResponse = true;
    }

    // Whether the peer has failed to answer inside the interval.
    //
    // A whole interval rather than a fraction: the standard gives the peer no
    // deadline for a PINGRESP, so anything shorter is us inventing one. A whole
    // interval is the longest wait that still detects a dead link inside the
    // time the protocol itself considers significant.
    //
    // AND SILENCE ALONE IS ENOUGH. A ping only starts the clock above once it
    // has been WRITTEN, and on a dead link the transmit side fills up - so the
    // PINGREQ can be refused, nothing is ever awaited, and nothing ever times
    // out. Two whole intervals with nothing received ends the connection
    // whether or not a ping got out.
    bool responseOverdue(mq_time_t now) const {
        if (!enabled()) return false;
        if (mq_elapsed(_lastReceivedAt, now) >= 2 * _intervalMs) return true;
        if (!_awaitingResponse) return false;
        return mq_elapsed(_pingSentAt, now) >= _intervalMs;
    }

    bool awaitingResponse() const { return _awaitingResponse; }
    uint16_t intervalSeconds() const { return (uint16_t)(_intervalMs / 1000u); }

private:
    mq_time_t _intervalMs = 0;
    mq_time_t _lastSentAt = 0;
    mq_time_t _lastReceivedAt = 0;
    mq_time_t _pingSentAt = 0;
    bool _awaitingResponse = false;
};

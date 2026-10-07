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
// THE OTHER WAY A LINK BEGINS
// =============================================================================
//
// A transport that was opened by somebody else.
//
// This is the whole of what an accepting role needs from the outside world,
// and it is ADDITIVE BY DESIGN: it adds a type, it does not change
// MqttTransport. That was a constraint on version 1 of this library long
// before a broker existed - the plan committed to it so that the broker would
// not need the client's link layer altered, and this file is the test of that
// commitment. Nothing in MqttTransport.h changed to let this exist.
//
// NOTHING HERE BLOCKS EITHER. accept() returns null when nobody is waiting,
// which is most of the time. A listener that blocked would take the whole
// reason this library exists and throw it away at the very bottom of the
// stack.
//
// OWNERSHIP: accept() hands back a transport the LISTENER owns. The caller
// uses it and returns it with release() when the connection ends. A listener
// that handed out `new` and hoped would put an allocation on the hot path of
// every connection, and on a device with no heap to speak of that is a leak
// waiting for a busy afternoon - so an implementation is free to serve
// connections from a fixed pool, and the pool is exactly why release() exists
// rather than `delete`.

#include "MqttTransport.h"

class MqttListener {
public:
    virtual ~MqttListener() = default;

    // Start accepting. False if the endpoint cannot be used - a port already
    // taken, a certificate that will not load.
    virtual bool begin() = 0;

    // Stop accepting. Does NOT close connections already handed out: they
    // belong to whoever took them, and tearing them down here would end a
    // plant's sessions because somebody closed a listening socket.
    virtual void end() = 0;

    // The next connection, or null if none is waiting. Never blocks.
    virtual MqttTransport* accept() = 0;

    // Hands a transport back. The listener closes and reuses it.
    //
    // Must tolerate a transport it did not produce, and a null pointer, so
    // that a caller unwinding an error path never has to reason about which.
    virtual void release(MqttTransport* transport) = 0;

    // Whether begin() succeeded and the endpoint is still usable.
    virtual bool isListening() const = 0;

    // The listening socket, for the same reason a transport has one: an
    // accept() that finds nothing is a syscall spent learning nothing, and
    // this broker made 2,413 of them per 20,000 messages. -1 where the
    // platform has no such concept.
    virtual int waitHandle() const { return -1; }

    // How many more connections this listener can produce before accept()
    // starts refusing. Zero means "no fixed limit".
    //
    // Worth asking rather than discovering: a role that knows it is out of
    // capacity can answer 0x97 Quota Exceeded on the connection it does have,
    // instead of leaving the next one silently unanswered.
    virtual size_t capacityRemaining() const { return 0; }
};

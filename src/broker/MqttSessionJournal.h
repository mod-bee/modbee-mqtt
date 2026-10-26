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
// WHAT A SESSION STORE IS TOLD (host only)
// =============================================================================
//
// A broker keeps its sessions in memory, which MQTT allows. A server that must
// not lose them across a restart (server/sessions/) is told of every change to
// one, in the order the changes happen, and keeps them somewhere that outlives
// the process. The broker does not know where, or whether: without a journal
// each change costs one test of a null pointer, and a board compiles none of
// this.
//
// Every call is made on the broker's own thread, during poll() or a call into
// the broker, and every reference is valid only for the call. What a session
// had when it ended is not reported item by item: sessionEnded() covers it.
//
// WRITTEN BEFORE IT IS SENT. A QoS 1 or 2 message goes to a kept session's
// client only once the identifier it goes under is kept, and a PUBREL only
// once its PUBREC is: deliveryChanged() returns a ticket, kept() says how far
// the store has got, and MqttBroker::journalKept() is how the store says it
// has got further. Without that, a server killed just after a send would
// resend the message as new - without DUP, under another identifier - which
// at QoS 2 is the exactly-once promise broken.

#include "../MqttPlatform.h"

#if defined(MQTT_PLATFORM_HOST)

#include <stdint.h>
#include <string>

class MqttBrokerSession;
struct MqttBrokerSubscription;
struct MqttBrokerDelivery;

class MqttSessionJournal {
public:
    virtual ~MqttSessionJournal() {}

    // The session's own record: made or resumed, connected or gone, its
    // owner, version, expiry interval or Will.
    virtual void sessionChanged(const MqttBrokerSession& session) = 0;

    // It ended - expired, replaced by a clean start, or its connection closed
    // with an expiry of zero - and everything it held with it.
    virtual void sessionEnded(const std::string& clientId) = 0;

    // A subscription granted (or replaced: the same filter again), or removed.
    virtual void subscriptionAdded(const MqttBrokerSession& session,
                                   const MqttBrokerSubscription& subscription) = 0;
    virtual void subscriptionRemoved(const MqttBrokerSession& session,
                                     const std::string& filter) = 0;

    // A QoS 1 or 2 message owed to the client: queued, or its exchange moved
    // on - given the packet identifier it goes under, or its PUBREC arrived.
    // Known by its sequence, which is unique within the session. QoS 0 is
    // never reported. Returns the ticket the change is kept under, or 0 when
    // it is not kept at all (a session that ends with its connection).
    virtual uint64_t deliveryChanged(const MqttBrokerSession& session,
                                     const MqttBrokerDelivery& delivery) = 0;
    virtual void deliveryRemoved(const MqttBrokerSession& session, uint64_t sequence) = 0;

    // A QoS 2 message received from the client: its identifier held until
    // the PUBREL (held), then released.
    virtual void inboundChanged(const MqttBrokerSession& session, uint16_t packetId,
                                bool held) = 0;

    // The last ticket that is kept: every change with a ticket up to it.
    virtual uint64_t kept() const = 0;
};

#endif  // MQTT_PLATFORM_HOST

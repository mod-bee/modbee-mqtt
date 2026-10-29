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
// THE SESSION
// =============================================================================
//
// What survives a network connection. V5:4.1, V311:4.1.
//
// The membership of this set IS the design, so it is written out rather than
// implied. Getting a member of the second list into the first is a silent
// interop bug that only appears after the first reconnect - which on a plant
// is weeks after commissioning.
//
//   IN THE SESSION                              WHERE
//   QoS 1 and 2 PUBLISH sent, unacknowledged    V5:4.1, V311:4.1
//   QoS 2 PUBLISH received, not yet completed   V5:4.1
//   PUBREL sent, unacknowledged                 V5:4.1
//   the subscriptions                           V5:4.1
//
//   NOT IN THE SESSION - RE-INITIALISED EVERY CONNECTION
//   the send quota and Receive Maximum          V5:4.9, last paragraph
//   topic aliases                               V5:3.3.2.3.4
//   Maximum QoS, Retain Available, and every
//     other acknowledgement negotiation         V5:3.2.2.3
//
// A session is destroyed by Clean Start, by the Session Expiry Interval
// elapsing, or by the peer telling us it did not have one.
//
// Stored publishes keep their ENCODED BYTES rather than their fields.
// [V5:MQTT-4.4.0-1] requires retransmission "using their original Packet
// Identifiers", and the surest way to resend what was sent is to have kept it.
// It also means the topic, the payload and every property are held once, in
// the form they will go out in, rather than copied field by field into a
// structure that then has to be re-encoded identically.

#include "MqttFeatures.h"
#include "MqttResult.h"
#include "MqttPacket.h"

// For MqttCompletionFn. A stored subscription holds the handler that asked for
// it, so a subscribe made before the connection existed still concludes.
#include "MqttOperations.h"

#include <vector>

// How many QoS>0 publishes may be outstanding at once, and therefore how many
// packet identifiers can be in use.
//
// Bounded rather than grown: this is a device with no spare heap, and an
// application that publishes faster than the link carries must be refused
// rather than allowed to consume the machine. Past this a publish returns
// QueueFull, never a silent drop.
#ifndef MQTT_SESSION_MAX_INFLIGHT
#define MQTT_SESSION_MAX_INFLIGHT 16
#endif

// Subscriptions the session holds and re-establishes: no fixed number, on a
// host or a board. The table grows with the program's subscriptions, each
// filter in its own allocation, until memory runs out (outOfMemory()). (It was
// MQTT_SESSION_MAX_SUBSCRIPTIONS, 16.)

// Inbound QoS 2 identifiers awaiting a PUBREL. This is what makes exactly-once
// delivery work: an identifier remembered here means the message has already
// been delivered upward, so a duplicate PUBLISH carrying it is acknowledged
// again and NOT delivered twice. [V5:MQTT-4.3.3-10] At 5.0 the client states
// it as its Receive Maximum, so a broker never has more in flight towards it
// than this can tell apart (MqttClient, CONNECT). 255 on a host
// (MqttFeatures.h).
#ifndef MQTT_SESSION_MAX_INBOUND_QOS2
#define MQTT_SESSION_MAX_INBOUND_QOS2 16
#endif

// An outbound publish waiting to be acknowledged.
struct MqttStoredPublish {
    uint16_t packetId = MQ_PACKET_ID_NONE;
    uint8_t qos = MQ_QOS_0;

    // Set once a PUBREC has arrived and a PUBREL has gone out. From then on
    // the stored bytes are a PUBREL rather than a PUBLISH, because that is
    // what a reconnect has to resend. [V5:MQTT-4.4.0-1] names PUBREL
    // explicitly alongside PUBLISH.
    bool awaitingPubcomp = false;

    // Whether this has already been sent once on the current connection, which
    // is what decides the DUP flag on a retransmission.
    bool sent = false;

    MQ_AREA_BYTES encoded;

    bool inUse() const { return packetId != MQ_PACKET_ID_NONE; }
};


// A BOUND THAT DOES NOT FIT ITS COUNTER CORRUPTS RATHER THAN REFUSES.
//
// These are build flags. Raised past what the field holding the count can
// express, the count wraps silently - and the symptom is not "the bound was
// too big", it is messages going missing. Checked here, at compile time,
// rather than by whoever picked the number.
static_assert(MQTT_SESSION_MAX_INBOUND_QOS2 <= 255,
              "MQTT_SESSION_MAX_INBOUND_QOS2 must fit the uint8_t _inboundCount");

struct MqttStoredSubscription {
    // Owned, not a span: a subscription outlives the SUBSCRIBE that carried it
    // and has to be re-issued from memory after a reconnect.
    MQ_OWNED_BYTES filter;
    uint8_t options = 0;
    bool established = false;   // a positive acknowledgement has been seen

    // A SUBSCRIBE for it has been queued on THIS connection. Cleared when a
    // connection is established, so each connection sends each filter once:
    // without it a subscription made in the session hook was queued by
    // subscribe() and again by the reissue straight after the hook.
    bool issued = false;

    // Who to tell when the peer answers, kept HERE rather than only on the
    // queued operation.
    //
    // A subscribe made before the connection exists is recorded and not
    // queued, so there is no operation to hold the handler - and without
    // somewhere to keep it, a caller that asked to be told was never told at
    // all. That is the exactly-one-conclusion contract broken for the most
    // ordinary case there is: subscribing in setup().
    //
    // Kept across reconnects too, so a subscription re-issued onto a fresh
    // session concludes to the same caller as the first time.
    MqttCompletionFn onComplete = nullptr;
    void* context = nullptr;

    bool inUse() const { return !filter.empty(); }
};

class MqttSession {
public:
    // Forgets everything, subscriptions included. For end(), and for a
    // protocol version change.
    //
    // The version case is easy to miss: stored publishes are encoded bytes, so
    // a 5.0 packet replayed onto a 3.1.1 connection would be gibberish. The
    // session cannot span a version change and this is where that is enforced.
    void clear();

    // Forgets the PROTOCOL state - stored publishes, inbound identifiers, the
    // packet identifier cursor - but KEEPS the subscriptions, marked as
    // needing to be established again.
    //
    // This is the one to call for Clean Start and for a session the peer says
    // it does not have, and the distinction is not academic. A subscription is
    // the APPLICATION'S standing intent, not the peer's session state: it was
    // asked for in setup() and it is still wanted. Clearing it because the
    // peer has no session means a reconnect silently stops delivering
    // everything the application subscribed to, with nothing reported and
    // nothing to see - which is exactly the failure mode this library is
    // written to avoid.
    //
    // Stored publishes are different: [V5:MQTT-4.4.0-1] only permits resending
    // them onto a session that is present, so without one they have nowhere to
    // go and must be concluded rather than kept.
    void resetProtocolState();

    // --- Packet identifiers ---
    //
    // [V5:MQTT-2.2.1-3] every new SUBSCRIBE, UNSUBSCRIBE or QoS>0 PUBLISH gets
    // a non-zero identifier THAT IS CURRENTLY UNUSED.
    //
    // A 16-bit bitmap would be 8 kB and is out of the question on an MCU, so
    // this is a rotating cursor over the small in-use set - which is legitimate
    // precisely because flow control bounds how many can be outstanding.
    uint16_t allocatePacketId();
    void releasePacketId(uint16_t packetId);
    bool packetIdInUse(uint16_t packetId) const;
    bool reserveControlId(uint16_t packetId);
    void releaseControlId(uint16_t packetId);

    // --- Outbound publishes ---
    bool storeOutbound(uint16_t packetId, uint8_t qos,
                       const uint8_t* encoded, size_t length);
    MqttStoredPublish* findOutbound(uint16_t packetId);
    void releaseOutbound(uint16_t packetId);

    // Replaces the stored bytes with a PUBREL, for the second half of a QoS 2
    // exchange.
    bool promoteToPubrel(uint16_t packetId, const uint8_t* encoded, size_t length);

    size_t outboundCount() const;
    bool outboundFull() const;

    // --- Inbound QoS 2 ---
    //
    // True when this identifier is now remembered. False when it was already
    // known - a duplicate PUBLISH: acknowledge it again, but do NOT deliver it a
    // second time - or when the table is full; inboundKnown() tells the two apart.
    bool rememberInbound(uint16_t packetId);
    bool inboundKnown(uint16_t packetId) const;
    void forgetInbound(uint16_t packetId);

    // --- Subscriptions ---
    //
    // A standing intent the library owns. A subscribe made before the
    // connection exists is recorded here and established when possible, rather
    // than refused - a caller that has to watch for the connection itself is a
    // caller that has to know the protocol's own rules.
    //
    // Re-adding a filter already held replaces its options and its completion
    // handler rather than adding a second entry [V5:3.8.4].
    bool addSubscription(const uint8_t* filter, uint16_t length, uint8_t options,
                         MqttCompletionFn onComplete = nullptr,
                         void* context = nullptr);
    void removeSubscription(const uint8_t* filter, uint16_t length);
    void markSubscriptionEstablished(const uint8_t* filter, uint16_t length);
    size_t subscriptionCount() const;
    const MqttStoredSubscription* subscriptionAt(size_t index) const;

    // Every subscription becomes unestablished again.
    //
    // Called when a connect reports the session was NOT present. Getting this
    // backwards is one of the two ways a reconnect goes wrong: skip it and
    // every subscription is silently lost, call it always and every
    // subscription is issued twice.
    void markAllSubscriptionsUnestablished();

    // Per connection: see MqttStoredSubscription::issued.
    void markSubscriptionIssued(const uint8_t* filter, uint16_t length);
    void clearSubscriptionsIssued();

    // --- Reconnect ---

    // Everything stored becomes unsent, so it is offered again.
    // [V5:MQTT-4.4.0-1] - the one circumstance in which resending is REQUIRED.
    void markAllUnsent();

    // The next stored publish that has not been sent on this connection, or
    // null. Walked rather than indexed, because releases leave gaps.
    MqttStoredPublish* nextUnsent();

    // --- Diagnostics ---
    uint32_t storedBytes() const;

#if MQTT_LEAN
    // On a board the resend copies live in the client's byte area
    // (MqttFixed.h), bound here once, before use.
    void bind(MqttByteArea& area);
#endif

    // A subscription was refused for want of memory (the table grows until
    // there is none). Cleared by clear().
    bool outOfMemory() const { return _outOfMemory; }

private:
    MqttStoredPublish _outbound[MQTT_SESSION_MAX_INFLIGHT];
    // PACKET IDENTIFIERS HELD BY SUBSCRIBE AND UNSUBSCRIBE.
    //
    // Those two take an identifier from the same 1..65535 space as a QoS > 0
    // publish, and they were never recorded anywhere - so packetIdInUse() could
    // not see them. Across a full wrap of the identifier space, with a
    // subscribe still outstanding, allocatePacketId() would hand that same
    // identifier to a publish; the SUBACK then arrives, releasePacketId()
    // matches the PUBLISH, and its replay bytes are discarded while the peer
    // still owes an acknowledgement for it.
    //
    // Small and fixed: a client with more than eight subscribes in flight at
    // once is not a shape this library is for, and the ninth simply waits.
    static const size_t kMaxControlIds = 8;
    uint16_t _controlIds[kMaxControlIds];

    MqttGrowList<MqttStoredSubscription> _subscriptions;
    bool _outOfMemory = false;

    uint16_t _inbound[MQTT_SESSION_MAX_INBOUND_QOS2] = {0};
    uint8_t _inboundCount = 0;

    // Where the next identifier search starts. Rotating rather than always
    // beginning at 1, so an identifier is not reused the instant it is freed -
    // a late acknowledgement for the previous owner would otherwise retire the
    // wrong operation.
    uint16_t _nextPacketId = 1;

    int indexOfOutbound(uint16_t packetId) const;
    int indexOfSubscription(const uint8_t* filter, uint16_t length) const;
};

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
// THE QUEUE
// =============================================================================
//
// What the application asks for, held until the link can carry it.
//
// THE CONTRACT: EVERY ACCEPTED OPERATION REACHES EXACTLY ONE CONCLUSION.
//
// Not "usually". Not "unless the connection dropped". An operation that is
// accepted and then silently never concluded does not look like a bug - it
// looks like a control loop that quietly stopped publishing, weeks after
// commissioning, on a plant where nothing else changed.
//
// What the application gets, with no configuration at all:
//
//   Stacked       Nothing blocks and nothing is sent inside the call that
//                 made it.
//   Ordered       What is accepted first leaves first. Anything that carries
//                 its own sequence numbers - Sparkplug does - depends on this.
//   Superseded    A publish to a topic already queued and not yet sent
//                 REPLACES the queued value rather than stacking behind it,
//                 and the superseded caller is told. A setpoint written every
//                 scan sends the value it settled on, not a history.
//   Concluded     Exactly once, always.
//
// THERE IS NO RETRANSMISSION TIMER, AND THAT IS NOT AN OMISSION.
//
// [V5:MQTT-4.4.0-1]: "This is the only circumstance where a Client or Server
// is REQUIRED to resend messages. Clients and Servers MUST NOT resend messages
// at any other time." The only circumstance is reconnecting with Clean Start 0
// onto a session that is present.
//
// So a sent publish awaiting its acknowledgement has NO DEADLINE OF ITS OWN.
// TCP either delivers or the connection fails; a timer that resent would emit
// duplicate application messages and violate the standard. What catches a peer
// that has gone quiet is keepalive, one layer up, and what a dropped
// connection does to an outstanding publish is move it into the session store.

#include "MqttResult.h"
#include "MqttPacket.h"
#include "MqttOwnedProperties.h"

#include <vector>

// Operations held at once. Past this, an operation is REFUSED with QueueFull
// rather than dropped - a caller that was told "accepted" and got nothing is
// the failure this whole file exists to prevent.
#ifndef MQTT_QUEUE_CAPACITY
#define MQTT_QUEUE_CAPACITY 32
#endif

// A completion handler: a plain function pointer and a context.
//
// Not a std::function. This is called from the poll path, it must not
// allocate, and generated IEC 61131 function-block code cannot register a
// lambda at all - the context pointer is how a C++ caller gets its own object
// back without one. A std::function convenience lives at L4, where the caller
// is known to be C++.
typedef void (*MqttCompletionFn)(void* context, MqttResult result, uint16_t packetId);

enum class MqttOpKind : uint8_t {
    Publish,
    Subscribe,
    Unsubscribe,
};

struct MqttOperation {
    MqttOpKind kind = MqttOpKind::Publish;

    // Owned, because an operation outlives the buffer its caller built it
    // from - and on a reconnect it may outlive the connection as well.
    std::vector<uint8_t> topic;      // the topic name, or the filter
    std::vector<uint8_t> payload;

    uint8_t qos = MQ_QOS_0;
    bool retain = false;
    uint8_t subscriptionOptions = 0;

    // 5.0 properties on an outbound PUBLISH - a Response Topic, Correlation
    // Data, a Content Type, User Properties, a Message Expiry.
    //
    // OWNED, not spans: the caller's strings are gone by the time this leaves
    // the queue. A publish made in setup() may not reach the wire until the
    // link comes up minutes later, and on a reconnect it may be replayed from
    // the session long after that.
    //
    // And held BEHIND A POINTER, so a queue of thirty-two slots does not carry
    // thirty-two property sets on a client that never sends one. See
    // MqttOptionalProperties - it is eight bytes here and allocates nothing
    // until something is actually set.
    MqttOptionalProperties properties;

    // Whether a later publish to the same topic may replace this one.
    //
    // True by default, because a setpoint should send the value it settled on.
    // TURNED OFF for any stream that carries its own sequence numbers:
    // eliding a message there leaves a gap the receiver reads as loss, which
    // is precisely what Sparkplug's seq field would report.
    bool supersedable = true;

    // Assigned when the operation is sent, not when it is queued. A QoS 0
    // publish never gets one.
    uint16_t packetId = MQ_PACKET_ID_NONE;

    // Ours, and unique for the life of the queue. Distinct from the packet
    // identifier, which is the protocol's, may be zero, and is reused.
    uint32_t opId = 0;

    bool sent = false;

    // WHEN it was sent, which is the difference between an operation that can
    // be given up on and one that waits for ever.
    //
    // There was no such field, and that was the whole of the defect: once a
    // QoS>0 PUBLISH was on the wire, the ONLY things that could conclude it
    // were the matching acknowledgement or the link dropping. A broker that
    // accepts a PUBLISH and never PUBACKs - which is conforming, since MQTT
    // sets no deadline on a server - left the operation in use for ever. Each
    // one also holds a unit of the peer's Receive Maximum, so after
    // receiveMaximum of them the client could never publish at QoS>0 again:
    // silently, with keepalive still answered and nothing in any status field
    // to say so.
    //
    // Observed on a plant: TLS up, keepalive answered, PENDING stuck at 6, and
    // PUBLISHED still climbing because the QoS 0 traffic was unaffected.
    mq_time_t sentAt = 0;

    // Goes ahead of everything already queued.
    //
    // For the session-established hook and nothing else. R7 says that hook
    // runs before the send queue drains - but running first is not enough if
    // what it queues is then appended BEHIND the traffic it was supposed to
    // precede. A Sparkplug birth certificate has to be the first publication
    // on a connection; queued behind three NDATA messages it introduces data
    // that already went past, and a host discards the lot.
    //
    // Deliberately not exposed to the application. Everything a caller queues
    // is ordinary traffic, and a priority flag on the public API would become
    // a way to jump the queue rather than a way to open a session.
    bool priority = false;

    MqttCompletionFn onComplete = nullptr;
    void* context = nullptr;

    bool inUse() const { return opId != 0; }
};

class MqttOperations {
public:
    MqttOperations();

    void setCapacity(size_t capacity);
    size_t capacity() const { return _capacity; }

    // --- Queueing ---
    //
    // Returns Queued on acceptance, or the reason it was refused. A refusal
    // here is reported through the return value and NOT through the handler,
    // because the caller has not yet recorded what it is waiting for - running
    // its completion before its own call has returned is how a caller ends up
    // concluding an operation it does not know it made.
    MqttResult add(const MqttOperation& operation);

    // While this is on, everything added is marked priority. Turned on around
    // the session-established hook by the client, and off again immediately.
    void setPriorityMode(bool on) { _priorityMode = on; }

    // --- Draining ---
    //
    // The next operation that has not been sent, or null. In queue order:
    // what was accepted first leaves first.
    MqttOperation* nextUnsent();

    void markSent(MqttOperation& operation, uint16_t packetId);

    // --- Concluding ---
    //
    // The only ways an operation leaves the queue. Nothing else erases.
    void concludeByPacketId(uint16_t packetId, MqttResult result);
    void concludeByOpId(uint32_t opId, MqttResult result);

    // Everything, with one reason. Used when a connection is lost and the
    // session cannot carry the work forward, and when the client is ended.
    void concludeAll(MqttResult result);

    // Concludes every SENT operation whose acknowledgement is overdue.
    //
    // NOTHING IS RESENT. [V5:MQTT-4.4.0-1] forbids that outside a session
    // resume and this library holds to it; these operations are given up on
    // and their handlers told, which the standard does not speak to at all.
    //
    // Returns how many it concluded, so a caller can count them.
    // Withdraw every queued publish to this topic that has not yet been
    // written, concluding each with the given result. Returns how many went.
    //
    // ANYTHING ALREADY SENT IS LEFT ALONE, and that is not a limitation to be
    // worked around: those bytes are on the wire and their acknowledgement is
    // still owed. Withdrawing a record whose PUBACK is in flight would leave
    // the acknowledgement arriving for an operation nobody remembers.
    size_t withdraw(const std::vector<uint8_t>& topic, MqttResult result);

    size_t concludeOverdue(mq_time_t now, mq_time_t timeoutMs,
                           MqttResult result);

    // Everything that has NOT been sent. Used when a connection drops: what
    // was sent has moved into the session and will be retransmitted, what was
    // not has nowhere to go.
    void concludeUnsent(MqttResult result);

    // Everything that HAS been sent, and only that.
    //
    // For a connection whose peer reports no session: what was sent was
    // relying on that session to carry it, and [V5:MQTT-4.4.0-1] only permits
    // resending onto one that is present - so those conclude. What was never
    // sent is untouched and goes out on this connection, which is the whole
    // point of having queued it.
    //
    // Concluding everything here instead was a real defect: a publish made
    // before connect() - which is ordinary, and is exactly what a scan-cycle
    // application does in its first scan - was silently discarded the moment
    // the connection it was waiting for came up.
    void concludeSent(MqttResult result);

    // Everything sent becomes unsent again, for a reconnect that resumed a
    // session.
    void markAllUnsent();

    MqttOperation* findByPacketId(uint16_t packetId);

    // Direct access to a slot, for the few callers that need to walk. Returns
    // null past the end; the slot may be unused, which inUse() answers.
    MqttOperation* at(size_t index) {
        return index < MQTT_QUEUE_CAPACITY ? &_ops[index] : nullptr;
    }

    // --- Diagnostics ---
    //
    // Every one of these is bounded by construction, and a rising count in any
    // of them means something an operator can act on.
    size_t pending() const;
    size_t sentCount() const;
    bool empty() const { return pending() == 0; }
    uint32_t supersededCount() const { return _superseded; }
    uint32_t refusedCount() const { return _refused; }
    uint32_t concludedCount() const { return _concluded; }

private:
    MqttOperation _ops[MQTT_QUEUE_CAPACITY];
    size_t _capacity = MQTT_QUEUE_CAPACITY;
    uint32_t _nextOpId = 1;
    bool _priorityMode = false;

    uint32_t _superseded = 0;
    uint32_t _refused = 0;
    uint32_t _concluded = 0;

    // --- Re-entrancy ---
    //
    // A completion handler is application code, and the natural thing for one
    // to do is publish again. So the record is removed from the queue BEFORE
    // its handler runs and nothing reads it afterwards.
    //
    // That makes a handler free to queue work, but it does not bound the
    // DEPTH: a handler told its publish was superseded that publishes the same
    // topic again supersedes the publish that replaced it, and the chain runs
    // until the stack is gone. So a conclusion reached while a handler is
    // already running is QUEUED rather than nested, and drained by the
    // outermost call.
    struct Deferred {
        MqttCompletionFn handler;
        void* context;
        MqttResult result;
        uint16_t packetId;
    };

    std::vector<Deferred> _deferred;
    int _dispatchDepth = 0;

    // The live slots at the moment a conclude* walk starts; see the .cpp.
    uint32_t liveMask() const;

    // Bounded, because "a handler may defer another" has no natural end. Every
    // entry is a real conclusion owed to somebody, so none is discarded - the
    // drain simply stops after a sane depth and the rest are carried to the
    // next call, which keeps one pass bounded without breaking the contract.
    static const size_t kMaxDrainPerDispatch = 64;

    int indexOfOpId(uint32_t opId) const;
    int indexOfPacketId(uint16_t packetId) const;

    // Removes the record, then runs its handler. The rule the whole class
    // follows, stated once here and nowhere contradicted.
    void retireAt(int index, MqttResult result);

    void dispatch(MqttCompletionFn handler, void* context,
                  MqttResult result, uint16_t packetId);

public:
    // Runs conclusions carried over from a previous pass. Called at the top of
    // the owner's poll(), so anything already owed is paid before new work is
    // taken on.
    void drainDeferred();
    size_t deferredCount() const { return _deferred.size(); }
};

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
// WHAT THE SERVER KEEPS FOR ONE CLIENT
// =============================================================================
//
// [V5:4.1] defines Session State separately for each end, and the two lists
// are NOT the same. The client keeps its own packets to replay; the server
// keeps the client's subscriptions and the messages owed to it. So this is a
// different type from MqttSession rather than a reuse of it - the storage is
// genuinely different state, not the same state seen from the other side.
//
// The Server's list, quoted, because every field below exists for one line of
// it:
//
//   - The existence of a Session, even if the rest of the Session State is
//     empty.
//   - The Client's subscriptions, including any Subscription Identifiers.
//   - QoS 1 and QoS 2 messages which have been sent to the Client, but have
//     not been completely acknowledged.
//   - QoS 1 and QoS 2 messages pending transmission to the Client.
//   - QoS 2 messages which have been received from the Client, but have not
//     been completely acknowledged.
//   - Optionally, QoS 0 messages pending transmission to the Client.
//   - If the Session is currently not connected, the time at which the Session
//     will end and Session State will be discarded.
//
// MESSAGES ARE STORED DECODED, NOT ENCODED. The client's session stores the
// bytes it sent, because [V5:MQTT-4.4.0-1] requires the same Packet
// Identifiers replayed unchanged. A server cannot do that: the session may
// resume on a connection that negotiated a different protocol version, a
// different Maximum Packet Size, or different topic aliases, and bytes encoded
// for the old connection would be wrong on the new one in ways nothing would
// catch.

#include "../MqttFeatures.h"


#include "../MqttPacket.h"
#include "../MqttOwnedProperties.h"
#include "../MqttTopic.h"
#include "../MqttPlatform.h"
#include "../MqttTimerWheel.h"
#include "MqttSessionIndex.h"
#include "MqttRouteIndex.h"

#include <string>
#include <vector>

// One of a client's subscriptions.
struct MqttBrokerSubscription {
    // As the client wrote it, including any "$share/name/" prefix, because
    // UNSUBSCRIBE matches on exactly what was sent.
    std::string filter;

    // For a shared subscription, the group name and the filter inside it.
    // Empty for an ordinary one. [V5:4.8.2]
    std::string shareName;

    // What routing matches against: the inner filter for a shared
    // subscription, and the filter itself otherwise.
    std::string effective;

    uint8_t options = 0;

    // [V5:3.8.2.1.2] Zero means none was given. A Subscription Identifier of
    // 0 is a Protocol Error, so zero is safe as "absent".
    uint32_t subscriptionId = 0;

    // RETAINED MESSAGES STILL OWED to this subscription, and where in the
    // retained store the walk has got to. A subscription to "plant/#" over
    // more retained topics than its session's queue holds is sent them in
    // instalments as the queue drains - they used to be dropped past the
    // queue's depth, which [MQTT-3.3.1-9] does not allow.
    bool retainedOwed = false;
    uint32_t retainedNext = 0;

    bool inUse() const { return !filter.empty(); }

    uint8_t qos() const { return options & MQ_SUBOPT_QOS_MASK; }
    bool noLocal() const { return (options & MQ_SUBOPT_NO_LOCAL) != 0; }
    bool retainAsPublished() const { return (options & MQ_SUBOPT_RETAIN_AS_PUB) != 0; }
    uint8_t retainHandling() const {
        return (uint8_t)((options & MQ_SUBOPT_RETAIN_HANDLING_MASK)
                         >> MQ_SUBOPT_RETAIN_HANDLING_SHIFT);
    }
    bool shared() const { return !shareName.empty(); }
};

// A message owed to this client.
struct MqttBrokerDelivery {
    std::string topic;
    std::vector<uint8_t> payload;
    uint8_t qos = MQ_QOS_0;
    bool retain = false;

    MqttOwnedProperties properties;

    // [V5:3.3.4] Every matching subscription's identifier travels with the
    // single copy that is delivered.
    uint32_t subscriptionIds[MQTT_MAX_SUBSCRIPTION_IDS] = {0};
    uint8_t subscriptionIdCount = 0;

    // Allocated when it goes out at QoS > 0, and held until the exchange
    // completes so a reconnect can replay it.
    uint16_t packetId = MQ_PACKET_ID_NONE;
    bool sent = false;

    // The PUBREC has arrived and a PUBREL is owed. The QoS 2 exchange is now
    // half done and what must be replayed after a reconnect is the PUBREL, not
    // the PUBLISH. [V5:4.3.3]
    bool awaitingPubcomp = false;

    // A PUBREL is owed and has not gone out on THIS connection. Set when the
    // PUBREC arrives, and set again after a reconnect - [V5:4.4] requires the
    // PUBREL to be resent, and without a flag the queue cannot tell a PUBREL
    // already sent from one owed, so it would either send none or send one on
    // every poll for ever.
    bool pubrelOwed = false;

    mq_time_t queuedAt = 0;

    // Strictly increasing within the session, which is what ordering is
    // actually decided on. A millisecond timestamp will not do it: two
    // messages queued in the same millisecond tie, and a 32-bit millisecond
    // clock wraps every 49.7 days - at which point "oldest first" would
    // silently become "newest first" for one pass.
    uint64_t sequence = 0;

    bool inUse() const { return !topic.empty() || !payload.empty() || packetId != MQ_PACKET_ID_NONE; }
    // RELEASED IN PLACE, keeping the buffers this slot has already grown.
    //
    // `*this = MqttBrokerDelivery()` freed topic, payload and every property
    // buffer, so the next delivery through the slot allocated them all again.
    // On a queue that is doing its job - taking a message, sending it, taking
    // the next - that was an allocation per message per subscriber, for ever.
    void release() {
        topic.clear();
        payload.clear();
        properties.clear();
        qos = MQ_QOS_0;
        retain = false;
        subscriptionIdCount = 0;
        packetId = MQ_PACKET_ID_NONE;
        sent = false;
        awaitingPubcomp = false;
        pubrelOwed = false;
        queuedAt = 0;
        sequence = 0;
    }
};

// The Will, copied out of the CONNECT that carried it.
struct MqttBrokerWill {
    bool present = false;
    std::string topic;
    std::vector<uint8_t> payload;
    uint8_t qos = MQ_QOS_0;
    bool retain = false;
    MqttOwnedProperties properties;

    // [V5:3.1.3.2.2] Seconds the Server waits before publishing it. The wait
    // is what lets a device reboot without every dashboard flashing red.
    uint32_t delayInterval = 0;

    // When it becomes due, once the connection has gone. Zero means it is not
    // scheduled - either there is no Will, or the client disconnected cleanly
    // and it was deleted.
    // WHEN THE DELAY STARTED, not when it ends.
    //
    // Storing the deadline meant computing now + delayInterval * 1000, and
    // delayInterval is a uint32_t of SECONDS while mq_time_t is a uint32_t of
    // MILLISECONDS - so anything above 4,294,967 s wrapped, and anything above
    // 24.85 days landed in the past half of the comparison and fired the Will
    // on the very next poll. 0xFFFFFFFF, which is what a client means by
    // "effectively never", was the worst case of all.
    //
    // Divided down rather than multiplied up, exactly as hasExpired() does
    // with the Session Expiry Interval, and for exactly the same reason.
    mq_time_t scheduledAt = 0;
    bool scheduled = false;

    void clear() { *this = MqttBrokerWill(); }
};

class MqttBrokerSession {
public:
    // Sized once, at startup. A broker that grew these per client would do its
    // allocating exactly when it is busiest.
    // `reserveNow` false defers reserving the slots until first use; see
    // MqttBrokerConfig::reserveUpFront.
    void configure(size_t maxSubscriptions, size_t maxQueued, bool reserveNow = true);

    // A counter the session bumps whenever its SUBSCRIPTIONS change.
    //
    // The broker keeps a routing index and has to know when it is stale. It
    // could set a flag at each call site that adds or removes a subscription -
    // and a missed one would mean a message silently not delivered to a
    // client that was subscribed to it, which is the worst thing this library
    // could do. So the session bumps it itself, at the only three places
    // subscriptions can change, and the broker compares one integer.
    void attachSubscriptionRevision(uint32_t* revision) { _revision = revision; }

#if defined(MQTT_PLATFORM_HOST)
    // The broker's identifier index, which this session keeps up to date as
    // its identity changes in adopt() and release(). See MqttSessionIndex.h.
    void attachIndex(MqttSessionIndex* index, uint32_t self) { _index = index; _self = self; }
    // The broker's routing index, which this session tells about every filter
    // it gains and loses. See MqttRouteIndex.h.
    void attachRoutes(MqttRouteIndex* routes) { _routes = routes; }
#endif

    void adopt(const MqttSpan& clientId);
    void release();

    bool inUse() const { return _inUse; }
    const std::string& clientId() const { return _clientId; }
    bool matchesClientId(const MqttSpan& clientId) const;

    // --- Subscriptions ---
    //
    // Returns the granted QoS, or 0x80 / a 5.0 reason code on refusal. A
    // filter already subscribed is REPLACED rather than added
    // [V5:MQTT-3.8.4-3]: "If a Server receives a SUBSCRIBE packet containing a
    // Topic Filter that is identical to a Non-shared Subscription's Topic
    // Filter for the current Session, then it MUST replace that existing
    // Subscription with a new Subscription." Whether it existed already is
    // reported, because Retain Handling 1 turns on exactly that question.
    bool addSubscription(const MqttSpan& filter, uint8_t options,
                         uint32_t subscriptionId, bool& replacedExisting);

    bool removeSubscription(const MqttSpan& filter);
    size_t subscriptionCount() const;
    const MqttBrokerSubscription* subscriptionAt(size_t index) const;
    void clearSubscriptions();

    // --- Retained messages owed to new subscriptions ---
    //
    // owe: the subscription at `index` is to be sent the retained messages
    // matching it, from the start of the store. progress: the walk got to
    // `next`, or finished. retainedOwed(): how many subscriptions are still
    // owed some, so the broker can skip the question in one comparison.
    void oweRetained(size_t index);
    void retainedProgress(size_t index, size_t next, bool finished);
    size_t retainedOwed() const { return _retainedOwed; }

    // --- The delivery queue ---
    bool enqueue(const MqttBrokerDelivery& message);

    // A slot to BUILD a delivery in, rather than a copy of one already built.
    //
    // route() used to construct a MqttBrokerDelivery on the stack, fill its
    // topic and payload, and hand it to enqueue() - which copy-assigned the
    // whole thing into a slot. That is two copies of topic and payload per
    // subscriber per message, and the stack temporary's buffers were
    // allocated and freed every time round.
    //
    // Reserving the slot first and filling it in place makes it one copy, into
    // a buffer the slot already owns, with no allocation at all once a queue
    // has been round once. commitDelivery() is what makes it ordered; until
    // then the slot is in use but has no sequence, and nothing looks at it.
    MqttBrokerDelivery* reserveDelivery();
    void commitDelivery(MqttBrokerDelivery& entry);
    size_t queued() const;
    size_t queuedExtent() const { return _queueExtent; }
    size_t queueCapacity() const { return _maxQueued; }
    bool queueFull() const;

    // The next message that has not gone out. Oldest first: a broker that
    // delivered out of order would break every command/acknowledgement pattern
    // built on it, and MQTT's ordering guarantee [V5:4.6] is per topic in the
    // order received.
    // `allowQos12` false considers only QoS 0 entries.
    //
    // [V5:MQTT-4.6.0-6] requires order to be kept "for the same Topic AND
    // QoS", so passing over a QoS>0 entry that has no quota to send a QoS 0
    // one behind it keeps every ordering the standard asks for. Without this,
    // one unacknowledged setpoint silenced a telemetry stream completely -
    // which deliverQueued()'s own comment said it did not.
    MqttBrokerDelivery* nextUnsent(bool allowQos12 = true);
    MqttBrokerDelivery* findByPacketId(uint16_t packetId);
    MqttBrokerDelivery* deliveryAt(size_t index);
    void releaseDelivery(uint16_t packetId);
    void markAllUnsent();

    // --- Packet identifiers, per session [V5:MQTT-2.2.1-3] ---
    uint16_t allocatePacketId();
    void releasePacketId(uint16_t packetId);
    bool packetIdInUse(uint16_t packetId) const;

    // --- Inbound QoS 2, method A [V5:4.3.3] ---
    //
    // The identifier is recorded when the PUBLISH is accepted and the message
    // is forwarded once. A second PUBLISH carrying the same identifier is
    // acknowledged but NOT forwarded again, which is what "exactly once" means
    // from this side.
    bool rememberInbound(uint16_t packetId);
    bool inboundKnown(uint16_t packetId) const;
    void forgetInbound(uint16_t packetId);
    void clearInbound();

    // --- Lifetime ---
    bool connected = false;

    // The user name the current connection logged in with ("" for none),
    // set at each CONNECT. Passed with every PUBLISH and SUBSCRIBE the hook is
    // asked about, so a permission can depend on who is asking - and so a
    // login that is revoked can have its connections found.
    std::string userName;

    // Which connection slot serves this session while it is connected, or
    // kNoConnection. Kept so that a broker can find the connection from the
    // session without walking every slot - which it used to do on every
    // takeover, and which an event-driven broker must do on every delivery,
    // to wake the connection that now has something to send.
    static const uint32_t kNoConnection = 0xFFFFFFFFu;
    uint32_t connectionSlot = kNoConnection;

#if defined(MQTT_PLATFORM_HOST)
    // The event-driven broker's deadlines for this session: its Will Delay
    // and its expiry. Armed when the session goes offline, cancelled when it
    // comes back. A board's broker walks its sessions instead and carries
    // neither.
    MqttTimer willTimer;
    MqttTimer expiryTimer;
#endif

    // [V5:3.1.2.11.2] 0 means the session ends when the connection does;
    // 0xFFFFFFFF means it does not end. At 3.1.1 there is no interval at all,
    // so Clean Session 0 is stored here as "never expires" and Clean Session 1
    // as zero - which is precisely what that version's two behaviours are.
    uint32_t expiryInterval = 0;
    mq_time_t offlineSince = 0;

    bool hasExpired(mq_time_t now) const;

    MqttBrokerWill will;

    // The version the session was created at. A session resumed by a CONNECT
    // at a different version is not the same session in any useful sense - the
    // subscriptions carry options 3.1.1 cannot express - so the broker starts
    // a clean one and says so.
    MqttVersion version = MqttVersion::V5;

    // Diagnostics, and what the tests assert on.
    uint32_t messagesDelivered = 0;
    uint32_t messagesDropped = 0;

private:
    bool _inUse = false;
    std::string _clientId;

    // THE LIVE SUBSCRIPTIONS ARE KEPT AT THE FRONT, WITH NO HOLES.
    //
    // The broker walks every session's subscriptions for every message it
    // routes, so a hole is not free: with the default thirty-two slots and
    // one filter in use, thirty-one of every thirty-two loads on the hot
    // path were of an empty slot. Measured at five hundred sessions that was
    // 64 ns per session per message against 10 ns compacted - the scan cost
    // six times more than the work. removeSubscription() therefore moves the
    // last entry into the gap rather than leaving one.
    //
    // Nothing may hold a subscription index across a removal. MQTT gives no
    // meaning to the order of a session's filters ([V5:3.8.4] speaks of the
    // set, never a sequence), so moving one is invisible from the wire.
    size_t _maxSubscriptions = 0;
    size_t _maxQueued = 0;
    // Reserve the arrays on first use (MqttBrokerConfig::reserveUpFront false).
    // ALL AT ONCE when they are reserved, never grown afterwards: the broker
    // holds pointers into both while it works, and an array that moved under
    // them would leave those pointing at freed memory.
    void reserveSubscriptions() { if (_subscriptions.empty()) _subscriptions.resize(_maxSubscriptions); }
    void reserveQueue() { if (_queue.empty()) _queue.resize(_maxQueued); }
    std::vector<MqttBrokerSubscription> _subscriptions;
    size_t _subscriptionsUsed = 0;
    uint32_t* _revision = nullptr;
#if defined(MQTT_PLATFORM_HOST)
    MqttSessionIndex* _index = nullptr;
    MqttRouteIndex* _routes = nullptr;
    uint32_t _self = 0;
#endif
    void bumpRevision() { if (_revision) (*_revision)++; }
    std::vector<MqttBrokerDelivery> _queue;
    size_t _retainedOwed = 0;

    // NO LIVE DELIVERY SITS AT OR ABOVE THIS INDEX.
    //
    // Every queue operation - enqueue, nextUnsent, findByPacketId,
    // packetIdInUse, the PUBREL scan - walked all maxQueuedPerSession slots,
    // which is 64 by default and mostly empty. An idle connection paid over a
    // hundred slot visits per poll for nothing, and allocatePacketId() was
    // worse than that: an O(64) loop calling an O(64) packetIdInUse(), so up
    // to ~4,160 visits across a 15 kB array for one identifier.
    //
    // An upper bound is enough to fix all of them and is far simpler to keep
    // honest than a live count: enqueue() raises it, and the scans lower it
    // when they notice trailing slots are free. It may lag high - that costs
    // a few wasted iterations and nothing else - but it can never lag low,
    // which is the only direction that would lose a delivery.
    size_t _queueExtent = 0;

    std::vector<uint16_t> _inbound;

    uint16_t _nextPacketId = 1;
    uint64_t _nextSequence = 1;

    int indexOfSubscription(const MqttSpan& filter) const;
};


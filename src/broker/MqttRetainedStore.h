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
// THE RETAINED MESSAGES
// =============================================================================
//
// One message per topic name, kept until it is replaced or cleared, and handed
// to whoever subscribes to a filter that matches it. [V5:3.3.1.3]
//
// THE RULES THAT ARE EASY TO GET WRONG, all of them in one place because all
// of them have been got wrong by shipping brokers:
//
//   - A retained PUBLISH with a ZERO-LENGTH PAYLOAD CLEARS the topic and is
//     NOT stored. [V5:MQTT-3.3.1-6] "If the Payload contains zero bytes it is
//     processed normally by the Server but any retained message with the same
//     topic name MUST be removed and any future subscribers for the topic will
//     not receive a retained message."
//   - The cleared message IS still delivered to current subscribers
//     [V5:MQTT-3.3.1-7] - it is a live message that also happens to clear the
//     store, not a store operation.
//   - A message delivered BECAUSE it was retained goes out with RETAIN = 1
//     [V5:MQTT-3.3.1-9]. NanoMQ gets this one wrong - see internal/INTEROP.md
//     D-2 - which is exactly why our own is tested rather than assumed.
//   - A retained message does NOT expire with the connection that published
//     it. It is the one piece of broker state that outlives everything.
//
// Message Expiry Interval is honoured: [V5:3.3.2.3.3] requires the Server to
// send the remaining expiry to a subscriber, and a retained message whose
// interval has elapsed must not be delivered at all.

#include "../MqttFeatures.h"


#include "../MqttTopic.h"
#include "../MqttPlatform.h"
#include "../MqttOwnedProperties.h"

#include <string>
#include <vector>
#if MQTT_RETAINED_INDEX
#include <unordered_map>
#endif

struct MqttRetainedMessage {
    std::string topic;
    std::vector<uint8_t> payload;
    uint8_t qos = MQ_QOS_0;

    // THE SAME TYPE THE DELIVERY QUEUE USES, deliberately.
    //
    // This was a separate set of fields once, and the one it left out was User
    // Property - so a retained message was served stripped of properties
    // [V5:MQTT-3.3.2-17] requires forwarded unaltered. Nothing in this
    // repository caught it; Paho's conformance suite did, on its first run.
    // Sharing the type is what stops the two lists drifting apart again.
    MqttOwnedProperties properties;

    // [V5:3.3.2.3.3] When it was stored, so the REMAINING expiry can be
    // computed for each subscriber rather than the original being repeated
    // for ever.
    mq_time_t storedAt = 0;

    bool inUse() const { return !topic.empty(); }
};

class MqttRetainedStore {
public:
    // Sized once. A broker that grew this without limit would let one client
    // with a loop in it exhaust the memory of everything else on the box.
    void configure(size_t maximum);

    // Stores, replaces, or - for a zero-length payload - clears.
    //
    // Returns false only when the store is full and this is a new topic. The
    // caller still delivers the message to current subscribers: failing to
    // RETAIN it is not a reason to fail to DELIVER it.
    bool store(const MqttSpan& topic, const MqttSpan& payload, uint8_t qos,
               const MqttProperties& properties, mq_time_t now);

    void clear(const MqttSpan& topic);
    void clearAll();

    size_t count() const;
    size_t capacity() const { return _messages.size(); }

    // Messages matching a filter, for a subscription that has just arrived.
    // Expired ones are removed as they are found rather than returned.
    //
    // The callback is a function pointer rather than std::function because
    // this is on the path of every SUBSCRIBE and generated wrapper code has to
    // be able to reach it - the same reason the client's per-packet hooks are.
    typedef void (*VisitFn)(void* context, const MqttRetainedMessage& message,
                            uint32_t remainingExpiry, bool hasRemainingExpiry);
    void visitMatching(const MqttSpan& filter, mq_time_t now,
                       VisitFn visit, void* context);

    // THE SAME WALK, IN INSTALMENTS, for a subscriber whose queue cannot take
    // everything at once. [MQTT-3.3.1-9] (5.0) and [MQTT-3.3.1-6] (3.1.1) say
    // the retained messages MUST be sent, so they are sent as room appears
    // rather than dropped when the queue is full.
    //
    // Starts at slot `from`, and stops when `visit` returns false - that
    // message was NOT taken, and the walk resumes AT it. Returns the slot to
    // resume from, or capacity() once every slot has been passed.
    //
    // A topic stored into a slot the walk has already passed is not missed:
    // it was published after the subscription existed, so it reached the
    // subscriber as an ordinary message.
    typedef bool (*StepFn)(void* context, const MqttRetainedMessage& message,
                           uint32_t remainingExpiry, bool hasRemainingExpiry);
    size_t visitMatchingFrom(const MqttSpan& filter, mq_time_t now, size_t from,
                             StepFn visit, void* context);

    // Whether this message has outlived its Message Expiry Interval.
    static bool hasExpired(const MqttRetainedMessage& message, mq_time_t now);

    // Removes everything that has expired. Called from the broker's poll so a
    // store full of expired messages does not refuse a new one.
    // Returns how many it removed, and through `liveOut` how many remain - so a
    // caller that wants the count does not walk the same array a second time.
    // poll() did exactly that, every poll, over 256 slots.
    size_t sweep(mq_time_t now, size_t* liveOut = nullptr);

    // From this many slots up, on a host, topics are found through a hash
    // index and free slots through a list, instead of by walking the store.
    // Below it the walk is faster than hashing, and both paths stay in use -
    // and tested.
    static const size_t kIndexFrom = 1024;
    bool indexed() const { return _indexed; }

private:
    std::vector<MqttRetainedMessage> _messages;

    int indexOf(const MqttSpan& topic) const;
    int freeSlot();

    // Empties a slot, and keeps the index and the free list true. Every
    // removal goes through here - a slot emptied anywhere else would leave
    // the index pointing at it.
    void release(size_t index);
    void occupied(size_t index);

    bool _indexed = false;
#if MQTT_RETAINED_INDEX
    std::unordered_map<std::string, uint32_t> _index;
    std::vector<uint32_t> _free;
#endif
};


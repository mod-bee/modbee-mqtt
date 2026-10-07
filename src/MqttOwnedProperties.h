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
// PROPERTIES THAT OUTLIVE THE PACKET THEY ARRIVED IN
// =============================================================================
//
// MqttProperties holds SPANS, which point into the buffer being decoded and
// are gone the moment the next packet arrives. That is right for decoding and
// wrong for everything else, so this is the other half: the same information,
// OWNED.
//
// Making the owned type DIFFERENT FROM THE WIRE TYPE means the mistake will
// not compile, which is worth more than a comment. It is the single most
// likely way to write a use-after-free in anything that holds a message.
//
// Three callers, and the list is why this sits at L2 rather than beside any
// one of them:
//
//   - a client PUBLISHING with properties, where the application supplies
//     strings the queue must outlive
//   - a broker's per-session queue, where a message waits for a client that
//     is not connected
//   - a broker's retained store, where it may wait for months
//
// It did not always exist. The retained store had its own set of fields once,
// and the one it forgot was User Property - [V5:MQTT-3.3.2-17] requires those
// forwarded unaltered, so a retained message arrived stripped of them and
// Paho's conformance suite found it on the first run. One type for every
// holder is the fix that keeps it fixed.

#include "MqttProperties.h"

#include <memory>
#include <string>
#include <vector>

struct MqttOwnedProperties {
    bool hasPayloadFormat = false;
    uint8_t payloadFormat = 0;

    bool hasExpiry = false;
    uint32_t expiryInterval = 0;

    bool hasContentType = false;
    std::string contentType;

    bool hasResponseTopic = false;
    std::string responseTopic;

    bool hasCorrelationData = false;
    std::vector<uint8_t> correlationData;

    // [V5:MQTT-3.3.2-17] "The Server MUST send all User Properties unaltered
    // in a PUBLISH packet when forwarding the Application Message to a
    // Client." Unaltered means every one of them, in the order they were sent,
    // so this is a list rather than a map - and two entries may share a key.
    struct UserProperty { std::string key; std::string value; };
    std::vector<UserProperty> user;

    // EMPTIED, NOT REBUILT.
    //
    // This was `*this = MqttOwnedProperties()`, which move-assigns from a
    // fresh temporary and so FREES every buffer it holds. The next message
    // through the same slot then had to allocate them all over again, for ever
    // - a guaranteed trip to the allocator per message per subscriber, on a
    // path that runs inside a PLC scan and on a board with 80 kB of RAM.
    //
    // clear() on each member keeps the capacity already paid for, so a warm
    // queue settles into allocating nothing at all. Every field is listed
    // rather than assigned from a temporary, which is the only way to keep
    // that property - and a field added later must be added here too.
    void clear() {
        hasPayloadFormat = false;
        payloadFormat = 0;
        hasExpiry = false;
        expiryInterval = 0;
        hasContentType = false;
        contentType.clear();
        hasResponseTopic = false;
        responseTopic.clear();
        hasCorrelationData = false;
        correlationData.clear();
        // resize(0), not clear(): both leave size zero, but this keeps the
        // vector's own capacity so the next message's user properties land in
        // slots that already exist. copyForForwarding() resizes back up and
        // assigns into them.
        user.resize(0);
    }

    // Needed by the queue's duplicate check. Two publishes with the same
    // payload and DIFFERENT properties are not the same message - reporting
    // the second as a duplicate would discard a correlation id or a response
    // topic and tell the caller nothing was lost.
    bool operator==(const MqttOwnedProperties& other) const;
    bool operator!=(const MqttOwnedProperties& other) const {
        return !(*this == other);
    }

    // Whether there is anything here at all. A publish with none of these is
    // the ordinary case and must cost nothing, on either version.
    bool empty() const {
        return !hasPayloadFormat && !hasExpiry && !hasContentType &&
               !hasResponseTopic && !hasCorrelationData && user.empty();
    }

    // --- Setting them, for an outbound message ---
    //
    // Named calls rather than raw fields, so the `has` flag cannot be
    // forgotten. A property that is ABSENT and one that is PRESENT AND ZERO
    // are different things on the wire, and setting the value without the flag
    // is the quiet way to send neither.

    // [V5:3.3.2.3.2] 0 says the payload is unspecified bytes, 1 says it is
    // UTF-8. A receiver may refuse a payload that is not what this claims -
    // reason code 0x99, Payload format invalid - so saying 1 about bytes that
    // are not text is a message the broker is entitled to reject.
    void setPayloadFormat(bool isUtf8Text) {
        hasPayloadFormat = true;
        payloadFormat = isUtf8Text ? 1 : 0;
    }

    // [V5:3.3.2.3.3] Seconds. The broker discards the message if it has not
    // been delivered within this long, and hands a retained message on with
    // the time it has LEFT rather than the time it was given. It is the only
    // thing in MQTT that puts an age on a retained value.
    void setMessageExpiry(uint32_t seconds) {
        hasExpiry = true;
        expiryInterval = seconds;
    }

    // [V5:3.3.2.3.9] A MIME type - "application/json", "text/plain". The
    // broker does not interpret it; the receiving application does.
    void setContentType(const char* type) {
        hasContentType = true;
        contentType = type ? type : "";
    }

    // [V5:3.3.2.3.5] Where the answer should be published. Must be a Topic
    // NAME - no wildcards - and MqttClient::publish() refuses one that is not,
    // at the call that made the mistake.
    void setResponseTopic(const char* topic) {
        hasResponseTopic = true;
        responseTopic = topic ? topic : "";
    }

    // [V5:3.3.2.3.6] Bytes the requester uses to match an answer to the
    // request that asked for it. The broker never looks at them.
    void setCorrelationData(const uint8_t* data, size_t length) {
        hasCorrelationData = true;
        correlationData.assign(data, data + length);
    }
    void setCorrelationData(const char* text);

    // [V5:3.3.2.3.7] Anything the two ends have agreed on. Duplicate keys are
    // allowed and order is preserved, both of which the standard requires.
    // False only when there is no room - never a silent drop.
    bool addUserProperty(const char* key, const char* value);

    // The first value for this key, or null. For the common case where a key
    // appears once; walk `user` directly when it may repeat.
    const char* userProperty(const char* key) const;

    // Copies what a forwarded PUBLISH must carry. Deliberately NOT Topic Alias
    // (per-connection, [V5:3.3.2.3.4]) and NOT Subscription Identifier
    // (the subscriber's, [V5:3.8.2.1.2]) - forwarding either is a bug that
    // only appears at the second subscriber.
    void copyForForwarding(const MqttProperties& from);

    // The other direction: fill a wire property set from this one. The spans
    // in `into` POINT AT THIS OBJECT, so it must outlive the encode.
    void describeTo(MqttProperties& into) const;
};


// =============================================================================
// PROPERTIES ON A QUEUED OPERATION, WITHOUT PAYING FOR THEM
// =============================================================================
//
// The send queue is a FIXED ARRAY, sized MQTT_QUEUE_CAPACITY at compile time so
// a device with no spare heap never grows one. An MqttOwnedProperties member
// inside each slot would therefore cost its full size on every client, used or
// not: 144 bytes x 32 slots = 4,608 bytes of RAM, measured, on a board that
// may have 320 kB in total and has already spent 8 kB on buffers.
//
// Almost no message carries properties. So the slot holds a POINTER - eight
// bytes, null for the ordinary case - and the storage is allocated only when
// something actually needs it.
//
// It has VALUE SEMANTICS on purpose. MqttOperation is copied by assignment in
// two places, one of them the supersede path, and a bare unique_ptr member
// would have forced a hand-written copy constructor listing every field in the
// operation - which is a silent bug the first time somebody adds a field and
// forgets it. This wrapper copies itself correctly and the operation keeps the
// compiler's own copy.
class MqttOptionalProperties {
public:
    MqttOptionalProperties() = default;
    MqttOptionalProperties(const MqttOptionalProperties& other) { *this = other; }
    MqttOptionalProperties& operator=(const MqttOptionalProperties& other) {
        if (this == &other) return *this;
        if (other._held) _held.reset(new MqttOwnedProperties(*other._held));
        else _held.reset();
        return *this;
    }

    // A set with nothing in it and no set at all are the same thing here: both
    // mean "encode no properties", and neither allocates.
    bool empty() const { return !_held || _held->empty(); }

    void set(const MqttOwnedProperties& properties) {
        if (properties.empty()) { _held.reset(); return; }
        _held.reset(new MqttOwnedProperties(properties));
    }

    void clear() { _held.reset(); }

    // Null when there are none. The caller has already asked empty().
    const MqttOwnedProperties* get() const { return _held.get(); }

    bool operator==(const MqttOptionalProperties& other) const {
        if (empty() && other.empty()) return true;
        if (empty() != other.empty()) return false;
        return *_held == *other._held;
    }
    bool operator!=(const MqttOptionalProperties& other) const {
        return !(*this == other);
    }

private:
    std::unique_ptr<MqttOwnedProperties> _held;
};

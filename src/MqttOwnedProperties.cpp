// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttOwnedProperties.h"

#include <string.h>
#include "MqttNoExcept.h"

namespace {

MqttSpan spanOf(const MQ_STRING_REF& text) {
    MqttSpan span;
    if (text.empty()) return span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

} // namespace

bool MqttOwnedProperties::operator==(const MqttOwnedProperties& other) const {
    if (hasPayloadFormat != other.hasPayloadFormat) return false;
    if (hasPayloadFormat && payloadFormat != other.payloadFormat) return false;
    if (hasExpiry != other.hasExpiry) return false;
    if (hasExpiry && expiryInterval != other.expiryInterval) return false;
    if (hasContentType != other.hasContentType) return false;
    if (hasContentType && contentType != other.contentType) return false;
    if (hasResponseTopic != other.hasResponseTopic) return false;
    if (hasResponseTopic && responseTopic != other.responseTopic) return false;
    if (hasCorrelationData != other.hasCorrelationData) return false;
    if (hasCorrelationData && correlationData != other.correlationData) return false;

    // Order matters: [V5:MQTT-3.3.2-17] requires User Properties forwarded
    // unaltered, and the order they were sent in is part of that.
    if (user.size() != other.user.size()) return false;
    for (size_t i = 0; i < user.size(); i++) {
        if (user[i].key != other.user[i].key) return false;
        if (user[i].value != other.user[i].value) return false;
    }
    return true;
}

void MqttOwnedProperties::setCorrelationData(const char* text) {
    const size_t length = text ? strlen(text) : 0;
    setCorrelationData((const uint8_t*)text, length);
}

bool MqttOwnedProperties::addUserProperty(const char* key, const char* value) {
    if (key == nullptr) return false;

    // Bounded by what the wire structure can carry, so a set built here always
    // encodes. Refusing here is a caller's problem; discovering it at encode
    // time would be a message that vanished.
    if (user.size() >= MQTT_MAX_USER_PROPERTIES) return false;
#if MQTT_LEAN
    if (user.full()) return false;   // a board's list is fixed (MqttFixed.h)
#endif

    UserProperty entry;
    entry.key = key;
    entry.value = value ? value : "";
    user.push_back(entry);
    return true;
}

const char* MqttOwnedProperties::userProperty(const char* key) const {
    if (key == nullptr) return nullptr;
    for (size_t i = 0; i < user.size(); i++) {
        if (user[i].key == key) return user[i].value.c_str();
    }
    return nullptr;
}

void MqttOwnedProperties::copyForForwarding(const MqttProperties& from) {
    clear();

    if (from.has(MQ_PROP_PAYLOAD_FORMAT_INDICATOR)) {
        hasPayloadFormat = true;
        payloadFormat = from.payloadFormatIndicator;
    }
    if (from.has(MQ_PROP_MESSAGE_EXPIRY_INTERVAL)) {
        hasExpiry = true;
        expiryInterval = from.messageExpiryInterval;
    }
    // assign() INTO THE EXISTING BUFFER rather than assigning a fresh string
    // over the top of it. toString() builds a temporary that then has to be
    // copied or moved in, and with clear() now keeping capacity these two
    // write straight into storage the slot already owns.
    if (from.has(MQ_PROP_CONTENT_TYPE)) {
        hasContentType = true;
        contentType.assign((const char*)from.contentType.data, from.contentType.length);
    }
    if (from.has(MQ_PROP_RESPONSE_TOPIC)) {
        hasResponseTopic = true;
        responseTopic.assign((const char*)from.responseTopic.data,
                             from.responseTopic.length);
    }
    if (from.has(MQ_PROP_CORRELATION_DATA)) {
        hasCorrelationData = true;
        correlationData.assign(
            from.correlationData.data,
            from.correlationData.data + from.correlationData.length);
    }
    // GROWN ONCE, not log2(n) times. push_back into an unreserved vector
    // reallocates as it goes, and every reallocation copies every std::string
    // already in it - so N user properties cost far more than N copies. The
    // count is known before the loop starts, so it is asked for up front.
    //
    // resize() rather than reserve() + push_back, because clear() left the
    // elements' own buffers behind: reusing those slots assigns into strings
    // that already have room instead of constructing new ones.
    if (from.userCount > 0) {
        const size_t previous = user.size();
        user.resize(from.userCount);
        (void)previous;
#if MQTT_LEAN
        // A board's list is fixed: what does not fit is not forwarded.
        for (uint8_t index = 0; index < (uint8_t)user.size(); index++) {
#else
        for (uint8_t index = 0; index < from.userCount; index++) {
#endif
            user[index].key.assign((const char*)from.user[index].key.data,
                                   from.user[index].key.length);
            user[index].value.assign((const char*)from.user[index].value.data,
                                     from.user[index].value.length);
        }
    }
}

void MqttOwnedProperties::describeTo(MqttProperties& into) const {
    // THE SPANS BELOW POINT INTO THIS OBJECT. Whoever calls this must keep it
    // alive until the encode is finished - which in this library means the
    // operation is still in the queue, because that is what owns it.
    if (hasPayloadFormat) {
        into.payloadFormatIndicator = payloadFormat;
        into.markPresent(MQ_PROP_PAYLOAD_FORMAT_INDICATOR);
    }
    if (hasExpiry) {
        into.messageExpiryInterval = expiryInterval;
        into.markPresent(MQ_PROP_MESSAGE_EXPIRY_INTERVAL);
    }
    if (hasContentType) {
        into.contentType = spanOf(contentType);
        into.markPresent(MQ_PROP_CONTENT_TYPE);
    }
    if (hasResponseTopic) {
        into.responseTopic = spanOf(responseTopic);
        into.markPresent(MQ_PROP_RESPONSE_TOPIC);
    }
    if (hasCorrelationData) {
        into.correlationData.data =
            correlationData.empty() ? nullptr : correlationData.data();
        into.correlationData.length = (uint32_t)correlationData.size();
        into.markPresent(MQ_PROP_CORRELATION_DATA);
    }
    for (size_t index = 0; index < user.size(); index++) {
        into.addUserProperty(spanOf(user[index].key), spanOf(user[index].value));
    }
}

#if MQTT_LEAN
// --- A queued publish's properties on a board ---

size_t MqttOptionalProperties::encode(uint8_t* out, size_t capacity) const {
    if (_source == nullptr) {
        if (_encoded.size() > capacity) return 0;
        if (!_encoded.empty()) memcpy(out, _encoded.data(), _encoded.size());
        return _encoded.size();
    }
    MqttProperties properties;
    _source->describeTo(properties);
    const size_t length = mq_propertiesSize(properties, MQ_PKT_PUBLISH);
    if (length > capacity) return 0;
    MqttWriter writer(out, capacity);
    mq_encodeProperties(writer, properties, MQ_PKT_PUBLISH);
    return writer.ok() ? writer.written() : 0;
}

void MqttOptionalProperties::store() {
    uint8_t bytes[MQTT_QUEUED_PROPERTY_BYTES];
    const size_t length = encode(bytes, sizeof bytes);
    _source = nullptr;
    if (length == 0) {
        _encoded.clear();
        _failed = true;
        return;
    }
    _encoded.assign(bytes, bytes + length);
}

void MqttOptionalProperties::set(const MqttOwnedProperties& properties) {
    clear();
    if (properties.empty()) return;
    _source = &properties;
    if (_encoded.bound()) store();
}

MqttOptionalProperties& MqttOptionalProperties::operator=(const MqttOptionalProperties& other) {
    if (this == &other) return *this;
    if (!_encoded.bound()) {
        _source = other._source;
        _encoded = other._encoded;
        _failed = other._failed;
        return *this;
    }
    clear();
    if (other.empty()) return *this;
    if (other._source) {
        _source = other._source;
        store();
    } else {
        _encoded = other._encoded;
    }
    return *this;
}

size_t MqttOptionalProperties::encodedSize() const {
    if (empty()) return 0;
    if (_source == nullptr) return _encoded.size();
    uint8_t bytes[MQTT_QUEUED_PROPERTY_BYTES];
    const size_t length = encode(bytes, sizeof bytes);
    return length ? length : (size_t)-1;
}

void MqttOptionalProperties::describeTo(MqttProperties& into) const {
    if (_source) {
        _source->describeTo(into);
        return;
    }
    if (_encoded.empty()) return;
    MqttReader reader(_encoded.data(), _encoded.size());
    mq_decodeProperties(reader, MQ_PKT_PUBLISH, into);
}

bool MqttOptionalProperties::operator==(const MqttOptionalProperties& other) const {
    if (empty() && other.empty()) return true;
    if (empty() != other.empty()) return false;
    uint8_t mine[MQTT_QUEUED_PROPERTY_BYTES];
    uint8_t theirs[MQTT_QUEUED_PROPERTY_BYTES];
    const size_t a = encode(mine, sizeof mine);
    const size_t b = other.encode(theirs, sizeof theirs);
    return a != 0 && a == b && memcmp(mine, theirs, a) == 0;
}
#endif

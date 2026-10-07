// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugTopic.h"

#include "../MqttWire.h"

namespace {

// One element into a fixed buffer. Returns false rather than truncating, and
// the caller abandons the whole topic - a half-written topic that still parses
// is the fault that publishes to the wrong place.
// Abandoning a topic empties the buffer.
//
// THE RETURN VALUE IS NOT THE ONLY THING A CALLER SEES. A refusal that left
// "spBv1.0" in the buffer would let a caller who ignored the return publish to
// the namespace root - found by test_sparkplug_topic, which asserted the empty
// string because the header promises nothing is ever truncated.
size_t abandon(char* out) {
    if (out != nullptr) out[0] = '\0';
    return 0;
}

bool appendText(char* out, size_t capacity, size_t& cursor, const char* text) {
    if (text == nullptr) return false;
    const size_t length = strlen(text);
    if (length + 1 > capacity - cursor) return false;
    memcpy(out + cursor, text, length);
    cursor += length;
    out[cursor] = '\0';
    return true;
}

bool appendChar(char* out, size_t capacity, size_t& cursor, char value) {
    if (cursor + 2 > capacity) return false;
    out[cursor++] = value;
    out[cursor] = '\0';
    return true;
}

// Copies one topic element into a fixed field, refusing rather than truncating.
bool copyId(char* out, size_t capacity, const char* from, size_t length) {
    if (length == 0 || length + 1 > capacity) return false;
    memcpy(out, from, length);
    out[length] = '\0';
    return true;
}

}  // namespace

bool mq_sparkplugValidId(const char* id) {
    if (id == nullptr) return false;

    const size_t length = strlen(id);
    if (length == 0) return false;
    if (length + 1 > MQ_SP_MAX_ID) return false;

    for (size_t index = 0; index < length; index++) {
        const char value = id[index];
        // The three reserved characters.
        // [tck-id-topic-structure-namespace-valid-group-id] and siblings.
        if (value == '+' || value == '/' || value == '#') return false;
        // A NUL inside a UTF-8 string is refused by MQTT itself
        // [V5:MQTT-1.5.4-2], and strlen would have stopped here anyway - this
        // is here so the reason is written down rather than implied.
        if (value == '\0') return false;
    }

    // Valid UTF-8, using the library's own validator rather than a second
    // implementation. The specification says "a valid UTF-8 string", and MQTT
    // already requires that of every topic, so a topic element that fails this
    // would be refused by the transport anyway - but it is refused here, where
    // the error can name the identifier.
    return mq_validateUtf8((const uint8_t*)id, length) == MqttError::None;
}

size_t mq_sparkplugBuildTopic(char* out, size_t capacity,
                              MqttSparkplugType type,
                              const char* group, const char* node,
                              const char* device) {
    if (out == nullptr || capacity == 0) return abandon(out);
    out[0] = '\0';

    // STATE has its own shape and its own builder. Routing it here would
    // produce spBv1.0/{group}/STATE/{node}, which is not a topic Sparkplug
    // defines.
    if (type == MqttSparkplugType::State) return abandon(out);

    if (!mq_sparkplugValidId(group)) return abandon(out);
    if (!mq_sparkplugValidId(node)) return abandon(out);

    const bool wantsDevice = mq_sparkplugIsDeviceType(type);
    if (wantsDevice) {
        // [tck-id-topic-structure-namespace-device-id-associated-message-types]
        // The device_id MUST be included for DBIRTH, DDEATH, DDATA and DCMD.
        if (!mq_sparkplugValidId(device)) return abandon(out);
    } else {
        // -non-associated-message-types: it MUST NOT be included for the rest.
        // A caller that passed one meant something, and quietly dropping it
        // would publish to a topic they did not ask for.
        if (device != nullptr) return abandon(out);
    }

    size_t cursor = 0;
    if (!appendText(out, capacity, cursor, MQ_SP_NAMESPACE)) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor, group)) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor, mq_sparkplugTypeName(type))) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor, node)) return abandon(out);
    if (wantsDevice) {
        if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
        if (!appendText(out, capacity, cursor, device)) return abandon(out);
    }
    return cursor;
}

size_t mq_sparkplugBuildStateTopic(char* out, size_t capacity,
                                   const char* hostId) {
    if (out == nullptr || capacity == 0) return abandon(out);
    out[0] = '\0';

    // [tck-id-payloads-state-will-message] ... on the topic
    // spBv1.0/STATE/[sparkplug_host_id]. "This can be any UTF-8 string" - but
    // it still occupies a topic element, so the reserved characters are still
    // out, which is what mq_sparkplugValidId() checks.
    if (!mq_sparkplugValidId(hostId)) return abandon(out);

    size_t cursor = 0;
    if (!appendText(out, capacity, cursor, MQ_SP_NAMESPACE)) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor, MQ_SP_STATE_ELEMENT)) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor, hostId)) return abandon(out);
    return cursor;
}

bool mq_sparkplugParseTopic(const char* topic, size_t length,
                            MqttSparkplugTopicParts& out) {
    out = MqttSparkplugTopicParts();
    if (topic == nullptr || length == 0) return false;

    // Split on '/' without copying. At most five elements matter; a sixth means
    // this is not a Sparkplug topic.
    const char* elements[6] = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
    size_t lengths[6] = {0, 0, 0, 0, 0, 0};
    size_t count = 0;

    size_t start = 0;
    for (size_t index = 0; index <= length; index++) {
        if (index == length || topic[index] == '/') {
            if (count >= 6) return false;
            elements[count] = topic + start;
            lengths[count] = index - start;
            count++;
            start = index + 1;
        }
    }

    // The namespace first, and exactly. A topic in another namespace is not
    // ours to interpret - it may be a perfectly good MQTT topic belonging to
    // something else on the same broker.
    const size_t namespaceLength = strlen(MQ_SP_NAMESPACE);
    if (lengths[0] != namespaceLength) return false;
    if (memcmp(elements[0], MQ_SP_NAMESPACE, namespaceLength) != 0) return false;

    // STATE is three elements with the literal in the second position, which is
    // where a group_id sits on every other topic. Checked before the message
    // type, because "STATE" is not a message_type value and would fail below.
    const size_t stateLength = strlen(MQ_SP_STATE_ELEMENT);
    if (count == 3 && lengths[1] == stateLength &&
        memcmp(elements[1], MQ_SP_STATE_ELEMENT, stateLength) == 0) {
        out.type = MqttSparkplugType::State;
        // The host id goes in `group` - see the comment on isState().
        if (!copyId(out.group, sizeof out.group, elements[2], lengths[2])) {
            return false;
        }
        return true;
    }

    if (count != 4 && count != 5) return false;

    if (!mq_sparkplugTypeFromName(elements[2], lengths[2], out.type)) return false;
    // A STATE reached through the message_type position would be a topic of the
    // form spBv1.0/{group}/STATE/{node}, which Sparkplug does not define.
    if (out.type == MqttSparkplugType::State) return false;

    const bool wantsDevice = mq_sparkplugIsDeviceType(out.type);
    if (wantsDevice && count != 5) return false;
    if (!wantsDevice && count != 4) return false;

    if (!copyId(out.group, sizeof out.group, elements[1], lengths[1])) return false;
    if (!copyId(out.node, sizeof out.node, elements[3], lengths[3])) return false;
    if (wantsDevice) {
        if (!copyId(out.device, sizeof out.device, elements[4], lengths[4])) {
            return false;
        }
        out.hasDevice = true;
    }

    // The elements came out of a topic MQTT already validated as UTF-8, so
    // re-validating would be work for nothing - but the reserved characters
    // cannot appear, because '/' was the separator and '+' and '#' in a
    // PUBLISH topic are refused by the broker [V5:MQTT-3.3.2-2]. Nothing more
    // to check.
    return true;
}

size_t mq_sparkplugNodeCommandFilter(char* out, size_t capacity,
                                     const char* group, const char* node) {
    return mq_sparkplugBuildTopic(out, capacity, MqttSparkplugType::NCmd,
                                  group, node);
}

size_t mq_sparkplugDeviceCommandFilter(char* out, size_t capacity,
                                       const char* group, const char* node) {
    if (out == nullptr || capacity == 0) return abandon(out);
    out[0] = '\0';
    if (!mq_sparkplugValidId(group)) return abandon(out);
    if (!mq_sparkplugValidId(node)) return abandon(out);

    size_t cursor = 0;
    if (!appendText(out, capacity, cursor, MQ_SP_NAMESPACE)) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor, group)) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor,
                    mq_sparkplugTypeName(MqttSparkplugType::DCmd))) return abandon(out);
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendText(out, capacity, cursor, node)) return abandon(out);
    // One level, not '#'. A '+' matches exactly one element, so this covers
    // every device under this node and nothing deeper - whereas '#' would also
    // match spBv1.0/{group}/DCMD/{node}/{device}/anything, which is not a topic
    // Sparkplug defines and which we would then have to ignore at runtime.
    if (!appendChar(out, capacity, cursor, '/')) return abandon(out);
    if (!appendChar(out, capacity, cursor, '+')) return abandon(out);
    return cursor;
}

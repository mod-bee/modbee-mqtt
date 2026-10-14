// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTopic.h"

#include <string.h>
#include "MqttNoExcept.h"

MqttSpan mq_topicSpan(const char* text) {
    MqttSpan span;
    if (!text) return span;
    const size_t length = strlen(text);
    if (length > MQ_STRING_MAX) return span;
    span.data = (const uint8_t*)text;
    span.length = (uint32_t)length;
    return span;
}

namespace {

inline bool isWildcard(uint8_t byte) {
    return byte == (uint8_t)MQ_TOPIC_MULTI_WILDCARD ||
           byte == (uint8_t)MQ_TOPIC_SINGLE_WILDCARD;
}

// Validates the filter part, after any $share/{name}/ prefix has been removed.
bool filterBodyIsValid(const uint8_t* data, size_t length) {
    // [V5:MQTT-4.7.3-1] at least one character.
    if (length == 0) return false;

    for (size_t i = 0; i < length; i++) {
        const uint8_t byte = data[i];

        // Already refused by the UTF-8 validator on the way in, but a caller
        // may hand us a literal that never went through it.
        // [V5:MQTT-4.7.3-2]
        if (byte == 0) return false;

        if (byte == (uint8_t)MQ_TOPIC_MULTI_WILDCARD) {
            // [V5:MQTT-4.7.1-1] "The multi-level wildcard character MUST be
            // specified either on its own or following a topic level
            // separator. In either case it MUST be the last character
            // specified in the Topic Filter."
            //
            // So "sport/tennis#" is invalid - the '#' does not follow a '/' -
            // and "sport/tennis/#/ranking" is invalid, because it is not last.
            if (i != 0 && data[i - 1] != (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR) return false;
            if (i != length - 1) return false;
        }

        if (byte == (uint8_t)MQ_TOPIC_SINGLE_WILDCARD) {
            // [V5:MQTT-4.7.1-2] "Where it is used, it MUST occupy an entire
            // level of the filter."
            //
            // So it must begin a level and end one: "sport+" is invalid,
            // "sport/+/player1" is valid, and a bare "+" is valid.
            const bool startsLevel =
                (i == 0) || data[i - 1] == (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR;
            const bool endsLevel =
                (i == length - 1) || data[i + 1] == (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR;
            if (!startsLevel || !endsLevel) return false;
        }
    }
    return true;
}

} // namespace

bool mq_topicNameIsValid(const MqttSpan& name) {
    // [V5:MQTT-4.7.3-1] at least one character.
    if (name.length == 0 || !name.data) return false;

    // [V5:MQTT-4.7.3-3] a Topic Name is a UTF-8 Encoded String, so the
    // two-byte length prefix already bounds it at 65,535. A span cannot hold
    // more, so there is nothing further to check here.

    for (uint32_t i = 0; i < name.length; i++) {
        // [V5:MQTT-4.7.0-1] "The wildcard characters can be used in Topic
        // Filters, but MUST NOT be used within a Topic Name."
        if (isWildcard(name.data[i])) return false;
        // [V5:MQTT-4.7.3-2]
        if (name.data[i] == 0) return false;
    }
    return true;
}

bool mq_topicFilterIsValid(const MqttSpan& filter) {
    if (filter.length == 0 || !filter.data) return false;

    if (mq_isSharedFilter(filter)) {
        MqttSpan shareName;
        MqttSpan inner;
        // The split checks the ShareName's own rules; the remainder is an
        // ordinary filter and is validated as one.
        return mq_splitSharedFilter(filter, shareName, inner);
    }
    return filterBodyIsValid(filter.data, filter.length);
}

bool mq_isSharedFilter(const MqttSpan& filter) {
    if (!filter.data || filter.length < MQ_SHARE_PREFIX_LEN) return false;
    return memcmp(filter.data, MQ_SHARE_PREFIX, MQ_SHARE_PREFIX_LEN) == 0;
}

bool mq_splitSharedFilter(const MqttSpan& filter, MqttSpan& shareName,
                          MqttSpan& innerFilter) {
    shareName = MqttSpan();
    innerFilter = MqttSpan();
    if (!mq_isSharedFilter(filter)) return false;

    const uint8_t* after = filter.data + MQ_SHARE_PREFIX_LEN;
    const uint32_t remaining = filter.length - MQ_SHARE_PREFIX_LEN;

    // [V5:MQTT-4.8.2-2] The ShareName MUST NOT contain '/', '+' or '#', but
    // MUST be followed by a '/'. So the first '/' ends the name, and anything
    // before it that is a wildcard makes the filter invalid.
    uint32_t separator = 0;
    while (separator < remaining &&
           after[separator] != (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR) {
        if (isWildcard(after[separator])) return false;
        separator++;
    }

    // [V5:MQTT-4.8.2-1] a ShareName at least one character long, and
    // [V5:MQTT-4.8.2-2] a '/' after it followed by a Topic Filter.
    if (separator == 0) return false;            // empty ShareName
    if (separator >= remaining) return false;    // no '/' after the ShareName

    const uint32_t innerLength = remaining - separator - 1;
    if (innerLength == 0) return false;          // nothing after the '/'

    if (!filterBodyIsValid(after + separator + 1, innerLength)) return false;

    shareName.data = after;
    shareName.length = separator;
    innerFilter.data = after + separator + 1;
    innerFilter.length = innerLength;
    return true;
}

bool mq_topicMatches(const MqttSpan& filter, const MqttSpan& name) {
    if (!filter.data || !name.data) return false;
    if (filter.length == 0 || name.length == 0) return false;

    // A shared subscription matches on the filter after the $share/{name}/
    // prefix. The prefix identifies the group; it is not part of what is
    // matched against a Topic Name.
    MqttSpan effective = filter;
    if (mq_isSharedFilter(filter)) {
        MqttSpan shareName;
        MqttSpan inner;
        if (!mq_splitSharedFilter(filter, shareName, inner)) return false;
        effective = inner;
    }

    const uint8_t* f = effective.data;
    const uint32_t flen = effective.length;
    const uint8_t* n = name.data;
    const uint32_t nlen = name.length;

    // [V5:MQTT-4.7.2-1] "The Server MUST NOT match Topic Filters starting with
    // a wildcard character (# or +) with Topic Names beginning with a $
    // character."
    //
    // Checked before anything else, because it is a property of the two whole
    // strings rather than of any one level. Without it, a client subscribing
    // to "#" for its own diagnostics is handed every $SYS statistic the broker
    // publishes.
    if (n[0] == (uint8_t)MQ_TOPIC_DOLLAR && isWildcard(f[0])) return false;

    uint32_t fi = 0;
    uint32_t ni = 0;

    while (fi < flen) {
        if (f[fi] == (uint8_t)MQ_TOPIC_MULTI_WILDCARD) {
            // '#' is the last character by validation, and it matches the
            // parent level and any number of child levels. V5:4.7.1.2 -
            // "sport/#" also matches the singular "sport", which is why the
            // remaining name may legitimately be empty here.
            return true;
        }

        if (f[fi] == (uint8_t)MQ_TOPIC_SINGLE_WILDCARD) {
            // Exactly one level, which may be zero characters long: "sport/+"
            // matches "sport/" because the level after the separator exists
            // and is empty.
            while (ni < nlen && n[ni] != (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR) ni++;
            fi++;                    // past the '+'
            // Both must now be at a separator or at the end, together.
            if (fi < flen && f[fi] == (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR &&
                ni < nlen && n[ni] == (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR) {
                fi++;
                ni++;
                continue;
            }
            if (fi == flen && ni == nlen) return true;

            // One ran out before the other. The exception is a trailing "/#"
            // after the '+', which still matches because '#' includes the
            // parent: "sport/+/#" matches "sport/x".
            if (ni == nlen && fi + 1 < flen &&
                f[fi] == (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR &&
                f[fi + 1] == (uint8_t)MQ_TOPIC_MULTI_WILDCARD &&
                fi + 2 == flen) {
                return true;
            }
            return false;
        }

        // [V5:MQTT-4.7.3-4] character for character, with no normalisation and
        // no substitution of anything unrecognised.
        if (ni >= nlen || f[fi] != n[ni]) {
            // The one exception: a filter ending "/#" matches the name that is
            // its parent. "sport/tennis/#" matches "sport/tennis", so the
            // filter may have two characters left when the name has none.
            if (ni == nlen && fi + 1 < flen &&
                f[fi] == (uint8_t)MQ_TOPIC_LEVEL_SEPARATOR &&
                f[fi + 1] == (uint8_t)MQ_TOPIC_MULTI_WILDCARD &&
                fi + 2 == flen) {
                return true;
            }
            return false;
        }
        fi++;
        ni++;
    }

    // Both must have ended together. A filter that ran out with name left over
    // does not match: "sport/+" does not match "sport/tennis/player1".
    return ni == nlen;
}

// =============================================================================
// DO TWO FILTERS SHARE ANY TOPIC AT ALL
// =============================================================================

bool mq_filtersOverlap(const MqttSpan& left, const MqttSpan& right) {
    if (left.data == nullptr || right.data == nullptr) return false;

    uint32_t leftAt = 0;
    uint32_t rightAt = 0;
    bool leftDone = false;
    bool rightDone = false;

    // "EXHAUSTED" IS NOT THE SAME AS "AT THE END OF THE BYTES". A filter that
    // ends in a separator has one more level after it, and that level is the
    // empty string - "sport/" has two levels and matches "sport/+", while
    // "sport" has one and does not. A cursor that treated the end of the bytes
    // as the end of the levels got that pair wrong, which is what the topic
    // test caught.
    while (true) {
        if (leftDone && rightDone) return true;

        // One side has run out of levels. The other can still reach the
        // shorter topic only if what remains is exactly "#", because
        // "sport/#" matches "sport" itself [V5:4.7.1.2].
        if (leftDone) {
            return right.length - rightAt == 1 &&
                   right.data[rightAt] == MQ_TOPIC_MULTI_WILDCARD;
        }
        if (rightDone) {
            return left.length - leftAt == 1 &&
                   left.data[leftAt] == MQ_TOPIC_MULTI_WILDCARD;
        }

        uint32_t leftEnd = leftAt;
        while (leftEnd < left.length && left.data[leftEnd] != MQ_TOPIC_LEVEL_SEPARATOR) {
            leftEnd++;
        }
        uint32_t rightEnd = rightAt;
        while (rightEnd < right.length &&
               right.data[rightEnd] != MQ_TOPIC_LEVEL_SEPARATOR) {
            rightEnd++;
        }

        const uint32_t leftLength = leftEnd - leftAt;
        const uint32_t rightLength = rightEnd - rightAt;

        // "#" swallows everything remaining on the other side, so from here on
        // there is always some topic both would match.
        if (leftLength == 1 && left.data[leftAt] == MQ_TOPIC_MULTI_WILDCARD) return true;
        if (rightLength == 1 && right.data[rightAt] == MQ_TOPIC_MULTI_WILDCARD) return true;

        const bool leftAny = leftLength == 1 && left.data[leftAt] == MQ_TOPIC_SINGLE_WILDCARD;
        const bool rightAny = rightLength == 1 && right.data[rightAt] == MQ_TOPIC_SINGLE_WILDCARD;

        if (!leftAny && !rightAny) {
            if (leftLength != rightLength) return false;
            for (uint32_t index = 0; index < leftLength; index++) {
                if (left.data[leftAt + index] != right.data[rightAt + index]) return false;
            }
        }

        if (leftEnd >= left.length) { leftDone = true; leftAt = left.length; }
        else leftAt = leftEnd + 1;

        if (rightEnd >= right.length) { rightDone = true; rightAt = right.length; }
        else rightAt = rightEnd + 1;
    }
}

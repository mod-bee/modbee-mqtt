// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttWire.h"

#include <string.h>
#include "MqttNoExcept.h"

// =============================================================================
// ERRORS
// =============================================================================

const char* mq_errorName(MqttError error) {
    switch (error) {
        case MqttError::None:            return "ok";
        case MqttError::Truncated:       return "ran out of bytes";
        case MqttError::TrailingBytes:   return "bytes left over";
        case MqttError::VbiTooLong:      return "variable byte integer over four bytes";
        case MqttError::VbiNotCanonical: return "variable byte integer not minimally encoded";
        case MqttError::InvalidUtf8:     return "ill-formed UTF-8";
        case MqttError::NullCharacter:   return "UTF-8 string contains U+0000";
        case MqttError::Surrogate:       return "UTF-8 string contains a surrogate";
        case MqttError::Overflow:        return "would not fit in the buffer";
        case MqttError::BadArgument:     return "null data with a non-zero length";
        case MqttError::PropertyNotAllowed:  return "property not valid for this packet";
        case MqttError::PropertyRepeated:    return "property repeated";
        case MqttError::PropertyValueInvalid: return "property value out of range";
        case MqttError::TooManyProperties:   return "more properties than this build holds";
        case MqttError::NotSupportedByProtocol: return "this protocol version cannot express it";
    }
    return "unknown error";
}

uint8_t mq_errorToReason(MqttError error) {
    switch (error) {
        case MqttError::None:
            return MQ_REASON_SUCCESS;

        // Every decode fault here is a Malformed Packet in the standard's own
        // terms: the bytes could not be parsed. A Protocol Error is different
        // and is raised a layer up, where a packet parses but says something
        // the protocol does not allow at that moment. V5:4.13.1.
        case MqttError::Truncated:
        case MqttError::TrailingBytes:
        case MqttError::VbiTooLong:
        case MqttError::VbiNotCanonical:
        case MqttError::InvalidUtf8:
        case MqttError::NullCharacter:
        case MqttError::Surrogate:
        case MqttError::PropertyNotAllowed:
            return MQ_REASON_MALFORMED_PACKET;

        // Parsed, but says something the protocol does not allow.
        case MqttError::PropertyRepeated:
        case MqttError::PropertyValueInvalid:
            return MQ_REASON_PROTOCOL_ERROR;

        // Ours, not theirs. Telling a peer our buffer was too small would be
        // reporting our own defect as their misbehaviour.
        // 0x97 says "an implementation or administrative imposed limit has
        // been exceeded", which is precisely and only what this is. A peer
        // that sent a perfectly conforming SUBSCRIBE with more filters than
        // this build holds used to be told 0x80 Unspecified error - a reason
        // that points nowhere, for a packet the comment above this enum
        // already describes as "the peer did nothing wrong".
        case MqttError::TooManyProperties:
            return MQ_REASON_QUOTA_EXCEEDED;

        case MqttError::Overflow:
        case MqttError::BadArgument:
        case MqttError::NotSupportedByProtocol:
            return MQ_REASON_UNSPECIFIED_ERROR;
    }
    return MQ_REASON_UNSPECIFIED_ERROR;
}

bool mq_spanEquals(const MqttSpan& span, const char* text) {
    if (!text) return span.data == nullptr || span.length == 0;
    const size_t length = strlen(text);
    if (length != (size_t)span.length) return false;
    if (length == 0) return true;
    return span.data && memcmp(span.data, text, length) == 0;
}

size_t mq_cstringSize(const char* text) {
    return (size_t)2 + (text ? strlen(text) : 0);
}

// =============================================================================
// VARIABLE BYTE INTEGER
// =============================================================================

uint8_t mq_vbiSize(uint32_t value) {
    if (value <= 127u) return 1;
    if (value <= 16383u) return 2;
    if (value <= 2097151u) return 3;
    if (value <= MQ_VBI_MAX_VALUE) return 4;
    return 0;
}

uint8_t mq_encodeVbi(uint8_t* out, size_t capacity, uint32_t value) {
    const uint8_t needed = mq_vbiSize(value);
    if (needed == 0 || !out || capacity < needed) return 0;

    // The algorithm from V5:1.5.5, which produces the minimum encoding by
    // construction - so nothing this library emits can fail the canonical
    // check that mq_decodeVbi applies to what it receives.
    uint8_t written = 0;
    uint32_t remaining = value;
    do {
        uint8_t encoded = (uint8_t)(remaining % 128u);
        remaining /= 128u;
        if (remaining > 0) encoded |= 0x80u;
        out[written++] = encoded;
    } while (remaining > 0);

    return written;
}

MqttError mq_decodeVbi(const uint8_t* in, size_t available,
                       uint32_t& value, uint8_t& consumed) {
    value = 0;
    consumed = 0;
    if (!in) return MqttError::Truncated;

    uint32_t multiplier = 1;
    uint32_t result = 0;

    for (uint8_t index = 0; index < MQ_VBI_MAX_BYTES; index++) {
        if (index >= available) return MqttError::Truncated;

        const uint8_t byte = in[index];
        result += (uint32_t)(byte & 0x7Fu) * multiplier;
        multiplier *= 128u;

        if ((byte & 0x80u) == 0) {
            // [V5:MQTT-1.5.5-1] The encoded value MUST use the minimum number
            // of bytes necessary. Checked by re-deriving the length rather
            // than by inspecting the bytes, so the two can never disagree.
            //
            // Without this, 0x81 0x00 decodes to 1 exactly as 0x01 does, and
            // one packet has more than one byte representation.
            if (mq_vbiSize(result) != (uint8_t)(index + 1)) {
                return MqttError::VbiNotCanonical;
            }
            value = result;
            consumed = (uint8_t)(index + 1);
            return MqttError::None;
        }
    }

    // Four bytes all carrying a continuation bit. A fifth is not permitted,
    // and treating it as an enormous number rather than an error is how a
    // decoder ends up trusting a length it should have refused.
    return MqttError::VbiTooLong;
}

// =============================================================================
// UTF-8
// =============================================================================

namespace {

// How many bytes the sequence starting with this one claims, or 0 if it is not
// a legal lead byte. 0xC0 and 0xC1 can only ever begin an overlong two-byte
// form, and 0xF5..0xFF would encode above U+10FFFF, so both are refused here
// rather than later.
inline uint8_t sequenceLength(uint8_t lead) {
    if (lead < 0x80u) return 1;
    if (lead < 0xC2u) return 0;      // continuation byte, or an overlong lead
    if (lead < 0xE0u) return 2;
    if (lead < 0xF0u) return 3;
    if (lead < 0xF5u) return 4;
    return 0;
}

inline bool isContinuation(uint8_t byte) {
    return (byte & 0xC0u) == 0x80u;
}

// Decodes one sequence. `length` is what sequenceLength said, already checked
// against the buffer.
inline uint32_t codePointOf(const uint8_t* data, uint8_t length) {
    switch (length) {
        case 1: return data[0];
        case 2: return ((uint32_t)(data[0] & 0x1Fu) << 6) |
                        (uint32_t)(data[1] & 0x3Fu);
        case 3: return ((uint32_t)(data[0] & 0x0Fu) << 12) |
                       ((uint32_t)(data[1] & 0x3Fu) << 6) |
                        (uint32_t)(data[2] & 0x3Fu);
        default: return ((uint32_t)(data[0] & 0x07u) << 18) |
                        ((uint32_t)(data[1] & 0x3Fu) << 12) |
                        ((uint32_t)(data[2] & 0x3Fu) << 6) |
                         (uint32_t)(data[3] & 0x3Fu);
    }
}

// The smallest code point this many bytes is allowed to encode. An encoding
// longer than a value needs is ill-formed UTF-8, and historically it is how
// a "/" gets past a filter that only looks for the one-byte form.
inline uint32_t minimumFor(uint8_t length) {
    switch (length) {
        case 1: return 0x00000u;
        case 2: return 0x00080u;
        case 3: return 0x00800u;
        default: return 0x10000u;
    }
}

} // namespace

MqttError mq_validateUtf8(const uint8_t* data, size_t length) {
    if (length == 0) return MqttError::None;
    if (!data) return MqttError::BadArgument;

    size_t index = 0;
    while (index < length) {
        const uint8_t needed = sequenceLength(data[index]);
        if (needed == 0) return MqttError::InvalidUtf8;
        if (index + needed > length) return MqttError::InvalidUtf8;

        for (uint8_t offset = 1; offset < needed; offset++) {
            if (!isContinuation(data[index + offset])) return MqttError::InvalidUtf8;
        }

        const uint32_t codePoint = codePointOf(data + index, needed);

        if (codePoint < minimumFor(needed)) return MqttError::InvalidUtf8;
        if (codePoint > 0x10FFFFu) return MqttError::InvalidUtf8;

        // [V5:MQTT-1.5.4-1] The character data MUST NOT include encodings of
        // code points between U+D800 and U+DFFF. These are the UTF-16
        // surrogate halves and have no meaning on their own.
        if (codePoint >= 0xD800u && codePoint <= 0xDFFFu) return MqttError::Surrogate;

        // [V5:MQTT-1.5.4-2] A UTF-8 Encoded String MUST NOT include an
        // encoding of the null character U+0000. The length prefix already
        // delimits the string, so a zero byte is not a terminator here - it is
        // a character the standard forbids.
        if (codePoint == 0) return MqttError::NullCharacter;

        index += needed;
    }
    return MqttError::None;
}

bool mq_utf8HasDisallowed(const uint8_t* data, size_t length) {
    if (!data || length == 0) return false;

    size_t index = 0;
    while (index < length) {
        const uint8_t needed = sequenceLength(data[index]);
        if (needed == 0 || index + needed > length) return false;   // ill-formed; not our question
        const uint32_t codePoint = codePointOf(data + index, needed);

        // V5:1.5.4 - the two control ranges.
        if (codePoint <= 0x1Fu) return true;
        if (codePoint >= 0x7Fu && codePoint <= 0x9Fu) return true;

        // The Unicode non-characters: U+FDD0..U+FDEF, and the last two code
        // points of every plane.
        if (codePoint >= 0xFDD0u && codePoint <= 0xFDEFu) return true;
        if ((codePoint & 0xFFFEu) == 0xFFFEu) return true;

        index += needed;
    }
    return false;
}

// =============================================================================
// READER
// =============================================================================

const uint8_t* MqttReader::take(size_t count) {
    if (!ok()) return nullptr;
    if (count > remaining()) {
        _error = MqttError::Truncated;
        return nullptr;
    }
    const uint8_t* at = _data + _cursor;
    _cursor += count;
    return at;
}

uint8_t MqttReader::u8() {
    const uint8_t* at = take(1);
    return at ? at[0] : 0;
}

uint16_t MqttReader::u16() {
    const uint8_t* at = take(2);
    return at ? mq_readU16(at) : 0;
}

uint32_t MqttReader::u32() {
    const uint8_t* at = take(4);
    return at ? mq_readU32(at) : 0;
}

uint32_t MqttReader::vbi() {
    if (!ok()) return 0;

    uint32_t value = 0;
    uint8_t consumed = 0;
    const MqttError result = mq_decodeVbi(_data + _cursor, remaining(),
                                          value, consumed);
    if (result != MqttError::None) {
        _error = result;
        return 0;
    }
    _cursor += consumed;
    return value;
}

MqttSpan MqttReader::bytes(size_t count) {
    MqttSpan span;

    // Bounded by what a packet can hold, not by what a length prefix can
    // express. rest() reaches here with a whole PUBLISH payload, which has no
    // prefix and may be far larger than 65,535.
    if (count > MQ_VBI_MAX_VALUE) {
        fail(MqttError::Truncated);
        return span;
    }
    const uint8_t* at = take(count);
    if (!at) return span;
    span.data = at;
    span.length = (uint32_t)count;
    return span;
}

MqttSpan MqttReader::utf8() {
    MqttSpan span;
    if (!ok()) return span;

    const uint16_t length = u16();
    if (!ok()) return span;

    span = bytes(length);
    if (!ok()) return span;

    // Validated on the way in, every time, rather than where it is used. A
    // string that reaches the application unvalidated is one the application
    // has to remember to check, and it will not.
    const MqttError result = mq_validateUtf8(span.data, span.length);
    if (result != MqttError::None) {
        _error = result;
        span = MqttSpan();
    }
    return span;
}

MqttSpan MqttReader::binary() {
    if (!ok()) return MqttSpan();
    const uint16_t length = u16();
    if (!ok()) return MqttSpan();
    return bytes(length);
}

MqttSpan MqttReader::rest() {
    return bytes(remaining());
}

// =============================================================================
// WRITER
// =============================================================================

uint8_t* MqttWriter::take(size_t count) {
    if (!ok()) return nullptr;
    if (count > room()) {
        // Fails BEFORE anything is written, so a packet is never half-built.
        // A writer that wrote what fitted and then reported failure would
        // leave a buffer that looks like a short packet.
        _error = MqttError::Overflow;
        return nullptr;
    }
    uint8_t* at = _data + _cursor;
    _cursor += count;
    return at;
}

void MqttWriter::u8(uint8_t value) {
    uint8_t* at = take(1);
    if (at) at[0] = value;
}

void MqttWriter::u16(uint16_t value) {
    uint8_t* at = take(2);
    if (at) mq_writeU16(at, value);
}

void MqttWriter::u32(uint32_t value) {
    uint8_t* at = take(4);
    if (at) mq_writeU32(at, value);
}

void MqttWriter::vbi(uint32_t value) {
    if (!ok()) return;

    const uint8_t needed = mq_vbiSize(value);
    if (needed == 0) {
        _error = MqttError::Overflow;   // above 268,435,455; nothing can carry it
        return;
    }
    uint8_t* at = take(needed);
    if (at) mq_encodeVbi(at, needed, value);
}

void MqttWriter::raw(const uint8_t* data, size_t length) {
    if (length == 0) return;
    if (!data) {
        fail(MqttError::BadArgument);
        return;
    }
    uint8_t* at = take(length);
    if (at) memcpy(at, data, length);
}

void MqttWriter::utf8(const uint8_t* data, uint32_t length) {
    // The two-byte prefix cannot express more. Failing is the only honest
    // answer: truncating would put a shorter string on the wire than the
    // caller asked for, and say nothing.
    if (length > MQ_STRING_MAX) {
        fail(MqttError::Overflow);
        return;
    }
    u16((uint16_t)length);
    raw(data, length);
}

void MqttWriter::utf8(const char* text) {
    const size_t length = text ? strlen(text) : 0;
    if (length > MQ_STRING_MAX) {
        fail(MqttError::Overflow);
        return;
    }
    utf8((const uint8_t*)text, (uint32_t)length);
}

void MqttWriter::binary(const uint8_t* data, uint32_t length) {
    if (length > MQ_BINARY_MAX) {
        fail(MqttError::Overflow);
        return;
    }
    u16((uint16_t)length);
    raw(data, length);
}

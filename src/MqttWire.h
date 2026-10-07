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
// WIRE PRIMITIVES
// =============================================================================
//
// The five data representations both versions share, and the two cursors that
// read and write them. V5:1.5, V311:1.5.
//
// No I/O, no clock, no allocation, no state beyond a cursor. That is what
// makes this layer exhaustively testable against the standards' own worked
// byte examples, and fuzzable on its own.
//
// THE GUARANTEE, AND IT IS FUZZED:
//
//   A READER NEVER READS OUTSIDE ITS BUFFER AND A WRITER NEVER WRITES PAST ITS
//   CAPACITY, WHATEVER THE BYTES SAY.
//
// MQTT hands a decoder four independently declared lengths - the Remaining
// Length, the Property Length, each two-byte string or binary prefix, and
// whatever payload is left over. Every one of them is a number the peer chose.
// Deriving any one of them from another, or trusting one as a bound on the
// buffer, is how a parser reads someone else's memory.

#include "MqttTypes.h"

// --- What went wrong ---
//
// Split by cause rather than by consequence, because the consequence is nearly
// always "close the connection" and that tells a person nothing. Which of
// these it was is the difference between a peer with a bug, a peer on a
// different version, and a corrupted stream.
enum class MqttError : uint8_t {
    None = 0,

    // Ran out of bytes partway through a field. The commonest decode fault and
    // the one a split packet produces if the reassembly above is wrong.
    Truncated,

    // Bytes left over after the packet's last field. V5 and V311 both parse
    // strictly in both directions: a trailing byte means the two ends disagree
    // about the layout, and guessing is worse than refusing.
    TrailingBytes,

    // A fifth continuation byte. A Variable Byte Integer is at most four.
    VbiTooLong,

    // A value encoded in more bytes than it needs - 0x81 0x00 for 1.
    // [V5:MQTT-1.5.5-1] Refused rather than accepted, because otherwise one
    // packet has several byte encodings: anything that compares or forwards
    // encoded bytes breaks, and a proxy that parses differently from the
    // endpoint becomes a smuggling route.
    VbiNotCanonical,

    // Ill-formed UTF-8. [V5:MQTT-1.5.4-1]
    InvalidUtf8,

    // An encoded U+0000. [V5:MQTT-1.5.4-2]
    NullCharacter,

    // An encoded surrogate, U+D800 to U+DFFF. [V5:MQTT-1.5.4-1]
    Surrogate,

    // The writer was asked for more room than it has. Never a peer's fault -
    // always ours, and always caught before a byte is written.
    Overflow,

    // A field the caller must supply was null with a non-zero length.
    BadArgument,

    // --- Properties, 5.0 only ---
    //
    // Split from the faults above because the standard splits them, and
    // because the reason code that follows differs. V5:4.13.1.

    // A property identifier that is not valid for the packet carrying it.
    // V5:2.2.2.2 makes this a Malformed Packet, not an unknown extension:
    // "A Control Packet which contains an Identifier which is not valid for
    // its packet type ... is a Malformed Packet."
    PropertyNotAllowed,

    // A property repeated where the standard permits it once. A Protocol
    // Error rather than a Malformed Packet: the bytes parsed, and what they
    // said is something the protocol does not allow.
    PropertyRepeated,

    // A property whose value is outside the range its section defines - a
    // Receive Maximum of 0, a Maximum QoS of 2, a Subscription Identifier of
    // 0. Also a Protocol Error.
    PropertyValueInvalid,

    // More User Properties or Subscription Identifiers than this build has
    // room for. Ours, not theirs: the peer did nothing wrong.
    TooManyProperties,

    // The packet asks for something this protocol version cannot express - a
    // topic alias, a message expiry interval, a user property, an AUTH packet.
    //
    // Never a decode fault: it is only ever raised while ENCODING, and it is
    // the codec refusing to send a lie. The alternative is to drop what does
    // not fit and send the rest, which is what most libraries do and which
    // means a setpoint's expiry interval silently vanishes on the way to an
    // older broker. The caller is told instead, and decides.
    NotSupportedByProtocol,
};

const char* mq_errorName(MqttError error);

// The reason code to put in the DISCONNECT that follows. V5:4.13.1.
//
// The Malformed Packet / Protocol Error distinction is the standard's own and
// is worth keeping: a Malformed Packet means the bytes could not be parsed, a
// Protocol Error means they parsed and said something not allowed. A peer
// receiving the wrong one of those looks in the wrong place.
uint8_t mq_errorToReason(MqttError error);

// --- A view of bytes we do not own ---
//
// Valid only as long as the buffer it points into. Every decode hands these
// out rather than copying, because a client that publishes a 200 kB payload
// should not see it copied twice on the way past.
struct MqttSpan {
    const uint8_t* data = nullptr;

    // 32 bits, not 16, and the difference matters exactly once - but that once
    // is the common case.
    //
    // Every LENGTH-PREFIXED field is bounded at 65,535 by its own two-byte
    // prefix: topic names, user properties, binary data. A PUBLISH PAYLOAD IS
    // NOT ONE OF THEM. It has no length field at all - it is whatever the
    // Remaining Length leaves over, so the standard permits it up to
    // 268,435,455 bytes.
    //
    // A 16-bit length here silently capped every payload at 64 kB. It passed
    // every test built on a 60 kB message and would have refused a 100 kB one
    // with InvalidArgument, which reads as the caller's mistake. On a 64-bit
    // host the wider field costs nothing at all: the struct was already padded
    // to sixteen bytes.
    uint32_t length = 0;

    bool empty() const { return length == 0; }
};

// Compares a span against a C string. For the handful of places a fixed value
// has to be recognised - the protocol name, an authentication method.
bool mq_spanEquals(const MqttSpan& span, const char* text);

// --- Variable Byte Integer ---
//
// V5:1.5.5, V311:1.5.2. One to four bytes, seven data bits each, top bit set
// while more follow.

// Bytes this value occupies once encoded. 1 to 4, or 0 if it is out of range.
uint8_t mq_vbiSize(uint32_t value);

// Writes value into `out`, which must have mq_vbiSize(value) bytes. Returns
// the bytes written, or 0 if the value cannot be encoded.
uint8_t mq_encodeVbi(uint8_t* out, size_t capacity, uint32_t value);

// Reads a Variable Byte Integer.
//
// `consumed` receives the bytes read on success. On failure the error says
// which of the two rules was broken: too long, or not the minimum encoding.
MqttError mq_decodeVbi(const uint8_t* in, size_t available,
                       uint32_t& value, uint8_t& consumed);

// --- UTF-8 ---
//
// Validation is a real decode, not a byte scan. The surrogate and U+0000 rules
// cannot be enforced without working out what each sequence encodes, and a
// library that only checked the length would pass both of the things the
// standard singles out.
MqttError mq_validateUtf8(const uint8_t* data, size_t length);

// Whether the text contains code points V5:1.5.4 says SHOULD NOT appear - the
// two control ranges and the Unicode non-characters.
//
// Reported rather than refused. The standard says a receiver MAY treat these
// as malformed, and refusing by default would break against brokers and
// devices that pass them through. A caller that wants to be strict asks.
bool mq_utf8HasDisallowed(const uint8_t* data, size_t length);

// --- Reading ---
//
// Every accessor checks the buffer first. Once a reader has failed it stays
// failed and every later accessor returns a zero or an empty span, so a decode
// can be written as a straight run of reads with one check at the end rather
// than a check after each one. That shape is why the checks do not get missed.
class MqttReader {
public:
    MqttReader(const uint8_t* data, size_t length)
        : _data(data), _length(data ? length : 0) {}

    bool ok() const { return _error == MqttError::None; }
    MqttError error() const { return _error; }
    size_t consumed() const { return _cursor; }
    size_t remaining() const { return _cursor < _length ? _length - _cursor : 0; }
    bool atEnd() const { return remaining() == 0; }

    // Marks the reader failed. Used where a field is syntactically fine and
    // semantically wrong - a QoS of 3, a reason code not valid for its packet.
    void fail(MqttError error) {
        if (_error == MqttError::None) _error = error;
    }

    // Refuses anything left over. V5 and V311 both define every packet's
    // layout exactly, so a decode that leaves bytes has misread something.
    void expectEnd() {
        if (ok() && !atEnd()) _error = MqttError::TrailingBytes;
    }

    uint8_t u8();
    uint16_t u16();
    uint32_t u32();
    uint32_t vbi();

    // A UTF-8 Encoded String: two-byte length, then that many validated bytes.
    // Bounded at 65,535 by the prefix itself.
    MqttSpan utf8();

    // Binary Data: two-byte length, then that many bytes, not validated.
    MqttSpan binary();

    // Exactly `count` raw bytes.
    MqttSpan bytes(size_t count);

    // Everything that is left, which is how a PUBLISH payload is taken: its
    // length is what the Remaining Length leaves over rather than a field.
    MqttSpan rest();

private:
    const uint8_t* _data;
    size_t _length;
    size_t _cursor = 0;
    MqttError _error = MqttError::None;

    // The single bounds check every accessor goes through. Returns null and
    // fails the reader rather than returning a pointer the caller must check,
    // because a pointer the caller must check is a pointer some caller will not.
    const uint8_t* take(size_t count);
};

// --- Writing ---
//
// A writer never writes past capacity. Asked for more room than it has it
// fails, writes nothing, and stays failed - so a half-built packet is never
// mistaken for a whole one.
//
// Encoding is always sized first and written second: mq_*Size() computes the
// length, the caller checks it fits, and only then does anything get written.
// A writer that grew its own buffer would make that discipline optional.
class MqttWriter {
public:
    MqttWriter(uint8_t* data, size_t capacity)
        : _data(data), _capacity(data ? capacity : 0) {}

    bool ok() const { return _error == MqttError::None; }
    MqttError error() const { return _error; }
    size_t written() const { return _cursor; }
    size_t capacity() const { return _capacity; }
    size_t room() const { return _cursor < _capacity ? _capacity - _cursor : 0; }

    void fail(MqttError error) {
        if (_error == MqttError::None) _error = error;
    }

    void u8(uint8_t value);
    void u16(uint16_t value);
    void u32(uint32_t value);
    void vbi(uint32_t value);

    // Length-prefixed. The length is written from `length`, never recomputed
    // from the data, so a caller cannot pass one and mean the other.
    // Length-prefixed, so bounded at 65,535 - the prefix cannot say more.
    // A length above that fails the writer rather than truncating.
    void utf8(const uint8_t* data, uint32_t length);
    void utf8(const char* text);
    void binary(const uint8_t* data, uint32_t length);

    void raw(const uint8_t* data, size_t length);

private:
    uint8_t* _data;
    size_t _capacity;
    size_t _cursor = 0;
    MqttError _error = MqttError::None;

    uint8_t* take(size_t count);
};

// --- Encoded sizes ---
//
// Everything a packet encoder needs to work out how long a packet will be
// before it writes a byte of it.
inline size_t mq_utf8Size(uint32_t length) { return (size_t)2 + length; }
inline size_t mq_binarySize(uint32_t length) { return (size_t)2 + length; }
size_t mq_cstringSize(const char* text);

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// ONE METRIC VALUE
// =============================================================================
//
// The same shape as MqttValue in the typed layer, and for the same reason: a
// value plus the type it is, so nothing downstream has to guess. The difference
// is that Sparkplug FIXES the type set - MqttValueType is this library's own
// choice, and MqttSparkplugDataType is the specification's.
//
// The two line up almost exactly, which is worth knowing before writing a
// conversion:
//
//   MqttValueType   IEC            MqttSparkplugDataType
//   Bool            BOOL           Boolean   (11)
//   Int8            SINT           Int8      (1)
//   UInt8           USINT/BYTE     UInt8     (5)
//   Int16           INT            Int16     (2)
//   UInt16          UINT/WORD      UInt16    (6)
//   Int32           DINT           Int32     (3)
//   UInt32          UDINT/DWORD    UInt32    (7)
//   Int64           LINT           Int64     (4)
//   UInt64          ULINT/LWORD    UInt64    (8)
//   Float32         REAL           Float     (9)
//   Float64         LREAL          Double    (10)
//   Text            STRING         String    (12)
//
// All twelve of the typed layer's kinds have a Sparkplug datatype, so a
// SPARKPLUG_STRUCT block can bind any STRUCT the MQTT_STRUCT_PUBLISH block
// could. Sparkplug adds what the typed layer has no entry for - DateTime,
// Text, UUID, Bytes/File, the arrays, DataSet and Template - and those are
// reachable from here directly: an ARRAY OF REAL is a FloatArray
// (mq_spPackArray), a STRUCT a Template (MqttSparkplugTemplate).
//
// THE STORAGE IS A UNION AND THE REASON IS THE BOARD. A metric is 24 bytes here
// against about 40 for a struct of every field, and a payload holds up to
// MQ_SP_MAX_METRICS of them. On an ESP8266 with 80 kB that difference is the
// whole of the headroom.

#include "MqttSparkplugTypes.h"

// For the MqttValueType conversion at the bottom. The dependency runs this way
// round on purpose: Sparkplug is the later layer and may know about the typed
// one, while the typed layer must never know about Sparkplug.
#include "../typed/MqttValue.h"

// Text and bytes are NOT copied. A value points at whatever the caller owns -
// a std::string in a metric registry, a span into a decode buffer - exactly as
// MqttSpan does, and with the same obligation: the bytes must outlive the
// value. Copying here would put an allocator on the hot path of a codec whose
// whole point is not having one.
struct MqttSparkplugValue {
    MqttSparkplugDataType type = MqttSparkplugDataType::Unknown;

    // [tck-id-payloads-metric-...] is_null is explicit in the schema rather
    // than a sentinel value, so a null Int32 is distinguishable from zero. That
    // distinction is the difference between "the sensor is unplugged" and "the
    // tank is empty", and every SCADA system that conflated them has a story.
    bool isNull = false;

    union {
        // Every signed width lands here and is sign-extended on the way out;
        // see the int_value discussion in MqttSparkplugPayload.h.
        int64_t signedValue;
        uint64_t unsignedValue;
        float float32;
        double float64;
        bool boolean;
        struct {
            const uint8_t* data;
            uint32_t length;
        } bytes;
    } as;

    MqttSparkplugValue() { as.unsignedValue = 0; }

    // --- Making one ---
    //
    // Free functions rather than constructors, because `MqttSparkplugValue v =
    // mq_spReal(21.5f)` reads at the call site and an overloaded constructor
    // taking int64_t would silently swallow a float.

    bool isNumeric() const {
        switch (type) {
            case MqttSparkplugDataType::Int8:
            case MqttSparkplugDataType::Int16:
            case MqttSparkplugDataType::Int32:
            case MqttSparkplugDataType::Int64:
            case MqttSparkplugDataType::UInt8:
            case MqttSparkplugDataType::UInt16:
            case MqttSparkplugDataType::UInt32:
            case MqttSparkplugDataType::UInt64:
            case MqttSparkplugDataType::Float:
            case MqttSparkplugDataType::Double:
            case MqttSparkplugDataType::DateTime:
                return true;
            default:
                return false;
        }
    }

    bool isText() const {
        return type == MqttSparkplugDataType::String ||
               type == MqttSparkplugDataType::Text ||
               type == MqttSparkplugDataType::UUID;
    }

    bool isBytes() const {
        return type == MqttSparkplugDataType::Bytes ||
               type == MqttSparkplugDataType::File;
    }

    // Int8Array .. DateTimeArray: the packed bytes, in `as.bytes`.
    bool isArray() const { return mq_sparkplugIsArrayType(type); }

    // DataSet and Template: the nested message's encoded bytes, in `as.bytes`.
    bool isStructured() const {
        return type == MqttSparkplugDataType::DataSet ||
               type == MqttSparkplugDataType::Template;
    }

    // Held as a span in `as.bytes` - text, bytes, an array, a dataset or a
    // template - rather than as a number. What a holder of a value past its
    // message must copy (MqttSparkplugBytes), and what compares by content.
    bool isSpan() const {
        return isText() || isBytes() || isArray() || isStructured();
    }

    // One number, whatever the declared type, for the change detection a
    // report-by-exception layer needs. A BOOL is 1.0 or 0.0; text and bytes are
    // 0.0 and are compared by content instead.
    double asNumber() const {
        if (isNull) return 0.0;
        switch (type) {
            case MqttSparkplugDataType::Boolean:  return as.boolean ? 1.0 : 0.0;
            case MqttSparkplugDataType::Float:    return (double)as.float32;
            case MqttSparkplugDataType::Double:   return as.float64;
            case MqttSparkplugDataType::Int8:
            case MqttSparkplugDataType::Int16:
            case MqttSparkplugDataType::Int32:
            case MqttSparkplugDataType::Int64:    return (double)as.signedValue;
            case MqttSparkplugDataType::UInt8:
            case MqttSparkplugDataType::UInt16:
            case MqttSparkplugDataType::UInt32:
            case MqttSparkplugDataType::UInt64:
            case MqttSparkplugDataType::DateTime: return (double)as.unsignedValue;
            default:                              return 0.0;
        }
    }

    // Whether two values of the same metric differ. Used by publishChanged(),
    // and it compares BYTES for text rather than pointers - two spans into
    // different buffers holding the same string have not changed.
    bool differsFrom(const MqttSparkplugValue& other) const;
};

// --- Constructors, by IEC name where there is one ---

MqttSparkplugValue mq_spBool(bool value);
MqttSparkplugValue mq_spInt8(int8_t value);
MqttSparkplugValue mq_spInt16(int16_t value);
MqttSparkplugValue mq_spInt32(int32_t value);
MqttSparkplugValue mq_spInt64(int64_t value);
MqttSparkplugValue mq_spUInt8(uint8_t value);
MqttSparkplugValue mq_spUInt16(uint16_t value);
MqttSparkplugValue mq_spUInt32(uint32_t value);
MqttSparkplugValue mq_spUInt64(uint64_t value);
MqttSparkplugValue mq_spFloat(float value);
MqttSparkplugValue mq_spDouble(double value);

// UTF-8, not copied. [tck-id-payloads-metric-...] String and Text are the same
// bytes with different intent: String is a value, Text is prose.
MqttSparkplugValue mq_spString(const char* text);
MqttSparkplugValue mq_spText(const char* text);
MqttSparkplugValue mq_spUuid(const char* text);

// Bytes and File are the same wire field; File carries a name in MetaData.
MqttSparkplugValue mq_spBytes(const uint8_t* data, uint32_t length);
MqttSparkplugValue mq_spFile(const uint8_t* data, uint32_t length);

// An array, from bytes already packed as section 6.4.17 packs them (see
// mq_spPackArray). Not copied.
MqttSparkplugValue mq_spArray(MqttSparkplugDataType arrayType,
                              const uint8_t* packed, uint32_t length);

// A Template or DataSet, from its encoded message (mq_sparkplugEncodeTemplate,
// mq_sparkplugEncodeDataSet in MqttSparkplugPayload.h). Not copied.
MqttSparkplugValue mq_spTemplate(const uint8_t* encoded, uint32_t length);
MqttSparkplugValue mq_spDataSet(const uint8_t* encoded, uint32_t length);

// Milliseconds since the Unix epoch, UTC. Section 6.4.17: "Date time value as
// uint64 value representing milliseconds since epoch (Jan 1, 1970)".
MqttSparkplugValue mq_spDateTime(uint64_t millisecondsSinceEpoch);

// A null of a declared type. The type still travels, because a Host
// Application needs to know what the metric IS in order to show it greyed out
// rather than absent.
MqttSparkplugValue mq_spNull(MqttSparkplugDataType type);

// --- Arrays ---
//
// Packed as section 6.4.17 packs them, little-endian byte by byte whatever the
// board, so the same bytes go out from an ESP32 and an x86 host.
//
// FROM A PROGRAM'S OWN ARRAY. `elements` is `count` values laid out as C lays
// out the element type - int8_t .. uint64_t, float, double; `bool` for a
// BooleanArray; `const char*` (NUL-ended) for a StringArray; uint64_t
// milliseconds for a DateTimeArray - which is how a PLC holds ARRAY OF INT or
// ARRAY OF REAL. Returns the packed length; writes only when it fits in
// `capacity`, so a null `out` asks the size. A StringArray with a null string
// packs it as empty.
size_t mq_spPackArray(MqttSparkplugDataType arrayType, const void* elements,
                      size_t count, uint8_t* out, size_t capacity);

// How many elements a packed array holds. False when the bytes are not a
// well-formed array of that type: a length that is not a multiple of the
// width, a BooleanArray with fewer bits than its count, a StringArray whose
// last string has no NUL.
bool mq_spArrayCount(const MqttSparkplugValue& array, size_t& count);

// One element, as a scalar value of the element's type: Int16 for an
// Int16Array. A StringArray's element is a span into the array. False past
// the end or for a malformed array.
bool mq_spArrayAt(const MqttSparkplugValue& array, size_t index,
                  MqttSparkplugValue& element);

// --- A value's bytes, owned ---
//
// A span value - text, bytes, an array, a dataset, a template - points into
// someone's buffer, and a receive buffer is reused for the next message. What
// keeps a value past its message (a node's last-published copy and its
// commands, a host's current picture) copies it here. Grows to the largest it
// has held and never shrinks, in PSRAM where the board has some; it was a
// 127-character text field on a board, which cut every longer value.
class MqttSparkplugBytes {
public:
    MqttSparkplugBytes() noexcept = default;
    ~MqttSparkplugBytes() noexcept;
    MqttSparkplugBytes(const MqttSparkplugBytes&) = delete;
    MqttSparkplugBytes& operator=(const MqttSparkplugBytes&) = delete;
    // Moved, so a table of them can grow: the bytes stay where they are.
    MqttSparkplugBytes(MqttSparkplugBytes&& other) noexcept;
    MqttSparkplugBytes& operator=(MqttSparkplugBytes&& other) noexcept;

    // A copy of `length` bytes, which may be inside this one. False, and
    // empty, when the memory is not there.
    bool assign(const uint8_t* data, size_t length) noexcept;
    void clear() noexcept { _size = 0; }

    const uint8_t* data() const noexcept { return _data; }
    size_t size() const noexcept { return _size; }

    // `value` with its span pointed here, after assign()ing its bytes. A null
    // value, or a number, is returned as it is.
    MqttSparkplugValue keep(const MqttSparkplugValue& value) noexcept;

private:
    uint8_t* _data = nullptr;
    uint32_t _size = 0;
    uint32_t _capacity = 0;
};

// --- Converting to and from the typed layer ---
//
// Declared here rather than in the typed layer, because the dependency runs
// this way: Sparkplug is a later layer and may know about MqttValue, while
// MqttValue must not know about Sparkplug.

// True if this MqttValueType has a Sparkplug datatype. Always true today - all
// twelve map - and checked rather than asserted because the typed layer may
// grow an entry before this file does.
bool mq_sparkplugTypeForValueType(MqttValueType type,
                                  MqttSparkplugDataType& out);

// The reverse. False for DateTime, UUID, Bytes and File, which the typed layer
// has no entry for.
bool mq_valueTypeForSparkplugType(MqttSparkplugDataType type,
                                  MqttValueType& out);

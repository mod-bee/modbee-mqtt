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
// could. Sparkplug adds four the typed layer has no entry for - DateTime, Text,
// UUID and Bytes/File - and those are reachable from here directly.
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

// Milliseconds since the Unix epoch, UTC. Section 6.4.17: "Date time value as
// uint64 value representing milliseconds since epoch (Jan 1, 1970)".
MqttSparkplugValue mq_spDateTime(uint64_t millisecondsSinceEpoch);

// A null of a declared type. The type still travels, because a Host
// Application needs to know what the metric IS in order to show it greyed out
// rather than absent.
MqttSparkplugValue mq_spNull(MqttSparkplugDataType type);

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

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugValue.h"

#include <string.h>

namespace {

MqttSparkplugValue signedOf(MqttSparkplugDataType type, int64_t value) {
    MqttSparkplugValue out;
    out.type = type;
    out.as.signedValue = value;
    return out;
}

MqttSparkplugValue unsignedOf(MqttSparkplugDataType type, uint64_t value) {
    MqttSparkplugValue out;
    out.type = type;
    out.as.unsignedValue = value;
    return out;
}

MqttSparkplugValue textOf(MqttSparkplugDataType type, const char* text) {
    MqttSparkplugValue out;
    out.type = type;
    out.as.bytes.data = (const uint8_t*)text;
    out.as.bytes.length = text ? (uint32_t)strlen(text) : 0;
    // A null pointer is a null metric, not an empty string. An empty string is
    // a value somebody chose; a null pointer is the absence of one.
    out.isNull = (text == nullptr);
    return out;
}

MqttSparkplugValue bytesOf(MqttSparkplugDataType type,
                           const uint8_t* data, uint32_t length) {
    MqttSparkplugValue out;
    out.type = type;
    out.as.bytes.data = data;
    out.as.bytes.length = data ? length : 0;
    out.isNull = (data == nullptr);
    return out;
}

}  // namespace

MqttSparkplugValue mq_spBool(bool value) {
    MqttSparkplugValue out;
    out.type = MqttSparkplugDataType::Boolean;
    out.as.boolean = value;
    return out;
}

MqttSparkplugValue mq_spInt8(int8_t value) {
    return signedOf(MqttSparkplugDataType::Int8, (int64_t)value);
}
MqttSparkplugValue mq_spInt16(int16_t value) {
    return signedOf(MqttSparkplugDataType::Int16, (int64_t)value);
}
MqttSparkplugValue mq_spInt32(int32_t value) {
    return signedOf(MqttSparkplugDataType::Int32, (int64_t)value);
}
MqttSparkplugValue mq_spInt64(int64_t value) {
    return signedOf(MqttSparkplugDataType::Int64, value);
}

MqttSparkplugValue mq_spUInt8(uint8_t value) {
    return unsignedOf(MqttSparkplugDataType::UInt8, (uint64_t)value);
}
MqttSparkplugValue mq_spUInt16(uint16_t value) {
    return unsignedOf(MqttSparkplugDataType::UInt16, (uint64_t)value);
}
MqttSparkplugValue mq_spUInt32(uint32_t value) {
    return unsignedOf(MqttSparkplugDataType::UInt32, (uint64_t)value);
}
MqttSparkplugValue mq_spUInt64(uint64_t value) {
    return unsignedOf(MqttSparkplugDataType::UInt64, value);
}

MqttSparkplugValue mq_spFloat(float value) {
    MqttSparkplugValue out;
    out.type = MqttSparkplugDataType::Float;
    out.as.float32 = value;
    return out;
}

MqttSparkplugValue mq_spDouble(double value) {
    MqttSparkplugValue out;
    out.type = MqttSparkplugDataType::Double;
    out.as.float64 = value;
    return out;
}

MqttSparkplugValue mq_spString(const char* text) {
    return textOf(MqttSparkplugDataType::String, text);
}
MqttSparkplugValue mq_spText(const char* text) {
    return textOf(MqttSparkplugDataType::Text, text);
}
MqttSparkplugValue mq_spUuid(const char* text) {
    return textOf(MqttSparkplugDataType::UUID, text);
}

MqttSparkplugValue mq_spBytes(const uint8_t* data, uint32_t length) {
    return bytesOf(MqttSparkplugDataType::Bytes, data, length);
}
MqttSparkplugValue mq_spFile(const uint8_t* data, uint32_t length) {
    return bytesOf(MqttSparkplugDataType::File, data, length);
}

MqttSparkplugValue mq_spDateTime(uint64_t millisecondsSinceEpoch) {
    return unsignedOf(MqttSparkplugDataType::DateTime, millisecondsSinceEpoch);
}

MqttSparkplugValue mq_spNull(MqttSparkplugDataType type) {
    MqttSparkplugValue out;
    out.type = type;
    out.isNull = true;
    return out;
}

bool MqttSparkplugValue::differsFrom(const MqttSparkplugValue& other) const {
    if (type != other.type) return true;
    if (isNull != other.isNull) return true;
    if (isNull) return false;

    if (isText() || isBytes()) {
        // By content, not by pointer. Two spans into different buffers holding
        // the same bytes have not changed, and a report-by-exception layer that
        // compared pointers would publish on every scan.
        if (as.bytes.length != other.as.bytes.length) return true;
        if (as.bytes.length == 0) return false;
        if (as.bytes.data == nullptr || other.as.bytes.data == nullptr) return true;
        return memcmp(as.bytes.data, other.as.bytes.data, as.bytes.length) != 0;
    }

    switch (type) {
        case MqttSparkplugDataType::Boolean:
            return as.boolean != other.as.boolean;
        case MqttSparkplugDataType::Float:
            // Bit comparison, not ==, so a NaN compares equal to itself and a
            // metric stuck at NaN does not republish for ever. -0.0 and 0.0
            // differ, which is correct: something wrote a different value.
            return memcmp(&as.float32, &other.as.float32, sizeof(float)) != 0;
        case MqttSparkplugDataType::Double:
            return memcmp(&as.float64, &other.as.float64, sizeof(double)) != 0;
        default:
            return as.unsignedValue != other.as.unsignedValue;
    }
}

bool mq_sparkplugTypeForValueType(MqttValueType type,
                                  MqttSparkplugDataType& out) {
    switch (type) {
        case MqttValueType::Bool:    out = MqttSparkplugDataType::Boolean; return true;
        case MqttValueType::Int8:    out = MqttSparkplugDataType::Int8;    return true;
        case MqttValueType::UInt8:   out = MqttSparkplugDataType::UInt8;   return true;
        case MqttValueType::Int16:   out = MqttSparkplugDataType::Int16;   return true;
        case MqttValueType::UInt16:  out = MqttSparkplugDataType::UInt16;  return true;
        case MqttValueType::Int32:   out = MqttSparkplugDataType::Int32;   return true;
        case MqttValueType::UInt32:  out = MqttSparkplugDataType::UInt32;  return true;
        case MqttValueType::Int64:   out = MqttSparkplugDataType::Int64;   return true;
        case MqttValueType::UInt64:  out = MqttSparkplugDataType::UInt64;  return true;
        case MqttValueType::Float32: out = MqttSparkplugDataType::Float;   return true;
        case MqttValueType::Float64: out = MqttSparkplugDataType::Double;  return true;
        case MqttValueType::Text:    out = MqttSparkplugDataType::String;  return true;
    }
    // Not reachable today - all twelve map - and here rather than omitted so
    // that a new MqttValueType is a compiler warning and a false return rather
    // than a silent Unknown on the wire.
    out = MqttSparkplugDataType::Unknown;
    return false;
}

bool mq_valueTypeForSparkplugType(MqttSparkplugDataType type,
                                  MqttValueType& out) {
    switch (type) {
        case MqttSparkplugDataType::Boolean: out = MqttValueType::Bool;    return true;
        case MqttSparkplugDataType::Int8:    out = MqttValueType::Int8;    return true;
        case MqttSparkplugDataType::UInt8:   out = MqttValueType::UInt8;   return true;
        case MqttSparkplugDataType::Int16:   out = MqttValueType::Int16;   return true;
        case MqttSparkplugDataType::UInt16:  out = MqttValueType::UInt16;  return true;
        case MqttSparkplugDataType::Int32:   out = MqttValueType::Int32;   return true;
        case MqttSparkplugDataType::UInt32:  out = MqttValueType::UInt32;  return true;
        case MqttSparkplugDataType::Int64:   out = MqttValueType::Int64;   return true;
        case MqttSparkplugDataType::UInt64:  out = MqttValueType::UInt64;  return true;
        case MqttSparkplugDataType::Float:   out = MqttValueType::Float32; return true;
        case MqttSparkplugDataType::Double:  out = MqttValueType::Float64; return true;

        // Text and String are both STRING to a PLC. UUID is too, but it is not
        // mapped: a UUID metric bound to a STRING pin would let a PLC write
        // something that is not a UUID, and the far end is entitled to assume
        // otherwise.
        case MqttSparkplugDataType::String:
        case MqttSparkplugDataType::Text:    out = MqttValueType::Text;    return true;

        default:
            return false;
    }
}

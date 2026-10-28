// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugValue.h"

#include <string.h>
#include "../MqttNoExcept.h"
#include "../MqttPlatform.h"

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

    if (isSpan()) {
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

MqttSparkplugValue mq_spArray(MqttSparkplugDataType arrayType,
                              const uint8_t* packed, uint32_t length) {
    MqttSparkplugValue out;
    out.type = arrayType;
    out.as.bytes.data = packed;
    out.as.bytes.length = length;
    // An empty array is a value - none of them - and not a null: packed may
    // be null with length 0, as std::vector::data() of an empty one can be.
    out.isNull = (packed == nullptr && length > 0);
    return out;
}

MqttSparkplugValue mq_spTemplate(const uint8_t* encoded, uint32_t length) {
    return bytesOf(MqttSparkplugDataType::Template, encoded, length);
}

MqttSparkplugValue mq_spDataSet(const uint8_t* encoded, uint32_t length) {
    return bytesOf(MqttSparkplugDataType::DataSet, encoded, length);
}

// --- Arrays ---

namespace {

void putLittle(uint8_t* at, uint64_t value, size_t width) {
    for (size_t index = 0; index < width; index++) {
        at[index] = (uint8_t)(value >> (8 * index));
    }
}

uint64_t takeLittle(const uint8_t* at, size_t width) {
    uint64_t value = 0;
    for (size_t index = 0; index < width; index++) {
        value |= (uint64_t)at[index] << (8 * index);
    }
    return value;
}

// One element of a program's own array, as the raw little-endian bits.
uint64_t nativeBits(MqttSparkplugDataType arrayType, const void* elements,
                    size_t index) {
    switch (arrayType) {
        case MqttSparkplugDataType::Int8Array:
            return (uint64_t)(uint8_t)((const int8_t*)elements)[index];
        case MqttSparkplugDataType::UInt8Array:
            return ((const uint8_t*)elements)[index];
        case MqttSparkplugDataType::Int16Array:
            return (uint64_t)(uint16_t)((const int16_t*)elements)[index];
        case MqttSparkplugDataType::UInt16Array:
            return ((const uint16_t*)elements)[index];
        case MqttSparkplugDataType::Int32Array:
            return (uint64_t)(uint32_t)((const int32_t*)elements)[index];
        case MqttSparkplugDataType::UInt32Array:
            return ((const uint32_t*)elements)[index];
        case MqttSparkplugDataType::FloatArray: {
            uint32_t bits = 0;
            memcpy(&bits, &((const float*)elements)[index], sizeof bits);
            return bits;
        }
        case MqttSparkplugDataType::Int64Array:
            return (uint64_t)((const int64_t*)elements)[index];
        case MqttSparkplugDataType::UInt64Array:
        case MqttSparkplugDataType::DateTimeArray:
            return ((const uint64_t*)elements)[index];
        case MqttSparkplugDataType::DoubleArray: {
            uint64_t bits = 0;
            memcpy(&bits, &((const double*)elements)[index], sizeof bits);
            return bits;
        }
        default:
            return 0;
    }
}

}  // namespace

size_t mq_spPackArray(MqttSparkplugDataType arrayType, const void* elements,
                      size_t count, uint8_t* out, size_t capacity) {
    if (count > 0 && elements == nullptr) return 0;
    if (arrayType == MqttSparkplugDataType::StringArray) {
        const char* const* strings = (const char* const*)elements;
        size_t needed = 0;
        for (size_t index = 0; index < count; index++) {
            needed += (strings[index] ? strlen(strings[index]) : 0) + 1;
        }
        if (out == nullptr || capacity < needed) return needed;
        size_t at = 0;
        for (size_t index = 0; index < count; index++) {
            const size_t length = strings[index] ? strlen(strings[index]) : 0;
            if (length > 0) memcpy(out + at, strings[index], length);
            at += length;
            out[at++] = 0;
        }
        return needed;
    }
    if (arrayType == MqttSparkplugDataType::BooleanArray) {
        // A uint32 count, then the bits, most significant first.
        if (count > 0xFFFFFFFFu) return 0;
        const size_t needed = 4 + (count + 7) / 8;
        if (out == nullptr || capacity < needed) return needed;
        memset(out, 0, needed);
        putLittle(out, (uint64_t)count, 4);
        const bool* flags = (const bool*)elements;
        for (size_t index = 0; index < count; index++) {
            if (flags[index]) out[4 + index / 8] |= (uint8_t)(1u << (7 - index % 8));
        }
        return needed;
    }
    const size_t width = mq_sparkplugArrayElementBytes(arrayType);
    if (width == 0 || count > ((size_t)-1) / width) return 0;
    const size_t needed = count * width;
    if (out == nullptr || capacity < needed) return needed;
    for (size_t index = 0; index < count; index++) {
        putLittle(out + index * width, nativeBits(arrayType, elements, index), width);
    }
    return needed;
}

bool mq_spArrayCount(const MqttSparkplugValue& array, size_t& count) {
    count = 0;
    if (!array.isArray()) return false;
    if (array.isNull) return true;
    const uint8_t* data = array.as.bytes.data;
    const size_t length = array.as.bytes.length;
    if (length > 0 && data == nullptr) return false;
    if (array.type == MqttSparkplugDataType::StringArray) {
        // Every string ends with a NUL, the last one included.
        if (length > 0 && data[length - 1] != 0) return false;
        for (size_t at = 0; at < length; at++) {
            if (data[at] == 0) count++;
        }
        return true;
    }
    if (array.type == MqttSparkplugDataType::BooleanArray) {
        if (length < 4) return length == 0;   // no count at all: none
        const uint64_t claimed = takeLittle(data, 4);
        if (length - 4 < (claimed + 7) / 8) return false;
        count = (size_t)claimed;
        return true;
    }
    const size_t width = mq_sparkplugArrayElementBytes(array.type);
    if (width == 0 || length % width != 0) return false;
    count = length / width;
    return true;
}

bool mq_spArrayAt(const MqttSparkplugValue& array, size_t index,
                  MqttSparkplugValue& element) {
    size_t count = 0;
    if (!mq_spArrayCount(array, count) || index >= count) return false;
    const uint8_t* data = array.as.bytes.data;
    element = MqttSparkplugValue();
    element.type = mq_sparkplugArrayElement(array.type);
    if (array.type == MqttSparkplugDataType::StringArray) {
        size_t at = 0;
        for (size_t skipped = 0; skipped < index; skipped++) {
            while (data[at] != 0) at++;
            at++;
        }
        size_t end = at;
        while (data[end] != 0) end++;
        element.as.bytes.data = data + at;
        element.as.bytes.length = (uint32_t)(end - at);
        return true;
    }
    if (array.type == MqttSparkplugDataType::BooleanArray) {
        element.as.boolean = ((data[4 + index / 8] >> (7 - index % 8)) & 1u) != 0;
        return true;
    }
    const size_t width = mq_sparkplugArrayElementBytes(array.type);
    const uint64_t raw = takeLittle(data + index * width, width);
    switch (element.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::Int64: {
            // Sign-extended from its own width.
            const unsigned shift = (unsigned)(64 - 8 * width);
            element.as.signedValue = (int64_t)(raw << shift) >> shift;
            break;
        }
        case MqttSparkplugDataType::Float: {
            const uint32_t bits = (uint32_t)raw;
            memcpy(&element.as.float32, &bits, sizeof bits);
            break;
        }
        case MqttSparkplugDataType::Double:
            memcpy(&element.as.float64, &raw, sizeof raw);
            break;
        default:
            element.as.unsignedValue = raw;
            break;
    }
    return true;
}

// --- A value's bytes, owned ---

MqttSparkplugBytes::~MqttSparkplugBytes() noexcept { mq_freeLarge(_data); }

MqttSparkplugBytes::MqttSparkplugBytes(MqttSparkplugBytes&& other) noexcept
    : _data(other._data), _size(other._size), _capacity(other._capacity) {
    other._data = nullptr;
    other._size = 0;
    other._capacity = 0;
}

MqttSparkplugBytes& MqttSparkplugBytes::operator=(MqttSparkplugBytes&& other) noexcept {
    if (this != &other) {
        mq_freeLarge(_data);
        _data = other._data;
        _size = other._size;
        _capacity = other._capacity;
        other._data = nullptr;
        other._size = 0;
        other._capacity = 0;
    }
    return *this;
}

bool MqttSparkplugBytes::assign(const uint8_t* data, size_t length) noexcept {
    if (length == 0 || data == nullptr) {
        _size = 0;
        return length == 0;
    }
    if (length > 0xFFFFFFFFu) {
        _size = 0;
        return false;
    }
    if (length > _capacity) {
        // A little over, so a value that grows by a character does not
        // allocate again on the next scan. The old block is freed only after
        // the copy: `data` may be inside it.
        const size_t want = length + length / 8 + 16;
        uint8_t* grown = (uint8_t*)mq_allocLarge(want);
        if (grown == nullptr) {
            _size = 0;
            return false;
        }
        memcpy(grown, data, length);
        mq_freeLarge(_data);
        _data = grown;
        _capacity = (uint32_t)want;
    } else if (data != _data) {
        memmove(_data, data, length);
    }
    _size = (uint32_t)length;
    return true;
}

MqttSparkplugValue MqttSparkplugBytes::keep(const MqttSparkplugValue& value) noexcept {
    if (value.isNull || !value.isSpan()) return value;
    MqttSparkplugValue owned = value;
    if (!assign(value.as.bytes.data, value.as.bytes.length)) {
        owned.isNull = true;
        owned.as.bytes.data = nullptr;
        owned.as.bytes.length = 0;
        return owned;
    }
    owned.as.bytes.data = _size > 0 ? _data : nullptr;
    owned.as.bytes.length = _size;
    return owned;
}

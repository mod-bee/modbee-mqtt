// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugPayload.h"

#include "../MqttPlatform.h"

#include <new>
#include <string.h>
#include "../MqttNoExcept.h"

// =============================================================================
// PROTOBUF PRIMITIVES
// =============================================================================

size_t mq_pbVarintSize(uint64_t value) {
    size_t bytes = 1;
    while (value > 0x7Fu) {
        value >>= 7;
        bytes++;
    }
    return bytes;
}

size_t mq_pbWriteVarint(uint8_t* out, size_t capacity, uint64_t value) {
    const size_t needed = mq_pbVarintSize(value);
    if (out == nullptr || capacity < needed) return 0;

    size_t written = 0;
    while (value > 0x7Fu) {
        out[written++] = (uint8_t)((value & 0x7Fu) | 0x80u);
        value >>= 7;
    }
    out[written++] = (uint8_t)value;
    return written;
}

MqttSparkplugError mq_pbReadVarint(const uint8_t* in, size_t available,
                                   uint64_t& value, size_t& consumed) {
    value = 0;
    consumed = 0;
    if (in == nullptr) return MqttSparkplugError::Truncated;

    // Ten bytes is the most a 64-bit value needs: nine groups of seven bits is
    // 63, so the tenth byte carries the last one. An eleventh continuation bit
    // is either corruption or an attempt to encode a value this type cannot
    // hold, and both are refused rather than truncated silently.
    for (size_t index = 0; index < 10; index++) {
        if (index >= available) {
            consumed = available;
            return MqttSparkplugError::Truncated;
        }
        const uint8_t byte = in[index];
        // The shift is guarded by the loop bound: index is at most 9, so the
        // shift is at most 63 and never undefined.
        value |= ((uint64_t)(byte & 0x7Fu)) << (7 * index);
        if ((byte & 0x80u) == 0) {
            consumed = index + 1;
            return MqttSparkplugError::None;
        }
    }
    consumed = 10;
    return MqttSparkplugError::VarintTooLong;
}

namespace {

// --- A bounded writer ---
//
// Not MqttWriter, because that one is big-endian by construction (u16/u32 write
// network byte order, which is what MQTT wants) and protobuf's fixed widths are
// little-endian. Reusing it would mean using only its raw() and reimplementing
// everything else, so this is a small local one with the same contract: asked
// for more room than it has it fails, writes nothing further, and stays failed.
class PbWriter {
public:
    PbWriter(uint8_t* data, size_t capacity)
        : _data(data), _capacity(data ? capacity : 0) {}

    bool ok() const { return _ok; }
    size_t written() const { return _cursor; }

    void tag(uint32_t field, uint32_t wireType) {
        varint(((uint64_t)field << 3) | wireType);
    }

    void varint(uint64_t value) {
        if (!_ok) return;
        const size_t need = mq_pbVarintSize(value);
        uint8_t* room = take(need);
        if (room == nullptr) return;
        mq_pbWriteVarint(room, need, value);
    }

    void fixed32(uint32_t value) {
        if (!_ok) return;
        uint8_t* room = take(4);
        if (room == nullptr) return;
        // Little-endian, explicitly, byte by byte. A memcpy of the host's
        // representation would be correct on every board this library targets
        // and wrong on a big-endian one, and "wrong only on hardware we have
        // not tried" is how a wire format quietly acquires a platform.
        room[0] = (uint8_t)(value & 0xFFu);
        room[1] = (uint8_t)((value >> 8) & 0xFFu);
        room[2] = (uint8_t)((value >> 16) & 0xFFu);
        room[3] = (uint8_t)((value >> 24) & 0xFFu);
    }

    void fixed64(uint64_t value) {
        if (!_ok) return;
        uint8_t* room = take(8);
        if (room == nullptr) return;
        for (size_t index = 0; index < 8; index++) {
            room[index] = (uint8_t)((value >> (8 * index)) & 0xFFu);
        }
    }

    void bytes(const uint8_t* data, size_t length) {
        if (!_ok) return;
        varint((uint64_t)length);
        if (length == 0) return;
        uint8_t* room = take(length);
        if (room == nullptr) return;
        if (data == nullptr) {
            _ok = false;
            return;
        }
        memcpy(room, data, length);
    }

    void text(const char* value) {
        const size_t length = value ? strlen(value) : 0;
        bytes((const uint8_t*)value, length);
    }

private:
    uint8_t* take(size_t count) {
        if (!_ok) return nullptr;
        if (count > _capacity - _cursor || _cursor > _capacity) {
            _ok = false;
            return nullptr;
        }
        uint8_t* at = _data + _cursor;
        _cursor += count;
        return at;
    }

    uint8_t* _data;
    size_t _capacity;
    size_t _cursor = 0;
    bool _ok = true;
};

// --- Float bit punning, without type punning ---
//
// memcpy rather than a union or a reinterpret_cast: it is the only form that is
// not undefined behaviour, and every compiler this library is built with
// reduces it to a single move. -Werror is on, so the alternatives would not
// survive the build anyway.
uint32_t bitsOfFloat(float value) {
    uint32_t bits = 0;
    memcpy(&bits, &value, sizeof bits);
    return bits;
}

float floatOfBits(uint32_t bits) {
    float value = 0.0f;
    memcpy(&value, &bits, sizeof value);
    return value;
}

uint64_t bitsOfDouble(double value) {
    uint64_t bits = 0;
    memcpy(&bits, &value, sizeof bits);
    return bits;
}

double doubleOfBits(uint64_t bits) {
    double value = 0.0;
    memcpy(&value, &bits, sizeof value);
    return value;
}

// --- Sign handling, the part that is easy to get wrong ---
//
// Section 6.4.17 puts Int8, Int16 and Int32 in int_value, whose protobuf type
// is uint32. So the value is reinterpreted, not converted: Int8 -1 becomes the
// uint32 0xFFFFFFFF, NOT 0xFF. The width lives in the datatype and the
// encoding is always 32 or 64 bits wide.
//
// This matters because a decoder that masked to 8 bits would read -1 as 255,
// and both ends would look self-consistent while disagreeing.
uint64_t encodeSigned(MqttSparkplugDataType type, int64_t value) {
    switch (type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
            return (uint64_t)(uint32_t)(int32_t)value;
        default:
            return (uint64_t)value;
    }
}

int64_t decodeSigned(MqttSparkplugDataType type, uint64_t raw) {
    switch (type) {
        case MqttSparkplugDataType::Int8:
            return (int64_t)(int8_t)(uint8_t)(raw & 0xFFu);
        case MqttSparkplugDataType::Int16:
            return (int64_t)(int16_t)(uint16_t)(raw & 0xFFFFu);
        case MqttSparkplugDataType::Int32:
            return (int64_t)(int32_t)(uint32_t)(raw & 0xFFFFFFFFu);
        default:
            return (int64_t)raw;
    }
}

bool isSignedType(MqttSparkplugDataType type) {
    return type == MqttSparkplugDataType::Int8 ||
           type == MqttSparkplugDataType::Int16 ||
           type == MqttSparkplugDataType::Int32 ||
           type == MqttSparkplugDataType::Int64;
}

// --- Property sizing and writing ---

size_t propertyValueBodySize(const MqttSparkplugProperty& property,
                             MqttSparkplugError& error) {
    size_t total = 0;
    total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_TYPE) +
             mq_pbVarintSize((uint64_t)property.type);

    if (property.isNull) {
        total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_IS_NULL) + 1;
        return total;
    }

    switch (property.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
        case MqttSparkplugDataType::UInt32:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_INT);
            total += mq_pbVarintSize(isSignedType(property.type)
                                         ? encodeSigned(property.type,
                                                        property.as.signedValue)
                                         : property.as.unsignedValue);
            break;
        case MqttSparkplugDataType::Int64:
        case MqttSparkplugDataType::UInt64:
        case MqttSparkplugDataType::DateTime:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_LONG);
            total += mq_pbVarintSize(isSignedType(property.type)
                                         ? (uint64_t)property.as.signedValue
                                         : property.as.unsignedValue);
            break;
        case MqttSparkplugDataType::Float:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_FLOAT) + 4;
            break;
        case MqttSparkplugDataType::Double:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_DOUBLE) + 8;
            break;
        case MqttSparkplugDataType::Boolean:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_BOOLEAN) + 1;
            break;
        case MqttSparkplugDataType::String:
        case MqttSparkplugDataType::Text:
        case MqttSparkplugDataType::UUID:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_STRING) +
                     mq_pbVarintSize(property.as.text.length) +
                     property.as.text.length;
            break;
        default:
            error = MqttSparkplugError::DataTypeNotSupported;
            return 0;
    }
    return total;
}

void writePropertyValue(PbWriter& writer,
                        const MqttSparkplugProperty& property) {
    writer.tag(MQ_SP_F_PROPERTYVALUE_TYPE, MQ_PB_VARINT);
    writer.varint((uint64_t)property.type);

    if (property.isNull) {
        writer.tag(MQ_SP_F_PROPERTYVALUE_IS_NULL, MQ_PB_VARINT);
        writer.varint(1);
        return;
    }

    switch (property.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
        case MqttSparkplugDataType::UInt32:
            writer.tag(MQ_SP_F_PROPERTYVALUE_INT, MQ_PB_VARINT);
            writer.varint(isSignedType(property.type)
                              ? encodeSigned(property.type,
                                             property.as.signedValue)
                              : property.as.unsignedValue);
            break;
        case MqttSparkplugDataType::Int64:
        case MqttSparkplugDataType::UInt64:
        case MqttSparkplugDataType::DateTime:
            writer.tag(MQ_SP_F_PROPERTYVALUE_LONG, MQ_PB_VARINT);
            writer.varint(isSignedType(property.type)
                              ? (uint64_t)property.as.signedValue
                              : property.as.unsignedValue);
            break;
        case MqttSparkplugDataType::Float:
            writer.tag(MQ_SP_F_PROPERTYVALUE_FLOAT, MQ_PB_FIXED32);
            writer.fixed32(bitsOfFloat(property.as.float32));
            break;
        case MqttSparkplugDataType::Double:
            writer.tag(MQ_SP_F_PROPERTYVALUE_DOUBLE, MQ_PB_FIXED64);
            writer.fixed64(bitsOfDouble(property.as.float64));
            break;
        case MqttSparkplugDataType::Boolean:
            writer.tag(MQ_SP_F_PROPERTYVALUE_BOOLEAN, MQ_PB_VARINT);
            writer.varint(property.as.boolean ? 1 : 0);
            break;
        case MqttSparkplugDataType::String:
        case MqttSparkplugDataType::Text:
        case MqttSparkplugDataType::UUID:
            writer.tag(MQ_SP_F_PROPERTYVALUE_STRING, MQ_PB_LENGTH);
            writer.bytes(property.as.text.data, property.as.text.length);
            break;
        default:
            break;
    }
}

// The PropertySet wrapping a metric's properties. Two parallel repeated
// fields, keys then values, index-matched.
size_t propertySetBodySize(const MqttSparkplugMetric& metric,
                           MqttSparkplugError& error) {
    size_t total = 0;
    for (size_t index = 0; index < metric.propertyCount; index++) {
        const MqttSparkplugProperty& property = metric.properties[index];
        const size_t keyLength = property.key ? strlen(property.key) : 0;
        total += mq_pbTagSize(MQ_SP_F_PROPERTYSET_KEYS) +
                 mq_pbVarintSize(keyLength) + keyLength;

        const size_t valueBody = propertyValueBodySize(property, error);
        if (error != MqttSparkplugError::None) return 0;
        total += mq_pbTagSize(MQ_SP_F_PROPERTYSET_VALUES) +
                 mq_pbVarintSize(valueBody) + valueBody;
    }
    return total;
}

void writePropertySet(PbWriter& writer, const MqttSparkplugMetric& metric) {
    for (size_t index = 0; index < metric.propertyCount; index++) {
        const MqttSparkplugProperty& property = metric.properties[index];
        writer.tag(MQ_SP_F_PROPERTYSET_KEYS, MQ_PB_LENGTH);
        writer.text(property.key);

        MqttSparkplugError ignored = MqttSparkplugError::None;
        const size_t valueBody = propertyValueBodySize(property, ignored);
        writer.tag(MQ_SP_F_PROPERTYSET_VALUES, MQ_PB_LENGTH);
        writer.varint(valueBody);
        writePropertyValue(writer, property);
    }
}

// --- Metric sizing and writing ---
//
// `type` decides whether the name or the alias is written, and that is the
// whole of [tck-id-payloads-alias-birth-requirement] and
// [tck-id-payloads-alias-data-cmd-requirement] in one place rather than
// scattered through the callers.
bool metricWritesName(MqttSparkplugType type, const MqttSparkplugMetric& metric) {
    if (metric.name == nullptr) return false;
    // Births carry both. Data and command messages carry the alias ALONE, and
    // only if there is one - a metric with no alias must still carry its name
    // or the far end cannot identify it at all.
    if (mq_sparkplugIsBirth(type)) return true;
    if (type == MqttSparkplugType::State) return true;
    return !metric.hasAlias;
}

bool metricWritesAlias(const MqttSparkplugMetric& metric) {
    return metric.hasAlias;
}

size_t metricBodySize(const MqttSparkplugMetric& metric,
                      MqttSparkplugType type,
                      MqttSparkplugError& error) {
    if (!mq_sparkplugDataTypeSupported(metric.value.type)) {
        error = MqttSparkplugError::DataTypeNotSupported;
        return 0;
    }
    if (metric.name == nullptr && !metric.hasAlias) {
        // Neither a name nor an alias is a metric nothing can identify.
        error = MqttSparkplugError::InvalidName;
        return 0;
    }

    size_t total = 0;

    if (metricWritesName(type, metric)) {
        const size_t nameLength = strlen(metric.name);
        total += mq_pbTagSize(MQ_SP_F_METRIC_NAME) +
                 mq_pbVarintSize(nameLength) + nameLength;
    }
    if (metricWritesAlias(metric)) {
        total += mq_pbTagSize(MQ_SP_F_METRIC_ALIAS) +
                 mq_pbVarintSize(metric.alias);
    }
    if (metric.hasTimestamp) {
        total += mq_pbTagSize(MQ_SP_F_METRIC_TIMESTAMP) +
                 mq_pbVarintSize(metric.timestamp);
    }

    total += mq_pbTagSize(MQ_SP_F_METRIC_DATATYPE) +
             mq_pbVarintSize((uint64_t)metric.value.type);

    if (metric.isHistorical) {
        total += mq_pbTagSize(MQ_SP_F_METRIC_IS_HISTORICAL) + 1;
    }
    if (metric.isTransient) {
        total += mq_pbTagSize(MQ_SP_F_METRIC_IS_TRANSIENT) + 1;
    }

    if (metric.value.isNull) {
        // is_null true and NO value field. The schema's own comment: "If this
        // is null - explicitly say so rather than using -1, false, etc".
        total += mq_pbTagSize(MQ_SP_F_METRIC_IS_NULL) + 1;
    } else {
        const uint32_t field = mq_sparkplugValueField(metric.value.type);
        switch (metric.value.type) {
            case MqttSparkplugDataType::Int8:
            case MqttSparkplugDataType::Int16:
            case MqttSparkplugDataType::Int32:
            case MqttSparkplugDataType::UInt8:
            case MqttSparkplugDataType::UInt16:
            case MqttSparkplugDataType::UInt32:
                total += mq_pbTagSize(field);
                total += mq_pbVarintSize(
                    isSignedType(metric.value.type)
                        ? encodeSigned(metric.value.type,
                                       metric.value.as.signedValue)
                        : metric.value.as.unsignedValue);
                break;
            case MqttSparkplugDataType::Int64:
            case MqttSparkplugDataType::UInt64:
            case MqttSparkplugDataType::DateTime:
                total += mq_pbTagSize(field);
                total += mq_pbVarintSize(
                    isSignedType(metric.value.type)
                        ? (uint64_t)metric.value.as.signedValue
                        : metric.value.as.unsignedValue);
                break;
            case MqttSparkplugDataType::Float:
                total += mq_pbTagSize(field) + 4;
                break;
            case MqttSparkplugDataType::Double:
                total += mq_pbTagSize(field) + 8;
                break;
            case MqttSparkplugDataType::Boolean:
                total += mq_pbTagSize(field) + 1;
                break;
            case MqttSparkplugDataType::String:
            case MqttSparkplugDataType::Text:
            case MqttSparkplugDataType::UUID:
            case MqttSparkplugDataType::Bytes:
            case MqttSparkplugDataType::File:
                total += mq_pbTagSize(field) +
                         mq_pbVarintSize(metric.value.as.bytes.length) +
                         metric.value.as.bytes.length;
                break;
            default:
                error = MqttSparkplugError::DataTypeNotSupported;
                return 0;
        }
    }

    if (metric.propertyCount > 0) {
        const size_t setBody = propertySetBodySize(metric, error);
        if (error != MqttSparkplugError::None) return 0;
        total += mq_pbTagSize(MQ_SP_F_METRIC_PROPERTIES) +
                 mq_pbVarintSize(setBody) + setBody;
    }

    return total;
}

void writeMetric(PbWriter& writer, const MqttSparkplugMetric& metric,
                 MqttSparkplugType type) {
    // Field order is ascending by number. Protobuf does not require it, but
    // every generated encoder does it, and matching them means a byte-for-byte
    // comparison against another implementation is possible at all.
    if (metricWritesName(type, metric)) {
        writer.tag(MQ_SP_F_METRIC_NAME, MQ_PB_LENGTH);
        writer.text(metric.name);
    }
    if (metricWritesAlias(metric)) {
        writer.tag(MQ_SP_F_METRIC_ALIAS, MQ_PB_VARINT);
        writer.varint(metric.alias);
    }
    if (metric.hasTimestamp) {
        writer.tag(MQ_SP_F_METRIC_TIMESTAMP, MQ_PB_VARINT);
        writer.varint(metric.timestamp);
    }

    writer.tag(MQ_SP_F_METRIC_DATATYPE, MQ_PB_VARINT);
    writer.varint((uint64_t)metric.value.type);

    if (metric.isHistorical) {
        writer.tag(MQ_SP_F_METRIC_IS_HISTORICAL, MQ_PB_VARINT);
        writer.varint(1);
    }
    if (metric.isTransient) {
        writer.tag(MQ_SP_F_METRIC_IS_TRANSIENT, MQ_PB_VARINT);
        writer.varint(1);
    }
    if (metric.value.isNull) {
        writer.tag(MQ_SP_F_METRIC_IS_NULL, MQ_PB_VARINT);
        writer.varint(1);
    }

    if (metric.propertyCount > 0) {
        MqttSparkplugError ignored = MqttSparkplugError::None;
        const size_t setBody = propertySetBodySize(metric, ignored);
        writer.tag(MQ_SP_F_METRIC_PROPERTIES, MQ_PB_LENGTH);
        writer.varint(setBody);
        writePropertySet(writer, metric);
    }

    if (metric.value.isNull) return;

    const uint32_t field = mq_sparkplugValueField(metric.value.type);
    switch (metric.value.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
        case MqttSparkplugDataType::UInt32:
            writer.tag(field, MQ_PB_VARINT);
            writer.varint(isSignedType(metric.value.type)
                              ? encodeSigned(metric.value.type,
                                             metric.value.as.signedValue)
                              : metric.value.as.unsignedValue);
            break;
        case MqttSparkplugDataType::Int64:
        case MqttSparkplugDataType::UInt64:
        case MqttSparkplugDataType::DateTime:
            writer.tag(field, MQ_PB_VARINT);
            writer.varint(isSignedType(metric.value.type)
                              ? (uint64_t)metric.value.as.signedValue
                              : metric.value.as.unsignedValue);
            break;
        case MqttSparkplugDataType::Float:
            writer.tag(field, MQ_PB_FIXED32);
            writer.fixed32(bitsOfFloat(metric.value.as.float32));
            break;
        case MqttSparkplugDataType::Double:
            writer.tag(field, MQ_PB_FIXED64);
            writer.fixed64(bitsOfDouble(metric.value.as.float64));
            break;
        case MqttSparkplugDataType::Boolean:
            writer.tag(field, MQ_PB_VARINT);
            writer.varint(metric.value.as.boolean ? 1 : 0);
            break;
        case MqttSparkplugDataType::String:
        case MqttSparkplugDataType::Text:
        case MqttSparkplugDataType::UUID:
        case MqttSparkplugDataType::Bytes:
        case MqttSparkplugDataType::File:
            writer.tag(field, MQ_PB_LENGTH);
            writer.bytes(metric.value.as.bytes.data,
                         metric.value.as.bytes.length);
            break;
        default:
            break;
    }
}

// --- Decoding helpers ---

struct PbReader {
    // An explicit constructor, not aggregate initialisation: it is what turns a
    // null pointer into an empty reader, which an aggregate would not. (It was
    // first written for C++11, where the default member initialisers below
    // stopped this being an aggregate at all; the library is C++14 now -
    // scripts/check-cxx14.sh - and the null guard is why it stays.)
    PbReader(const uint8_t* bytes, size_t count)
        : data(bytes), length(bytes ? count : 0) {}

    const uint8_t* data;
    size_t length;
    size_t cursor = 0;
    MqttSparkplugError error = MqttSparkplugError::None;

    bool ok() const { return error == MqttSparkplugError::None; }
    bool atEnd() const { return cursor >= length; }
    size_t remaining() const { return cursor < length ? length - cursor : 0; }

    void fail(MqttSparkplugError reason) {
        if (error == MqttSparkplugError::None) error = reason;
    }

    uint64_t varint() {
        if (!ok()) return 0;
        uint64_t value = 0;
        size_t consumed = 0;
        const MqttSparkplugError result =
            mq_pbReadVarint(data + cursor, remaining(), value, consumed);
        cursor += consumed;
        if (result != MqttSparkplugError::None) {
            fail(result);
            return 0;
        }
        return value;
    }

    uint32_t fixed32() {
        if (!ok()) return 0;
        if (remaining() < 4) {
            cursor = length;
            fail(MqttSparkplugError::Truncated);
            return 0;
        }
        const uint8_t* at = data + cursor;
        cursor += 4;
        return (uint32_t)at[0] | ((uint32_t)at[1] << 8) |
               ((uint32_t)at[2] << 16) | ((uint32_t)at[3] << 24);
    }

    uint64_t fixed64() {
        if (!ok()) return 0;
        if (remaining() < 8) {
            cursor = length;
            fail(MqttSparkplugError::Truncated);
            return 0;
        }
        const uint8_t* at = data + cursor;
        cursor += 8;
        uint64_t value = 0;
        for (size_t index = 0; index < 8; index++) {
            value |= ((uint64_t)at[index]) << (8 * index);
        }
        return value;
    }

    // A length-delimited field. LengthOverrun rather than Truncated when the
    // prefix claims more than the message holds: that is the shape a hostile
    // payload takes - a small message claiming a large field - and telling it
    // apart from an honestly truncated one is worth a distinct name.
    bool lengthDelimited(const uint8_t*& out, size_t& outLength) {
        const uint64_t claimed = varint();
        if (!ok()) return false;
        if (claimed > remaining()) {
            cursor = length;
            fail(MqttSparkplugError::LengthOverrun);
            return false;
        }
        out = data + cursor;
        outLength = (size_t)claimed;
        cursor += (size_t)claimed;
        return true;
    }

    // Everything this build does not recognise. Protobuf's whole forward
    // compatibility story, and Payload carries `extensions 6 to max`, so this
    // path is exercised by real traffic and not only by the fuzzer.
    bool skip(uint32_t wireType) {
        switch (wireType) {
            case MQ_PB_VARINT:
                varint();
                return ok();
            case MQ_PB_FIXED64:
                fixed64();
                return ok();
            case MQ_PB_FIXED32:
                fixed32();
                return ok();
            case MQ_PB_LENGTH: {
                const uint8_t* ignored = nullptr;
                size_t ignoredLength = 0;
                return lengthDelimited(ignored, ignoredLength);
            }
            default:
                // Wire types 3 and 4 are the deprecated start-group and
                // end-group, and 6 and 7 were never defined. None appears in
                // this schema, and a payload carrying one is malformed rather
                // than merely unknown - there is no length to skip by.
                fail(MqttSparkplugError::WireTypeMismatch);
                return false;
        }
    }
};

bool readTag(PbReader& reader, uint32_t& field, uint32_t& wireType) {
    const uint64_t tag = reader.varint();
    if (!reader.ok()) return false;
    field = (uint32_t)(tag >> 3);
    wireType = (uint32_t)(tag & 0x7u);
    // Field number 0 is not legal protobuf and is what a run of zero bytes
    // decodes to, so it is refused rather than looping.
    if (field == 0) {
        reader.fail(MqttSparkplugError::WireTypeMismatch);
        return false;
    }
    return true;
}

bool decodePropertyValue(const uint8_t* body, size_t bodyLength,
                         MqttSparkplugProperty& out,
                         MqttSparkplugError& error) {
    // THE FIELD THAT ARRIVED IS HELD RAW, AND INTERPRETED BY THE DECLARED TYPE
    // ONCE BOTH ARE KNOWN - exactly as decodeMetric() does, and for the same
    // reason. This used to write whichever union member the wire field named,
    // and then trust the declared type: a property declared Boolean whose value
    // came in int_value left the union's INTEGER bytes under its bool, and
    // re-encoding it read a bool of 244. Undefined behaviour, found by the
    // fuzzer under -fsanitize=undefined; and wrong even where it happened not
    // to crash, because a property's meaning is its declared type, not
    // whatever field a sender chose.
    uint64_t rawVarint = 0;
    bool sawVarint = false;
    bool rawBoolean = false;
    bool sawBoolean = false;
    uint32_t rawFixed32 = 0;
    bool sawFixed32 = false;
    uint64_t rawFixed64 = 0;
    bool sawFixed64 = false;
    const uint8_t* rawText = nullptr;
    size_t rawTextLength = 0;
    bool sawText = false;

    PbReader reader(body, bodyLength);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;

        switch (field) {
            case MQ_SP_F_PROPERTYVALUE_TYPE:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.type = (MqttSparkplugDataType)(uint32_t)reader.varint();
                break;
            case MQ_SP_F_PROPERTYVALUE_IS_NULL:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.isNull = reader.varint() != 0;
                break;
            case MQ_SP_F_PROPERTYVALUE_INT:
            case MQ_SP_F_PROPERTYVALUE_LONG:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawVarint = reader.varint();
                sawVarint = true;
                break;
            case MQ_SP_F_PROPERTYVALUE_FLOAT:
                if (wireType != MQ_PB_FIXED32) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawFixed32 = reader.fixed32();
                sawFixed32 = true;
                break;
            case MQ_SP_F_PROPERTYVALUE_DOUBLE:
                if (wireType != MQ_PB_FIXED64) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawFixed64 = reader.fixed64();
                sawFixed64 = true;
                break;
            case MQ_SP_F_PROPERTYVALUE_BOOLEAN:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawBoolean = reader.varint() != 0;
                sawBoolean = true;
                break;
            case MQ_SP_F_PROPERTYVALUE_STRING: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                const uint8_t* text = nullptr;
                size_t textLength = 0;
                if (!reader.lengthDelimited(text, textLength)) break;
                rawText = text;
                rawTextLength = textLength;
                sawText = true;
                break;
            }
            default:
                reader.skip(wireType);
                break;
        }
    }

    if (!reader.ok()) {
        error = reader.error;
        return false;
    }

    // A field that does not belong to the declared type is ignored, and the
    // value stays at its zero - the same answer decodeMetric() gives. Every
    // member is written through the one that matches the type, so reading it
    // back is always defined.
    out.as.unsignedValue = 0;
    if (!out.isNull) {
        switch (out.type) {
            case MqttSparkplugDataType::Int8:
            case MqttSparkplugDataType::Int16:
            case MqttSparkplugDataType::Int32:
            case MqttSparkplugDataType::Int64:
                // Sign-correct now that the datatype is known. The fields can
                // arrive in any order, so this cannot be done as they are read.
                if (sawVarint) out.as.signedValue = decodeSigned(out.type, rawVarint);
                break;
            case MqttSparkplugDataType::UInt8:
            case MqttSparkplugDataType::UInt16:
            case MqttSparkplugDataType::UInt32:
            case MqttSparkplugDataType::UInt64:
            case MqttSparkplugDataType::DateTime:
                if (sawVarint) out.as.unsignedValue = rawVarint;
                break;
            case MqttSparkplugDataType::Boolean:
                out.as.boolean = sawBoolean && rawBoolean;
                break;
            case MqttSparkplugDataType::Float:
                if (sawFixed32) out.as.float32 = floatOfBits(rawFixed32);
                break;
            case MqttSparkplugDataType::Double:
                if (sawFixed64) out.as.float64 = doubleOfBits(rawFixed64);
                break;
            case MqttSparkplugDataType::String:
            case MqttSparkplugDataType::Text:
            case MqttSparkplugDataType::UUID:
                if (sawText) {
                    out.as.text.data = rawText;
                    out.as.text.length = (uint32_t)rawTextLength;
                } else {
                    out.as.text.data = nullptr;
                    out.as.text.length = 0;
                }
                break;
            default:
                break;
        }
    }
    return true;
}

}  // namespace

bool mq_sparkplugApplyDatatype(MqttSparkplugMetric& metric, MqttSparkplugDataType declared) {
    if (!metric.datatypeAbsent) return metric.value.type == declared;
    metric.datatypeAbsent = false;
    MqttSparkplugValue& value = metric.value;
    const MqttSparkplugDataType arrived = value.type;
    value.type = declared;
    metric.valueUnsupported = !mq_sparkplugDataTypeSupported(declared);
    if (metric.valueUnsupported) {
        value.isNull = true;
        value.as.unsignedValue = 0;
        return true;
    }
    if (value.isNull) return true;

    bool fits = false;
    switch (declared) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::Int64:
            fits = arrived == MqttSparkplugDataType::UInt64;
            if (fits) value.as.signedValue = decodeSigned(declared, value.as.unsignedValue);
            break;
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
        case MqttSparkplugDataType::UInt32:
        case MqttSparkplugDataType::UInt64:
        case MqttSparkplugDataType::DateTime:
            fits = arrived == MqttSparkplugDataType::UInt64;
            break;
        case MqttSparkplugDataType::Float:
        case MqttSparkplugDataType::Double:
        case MqttSparkplugDataType::Boolean:
            fits = arrived == declared;
            break;
        case MqttSparkplugDataType::String:
        case MqttSparkplugDataType::Text:
        case MqttSparkplugDataType::UUID:
        case MqttSparkplugDataType::Bytes:
        case MqttSparkplugDataType::File:
            fits = arrived == MqttSparkplugDataType::Bytes;
            break;
        default:
            break;
    }
    if (!fits) {
        value.isNull = true;
        value.as.unsignedValue = 0;
    }
    return fits;
}

uint32_t mq_sparkplugValueField(MqttSparkplugDataType type) {
    switch (type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
        case MqttSparkplugDataType::UInt32:
            return MQ_SP_F_METRIC_INT;
        case MqttSparkplugDataType::Int64:
        case MqttSparkplugDataType::UInt64:
        case MqttSparkplugDataType::DateTime:
            return MQ_SP_F_METRIC_LONG;
        case MqttSparkplugDataType::Float:
            return MQ_SP_F_METRIC_FLOAT;
        case MqttSparkplugDataType::Double:
            return MQ_SP_F_METRIC_DOUBLE;
        case MqttSparkplugDataType::Boolean:
            return MQ_SP_F_METRIC_BOOLEAN;
        case MqttSparkplugDataType::String:
        case MqttSparkplugDataType::Text:
        case MqttSparkplugDataType::UUID:
            return MQ_SP_F_METRIC_STRING;
        case MqttSparkplugDataType::Bytes:
        case MqttSparkplugDataType::File:
            return MQ_SP_F_METRIC_BYTES;
        default:
            return 0;
    }
}

MqttSparkplugProperty mq_spEngUnit(const char* units) {
    MqttSparkplugProperty out;
    out.key = "engUnit";
    out.type = MqttSparkplugDataType::String;
    out.as.text.data = (const uint8_t*)units;
    out.as.text.length = units ? (uint32_t)strlen(units) : 0;
    out.isNull = (units == nullptr);
    return out;
}

MqttSparkplugProperty mq_spQuality(int32_t quality) {
    MqttSparkplugProperty out;
    out.key = MQ_SP_PROPERTY_QUALITY;
    // [tck-id-payloads-propertyset-quality-value-type] The type of the Property
    // Value MUST be a value of 3 which represents a Signed 32-bit Integer.
    out.type = MqttSparkplugDataType::Int32;
    // [tck-id-payloads-propertyset-quality-value-value] ... one of the valid
    // quality codes of 0, 192, or 500. Anything else is clamped to BAD rather
    // than sent: a code the far end does not recognise is worse than one that
    // says "do not trust this", which is what an out-of-range code means.
    if (quality != MQ_SP_QUALITY_BAD && quality != MQ_SP_QUALITY_GOOD &&
        quality != MQ_SP_QUALITY_STALE) {
        quality = MQ_SP_QUALITY_BAD;
    }
    out.as.signedValue = quality;
    return out;
}

MqttSparkplugProperty mq_spDescription(const char* text) {
    MqttSparkplugProperty out;
    out.key = "description";
    out.type = MqttSparkplugDataType::String;
    out.as.text.data = (const uint8_t*)text;
    out.as.text.length = text ? (uint32_t)strlen(text) : 0;
    out.isNull = (text == nullptr);
    return out;
}

static MqttSparkplugProperty mq_spDoubleProperty(const char* key, double value) {
    MqttSparkplugProperty out;
    out.key = key;
    out.type = MqttSparkplugDataType::Double;
    out.as.float64 = value;
    return out;
}

MqttSparkplugProperty mq_spEngLow(double low) {
    return mq_spDoubleProperty("engLow", low);
}

MqttSparkplugProperty mq_spEngHigh(double high) {
    return mq_spDoubleProperty("engHigh", high);
}

const MqttSparkplugProperty*
MqttSparkplugMetric::findProperty(const char* key) const {
    if (key == nullptr) return nullptr;
    for (size_t index = 0; index < propertyCount; index++) {
        if (properties[index].key == nullptr) continue;
        if (strcmp(properties[index].key, key) == 0) return &properties[index];
    }
    return nullptr;
}

void MqttSparkplugPayload::clear() {
    // Assign a fresh default rather than memset: the metrics hold structs with
    // their own defaults, and a memset would zero an enum to Unknown in a way
    // that happens to be right today and would not survive a reordering.
    timestamp = 0;
    hasTimestamp = false;
    seq = 0;
    hasSeq = false;
    uuid = nullptr;
    body = nullptr;
    bodyLength = 0;
    for (size_t index = 0; index < metricCount; index++) {
        metrics[index] = MqttSparkplugMetric();
    }
    metricCount = 0;
    // The arena goes too. Every name the previous payload handed out is now
    // dangling, which is the documented contract - a payload's names live as
    // long as the payload holds them and no longer.
    namesUsed = 0;
}

const char* MqttSparkplugPayload::intern(const uint8_t* bytes, size_t length) {
    // +1 for the terminator. A name that does not fit returns null and the
    // decode fails by name: truncating it would create a second tag at the far
    // end that looks almost like the right one.
    if (names == nullptr || length + 1 > _nameCapacity - namesUsed) return nullptr;
    char* at = names + namesUsed;
    if (length > 0 && bytes != nullptr) memcpy(at, bytes, length);
    at[length] = '\0';
    namesUsed += length + 1;
    return at;
}

const char* MqttSparkplugPayload::rebase(const MqttSparkplugPayload& other,
                                         const char* pointer) const {
    if (pointer == nullptr) return nullptr;
    // Only what lives in the source's arena moves. Anything else - a string
    // literal a caller passed, a span into a receive buffer - is left exactly
    // as it was, because rebasing it would point at nothing.
    if (other.names == nullptr) return pointer;
    if (pointer < other.names) return pointer;
    if (pointer >= other.names + other._nameCapacity) return pointer;
    return names + (pointer - other.names);
}

MqttSparkplugPayload::MqttSparkplugPayload(size_t metricCapacity,
                                           size_t nameBytes) {
    allocate(metricCapacity, nameBytes);
}

MqttSparkplugPayload::~MqttSparkplugPayload() {
    release();
}

void MqttSparkplugPayload::release() {
    if (metrics != nullptr) {
        for (size_t index = 0; index < _metricCapacity; index++) {
            metrics[index].~MqttSparkplugMetric();
        }
        mq_freeLarge(metrics);
    }
    mq_freeLarge(names);
    metrics = nullptr;
    names = nullptr;
    _metricCapacity = 0;
    _nameCapacity = 0;
    metricCount = 0;
    namesUsed = 0;
}

bool MqttSparkplugPayload::allocate(size_t metricCapacity, size_t nameBytes) {
    release();
    if (metricCapacity > 0) {
        void* raw = mq_allocLarge(metricCapacity * sizeof(MqttSparkplugMetric));
        if (raw == nullptr) return false;
        metrics = static_cast<MqttSparkplugMetric*>(raw);
        for (size_t index = 0; index < metricCapacity; index++) {
            new (&metrics[index]) MqttSparkplugMetric();
        }
        _metricCapacity = metricCapacity;
    }
    if (nameBytes > 0) {
        names = static_cast<char*>(mq_allocLarge(nameBytes));
        if (names == nullptr) {
            release();
            return false;
        }
        _nameCapacity = nameBytes;
    }
    return true;
}

bool MqttSparkplugPayload::reserve(size_t metricCapacity, size_t nameBytes) {
    if (metricCapacity <= _metricCapacity && nameBytes <= _nameCapacity) {
        return true;
    }
    // Never smaller than it was in either dimension.
    if (metricCapacity < _metricCapacity) metricCapacity = _metricCapacity;
    if (nameBytes < _nameCapacity) nameBytes = _nameCapacity;

    // The new storage first, so a failure leaves the payload as it was.
    MqttSparkplugPayload grown(metricCapacity, nameBytes);
    if (grown._metricCapacity < metricCapacity || grown._nameCapacity < nameBytes) {
        return false;
    }
    release();
    metrics = grown.metrics;
    names = grown.names;
    _metricCapacity = grown._metricCapacity;
    _nameCapacity = grown._nameCapacity;
    grown.metrics = nullptr;
    grown.names = nullptr;
    grown._metricCapacity = 0;
    grown._nameCapacity = 0;
    clear();
    return true;
}

void MqttSparkplugPayload::copyFrom(const MqttSparkplugPayload& other) {
    // Room for what the source holds - its CONTENT, not its capacity, so a
    // copy of a decoded command does not inherit a birth certificate's size.
    if (_metricCapacity < other.metricCount || _nameCapacity < other.namesUsed) {
        const size_t wantMetrics = other.metricCount > _metricCapacity
                                       ? other.metricCount : _metricCapacity;
        const size_t wantNames = other.namesUsed > _nameCapacity
                                     ? other.namesUsed : _nameCapacity;
        if (!allocate(wantMetrics, wantNames)) {
            // Out of memory. An empty payload rather than a half-copied one.
            timestamp = 0;
            hasTimestamp = false;
            seq = 0;
            hasSeq = false;
            uuid = nullptr;
            body = nullptr;
            bodyLength = 0;
            return;
        }
    } else {
        clear();
    }

    timestamp = other.timestamp;
    hasTimestamp = other.hasTimestamp;
    seq = other.seq;
    hasSeq = other.hasSeq;
    body = other.body;
    bodyLength = other.bodyLength;

    namesUsed = other.namesUsed;
    if (namesUsed > 0 && names != nullptr) memcpy(names, other.names, namesUsed);

    uuid = rebase(other, other.uuid);

    metricCount = other.metricCount;
    for (size_t index = 0; index < metricCount; index++) {
        metrics[index] = other.metrics[index];
        metrics[index].name = rebase(other, other.metrics[index].name);
        for (size_t p = 0; p < metrics[index].propertyCount; p++) {
            metrics[index].properties[p].key =
                rebase(other, other.metrics[index].properties[p].key);
        }
    }
}

const char* MqttSparkplugPayload::intern(const char* text) {
    if (text == nullptr) return nullptr;
    return intern((const uint8_t*)text, strlen(text));
}

const MqttSparkplugMetric*
MqttSparkplugPayload::findMetric(const char* name) const {
    if (name == nullptr) return nullptr;
    for (size_t index = 0; index < metricCount; index++) {
        if (metrics[index].name == nullptr) continue;
        // strcmp, not a case-insensitive compare.
        // [tck-id-case-sensitivity-metric-names] makes metric names case
        // sensitive, so "LEVEL" and "level" are two metrics and folding them
        // together would merge two tags into one at the far end.
        if (strcmp(metrics[index].name, name) == 0) return &metrics[index];
    }
    return nullptr;
}

const MqttSparkplugMetric*
MqttSparkplugPayload::findAlias(uint64_t alias) const {
    for (size_t index = 0; index < metricCount; index++) {
        if (metrics[index].hasAlias && metrics[index].alias == alias) {
            return &metrics[index];
        }
    }
    return nullptr;
}

size_t mq_sparkplugSize(const MqttSparkplugPayload& payload,
                        MqttSparkplugType type,
                        MqttSparkplugError& error) {
    error = MqttSparkplugError::None;

    if (payload.metricCount > payload.capacity()) {
        error = MqttSparkplugError::TooManyMetrics;
        return 0;
    }

    size_t total = 0;

    if (payload.hasTimestamp) {
        total += mq_pbTagSize(MQ_SP_F_PAYLOAD_TIMESTAMP) +
                 mq_pbVarintSize(payload.timestamp);
    }

    for (size_t index = 0; index < payload.metricCount; index++) {
        const size_t body = metricBodySize(payload.metrics[index], type, error);
        if (error != MqttSparkplugError::None) return 0;
        total += mq_pbTagSize(MQ_SP_F_PAYLOAD_METRICS) +
                 mq_pbVarintSize(body) + body;
    }

    if (payload.hasSeq) {
        total += mq_pbTagSize(MQ_SP_F_PAYLOAD_SEQ) +
                 mq_pbVarintSize(payload.seq);
    }
    if (payload.uuid != nullptr) {
        const size_t uuidLength = strlen(payload.uuid);
        total += mq_pbTagSize(MQ_SP_F_PAYLOAD_UUID) +
                 mq_pbVarintSize(uuidLength) + uuidLength;
    }
    if (payload.body != nullptr && payload.bodyLength > 0) {
        total += mq_pbTagSize(MQ_SP_F_PAYLOAD_BODY) +
                 mq_pbVarintSize(payload.bodyLength) + payload.bodyLength;
    }

    return total;
}

size_t mq_sparkplugEncode(uint8_t* out, size_t capacity,
                          const MqttSparkplugPayload& payload,
                          MqttSparkplugType type,
                          MqttSparkplugError& error) {
    const size_t needed = mq_sparkplugSize(payload, type, error);
    if (error != MqttSparkplugError::None) return 0;
    if (out == nullptr || capacity < needed) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }

    PbWriter writer(out, capacity);

    // Ascending field order, matching what every generated encoder emits, so
    // that a byte comparison against another implementation means something.
    if (payload.hasTimestamp) {
        writer.tag(MQ_SP_F_PAYLOAD_TIMESTAMP, MQ_PB_VARINT);
        writer.varint(payload.timestamp);
    }

    for (size_t index = 0; index < payload.metricCount; index++) {
        MqttSparkplugError ignored = MqttSparkplugError::None;
        const size_t body =
            metricBodySize(payload.metrics[index], type, ignored);
        writer.tag(MQ_SP_F_PAYLOAD_METRICS, MQ_PB_LENGTH);
        writer.varint(body);
        writeMetric(writer, payload.metrics[index], type);
    }

    if (payload.hasSeq) {
        writer.tag(MQ_SP_F_PAYLOAD_SEQ, MQ_PB_VARINT);
        writer.varint(payload.seq);
    }
    if (payload.uuid != nullptr) {
        writer.tag(MQ_SP_F_PAYLOAD_UUID, MQ_PB_LENGTH);
        writer.text(payload.uuid);
    }
    if (payload.body != nullptr && payload.bodyLength > 0) {
        writer.tag(MQ_SP_F_PAYLOAD_BODY, MQ_PB_LENGTH);
        writer.bytes(payload.body, payload.bodyLength);
    }

    if (!writer.ok()) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }

    // The size pass and the write pass must agree exactly. If they ever do not,
    // the bug is here rather than at the far end, and finding out now is worth
    // the comparison.
    if (writer.written() != needed) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }
    return writer.written();
}

// The decoder's name storage. One arena, file-scope-free: it is a member of the
// decode context the caller passes in, and the caller is the node, which owns
// one per connection.
//
// Kept as a static thread-local-free single instance would break R5, so it is
// not. Instead the arena lives inside the payload the caller already owns: see
// the `names` member, sized by whoever constructed the payload, which is why it
// is passed by reference everywhere.
namespace {

bool decodeMetric(const uint8_t* body, size_t bodyLength,
                  MqttSparkplugMetric& out, MqttSparkplugPayload& owner,
                  MqttSparkplugError& error) {
    PbReader reader(body, bodyLength);

    // The value fields can arrive before the datatype, so the raw bits are
    // parked and interpreted once the whole metric is read. A decoder that
    // interpreted as it went would be wrong for any encoder that ordered the
    // fields differently - which protobuf permits and some do.
    uint64_t rawVarint = 0;
    uint32_t rawFixed32 = 0;
    uint64_t rawFixed64 = 0;
    const uint8_t* rawBytes = nullptr;
    size_t rawBytesLength = 0;
    bool sawVarintValue = false;
    bool sawFixed32 = false;
    bool sawFixed64 = false;
    bool sawBytes = false;
    bool sawBoolean = false;
    bool sawDatatype = false;

    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;

        switch (field) {
            case MQ_SP_F_METRIC_NAME: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                const uint8_t* text = nullptr;
                size_t textLength = 0;
                if (!reader.lengthDelimited(text, textLength)) break;
                out.name = owner.intern(text, textLength);
                if (out.name == nullptr) {
                    reader.fail(MqttSparkplugError::TooManyMetrics);
                }
                break;
            }
            case MQ_SP_F_METRIC_ALIAS:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.alias = reader.varint();
                out.hasAlias = true;
                break;
            case MQ_SP_F_METRIC_TIMESTAMP:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.timestamp = reader.varint();
                out.hasTimestamp = true;
                break;
            case MQ_SP_F_METRIC_DATATYPE:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.value.type = (MqttSparkplugDataType)(uint32_t)reader.varint();
                sawDatatype = true;
                break;
            case MQ_SP_F_METRIC_IS_HISTORICAL:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.isHistorical = reader.varint() != 0;
                break;
            case MQ_SP_F_METRIC_IS_TRANSIENT:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.isTransient = reader.varint() != 0;
                break;
            case MQ_SP_F_METRIC_IS_NULL:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.value.isNull = reader.varint() != 0;
                break;
            case MQ_SP_F_METRIC_PROPERTIES: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                const uint8_t* setBody = nullptr;
                size_t setLength = 0;
                if (!reader.lengthDelimited(setBody, setLength)) break;

                // PropertySet is two parallel repeated fields, keys then
                // values, matched by index. They are read in one pass and
                // paired as they arrive, which assumes an encoder emits them
                // interleaved or keys-then-values consistently - both of which
                // index-match correctly because each is counted separately.
                PbReader setReader(setBody, setLength);
                size_t keyCount = 0;
                size_t valueCount = 0;
                while (setReader.ok() && !setReader.atEnd()) {
                    uint32_t setField = 0;
                    uint32_t setWire = 0;
                    if (!readTag(setReader, setField, setWire)) break;
                    if (setField == MQ_SP_F_PROPERTYSET_KEYS &&
                        setWire == MQ_PB_LENGTH) {
                        const uint8_t* key = nullptr;
                        size_t keyLength = 0;
                        if (!setReader.lengthDelimited(key, keyLength)) break;
                        if (keyCount >= MQ_SP_MAX_PROPERTIES) {
                            setReader.fail(MqttSparkplugError::TooManyMetrics);
                            break;
                        }
                        out.properties[keyCount].key =
                            owner.intern(key, keyLength);
                        if (out.properties[keyCount].key == nullptr) {
                            setReader.fail(MqttSparkplugError::TooManyMetrics);
                            break;
                        }
                        keyCount++;
                    } else if (setField == MQ_SP_F_PROPERTYSET_VALUES &&
                               setWire == MQ_PB_LENGTH) {
                        const uint8_t* valueBody = nullptr;
                        size_t valueLength = 0;
                        if (!setReader.lengthDelimited(valueBody, valueLength)) break;
                        if (valueCount >= MQ_SP_MAX_PROPERTIES) {
                            setReader.fail(MqttSparkplugError::TooManyMetrics);
                            break;
                        }
                        MqttSparkplugError valueError = MqttSparkplugError::None;
                        if (!decodePropertyValue(valueBody, valueLength,
                                                 out.properties[valueCount],
                                                 valueError)) {
                            setReader.fail(valueError);
                            break;
                        }
                        valueCount++;
                    } else {
                        setReader.skip(setWire);
                    }
                }
                if (!setReader.ok()) {
                    reader.fail(setReader.error);
                    break;
                }
                // A PropertySet with a different number of keys and values is
                // malformed. Taking the smaller would silently mis-pair every
                // property after the mismatch.
                out.propertyCount = keyCount < valueCount ? keyCount : valueCount;
                break;
            }
            case MQ_SP_F_METRIC_INT:
            case MQ_SP_F_METRIC_LONG:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawVarint = reader.varint();
                sawVarintValue = true;
                break;
            case MQ_SP_F_METRIC_BOOLEAN:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawVarint = reader.varint();
                sawBoolean = true;
                break;
            case MQ_SP_F_METRIC_FLOAT:
                if (wireType != MQ_PB_FIXED32) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawFixed32 = reader.fixed32();
                sawFixed32 = true;
                break;
            case MQ_SP_F_METRIC_DOUBLE:
                if (wireType != MQ_PB_FIXED64) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                rawFixed64 = reader.fixed64();
                sawFixed64 = true;
                break;
            case MQ_SP_F_METRIC_STRING:
            case MQ_SP_F_METRIC_BYTES: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                const uint8_t* text = nullptr;
                size_t textLength = 0;
                if (!reader.lengthDelimited(text, textLength)) break;
                rawBytes = text;
                rawBytesLength = textLength;
                sawBytes = true;
                break;
            }
            case MQ_SP_F_METRIC_DATASET:
            case MQ_SP_F_METRIC_TEMPLATE:
                // Present in the schema, not carried by this build. Skipped,
                // and the metric marked below - valueUnsupported, and NULL,
                // never a zero. A value that silently read as zero is the
                // fault that survives commissioning; a metric marked as having
                // no value this library can show is not that fault.
                reader.skip(wireType);
                break;
            default:
                reader.skip(wireType);
                break;
        }
    }

    if (!reader.ok()) {
        error = reader.error;
        return false;
    }

    // No datatype - the usual case for data and commands. A provisional one,
    // from the field the value arrived in, until the declared one is applied.
    if (!sawDatatype) {
        out.datatypeAbsent = true;
        out.value.type = sawBoolean ? MqttSparkplugDataType::Boolean
                       : sawVarintValue ? MqttSparkplugDataType::UInt64
                       : sawFixed32 ? MqttSparkplugDataType::Float
                       : sawFixed64 ? MqttSparkplugDataType::Double
                       : sawBytes ? MqttSparkplugDataType::Bytes
                       : MqttSparkplugDataType::Unknown;
        if (out.value.type == MqttSparkplugDataType::Unknown) out.value.isNull = true;
    }

    // Now the datatype is known, interpret whichever value field arrived.
    if (!out.value.isNull) {
        switch (out.value.type) {
            case MqttSparkplugDataType::Int8:
            case MqttSparkplugDataType::Int16:
            case MqttSparkplugDataType::Int32:
            case MqttSparkplugDataType::Int64:
                if (!sawVarintValue) break;
                out.value.as.signedValue =
                    decodeSigned(out.value.type, rawVarint);
                break;
            case MqttSparkplugDataType::UInt8:
            case MqttSparkplugDataType::UInt16:
            case MqttSparkplugDataType::UInt32:
            case MqttSparkplugDataType::UInt64:
            case MqttSparkplugDataType::DateTime:
                if (!sawVarintValue) break;
                out.value.as.unsignedValue = rawVarint;
                break;
            case MqttSparkplugDataType::Boolean:
                if (!sawBoolean) break;
                out.value.as.boolean = rawVarint != 0;
                break;
            case MqttSparkplugDataType::Float:
                if (!sawFixed32) break;
                out.value.as.float32 = floatOfBits(rawFixed32);
                break;
            case MqttSparkplugDataType::Double:
                if (!sawFixed64) break;
                out.value.as.float64 = doubleOfBits(rawFixed64);
                break;
            case MqttSparkplugDataType::String:
            case MqttSparkplugDataType::Text:
            case MqttSparkplugDataType::UUID:
            case MqttSparkplugDataType::Bytes:
            case MqttSparkplugDataType::File:
                if (!sawBytes) break;
                out.value.as.bytes.data = rawBytes;
                out.value.as.bytes.length = (uint32_t)rawBytesLength;
                break;
            default:
                // A datatype outside what this build carries. The metric parsed
                // cleanly and cannot be represented: kept, without a value.
                out.valueUnsupported = true;
                out.value.isNull = true;
                out.value.as.unsignedValue = 0;
                break;
        }
    }

    return true;
}

}  // namespace

// --- The public decode ---
//
// Writes into a caller-owned payload rather than returning one, and the reason
// is the name arena: the decoded names live inside `out`, so `out` has to exist
// before the decode and outlive the result. A function that returned a payload
// by value would copy 8 kB per message on a board.
MqttSparkplugError mq_sparkplugDecode(const uint8_t* in, size_t length,
                                      MqttSparkplugPayload& out) {
    out.clear();

    // ZERO BYTES IS A VALID, EMPTY PAYLOAD - and a null pointer with zero
    // length is the same thing said differently, because std::vector::data()
    // on an empty vector may return null.
    //
    // This returned Truncated, which made an empty NDATA - a legal message,
    // and what a node with nothing to report would send - look like a broken
    // one. Found by the reverse half of the docker cross-check: another
    // implementation re-encoded our empty payload to zero bytes and we then
    // refused our own fixture.
    if (length == 0) return MqttSparkplugError::None;
    if (in == nullptr) return MqttSparkplugError::Truncated;

    PbReader reader(in, length);
    MqttSparkplugError error = MqttSparkplugError::None;

    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;

        switch (field) {
            case MQ_SP_F_PAYLOAD_TIMESTAMP:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.timestamp = reader.varint();
                out.hasTimestamp = true;
                break;
            case MQ_SP_F_PAYLOAD_SEQ:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.seq = reader.varint();
                out.hasSeq = true;
                break;
            case MQ_SP_F_PAYLOAD_UUID: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                const uint8_t* text = nullptr;
                size_t textLength = 0;
                if (!reader.lengthDelimited(text, textLength)) break;
                out.uuid = out.intern(text, textLength);
                if (out.uuid == nullptr) {
                    reader.fail(MqttSparkplugError::TooManyMetrics);
                }
                break;
            }
            case MQ_SP_F_PAYLOAD_BODY: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                const uint8_t* blob = nullptr;
                size_t blobLength = 0;
                if (!reader.lengthDelimited(blob, blobLength)) break;
                // A span into `in`, not a copy. The body is opaque and can be
                // large, and copying it would defeat the point of having it.
                out.body = blob;
                out.bodyLength = (uint32_t)blobLength;
                break;
            }
            case MQ_SP_F_PAYLOAD_METRICS: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                const uint8_t* body = nullptr;
                size_t bodyLength = 0;
                if (!reader.lengthDelimited(body, bodyLength)) break;
                if (out.metricCount >= out.capacity()) {
                    reader.fail(MqttSparkplugError::TooManyMetrics);
                    break;
                }
                MqttSparkplugMetric metric;
                if (!decodeMetric(body, bodyLength, metric, out, error)) {
                    reader.fail(error);
                    break;
                }
                out.metrics[out.metricCount++] = metric;
                break;
            }
            default:
                // Payload carries `extensions 6 to max`, so an unknown field
                // here is a future Sparkplug or a vendor extension and must be
                // skipped rather than refused.
                reader.skip(wireType);
                break;
        }
    }

    if (!reader.ok()) return reader.error;
    return MqttSparkplugError::None;
}

// =============================================================================
// THE STATE PAYLOAD
// =============================================================================

// --- The STATE payload ---
//
// [tck-id-payloads-state-will-message-payload] The Death Certificate Payload
// MUST be JSON UTF-8 data. It MUST include two key/value pairs where one key
// MUST be online and it's value is a boolean false. The other key MUST be
// timestamp and the value MUST be a numeric value representing the current UTC
// time in milliseconds since Epoch.
//
// TWO KEYS AND TWO VALUE TYPES, so this is fifty lines rather than a JSON
// dependency. Nothing else in this library parses JSON and acquiring a parser
// to read one flat object would be a poor trade - especially as the parser
// would then be on the network-facing path and need its own fuzzer.
//
// Deliberately tolerant of whitespace and key order, and deliberately NOT
// tolerant of anything else: a payload that is not this shape is not a STATE
// message and is ignored rather than guessed at.
bool mq_sparkplugParseState(const uint8_t* data, size_t length,
                    bool& online, uint64_t& timestamp) {
    if (data == nullptr || length == 0) return false;

    bool sawOnline = false;
    bool sawTimestamp = false;
    online = false;
    timestamp = 0;

    size_t index = 0;
    while (index < length) {
        // Find the next quoted key.
        while (index < length && data[index] != '"') index++;
        if (index >= length) break;
        index++;

        const size_t keyStart = index;
        while (index < length && data[index] != '"') index++;
        if (index >= length) return false;
        const size_t keyLength = index - keyStart;
        index++;

        // Skip to the value.
        while (index < length && (data[index] == ' ' || data[index] == ':' ||
                                  data[index] == '\t')) {
            index++;
        }
        if (index >= length) return false;

        if (keyLength == 6 && memcmp(data + keyStart, "online", 6) == 0) {
            if (length - index >= 4 && memcmp(data + index, "true", 4) == 0) {
                online = true;
                sawOnline = true;
                index += 4;
            } else if (length - index >= 5 &&
                       memcmp(data + index, "false", 5) == 0) {
                online = false;
                sawOnline = true;
                index += 5;
            } else {
                // The specification says boolean. A 0 or 1 here would be a
                // different document, and treating it as one would mean
                // accepting a payload a conformant host would reject.
                return false;
            }
        } else if (keyLength == 9 &&
                   memcmp(data + keyStart, "timestamp", 9) == 0) {
            uint64_t value = 0;
            bool any = false;
            while (index < length && data[index] >= '0' && data[index] <= '9') {
                // Milliseconds since epoch fits in 41 bits until the year 71000,
                // so the only overflow reachable here is a hostile payload of
                // twenty digits - refused rather than wrapped.
                if (value > (uint64_t)0xFFFFFFFFFFFFFFFFull / 10) return false;
                value = value * 10 + (uint64_t)(data[index] - '0');
                index++;
                any = true;
            }
            if (!any) return false;
            timestamp = value;
            sawTimestamp = true;
        } else {
            // An unknown key. Skipped rather than refused - the specification
            // says the payload MUST include these two, not that it may include
            // nothing else.
            while (index < length && data[index] != ',' && data[index] != '}') {
                index++;
            }
        }
    }

    return sawOnline && sawTimestamp;
}

// The other direction, for a Host Application's own certificates. Fixed
// spelling, because [tck-id-payloads-state-birth-payload] requires the birth's
// timestamp to MATCH the will's exactly, and a formatter that rendered the same
// number differently in two places would break that.
size_t mq_sparkplugWriteState(char* out, size_t capacity, bool online,
                      uint64_t timestamp) {
    if (out == nullptr || capacity < 48) return 0;

    // Hand-rolled rather than snprintf: %llu is not available under every
    // toolchain this library builds with, and an Arduino's newlib prints a
    // uint64 as "0" without warning.
    char digits[24];
    size_t digitCount = 0;
    uint64_t value = timestamp;
    do {
        digits[digitCount++] = (char)('0' + (value % 10));
        value /= 10;
    } while (value > 0 && digitCount < sizeof digits);

    size_t cursor = 0;
    const char* head = online ? "{\"online\":true,\"timestamp\":"
                              : "{\"online\":false,\"timestamp\":";
    const size_t headLength = strlen(head);
    if (headLength + digitCount + 2 > capacity) return 0;
    memcpy(out, head, headLength);
    cursor = headLength;
    while (digitCount > 0) out[cursor++] = digits[--digitCount];
    out[cursor++] = '}';
    out[cursor] = '\0';
    return cursor;
}

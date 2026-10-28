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

// The value fields alone. PropertyValue and Template.Parameter number them
// the same, 3 to 8, so one sizer and one writer serve both; a nested
// PropertySet or PropertySetList (fields 9 and 10) only a PropertyValue has.
size_t propertyScalarSize(const MqttSparkplugProperty& property,
                          MqttSparkplugError& error) {
    size_t total = 0;
    switch (property.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_INT);
            total += mq_pbVarintSize(isSignedType(property.type)
                                         ? encodeSigned(property.type,
                                                        property.as.signedValue)
                                         : property.as.unsignedValue);
            break;
        // UInt32 in long_value, as everywhere: see mq_sparkplugValueField().
        case MqttSparkplugDataType::UInt32:
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
        case MqttSparkplugDataType::PropertySet:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_PROPERTYSET) +
                     mq_pbVarintSize(property.as.text.length) +
                     property.as.text.length;
            break;
        case MqttSparkplugDataType::PropertySetList:
            total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_PROPERTYSETS) +
                     mq_pbVarintSize(property.as.text.length) +
                     property.as.text.length;
            break;
        default:
            error = MqttSparkplugError::DataTypeNotSupported;
            return 0;
    }
    return total;
}

size_t propertyValueBodySize(const MqttSparkplugProperty& property,
                             MqttSparkplugError& error) {
    size_t total = 0;
    total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_TYPE) +
             mq_pbVarintSize((uint64_t)property.type);

    if (property.isNull) {
        total += mq_pbTagSize(MQ_SP_F_PROPERTYVALUE_IS_NULL) + 1;
        return total;
    }
    const size_t scalar = propertyScalarSize(property, error);
    if (error != MqttSparkplugError::None) return 0;
    return total + scalar;
}

void writePropertyScalar(PbWriter& writer,
                         const MqttSparkplugProperty& property) {
    switch (property.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
            writer.tag(MQ_SP_F_PROPERTYVALUE_INT, MQ_PB_VARINT);
            writer.varint(isSignedType(property.type)
                              ? encodeSigned(property.type,
                                             property.as.signedValue)
                              : property.as.unsignedValue);
            break;
        case MqttSparkplugDataType::UInt32:
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
        case MqttSparkplugDataType::PropertySet:
            writer.tag(MQ_SP_F_PROPERTYVALUE_PROPERTYSET, MQ_PB_LENGTH);
            writer.bytes(property.as.text.data, property.as.text.length);
            break;
        case MqttSparkplugDataType::PropertySetList:
            writer.tag(MQ_SP_F_PROPERTYVALUE_PROPERTYSETS, MQ_PB_LENGTH);
            writer.bytes(property.as.text.data, property.as.text.length);
            break;
        default:
            break;
    }
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
    writePropertyScalar(writer, property);
}

// The PropertySet wrapping a metric's properties. Two parallel repeated
// fields, keys then values, index-matched.
size_t propertySetBodySize(const MqttSparkplugProperty* properties,
                           size_t count, MqttSparkplugError& error) {
    size_t total = 0;
    for (size_t index = 0; index < count; index++) {
        const MqttSparkplugProperty& property = properties[index];
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

// Every key, then every value: field order, as every generated encoder writes
// a message's repeated fields - so the bytes compare equal with theirs. (They
// were interleaved, key and value; just as valid, and caught only when a
// PropertySet was compared byte for byte against Eclipse Tahu's.)
void writePropertySet(PbWriter& writer, const MqttSparkplugProperty* properties,
                      size_t count) {
    for (size_t index = 0; index < count; index++) {
        writer.tag(MQ_SP_F_PROPERTYSET_KEYS, MQ_PB_LENGTH);
        writer.text(properties[index].key);
    }
    for (size_t index = 0; index < count; index++) {
        const MqttSparkplugProperty& property = properties[index];
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
            default:
                // Text, bytes, an array, a dataset, a template: a span, as
                // its own bytes - a DataSet's or Template's are the encoded
                // nested message, written as it is.
                if (!metric.value.isSpan()) {
                    error = MqttSparkplugError::DataTypeNotSupported;
                    return 0;
                }
                total += mq_pbTagSize(field) +
                         mq_pbVarintSize(metric.value.as.bytes.length) +
                         metric.value.as.bytes.length;
                break;
        }
    }

    if (metric.propertyCount > 0) {
        const size_t setBody =
            propertySetBodySize(metric.properties, metric.propertyCount, error);
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
        const size_t setBody =
            propertySetBodySize(metric.properties, metric.propertyCount, ignored);
        writer.tag(MQ_SP_F_METRIC_PROPERTIES, MQ_PB_LENGTH);
        writer.varint(setBody);
        writePropertySet(writer, metric.properties, metric.propertyCount);
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
        default:
            if (!metric.value.isSpan()) break;
            writer.tag(field, MQ_PB_LENGTH);
            writer.bytes(metric.value.as.bytes.data,
                         metric.value.as.bytes.length);
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

// A Template.Parameter too, when `parameterNames` is given: its fields 1 and 2
// are the name (interned there) and the type, where a PropertyValue's are the
// type and is_null; the value fields, 3 to 8, are the same. A parameter with
// no value is null.
bool decodePropertyValue(const uint8_t* body, size_t bodyLength,
                         MqttSparkplugProperty& out,
                         MqttSparkplugError& error,
                         MqttSparkplugPayload* parameterNames = nullptr) {
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
    const uint8_t* rawSet = nullptr;
    size_t rawSetLength = 0;
    bool sawSet = false;

    // A SLOT IS REUSED, SO IT STARTS EMPTY. The caller's array is the last
    // message's: a value whose type field was absent kept the previous
    // occupant's type, and one without is_null stayed null because the one
    // before it had been - a property read as somebody else's. The key is
    // kept for a PropertyValue (decodePropertySet reads the keys into the
    // same slots, before or after the values) and cleared for a parameter,
    // whose name is one of its own fields.
    out.type = MqttSparkplugDataType::Unknown;
    out.isNull = false;
    out.as.unsignedValue = 0;
    if (parameterNames != nullptr) out.key = nullptr;

    PbReader reader(body, bodyLength);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;

        if (parameterNames != nullptr && field == MQ_SP_F_PARAMETER_NAME) {
            const uint8_t* name = nullptr;
            size_t nameLength = 0;
            if (wireType != MQ_PB_LENGTH) {
                reader.fail(MqttSparkplugError::WireTypeMismatch);
            } else if (reader.lengthDelimited(name, nameLength)) {
                out.key = parameterNames->intern(name, nameLength);
                if (out.key == nullptr) reader.fail(MqttSparkplugError::TooManyMetrics);
            }
            continue;
        }
        if (parameterNames != nullptr && field == MQ_SP_F_PARAMETER_TYPE) {
            if (wireType != MQ_PB_VARINT) {
                reader.fail(MqttSparkplugError::WireTypeMismatch);
            } else {
                out.type = (MqttSparkplugDataType)(uint32_t)reader.varint();
            }
            continue;
        }

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
            case MQ_SP_F_PROPERTYVALUE_PROPERTYSET:
            case MQ_SP_F_PROPERTYVALUE_PROPERTYSETS:
                // The nested set (or list of sets) as its bytes, as a
                // template's: mq_sparkplugDecodePropertySet reads it.
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                if (!reader.lengthDelimited(rawSet, rawSetLength)) break;
                sawSet = true;
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

    if (parameterNames != nullptr && !sawVarint && !sawBoolean && !sawFixed32 &&
        !sawFixed64 && !sawText) {
        out.isNull = true;
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
            case MqttSparkplugDataType::PropertySet:
            case MqttSparkplugDataType::PropertySetList:
                out.as.text.data = sawSet ? rawSet : nullptr;
                out.as.text.length = sawSet ? (uint32_t)rawSetLength : 0;
                break;
            default:
                break;
        }
    }
    return true;
}

// A PropertySet: two parallel repeated fields, keys then values, matched by
// index. They are read in one pass and paired as they arrive, which assumes an
// encoder emits them interleaved or keys-then-values consistently - both of
// which index-match correctly because each is counted separately. Keys are
// interned into `owner`.
//
// `dropped`, when given, is where a set larger than `capacity` is counted: the
// first `capacity` pairs are kept and the rest are read - every value still
// checked, so a malformed one is refused as before - and counted rather than
// failing the set. A metric's own properties go this way (see
// MqttSparkplugPayload::propertiesDropped); a nested set the caller asked for
// into an array of its own size is refused when it does not fit, as it was.
bool decodePropertySet(const uint8_t* setBody, size_t setLength,
                       MqttSparkplugProperty* properties, size_t capacity,
                       size_t& count, MqttSparkplugPayload& owner,
                       MqttSparkplugError& error, uint32_t* dropped = nullptr) {
    count = 0;
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
            if (keyCount >= capacity) {
                if (dropped == nullptr) {
                    setReader.fail(MqttSparkplugError::TooManyMetrics);
                    break;
                }
                keyCount++;   // read, not kept
                continue;
            }
            properties[keyCount].key = owner.intern(key, keyLength);
            if (properties[keyCount].key == nullptr) {
                setReader.fail(MqttSparkplugError::TooManyMetrics);
                break;
            }
            keyCount++;
        } else if (setField == MQ_SP_F_PROPERTYSET_VALUES &&
                   setWire == MQ_PB_LENGTH) {
            const uint8_t* valueBody = nullptr;
            size_t valueLength = 0;
            if (!setReader.lengthDelimited(valueBody, valueLength)) break;
            MqttSparkplugProperty overflow;
            MqttSparkplugProperty* into = &overflow;
            if (valueCount < capacity) {
                into = &properties[valueCount];
            } else if (dropped == nullptr) {
                setReader.fail(MqttSparkplugError::TooManyMetrics);
                break;
            }
            MqttSparkplugError valueError = MqttSparkplugError::None;
            if (!decodePropertyValue(valueBody, valueLength, *into,
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
        error = setReader.error;
        return false;
    }
    // A PropertySet with a different number of keys and values is
    // malformed. Taking the smaller would silently mis-pair every
    // property after the mismatch.
    const size_t pairs = keyCount < valueCount ? keyCount : valueCount;
    count = pairs < capacity ? pairs : capacity;
    if (dropped != nullptr && pairs > count) {
        const size_t lost = pairs - count;
        *dropped = lost > 0xFFFFFFFFu - *dropped ? 0xFFFFFFFFu
                                                 : *dropped + (uint32_t)lost;
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
        case MqttSparkplugDataType::DataSet:
        case MqttSparkplugDataType::Template:
            // Their own fields, so the provisional type is already theirs.
            fits = arrived == declared;
            break;
        default:
            fits = mq_sparkplugIsArrayType(declared) &&
                   arrived == MqttSparkplugDataType::Bytes;
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
            return MQ_SP_F_METRIC_INT;
        // UINT32 IN long_value. The specification's table says only "Google
        // Protocol Buffer Type: uint32", which read as the uint32 field
        // int_value - and that is what this wrote until the Eclipse TCK
        // failed it: its Utils.hasValue() takes a UInt32 from long_value
        // alone, as Eclipse Tahu's Java and C libraries and the npm
        // sparkplug-payload (Node-RED's) all write it. sparkplug-payload
        // also reads it from there alone, so our UInt32 arrived as null in
        // Node-RED. Tahu's Python writes int_value; every decoder here takes
        // either field, so both still read.
        case MqttSparkplugDataType::UInt32:
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
        case MqttSparkplugDataType::DataSet:
            return MQ_SP_F_METRIC_DATASET;
        case MqttSparkplugDataType::Template:
            return MQ_SP_F_METRIC_TEMPLATE;
        default:
            // Section 6.4.17: every array travels in bytes_value.
            return mq_sparkplugIsArrayType(type) ? MQ_SP_F_METRIC_BYTES : 0;
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
    propertiesDropped = 0;
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

bool MqttSparkplugPayload::resize(size_t metricCapacity, size_t nameBytes) {
    if (metricCapacity == _metricCapacity && nameBytes == _nameCapacity) {
        clear();
        return true;
    }
    // The new storage first, as reserve(): a failure leaves this as it was.
    MqttSparkplugPayload sized(metricCapacity, nameBytes);
    if (sized._metricCapacity < metricCapacity || sized._nameCapacity < nameBytes) {
        return false;
    }
    release();
    metrics = sized.metrics;
    names = sized.names;
    _metricCapacity = sized._metricCapacity;
    _nameCapacity = sized._nameCapacity;
    sized.metrics = nullptr;
    sized.names = nullptr;
    sized._metricCapacity = 0;
    sized._nameCapacity = 0;
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
    propertiesDropped = other.propertiesDropped;

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
    // A DataSet's or Template's nested message, kept as its bytes.
    const uint8_t* rawNested = nullptr;
    size_t rawNestedLength = 0;
    bool sawDataSet = false;
    bool sawTemplate = false;

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

                MqttSparkplugError setError = MqttSparkplugError::None;
                if (!decodePropertySet(setBody, setLength, out.properties,
                                       MQ_SP_MAX_PROPERTIES, out.propertyCount,
                                       owner, setError,
                                       &owner.propertiesDropped)) {
                    reader.fail(setError);
                }
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
            case MQ_SP_F_METRIC_TEMPLATE: {
                // The nested message, as its bytes: read it with
                // MqttSparkplugDataSetReader or mq_sparkplugDecodeTemplate.
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                if (!reader.lengthDelimited(rawNested, rawNestedLength)) break;
                sawDataSet = field == MQ_SP_F_METRIC_DATASET;
                sawTemplate = field == MQ_SP_F_METRIC_TEMPLATE;
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

    // No datatype - the usual case for data and commands. A provisional one,
    // from the field the value arrived in, until the declared one is applied.
    if (!sawDatatype) {
        out.datatypeAbsent = true;
        out.value.type = sawBoolean ? MqttSparkplugDataType::Boolean
                       : sawVarintValue ? MqttSparkplugDataType::UInt64
                       : sawFixed32 ? MqttSparkplugDataType::Float
                       : sawFixed64 ? MqttSparkplugDataType::Double
                       : sawBytes ? MqttSparkplugDataType::Bytes
                       : sawDataSet ? MqttSparkplugDataType::DataSet
                       : sawTemplate ? MqttSparkplugDataType::Template
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
            case MqttSparkplugDataType::DataSet:
                if (!sawDataSet) break;
                out.value.as.bytes.data = rawNested;
                out.value.as.bytes.length = (uint32_t)rawNestedLength;
                break;
            case MqttSparkplugDataType::Template:
                if (!sawTemplate) break;
                out.value.as.bytes.data = rawNested;
                out.value.as.bytes.length = (uint32_t)rawNestedLength;
                break;
            default:
                if (mq_sparkplugIsArrayType(out.value.type)) {
                    if (!sawBytes) break;
                    out.value.as.bytes.data = rawBytes;
                    out.value.as.bytes.length = (uint32_t)rawBytesLength;
                    break;
                }
                // Unknown, or a property's type (PropertySet): no metric can
                // carry it. The metric parsed cleanly: kept, without a value.
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

// --- Measuring before decoding ---
//
// The same walk as the decode, reading only what it would intern: each
// metric's name, each property key at a metric's top level, and the uuid. A
// field the decode would refuse is refused here with the same error, so a
// caller that measures and then decodes sees one answer.
MqttSparkplugError mq_sparkplugMeasure(const uint8_t* in, size_t length,
                                       size_t& metrics, size_t& nameBytes) {
    metrics = 0;
    nameBytes = 0;
    if (length == 0) return MqttSparkplugError::None;
    if (in == nullptr) return MqttSparkplugError::Truncated;

    PbReader reader(in, length);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        if (field == MQ_SP_F_PAYLOAD_UUID && wireType == MQ_PB_LENGTH) {
            const uint8_t* text = nullptr;
            size_t textLength = 0;
            if (!reader.lengthDelimited(text, textLength)) break;
            nameBytes += textLength + 1;
            continue;
        }
        if (field != MQ_SP_F_PAYLOAD_METRICS || wireType != MQ_PB_LENGTH) {
            reader.skip(wireType);
            continue;
        }
        const uint8_t* body = nullptr;
        size_t bodyLength = 0;
        if (!reader.lengthDelimited(body, bodyLength)) break;
        metrics++;

        PbReader metric(body, bodyLength);
        while (metric.ok() && !metric.atEnd()) {
            if (!readTag(metric, field, wireType)) break;
            const uint8_t* inner = nullptr;
            size_t innerLength = 0;
            if (field == MQ_SP_F_METRIC_NAME && wireType == MQ_PB_LENGTH) {
                if (!metric.lengthDelimited(inner, innerLength)) break;
                nameBytes += innerLength + 1;
            } else if (field == MQ_SP_F_METRIC_PROPERTIES &&
                       wireType == MQ_PB_LENGTH) {
                if (!metric.lengthDelimited(inner, innerLength)) break;
                PbReader set(inner, innerLength);
                while (set.ok() && !set.atEnd()) {
                    if (!readTag(set, field, wireType)) break;
                    const uint8_t* key = nullptr;
                    size_t keyLength = 0;
                    if (field == MQ_SP_F_PROPERTYSET_KEYS &&
                        wireType == MQ_PB_LENGTH) {
                        if (!set.lengthDelimited(key, keyLength)) break;
                        nameBytes += keyLength + 1;
                    } else {
                        set.skip(wireType);
                    }
                }
                if (!set.ok()) return set.error;
            } else {
                metric.skip(wireType);
            }
        }
        if (!metric.ok()) return metric.error;
    }
    return reader.ok() ? MqttSparkplugError::None : reader.error;
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

    // A value ends at whitespace, a comma or the closing brace. Checked after
    // each token, so "truex" is not true and "12abc" is not twelve - both
    // were, which let a payload no JSON parser accepts steer a node's birth.
    auto endsValue = [&](size_t at) {
        return at >= length || data[at] == ',' || data[at] == '}' ||
               data[at] == ' ' || data[at] == '\t' || data[at] == '\r' ||
               data[at] == '\n';
    };

    size_t index = 0;
    while (index < length) {
        // Find the next quoted key.
        while (index < length && data[index] != '"') index++;
        if (index >= length) break;
        index++;

        const size_t keyStart = index;
        while (index < length && data[index] != '"') {
            if (data[index] == '\\') index++;   // an escaped character
            index++;
        }
        if (index >= length) return false;
        const size_t keyLength = index - keyStart;
        index++;

        // Skip to the value: whitespace and exactly one colon.
        bool colon = false;
        while (index < length && (data[index] == ' ' || data[index] == ':' ||
                                  data[index] == '\t' || data[index] == '\r' ||
                                  data[index] == '\n')) {
            if (data[index] == ':') {
                if (colon) return false;
                colon = true;
            }
            index++;
        }
        if (!colon || index >= length) return false;

        if (keyLength == 6 && memcmp(data + keyStart, "online", 6) == 0) {
            if (length - index >= 4 && memcmp(data + index, "true", 4) == 0 &&
                endsValue(index + 4)) {
                online = true;
                sawOnline = true;
                index += 4;
            } else if (length - index >= 5 &&
                       memcmp(data + index, "false", 5) == 0 &&
                       endsValue(index + 5)) {
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
                // Milliseconds since epoch fits in 41 bits until the year
                // 71000, so the only overflow reachable here is a hostile
                // payload of twenty digits - refused rather than wrapped. The
                // digit is part of the test: 18446744073709551616 passed the
                // old one (value at the limit, times ten, plus six) and read
                // as ZERO, an offline certificate older than any online.
                const uint64_t digit = (uint64_t)(data[index] - '0');
                if (value > (0xFFFFFFFFFFFFFFFFull - digit) / 10) return false;
                value = value * 10 + digit;
                index++;
                any = true;
            }
            if (!any || !endsValue(index)) return false;
            timestamp = value;
            sawTimestamp = true;
        } else {
            // An unknown key. Skipped rather than refused - the specification
            // says the payload MUST include these two, not that it may include
            // nothing else. A string value is skipped as a string, escapes and
            // all: its commas and quotes are not the document's, and a quoted
            // "online" inside one is not a key.
            if (data[index] == '"') {
                index++;
                while (index < length && data[index] != '"') {
                    if (data[index] == '\\') index++;
                    index++;
                }
                if (index >= length) return false;
                index++;
            } else if (data[index] == '{' || data[index] == '[') {
                // A nested value: not this document's shape at all.
                return false;
            }
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

// =============================================================================
// TEMPLATES, DATASETS, AND PROPERTY SETS AS VALUES
// =============================================================================

namespace {

// The specification's "basic types": what a DataSet column and a Template
// parameter may be.
bool isBasicType(MqttSparkplugDataType type) {
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
        case MqttSparkplugDataType::Boolean:
        case MqttSparkplugDataType::String:
        case MqttSparkplugDataType::DateTime:
        case MqttSparkplugDataType::Text:
            return true;
        default:
            return false;
    }
}

size_t textFieldSize(uint32_t field, const char* text) {
    const size_t length = text ? strlen(text) : 0;
    return mq_pbTagSize(field) + mq_pbVarintSize(length) + length;
}

// --- Template ---

size_t parameterBodySize(const MqttSparkplugProperty& parameter,
                         MqttSparkplugError& error) {
    // [tck-id-payloads-template-parameter-name-required]
    if (parameter.key == nullptr) {
        error = MqttSparkplugError::InvalidName;
        return 0;
    }
    // [tck-id-payloads-template-parameter-type-value]
    if (!isBasicType(parameter.type)) {
        error = MqttSparkplugError::DataTypeNotSupported;
        return 0;
    }
    size_t total = textFieldSize(MQ_SP_F_PARAMETER_NAME, parameter.key) +
                   mq_pbTagSize(MQ_SP_F_PARAMETER_TYPE) +
                   mq_pbVarintSize((uint64_t)parameter.type);
    if (!parameter.isNull) {
        const size_t scalar = propertyScalarSize(parameter, error);
        if (error != MqttSparkplugError::None) return 0;
        total += scalar;
    }
    return total;
}

size_t templateBodySize(const MqttSparkplugTemplate& value,
                        MqttSparkplugError& error) {
    if (value.isDefinition == (value.templateRef != nullptr)) {
        error = MqttSparkplugError::TemplateReference;
        return 0;
    }
    size_t total = 0;
    if (value.version != nullptr) {
        total += textFieldSize(MQ_SP_F_TEMPLATE_VERSION, value.version);
    }
    for (size_t index = 0; index < value.members.metricCount; index++) {
        const MqttSparkplugMetric& member = value.members.metrics[index];
        if (member.name == nullptr) {
            error = MqttSparkplugError::InvalidName;
            return 0;
        }
        const size_t body = metricBodySize(member, MqttSparkplugType::NBirth, error);
        if (error != MqttSparkplugError::None) return 0;
        total += mq_pbTagSize(MQ_SP_F_TEMPLATE_METRICS) + mq_pbVarintSize(body) + body;
    }
    for (size_t index = 0; index < value.parameterCount; index++) {
        const size_t body = parameterBodySize(value.parameters[index], error);
        if (error != MqttSparkplugError::None) return 0;
        total += mq_pbTagSize(MQ_SP_F_TEMPLATE_PARAMETERS) + mq_pbVarintSize(body) + body;
    }
    if (value.templateRef != nullptr) {
        total += textFieldSize(MQ_SP_F_TEMPLATE_REF, value.templateRef);
    }
    // Always, true or false: [tck-id-payloads-template-is-definition].
    total += mq_pbTagSize(MQ_SP_F_TEMPLATE_IS_DEFINITION) + 1;
    return total;
}

// --- DataSet ---

// A cell's DataSetValue body: its one value field, or nothing for a null.
size_t cellBodySize(const MqttSparkplugValue& cell) {
    if (cell.isNull) return 0;
    switch (cell.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
            return mq_pbTagSize(MQ_SP_F_DATASETVALUE_INT) +
                   mq_pbVarintSize(encodeSigned(cell.type, cell.as.signedValue));
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
            return mq_pbTagSize(MQ_SP_F_DATASETVALUE_INT) +
                   mq_pbVarintSize(cell.as.unsignedValue);
        case MqttSparkplugDataType::UInt32:   // long_value: mq_sparkplugValueField()
        case MqttSparkplugDataType::Int64:
        case MqttSparkplugDataType::UInt64:
        case MqttSparkplugDataType::DateTime:
            return mq_pbTagSize(MQ_SP_F_DATASETVALUE_LONG) +
                   mq_pbVarintSize(cell.as.unsignedValue);
        case MqttSparkplugDataType::Float:
            return mq_pbTagSize(MQ_SP_F_DATASETVALUE_FLOAT) + 4;
        case MqttSparkplugDataType::Double:
            return mq_pbTagSize(MQ_SP_F_DATASETVALUE_DOUBLE) + 8;
        case MqttSparkplugDataType::Boolean:
            return mq_pbTagSize(MQ_SP_F_DATASETVALUE_BOOLEAN) + 1;
        default:   // String, Text
            return mq_pbTagSize(MQ_SP_F_DATASETVALUE_STRING) +
                   mq_pbVarintSize(cell.as.bytes.length) + cell.as.bytes.length;
    }
}

void writeCell(PbWriter& writer, const MqttSparkplugValue& cell) {
    if (cell.isNull) return;
    switch (cell.type) {
        case MqttSparkplugDataType::Int8:
        case MqttSparkplugDataType::Int16:
        case MqttSparkplugDataType::Int32:
            writer.tag(MQ_SP_F_DATASETVALUE_INT, MQ_PB_VARINT);
            writer.varint(encodeSigned(cell.type, cell.as.signedValue));
            break;
        case MqttSparkplugDataType::UInt8:
        case MqttSparkplugDataType::UInt16:
            writer.tag(MQ_SP_F_DATASETVALUE_INT, MQ_PB_VARINT);
            writer.varint(cell.as.unsignedValue);
            break;
        case MqttSparkplugDataType::UInt32:   // long_value: mq_sparkplugValueField()
        case MqttSparkplugDataType::Int64:
        case MqttSparkplugDataType::UInt64:
        case MqttSparkplugDataType::DateTime:
            writer.tag(MQ_SP_F_DATASETVALUE_LONG, MQ_PB_VARINT);
            writer.varint(cell.as.unsignedValue);
            break;
        case MqttSparkplugDataType::Float:
            writer.tag(MQ_SP_F_DATASETVALUE_FLOAT, MQ_PB_FIXED32);
            writer.fixed32(bitsOfFloat(cell.as.float32));
            break;
        case MqttSparkplugDataType::Double:
            writer.tag(MQ_SP_F_DATASETVALUE_DOUBLE, MQ_PB_FIXED64);
            writer.fixed64(bitsOfDouble(cell.as.float64));
            break;
        case MqttSparkplugDataType::Boolean:
            writer.tag(MQ_SP_F_DATASETVALUE_BOOLEAN, MQ_PB_VARINT);
            writer.varint(cell.as.boolean ? 1 : 0);
            break;
        default:
            writer.tag(MQ_SP_F_DATASETVALUE_STRING, MQ_PB_LENGTH);
            writer.bytes(cell.as.bytes.data, cell.as.bytes.length);
            break;
    }
}

size_t rowBodySize(const MqttSparkplugValue* row, size_t columnCount) {
    size_t total = 0;
    for (size_t column = 0; column < columnCount; column++) {
        const size_t cell = cellBodySize(row[column]);
        total += mq_pbTagSize(MQ_SP_F_DATASET_ROW_ELEMENTS) + mq_pbVarintSize(cell) + cell;
    }
    return total;
}

size_t dataSetBodySize(const MqttSparkplugDataSetColumn* columns,
                       size_t columnCount, const MqttSparkplugValue* cells,
                       size_t rowCount, MqttSparkplugError& error) {
    if ((columnCount > 0 && columns == nullptr) ||
        (rowCount > 0 && columnCount > 0 && cells == nullptr)) {
        error = MqttSparkplugError::InvalidName;
        return 0;
    }
    size_t total = mq_pbTagSize(MQ_SP_F_DATASET_NUM_COLUMNS) +
                   mq_pbVarintSize((uint64_t)columnCount);
    for (size_t column = 0; column < columnCount; column++) {
        if (columns[column].name == nullptr) {
            error = MqttSparkplugError::InvalidName;
            return 0;
        }
        if (!isBasicType(columns[column].type)) {
            error = MqttSparkplugError::DataTypeNotSupported;
            return 0;
        }
        total += textFieldSize(MQ_SP_F_DATASET_COLUMNS, columns[column].name) +
                 mq_pbTagSize(MQ_SP_F_DATASET_TYPES) +
                 mq_pbVarintSize((uint64_t)columns[column].type);
    }
    for (size_t row = 0; row < rowCount; row++) {
        const MqttSparkplugValue* cellsOf = cells + row * columnCount;
        for (size_t column = 0; column < columnCount; column++) {
            const MqttSparkplugValue& cell = cellsOf[column];
            // String and Text share a field, so either fills either column.
            const bool textual = (cell.type == MqttSparkplugDataType::String ||
                                  cell.type == MqttSparkplugDataType::Text) &&
                                 (columns[column].type == MqttSparkplugDataType::String ||
                                  columns[column].type == MqttSparkplugDataType::Text);
            if (!cell.isNull && cell.type != columns[column].type && !textual) {
                error = MqttSparkplugError::ValueTypeMismatch;
                return 0;
            }
        }
        const size_t body = rowBodySize(cellsOf, columnCount);
        total += mq_pbTagSize(MQ_SP_F_DATASET_ROWS) + mq_pbVarintSize(body) + body;
    }
    return total;
}

// One DataSetValue as a value of the column's type. False when its field is
// not the one that type travels in.
bool readCell(const uint8_t* body, size_t length, MqttSparkplugDataType type,
              MqttSparkplugValue& out) {
    out = MqttSparkplugValue();
    out.type = type;
    out.isNull = true;
    PbReader reader(body, length);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) return false;
        bool fits = false;
        switch (type) {
            case MqttSparkplugDataType::Int8:
            case MqttSparkplugDataType::Int16:
            case MqttSparkplugDataType::Int32:
            case MqttSparkplugDataType::UInt8:
            case MqttSparkplugDataType::UInt16:
                fits = field == MQ_SP_F_DATASETVALUE_INT && wireType == MQ_PB_VARINT;
                if (fits) {
                    const uint64_t raw = reader.varint();
                    if (isSignedType(type)) out.as.signedValue = decodeSigned(type, raw);
                    else out.as.unsignedValue = raw & 0xFFFFFFFFu;
                }
                break;
            case MqttSparkplugDataType::UInt32:
                // Either field. Eclipse Tahu's Java (Ignition's) writes a
                // UInt32 cell in long_value, Tahu's Python in int_value; a
                // table from Ignition with a UInt32 column was refused whole.
                fits = (field == MQ_SP_F_DATASETVALUE_INT || field == MQ_SP_F_DATASETVALUE_LONG) &&
                       wireType == MQ_PB_VARINT;
                if (fits) out.as.unsignedValue = reader.varint() & 0xFFFFFFFFu;
                break;
            case MqttSparkplugDataType::Int64:
            case MqttSparkplugDataType::UInt64:
            case MqttSparkplugDataType::DateTime:
                fits = field == MQ_SP_F_DATASETVALUE_LONG && wireType == MQ_PB_VARINT;
                if (fits) out.as.unsignedValue = reader.varint();
                break;
            case MqttSparkplugDataType::Float:
                fits = field == MQ_SP_F_DATASETVALUE_FLOAT && wireType == MQ_PB_FIXED32;
                if (fits) out.as.float32 = floatOfBits(reader.fixed32());
                break;
            case MqttSparkplugDataType::Double:
                fits = field == MQ_SP_F_DATASETVALUE_DOUBLE && wireType == MQ_PB_FIXED64;
                if (fits) out.as.float64 = doubleOfBits(reader.fixed64());
                break;
            case MqttSparkplugDataType::Boolean:
                fits = field == MQ_SP_F_DATASETVALUE_BOOLEAN && wireType == MQ_PB_VARINT;
                if (fits) out.as.boolean = reader.varint() != 0;
                break;
            default: {   // String, Text
                fits = field == MQ_SP_F_DATASETVALUE_STRING && wireType == MQ_PB_LENGTH;
                const uint8_t* text = nullptr;
                size_t textLength = 0;
                if (fits && reader.lengthDelimited(text, textLength)) {
                    out.as.bytes.data = text;
                    out.as.bytes.length = (uint32_t)textLength;
                }
                break;
            }
        }
        if (!fits) return false;
        out.isNull = false;
    }
    return reader.ok();
}

}  // namespace

// --- Templates ---

void MqttSparkplugTemplate::clear() {
    version = nullptr;
    templateRef = nullptr;
    isDefinition = false;
    for (size_t index = 0; index < parameterCount; index++) {
        parameters[index] = MqttSparkplugProperty();
    }
    parameterCount = 0;
    members.clear();
}

size_t mq_sparkplugTemplateSize(const MqttSparkplugTemplate& value,
                                MqttSparkplugError& error) {
    error = MqttSparkplugError::None;
    return templateBodySize(value, error);
}

size_t mq_sparkplugEncodeTemplate(uint8_t* out, size_t capacity,
                                  const MqttSparkplugTemplate& value,
                                  MqttSparkplugError& error) {
    const size_t needed = mq_sparkplugTemplateSize(value, error);
    if (error != MqttSparkplugError::None) return 0;
    if ((out == nullptr && needed > 0) || capacity < needed) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }
    PbWriter writer(out, capacity);
    if (value.version != nullptr) {
        writer.tag(MQ_SP_F_TEMPLATE_VERSION, MQ_PB_LENGTH);
        writer.text(value.version);
    }
    for (size_t index = 0; index < value.members.metricCount; index++) {
        const MqttSparkplugMetric& member = value.members.metrics[index];
        MqttSparkplugError ignored = MqttSparkplugError::None;
        writer.tag(MQ_SP_F_TEMPLATE_METRICS, MQ_PB_LENGTH);
        writer.varint(metricBodySize(member, MqttSparkplugType::NBirth, ignored));
        writeMetric(writer, member, MqttSparkplugType::NBirth);
    }
    for (size_t index = 0; index < value.parameterCount; index++) {
        const MqttSparkplugProperty& parameter = value.parameters[index];
        MqttSparkplugError ignored = MqttSparkplugError::None;
        writer.tag(MQ_SP_F_TEMPLATE_PARAMETERS, MQ_PB_LENGTH);
        writer.varint(parameterBodySize(parameter, ignored));
        writer.tag(MQ_SP_F_PARAMETER_NAME, MQ_PB_LENGTH);
        writer.text(parameter.key);
        writer.tag(MQ_SP_F_PARAMETER_TYPE, MQ_PB_VARINT);
        writer.varint((uint64_t)parameter.type);
        if (!parameter.isNull) writePropertyScalar(writer, parameter);
    }
    if (value.templateRef != nullptr) {
        writer.tag(MQ_SP_F_TEMPLATE_REF, MQ_PB_LENGTH);
        writer.text(value.templateRef);
    }
    writer.tag(MQ_SP_F_TEMPLATE_IS_DEFINITION, MQ_PB_VARINT);
    writer.varint(value.isDefinition ? 1 : 0);
    if (!writer.ok() || writer.written() != needed) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }
    return needed;
}

MqttSparkplugError mq_sparkplugDecodeTemplate(const uint8_t* in, size_t length,
                                              MqttSparkplugTemplate& out) {
    out.clear();
    if (length == 0) return MqttSparkplugError::None;
    if (in == nullptr) return MqttSparkplugError::Truncated;
    PbReader reader(in, length);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        const uint8_t* body = nullptr;
        size_t bodyLength = 0;
        switch (field) {
            case MQ_SP_F_TEMPLATE_VERSION:
            case MQ_SP_F_TEMPLATE_REF: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                if (!reader.lengthDelimited(body, bodyLength)) break;
                const char* text = out.members.intern(body, bodyLength);
                if (text == nullptr) reader.fail(MqttSparkplugError::TooManyMetrics);
                if (field == MQ_SP_F_TEMPLATE_VERSION) out.version = text;
                else out.templateRef = text;
                break;
            }
            case MQ_SP_F_TEMPLATE_METRICS: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                if (!reader.lengthDelimited(body, bodyLength)) break;
                if (out.members.metricCount >= out.members.capacity()) {
                    reader.fail(MqttSparkplugError::TooManyMetrics);
                    break;
                }
                MqttSparkplugMetric member;
                MqttSparkplugError error = MqttSparkplugError::None;
                if (!decodeMetric(body, bodyLength, member, out.members, error)) {
                    reader.fail(error);
                    break;
                }
                out.members.metrics[out.members.metricCount++] = member;
                break;
            }
            case MQ_SP_F_TEMPLATE_PARAMETERS: {
                if (wireType != MQ_PB_LENGTH) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                if (!reader.lengthDelimited(body, bodyLength)) break;
                if (out.parameterCount >= MQ_SP_MAX_PARAMETERS) {
                    reader.fail(MqttSparkplugError::TooManyMetrics);
                    break;
                }
                MqttSparkplugProperty parameter;
                MqttSparkplugError error = MqttSparkplugError::None;
                if (!decodePropertyValue(body, bodyLength, parameter, error,
                                         &out.members)) {
                    reader.fail(error);
                    break;
                }
                out.parameters[out.parameterCount++] = parameter;
                break;
            }
            case MQ_SP_F_TEMPLATE_IS_DEFINITION:
                if (wireType != MQ_PB_VARINT) {
                    reader.fail(MqttSparkplugError::WireTypeMismatch);
                    break;
                }
                out.isDefinition = reader.varint() != 0;
                break;
            default:
                reader.skip(wireType);
                break;
        }
    }
    return reader.error;
}

bool mq_sparkplugTemplateHeader(const uint8_t* in, size_t length,
                                bool& isDefinition, const uint8_t*& ref,
                                size_t& refLength) {
    isDefinition = false;
    ref = nullptr;
    refLength = 0;
    PbReader reader(in, length);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        if (field == MQ_SP_F_TEMPLATE_REF && wireType == MQ_PB_LENGTH) {
            reader.lengthDelimited(ref, refLength);
        } else if (field == MQ_SP_F_TEMPLATE_IS_DEFINITION && wireType == MQ_PB_VARINT) {
            isDefinition = reader.varint() != 0;
        } else {
            reader.skip(wireType);
        }
    }
    return reader.ok();
}

// --- DataSets ---

bool mq_sparkplugDataSetTypeAllowed(MqttSparkplugDataType type) {
    return isBasicType(type);
}

size_t mq_sparkplugDataSetSize(const MqttSparkplugDataSetColumn* columns,
                               size_t columnCount,
                               const MqttSparkplugValue* cells, size_t rowCount,
                               MqttSparkplugError& error) {
    error = MqttSparkplugError::None;
    return dataSetBodySize(columns, columnCount, cells, rowCount, error);
}

size_t mq_sparkplugEncodeDataSet(uint8_t* out, size_t capacity,
                                 const MqttSparkplugDataSetColumn* columns,
                                 size_t columnCount,
                                 const MqttSparkplugValue* cells,
                                 size_t rowCount, MqttSparkplugError& error) {
    const size_t needed =
        mq_sparkplugDataSetSize(columns, columnCount, cells, rowCount, error);
    if (error != MqttSparkplugError::None) return 0;
    if (out == nullptr || capacity < needed) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }
    PbWriter writer(out, capacity);
    // [tck-id-payloads-dataset-column-size]
    writer.tag(MQ_SP_F_DATASET_NUM_COLUMNS, MQ_PB_VARINT);
    writer.varint((uint64_t)columnCount);
    for (size_t column = 0; column < columnCount; column++) {
        writer.tag(MQ_SP_F_DATASET_COLUMNS, MQ_PB_LENGTH);
        writer.text(columns[column].name);
    }
    // Unpacked, one field each, as the proto2 schema's generated encoders
    // write them; the reader takes either.
    for (size_t column = 0; column < columnCount; column++) {
        writer.tag(MQ_SP_F_DATASET_TYPES, MQ_PB_VARINT);
        writer.varint((uint64_t)columns[column].type);
    }
    for (size_t row = 0; row < rowCount; row++) {
        const MqttSparkplugValue* cellsOf = cells + row * columnCount;
        writer.tag(MQ_SP_F_DATASET_ROWS, MQ_PB_LENGTH);
        writer.varint(rowBodySize(cellsOf, columnCount));
        for (size_t column = 0; column < columnCount; column++) {
            writer.tag(MQ_SP_F_DATASET_ROW_ELEMENTS, MQ_PB_LENGTH);
            writer.varint(cellBodySize(cellsOf[column]));
            writeCell(writer, cellsOf[column]);
        }
    }
    if (!writer.ok() || writer.written() != needed) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }
    return needed;
}

MqttSparkplugError MqttSparkplugDataSetReader::open(const uint8_t* data,
                                                    size_t length) {
    _data = data;
    _length = length;
    _columns = 0;
    _rows = 0;
    _cursorValid = false;
    if (length > 0 && data == nullptr) return MqttSparkplugError::Truncated;

    // First the shape: names, types, the declared count, how many rows - and
    // the first columns' types, kept.
    size_t names = 0;
    size_t types = 0;
    uint64_t declared = 0;
    bool sawDeclared = false;
    size_t rows = 0;
    PbReader reader(data, length);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        const uint8_t* body = nullptr;
        size_t bodyLength = 0;
        if (field == MQ_SP_F_DATASET_NUM_COLUMNS && wireType == MQ_PB_VARINT) {
            declared = reader.varint();
            sawDeclared = true;
        } else if (field == MQ_SP_F_DATASET_COLUMNS && wireType == MQ_PB_LENGTH) {
            if (reader.lengthDelimited(body, bodyLength)) names++;
        } else if (field == MQ_SP_F_DATASET_TYPES && wireType == MQ_PB_VARINT) {
            const uint64_t type = reader.varint();
            if (!isBasicType((MqttSparkplugDataType)(uint32_t)type)) {
                return MqttSparkplugError::DataTypeNotSupported;
            }
            if (types < MQ_SP_DATASET_TYPE_CACHE) _types[types] = (uint8_t)type;
            types++;
        } else if (field == MQ_SP_F_DATASET_TYPES && wireType == MQ_PB_LENGTH) {
            // Packed, as a proto3 encoder writes a repeated scalar.
            if (!reader.lengthDelimited(body, bodyLength)) break;
            PbReader packed(body, bodyLength);
            while (packed.ok() && !packed.atEnd()) {
                const uint64_t type = packed.varint();
                if (!packed.ok()) break;
                if (!isBasicType((MqttSparkplugDataType)(uint32_t)type)) {
                    return MqttSparkplugError::DataTypeNotSupported;
                }
                if (types < MQ_SP_DATASET_TYPE_CACHE) _types[types] = (uint8_t)type;
                types++;
            }
            if (!packed.ok()) return packed.error;
        } else if (field == MQ_SP_F_DATASET_ROWS && wireType == MQ_PB_LENGTH) {
            if (reader.lengthDelimited(body, bodyLength)) rows++;
        } else if (field == MQ_SP_F_DATASET_NUM_COLUMNS ||
                   field == MQ_SP_F_DATASET_COLUMNS ||
                   field == MQ_SP_F_DATASET_ROWS) {
            return MqttSparkplugError::WireTypeMismatch;
        } else {
            reader.skip(wireType);
        }
    }
    if (!reader.ok()) return reader.error;
    // [tck-id-payloads-dataset-column-num-headers], -types-num, -column-size
    if (names != types || (sawDeclared && declared != (uint64_t)names)) {
        return MqttSparkplugError::ValueTypeMismatch;
    }

    // A WIDE TABLE'S TYPES, for this pass only. At most a byte a column, and a
    // column costs at least two bytes of the message, so this is never more
    // than half of what the caller already holds. Without the memory the
    // table is refused rather than read the slow way - the slow way is the
    // stall this replaced.
    uint8_t* wide = nullptr;
    if (names > MQ_SP_DATASET_TYPE_CACHE) {
        wide = (uint8_t*)mq_allocLarge(names);
        if (wide == nullptr) return MqttSparkplugError::TooManyMetrics;
        size_t at = 0;
        PbReader again(data, length);
        while (again.ok() && !again.atEnd()) {
            uint32_t field = 0;
            uint32_t wireType = 0;
            if (!readTag(again, field, wireType)) break;
            if (field == MQ_SP_F_DATASET_TYPES && wireType == MQ_PB_VARINT) {
                const uint64_t type = again.varint();
                if (at < names) wide[at++] = (uint8_t)type;
            } else if (field == MQ_SP_F_DATASET_TYPES && wireType == MQ_PB_LENGTH) {
                const uint8_t* body = nullptr;
                size_t bodyLength = 0;
                if (!again.lengthDelimited(body, bodyLength)) break;
                PbReader packed(body, bodyLength);
                while (packed.ok() && !packed.atEnd()) {
                    const uint64_t type = packed.varint();
                    if (at < names) wide[at++] = (uint8_t)type;
                }
            } else {
                again.skip(wireType);
            }
        }
    }
    _columns = names;

    // Then every row, in one walk: a cell for each column, each in its
    // column's field.
    MqttSparkplugError failed = MqttSparkplugError::None;
    PbReader walk(_data, _length);
    while (failed == MqttSparkplugError::None && walk.ok() && !walk.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(walk, field, wireType)) break;
        if (field != MQ_SP_F_DATASET_ROWS || wireType != MQ_PB_LENGTH) {
            walk.skip(wireType);
            continue;
        }
        const uint8_t* rowBody = nullptr;
        size_t rowLength = 0;
        if (!walk.lengthDelimited(rowBody, rowLength)) break;
        PbReader rowReader(rowBody, rowLength);
        size_t cells = 0;
        bool fits = true;
        while (fits && rowReader.ok() && !rowReader.atEnd()) {
            if (!readTag(rowReader, field, wireType)) break;
            if (field != MQ_SP_F_DATASET_ROW_ELEMENTS || wireType != MQ_PB_LENGTH) {
                rowReader.skip(wireType);
                continue;
            }
            const uint8_t* body = nullptr;
            size_t bodyLength = 0;
            if (!rowReader.lengthDelimited(body, bodyLength)) break;
            MqttSparkplugValue cell;
            fits = cells < _columns;
            if (fits) {
                const MqttSparkplugDataType type =
                    (MqttSparkplugDataType)(cells < MQ_SP_DATASET_TYPE_CACHE
                                                ? _types[cells] : wide[cells]);
                fits = readCell(body, bodyLength, type, cell);
            }
            cells++;
        }
        failed = rowReader.error;
        if (failed == MqttSparkplugError::None && (!fits || cells != _columns)) {
            failed = MqttSparkplugError::ValueTypeMismatch;
        }
    }
    mq_freeLarge(wide);
    if (failed == MqttSparkplugError::None && !walk.ok()) failed = walk.error;
    if (failed != MqttSparkplugError::None) {
        _columns = _rows = 0;
        return failed;
    }
    _rows = rows;
    return MqttSparkplugError::None;
}

bool MqttSparkplugDataSetReader::typeOf(size_t index,
                                        MqttSparkplugDataType& type) const {
    if (index >= _columns) return false;
    if (index < MQ_SP_DATASET_TYPE_CACHE) {
        type = (MqttSparkplugDataType)_types[index];
        return true;
    }
    // Past the cache: walked, as it always was.
    type = MqttSparkplugDataType::Unknown;
    size_t types = 0;
    PbReader reader(_data, _length);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        if (field == MQ_SP_F_DATASET_TYPES && wireType == MQ_PB_VARINT) {
            const uint64_t raw = reader.varint();
            if (types++ == index) {
                type = (MqttSparkplugDataType)(uint32_t)raw;
                return true;
            }
        } else if (field == MQ_SP_F_DATASET_TYPES && wireType == MQ_PB_LENGTH) {
            const uint8_t* body = nullptr;
            size_t bodyLength = 0;
            reader.lengthDelimited(body, bodyLength);
            PbReader packed(body, bodyLength);
            while (packed.ok() && !packed.atEnd()) {
                const uint64_t raw = packed.varint();
                if (types++ == index) {
                    type = (MqttSparkplugDataType)(uint32_t)raw;
                    return true;
                }
            }
        } else {
            reader.skip(wireType);
        }
    }
    return false;
}

bool MqttSparkplugDataSetReader::column(size_t index, const uint8_t*& name,
                                        size_t& nameLength,
                                        MqttSparkplugDataType& type) const {
    if (index >= _columns) return false;
    name = nullptr;
    nameLength = 0;
    type = MqttSparkplugDataType::Unknown;
    size_t names = 0;
    PbReader reader(_data, _length);
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        if (field == MQ_SP_F_DATASET_COLUMNS && wireType == MQ_PB_LENGTH) {
            const uint8_t* body = nullptr;
            size_t bodyLength = 0;
            reader.lengthDelimited(body, bodyLength);
            if (names++ == index) {
                name = body;
                nameLength = bodyLength;
                break;
            }
        } else {
            reader.skip(wireType);
        }
    }
    return reader.ok() && typeOf(index, type) &&
           type != MqttSparkplugDataType::Unknown;
}

bool MqttSparkplugDataSetReader::cell(size_t row, size_t column,
                                      MqttSparkplugValue& out) const {
    MqttSparkplugDataType type = MqttSparkplugDataType::Unknown;
    if (row >= _rows || !typeOf(column, type)) return false;

    // THE ROW: from the cursor when it is at or before it, else from the top.
    PbReader reader(_data, _length);
    size_t rows = 0;
    if (_cursorValid && row >= _cursorRow) {
        reader.cursor = _cursorRowAt;
        rows = _cursorRow;
    }
    while (reader.ok() && !reader.atEnd()) {
        const size_t fieldAt = reader.cursor;
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        if (field != MQ_SP_F_DATASET_ROWS || wireType != MQ_PB_LENGTH) {
            reader.skip(wireType);
            continue;
        }
        const uint8_t* rowBody = nullptr;
        size_t rowLength = 0;
        if (!reader.lengthDelimited(rowBody, rowLength)) break;
        if (rows++ != row) continue;

        // THE CELL: from the cursor when it is in this row, before it.
        PbReader cells(rowBody, rowLength);
        size_t seen = 0;
        if (_cursorValid && _cursorRow == row && _cursorRowAt == fieldAt &&
            column >= _cursorColumn) {
            cells.cursor = _cursorCellAt;
            seen = _cursorColumn;
        }
        while (cells.ok() && !cells.atEnd()) {
            if (!readTag(cells, field, wireType)) break;
            if (field != MQ_SP_F_DATASET_ROW_ELEMENTS || wireType != MQ_PB_LENGTH) {
                cells.skip(wireType);
                continue;
            }
            const uint8_t* body = nullptr;
            size_t bodyLength = 0;
            if (!cells.lengthDelimited(body, bodyLength)) break;
            if (seen++ != column) continue;
            _cursorValid = true;
            _cursorRow = row;
            _cursorRowAt = fieldAt;
            _cursorColumn = column + 1;
            _cursorCellAt = cells.cursor;
            return readCell(body, bodyLength, type, out);
        }
        return false;
    }
    return false;
}

// --- PropertySets as values ---

MqttSparkplugProperty mq_spPropertySet(const char* key, const uint8_t* encoded,
                                       uint32_t length) {
    MqttSparkplugProperty out;
    out.key = key;
    out.type = MqttSparkplugDataType::PropertySet;
    out.as.text.data = encoded;
    out.as.text.length = encoded ? length : 0;
    return out;
}

MqttSparkplugProperty mq_spPropertySetList(const char* key,
                                           const uint8_t* encoded,
                                           uint32_t length) {
    MqttSparkplugProperty out = mq_spPropertySet(key, encoded, length);
    out.type = MqttSparkplugDataType::PropertySetList;
    return out;
}

size_t mq_sparkplugEncodePropertySet(uint8_t* out, size_t capacity,
                                     const MqttSparkplugProperty* properties,
                                     size_t count, MqttSparkplugError& error) {
    error = MqttSparkplugError::None;
    if (count > 0 && properties == nullptr) {
        error = MqttSparkplugError::InvalidName;
        return 0;
    }
    const size_t needed = propertySetBodySize(properties, count, error);
    if (error != MqttSparkplugError::None) return 0;
    if (out == nullptr || capacity < needed) return needed;
    PbWriter writer(out, capacity);
    writePropertySet(writer, properties, count);
    if (!writer.ok() || writer.written() != needed) {
        error = MqttSparkplugError::Overflow;
        return 0;
    }
    return needed;
}

MqttSparkplugError mq_sparkplugDecodePropertySet(
    const uint8_t* in, size_t length, MqttSparkplugProperty* properties,
    size_t capacity, size_t& count, MqttSparkplugPayload& names) {
    MqttSparkplugError error = MqttSparkplugError::None;
    count = 0;
    if (length > 0 && in == nullptr) return MqttSparkplugError::Truncated;
    decodePropertySet(in, length, properties, capacity, count, names, error);
    return error;
}

size_t mq_sparkplugEncodePropertySetList(uint8_t* out, size_t capacity,
                                         const uint8_t* const* sets,
                                         const size_t* lengths, size_t count) {
    if (count > 0 && (sets == nullptr || lengths == nullptr)) return 0;
    size_t needed = 0;
    for (size_t index = 0; index < count; index++) {
        needed += mq_pbTagSize(MQ_SP_F_PROPERTYSETLIST_SETS) +
                  mq_pbVarintSize(lengths[index]) + lengths[index];
    }
    if (out == nullptr || capacity < needed) return needed;
    PbWriter writer(out, capacity);
    for (size_t index = 0; index < count; index++) {
        writer.tag(MQ_SP_F_PROPERTYSETLIST_SETS, MQ_PB_LENGTH);
        writer.bytes(sets[index], lengths[index]);
    }
    return writer.ok() ? needed : 0;
}

bool mq_sparkplugPropertySetListAt(const uint8_t* in, size_t length,
                                   size_t index, const uint8_t*& set,
                                   size_t& setLength) {
    set = nullptr;
    setLength = 0;
    PbReader reader(in, length);
    size_t seen = 0;
    while (reader.ok() && !reader.atEnd()) {
        uint32_t field = 0;
        uint32_t wireType = 0;
        if (!readTag(reader, field, wireType)) break;
        if (field != MQ_SP_F_PROPERTYSETLIST_SETS || wireType != MQ_PB_LENGTH) {
            reader.skip(wireType);
            continue;
        }
        const uint8_t* body = nullptr;
        size_t bodyLength = 0;
        if (!reader.lengthDelimited(body, bodyLength)) return false;
        if (seen++ == index) {
            set = body;
            setLength = bodyLength;
            return true;
        }
    }
    return false;
}

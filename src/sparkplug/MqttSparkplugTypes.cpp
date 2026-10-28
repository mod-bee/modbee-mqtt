// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugTypes.h"

#include <string.h>
#include "../MqttNoExcept.h"

// The wire spellings, indexed by the enum. A table rather than a switch so the
// two directions - name from type, type from name - cannot disagree.
namespace {

struct TypeName {
    const char* text;
    size_t length;
};

// Order MUST match MqttSparkplugType.
const TypeName kTypeNames[MQ_SP_TYPE_COUNT] = {
    { "NBIRTH", 6 },
    { "NDEATH", 6 },
    { "NDATA",  5 },
    { "NCMD",   4 },
    { "DBIRTH", 6 },
    { "DDEATH", 6 },
    { "DDATA",  5 },
    { "DCMD",   4 },
    { "STATE",  5 },
};

}  // namespace

const char* mq_sparkplugTypeName(MqttSparkplugType type) {
    const uint8_t index = (uint8_t)type;
    if (index >= MQ_SP_TYPE_COUNT) return "?";
    return kTypeNames[index].text;
}

bool mq_sparkplugTypeFromName(const char* name, size_t length,
                              MqttSparkplugType& out) {
    if (name == nullptr || length == 0) return false;

    for (uint8_t index = 0; index < MQ_SP_TYPE_COUNT; index++) {
        if (kTypeNames[index].length != length) continue;
        // Exact bytes. NOT case insensitive: Sparkplug identifiers are case
        // sensitive [tck-id-case-sensitivity-sparkplug-ids], and a topic
        // element that differs in case is a different element.
        if (memcmp(name, kTypeNames[index].text, length) == 0) {
            out = (MqttSparkplugType)index;
            return true;
        }
    }
    return false;
}

const char* mq_sparkplugDataTypeName(MqttSparkplugDataType type) {
    switch (type) {
        case MqttSparkplugDataType::Unknown:         return "Unknown";
        case MqttSparkplugDataType::Int8:            return "Int8";
        case MqttSparkplugDataType::Int16:           return "Int16";
        case MqttSparkplugDataType::Int32:           return "Int32";
        case MqttSparkplugDataType::Int64:           return "Int64";
        case MqttSparkplugDataType::UInt8:           return "UInt8";
        case MqttSparkplugDataType::UInt16:          return "UInt16";
        case MqttSparkplugDataType::UInt32:          return "UInt32";
        case MqttSparkplugDataType::UInt64:          return "UInt64";
        case MqttSparkplugDataType::Float:           return "Float";
        case MqttSparkplugDataType::Double:          return "Double";
        case MqttSparkplugDataType::Boolean:         return "Boolean";
        case MqttSparkplugDataType::String:          return "String";
        case MqttSparkplugDataType::DateTime:        return "DateTime";
        case MqttSparkplugDataType::Text:            return "Text";
        case MqttSparkplugDataType::UUID:            return "UUID";
        case MqttSparkplugDataType::DataSet:         return "DataSet";
        case MqttSparkplugDataType::Bytes:           return "Bytes";
        case MqttSparkplugDataType::File:            return "File";
        case MqttSparkplugDataType::Template:        return "Template";
        case MqttSparkplugDataType::PropertySet:     return "PropertySet";
        case MqttSparkplugDataType::PropertySetList: return "PropertySetList";
        case MqttSparkplugDataType::Int8Array:       return "Int8Array";
        case MqttSparkplugDataType::Int16Array:      return "Int16Array";
        case MqttSparkplugDataType::Int32Array:      return "Int32Array";
        case MqttSparkplugDataType::Int64Array:      return "Int64Array";
        case MqttSparkplugDataType::UInt8Array:      return "UInt8Array";
        case MqttSparkplugDataType::UInt16Array:     return "UInt16Array";
        case MqttSparkplugDataType::UInt32Array:     return "UInt32Array";
        case MqttSparkplugDataType::UInt64Array:     return "UInt64Array";
        case MqttSparkplugDataType::FloatArray:      return "FloatArray";
        case MqttSparkplugDataType::DoubleArray:     return "DoubleArray";
        case MqttSparkplugDataType::BooleanArray:    return "BooleanArray";
        case MqttSparkplugDataType::StringArray:     return "StringArray";
        case MqttSparkplugDataType::DateTimeArray:   return "DateTimeArray";
    }
    return "?";
}

bool mq_sparkplugDataTypeSupported(MqttSparkplugDataType type) {
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
        case MqttSparkplugDataType::UUID:
        case MqttSparkplugDataType::Bytes:
        case MqttSparkplugDataType::File:
        case MqttSparkplugDataType::DataSet:
        case MqttSparkplugDataType::Template:
            return true;

        // Unknown is a legal enum value and not a legal metric datatype. It
        // exists so a decoder can say "the far end did not tell me", which is
        // different from "the far end told me something I cannot carry".
        // PropertySet and PropertySetList are a property's types: a metric's
        // value `oneof` has no field for either.
        case MqttSparkplugDataType::Unknown:
        case MqttSparkplugDataType::PropertySet:
        case MqttSparkplugDataType::PropertySetList:
            return false;

        default:
            return mq_sparkplugIsArrayType(type);
    }
}

MqttSparkplugDataType mq_sparkplugArrayElement(MqttSparkplugDataType type) {
    switch (type) {
        case MqttSparkplugDataType::Int8Array:     return MqttSparkplugDataType::Int8;
        case MqttSparkplugDataType::Int16Array:    return MqttSparkplugDataType::Int16;
        case MqttSparkplugDataType::Int32Array:    return MqttSparkplugDataType::Int32;
        case MqttSparkplugDataType::Int64Array:    return MqttSparkplugDataType::Int64;
        case MqttSparkplugDataType::UInt8Array:    return MqttSparkplugDataType::UInt8;
        case MqttSparkplugDataType::UInt16Array:   return MqttSparkplugDataType::UInt16;
        case MqttSparkplugDataType::UInt32Array:   return MqttSparkplugDataType::UInt32;
        case MqttSparkplugDataType::UInt64Array:   return MqttSparkplugDataType::UInt64;
        case MqttSparkplugDataType::FloatArray:    return MqttSparkplugDataType::Float;
        case MqttSparkplugDataType::DoubleArray:   return MqttSparkplugDataType::Double;
        case MqttSparkplugDataType::BooleanArray:  return MqttSparkplugDataType::Boolean;
        case MqttSparkplugDataType::StringArray:   return MqttSparkplugDataType::String;
        case MqttSparkplugDataType::DateTimeArray: return MqttSparkplugDataType::DateTime;
        default:                                   return MqttSparkplugDataType::Unknown;
    }
}

size_t mq_sparkplugArrayElementBytes(MqttSparkplugDataType type) {
    switch (type) {
        case MqttSparkplugDataType::Int8Array:
        case MqttSparkplugDataType::UInt8Array:    return 1;
        case MqttSparkplugDataType::Int16Array:
        case MqttSparkplugDataType::UInt16Array:   return 2;
        case MqttSparkplugDataType::Int32Array:
        case MqttSparkplugDataType::UInt32Array:
        case MqttSparkplugDataType::FloatArray:    return 4;
        case MqttSparkplugDataType::Int64Array:
        case MqttSparkplugDataType::UInt64Array:
        case MqttSparkplugDataType::DoubleArray:
        case MqttSparkplugDataType::DateTimeArray: return 8;
        default:                                   return 0;
    }
}

const char* mq_sparkplugErrorName(MqttSparkplugError error) {
    switch (error) {
        case MqttSparkplugError::None:                 return "none";
        case MqttSparkplugError::Truncated:            return "truncated";
        case MqttSparkplugError::VarintTooLong:        return "varint too long";
        case MqttSparkplugError::WireTypeMismatch:     return "wire type mismatch";
        case MqttSparkplugError::LengthOverrun:        return "length overrun";
        case MqttSparkplugError::Overflow:             return "buffer too small";
        case MqttSparkplugError::DataTypeNotSupported: return "datatype not supported";
        case MqttSparkplugError::TooManyMetrics:       return "too many metrics";
        case MqttSparkplugError::InvalidName:          return "invalid name";
        case MqttSparkplugError::ValueTypeMismatch:    return "value type mismatch";
        case MqttSparkplugError::TemplateReference:    return "template reference";
    }
    return "?";
}

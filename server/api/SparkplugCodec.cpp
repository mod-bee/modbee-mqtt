// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "SparkplugCodec.h"

#include "modbee/sparkplug/v1/wire.pb.h"

#include <zlib.h>

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace sparkplug_codec {

namespace v1 = modbee::app::v1;
namespace wire = modbee::sparkplug::v1;

namespace {

const char kCompressed[] = "SPBV1.0_COMPRESSED";
const size_t kMaxInflated = 16u << 20;      // a payload is at most this, once inflated

// --- Datatypes ----------------------------------------------------------------

bool isArray(uint32_t t) { return t >= v1::DATATYPE_INT8_ARRAY && t <= v1::DATATYPE_DATETIME_ARRAY; }

// An array element's width in bytes, 0 for none (BooleanArray, StringArray).
size_t widthOf(uint32_t t) {
    switch (t) {
    case v1::DATATYPE_INT8_ARRAY: case v1::DATATYPE_UINT8_ARRAY: return 1;
    case v1::DATATYPE_INT16_ARRAY: case v1::DATATYPE_UINT16_ARRAY: return 2;
    case v1::DATATYPE_INT32_ARRAY: case v1::DATATYPE_UINT32_ARRAY: case v1::DATATYPE_FLOAT_ARRAY: return 4;
    case v1::DATATYPE_INT64_ARRAY: case v1::DATATYPE_UINT64_ARRAY: case v1::DATATYPE_DOUBLE_ARRAY:
    case v1::DATATYPE_DATETIME_ARRAY: return 8;
    default: return 0;
    }
}

bool isSignedArray(uint32_t t) {
    return t == v1::DATATYPE_INT8_ARRAY || t == v1::DATATYPE_INT16_ARRAY || t == v1::DATATYPE_INT32_ARRAY ||
           t == v1::DATATYPE_INT64_ARRAY;
}

// A whole number at a scalar type's width: what was sent as a uint32 or uint64.
void integerTo(uint32_t t, uint64_t raw, v1::SparkplugValue& out) {
    switch (t) {
    case v1::DATATYPE_INT8: out.set_int_value((int8_t)(uint8_t)raw); return;
    case v1::DATATYPE_INT16: out.set_int_value((int16_t)(uint16_t)raw); return;
    case v1::DATATYPE_INT32: out.set_int_value((int32_t)(uint32_t)raw); return;
    case v1::DATATYPE_INT64: out.set_int_value((int64_t)raw); return;
    case v1::DATATYPE_UINT8: out.set_uint_value(raw & 0xFFu); return;
    case v1::DATATYPE_UINT16: out.set_uint_value(raw & 0xFFFFu); return;
    case v1::DATATYPE_UINT32: out.set_uint_value(raw & 0xFFFFFFFFu); return;
    case v1::DATATYPE_BOOLEAN: out.set_boolean_value(raw != 0); return;
    default: out.set_uint_value(raw); return;      // UInt64, DateTime, and a datatype nobody stated
    }
}

// --- One scalar, whichever message carries it ---------------------------------

// The value fields every value-carrying message has, by name, with its own
// field numbers: read into this, and written back from it.
struct Scalar {
    enum Which { None, Int, Long, Float, Double, Bool, String, Bytes } which = None;
    uint64_t integer = 0;
    float f = 0;
    double d = 0;
    bool b = false;
    std::string text;
};

template <typename W>
Scalar scalarOf(const W& w) {
    Scalar s;
    if (w.has_int_value()) { s.which = Scalar::Int; s.integer = w.int_value(); }
    else if (w.has_long_value()) { s.which = Scalar::Long; s.integer = w.long_value(); }
    else if (w.has_float_value()) { s.which = Scalar::Float; s.f = w.float_value(); }
    else if (w.has_double_value()) { s.which = Scalar::Double; s.d = w.double_value(); }
    else if (w.has_boolean_value()) { s.which = Scalar::Bool; s.b = w.boolean_value(); }
    else if (w.has_string_value()) { s.which = Scalar::String; s.text = w.string_value(); }
    return s;
}

template <typename W>
void scalarInto(const Scalar& s, W& w) {
    switch (s.which) {
    case Scalar::Int: w.set_int_value((uint32_t)s.integer); break;
    case Scalar::Long: w.set_long_value(s.integer); break;
    case Scalar::Float: w.set_float_value(s.f); break;
    case Scalar::Double: w.set_double_value(s.d); break;
    case Scalar::Bool: w.set_boolean_value(s.b); break;
    case Scalar::String: w.set_string_value(s.text); break;
    default: break;
    }
}

void valueOf(uint32_t type, const Scalar& s, v1::SparkplugValue& out) {
    switch (s.which) {
    case Scalar::Int:
    case Scalar::Long: integerTo(type, s.integer, out); break;
    case Scalar::Float: out.set_float_value(s.f); break;
    case Scalar::Double: out.set_double_value(s.d); break;
    case Scalar::Bool: out.set_boolean_value(s.b); break;
    case Scalar::String: out.set_string_value(s.text); break;
    case Scalar::Bytes: out.set_bytes_value(s.text); break;
    default: break;
    }
}

// A value for the wire, as its datatype says to send it; false when it cannot be.
bool scalarFor(uint32_t type, const v1::SparkplugValue& v, Scalar& s, std::string& error) {
    const auto whole = [&](bool wide) {
        s.which = wide ? Scalar::Long : Scalar::Int;
        s.integer = v.kind_case() == v1::SparkplugValue::kIntValue ? (uint64_t)v.int_value()
                    : v.kind_case() == v1::SparkplugValue::kBooleanValue ? (uint64_t)v.boolean_value()
                    : v.kind_case() == v1::SparkplugValue::kFloatValue ? (uint64_t)(int64_t)v.float_value()
                    : v.kind_case() == v1::SparkplugValue::kDoubleValue ? (uint64_t)(int64_t)v.double_value()
                                                                         : v.uint_value();
        if (!wide) s.integer = (uint32_t)s.integer;
        return true;
    };
    const auto number = [&]() -> double {
        switch (v.kind_case()) {
        case v1::SparkplugValue::kIntValue: return (double)v.int_value();
        case v1::SparkplugValue::kUintValue: return (double)v.uint_value();
        case v1::SparkplugValue::kFloatValue: return v.float_value();
        case v1::SparkplugValue::kBooleanValue: return v.boolean_value() ? 1 : 0;
        default: return v.double_value();
        }
    };
    switch (type) {
    case v1::DATATYPE_INT8: case v1::DATATYPE_INT16: case v1::DATATYPE_INT32:
    case v1::DATATYPE_UINT8: case v1::DATATYPE_UINT16: case v1::DATATYPE_UINT32: return whole(false);
    case v1::DATATYPE_INT64: case v1::DATATYPE_UINT64: case v1::DATATYPE_DATETIME: return whole(true);
    case v1::DATATYPE_FLOAT: s.which = Scalar::Float; s.f = (float)number(); return true;
    case v1::DATATYPE_DOUBLE: s.which = Scalar::Double; s.d = number(); return true;
    case v1::DATATYPE_BOOLEAN:
        s.which = Scalar::Bool;
        s.b = v.kind_case() == v1::SparkplugValue::kBooleanValue ? v.boolean_value() : number() != 0;
        return true;
    case v1::DATATYPE_STRING: case v1::DATATYPE_TEXT: case v1::DATATYPE_UUID:
        s.which = Scalar::String;
        s.text = v.string_value();
        return true;
    case v1::DATATYPE_BYTES: case v1::DATATYPE_FILE:
        s.which = Scalar::Bytes;
        s.text = v.bytes_value();
        return true;
    default:
        error = "datatype " + v1::SparkplugDataType_Name((v1::SparkplugDataType)type) + " is not a scalar";
        return false;
    }
}

// --- Arrays, packed in bytes_value --------------------------------------------

void put(std::string& out, uint64_t value, size_t width) {
    for (size_t i = 0; i < width; i++) out.push_back((char)(value >> (8 * i)));
}

uint64_t take(const unsigned char* at, size_t width) {
    uint64_t value = 0;
    for (size_t i = 0; i < width; i++) value |= (uint64_t)at[i] << (8 * i);
    return value;
}

bool unpack(uint32_t t, const std::string& bytes, v1::SparkplugArray& out, std::string& error) {
    const unsigned char* data = (const unsigned char*)bytes.data();
    if (t == v1::DATATYPE_STRING_ARRAY) {
        size_t start = 0;
        for (size_t i = 0; i < bytes.size(); i++) {
            if (bytes[i] != '\0') continue;
            out.add_strings(bytes.substr(start, i - start));
            start = i + 1;
        }
        if (start < bytes.size()) {
            error = "a StringArray ends each string with a NUL";
            return false;
        }
        return true;
    }
    if (t == v1::DATATYPE_BOOLEAN_ARRAY) {
        if (bytes.size() < 4) {
            error = "a BooleanArray starts with a 4-byte count";
            return false;
        }
        const uint32_t n = (uint32_t)take(data, 4);
        if (bytes.size() < 4 + ((size_t)n + 7) / 8) {
            error = "a BooleanArray has fewer bits than its count";
            return false;
        }
        for (uint32_t i = 0; i < n; i++) out.add_booleans(((data[4 + i / 8] >> (7 - i % 8)) & 1) != 0);
        return true;
    }
    const size_t width = widthOf(t);
    if (width == 0 || bytes.size() % width != 0) {
        error = "an array's bytes are not a whole number of its elements";
        return false;
    }
    for (size_t at = 0; at < bytes.size(); at += width) {
        const uint64_t raw = take(data + at, width);
        if (t == v1::DATATYPE_FLOAT_ARRAY) {
            const uint32_t bits = (uint32_t)raw;
            float f;
            std::memcpy(&f, &bits, sizeof f);
            out.add_floats(f);
        } else if (t == v1::DATATYPE_DOUBLE_ARRAY) {
            double d;
            std::memcpy(&d, &raw, sizeof d);
            out.add_doubles(d);
        } else if (isSignedArray(t)) {
            const int shift = (int)(64 - 8 * width);
            out.add_ints((int64_t)(raw << shift) >> shift);
        } else {
            out.add_uints(raw);
        }
    }
    return true;
}

std::string pack(uint32_t t, const v1::SparkplugArray& a) {
    std::string out;
    if (t == v1::DATATYPE_STRING_ARRAY) {
        for (const std::string& s : a.strings()) {
            out += s;
            out.push_back('\0');
        }
        return out;
    }
    if (t == v1::DATATYPE_BOOLEAN_ARRAY) {
        put(out, (uint64_t)a.booleans_size(), 4);
        out.resize(4 + ((size_t)a.booleans_size() + 7) / 8, '\0');
        for (int i = 0; i < a.booleans_size(); i++) {
            if (a.booleans(i)) out[4 + i / 8] = (char)(out[4 + i / 8] | (1 << (7 - i % 8)));
        }
        return out;
    }
    const size_t width = widthOf(t);
    if (t == v1::DATATYPE_FLOAT_ARRAY) {
        for (float f : a.floats()) {
            uint32_t bits;
            std::memcpy(&bits, &f, sizeof bits);
            put(out, bits, 4);
        }
    } else if (t == v1::DATATYPE_DOUBLE_ARRAY) {
        for (double d : a.doubles()) {
            uint64_t bits;
            std::memcpy(&bits, &d, sizeof bits);
            put(out, bits, 8);
        }
    } else if (isSignedArray(t)) {
        for (int64_t v : a.ints()) put(out, (uint64_t)v, width);
    } else {
        for (uint64_t v : a.uints()) put(out, v, width);
    }
    return out;
}

// --- Structures ---------------------------------------------------------------

bool metricOf(const wire::Payload::Metric& w, v1::SparkplugMetric& out, std::string& error);
bool metricFor(const v1::SparkplugMetric& m, wire::Payload::Metric& w, std::string& error);

void propertySetOf(const wire::Payload::PropertySet& w, v1::SparkplugPropertySet& out) {
    for (int i = 0; i < w.keys_size() && i < w.values_size(); i++) {
        const wire::Payload::PropertyValue& pv = w.values(i);
        v1::SparkplugProperty* p = out.add_properties();
        p->set_key(w.keys(i));
        p->set_type((v1::SparkplugDataType)pv.type());
        p->set_is_null(pv.is_null());
        if (pv.has_propertyset_value()) {
            propertySetOf(pv.propertyset_value(), *p->mutable_set());
        } else if (pv.has_propertysets_value()) {
            for (const wire::Payload::PropertySet& set : pv.propertysets_value().propertyset()) {
                propertySetOf(set, *p->add_sets());
            }
        } else if (!pv.is_null()) {
            valueOf(pv.type(), scalarOf(pv), *p->mutable_value());
        }
    }
}

bool propertySetFor(const v1::SparkplugPropertySet& set, wire::Payload::PropertySet& w, std::string& error) {
    for (const v1::SparkplugProperty& p : set.properties()) {
        w.add_keys(p.key());
        wire::Payload::PropertyValue* pv = w.add_values();
        pv->set_type(p.type());
        if (p.is_null()) {
            pv->set_is_null(true);
        } else if (p.type() == v1::DATATYPE_PROPERTY_SET) {
            if (!propertySetFor(p.set(), *pv->mutable_propertyset_value(), error)) return false;
        } else if (p.type() == v1::DATATYPE_PROPERTY_SET_LIST) {
            for (const v1::SparkplugPropertySet& one : p.sets()) {
                if (!propertySetFor(one, *pv->mutable_propertysets_value()->add_propertyset(), error)) return false;
            }
        } else {
            Scalar s;
            if (!scalarFor(p.type(), p.value(), s, error)) {
                error = "property " + p.key() + ": " + error;
                return false;
            }
            scalarInto(s, *pv);
        }
    }
    return true;
}

void dataSetOf(const wire::Payload::DataSet& w, v1::SparkplugDataSet& out) {
    for (const std::string& c : w.columns()) out.add_columns(c);
    for (uint32_t t : w.types()) out.add_types((v1::SparkplugDataType)t);
    for (const wire::Payload::DataSet::Row& row : w.rows()) {
        v1::SparkplugRow* r = out.add_rows();
        for (int i = 0; i < row.elements_size(); i++) {
            valueOf(i < w.types_size() ? w.types(i) : 0, scalarOf(row.elements(i)), *r->add_values());
        }
    }
}

bool dataSetFor(const v1::SparkplugDataSet& d, wire::Payload::DataSet& w, std::string& error) {
    w.set_num_of_columns((uint64_t)d.columns_size());
    for (const std::string& c : d.columns()) w.add_columns(c);
    for (int t : d.types()) w.add_types((uint32_t)t);
    for (const v1::SparkplugRow& row : d.rows()) {
        wire::Payload::DataSet::Row* r = w.add_rows();
        for (int i = 0; i < row.values_size(); i++) {
            Scalar s;
            if (!scalarFor(i < d.types_size() ? d.types(i) : 0, row.values(i), s, error)) return false;
            scalarInto(s, *r->add_elements());
        }
    }
    return true;
}

void templateOf(const wire::Payload::Template& w, v1::SparkplugTemplate& out, std::string& error) {
    out.set_version(w.version());
    out.set_template_ref(w.template_ref());
    out.set_is_definition(w.is_definition());
    for (const wire::Payload::Metric& m : w.metrics()) metricOf(m, *out.add_metrics(), error);
    for (const wire::Payload::Template::Parameter& p : w.parameters()) {
        v1::SparkplugParameter* param = out.add_parameters();
        param->set_name(p.name());
        param->set_type((v1::SparkplugDataType)p.type());
        valueOf(p.type(), scalarOf(p), *param->mutable_value());
    }
}

bool templateFor(const v1::SparkplugTemplate& t, wire::Payload::Template& w, std::string& error) {
    if (!t.version().empty()) w.set_version(t.version());
    if (!t.template_ref().empty()) w.set_template_ref(t.template_ref());
    w.set_is_definition(t.is_definition());
    for (const v1::SparkplugMetric& m : t.metrics()) {
        if (!metricFor(m, *w.add_metrics(), error)) return false;
    }
    for (const v1::SparkplugParameter& p : t.parameters()) {
        wire::Payload::Template::Parameter* param = w.add_parameters();
        param->set_name(p.name());
        param->set_type(p.type());
        if (p.has_value() && p.value().kind_case() != v1::SparkplugValue::KIND_NOT_SET) {
            Scalar s;
            if (!scalarFor(p.type(), p.value(), s, error)) return false;
            scalarInto(s, *param);
        }
    }
    return true;
}

bool metricOf(const wire::Payload::Metric& w, v1::SparkplugMetric& out, std::string& error) {
    out.set_name(w.name());
    if (w.has_alias()) out.set_alias(w.alias());
    if (w.has_timestamp()) out.set_timestamp(w.timestamp());
    out.set_datatype((v1::SparkplugDataType)w.datatype());
    out.set_is_historical(w.is_historical());
    out.set_is_transient(w.is_transient());
    out.set_is_null(w.is_null());
    if (w.has_metadata()) {
        const wire::Payload::MetaData& md = w.metadata();
        v1::SparkplugMetaData* m = out.mutable_metadata();
        m->set_is_multi_part(md.is_multi_part());
        m->set_content_type(md.content_type());
        m->set_size(md.size());
        m->set_seq(md.seq());
        m->set_file_name(md.file_name());
        m->set_file_type(md.file_type());
        m->set_md5(md.md5());
        m->set_description(md.description());
    }
    if (w.has_properties()) {
        v1::SparkplugPropertySet set;
        propertySetOf(w.properties(), set);
        for (const v1::SparkplugProperty& p : set.properties()) *out.add_properties() = p;
    }
    if (w.is_null()) return true;
    const uint32_t t = w.datatype();
    v1::SparkplugValue& v = *out.mutable_value();
    if (w.has_dataset_value()) {
        dataSetOf(w.dataset_value(), *v.mutable_dataset_value());
    } else if (w.has_template_value()) {
        templateOf(w.template_value(), *v.mutable_template_value(), error);
    } else if (w.has_bytes_value()) {
        if (isArray(t)) {
            std::string why;
            if (!unpack(t, w.bytes_value(), *v.mutable_array_value(), why)) {
                v.set_bytes_value(w.bytes_value());     // kept as it came, and said why
                if (error.empty()) error = "metric " + w.name() + ": " + why;
            }
        } else {
            v.set_bytes_value(w.bytes_value());
        }
    } else {
        valueOf(t, scalarOf(w), v);
    }
    return true;
}

bool metricFor(const v1::SparkplugMetric& m, wire::Payload::Metric& w, std::string& error) {
    if (!m.name().empty()) w.set_name(m.name());
    if (m.has_alias()) w.set_alias(m.alias());
    if (m.has_timestamp()) w.set_timestamp(m.timestamp());
    const uint32_t t = m.datatype();
    if (t != v1::DATATYPE_UNKNOWN) w.set_datatype(t);
    if (m.is_historical()) w.set_is_historical(true);
    if (m.is_transient()) w.set_is_transient(true);
    if (m.has_metadata()) {
        const v1::SparkplugMetaData& md = m.metadata();
        wire::Payload::MetaData* out = w.mutable_metadata();
        if (md.is_multi_part()) out->set_is_multi_part(true);
        if (!md.content_type().empty()) out->set_content_type(md.content_type());
        if (md.size()) out->set_size(md.size());
        if (md.seq()) out->set_seq(md.seq());
        if (!md.file_name().empty()) out->set_file_name(md.file_name());
        if (!md.file_type().empty()) out->set_file_type(md.file_type());
        if (!md.md5().empty()) out->set_md5(md.md5());
        if (!md.description().empty()) out->set_description(md.description());
    }
    if (m.properties_size() > 0) {
        v1::SparkplugPropertySet set;
        for (const v1::SparkplugProperty& p : m.properties()) *set.add_properties() = p;
        if (!propertySetFor(set, *w.mutable_properties(), error)) return false;
    }
    const bool none = !m.has_value() || m.value().kind_case() == v1::SparkplugValue::KIND_NOT_SET;
    if (m.is_null() || none) {
        if (m.is_null()) w.set_is_null(true);
        return true;
    }
    const v1::SparkplugValue& v = m.value();
    std::string why;
    if (t == v1::DATATYPE_DATASET) {
        if (!dataSetFor(v.dataset_value(), *w.mutable_dataset_value(), why)) goto refused;
    } else if (t == v1::DATATYPE_TEMPLATE) {
        if (!templateFor(v.template_value(), *w.mutable_template_value(), why)) goto refused;
    } else if (isArray(t)) {
        w.set_bytes_value(pack(t, v.array_value()));
    } else {
        Scalar s;
        if (!scalarFor(t, v, s, why)) goto refused;
        if (s.which == Scalar::Bytes) w.set_bytes_value(s.text);
        else scalarInto(s, w);
    }
    return true;
refused:
    error = "metric " + (m.name().empty() ? "alias " + std::to_string(m.alias()) : m.name()) + ": " + why;
    return false;
}

// --- Compression ----------------------------------------------------------------

bool inflate(const std::string& in, int windowBits, std::string& out) {
    z_stream z;
    std::memset(&z, 0, sizeof z);
    if (inflateInit2(&z, windowBits) != Z_OK) return false;
    z.next_in = (Bytef*)in.data();
    z.avail_in = (uInt)in.size();
    out.clear();
    char chunk[16384];
    int result = Z_OK;
    while (result == Z_OK) {
        z.next_out = (Bytef*)chunk;
        z.avail_out = sizeof chunk;
        result = ::inflate(&z, Z_NO_FLUSH);
        out.append(chunk, sizeof chunk - z.avail_out);
        if (out.size() > kMaxInflated) result = Z_BUF_ERROR;
    }
    inflateEnd(&z);
    return result == Z_STREAM_END;
}

bool deflate(const std::string& in, std::string& out) {
    uLongf size = compressBound((uLong)in.size());
    out.resize(size);
    if (compress2((Bytef*)&out[0], &size, (const Bytef*)in.data(), (uLong)in.size(), Z_BEST_SPEED) != Z_OK) {
        return false;
    }
    out.resize(size);
    return true;
}

} // namespace

bool isSparkplugTopic(const std::string& topic) { return topic.compare(0, 8, "spBv1.0/") == 0; }

bool decode(const std::string& topic, const std::string& payload, v1::SparkplugMessage& out,
            const std::vector<AppSparkplugName>* names) {
    std::vector<std::string> parts;
    size_t start = 0;
    for (size_t i = 0; i <= topic.size(); i++) {
        if (i == topic.size() || topic[i] == '/') {
            parts.push_back(topic.substr(start, i - start));
            start = i + 1;
        }
    }
    if (parts.size() < 3 || parts[0] != "spBv1.0") {
        out.set_error("not a Sparkplug B topic");
        return false;
    }
    if (parts[1] == "STATE") {
        out.set_kind(v1::SPARKPLUG_STATE);
        out.set_host_id(parts[2]);
        // {"online": true, "timestamp": 1700000000000}
        const bool online = payload.find("\"online\":true") != std::string::npos ||
                            payload.find("\"online\": true") != std::string::npos;
        const size_t at = payload.find("\"timestamp\"");
        if (at == std::string::npos) {
            out.set_error("a STATE payload is {\"online\": ..., \"timestamp\": ...}");
            return false;
        }
        const size_t colon = payload.find(':', at);
        out.set_online(online);
        out.set_state_timestamp(colon == std::string::npos ? 0 : std::strtoull(payload.c_str() + colon + 1, nullptr, 10));
        return true;
    }
    static const struct { const char* name; v1::SparkplugKind kind; bool device; } kKinds[] = {
        {"NBIRTH", v1::SPARKPLUG_NBIRTH, false}, {"NDEATH", v1::SPARKPLUG_NDEATH, false},
        {"DBIRTH", v1::SPARKPLUG_DBIRTH, true},  {"DDEATH", v1::SPARKPLUG_DDEATH, true},
        {"NDATA", v1::SPARKPLUG_NDATA, false},   {"DDATA", v1::SPARKPLUG_DDATA, true},
        {"NCMD", v1::SPARKPLUG_NCMD, false},     {"DCMD", v1::SPARKPLUG_DCMD, true},
    };
    bool known = false, device = false;
    for (const auto& k : kKinds) {
        if (parts.size() >= 3 && parts[2] == k.name) {
            out.set_kind(k.kind);
            known = true;
            device = k.device;
        }
    }
    out.set_group(parts[1]);
    if (!known || parts.size() != (device ? 5u : 4u)) {
        out.set_error("not a Sparkplug B topic: spBv1.0/<group>/<type>/<node>[/<device>]");
        return false;
    }
    out.set_node(parts[3]);
    if (device) out.set_device(parts[4]);

    wire::Payload w;
    if (!w.ParseFromString(payload)) {
        out.set_error("the payload is not a Sparkplug B payload");
        return false;
    }
    v1::SparkplugPayload& p = *out.mutable_payload();
    if (w.uuid() == kCompressed) {
        std::string algorithm = "DEFLATE";
        for (const wire::Payload::Metric& m : w.metrics()) {
            if (m.name() == "algorithm") algorithm = m.string_value();
        }
        for (char& c : algorithm) c = (char)std::toupper((unsigned char)c);
        std::string inner;
        const bool inflated = algorithm == "GZIP" ? inflate(w.body(), 31, inner)
                                                  : (inflate(w.body(), 15, inner) || inflate(w.body(), -15, inner));
        wire::Payload unpacked;
        if (!inflated || !unpacked.ParseFromString(inner) || unpacked.uuid() == kCompressed) {
            out.set_error("a compressed payload (" + algorithm + ") that does not inflate to one payload");
            return false;
        }
        w.Swap(&unpacked);
        p.set_compressed(true);
    }
    if (w.has_timestamp()) p.set_timestamp(w.timestamp());
    if (w.has_seq()) p.set_seq(w.seq());
    p.set_uuid(w.uuid());
    p.set_body(w.body());
    std::string error;
    for (wire::Payload::Metric& m : *w.mutable_metrics()) {
        // Alias only: the name and datatype the birth gave it, before its value is read at that datatype.
        if (names != nullptr && m.has_alias() && m.name().empty()) {
            for (const AppSparkplugName& n : *names) {
                if (n.alias != m.alias()) continue;
                if (!n.name.empty()) m.set_name(n.name);
                if (!m.has_datatype() && n.datatype != 0) m.set_datatype(n.datatype);
                break;
            }
        }
        metricOf(m, *p.add_metrics(), error);
    }
    if (!error.empty()) out.set_error(error);
    return error.empty();
}

bool encode(const v1::SparkplugPayload& payload, bool compress, std::string& out, std::string& error) {
    wire::Payload w;
    if (payload.has_timestamp()) w.set_timestamp(payload.timestamp());
    if (payload.has_seq()) w.set_seq(payload.seq());
    if (!payload.uuid().empty()) w.set_uuid(payload.uuid());
    if (!payload.body().empty()) w.set_body(payload.body());
    for (const v1::SparkplugMetric& m : payload.metrics()) {
        if (!metricFor(m, *w.add_metrics(), error)) return false;
    }
    std::string bytes;
    if (!w.SerializeToString(&bytes)) {
        error = "the payload could not be written";
        return false;
    }
    if (!compress) {
        out.swap(bytes);
        return true;
    }
    wire::Payload outer;
    if (payload.has_timestamp()) outer.set_timestamp(payload.timestamp());
    if (payload.has_seq()) outer.set_seq(payload.seq());
    outer.set_uuid(kCompressed);
    std::string body;
    if (!deflate(bytes, body)) {
        error = "the payload could not be compressed";
        return false;
    }
    outer.set_body(body);
    wire::Payload::Metric* algorithm = outer.add_metrics();
    algorithm->set_name("algorithm");
    algorithm->set_datatype(v1::DATATYPE_STRING);
    algorithm->set_string_value("DEFLATE");
    return outer.SerializeToString(&out);
}

} // namespace sparkplug_codec

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "Json.h"

#include <cstdio>
#include <cstdlib>

namespace json {

namespace {

struct Parser {
    const std::string& s;
    size_t at = 0;
    int depth = 0;
    explicit Parser(const std::string& text) : s(text) {}

    void space() { while (at < s.size() && (s[at] == ' ' || s[at] == '\t' || s[at] == '\n' || s[at] == '\r')) at++; }
    bool literal(const char* word) {
        const size_t n = std::char_traits<char>::length(word);
        if (s.compare(at, n, word) != 0) return false;
        at += n;
        return true;
    }

    bool string(std::string& out) {
        if (at >= s.size() || s[at] != '"') return false;
        at++;
        out.clear();
        while (at < s.size()) {
            const char c = s[at++];
            if (c == '"') return true;
            if ((unsigned char)c < 0x20) return false;
            if (c != '\\') { out.push_back(c); continue; }
            if (at >= s.size()) return false;
            const char e = s[at++];
            switch (e) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case 'u': {
                    if (at + 4 > s.size()) return false;
                    unsigned code = (unsigned)std::strtoul(s.substr(at, 4).c_str(), nullptr, 16);
                    at += 4;
                    // UTF-8; a surrogate pair is joined.
                    if (code >= 0xD800 && code <= 0xDBFF && s.compare(at, 2, "\\u") == 0) {
                        const unsigned low = (unsigned)std::strtoul(s.substr(at + 2, 4).c_str(), nullptr, 16);
                        if (low >= 0xDC00 && low <= 0xDFFF) {
                            code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                            at += 6;
                        }
                    }
                    if (code < 0x80) out.push_back((char)code);
                    else if (code < 0x800) { out.push_back((char)(0xC0 | (code >> 6))); out.push_back((char)(0x80 | (code & 63))); }
                    else if (code < 0x10000) { out.push_back((char)(0xE0 | (code >> 12))); out.push_back((char)(0x80 | ((code >> 6) & 63))); out.push_back((char)(0x80 | (code & 63))); }
                    else { out.push_back((char)(0xF0 | (code >> 18))); out.push_back((char)(0x80 | ((code >> 12) & 63))); out.push_back((char)(0x80 | ((code >> 6) & 63))); out.push_back((char)(0x80 | (code & 63))); }
                    break;
                }
                default: return false;
            }
        }
        return false;
    }

    bool value(Value& out) {
        if (++depth > 32) return false;
        space();
        if (at >= s.size()) return false;
        const char c = s[at];
        bool ok = false;
        if (c == '{') {
            at++;
            out.type = Value::Object;
            space();
            if (at < s.size() && s[at] == '}') { at++; ok = true; }
            else {
                for (;;) {
                    space();
                    std::string key;
                    if (!string(key)) break;
                    space();
                    if (at >= s.size() || s[at] != ':') break;
                    at++;
                    Value item;
                    if (!value(item)) break;
                    out.fields[key] = item;
                    space();
                    if (at < s.size() && s[at] == ',') { at++; continue; }
                    if (at < s.size() && s[at] == '}') { at++; ok = true; }
                    break;
                }
            }
        } else if (c == '[') {
            at++;
            out.type = Value::Array;
            space();
            if (at < s.size() && s[at] == ']') { at++; ok = true; }
            else {
                for (;;) {
                    Value item;
                    if (!value(item)) break;
                    out.items.push_back(item);
                    space();
                    if (at < s.size() && s[at] == ',') { at++; continue; }
                    if (at < s.size() && s[at] == ']') { at++; ok = true; }
                    break;
                }
            }
        } else if (c == '"') {
            out.type = Value::String;
            ok = string(out.text);
        } else if (literal("true")) { out.type = Value::Bool; out.boolean = true; ok = true; }
        else if (literal("false")) { out.type = Value::Bool; out.boolean = false; ok = true; }
        else if (literal("null")) { out.type = Value::Null; ok = true; }
        else if (c == '-' || (c >= '0' && c <= '9')) {
            const char* start = s.c_str() + at;
            char* end = nullptr;
            out.number = std::strtod(start, &end);
            out.type = Value::Number;
            ok = end != start;
            at += (size_t)(end - start);
        }
        depth--;
        return ok;
    }
};

const Value kNull;

} // namespace

const Value& Value::operator[](const std::string& key) const {
    if (type != Object) return kNull;
    auto found = fields.find(key);
    return found == fields.end() ? kNull : found->second;
}

std::string Value::str(const std::string& key, const std::string& fallback) const {
    const Value& v = (*this)[key];
    return v.type == String ? v.text : fallback;
}

long long Value::integer(const std::string& key, long long fallback) const {
    const Value& v = (*this)[key];
    return v.type == Number ? (long long)v.number : fallback;
}

bool Value::flag(const std::string& key, bool fallback) const {
    const Value& v = (*this)[key];
    return v.type == Bool ? v.boolean : fallback;
}

bool Value::strings(const std::string& key, std::vector<std::string>& out) const {
    const Value& v = (*this)[key];
    if (v.type == Null) return true;
    if (v.type != Array) return false;
    out.clear();
    for (const Value& item : v.items) {
        if (item.type != String) return false;
        out.push_back(item.text);
    }
    return true;
}

bool parse(const std::string& text, Value& out) {
    Parser p(text);
    out = Value();
    if (!p.value(out)) return false;
    p.space();
    return p.at == text.size();
}

std::string quote(const std::string& text) {
    std::string out = "\"";
    for (unsigned char c : text) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char escaped[8];
                    std::snprintf(escaped, sizeof escaped, "\\u%04x", c);
                    out += escaped;
                } else {
                    out.push_back((char)c);
                }
        }
    }
    return out + "\"";
}

std::string array(const std::vector<std::string>& items) {
    std::string out = "[";
    for (size_t i = 0; i < items.size(); i++) {
        if (i) out += ",";
        out += quote(items[i]);
    }
    return out + "]";
}

void Object::key(const std::string& name) {
    if (!_body.empty()) _body += ",";
    _body += quote(name) + ":";
}
Object& Object::add(const std::string& name, const std::string& value) { key(name); _body += quote(value); return *this; }
Object& Object::add(const std::string& name, long long value) { key(name); _body += std::to_string(value); return *this; }
Object& Object::add(const std::string& name, bool value) { key(name); _body += value ? "true" : "false"; return *this; }
Object& Object::raw(const std::string& name, const std::string& json) { key(name); _body += json; return *this; }

} // namespace json

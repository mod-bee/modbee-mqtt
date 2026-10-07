// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// Just enough JSON for the admin and provisioning messages: objects, arrays,
// strings, numbers, true/false/null. Parsing never throws (the server is
// built without exceptions); a document that is not JSON parses to false.

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace json {

struct Value {
    enum Type { Null, Bool, Number, String, Array, Object } type = Null;
    bool boolean = false;
    double number = 0;
    std::string text;
    std::vector<Value> items;
    std::map<std::string, Value> fields;

    // Convenience for reading a request. A missing field reads as null.
    const Value& operator[](const std::string& key) const;
    bool has(const std::string& key) const { return type == Object && fields.count(key) != 0; }
    std::string str(const std::string& key, const std::string& fallback = std::string()) const;
    long long integer(const std::string& key, long long fallback = 0) const;
    bool flag(const std::string& key, bool fallback = false) const;
    // An array of strings; false if the field is present and is not one.
    bool strings(const std::string& key, std::vector<std::string>& out) const;
};

bool parse(const std::string& text, Value& out);

// Writing: build a document as text.
std::string quote(const std::string& text);
std::string array(const std::vector<std::string>& items);

class Object {
public:
    Object& add(const std::string& key, const std::string& value);
    Object& add(const std::string& key, const char* value) { return add(key, std::string(value)); }
    Object& add(const std::string& key, long long value);
    Object& add(const std::string& key, bool value);
    Object& raw(const std::string& key, const std::string& json);   // already JSON
    std::string str() const { return "{" + _body + "}"; }
private:
    void key(const std::string& name);
    std::string _body;
};

} // namespace json

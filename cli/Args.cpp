// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "Args.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

const ArgSpec* findLong(const std::vector<ArgSpec>& specs, const std::string& name) {
    for (const ArgSpec& spec : specs) {
        if (name == spec.name) return &spec;
    }
    return nullptr;
}

const ArgSpec* findShort(const std::vector<ArgSpec>& specs, char name) {
    for (const ArgSpec& spec : specs) {
        if (spec.shortName != 0 && spec.shortName == name) return &spec;
    }
    return nullptr;
}

} // namespace

bool Args::parse(int argc, char** argv, int first, const std::vector<ArgSpec>& specs) {
    _values.clear();
    _positional.clear();
    _error.clear();

    for (int index = first; index < argc; index++) {
        const std::string arg = argv[index];

        // "--" ends the options, so a payload may begin with a dash.
        if (arg == "--") {
            for (index++; index < argc; index++) _positional.push_back(argv[index]);
            break;
        }

        const ArgSpec* spec = nullptr;
        std::string inlineValue;
        bool hasInline = false;

        if (arg.size() > 2 && arg[0] == '-' && arg[1] == '-') {
            std::string name = arg.substr(2);
            const size_t equals = name.find('=');
            if (equals != std::string::npos) {
                inlineValue = name.substr(equals + 1);
                hasInline = true;
                name = name.substr(0, equals);
            }
            spec = findLong(specs, name);
            if (spec == nullptr) {
                _error = "unknown option --" + name;
                return false;
            }
        } else if (arg.size() == 2 && arg[0] == '-' && arg[1] != '-') {
            spec = findShort(specs, arg[1]);
            if (spec == nullptr) {
                _error = "unknown option " + arg;
                return false;
            }
        } else {
            _positional.push_back(arg);
            continue;
        }

        Value value;
        value.name = spec->name;
        if (spec->takesValue) {
            if (hasInline) {
                value.value = inlineValue;
            } else if (index + 1 < argc) {
                value.value = argv[++index];
            } else {
                _error = std::string("--") + spec->name + " needs a value";
                return false;
            }
        } else if (hasInline) {
            _error = std::string("--") + spec->name + " takes no value";
            return false;
        }
        _values.push_back(value);
    }
    return true;
}

bool Args::has(const char* name) const {
    for (const Value& value : _values) {
        if (value.name == name) return true;
    }
    return false;
}

std::string Args::get(const char* name, const char* fallback) const {
    // The LAST occurrence wins, as it does for every Unix tool, so an option
    // set in a script can be overridden by appending it again.
    for (size_t index = _values.size(); index > 0; index--) {
        if (_values[index - 1].name == name) return _values[index - 1].value;
    }
    return fallback;
}

std::vector<std::string> Args::all(const char* name) const {
    std::vector<std::string> out;
    for (const Value& value : _values) {
        if (value.name == name) out.push_back(value.value);
    }
    return out;
}

bool Args::getInt(const char* name, long long fallback, long long minimum,
                  long long maximum, long long& out) {
    if (!has(name)) {
        out = fallback;
        return true;
    }
    const std::string text = get(name);
    errno = 0;
    char* end = nullptr;
    const long long parsed = std::strtoll(text.c_str(), &end, 10);
    if (text.empty() || end == nullptr || *end != '\0' || errno == ERANGE) {
        _error = std::string("--") + name + " wants a whole number, not '" + text + "'";
        return false;
    }
    if (parsed < minimum || parsed > maximum) {
        _error = std::string("--") + name + " must be between " +
                 std::to_string(minimum) + " and " + std::to_string(maximum);
        return false;
    }
    out = parsed;
    return true;
}

void Args::printHelp(const char* usage, const std::vector<ArgSpec>& specs) {
    std::printf("usage: %s\n\n", usage);
    for (const ArgSpec& spec : specs) {
        char left[64];
        if (spec.shortName != 0) {
            std::snprintf(left, sizeof left, "-%c, --%s%s", spec.shortName, spec.name,
                          spec.takesValue ? " <v>" : "");
        } else {
            std::snprintf(left, sizeof left, "    --%s%s", spec.name,
                          spec.takesValue ? " <v>" : "");
        }
        std::printf("  %-28s %s\n", left, spec.help);
    }
}

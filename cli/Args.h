// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// A small command-line parser, written here rather than taken from somewhere.
//
// The CLI builds with -fno-exceptions like the rest of this repository, which
// rules out most option libraries, and what it needs is small: long options,
// a few short aliases, values that may repeat, and an error that names the
// option it is about. getopt_long would do most of it and is not on every
// host this builds on.

#include <cstdint>
#include <string>
#include <vector>

struct ArgSpec {
    const char* name;        // long form, without the dashes: "topic"
    char shortName;          // 't', or 0 for none
    bool takesValue;
    const char* help;
};

class Args {
public:
    // Parses argv[first..]. False on an unknown option or a missing value,
    // with error() saying which.
    bool parse(int argc, char** argv, int first, const std::vector<ArgSpec>& specs);

    bool has(const char* name) const;
    std::string get(const char* name, const char* fallback = "") const;
    // Every value given for an option that may repeat, in order.
    std::vector<std::string> all(const char* name) const;

    // Integer options. False, with error() set, when the value is not a
    // number or does not fit - "--qos banana" is a mistake to report, not a 0.
    bool getInt(const char* name, long long fallback, long long minimum,
                long long maximum, long long& out);

    const std::vector<std::string>& positional() const { return _positional; }
    const std::string& error() const { return _error; }

    static void printHelp(const char* usage, const std::vector<ArgSpec>& specs);

private:
    struct Value { std::string name; std::string value; };
    std::vector<Value> _values;
    std::vector<std::string> _positional;
    std::string _error;
};

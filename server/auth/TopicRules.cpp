// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "TopicRules.h"

#include <vector>

namespace {

// What may be put into a filter: no wildcard, no level separator, no `$`
// first, no control character.
bool safeValue(const std::string& value) {
    if (value.empty() || value.size() > 65535 || value[0] == '$') return false;
    for (unsigned char c : value) {
        if (c < 0x20 || c == 0x7F || c == '/' || c == '+' || c == '#') return false;
    }
    return true;
}

std::vector<std::string> levels(const std::string& s) {
    std::vector<std::string> out;
    size_t from = 0;
    for (;;) {
        const size_t next = s.find('/', from);
        out.push_back(s.substr(from, next - from));
        if (next == std::string::npos) return out;
        from = next + 1;
    }
}

} // namespace

// ${username} and ${clientid}, or false if either would put a wildcard or a
// level separator into a filter.
//
// ONE PASS OVER THE PATTERN: a value is copied in and never read again. The
// two used to be replaced one after the other, the second searched for in the
// result of the first - so a login named "t${clientid}" connecting as "ank-07"
// had "plant/${username}/#" become "plant/tank-07/#", another device's.
bool expand(const std::string& pattern, const std::string& username, const std::string& clientId,
            std::string& out) {
    static const char kUser[] = "${username}";
    static const char kClient[] = "${clientid}";
    out.clear();
    size_t at = 0;
    while (at < pattern.size()) {
        const std::string* value = nullptr;
        size_t length = 0;
        if (pattern.compare(at, sizeof kUser - 1, kUser) == 0) {
            value = &username;
            length = sizeof kUser - 1;
        } else if (pattern.compare(at, sizeof kClient - 1, kClient) == 0) {
            value = &clientId;
            length = sizeof kClient - 1;
        }
        if (value == nullptr) {
            out.push_back(pattern[at++]);
            continue;
        }
        if (!safeValue(*value)) return false;
        out += *value;
        at += length;
    }
    return true;
}

// Whether every topic `requested` can match is one `allowed` matches too - so
// granting the subscription gives nothing the permission does not.
bool covers(const std::string& allowed, const std::string& requested) {
    const std::vector<std::string> a = levels(allowed), r = levels(requested);
    for (size_t i = 0; i < r.size(); i++) {
        if (i >= a.size()) return false;
        if (a[i] == "#") return true;
        if (a[i] == "+") {
            if (r[i] == "#") return false;
            continue;
        }
        if (r[i] == "+" || r[i] == "#" || a[i] != r[i]) return false;
    }
    // "a/#" also covers "a" itself [MQTT-4.7.1-2].
    return a.size() == r.size() || (a.size() == r.size() + 1 && a.back() == "#");
}

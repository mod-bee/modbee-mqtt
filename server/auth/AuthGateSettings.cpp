// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// --- Settings: config.* --------------------------------------------------------
//
// config.get     the settings: where they come from (the file, or a document
//                stored here), every override, what the environment pins,
//                whether the running server is behind them, and what TLS is
//                serving now
// config.check   would these be accepted? {document?, set?, remove?} -> problems
// config.set     {set: {"broker.port": 1884, ...}, remove?: [paths], restart?}
// config.apply   {document: "<yaml>", restart?} - a whole document, in place of the file
// config.reset   back to the file alone
//
// Everything is validated by the loader the file goes through, and refused with
// its problems. Authentication (the database and the admin login) is the
// file's and the environment's alone: a setting that locked every admin out
// could not be undone over MQTT. A change applies when the server restarts;
// "restart": true restarts it once the reply is sent (under Docker, the restart
// policy brings it back).
//
// THE ONE EXCEPTION IS THE TLS MATERIAL. A certificate is renewed every few
// months, and a broker a plant depends on is not restarted for that. The
// certificate, its key, the client CA and the client-certificate mode
// (TlsSettings.h) are applied to the running listeners as soon as they are
// stored, and so never make restart_required true while TLS is being served.

#include "AuthGate.h"
#include "AdminReply.h"
#include "TlsSettings.h"

#include <cstdio>

using admin_reply::failure;
using admin_reply::reply;

namespace {

const char kDocumentSetting[] = "config.document";
const char kOverridePrefix[] = "config.override.";

// What the server was started from, as text that changes whenever anything a
// restart would pick up changes. With `skipLive`, the settings a running
// server applies by itself are left out - they never put it behind.
std::string fingerprint(const StoredSettings& s, bool skipLive) {
    std::string out = s.hasDocument ? "doc:" + s.document : std::string("file");
    for (const auto& o : s.overrides) {
        if (skipLive && isLiveTlsSetting(o.first)) continue;
        out += "\n" + o.first + "=" + o.second;
    }
    return out;
}

bool endsWith(const std::string& s, const char* tail) {
    const size_t n = std::char_traits<char>::length(tail);
    return s.size() >= n && s.compare(s.size() - n, n, tail) == 0;
}

// A setting whose value is never shown: a password, and a private key.
bool isSecretPath(const std::string& path) {
    return endsWith(path, "password") || endsWith(path, "key-pem");
}

// Every "password: <value>" and "key-pem: <value>" line, whatever its indent,
// with the value hidden. A key is PEM, so it is usually a block scalar
// ("key-pem: |" and the lines under it): everything indented deeper than the
// key goes with it.
std::string redact(const std::string& yaml) {
    std::string out;
    size_t from = 0;
    size_t hiding = std::string::npos;     // the indent of a key-pem whose block is being skipped
    while (from <= yaml.size()) {
        size_t end = yaml.find('\n', from);
        if (end == std::string::npos) end = yaml.size();
        std::string line = yaml.substr(from, end - from);
        const size_t indent = line.find_first_not_of(" -");
        if (hiding != std::string::npos) {
            const bool blank = line.find_first_not_of(" \t\r") == std::string::npos;
            if (blank || (indent != std::string::npos && indent > hiding)) {
                from = end + 1;
                continue;
            }
            hiding = std::string::npos;
        }
        for (const char* secret : {"password:", "key-pem:"}) {
            const size_t key = line.find(secret);
            if (key != std::string::npos && indent == key) {
                const size_t length = std::char_traits<char>::length(secret);
                if (std::string(secret) == "key-pem:") hiding = key;
                line = line.substr(0, key + length) + " (set)";
            }
        }
        out += line;
        if (end < yaml.size()) out += "\n";
        from = end + 1;
    }
    return out;
}

std::string problemsJson(const std::vector<ConfigProblem>& problems) {
    std::string out = "[";
    for (size_t i = 0; i < problems.size(); i++) {
        json::Object o;
        o.add("line", (long long)problems[i].line)
            .add("path", problems[i].path)
            .add("message", problems[i].message)
            .add("warning", problems[i].warning);
        if (i) out += ",";
        out += o.str();
    }
    return out + "]";
}

// A request's value as the text a setting takes: "1884", "true", "a,b".
bool settingText(const json::Value& v, std::string& out) {
    switch (v.type) {
    case json::Value::String: out = v.text; return true;
    case json::Value::Bool: out = v.boolean ? "true" : "false"; return true;
    case json::Value::Number: {
        char buf[64];
        if (v.number == (double)(long long)v.number) std::snprintf(buf, sizeof buf, "%lld", (long long)v.number);
        else std::snprintf(buf, sizeof buf, "%.17g", v.number);
        out = buf;
        return true;
    }
    case json::Value::Array:
        out.clear();
        for (size_t i = 0; i < v.items.size(); i++) {
            std::string item;
            if (!settingText(v.items[i], item)) return false;
            out += (i ? "," : "") + item;
        }
        return true;
    default: return false;
    }
}

bool sameAuthentication(const AuthenticationSection& a, const AuthenticationSection& b) {
    if (a.database != b.database || a.hasAdmin != b.hasAdmin || a.accounts.size() != b.accounts.size() ||
        a.certificateLogin != b.certificateLogin || a.defaultTopics != b.defaultTopics) {
        return false;
    }
    for (size_t i = 0; i < a.accounts.size(); i++) {
        if (a.accounts[i].username != b.accounts[i].username || a.accounts[i].password != b.accounts[i].password) return false;
    }
    return true;
}

} // namespace

StoredSettings AuthGate::storedSettings() {
    StoredSettings s;
    s.hasDocument = _store.getSetting(kDocumentSetting, s.document);
    std::vector<std::pair<std::string, std::string>> rows;
    _store.listSettings(kOverridePrefix, rows);
    for (const auto& r : rows) s.overrides.emplace_back(r.first.substr(sizeof kOverridePrefix - 1), r.second);
    return s;
}

void AuthGate::rememberRunningSettings() { _running = storedSettings(); }

std::string AuthGate::fileText() const {
    if (_config.configFile.empty()) return std::string();
    std::FILE* f = std::fopen(_config.configFile.c_str(), "rb");
    if (!f) return std::string();
    std::string text;
    char buf[4096];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof buf, f)) > 0) text.append(buf, n);
    std::fclose(f);
    return text;
}

ShardedTlsConfig AuthGate::servedTls() const {
    return _config.server != nullptr ? _config.server->servedTls() : ShardedTlsConfig();
}

// Whether what is stored differs from what the server is running in anything
// only a restart can apply. TLS turned on or off is one of those - the
// listener's socket is bound at the start - and the TLS material, while TLS
// is being served, is not.
bool AuthGate::behind(const StoredSettings& stored, const ServerConfig& effective) const {
    const bool serving = servedTls().enabled;
    if (fingerprint(stored, serving) != fingerprint(_running, serving)) return true;
    return _config.server != nullptr && effective.broker.tls != serving;
}

// After a change is stored: the TLS material the settings now name, given to
// the running listeners if it differs from what they serve. Nothing to do when
// TLS is not being served, or when the settings would turn it off - both wait
// for the restart. A failure is reported as a problem: the change is stored,
// and the listeners keep serving what they had.
bool AuthGate::applyTlsLive(const ServerConfig& effective, std::vector<ConfigProblem>& problems) {
    const ShardedTlsConfig served = servedTls();
    if (!served.enabled || !effective.broker.tls || _config.server == nullptr) return false;
    ShardedTlsConfig wanted;
    std::string error;
    if (tlsFromSettings(effective.broker, wanted, error) && !sameTlsMaterial(wanted, served) &&
        _config.server->replaceTls(wanted, error)) {
        std::fprintf(stdout, "modbee serve: the TLS material changed - new handshakes get it, connections already up keep theirs\n");
        std::fflush(stdout);
        return true;
    }
    if (!error.empty()) {
        ConfigProblem p;
        p.path = "broker.tls";
        p.message = "stored, but not applied to the running listener: " + error;
        p.warning = true;
        problems.push_back(p);
    }
    return false;
}

std::string AuthGate::configCommand(const std::string& command, const json::Value& request) {
    const std::string fileName = _config.configFile.empty() ? std::string("(no file)") : _config.configFile;
    const std::string file = fileText();
    const StoredSettings current = storedSettings();

    if (command == "config.get") {
        ServerConfig effective;
        std::vector<ConfigProblem> problems;
        EffectiveConfigInfo info;
        loadEffectiveConfig(file, fileName, current, effective, problems, &info);
        json::Object overrides;
        for (const auto& o : current.overrides) overrides.add(o.first, isSecretPath(o.first) ? "(set)" : o.second);
        json::Object body;
        body.add("source", current.hasDocument ? "database" : "file")
            .add("file", _config.configFile)
            .add("document", redact(current.hasDocument ? current.document : file))
            .raw("overrides", overrides.str())
            .raw("pinned", json::array(info.pinned))
            .raw("problems", problemsJson(problems))
            .add("restart_required", behind(current, effective))
            .raw("tls", describeTls(servedTls(), _config.certificateLogin));
        return reply(request, true, body);
    }
    if (command == "config.reset") {
        _store.removeSetting(kDocumentSetting);
        for (const auto& o : current.overrides) _store.removeSetting(kOverridePrefix + o.first);
    } else if (command != "config.check" && command != "config.set" && command != "config.apply") {
        return failure(request, "unknown command " + command);
    }

    // The candidate: what is stored, with this request's changes.
    StoredSettings candidate = command == "config.reset" ? StoredSettings() : current;
    if (request.has("document")) {
        if (request["document"].type != json::Value::String) return failure(request, "document is YAML text");
        candidate.hasDocument = true;
        candidate.document = request.str("document");
    }
    std::vector<std::string> removed;
    if (request.has("remove") && !request.strings("remove", removed)) return failure(request, "remove is a list of setting paths");
    for (const std::string& path : removed) {
        for (size_t i = 0; i < candidate.overrides.size(); i++) {
            if (candidate.overrides[i].first == path) candidate.overrides.erase(candidate.overrides.begin() + (long)i--);
        }
    }
    std::vector<std::pair<std::string, std::string>> set;
    if (request.has("set")) {
        const json::Value& s = request["set"];
        if (s.type != json::Value::Object) return failure(request, "set is an object of setting paths and values");
        for (const auto& f : s.fields) {
            std::string value;
            if (!settingText(f.second, value)) return failure(request, "the value of " + f.first + " is not a setting value");
            set.emplace_back(f.first, value);
        }
    }
    for (const auto& kv : set) {
        bool replaced = false;
        for (auto& o : candidate.overrides) if (o.first == kv.first) { o.second = kv.second; replaced = true; }
        if (!replaced) candidate.overrides.push_back(kv);
    }

    ServerConfig next, now;
    std::vector<ConfigProblem> problems, ignored;
    for (const auto& kv : set) {
        if (kv.first.compare(0, 14, "authentication") == 0) {
            ConfigProblem p;
            p.path = kv.first;
            p.message = "authentication is set in the file and the environment only - a change over MQTT could lock every admin out";
            problems.push_back(p);
        }
    }
    bool valid = problems.empty() && loadEffectiveConfig(file, fileName, candidate, next, problems);
    loadEffectiveConfig(file, fileName, StoredSettings(), now, ignored);
    if (valid && !sameAuthentication(next.authentication, now.authentication)) {
        ConfigProblem p;
        p.path = "authentication";
        p.message = "authentication is set in the file and the environment only - a change over MQTT could lock every admin out";
        problems.push_back(p);
        valid = false;
    }
    if (command == "config.check") {
        json::Object body;
        body.raw("problems", problemsJson(problems));
        return reply(request, valid, body);
    }
    if (!valid) {
        json::Object body;
        body.raw("problems", problemsJson(problems));
        body.add("error", "the settings were not stored: they would not load");
        return reply(request, false, body);
    }
    if (command == "config.apply" &&
        _store.putSetting(kDocumentSetting, candidate.document) != AuthResult::Ok) {
        return failure(request, "the settings could not be stored");
    }
    for (const std::string& path : removed) _store.removeSetting(kOverridePrefix + path);
    for (const auto& kv : set) {
        if (_store.putSetting(kOverridePrefix + kv.first, kv.second) != AuthResult::Ok) {
            return failure(request, "the settings could not be stored");
        }
    }
    const bool tlsApplied = applyTlsLive(next, problems);
    const bool restartNeeded = behind(storedSettings(), next);
    const bool restarting = restartNeeded && request.flag("restart") && _config.restart != nullptr;
    if (restarting) _restartAfterReply = true;
    json::Object body;
    body.raw("problems", problemsJson(problems))
        .add("restart_required", restartNeeded && !restarting)
        .add("restarting", restarting)
        .add("tls_applied", tlsApplied);
    return reply(request, true, body);
}

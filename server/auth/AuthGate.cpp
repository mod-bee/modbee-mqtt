// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AuthGate.h"
#include "ServerOs.h"
#include "TopicRules.h"
#include "AdminReply.h"
#include "AuthHash.h"
#include "Json.h"

#include "MqttTopic.h"

#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <algorithm>

using admin_reply::failure;
using admin_reply::reply;

namespace {

const char kProvisionUser[] = "$provision";
const char kAdminRequest[] = "$modbee/admin/request";
const char kProvisionPrefix[] = "$modbee/provision/";

// Cached permissions per shard are bounded; past this the cache starts again.
const size_t kCacheLimit = 100000;

std::string text(const MqttSpan& span) {
    return span.data && span.length ? std::string((const char*)span.data, span.length) : std::string();
}

MqttSpan spanOf(const std::string& s) {
    MqttSpan span;
    span.data = (const uint8_t*)s.data();
    span.length = (uint32_t)s.size();
    return span;
}

bool startsWith(const std::string& s, const char* prefix) {
    return s.compare(0, std::strlen(prefix), prefix) == 0;
}

int64_t nowSeconds() {
    return (int64_t)std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// A user name, device name or key name that can sit inside a topic: no
// wildcard, no level separator, no leading $, no control characters - so
// expanding ${username} into "plant/${username}/#" can never widen it.
bool safeName(const std::string& name, size_t maximum = 128) {
    if (name.empty() || name.size() > maximum || name[0] == '$') return false;
    for (unsigned char c : name) {
        if (c < 0x20 || c == 0x7F || c == '/' || c == '+' || c == '#') return false;
    }
    return true;
}

// A LOGIN NAME: anything MQTT allows in a user name, except a leading $ and
// control characters. "/" IS allowed - a platform names its devices
// "<namespace>/<device>" - and that is safe because a ${username} rule never
// widens: expand() refuses to put a value with / + or # into a filter, so the
// rule simply does not match for such a login (Mosquitto's answer to
// CVE-2017-7650). Such a login gets its topics from explicit rules instead.
// A name that BECOMES a topic - a device registering itself, a key's device -
// keeps the strict safeName.
bool loginNameOk(const std::string& name) {
    if (name.empty() || name.size() > 256 || name[0] == '$') return false;
    for (unsigned char c : name) {
        if (c < 0x20 || c == 0x7F) return false;
    }
    return true;
}

bool keyNameOk(const std::string& name) {
    if (name.empty() || name.size() > 64) return false;
    for (char c : name) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              c == '-' || c == '_')) {
            return false;
        }
    }
    return true;
}

bool filtersOk(const std::vector<std::string>& filters) {
    for (const std::string& f : filters) {
        if (f.find('\n') != std::string::npos || !mq_topicFilterIsValid(spanOf(f))) return false;
    }
    return true;
}

// A certificate fingerprint as somebody pasted it - colons, spaces, upper
// case, and openssl's own "sha256 Fingerprint=" in front - reduced to the 64
// lowercase hexadecimal characters a verified certificate is compared by.
// Empty stays empty: no certificate bound. False if what is left is not
// SHA-256, because a fingerprint that can never match locks a device out
// with no message saying why.
bool certificateFingerprint(const std::string& typed, std::string& out) {
    out.clear();
    const size_t equals = typed.rfind('=');
    const std::string value = equals == std::string::npos ? typed : typed.substr(equals + 1);
    for (char c : value) {
        if (c == ':' || c == ' ' || c == '-' || c == '\t' || c == '\n' || c == '\r') continue;
        if (c >= 'A' && c <= 'F') c = (char)(c - 'A' + 'a');
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
        out.push_back(c);
    }
    return out.empty() || out.size() == 64;
}

const char kFingerprintError[] =
    "fingerprint is the SHA-256 of the device's certificate: 64 hexadecimal characters "
    "(openssl x509 -noout -fingerprint -sha256 -in device.crt), or \"\" to clear it";

// A login's or key's data as a JSON object (always one, {} when empty).
std::string dataJson(const std::map<std::string, std::string>& data) {
    json::Object o;
    for (const auto& item : data) o.add(item.first, item.second);
    return o.str();
}

// The data of an admin request: an object of strings, names of 1-32 letters, digits, - _ . and values of at
// most 255 bytes, 16 at most. False, with `error` said, if it is anything else.
bool requestData(const json::Value& request, std::map<std::string, std::string>& out, std::string& error) {
    const json::Value& d = request["data"];
    out.clear();
    if (d.type == json::Value::Null) return true;
    error = "data is an object of up to 16 names (1-32 letters, digits, - _ .) and text values (255 bytes at most)";
    if (d.type != json::Value::Object || d.fields.size() > 16) return false;
    for (const auto& field : d.fields) {
        const std::string& name = field.first;
        if (name.empty() || name.size() > 32 || field.second.type != json::Value::String || field.second.text.size() > 255) {
            return false;
        }
        for (char c : name) {
            if (!std::isalnum((unsigned char)c) && c != '-' && c != '_' && c != '.') return false;
        }
        out[name] = field.second.text;
    }
    return true;
}

std::string describeUser(const AuthUser& u) {
    json::Object o;
    o.add("username", u.username)
        .add("enabled", u.enabled)
        .add("pending", u.pending)
        .add("admin", u.admin)
        .add("password", u.generated ? "generated" : "chosen")
        .raw("publish", json::array(u.publish))
        .raw("subscribe", json::array(u.subscribe))
        .add("provisioned_by", u.provisionedBy)
        .add("site", u.site)
        .add("created", (long long)u.created)
        .add("updated", (long long)u.updated)
        .add("fingerprint", u.fingerprint)
        .raw("data", dataJson(u.data));
    return o.str();
}

std::string describeKey(const AuthKey& k) {
    json::Object o;
    o.add("name", k.name)
        .add("approval", k.manualApproval ? "manual" : "auto")
        .raw("publish", json::array(k.publish))
        .raw("subscribe", json::array(k.subscribe))
        .add("devices", (long long)k.devices)
        .add("expires", (long long)k.expires)
        .add("device", k.device)
        .add("site", k.site)
        .add("created", (long long)k.created)
        .raw("data", dataJson(k.data));
    return o.str();
}

std::string stamp() {
    const std::tm utc = server_os::utcTime(std::time(nullptr));
    char out[32];
    std::strftime(out, sizeof out, "%Y%m%d-%H%M%S", &utc);
    return out;
}

} // namespace

AuthGate::AuthGate(const AuthGateConfig& config) : _config(config) {
    if (_config.backupDirectory.empty()) {
        const size_t slash = _config.database.rfind('/');
        _config.backupDirectory =
            (slash == std::string::npos ? std::string(".") : _config.database.substr(0, slash)) + "/backups";
    }
}

AuthGate::~AuthGate() {
    {
        std::lock_guard<std::mutex> lock(_backupMutex);
        _quit = true;
    }
    _backupWake.notify_all();
    if (_backups.joinable()) _backups.join();
}

bool AuthGate::begin(size_t shards, std::string& error) {
    if (!_store.open(_config.database, shards, error)) return false;
    rememberRunningSettings();
    _shards.clear();
    for (size_t i = 0; i < shards; i++) {
        std::unique_ptr<Shard> shard(new Shard());
        shard->gate = this;
        shard->index = (uint32_t)i;
        _shards.push_back(std::move(shard));
    }
    _stopped = 0;
    if (!_backups.joinable()) _backups = std::thread([this] { backupLoop(); });
    return true;
}

void AuthGate::prepare(uint32_t shard, MqttBroker& broker) {
    broker.onAuthorise(&AuthGate::hook, _shards[shard].get());
    broker.onConnection(&AuthGate::connectionHook, _shards[shard].get());
}

void AuthGate::connectionHook(void* context, const MqttSpan& clientIdSpan, bool connected, uint8_t) {
    AuthGate& gate = *static_cast<Shard*>(context)->gate;
    const std::string clientId = text(clientIdSpan);
    if (clientId.empty()) return;
    std::lock_guard<std::mutex> lock(gate._onlineMutex);
    if (connected) {
        auto pending = gate._connecting.find(clientId);
        if (pending == gate._connecting.end()) return;
        auto& entry = gate._online[clientId];
        entry.first = pending->second;
        entry.second++;
        gate._connecting.erase(pending);
        return;
    }
    auto at = gate._online.find(clientId);
    if (at != gate._online.end() && --at->second.second == 0) gate._online.erase(at);
}

bool AuthGate::online(const std::string& username) {
    std::lock_guard<std::mutex> lock(_onlineMutex);
    for (const auto& one : _online) {
        if (one.second.first == username) return true;
    }
    return false;
}

size_t AuthGate::onlineOf(const std::string& keyName, const std::string& except) {
    std::vector<std::string> names;
    {
        std::lock_guard<std::mutex> lock(_onlineMutex);
        for (const auto& one : _online) {
            if (one.second.first != except && one.second.first != kProvisionUser) names.push_back(one.second.first);
        }
    }
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    size_t n = 0;
    for (const std::string& name : names) {
        AuthUser u;
        if (_store.findUser(name, u) && u.provisionedBy == keyName) n++;
    }
    return n;
}

bool AuthGate::hook(void* context, const MqttBrokerRequest& request, uint8_t& reasonCode) {
    Shard& shard = *static_cast<Shard*>(context);
    return shard.gate->authorise(shard, request, reasonCode);
}

bool AuthGate::fixedLogin(const std::vector<std::pair<std::string, std::string>>& list,
                          const MqttSpan& user, const MqttSpan& password) const {
    for (const auto& login : list) {
        if (login.first.size() != user.length ||
            std::memcmp(login.first.data(), user.data, user.length) != 0) {
            continue;
        }
        // Constant time over the stored password, whatever was offered.
        uint8_t difference = login.second.size() == password.length ? 0 : 1;
        for (size_t i = 0; i < login.second.size(); i++) {
            const uint8_t offered = i < password.length ? password.data[i] : 0;
            difference |= (uint8_t)(offered ^ (uint8_t)login.second[i]);
        }
        return difference == 0;
    }
    return false;
}

bool AuthGate::isFixedAdmin(const std::string& username) const {
    for (const auto& a : _config.admins) if (a.first == username) return true;
    return false;
}

bool AuthGate::isFixedLogin(const std::string& username) const {
    for (const auto& a : _config.logins) if (a.first == username) return true;
    return false;
}

// --- The diagnostics log -------------------------------------------------------

namespace {

bool endsWith(const std::string& s, const char* tail) {
    const size_t n = std::strlen(tail);
    return s.size() >= n && s.compare(s.size() - n, n, tail) == 0;
}

// The admin commands that change something: each one that succeeds is kept, with what it named.
bool changesSomething(const std::string& command) {
    static const char* const kCommands[] = {
        "user.add", "user.set", "user.remove", "user.enable", "user.disable", "user.approve", "user.reset",
        "user.password", "user.permissions", "user.admin", "key.create", "key.set", "key.remove",
        "config.set", "config.apply", "config.reset", nullptr};
    for (const char* const* c = kCommands; *c; c++) if (command == *c) return true;
    return false;
}

// "name=value" for one field of a request, appended to `out`: a secret as (set), a long text by its size, an
// object field by field ("set broker.port=1884").
void describeField(std::string& out, const std::string& name, const json::Value& v) {
    const auto add = [&](const std::string& value) {
        if (!out.empty()) out += ", ";
        out += name + "=" + value;
    };
    const bool secret = endsWith(name, "password") || endsWith(name, "key-pem");
    switch (v.type) {
    case json::Value::Null: add("null"); break;
    case json::Value::Bool: add(v.boolean ? "true" : "false"); break;
    case json::Value::Number: {
        char number[32];
        if (v.number == (double)(long long)v.number) std::snprintf(number, sizeof number, "%lld", (long long)v.number);
        else std::snprintf(number, sizeof number, "%g", v.number);
        add(number);
        break;
    }
    case json::Value::String:
        add(secret ? std::string("(set)")
            : v.text.size() > 120 || v.text.find('\n') != std::string::npos
                ? "(" + std::to_string(v.text.size()) + " bytes)"
                : v.text);
        break;
    case json::Value::Array: {
        std::string items;
        for (const json::Value& item : v.items) {
            if (!items.empty()) items += " ";
            items += item.type == json::Value::String ? item.text : std::string("...");
        }
        add("[" + items + "]");
        break;
    }
    case json::Value::Object:
        if (v.fields.empty()) add("{}");
        for (const auto& field : v.fields) describeField(out, name + " " + field.first, field.second);
        break;
    }
}

} // namespace

void AuthGate::noteRefusal(const MqttBrokerRequest& request, uint8_t code, const std::string& why,
                           const std::string& detail) {
    if (_config.diagnostics == nullptr) return;
    DiagnosticsEntry e;
    e.kind = "connect-refused";
    e.clientId = text(request.clientId);
    e.login = text(request.userName);
    e.address = request.peerName ? request.peerName : "";
    e.reasonCode = code;
    e.reason = std::string(mq_reasonName(code, MQ_PKT_CONNACK)) + (why.empty() ? "" : ": " + why);
    e.detail = detail;
    _config.diagnostics->record(std::move(e));
}

void AuthGate::noteAdminChange(const std::string& requestText, const std::string& answer, const std::string& clientId,
                               const std::string& login, const char* via) {
    json::Value request, reply;
    if (!json::parse(requestText, request) || request.type != json::Value::Object) return;
    const std::string command = request.str("command");
    if (!changesSomething(command) || !json::parse(answer, reply) || !reply.flag("ok")) return;
    DiagnosticsEntry e;
    e.kind = "admin";
    e.clientId = clientId;
    e.login = login;
    const std::string target = request.str("username", request.str("name"));
    e.reason = command + (target.empty() ? "" : " " + target);
    std::string what;
    for (const auto& field : request.fields) {
        if (field.first == "id" || field.first == "command" || field.first == "username" || field.first == "name") {
            continue;
        }
        describeField(what, field.first, field.second);
    }
    if (via != nullptr) what = std::string("over ") + via + (what.empty() ? "" : "; " + what);
    e.detail = what;
    _config.diagnostics->record(std::move(e));
}

// log.list: the diagnostics log, newest first, a page at a time - what `modbee-mqtt log` reads over MQTT.
std::string AuthGate::listLog(const json::Value& request) {
    if (_config.diagnostics == nullptr) {
        return failure(request, "this server keeps no diagnostics log (docs/diagnostics.md)");
    }
    DiagnosticsQuery q;
    q.clientId = request.str("client");
    q.login = request.str("login");
    if (request["kinds"].type == json::Value::Array) {
        request.strings("kinds", q.kinds);
    } else {
        const std::string kinds = request.str("kinds");
        size_t from = 0;
        while (from < kinds.size()) {
            size_t comma = kinds.find(',', from);
            if (comma == std::string::npos) comma = kinds.size();
            if (comma > from) q.kinds.push_back(kinds.substr(from, comma - from));
            from = comma + 1;
        }
    }
    q.sinceMs = request.integer("since", 0);
    q.untilMs = request.integer("until", 0);
    q.beforeId = request.integer("before", 0);
    q.forward = request.has("after");
    q.afterId = request.integer("after", 0);
    long long limit = request.integer("limit", 100);
    if (limit < 1) limit = 1;
    if (limit > 1000) limit = 1000;
    q.limit = (size_t)limit;
    std::vector<DiagnosticsEntry> entries;
    std::string error;
    if (!_config.diagnostics->query(q, entries, error)) return failure(request, error);
    // A page stays well inside what an admin client takes in one message; the rest is the next page.
    const size_t kBudget = 200000;
    std::string items = "[";
    size_t shown = 0;
    for (const DiagnosticsEntry& e : entries) {
        json::Object o;
        o.add("id", (long long)e.id).add("time", (long long)e.timeMs).add("kind", e.kind).add("clientid", e.clientId)
            .add("login", e.login).add("address", e.address);
        if (e.reasonCode >= 0) o.add("reason_code", (long long)e.reasonCode);
        o.add("reason", e.reason).add("detail", e.detail);
        const std::string one = o.str();
        if (shown > 0 && items.size() + one.size() > kBudget) break;
        items += (shown ? "," : "") + one;
        shown++;
    }
    items += "]";
    json::Object o;
    o.raw("entries", items);
    const DiagnosticsStatus status = _config.diagnostics->status();
    o.add("dropped", (long long)status.dropped);
    // More - older ones, or following, newer ones - when the page is full.
    if (shown > 0 && (shown < entries.size() || entries.size() == q.limit)) {
        o.add("next", (long long)entries[shown - 1].id);
    }
    return reply(request, true, o);
}

bool AuthGate::checkKey(const std::string& offered, const std::string& clientId, std::string& keyName,
                        std::string& why) {
    // A key is "<name>.<secret>": the name finds the record, the secret is
    // checked against its hash.
    const size_t dot = offered.find('.');
    if (dot == std::string::npos) {
        why = "signing up: the password is not a key (<name>.<secret>)";
        return false;
    }
    keyName = offered.substr(0, dot);
    AuthKey key;
    if (!keyNameOk(keyName) || !_store.findKey(keyName, key)) {
        why = "signing up: no key is called \"" + keyName + "\"";
        return false;
    }
    if (!auth::verify(key.hash, (const uint8_t*)offered.data() + dot + 1, offered.size() - dot - 1)) {
        why = "signing up: the key's secret is wrong";
        return false;
    }
    if (key.expires != 0 && nowSeconds() >= key.expires) {
        why = "signing up: the key has expired";
        return false;
    }
    if (key.devices == 0) {
        why = "signing up: the key is turned off (devices 0)";
        return false;
    }
    if (!key.device.empty() && key.device != clientId) {
        why = "signing up: the key is for the device \"" + key.device + "\" only";
        return false;
    }
    return true;
}

const AuthGate::Compiled& AuthGate::permissions(Shard& shard, const std::string& username) {
    const uint64_t generation = _store.generation();
    if (generation != shard.cacheGeneration || shard.cache.size() > kCacheLimit) {
        shard.cache.clear();
        shard.cacheGeneration = generation;
    }
    auto found = shard.cache.find(username);
    if (found != shard.cache.end()) return found->second;
    Compiled c;
    AuthUser user;
    if (_store.getUser(shard.index, username, user) && user.enabled && !user.pending) {
        c.found = true;
        c.admin = user.admin;
        c.publish = user.publish;
        c.subscribe = user.subscribe;
    }
    return shard.cache.emplace(username, c).first->second;
}

bool AuthGate::authorise(Shard& shard, const MqttBrokerRequest& request, uint8_t& reasonCode) {
    const std::string username = text(request.userName);
    const std::string clientId = text(request.clientId);

    if (request.action == MqttBrokerAction::Connect) {
        if (_config.certificateLogin && request.certificateName != nullptr &&
            request.certificateName[0] != '\0') {
            return certificateLogin(shard, request, reasonCode);
        }
        reasonCode = MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
        bool ok = false;
        std::string why;            // a refusal, in words, for the diagnostics log
        std::string detail;
        if (username == kProvisionUser) {
            std::string keyName;
            if (!safeName(clientId)) why = "signing up: the client id is not a device name (empty, $, / + #)";
            else ok = checkKey(text(request.password), clientId, keyName, why);
            if (!keyName.empty()) detail = "key \"" + keyName + "\"";
            if (ok) {
                std::lock_guard<std::mutex> lock(_provisioningMutex);
                _provisioning[clientId] = keyName;
            }
            // A device registering again under its own name never takes the
            // session of a board still connected as it: while that board is
            // online the client identifier is its, and this CONNECT is refused
            // - a copy of the key cannot knock a working board off.
        } else if (fixedLogin(_config.admins, request.userName, request.password) ||
                   fixedLogin(_config.logins, request.userName, request.password)) {
            ok = true;
        } else if (isFixedAdmin(username) || isFixedLogin(username)) {
            why = "wrong password (a login from the server's settings)";
        } else if (!username.empty()) {
            AuthUser user;
            if (_store.getUser(shard.index, username, user)) {
                if (!user.enabled || user.pending) {
                    reasonCode = MQ_REASON_NOT_AUTHORIZED;
                    why = !user.enabled ? "the login is disabled" : "the login is waiting for an admin to approve it";
                } else {
                    ok = auth::verify(user.hash, request.password.data, request.password.length);
                    if (!ok) why = "wrong password";
                }
                // A device a key let in counts against that key's devices at once: off (0), it is refused;
                // with the key's number of its other devices online, it waits for one to go.
                AuthKey key;
                if (ok && !user.provisionedBy.empty() && _store.findKey(user.provisionedBy, key)) {
                    if (key.devices == 0) {
                        ok = false;
                        reasonCode = MQ_REASON_NOT_AUTHORIZED;
                        why = "its key \"" + key.name + "\" is turned off (devices 0)";
                    } else if (key.devices > 0 && onlineOf(key.name, username) >= (size_t)key.devices) {
                        ok = false;
                        reasonCode = MQ_REASON_QUOTA_EXCEEDED;
                        why = "its key \"" + key.name + "\" has its " + std::to_string(key.devices) +
                              " device(s) at once online";
                    }
                }
            } else {
                // Hashed anyway, so an unknown name takes as long as a wrong
                // password and the two cannot be told apart from outside.
                static const std::string kDecoy = auth::hashGenerated("decoy");
                auth::verify(kDecoy, request.password.data, request.password.length);
                why = "no login has that name";
            }
        } else {
            why = "no user name - every client logs in";
        }
        (ok ? _accepted : _refused).fetch_add(1, std::memory_order_relaxed);
        if (!ok) noteRefusal(request, reasonCode, why, detail);
        if (ok && !clientId.empty()) {
            std::lock_guard<std::mutex> lock(_onlineMutex);
            _connecting[clientId] = username;
        }
        return ok;
    }

    reasonCode = MQ_REASON_NOT_AUTHORIZED;
    std::string topic = text(request.topic);
    const bool publish = request.action == MqttBrokerAction::Publish;

    if (username == kProvisionUser) {
        const std::string mine = std::string(kProvisionPrefix) + clientId + (publish ? "/request" : "/reply");
        return safeName(clientId) && topic == mine;
    }
    if (isFixedAdmin(username)) return true;
    // Sparkplug Aware: every login may read the births the server keeps there; none may write them.
    if (topic.compare(0, 24, "$sparkplug/certificates/") == 0) return !publish;
    for (const auto& login : _config.logins) {
        // A fixed login from the config: any topic, except the $ ones.
        if (login.first == username) return topic.empty() || topic[0] != '$';
    }

    const Compiled& c = permissions(shard, username);
    if (!c.found) return false;     // removed, disabled or pending since it connected
    if (c.admin) return true;

    // Its own provisioning topics: a signed-in device asks again for its data (refresh), and hears the answer.
    if (topic == std::string(kProvisionPrefix) + clientId + (publish ? "/request" : "/reply")) return safeName(clientId);

    // A shared subscription is judged by the filter inside it.
    if (!publish) {
        MqttSpan share, inner;
        if (mq_isSharedFilter(request.topic) && mq_splitSharedFilter(request.topic, share, inner)) {
            topic = text(inner);
        }
    }
    if (!topic.empty() && topic[0] == '$') return false;

    // No rules: every topic, as a broker without per-login rules - or, with
    // authentication.default-topics: none, no topic at all.
    const std::vector<std::string>& rules = publish ? c.publish : c.subscribe;
    if (rules.empty()) return !_config.noTopicsWithoutRules;
    for (const std::string& rule : rules) {
        std::string filter;
        if (!expand(rule, username, clientId, filter)) continue;
        if (publish ? mq_topicMatches(spanOf(filter), spanOf(topic)) : covers(filter, topic)) return true;
    }
    return false;
}

// A CONNECT whose client certificate the TLS handshake verified, on a server
// with certificate-login: the certificate IS the credential (docs/logins.md).
//
//   the login        the certificate's CN - and if the CONNECT names a user,
//                    it must be that one, or a device could present its own
//                    certificate and ask to be somebody else
//   the password     not asked: the handshake proved the private key
//   the login record must exist, be enabled and approved; its permissions
//                    apply exactly as for a password login
//   a fingerprint    on the record binds it to ONE certificate: another one
//                    the same CA issued with the same CN is refused
//
// Every refusal is counted with the rest and said on stderr, with the CN and
// the fingerprint - the two things an operator needs to find the device and
// the certificate, neither of them a secret.
bool AuthGate::certificateLogin(Shard& shard, const MqttBrokerRequest& request, uint8_t& reasonCode) {
    const std::string name = request.certificateName;
    const std::string fingerprint = request.certificateFingerprint ? request.certificateFingerprint : "";
    const std::string offered = text(request.userName);
    const auto refuse = [&](uint8_t code, const char* why) {
        reasonCode = code;
        _refused.fetch_add(1, std::memory_order_relaxed);
        _certificateRefused.fetch_add(1, std::memory_order_relaxed);
        std::fprintf(stderr, "modbee-mqtt serve: certificate login refused for \"%s\" from %s (certificate %s): %s\n",
                     name.c_str(), request.peerName ? request.peerName : "?", fingerprint.c_str(), why);
        noteRefusal(request, code, std::string("certificate login: ") + why,
                    "certificate CN \"" + name + "\", sha256 " + fingerprint);
        return false;
    };
    if (!offered.empty() && offered != name) {
        return refuse(MQ_REASON_BAD_USER_NAME_OR_PASSWORD,
                      "the CONNECT's user name is not the certificate's CN");
    }
    bool fixed = isFixedAdmin(name);
    for (const auto& login : _config.logins) fixed = fixed || login.first == name;
    if (!fixed) {
        AuthUser user;
        if (!_store.getUser(shard.index, name, user)) {
            return refuse(MQ_REASON_BAD_USER_NAME_OR_PASSWORD, "no login has that name");
        }
        if (!user.enabled || user.pending) {
            return refuse(MQ_REASON_NOT_AUTHORIZED, "the login is disabled or waiting for approval");
        }
        if (!user.fingerprint.empty() && user.fingerprint != fingerprint) {
            return refuse(MQ_REASON_BAD_USER_NAME_OR_PASSWORD,
                          "the login is bound to a different certificate (its fingerprint)");
        }
    }
    if (request.login != nullptr) *request.login = name;
    _accepted.fetch_add(1, std::memory_order_relaxed);
    _certificateAccepted.fetch_add(1, std::memory_order_relaxed);
    return true;
}

// =============================================================================
// REQUESTS ON $modbee/
// =============================================================================

uint32_t AuthGate::offer(uint32_t shard, MqttBroker&, const MqttBrokerRouted& message, uint64_t) {
    if (message.topic.length < 8 || message.topic.data[0] != '$') return 0;
    // A request is a CLIENT's: published over MQTT, through the hook. What an
    // extension injected (the Kafka pipe's records carry an origin) or the
    // application published (no client) never met the hook, so a record on
    // $modbee/admin/request is nobody's request - not an admin's.
    if (message.origin != nullptr || message.clientId.length == 0) return 0;
    const std::string topic = text(message.topic);
    const std::string clientId = text(message.clientId);
    Request request;
    request.clientId = clientId;
    request.login = text(message.userName);
    request.payload = text(message.payload);
    if (topic == kAdminRequest) {
        // The hook refused everybody else; asked again here, so the request
        // stands on the publisher's login and not on the topic alone.
        if (!isFixedAdmin(request.login) && !permissions(*_shards[shard], request.login).admin) return 0;
        request.admin = true;
        queue(shard, request);
    } else if (topic == std::string(kProvisionPrefix) + clientId + "/request") {
        queue(shard, request);
    }
    return 0;
}

void AuthGate::queue(uint32_t shard, const Request& request) {
    Shard& s = *_shards[shard];
    std::lock_guard<std::mutex> lock(s.mutex);
    s.requests.push_back(request);
    s.pending = true;
}

void AuthGate::kick(const std::string& username) {
    for (auto& s : _shards) {
        std::lock_guard<std::mutex> lock(s->mutex);
        s->kicks.push_back(username);
        s->pending = true;
    }
}

bool AuthGate::hasPendingWork(uint32_t shard) {
    return _shards[shard]->pending.load(std::memory_order_acquire);
}

void AuthGate::service(uint32_t shard, MqttBroker& broker) {
    Shard& s = *_shards[shard];
    if (!s.pending.load(std::memory_order_acquire)) return;
    std::vector<std::string> kicks;
    std::vector<Request> requests;
    {
        std::lock_guard<std::mutex> lock(s.mutex);
        kicks.swap(s.kicks);
        requests.swap(s.requests);
        s.pending = false;
    }
    for (const std::string& name : kicks) {
        _kicked.fetch_add(broker.disconnectUser(spanOf(name), MQ_REASON_NOT_AUTHORIZED),
                          std::memory_order_relaxed);
    }
    for (const Request& r : requests) {
        std::string topic, body;
        if (r.admin) {
            topic = "$modbee/admin/" + r.clientId + "/reply";
            body = admin(r.payload, r.clientId, r.login);
        } else {
            std::string keyName;
            {
                std::lock_guard<std::mutex> lock(_provisioningMutex);
                auto found = _provisioning.find(r.clientId);
                if (found != _provisioning.end()) keyName = found->second;
            }
            topic = std::string(kProvisionPrefix) + r.clientId + "/reply";
            body = r.login == kProvisionUser || r.login.empty() ? provision(r.clientId, keyName, r.payload)
                                                                 : refresh(r.login);
        }
        MqttBrokerRouted out;
        out.topic = spanOf(topic);
        out.payload = spanOf(body);
        out.qos = MQ_QOS_1;
        out.retain = false;
        broker.publishMessage(out);
    }
    // config.* with "restart": true - after the reply has been sent, so the
    // asker hears that the change was stored before the server goes.
    if (_restartAfterReply.exchange(false) && _config.restart) _config.restart(_config.restartContext);
}

void AuthGate::stop(uint32_t, MqttBroker&) {
    if (++_stopped == _shards.size()) {
        std::lock_guard<std::mutex> lock(_backupMutex);
        _quit = true;
        _backupWake.notify_all();
    }
}

// --- Provisioning ------------------------------------------------------------

std::string AuthGate::provision(const std::string& clientId, const std::string& keyName,
                                const std::string& requestText) {
    const std::string answer = signUp(clientId, keyName, requestText);
    // Every sign-up and the answer it got, for the diagnostics log.
    if (_config.diagnostics != nullptr) {
        json::Value request, reply;
        if (!requestText.empty()) json::parse(requestText, request);
        json::parse(answer, reply);
        DiagnosticsEntry e;
        e.kind = "signup";
        e.clientId = clientId;
        e.login = request.type == json::Value::Object ? request.str("name", clientId) : clientId;
        const std::string status = reply.str("status");
        e.reason = status == "ok"        ? "ok - its login is ready"
                   : status == "pending" ? "pending - waiting for an admin to approve it"
                                         : "refused: " + reply.str("reason", "no reason given");
        e.detail = keyName.empty() ? std::string() : "key \"" + keyName + "\"";
        _config.diagnostics->record(std::move(e));
    }
    return answer;
}

std::string AuthGate::signUp(const std::string& clientId, const std::string& keyName,
                             const std::string& requestText) {
    json::Value request;
    if (!requestText.empty() && !json::parse(requestText, request)) request = json::Value();
    json::Object body;
    const auto refused = [&](const std::string& reason) {
        json::Object o;
        o.add("status", "refused").add("reason", reason);
        return o.str();
    };
    if (keyName.empty()) return refused("this session did not connect with a provisioning key");
    AuthKey key;
    if (!_store.findKey(keyName, key)) return refused("the provisioning key no longer exists");
    if (key.expires != 0 && nowSeconds() >= key.expires) return refused("the provisioning key has expired");

    const std::string name = request.str("name", clientId);
    if (!safeName(name)) return refused("the name may not be empty, start with $, or contain / + #");
    if (!key.device.empty() && key.device != name) return refused("this key is for another device");
    if (isFixedAdmin(name)) return refused("that name belongs to an admin");

    AuthUser existing;
    const bool exists = _store.findUser(name, existing);
    if (exists && (existing.admin || existing.provisionedBy.empty())) {
        // A login an admin added by hand, or an admin, cannot be taken over
        // by anyone holding a key.
        return refused("that name has a login an admin created; an admin must reset it");
    }
    if (exists && !existing.enabled) {
        // Disabled means disabled: a key does not switch it back on, or a
        // device an admin turned off would provision itself straight back.
        return refused("this device is disabled; an admin must enable it");
    }
    if (exists) {
        // Re-provisioning (a replaced or wiped device gets a new secret, the
        // old one stops working) stays inside the device's own site: a key of
        // another site - another customer - must not take its login, its
        // topic rights and its place on the broker.
        //
        // ASKED BEFORE "pending", and for every state of the login. Answered
        // after it, a name waiting on site A told site B's key "pending" with
        // no reason - and A's admin approving it would have given a device
        // holding B's key a login with A's rights. A pending login, and an
        // approved one whose secret is not collected yet, belong to their
        // site exactly as a working one does.
        if (existing.site != key.site) return refused("that name belongs to another site; an admin must reset it");
    }
    if (exists && existing.pending) {
        json::Object o;
        o.add("status", "pending");
        return o.str();
    }
    // Approved, its secret not collected yet: the device that waited takes it now.
    const bool collecting = exists && existing.hash.empty();
    // A device signing up again (a wiped login, a replaced board) does so only while no board is connected as it.
    if (exists && online(name)) return refused("a board is connected as this device; it signs up again once that one is gone");
    // A key's number is its devices AT ONCE: with that many of the devices it let in online, another waits.
    if (key.devices > 0 && onlineOf(key.name, name) >= (size_t)key.devices) {
        return refused("the provisioning key's " + std::to_string(key.devices) + " device(s) at once are online");
    }

    const std::string secret = auth::generateSecret(32);
    AuthUser user = exists ? existing : AuthUser();
    user.username = name;
    user.generated = true;
    user.provisionedBy = key.name;
    user.site = key.site;
    user.data = key.data;           // what the key hands its devices, as it is now
    if (!exists) {
        user.enabled = true;
        user.publish = key.publish;
        user.subscribe = key.subscribe;
    }
    // A key that wants each device approved holds a new or re-provisioned one
    // pending, with no usable secret, until an admin approves it; the device
    // asks again and collects its secret then.
    user.pending = key.manualApproval && !collecting;
    user.hash = user.pending ? std::string() : auth::hashGenerated(secret);
    if (_store.putUser(user, true) != AuthResult::Ok) return refused("the login store could not be written");
    if (exists) kick(name);   // the old secret, and any device still using it, are done
    _provisioned.fetch_add(1, std::memory_order_relaxed);
    json::Object o;
    if (user.pending) {
        o.add("status", "pending");
    } else {
        o.add("status", "ok").add("username", name).add("password", secret).raw("data", dataJson(user.data));
    }
    return o.str();
}

std::string AuthGate::refresh(const std::string& username) {
    json::Object o;
    AuthUser user;
    if (!_store.findUser(username, user) || !user.enabled || user.pending) {
        o.add("status", "refused").add("reason", "no usable login");
        return o.str();
    }
    o.add("status", "ok").raw("data", dataJson(user.data));
    return o.str();
}

// --- Admin -------------------------------------------------------------------

std::string AuthGate::admin(const std::string& requestText) { return admin(requestText, std::string(), std::string()); }

std::string AuthGate::admin(const std::string& requestText, const std::string& clientId, const std::string& login,
                            const char* via) {
    const std::string answer = answerAdmin(requestText);
    if (_config.diagnostics != nullptr) noteAdminChange(requestText, answer, clientId, login, via);
    return answer;
}

std::string AuthGate::answerAdmin(const std::string& requestText) {
    json::Value request;
    if (!json::parse(requestText, request) || request.type != json::Value::Object) {
        return failure(json::Value(), "the request is not a JSON object");
    }
    const std::string command = request.str("command");
    const std::string username = request.str("username");

    const auto needUser = [&](AuthUser& u) {
        return !username.empty() && _store.findUser(username, u);
    };

    if (command == "user.add") {
        if (!loginNameOk(username)) {
            return failure(request, "a user name may not be empty, longer than 256, start with $, or hold control characters");
        }
        if (isFixedAdmin(username)) return failure(request, "that name is the admin login");
        AuthUser u;
        u.username = username;
        if (!request.strings("publish", u.publish) || !request.strings("subscribe", u.subscribe) ||
            !filtersOk(u.publish) || !filtersOk(u.subscribe)) {
            return failure(request, "publish and subscribe are lists of valid topic filters");
        }
        u.admin = request.flag("admin");
        if (request.has("fingerprint") && !certificateFingerprint(request.str("fingerprint"), u.fingerprint)) {
            return failure(request, kFingerprintError);
        }
        std::string dataError;
        if (!requestData(request, u.data, dataError)) return failure(request, dataError);
        const std::string chosen = request.str("password");
        const std::string secret = chosen.empty() ? auth::generateSecret(32) : chosen;
        u.generated = chosen.empty();
        u.hash = u.generated ? auth::hashGenerated(secret) : auth::hashChosen(secret);
        const AuthResult r = _store.putUser(u, false);
        if (r == AuthResult::Exists) return failure(request, "user " + username + " already exists");
        if (r != AuthResult::Ok) return failure(request, "the login store could not be written");
        json::Object o;
        o.add("username", username);
        if (u.generated) o.add("password", secret);
        return reply(request, true, o);
    }
    if (command == "user.get") {
        AuthUser u;
        if (!needUser(u)) return failure(request, "no user " + username);
        json::Object o;
        o.raw("user", describeUser(u));
        return reply(request, true, o);
    }
    if (command == "user.list") {
        long long limit = request.integer("limit", 100);
        if (limit < 1) limit = 1;
        if (limit > 1000) limit = 1000;
        std::vector<AuthUser> users;
        _store.listUsers(request.str("prefix"), request.str("after"), (size_t)limit, users);
        std::string items = "[";
        for (size_t i = 0; i < users.size(); i++) {
            if (i) items += ",";
            items += describeUser(users[i]);
        }
        items += "]";
        json::Object o;
        o.raw("users", items).add("total", (long long)_store.userCount());
        if ((long long)users.size() == limit) o.add("next", users.back().username);
        return reply(request, true, o);
    }
    if (command == "user.set") {
        // Several fields of one login at once - whichever the request names,
        // and nothing else. For a platform that keeps its own view of a
        // device and writes it back whole.
        AuthUser u;
        if (!needUser(u)) return failure(request, "no user " + username);
        bool changed = false, kickAfter = false;
        if (request.has("fingerprint")) {
            std::string fingerprint;
            if (!certificateFingerprint(request.str("fingerprint"), fingerprint)) {
                return failure(request, kFingerprintError);
            }
            // A new binding cuts off whatever certificate is connected now; a
            // cleared one takes nothing away.
            kickAfter = kickAfter || (!fingerprint.empty() && fingerprint != u.fingerprint);
            u.fingerprint = fingerprint;
            changed = true;
        }
        if (request.has("publish") || request.has("subscribe")) {
            std::vector<std::string> publish = u.publish, subscribe = u.subscribe;
            if ((request.has("publish") && !request.strings("publish", publish)) ||
                (request.has("subscribe") && !request.strings("subscribe", subscribe)) ||
                !filtersOk(publish) || !filtersOk(subscribe)) {
                return failure(request, "publish and subscribe are lists of valid topic filters");
            }
            u.publish = publish;
            u.subscribe = subscribe;
            kickAfter = changed = true;
        }
        if (request.has("admin")) {
            u.admin = request.flag("admin");
            kickAfter = kickAfter || !u.admin;
            changed = true;
        }
        if (request.has("enabled")) {
            u.enabled = request.flag("enabled", true);
            kickAfter = kickAfter || !u.enabled;
            changed = true;
        }
        if (request.has("password")) {
            const std::string chosen = request.str("password");
            if (chosen.empty()) return failure(request, "password may not be empty");
            u.hash = auth::hashChosen(chosen);
            u.generated = false;
            kickAfter = changed = true;
        }
        if (request.has("data")) {
            // Replaces the whole set; the device has it at its next refresh. No reconnect needed.
            std::string dataError;
            if (!requestData(request, u.data, dataError)) return failure(request, dataError);
            changed = true;
        }
        if (!changed) {
            return failure(request, "user.set changes fingerprint, publish, subscribe, admin, enabled, "
                                    "password or data - the request named none of them");
        }
        if (_store.putUser(u, true) != AuthResult::Ok) return failure(request, "the login store could not be written");
        if (kickAfter) kick(username);
        json::Object o;
        o.add("username", username).raw("user", describeUser(u));
        return reply(request, true, o);
    }
    if (command == "user.remove") {
        const AuthResult r = _store.removeUser(username);
        if (r == AuthResult::NotFound) return failure(request, "no user " + username);
        if (r != AuthResult::Ok) return failure(request, "the login store could not be written");
        kick(username);
        return reply(request, true);
    }
    if (command == "user.enable" || command == "user.disable" || command == "user.approve" ||
        command == "user.reset" || command == "user.password" || command == "user.permissions" ||
        command == "user.admin") {
        AuthUser u;
        if (!needUser(u)) return failure(request, "no user " + username);
        json::Object o;
        bool kickAfter = false;
        if (command == "user.enable") u.enabled = true;
        if (command == "user.disable") { u.enabled = false; kickAfter = true; }
        if (command == "user.approve") u.pending = false;
        if (command == "user.admin") { u.admin = request.flag("admin", true); kickAfter = !u.admin; }
        if (command == "user.reset") {
            const std::string secret = auth::generateSecret(32);
            u.hash = auth::hashGenerated(secret);
            u.generated = true;
            o.add("password", secret);
            kickAfter = true;
        }
        if (command == "user.password") {
            const std::string chosen = request.str("password");
            if (chosen.empty()) return failure(request, "password is required");
            u.hash = auth::hashChosen(chosen);
            u.generated = false;
            kickAfter = true;
        }
        if (command == "user.permissions") {
            if (!request.strings("publish", u.publish) || !request.strings("subscribe", u.subscribe) ||
                !filtersOk(u.publish) || !filtersOk(u.subscribe)) {
                return failure(request, "publish and subscribe are lists of valid topic filters");
            }
            // Live connections keep subscriptions granted under the old
            // rules; closing them makes the new ones apply to everything.
            kickAfter = true;
        }
        if (_store.putUser(u, true) != AuthResult::Ok) return failure(request, "the login store could not be written");
        if (kickAfter) kick(username);
        o.add("username", username);
        return reply(request, true, o);
    }
    if (command == "key.create") {
        AuthKey k;
        k.name = request.str("name");
        if (!keyNameOk(k.name)) return failure(request, "a key name is 1-64 letters, digits, - or _");
        const std::string approval = request.str("approval", "auto");
        if (approval != "auto" && approval != "manual") return failure(request, "approval is auto or manual");
        k.manualApproval = approval == "manual";
        if (!request.strings("publish", k.publish) || !request.strings("subscribe", k.subscribe) ||
            !filtersOk(k.publish) || !filtersOk(k.subscribe)) {
            return failure(request, "publish and subscribe are lists of valid topic filters");
        }
        k.devices = request.integer("devices", -1);
        if (k.devices < -1) return failure(request, "devices is how many of its devices may be online at once: 0 (off) or more, or left out for no limit");
        const long long days = request.integer("days", 0);
        k.expires = days > 0 ? nowSeconds() + days * 86400 : request.integer("expires", 0);
        k.device = request.str("device");
        if (!k.device.empty() && !safeName(k.device)) return failure(request, "device is not a valid name");
        // The site its devices belong to: a key re-provisions only devices of its own site. Optional;
        // keys without one share one site, as on a broker for a single site.
        k.site = request.str("site");
        if (!k.site.empty() && !keyNameOk(k.site)) return failure(request, "a site is 1-64 letters, digits, - or _");
        std::string dataError;
        if (!requestData(request, k.data, dataError)) return failure(request, dataError);
        const std::string secret = auth::generateSecret(32);
        k.hash = auth::hashGenerated(secret);
        const AuthResult r = _store.putKey(k);
        if (r == AuthResult::Exists) return failure(request, "key " + k.name + " already exists");
        if (r != AuthResult::Ok) return failure(request, "the login store could not be written");
        json::Object o;
        o.add("name", k.name).add("key", k.name + "." + secret);
        return reply(request, true, o);
    }
    if (command == "key.list") {
        std::vector<AuthKey> keys;
        _store.listKeys(keys);
        std::string items = "[";
        for (size_t i = 0; i < keys.size(); i++) {
            if (i) items += ",";
            items += describeKey(keys[i]);
        }
        json::Object o;
        o.raw("keys", items + "]");
        return reply(request, true, o);
    }
    if (command == "key.set") {
        // What a key may still do and what it hands its devices from now on - each only when named; the key
        // itself, its rules and its site stay as they were made, so a device set up with it needs nothing new.
        // `uses`: how many more devices it may register (0: none - closed, it refuses every registration until
        // raised; -1: no limit). `approval`: whether each one waits to be let in. `expires`: when it stops working,
        // UTC seconds (0: never), or `days` from now - set, extended, or taken away. `data`: what each is handed;
        // devices it registered already keep theirs (user.set data changes those).
        AuthKey k;
        if (!_store.findKey(request.str("name"), k)) return failure(request, "no key " + request.str("name"));
        if (!request.has("data") && !request.has("devices") && !request.has("approval") && !request.has("expires") && !request.has("days")) {
            return failure(request, "key.set changes devices, approval, expires or data - the request named none");
        }
        if (request.has("days")) {
            const long long days = request.integer("days", 0);
            if (days < 1) return failure(request, "days is how many days from now it stops working: 1 or more");
            k.expires = nowSeconds() + days * 86400;
        } else if (request.has("expires")) {
            const long long expires = request.integer("expires", -1);
            if (expires < 0) return failure(request, "expires is when it stops working, UTC seconds, or 0 for never");
            k.expires = expires;
        }
        bool off = false;
        if (request.has("devices")) {
            const long long devices = request.integer("devices", -2);
            if (devices < -1) return failure(request, "devices is how many of its devices may be online at once: 0 (off) or more, or -1 for no limit");
            off = devices == 0 && k.devices != 0;
            k.devices = devices;
        }
        if (request.has("approval")) {
            const std::string approval = request.str("approval");
            if (approval != "auto" && approval != "manual") return failure(request, "approval is auto or manual");
            k.manualApproval = approval == "manual";
        }
        std::string dataError;
        if (request.has("data") && !requestData(request, k.data, dataError)) return failure(request, dataError);
        if (_store.putKey(k, true) != AuthResult::Ok) return failure(request, "the login store could not be written");
        // Turned off: every device it let in that is online now is signed out (and refused from now on).
        if (off) {
            std::vector<std::string> names;
            {
                std::lock_guard<std::mutex> lock(_onlineMutex);
                for (const auto& one : _online) names.push_back(one.second.first);
            }
            for (const std::string& name : names) {
                AuthUser u;
                if (_store.findUser(name, u) && u.provisionedBy == k.name) kick(name);
            }
        }
        json::Object o;
        o.raw("key", describeKey(k));
        return reply(request, true, o);
    }
    if (command == "key.remove") {
        const AuthResult r = _store.removeKey(request.str("name"));
        if (r == AuthResult::NotFound) return failure(request, "no key " + request.str("name"));
        if (r != AuthResult::Ok) return failure(request, "the login store could not be written");
        return reply(request, true);
    }
    if (command == "backup") {
        std::string path, error;
        if (!backupNow(path, error)) return failure(request, error);
        json::Object o;
        o.add("file", path);
        return reply(request, true, o);
    }
    if (command.compare(0, 7, "config.") == 0) return configCommand(command, request);
    if (command == "log.list") return listLog(request);
    if (command == "status") {
        const AuthGateStatus s = status();
        json::Object o;
        o.add("users", (long long)_store.userCount())
            .add("accepted", (long long)s.accepted)
            .add("refused", (long long)s.refused)
            .add("provisioned", (long long)s.provisioned)
            .add("certificate_accepted", (long long)s.certificateAccepted)
            .add("certificate_refused", (long long)s.certificateRefused)
            .add("database", _store.path())
            .add("last_backup", lastBackup());
        return reply(request, true, o);
    }
    return failure(request, command.empty() ? "no command" : "unknown command " + command);
}

// --- Backups -----------------------------------------------------------------

bool AuthGate::backupNow(std::string& path, std::string& error) {
    server_os::makeDirectory(_config.backupDirectory, 0750);
    path = _config.backupDirectory + "/auth-" + stamp() + ".db";
    if (!_store.backup(path, error)) return false;
    // Keep the newest few. The names sort by time.
    std::vector<std::string> files;
    for (const std::string& name : server_os::listDirectory(_config.backupDirectory)) {
        if (startsWith(name, "auth-") && name.size() > 3 && name.compare(name.size() - 3, 3, ".db") == 0) {
            files.push_back(name);
        }
    }
    std::sort(files.begin(), files.end());
    for (size_t i = 0; _config.backupsKept > 0 && i + (size_t)_config.backupsKept < files.size(); i++) {
        std::remove((_config.backupDirectory + "/" + files[i]).c_str());
    }
    std::lock_guard<std::mutex> lock(_backupMutex);
    _lastBackup = path;
    return true;
}

// The gate's own thread. Once a second: whether another program wrote the
// login file - `modbee-mqtt admin`, setting the admin beside the running server -
// so what the shards cached is dropped and the change applies at once. And
// the backup, every backupEveryHours.
void AuthGate::backupLoop() {
    const auto every = std::chrono::hours(_config.backupEveryHours > 0 ? _config.backupEveryHours : 1);
    auto due = std::chrono::steady_clock::now() + every;
    std::unique_lock<std::mutex> lock(_backupMutex);
    while (!_quit) {
        _backupWake.wait_for(lock, std::chrono::seconds(1));
        if (_quit) break;
        lock.unlock();
        _store.noticeOutsideWrites();
        if (_config.backupEveryHours > 0 && std::chrono::steady_clock::now() >= due) {
            due = std::chrono::steady_clock::now() + every;
            std::string path, error;
            if (!backupNow(path, error)) {
                diagnostics::error("logins", "modbee-mqtt serve: the login store backup failed: %s\n", error.c_str());
            }
        }
        lock.lock();
    }
}

std::string AuthGate::lastBackup() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(_backupMutex));
    return _lastBackup;
}

AuthGateStatus AuthGate::status() const {
    AuthGateStatus s;
    s.accepted = _accepted.load();
    s.refused = _refused.load();
    s.provisioned = _provisioned.load();
    s.kicked = _kicked.load();
    s.certificateAccepted = _certificateAccepted.load();
    s.certificateRefused = _certificateRefused.load();
    return s;
}

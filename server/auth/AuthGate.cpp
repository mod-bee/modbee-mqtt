// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AuthGate.h"
#include "AdminReply.h"
#include "AuthHash.h"
#include "Json.h"

#include "MqttTopic.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <sys/stat.h>
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

// ${username} and ${clientid}, or false if either would put a wildcard or a
// level separator into a filter.
bool expand(const std::string& pattern, const std::string& username, const std::string& clientId,
            std::string& out) {
    out = pattern;
    struct { const char* name; const std::string* value; } vars[] = {
        {"${username}", &username}, {"${clientid}", &clientId}};
    for (auto& v : vars) {
        size_t at = out.find(v.name);
        if (at == std::string::npos) continue;
        if (!safeName(*v.value, 65535)) return false;
        while (at != std::string::npos) {
            out.replace(at, std::strlen(v.name), *v.value);
            at = out.find(v.name, at + v.value->size());
        }
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
        .add("created", (long long)u.created)
        .add("updated", (long long)u.updated)
        .add("fingerprint", u.fingerprint);
    return o.str();
}

std::string describeKey(const AuthKey& k) {
    json::Object o;
    o.add("name", k.name)
        .add("approval", k.manualApproval ? "manual" : "auto")
        .raw("publish", json::array(k.publish))
        .raw("subscribe", json::array(k.subscribe))
        .add("uses_left", (long long)k.usesLeft)
        .add("expires", (long long)k.expires)
        .add("device", k.device)
        .add("created", (long long)k.created);
    return o.str();
}

std::string stamp() {
    const std::time_t t = std::time(nullptr);
    std::tm utc;
    gmtime_r(&t, &utc);
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
    if (_config.backupEveryHours > 0 && !_backups.joinable()) {
        _backups = std::thread([this] { backupLoop(); });
    }
    return true;
}

void AuthGate::prepare(uint32_t shard, MqttBroker& broker) {
    broker.onAuthorise(&AuthGate::hook, _shards[shard].get());
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

bool AuthGate::checkKey(const std::string& offered, const std::string& clientId, std::string& keyName) {
    // A key is "<name>.<secret>": the name finds the record, the secret is
    // checked against its hash.
    const size_t dot = offered.find('.');
    if (dot == std::string::npos) return false;
    keyName = offered.substr(0, dot);
    AuthKey key;
    if (!keyNameOk(keyName) || !_store.findKey(keyName, key)) return false;
    if (!auth::verify(key.hash, (const uint8_t*)offered.data() + dot + 1, offered.size() - dot - 1)) {
        return false;
    }
    if (key.expires != 0 && nowSeconds() >= key.expires) return false;
    if (key.usesLeft == 0) return false;
    if (!key.device.empty() && key.device != clientId) return false;
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
        if (username == kProvisionUser) {
            std::string keyName;
            ok = safeName(clientId) && checkKey(text(request.password), clientId, keyName);
            if (ok) {
                std::lock_guard<std::mutex> lock(_provisioningMutex);
                _provisioning[clientId] = keyName;
            }
        } else if (fixedLogin(_config.admins, request.userName, request.password) ||
                   fixedLogin(_config.logins, request.userName, request.password)) {
            ok = true;
        } else if (!username.empty()) {
            AuthUser user;
            if (_store.getUser(shard.index, username, user)) {
                if (!user.enabled || user.pending) {
                    reasonCode = MQ_REASON_NOT_AUTHORIZED;
                } else {
                    ok = auth::verify(user.hash, request.password.data, request.password.length);
                }
            } else {
                // Hashed anyway, so an unknown name takes as long as a wrong
                // password and the two cannot be told apart from outside.
                static const std::string kDecoy = auth::hashGenerated("decoy");
                auth::verify(kDecoy, request.password.data, request.password.length);
            }
        }
        (ok ? _accepted : _refused).fetch_add(1, std::memory_order_relaxed);
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
    for (const auto& login : _config.logins) {
        // A fixed login from the config: any topic, except the $ ones.
        if (login.first == username) return topic.empty() || topic[0] != '$';
    }

    const Compiled& c = permissions(shard, username);
    if (!c.found) return false;     // removed, disabled or pending since it connected
    if (c.admin) return true;

    // A shared subscription is judged by the filter inside it.
    if (!publish) {
        MqttSpan share, inner;
        if (mq_isSharedFilter(request.topic) && mq_splitSharedFilter(request.topic, share, inner)) {
            topic = text(inner);
        }
    }
    if (!topic.empty() && topic[0] == '$') return false;

    const std::vector<std::string>& rules = publish ? c.publish : c.subscribe;
    if (rules.empty()) return true;
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
        std::fprintf(stderr, "modbee serve: certificate login refused for \"%s\" from %s (certificate %s): %s\n",
                     name.c_str(), request.peerName ? request.peerName : "?", fingerprint.c_str(), why);
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
    const std::string topic = text(message.topic);
    const std::string clientId = text(message.clientId);
    Request request;
    request.clientId = clientId;
    request.payload = text(message.payload);
    if (topic == kAdminRequest) {
        // Only an admin could publish it: the hook refused everybody else.
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
            body = admin(r.payload);
        } else {
            std::string keyName;
            {
                std::lock_guard<std::mutex> lock(_provisioningMutex);
                auto found = _provisioning.find(r.clientId);
                if (found != _provisioning.end()) keyName = found->second;
            }
            topic = std::string(kProvisionPrefix) + r.clientId + "/reply";
            body = provision(r.clientId, keyName, r.payload);
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
        // Owner's rule: re-provisioning is always allowed with a valid key -
        // for devices that were PROVISIONED. A login an admin added by hand,
        // or an admin, cannot be taken over by anyone holding a site key.
        return refused("that name has a login an admin created; an admin must reset it");
    }
    if (exists && !existing.enabled) {
        // Disabled means disabled: a key does not switch it back on, or a
        // device an admin turned off would provision itself straight back.
        return refused("this device is disabled; an admin must enable it");
    }
    if (exists && existing.pending) {
        json::Object o;
        o.add("status", "pending");
        return o.str();
    }
    if (!exists && !_store.spendKey(keyName)) return refused("the provisioning key has no uses left");

    const std::string secret = auth::generateSecret(32);
    AuthUser user = exists ? existing : AuthUser();
    user.username = name;
    user.hash = auth::hashGenerated(secret);
    user.generated = true;
    if (!exists) {
        user.enabled = true;
        user.pending = key.manualApproval;
        user.publish = key.publish;
        user.subscribe = key.subscribe;
        user.provisionedBy = key.name;
    } else {
        user.provisionedBy = key.name;
    }
    if (_store.putUser(user, true) != AuthResult::Ok) return refused("the login store could not be written");
    if (exists) kick(name);   // the old secret, and any device still using it, are done
    _provisioned.fetch_add(1, std::memory_order_relaxed);
    json::Object o;
    if (user.pending) {
        o.add("status", "pending");
    } else {
        o.add("status", "ok").add("username", name).add("password", secret);
    }
    return o.str();
}

// --- Admin -------------------------------------------------------------------

std::string AuthGate::admin(const std::string& requestText) {
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
        if (!changed) {
            return failure(request, "user.set changes fingerprint, publish, subscribe, admin, enabled "
                                    "or password - the request named none of them");
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
        k.usesLeft = request.integer("uses", -1);
        if (k.usesLeft == 0 || k.usesLeft < -1) return failure(request, "uses is a positive number, or left out for unlimited");
        const long long days = request.integer("days", 0);
        k.expires = days > 0 ? nowSeconds() + days * 86400 : request.integer("expires", 0);
        k.device = request.str("device");
        if (!k.device.empty() && !safeName(k.device)) return failure(request, "device is not a valid name");
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
    ::mkdir(_config.backupDirectory.c_str(), 0750);
    path = _config.backupDirectory + "/auth-" + stamp() + ".db";
    if (!_store.backup(path, error)) return false;
    // Keep the newest few. The names sort by time.
    std::vector<std::string> files;
    if (DIR* dir = ::opendir(_config.backupDirectory.c_str())) {
        while (dirent* entry = ::readdir(dir)) {
            const std::string name = entry->d_name;
            if (startsWith(name, "auth-") && name.size() > 3 && name.compare(name.size() - 3, 3, ".db") == 0) {
                files.push_back(name);
            }
        }
        ::closedir(dir);
    }
    std::sort(files.begin(), files.end());
    for (size_t i = 0; _config.backupsKept > 0 && i + (size_t)_config.backupsKept < files.size(); i++) {
        std::remove((_config.backupDirectory + "/" + files[i]).c_str());
    }
    std::lock_guard<std::mutex> lock(_backupMutex);
    _lastBackup = path;
    return true;
}

void AuthGate::backupLoop() {
    std::unique_lock<std::mutex> lock(_backupMutex);
    while (!_quit) {
        _backupWake.wait_for(lock, std::chrono::hours(_config.backupEveryHours));
        if (_quit) break;
        lock.unlock();
        std::string path, error;
        if (!backupNow(path, error)) {
            std::fprintf(stderr, "modbee serve: the login store backup failed: %s\n", error.c_str());
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

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "Authority.h"

#include "AuthHash.h"
#include "TopicRules.h"
#include "MqttTopic.h"

#include <cstring>
#include <list>

#include "WakeSignal.h"
#include <unistd.h>

namespace {

typedef std::chrono::steady_clock Clock;

std::string text(const MqttSpan& span) {
    return span.length ? std::string((const char*)span.data, span.length) : std::string();
}

MqttSpan spanOf(const std::string& s) {
    MqttSpan span;
    span.data = (const uint8_t*)s.data();
    span.length = (uint32_t)s.size();
    return span;
}

// The server's own requests: an admin's only.
bool isAdminTopic(const std::string& topic) {
    static const char kAdmin[] = "$modbee/admin";
    const size_t n = sizeof kAdmin - 1;
    return topic.compare(0, n, kAdmin) == 0 && (topic.size() == n || topic[n] == '/');
}

uint8_t reasonFor(AuthorityRefusal refusal) {
    switch (refusal) {
    case AuthorityRefusal::BadCredentials: return MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
    case AuthorityRefusal::Banned: return MQ_REASON_BANNED;
    case AuthorityRefusal::Quota: return MQ_REASON_QUOTA_EXCEEDED;
    case AuthorityRefusal::Unavailable: return MQ_REASON_SERVER_UNAVAILABLE;
    case AuthorityRefusal::NotAuthorized: break;
    }
    return MQ_REASON_NOT_AUTHORIZED;
}

// What a 5.0 client is told when its login is revoked: a code DISCONNECT may
// carry [V5:3.14.2.1], so "banned" and "bad credentials" are 0x87 there.
uint8_t disconnectReasonFor(AuthorityRefusal refusal) {
    switch (refusal) {
    case AuthorityRefusal::Quota: return MQ_REASON_QUOTA_EXCEEDED;
    case AuthorityRefusal::Unavailable: return MQ_REASON_SERVER_BUSY;
    default: break;
    }
    return MQ_REASON_NOT_AUTHORIZED;
}

// --- The rules, as they apply to one session ---

// What a session carries (MqttBrokerRequest::context): its scope's rules with
// its own ${username} and ${clientid} put in, once, at its CONNECT - so a
// PUBLISH matches and never builds a string. A rule whose value cannot be put
// in grants nothing and is left out (expand()).
struct SessionGrant {
    std::shared_ptr<const AuthorityScope> scope;
    std::vector<std::string> publish, subscribe, subscribeWide, deny;
};

std::vector<std::string> expanded(const std::vector<std::string>& rules, const std::string& user,
                                  const std::string& client) {
    std::vector<std::string> out;
    out.reserve(rules.size());
    std::string one;
    for (const std::string& rule : rules) {
        if (expand(rule, user, client, one)) out.push_back(one);
    }
    return out;
}

std::shared_ptr<const SessionGrant> grantFor(const std::shared_ptr<const AuthorityScope>& scope,
                                             const std::string& user, const std::string& client) {
    std::shared_ptr<SessionGrant> grant(new SessionGrant());
    grant->scope = scope;
    if (scope->templated) {
        grant->publish = expanded(scope->publish, user, client);
        grant->subscribe = expanded(scope->subscribe, user, client);
        grant->subscribeWide = expanded(scope->subscribeWide, user, client);
        grant->deny = expanded(scope->deny, user, client);
    } else {
        grant->publish = scope->publish;
        grant->subscribe = scope->subscribe;
        grant->subscribeWide = scope->subscribeWide;
        grant->deny = scope->deny;
    }
    return grant;
}

bool anyMatches(const std::vector<std::string>& rules, const std::string& topic) {
    const MqttSpan name = spanOf(topic);
    for (const std::string& filter : rules) {
        if (mq_topicMatches(spanOf(filter), name)) return true;
    }
    return false;
}

// A `$` filter is inside a rule only by naming it: a wildcard first level never
// reaches the `$` topics [MQTT-4.7.2-1].
bool anyCovers(const std::vector<std::string>& rules, const std::string& requested) {
    const bool dollar = !requested.empty() && requested[0] == '$';
    for (const std::string& filter : rules) {
        if (dollar && !filter.empty() && (filter[0] == '+' || filter[0] == '#')) continue;
        if (covers(filter, requested)) return true;
    }
    return false;
}

bool anyOverlaps(const std::vector<std::string>& rules, const std::string& requested) {
    const MqttSpan other = spanOf(requested);
    for (const std::string& filter : rules) {
        if (mq_filtersOverlap(spanOf(filter), other)) return true;
    }
    return false;
}

bool mayPublish(const SessionGrant& grant, const std::string& topic) {
    if (grant.scope->admin) return true;
    if (isAdminTopic(topic)) return false;
    if (grant.scope->everything) return topic.empty() || topic[0] != '$';
    if (anyMatches(grant.deny, topic)) return false;
    return anyMatches(grant.publish, topic);
}

bool mayReceive(const SessionGrant& grant, const std::string& topic) {
    if (grant.scope->admin) return true;
    if (isAdminTopic(topic)) return false;
    if (grant.scope->everything) return topic.empty() || topic[0] != '$';
    if (anyMatches(grant.deny, topic)) return false;
    return anyMatches(grant.subscribe, topic);
}

// Granted inside `subscribe` and clear of `deny`: delivered unchecked. Inside
// `subscribe` but touching `deny`, or inside `subscribeWide`: granted, every
// message through it asked about (narrow).
bool maySubscribe(const SessionGrant& grant, const MqttSpan& filter, bool& narrow) {
    narrow = false;
    MqttSpan share, inner = filter;
    if (mq_isSharedFilter(filter)) mq_splitSharedFilter(filter, share, inner);
    const std::string requested = text(inner);
    if (grant.scope->admin) return true;
    if (isAdminTopic(requested)) return false;
    if (grant.scope->everything) return requested.empty() || requested[0] != '$';
    if (anyCovers(grant.deny, requested)) return false;
    if (anyCovers(grant.subscribe, requested)) {
        narrow = anyOverlaps(grant.deny, requested);
        return true;
    }
    if (anyCovers(grant.subscribeWide, requested)) {
        narrow = true;
        return true;
    }
    return false;
}

// For MqttBroker::regrant: each session's own grant.
struct Regrant {
    std::shared_ptr<const AuthorityScope> scope;
};

std::shared_ptr<const void> regrantFor(void* context, const MqttBrokerSession& session) {
    return grantFor(static_cast<Regrant*>(context)->scope, session.userName, session.clientId());
}

} // namespace

std::string authorityTagOf(const void* context) {
    if (context == nullptr) return std::string();
    return static_cast<const SessionGrant*>(context)->scope->tag;
}

void AuthorityScope::settle() {
    templated = false;
    for (const std::vector<std::string>* rules : {&publish, &subscribe, &subscribeWide, &deny}) {
        for (const std::string& rule : *rules) {
            if (rule.find("${") != std::string::npos) templated = true;
        }
    }
}

// =============================================================================
// ONE SHARD: its kept decisions, and what the hub sends it
// =============================================================================

struct Authority::Command {
    enum Kind { Revoke, Regrant, Forget };
    Kind kind = Forget;
    std::string login, clientId;
    AuthorityRefusal reason = AuthorityRefusal::NotAuthorized;
    std::shared_ptr<const AuthorityScope> scope;
    uint32_t ttlSeconds = 0;
    std::atomic<size_t> remaining{0};
    std::mutex sum;
    AuthorityChange total;
    std::function<void(const AuthorityChange&)> done;
};

struct Authority::Shard {
    Authority* owner = nullptr;
    uint32_t index = 0;
    MqttBroker* broker = nullptr;
    WakeSignal waker;

    // --- The shard's own thread ---
    struct Kept {
        std::string login;               // the CONNECT's user name, for revoke and forget
        AuthorityDecision decision;      // allowed, or a refusal; never Unavailable
        Clock::time_point fresh;         // answered from without asking until
        Clock::time_point stale;         // allowed: still serves while the application is away
        std::list<std::string>::iterator place;
    };
    std::unordered_map<std::string, Kept> kept;
    std::list<std::string> order;        // most recently used first
    size_t capacity = 0;

    struct Outcome {
        bool unavailable = true;
        AuthorityDecision decision;
        std::string key;
        std::string login;               // the CONNECT's user name
    };
    std::unordered_map<uint64_t, Outcome> answers;   // a parked CONNECT's answer, by ticket

    // --- From the hub and the gRPC threads ---
    std::mutex lock;
    std::vector<std::pair<uint64_t, Outcome>> arrivals;
    std::vector<std::shared_ptr<Command>> commands;

    void wake() {
        waker.signal();
    }

    void keep(const std::string& key, const std::string& login, const AuthorityDecision& decision,
              uint32_t staleForSeconds) {
        if (capacity == 0) return;
        const Clock::time_point now = Clock::now();
        const Clock::time_point fresh = now + std::chrono::seconds(decision.ttlSeconds);
        // A refusal is kept only as long as it said; an allowance also as a
        // stand-in while the application is away.
        if (!decision.allow && decision.ttlSeconds == 0) return;
        if (decision.allow && decision.ttlSeconds == 0 && staleForSeconds == 0) return;
        auto found = kept.find(key);
        if (found == kept.end()) {
            while (kept.size() >= capacity && !order.empty()) {
                kept.erase(order.back());
                order.pop_back();
            }
            order.push_front(key);
            found = kept.emplace(key, Kept()).first;
            found->second.place = order.begin();
        } else {
            order.splice(order.begin(), order, found->second.place);
        }
        Kept& entry = found->second;
        entry.login = login;
        entry.decision = decision;
        entry.fresh = fresh;
        entry.stale = decision.allow ? fresh + std::chrono::seconds(staleForSeconds) : fresh;
    }

    size_t forgetLogin(const std::string& login) {
        size_t count = 0;
        for (auto at = kept.begin(); at != kept.end();) {
            if (login.empty() || at->second.login == login) {
                order.erase(at->second.place);
                at = kept.erase(at);
                count++;
            } else {
                ++at;
            }
        }
        return count;
    }
};

// =============================================================================
// THE EXTENSION
// =============================================================================

Authority::Authority(const AuthorityConfig& config) : _config(config) {
    std::memset(_salt, 0, sizeof _salt);
    std::shared_ptr<AuthorityScope> admin(new AuthorityScope());
    admin->admin = true;
    _adminScope = admin;
    std::shared_ptr<AuthorityScope> everything(new AuthorityScope());
    everything->everything = true;
    _everythingScope = everything;
}

Authority::~Authority() {
    {
        std::lock_guard<std::mutex> guard(_hub);
        _quit = true;
    }
    _tick.notify_all();
    if (_timer.joinable()) _timer.join();
    for (std::unique_ptr<Shard>& shard : _shards) {
        shard->waker.close();
    }
}

bool Authority::begin(size_t shards, std::string& error) {
    if (!auth::randomBytes(_salt, sizeof _salt)) {
        error = "authority: the kernel gave no random bytes";
        return false;
    }
    _shards.clear();
    for (size_t index = 0; index < shards; index++) {
        std::unique_ptr<Shard> shard(new Shard());
        shard->owner = this;
        shard->index = (uint32_t)index;
        shard->capacity = shards ? (_config.cacheSize + shards - 1) / shards : 0;
        if (!shard->waker.open()) {
            error = "authority: could not create a wake-up descriptor";
            return false;
        }
        _shards.push_back(std::move(shard));
    }
    _timer = std::thread([this] { timeouts(); });
    return true;
}

void Authority::prepare(uint32_t shard, MqttBroker& broker) {
    if (shard >= _shards.size()) return;
    _shards[shard]->broker = &broker;
    broker.onAuthorise(&Authority::hook, _shards[shard].get());
}

int Authority::waitHandle(uint32_t shard) {
    return shard < _shards.size() ? _shards[shard]->waker.handle() : -1;
}

void Authority::stop(uint32_t, MqttBroker&) {}

// --- The permission hook ---------------------------------------------------

bool Authority::hook(void* context, const MqttBrokerRequest& request, uint8_t& reasonCode) {
    Shard& shard = *static_cast<Shard*>(context);
    Authority& self = *shard.owner;
    if (request.action == MqttBrokerAction::Connect) return self.connect(shard, request, reasonCode);

    reasonCode = MQ_REASON_NOT_AUTHORIZED;
    const SessionGrant* grant = static_cast<const SessionGrant*>(request.context);
    // A session with no decision behind it - one restored from before a
    // restart, not yet back - may do nothing until it CONNECTs again.
    if (grant == nullptr) return false;
    switch (request.action) {
    case MqttBrokerAction::Publish:
        return mayPublish(*grant, text(request.topic));
    case MqttBrokerAction::Subscribe: {
        bool narrow = false;
        const bool ok = maySubscribe(*grant, request.topic, narrow);
        if (request.narrow) *request.narrow = narrow;
        return ok;
    }
    case MqttBrokerAction::Deliver:
        return mayReceive(*grant, text(request.topic));
    case MqttBrokerAction::Connect:
        break;
    }
    return false;
}

std::string Authority::keyOf(const MqttBrokerRequest& request) const {
    // Client id, user name, the password only as a salted hash - never kept in
    // clear - and the certificate: a different secret is a different question.
    std::string key = text(request.clientId);
    key.push_back('\0');
    key.push_back(request.userName.length ? '1' : '0');
    key += text(request.userName);
    key.push_back('\0');
    std::string salted((const char*)_salt, sizeof _salt);
    salted += text(request.password);
    uint8_t digest[32];
    auth::sha256((const uint8_t*)salted.data(), salted.size(), digest);
    key.append((const char*)digest, sizeof digest);
    key.push_back('\0');
    key += request.certificateFingerprint ? request.certificateFingerprint : "";
    return key;
}

namespace {

bool apply(const MqttBrokerRequest& request, const AuthorityDecision& decision, uint8_t& reasonCode) {
    if (!decision.allow || !decision.scope) {
        reasonCode = reasonFor(decision.refusal);
        return false;
    }
    // ${username} is the session's login: the one the decision names, if any.
    if (request.setContext) {
        *request.setContext = grantFor(decision.scope, decision.hasLogin ? decision.login : text(request.userName),
                                       text(request.clientId));
    }
    if (decision.hasLogin && request.login) *request.login = decision.login;
    if (decision.replaceSession && request.replaceSession) *request.replaceSession = true;
    if (decision.scope->hasMaxSessions && request.sessionLimit) *request.sessionLimit = decision.scope->maxSessions;
    if (decision.scope->hasMaxRetained && request.retainedLimit) *request.retainedLimit = decision.scope->maxRetained;
    return true;
}

} // namespace

bool Authority::connect(Shard& shard, const MqttBrokerRequest& request, uint8_t& reasonCode) {
    const std::string user = text(request.userName);

    // THE CONFIGURATION'S OWN LOGINS: the application itself, the modbee
    // command. Never asked - an application cannot be the authority for the
    // login it needs to reach the server.
    if (request.userName.length != 0) {
        const std::string password = text(request.password);
        for (int admin = 1; admin >= 0; admin--) {
            for (const auto& fixed : admin ? _config.admins : _config.logins) {
                if (fixed.first != user) continue;
                if (fixed.second != password) {
                    reasonCode = MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
                    return false;
                }
                if (request.setContext) *request.setContext = grantFor(admin ? _adminScope : _everythingScope, user, "");
                if (admin && request.sessionLimit) *request.sessionLimit = 0;
                if (admin && request.retainedLimit) *request.retainedLimit = 0;
                _fixed++;
                return true;
            }
        }
    }

    const std::string key = keyOf(request);
    const Clock::time_point now = Clock::now();

    // Nobody to ask, no answer in time, or too many waiting already: an
    // earlier answer still serves; without one, come back later.
    auto fromStale = [&](bool busy) {
        auto found = shard.kept.find(key);
        if (found != shard.kept.end() && found->second.decision.allow && now < found->second.stale) {
            _stale++;
            return apply(request, found->second.decision, reasonCode);
        }
        (busy ? _busy : _unavailable)++;
        reasonCode = busy ? MQ_REASON_SERVER_BUSY : MQ_REASON_SERVER_UNAVAILABLE;
        return false;
    };

    // Run again with its answer (retryConnect).
    if (request.ticket != 0) {
        auto answered = shard.answers.find(request.ticket);
        if (answered != shard.answers.end()) {
            Shard::Outcome outcome = std::move(answered->second);
            shard.answers.erase(answered);
            if (outcome.unavailable) return fromStale(false);
            return apply(request, outcome.decision, reasonCode);
        }
    }

    auto found = shard.kept.find(key);
    if (found != shard.kept.end() && now < found->second.fresh) {
        shard.order.splice(shard.order.begin(), shard.order, found->second.place);
        _cached++;
        return apply(request, found->second.decision, reasonCode);
    }

    AuthorityQuestion question;
    question.clientId = text(request.clientId);
    question.hasUsername = request.userName.length != 0;
    question.username = user;
    question.hasPassword = request.password.length != 0;
    question.password = text(request.password);
    question.address = request.peerName ? request.peerName : "";
    question.protocol = (uint8_t)request.version;
    question.certificateName = request.certificateName ? request.certificateName : "";
    question.certificateFingerprint = request.certificateFingerprint ? request.certificateFingerprint : "";
    const Asked asked = request.defer ? ask(shard.index, request.ticket, key, question) : Asked::NobodyToAsk;
    if (asked == Asked::Yes) {
        *request.defer = true;
        _asked++;
        return false;
    }
    return fromStale(asked == Asked::Full);
}

// --- The hub ---------------------------------------------------------------

Authority::Asked Authority::ask(uint32_t shard, uint64_t ticket, const std::string& key,
                                const AuthorityQuestion& question) {
    std::lock_guard<std::mutex> guard(_hub);
    if (_streams.empty()) return Asked::NobodyToAsk;
    if (_pending.size() >= _config.maxPending) return Asked::Full;
    Pending pending;
    pending.shard = shard;
    pending.ticket = ticket;
    pending.key = key;
    pending.question = question;
    pending.question.id = _nextId++;
    pending.on = _streams[_nextStream++ % _streams.size()];
    pending.deadline = Clock::now() + std::chrono::milliseconds(_config.timeoutMs);
    auto placed = _pending.emplace(pending.question.id, std::move(pending)).first;
    placed->second.on->ask(placed->second.question);
    return Asked::Yes;
}

void Authority::answer(const Pending& pending, const AuthorityDecision* decision) {
    if (pending.shard >= _shards.size()) return;
    Shard& shard = *_shards[pending.shard];
    Shard::Outcome outcome;
    outcome.key = pending.key;
    outcome.login = pending.question.username;
    if (decision != nullptr &&
        (decision->allow || decision->refusal != AuthorityRefusal::Unavailable)) {
        outcome.unavailable = false;
        outcome.decision = *decision;
    }
    {
        std::lock_guard<std::mutex> guard(shard.lock);
        shard.arrivals.emplace_back(pending.ticket, std::move(outcome));
    }
    shard.wake();
}

void Authority::attach(AuthorityStream* stream) {
    std::lock_guard<std::mutex> guard(_hub);
    for (AuthorityStream* known : _streams) {
        if (known == stream) return;
    }
    _streams.push_back(stream);
}

void Authority::detach(AuthorityStream* stream) {
    std::vector<Pending> unanswered;
    {
        std::lock_guard<std::mutex> guard(_hub);
        bool known = false;
        for (size_t at = 0; at < _streams.size(); at++) {
            if (_streams[at] == stream) {
                _streams.erase(_streams.begin() + (long)at);
                known = true;
                break;
            }
        }
        if (!known) return;
        // What it was asked and did not answer goes to another instance,
        // once; with none, or asked twice already, it is unavailable.
        for (auto at = _pending.begin(); at != _pending.end();) {
            Pending& pending = at->second;
            if (pending.on != stream) {
                ++at;
                continue;
            }
            if (!_streams.empty() && !pending.resent) {
                pending.resent = true;
                pending.on = _streams[_nextStream++ % _streams.size()];
                pending.on->ask(pending.question);
                _resent++;
                ++at;
            } else {
                unanswered.push_back(std::move(pending));
                at = _pending.erase(at);
            }
        }
    }
    for (const Pending& pending : unanswered) answer(pending, nullptr);
}

void Authority::decided(AuthorityStream*, const AuthorityDecision& decision) {
    Pending pending;
    {
        std::lock_guard<std::mutex> guard(_hub);
        auto found = _pending.find(decision.id);
        if (found == _pending.end()) return;     // late, or never asked
        pending = std::move(found->second);
        _pending.erase(found);
    }
    _answered++;
    answer(pending, &decision);
}

void Authority::timeouts() {
    std::unique_lock<std::mutex> guard(_hub);
    while (!_quit) {
        _tick.wait_for(guard, std::chrono::milliseconds(10));
        if (_quit) break;
        const Clock::time_point now = Clock::now();
        std::vector<Pending> expired;
        for (auto at = _pending.begin(); at != _pending.end();) {
            if (at->second.deadline <= now) {
                expired.push_back(std::move(at->second));
                at = _pending.erase(at);
            } else {
                ++at;
            }
        }
        if (expired.empty()) continue;
        guard.unlock();
        for (const Pending& pending : expired) {
            _timeouts++;
            answer(pending, nullptr);
        }
        guard.lock();
    }
}

// --- On each shard's thread ------------------------------------------------

void Authority::service(uint32_t index, MqttBroker& broker) {
    if (index >= _shards.size()) return;
    Shard& shard = *_shards[index];
    shard.waker.drain();

    std::vector<std::pair<uint64_t, Shard::Outcome>> arrivals;
    std::vector<std::shared_ptr<Command>> commands;
    {
        std::lock_guard<std::mutex> guard(shard.lock);
        arrivals.swap(shard.arrivals);
        commands.swap(shard.commands);
    }

    for (auto& arrival : arrivals) {
        Shard::Outcome& outcome = arrival.second;
        if (!outcome.unavailable) {
            shard.keep(outcome.key, outcome.login, outcome.decision, _config.staleForSeconds);
        }
        const uint64_t ticket = arrival.first;
        shard.answers[ticket] = std::move(outcome);
        // Gone meanwhile: its answer is kept, for when it comes back.
        if (!broker.retryConnect(ticket)) shard.answers.erase(ticket);
    }

    for (const std::shared_ptr<Command>& command : commands) {
        AuthorityChange change;
        switch (command->kind) {
        case Command::Revoke:
            change.forgotten = shard.forgetLogin(command->login);
            change.closed = broker.disconnectUser(spanOf(command->login), disconnectReasonFor(command->reason));
            break;
        case Command::Regrant: {
            size_t removed = 0;
            Regrant made;
            made.scope = command->scope;
            change.sessions = broker.regrant(spanOf(command->login), spanOf(command->clientId),
                                             &regrantFor, &made, &removed);
            change.removed = removed;
            for (auto& entry : shard.kept) {
                if (entry.second.login != command->login || !entry.second.decision.allow) continue;
                entry.second.decision.scope = command->scope;
                entry.second.fresh = Clock::now() + std::chrono::seconds(command->ttlSeconds);
                entry.second.stale = entry.second.fresh + std::chrono::seconds(_config.staleForSeconds);
            }
            break;
        }
        case Command::Forget:
            change.forgotten = shard.forgetLogin(command->login);
            break;
        }
        {
            std::lock_guard<std::mutex> guard(command->sum);
            command->total.closed += change.closed;
            command->total.sessions += change.sessions;
            command->total.removed += change.removed;
            command->total.forgotten += change.forgotten;
        }
        if (command->remaining.fetch_sub(1) == 1 && command->done) command->done(command->total);
    }
}

void Authority::broadcast(std::shared_ptr<Command> command) {
    if (_shards.empty()) {
        if (command->done) command->done(command->total);
        return;
    }
    command->remaining = _shards.size();
    for (std::unique_ptr<Shard>& shard : _shards) {
        {
            std::lock_guard<std::mutex> guard(shard->lock);
            shard->commands.push_back(command);
        }
        shard->wake();
    }
}

void Authority::revoke(const std::string& login, AuthorityRefusal reason,
                       std::function<void(const AuthorityChange&)> done) {
    std::shared_ptr<Command> command(new Command());
    command->kind = Command::Revoke;
    command->login = login;
    command->reason = reason;
    command->done = std::move(done);
    broadcast(command);
}

void Authority::regrant(const std::string& login, const std::string& clientId,
                        std::shared_ptr<const AuthorityScope> scope, uint32_t ttlSeconds,
                        std::function<void(const AuthorityChange&)> done) {
    std::shared_ptr<Command> command(new Command());
    command->kind = Command::Regrant;
    command->login = login;
    command->clientId = clientId;
    command->scope = std::move(scope);
    command->ttlSeconds = ttlSeconds;
    command->done = std::move(done);
    broadcast(command);
}

void Authority::forget(const std::string& login, std::function<void(const AuthorityChange&)> done) {
    std::shared_ptr<Command> command(new Command());
    command->kind = Command::Forget;
    command->login = login;
    command->done = std::move(done);
    broadcast(command);
}

AuthorityStatus Authority::status() const {
    AuthorityStatus out;
    out.asked = _asked.load();
    out.answered = _answered.load();
    out.cached = _cached.load();
    out.stale = _stale.load();
    out.unavailable = _unavailable.load();
    out.timeouts = _timeouts.load();
    out.resent = _resent.load();
    out.fixed = _fixed.load();
    out.busy = _busy.load();
    std::lock_guard<std::mutex> guard(_hub);
    out.streams = _streams.size();
    out.pending = _pending.size();
    return out;
}

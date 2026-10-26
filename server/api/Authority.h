// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// LOGINS DECIDED BY THE APPLICATION (authentication: authority:)
// =============================================================================
//
// The Application API's authority (proto/modbee/app/v1/app.proto, service
// Authority; internal/APP-API-SPEC.md section 2). A ShardExtension: on every
// shard it is the broker's permission hook.
//
//   CONNECT    a fixed login from the configuration is decided here, as with
//              a login database. Any other is asked of the application -
//              ONCE: its answer is kept, per shard (a client identifier
//              always lands on the same shard), for the time it said. While
//              it is asked the CONNECT is parked (MqttBrokerRequest::defer):
//              nothing waits. With nobody to ask, or no answer in time, an
//              expired answer still serves for `staleFor`; without one the
//              client is told 0x88 Server unavailable, which it retries.
//   PUBLISH,   decided here, from the Scope the application gave the login
//   SUBSCRIBE, at CONNECT - its compiled rules, carried by the session
//   DELIVER    (MqttBrokerRequest::context). No message waits on anybody.
//
// The application side is an AuthorityStream: one per connected application
// instance (ApiServer.cpp turns the gRPC stream into one). Questions are dealt
// round the instances; one left unanswered on a stream that ends is asked of
// another. This file knows nothing of gRPC, so it is tested without it.

#include "ShardedBroker.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// What a login may do, as the application said (app.proto Scope). Filters may
// hold ${username} and ${clientid}, put in per request by expand().
struct AuthorityScope {
    std::vector<std::string> publish;        // may publish to (its Will included)
    std::vector<std::string> subscribe;      // may receive; a SUBSCRIBE inside one is granted
    std::vector<std::string> subscribeWide;  // may subscribe WITH, receiving only what `subscribe` allows
    std::vector<std::string> deny;           // never, whatever the above say
    bool hasMaxSessions = false;
    uint32_t maxSessions = 0;
    bool hasMaxRetained = false;
    uint32_t maxRetained = 0;
    std::string tag;                         // opaque, <= 255 bytes

    // The configuration's fixed logins, decided without the application.
    bool admin = false;                      // everything, $modbee/admin included
    bool everything = false;                 // every topic but the $ ones

    // Any ${...} in any list: then every check expands; else none does.
    bool templated = false;
    void settle();                           // computes `templated`
};

enum class AuthorityRefusal : uint8_t {
    NotAuthorized,       // 0x87 (3.1.1: 5)
    BadCredentials,      // 0x86 (3.1.1: 4)
    Banned,              // 0x8A (3.1.1: 5)
    Quota,               // 0x97 (3.1.1: 3)
    Unavailable,         // 0x88 (3.1.1: 3) - could not check; never kept
};

struct AuthorityQuestion {
    uint64_t id = 0;
    std::string clientId;
    bool hasUsername = false;
    std::string username;
    bool hasPassword = false;
    std::string password;
    std::string address;
    uint8_t protocol = 5;
    std::string certificateName;             // a certificate the handshake VERIFIED
    std::string certificateFingerprint;
};

struct AuthorityDecision {
    uint64_t id = 0;
    bool allow = false;
    AuthorityRefusal refusal = AuthorityRefusal::NotAuthorized;
    uint32_t ttlSeconds = 0;
    std::shared_ptr<const AuthorityScope> scope;
    bool hasLogin = false;
    std::string login;
    bool replaceSession = false;
};

// One connected application instance, as the authority sees it. ask() is
// called with the authority's lock held and must not block or call back in.
class AuthorityStream {
public:
    virtual ~AuthorityStream() {}
    virtual void ask(const AuthorityQuestion& question) = 0;
};

struct AuthorityConfig {
    std::vector<std::pair<std::string, std::string>> admins;   // fixed admin logins
    std::vector<std::pair<std::string, std::string>> logins;   // fixed ordinary logins
    uint32_t timeoutMs = 2000;
    uint32_t staleForSeconds = 86400;
    size_t cacheSize = 1000000;       // whole server
    size_t maxPending = 10000;         // whole server
};

struct AuthorityStatus {
    uint64_t asked = 0;          // CONNECTs asked of the application
    uint64_t answered = 0;
    uint64_t cached = 0;         // CONNECTs answered from a kept decision
    uint64_t stale = 0;          // ... from one past its time, the application away
    uint64_t unavailable = 0;    // refused 0x88: nobody to ask, and nothing kept
    uint64_t timeouts = 0;       // questions with no answer in time
    uint64_t busy = 0;           // refused 0x89: max-pending CONNECTs were waiting already
    uint64_t resent = 0;         // asked again of another instance
    uint64_t fixed = 0;          // fixed logins from the configuration
    size_t streams = 0;          // application instances serving now
    size_t pending = 0;
};

// The outcome of revoke / regrant / forget, summed over the shards.
struct AuthorityChange {
    uint64_t closed = 0;         // connections closed (revoke)
    uint64_t sessions = 0;       // sessions given the new scope (regrant)
    uint64_t removed = 0;        // subscriptions it no longer allowed (regrant)
    uint64_t forgotten = 0;      // kept decisions dropped
};

class Authority : public ShardExtension {
public:
    explicit Authority(const AuthorityConfig& config);
    ~Authority() override;

    // --- ShardExtension ---
    bool begin(size_t shards, std::string& error) override;
    void prepare(uint32_t shard, MqttBroker& broker) override;
    int waitHandle(uint32_t shard) override;
    uint32_t offer(uint32_t, MqttBroker&, const MqttBrokerRouted&, uint64_t) override { return 0; }
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t) override { return false; }
    void stop(uint32_t shard, MqttBroker& broker) override;

    // --- The application's side, from any thread ---

    // An instance that opened Authority.Serve, and its end.
    void attach(AuthorityStream* stream);
    void detach(AuthorityStream* stream);
    // Its answer to a question it was asked.
    void decided(AuthorityStream* stream, const AuthorityDecision& decision);

    // Changes, carried out on every shard; `done` is called once, on a shard's
    // thread, with the totals.
    void revoke(const std::string& login, AuthorityRefusal reason,
                std::function<void(const AuthorityChange&)> done);
    void regrant(const std::string& login, const std::string& clientId,
                 std::shared_ptr<const AuthorityScope> scope, uint32_t ttlSeconds,
                 std::function<void(const AuthorityChange&)> done);
    void forget(const std::string& login, std::function<void(const AuthorityChange&)> done);

    AuthorityStatus status() const;

    struct Shard;

private:
    AuthorityConfig _config;
    std::vector<std::unique_ptr<Shard>> _shards;
    uint8_t _salt[32];

    // --- The hub: instances, and what they have been asked ---
    struct Pending {
        uint32_t shard = 0;
        uint64_t ticket = 0;
        std::string key;
        AuthorityQuestion question;
        AuthorityStream* on = nullptr;
        bool resent = false;
        std::chrono::steady_clock::time_point deadline;
    };
    mutable std::mutex _hub;
    std::vector<AuthorityStream*> _streams;
    size_t _nextStream = 0;
    uint64_t _nextId = 1;
    std::unordered_map<uint64_t, Pending> _pending;
    std::condition_variable _tick;
    bool _quit = false;
    std::thread _timer;

    std::atomic<uint64_t> _asked{0}, _answered{0}, _cached{0}, _stale{0}, _unavailable{0};
    std::atomic<uint64_t> _timeouts{0}, _resent{0}, _fixed{0}, _busy{0};

    std::shared_ptr<const AuthorityScope> _adminScope, _everythingScope;

    friend struct Shard;
    static bool hook(void* context, const MqttBrokerRequest& request, uint8_t& reasonCode);
    bool connect(Shard& shard, const MqttBrokerRequest& request, uint8_t& reasonCode);
    enum class Asked { Yes, NobodyToAsk, Full };
    Asked ask(uint32_t shard, uint64_t ticket, const std::string& key, const AuthorityQuestion& question);
    void answer(const Pending& pending, const AuthorityDecision* decision);   // null: unavailable
    void timeouts();
    std::string keyOf(const MqttBrokerRequest& request) const;
    struct Command;
    void broadcast(std::shared_ptr<Command> command);
};

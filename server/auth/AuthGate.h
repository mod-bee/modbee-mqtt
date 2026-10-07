// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// DEVICE LOGINS, LIVE: who may connect, what they may touch, and how devices
// are added - without a restart (authentication: database: in modbee.yaml)
// =============================================================================
//
// A ShardExtension. On every shard it is the broker's permission hook:
//
//   CONNECT    an admin login (from the config), a device login (from the
//              SQLite store, AuthStore.h), or `$provision` with a
//              provisioning key. Nothing else: anonymous is refused.
//   PUBLISH,   a device: only the topic filters in its record, with
//   SUBSCRIBE  ${username} and ${clientid} expanded - and never a `$` topic.
//              An admin: anything. `$provision`: its own request and reply
//              topics, nothing more.
//
// And it answers two kinds of request, published to `$modbee/` topics that
// no ordinary subscription can see [MQTT-4.7.2-1]:
//
//   $modbee/admin/request                 from an admin: add, reset, remove,
//      -> $modbee/admin/<clientid>/reply  approve devices; create keys;
//                                         backup. The modbee CLI uses it.
//   $modbee/provision/<clientid>/request  from a device with a provisioning
//      -> $modbee/provision/<clientid>/reply  key: its own login, generated
//
// Every change is live. A new login works at its first CONNECT; one that is
// removed, disabled or reset has its live connections closed (0x87) within a
// shard's loop pass; cached permissions are dropped when the store's
// generation moves. internal/AUTH-PLAN.md is the design.

#include "ShardedBroker.h"
#include "AuthStore.h"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

struct AuthGateConfig {
    std::string database;                                       // the SQLite file
    std::vector<std::pair<std::string, std::string>> admins;    // fixed admin logins
    std::vector<std::pair<std::string, std::string>> logins;    // fixed ordinary logins
    std::string backupDirectory;                                // "" = <database's folder>/backups
    int backupsKept = 7;
    int backupEveryHours = 24;                                  // 0 = only on request
};

struct AuthGateStatus {
    uint64_t accepted = 0;
    uint64_t refused = 0;
    uint64_t provisioned = 0;
    uint64_t kicked = 0;
};

class AuthGate : public ShardExtension {
public:
    explicit AuthGate(const AuthGateConfig& config);
    ~AuthGate() override;

    bool begin(size_t shards, std::string& error) override;
    void prepare(uint32_t shard, MqttBroker& broker) override;
    int waitHandle(uint32_t) override { return -1; }
    uint32_t offer(uint32_t shard, MqttBroker& broker, const MqttBrokerRouted& message,
                   uint64_t deferral) override;
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t shard) override;
    void stop(uint32_t shard, MqttBroker& broker) override;

    // The request handlers, callable directly (tests, and the CLI's own
    // offline commands): a request document in, a reply document out.
    std::string admin(const std::string& request);
    std::string provision(const std::string& clientId, const std::string& keyName,
                          const std::string& request);

    AuthStore& store() { return _store; }
    AuthGateStatus status() const;
    std::string lastBackup() const;

private:
    struct Compiled {
        bool found = false;
        bool admin = false;
        std::vector<std::string> publish;
        std::vector<std::string> subscribe;
    };
    struct Request {
        bool admin = false;
        std::string clientId;
        std::string payload;
    };
    struct Shard {
        AuthGate* gate = nullptr;
        uint32_t index = 0;
        std::unordered_map<std::string, Compiled> cache;
        uint64_t cacheGeneration = 0;
        std::mutex mutex;                        // guards the two lists below
        std::vector<std::string> kicks;
        std::vector<Request> requests;
        std::atomic<bool> pending{false};
    };

    static bool hook(void* context, const MqttBrokerRequest& request, uint8_t& reasonCode);
    bool authorise(Shard& shard, const MqttBrokerRequest& request, uint8_t& reasonCode);
    const Compiled& permissions(Shard& shard, const std::string& username);
    bool fixedLogin(const std::vector<std::pair<std::string, std::string>>& list,
                    const MqttSpan& user, const MqttSpan& password) const;
    bool isFixedAdmin(const std::string& username) const;
    bool checkKey(const std::string& offered, const std::string& clientId, std::string& keyName);

    // Closes every live connection of a login, on every shard.
    void kick(const std::string& username);
    void queue(uint32_t shard, const Request& request);

    bool backupNow(std::string& path, std::string& error);
    void backupLoop();

    AuthGateConfig _config;
    AuthStore _store;
    std::vector<std::unique_ptr<Shard>> _shards;

    // Which provisioning key each `$provision` session connected with, by
    // client identifier - set at its CONNECT, read at its request.
    std::mutex _provisioningMutex;
    std::unordered_map<std::string, std::string> _provisioning;

    std::atomic<uint64_t> _accepted{0}, _refused{0}, _provisioned{0}, _kicked{0};

    std::thread _backups;
    std::mutex _backupMutex;
    std::condition_variable _backupWake;
    bool _quit = false;
    std::string _lastBackup;
    std::atomic<size_t> _stopped{0};
};

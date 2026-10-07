// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// MQTT logins for a multi-core server (authentication: in modbee.yaml).
//
// The library's MqttBrokerAccounts on every shard: a CONNECT is checked on
// whichever shard it lands on, against the same list, compared in constant
// time over the whole value. One table PER SHARD, because a table counts the
// logins and refusals it decides, and shards decide at the same time.
//
// Any account makes logging in required - for every client, on every
// listener. Topic permissions are not here: this answers who may connect.

#include "ShardedBroker.h"

#include <broker/MqttBrokerAccounts.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

class ShardAccounts : public ShardExtension {
public:
    void add(const std::string& username, const std::string& password) {
        _accounts.push_back(std::make_pair(username, password));
    }
    size_t count() const { return _accounts.size(); }

    bool begin(size_t shards, std::string& error) override {
        _tables.clear();
        for (size_t s = 0; s < shards; s++) {
            std::unique_ptr<MqttBrokerAccounts> table(new MqttBrokerAccounts());
            for (const auto& account : _accounts) {
                if (!table->set(account.first.c_str(), account.second.c_str())) {
                    error = "the MQTT login \"" + account.first + "\" could not be added: a user name "
                            "and password are each at most " +
                            std::to_string(MQ_BROKER_ACCOUNT_FIELD_BYTES - 1) + " characters, and there "
                            "are at most " + std::to_string(MQ_BROKER_MAX_ACCOUNTS) + " logins";
                    return false;
                }
            }
            _tables.push_back(std::move(table));
        }
        return true;
    }

    void prepare(uint32_t shard, MqttBroker& broker) override {
        broker.onAuthorise(&MqttBrokerAccounts::hook, _tables[shard].get());
    }

    int waitHandle(uint32_t) override { return -1; }
    uint32_t offer(uint32_t, MqttBroker&, const MqttBrokerRouted&, uint64_t) override { return 0; }
    void service(uint32_t, MqttBroker&) override {}
    bool hasPendingWork(uint32_t) override { return false; }
    void stop(uint32_t, MqttBroker&) override {}

    // Refused logins, all shards together - a status line's worth. Read
    // without a lock: a count a moment stale is fine for a status line.
    uint64_t refused() const {
        uint64_t total = 0;
        for (const auto& table : _tables) {
            total += table->refusedUnknown() + table->refusedAnonymous() +
                     table->refusedPassword() + table->refusedDisabled();
        }
        return total;
    }

private:
    std::vector<std::pair<std::string, std::string>> _accounts;
    std::vector<std::unique_ptr<MqttBrokerAccounts>> _tables;
};

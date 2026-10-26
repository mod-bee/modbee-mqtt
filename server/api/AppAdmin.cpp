// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AppAdmin.h"

#include "WakeSignal.h"

struct AppAdmin::Shard {
    WakeSignal wake;
    MqttBroker* broker = nullptr;
    std::mutex lock;
    std::vector<std::function<void(MqttBroker&)>> work;
    bool stopped = false;
};

AppAdmin::AppAdmin() = default;

AppAdmin::~AppAdmin() {
    for (std::unique_ptr<Shard>& shard : _shards) {
        shard->wake.close();
    }
}

bool AppAdmin::begin(size_t shards, std::string& error) {
    _shards.clear();
    for (size_t index = 0; index < shards; index++) {
        std::unique_ptr<Shard> shard(new Shard());
        if (!shard->wake.open()) {
            error = "admin: could not create a wake-up descriptor";
            return false;
        }
        _shards.push_back(std::move(shard));
    }
    return true;
}

void AppAdmin::prepare(uint32_t shard, MqttBroker& broker) {
    if (shard < _shards.size()) _shards[shard]->broker = &broker;
}

int AppAdmin::waitHandle(uint32_t shard) {
    return shard < _shards.size() ? _shards[shard]->wake.handle() : -1;
}

void AppAdmin::service(uint32_t index, MqttBroker& broker) {
    if (index >= _shards.size()) return;
    Shard& shard = *_shards[index];
    shard.wake.drain();
    std::vector<std::function<void(MqttBroker&)>> work;
    {
        std::lock_guard<std::mutex> guard(shard.lock);
        work.swap(shard.work);
    }
    for (std::function<void(MqttBroker&)>& one : work) one(broker);
}

void AppAdmin::stop(uint32_t index, MqttBroker& broker) {
    if (index >= _shards.size()) return;
    // What was asked is still answered; nothing is asked after.
    service(index, broker);
    std::lock_guard<std::mutex> guard(_shards[index]->lock);
    _shards[index]->stopped = true;
}

void AppAdmin::oneShard(uint32_t index, std::function<void(MqttBroker&)> work) {
    if (index >= _shards.size()) return;
    Shard& shard = *_shards[index];
    {
        std::lock_guard<std::mutex> guard(shard.lock);
        if (shard.stopped) return;
        shard.work.push_back(std::move(work));
    }
    shard.wake.signal();
}

void AppAdmin::everyShard(std::function<void(uint32_t, MqttBroker&)> work, std::function<void()> done) {
    if (_shards.empty()) {
        if (done) done();
        return;
    }
    std::shared_ptr<std::atomic<size_t>> remaining(new std::atomic<size_t>(_shards.size()));
    for (uint32_t index = 0; index < _shards.size(); index++) {
        oneShard(index, [index, work, done, remaining](MqttBroker& broker) {
            work(index, broker);
            if (remaining->fetch_sub(1) == 1 && done) done();
        });
    }
}

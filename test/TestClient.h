// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// The library's own client, as the published tests drive it: what it was
// sent, what completed, what failed - and a fleet of them polled together
// with whatever serves them.

#include "Check.h"

#include <memory>
#include <utility>
#include <vector>

namespace check {

struct Got {
    std::string topic, payload;
    uint8_t qos = 0;
    bool retain = false;
};


// --- A client of the library's own ---

struct Client {
    MqttTcpTransport transport;
    MqttClient mqtt;
    MqttConnectionConfig config;
    MqttClientConfig behaviour;
    std::vector<Got> got;
    int completed = 0, failed = 0, subscribed = 0;
    MqttResult lastFailure = MqttResult::Ok;
    bool begun = false;
    std::vector<std::pair<std::string, uint8_t>> wanted;   // asked before start()

    Client(const std::string& id, uint16_t port, MqttVersion version = MqttVersion::V5) {
        transport.configure("127.0.0.1", port);
        config.host = "127.0.0.1";
        config.port = port;
        config.clientId = id;
        config.version = version;
        config.keepAlive = 30;
        behaviour.autoReconnect = false;
        behaviour.queueCapacity = 256;
        behaviour.receiveBufferSize = 300000;
        behaviour.transmitBufferSize = 300000;
    }
    void login(const std::string& user, const std::string& password) {
        config.hasUserName = true;
        config.userName = user;
        config.hasPassword = true;
        config.password.assign(password.begin(), password.end());
    }
    void persistent(uint32_t expirySeconds) {
        config.cleanStart = false;
        config.hasSessionExpiry = true;
        config.sessionExpiryInterval = expirySeconds;
    }
    void will(const std::string& topic, const std::string& payload, uint8_t qos, bool retain) {
        config.will.enabled = true;
        config.will.topic = topic;
        config.will.payload.assign(payload.begin(), payload.end());
        config.will.qos = qos;
        config.will.retain = retain;
    }
    void start() {
        mqtt.begin(transport, config, behaviour);
        mqtt.onMessage(&Client::received, this);
        begun = true;
        for (auto& w : wanted) mqtt.subscribe(w.first.c_str(), w.second, &Client::subscribedTo, this);
        mqtt.connect();
    }
    static void received(void* context, const MqttMessage& message) {
        Got g;
        g.topic = text(message.topic);
        g.payload = text(message.payload);
        g.qos = message.qos;
        g.retain = message.retain;
        static_cast<Client*>(context)->got.push_back(g);
    }
    static void done(void* context, MqttResult result, uint16_t) {
        Client& self = *static_cast<Client*>(context);
        if (result == MqttResult::Ok) {
            self.completed++;
        } else {
            self.failed++;
            self.lastFailure = result;
        }
    }
    static void subscribedTo(void* context, MqttResult result, uint16_t) {
        if (result == MqttResult::Ok) static_cast<Client*>(context)->subscribed++;
    }
    // Recorded before the connection; answered by its SUBACK.
    void subscribe(const std::string& filter, uint8_t qos) {
        if (!begun) wanted.emplace_back(filter, qos);
        else mqtt.subscribe(filter.c_str(), qos, &Client::subscribedTo, this);
    }
    // One of a sequence: never replaced by a newer one (publish() is a value,
    // and a newer value for the topic replaces one still waiting).
    MqttResult series(const std::string& topic, const std::string& payload, uint8_t qos) {
        return mqtt.publishSeries(topic.c_str(), (const uint8_t*)payload.data(), payload.size(), qos, false,
                                  &Client::done, this);
    }
    MqttResult publish(const std::string& topic, const std::string& payload, uint8_t qos, bool retain = false) {
        return mqtt.publish(topic.c_str(), (const uint8_t*)payload.data(), payload.size(), qos, retain,
                            &Client::done, this);
    }
};


// Clients made here are polled together, with `serve` - a broker's poll(),
// or nothing for a server on threads of its own.
struct Fleet {
    uint16_t port;
    std::function<void()> serve;
    std::vector<std::unique_ptr<Client>> all;

    Fleet(uint16_t p, std::function<void()> s = [] {}) : port(p), serve(std::move(s)) {}
    Client& make(const std::string& id, MqttVersion version = MqttVersion::V5) {
        all.emplace_back(new Client(id, port, version));
        return *all.back();
    }
    void poll() {
        serve();
        for (auto& c : all) c->mqtt.poll();
    }
    bool until(const std::function<bool()>& done, int ms = 5000) {
        return check::until(done, [this] { poll(); }, ms);
    }
    bool connected(Client& c, int ms = 5000) {
        c.start();
        return until([&] { return c.mqtt.isConnected(); }, ms);
    }
};

} // namespace check

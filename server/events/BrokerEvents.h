// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// WHAT CLIENTS DO, AS MESSAGES (events: in modbee.yaml)
// =============================================================================
//
// A ShardExtension. Every client that connects, goes, subscribes or
// unsubscribes is published, as a small JSON document, on
//
//   $modbee/events/started        {"event","time"} - once, when the server starts: every
//                                 session before it is gone (sessions live in memory)
//   $modbee/events/connected      {"event","clientid","username","resumed","time"}
//   $modbee/events/disconnected   + "reason": the reason code sent, 0 if the peer vanished
//   $modbee/events/subscribed     + "filter", "qos"
//   $modbee/events/unsubscribed   + "filter"
//
// WHY. Some facts exist only in the broker: that a device is connected right
// now, and which topics it listens on - a device subscribing to
// .../cmd/SETPOINT is the only place it ever says it accepts SETPOINT. An
// application that is not an MQTT client (one reading Kafka) learns them here.
//
// HOW IT TRAVELS. As any message: an admin login may subscribe to
// $modbee/events/#, and a Kafka mapping with that filter makes them durable
// (an application that was down reads what it missed). Each event carries the
// client's id as its publisher, so `key: clientid` keeps one client's events
// in order. `$` topics are never matched by a `#` [MQTT-4.7.2-1], so no
// ordinary subscription or catch-all mapping sees them by accident.
//
// Events are queued in the broker's hook and published from the shard's own
// service pass - never re-entering the broker from inside its handling of the
// packet that caused them. A client's events happen on its home shard, in order.

#include "ShardedBroker.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

class BrokerEvents : public ShardExtension {
public:
    bool begin(size_t shards, std::string& error) override;
    void prepare(uint32_t shard, MqttBroker& broker) override;
    int waitHandle(uint32_t) override { return -1; }
    uint32_t offer(uint32_t, MqttBroker&, const MqttBrokerRouted&, uint64_t) override { return 0; }
    void service(uint32_t shard, MqttBroker& broker) override;
    bool hasPendingWork(uint32_t shard) override;
    void stop(uint32_t shard, MqttBroker& broker) override;

    uint64_t published() const { return _published.load(); }

    // On or off, live (Admin.PutEvents): off, a client's events are not
    // made at all. On by default; serve sets it from events: enabled.
    void enable(bool on) { _enabled.store(on, std::memory_order_relaxed); }
    bool enabled() const { return _enabled.load(std::memory_order_relaxed); }

    // The document a server start becomes (exposed for tests).
    static std::string startedDocument(uint64_t timeMillis);

    // The document an event becomes (exposed for tests).
    static std::string document(const MqttBrokerClientEvent& event, uint64_t timeMillis);
    static const char* topicFor(MqttBrokerClientEventKind kind);

private:
    struct Pending {
        std::string topic;
        std::string clientId;
        std::string userName;
        std::string body;
    };
    struct Shard {
        BrokerEvents* owner = nullptr;
        std::vector<Pending> pending;      // only ever touched on the shard's own thread
    };
    static void onEvent(void* context, const MqttBrokerClientEvent& event);

    std::vector<std::unique_ptr<Shard>> _shards;
    bool _announceStart = false;            // read and cleared on shard 0's own thread
    std::atomic<uint64_t> _published{0};
    std::atomic<bool> _enabled{true};
};

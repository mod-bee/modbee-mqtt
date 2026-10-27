// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "BrokerEvents.h"

#include <chrono>
#include <cstdio>

namespace {

std::string text(const MqttSpan& s) { return std::string((const char*)s.data, s.length); }

MqttSpan spanOf(const std::string& s) {
    MqttSpan out;
    out.data = (const uint8_t*)s.data();
    out.length = (uint32_t)s.size();
    return out;
}

// A JSON string: quotes, backslashes and control characters escaped.
std::string quote(const std::string& in) {
    std::string out = "\"";
    for (unsigned char c : in) {
        if (c == '"' || c == '\\') { out += '\\'; out += (char)c; }
        else if (c < 0x20) {
            char buf[8];
            std::snprintf(buf, sizeof buf, "\\u%04x", c);
            out += buf;
        } else {
            out += (char)c;
        }
    }
    return out + "\"";
}

uint64_t nowMillis() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

} // namespace

const char* BrokerEvents::topicFor(MqttBrokerClientEventKind kind) {
    switch (kind) {
    case MqttBrokerClientEventKind::Connected: return "$modbee/events/connected";
    case MqttBrokerClientEventKind::Disconnected: return "$modbee/events/disconnected";
    case MqttBrokerClientEventKind::Subscribed: return "$modbee/events/subscribed";
    case MqttBrokerClientEventKind::Unsubscribed: return "$modbee/events/unsubscribed";
    case MqttBrokerClientEventKind::PublishRefused: return "$modbee/events/publish-refused";
    case MqttBrokerClientEventKind::SubscribeRefused: return "$modbee/events/subscribe-refused";
    case MqttBrokerClientEventKind::ConnectRefused: return "$modbee/events/connect-refused";
    }
    return "$modbee/events/unknown";
}

std::string BrokerEvents::document(const MqttBrokerClientEvent& e, uint64_t timeMillis) {
    const char* name = topicFor(e.kind) + 15;   // after "$modbee/events/"
    std::string out = "{\"event\":" + quote(name) + ",\"clientid\":" + quote(text(e.clientId)) +
                      ",\"username\":" + quote(text(e.userName));
    if (e.kind == MqttBrokerClientEventKind::Subscribed || e.kind == MqttBrokerClientEventKind::Unsubscribed ||
        e.kind == MqttBrokerClientEventKind::SubscribeRefused) {
        out += ",\"filter\":" + quote(text(e.filter));
    }
    if (e.kind == MqttBrokerClientEventKind::PublishRefused) out += ",\"topic\":" + quote(text(e.filter));
    if (e.kind == MqttBrokerClientEventKind::Subscribed) out += ",\"qos\":" + std::to_string(e.qos);
    if (e.kind == MqttBrokerClientEventKind::Disconnected || e.kind == MqttBrokerClientEventKind::PublishRefused ||
        e.kind == MqttBrokerClientEventKind::SubscribeRefused || e.kind == MqttBrokerClientEventKind::ConnectRefused) {
        out += ",\"reason\":" + std::to_string(e.reasonCode);
    }
    // A clean start drops every subscription the client had, with no unsubscribed events; a resumed
    // session keeps them without new subscribed ones. A reader keeping subscriptions needs to know which.
    if (e.kind == MqttBrokerClientEventKind::Connected) out += std::string(",\"resumed\":") + (e.resumed ? "true" : "false");
    return out + ",\"time\":" + std::to_string(timeMillis) + "}";
}

std::string BrokerEvents::startedDocument(uint64_t timeMillis) {
    return "{\"event\":\"started\",\"time\":" + std::to_string(timeMillis) + "}";
}

bool BrokerEvents::begin(size_t shards, std::string&) {
    _shards.clear();
    _announceStart = true;
    for (size_t i = 0; i < shards; i++) {
        std::unique_ptr<Shard> s(new Shard());
        s->owner = this;
        _shards.push_back(std::move(s));
    }
    return true;
}

void BrokerEvents::prepare(uint32_t shard, MqttBroker& broker) {
    broker.onClientEvent(&BrokerEvents::onEvent, _shards[shard].get());
}

void BrokerEvents::onEvent(void* context, const MqttBrokerClientEvent& event) {
    Shard& shard = *static_cast<Shard*>(context);
    // A connection refused before it had a session has no client to speak of.
    if (event.clientId.length == 0) return;
    Pending p;
    p.topic = topicFor(event.kind);
    p.clientId = text(event.clientId);
    p.userName = text(event.userName);
    p.body = document(event, nowMillis());
    shard.pending.push_back(std::move(p));
}

bool BrokerEvents::hasPendingWork(uint32_t shard) {
    return !_shards[shard]->pending.empty() || (shard == 0 && _announceStart);
}

void BrokerEvents::service(uint32_t shard, MqttBroker& broker) {
    Shard& s = *_shards[shard];
    // THE SERVER STARTED: sessions live in memory, so every client and subscription from before is gone.
    // Published once, from shard 0, before any client's events - a reader starts over from here.
    if (shard == 0 && _announceStart) {
        _announceStart = false;
        Pending started;
        started.topic = "$modbee/events/started";
        started.body = startedDocument(nowMillis());
        s.pending.insert(s.pending.begin(), std::move(started));
    }
    if (s.pending.empty()) return;
    std::vector<Pending> batch;
    batch.swap(s.pending);
    for (const Pending& p : batch) {
        MqttBrokerRouted out;
        out.topic = spanOf(p.topic);
        out.payload = spanOf(p.body);
        out.qos = MQ_QOS_1;
        out.retain = false;
        out.clientId = spanOf(p.clientId);   // the subject: `key: clientid` keeps a client's events in order
        out.userName = spanOf(p.userName);   // and its login, as the mqtt-username header
        broker.publishMessage(out);
        _published++;
    }
}

void BrokerEvents::stop(uint32_t shard, MqttBroker& broker) { service(shard, broker); }

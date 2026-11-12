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

// "Not authorized: ..." - the reason code's name in the specification's words, and what it means here.
std::string named(uint8_t code, uint8_t packet, const char* meaning) {
    std::string out = mq_reasonName(code, packet);
    if (meaning != nullptr && *meaning) out += std::string(": ") + meaning;
    return out;
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
    case MqttBrokerClientEventKind::HandshakeFailed: return "$modbee/events/handshake-failed";
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
    // Where it came from: a client connected or turned away, and one whose TLS handshake failed - which has no
    // client id to go by, only its address and what the TLS engine said.
    if (e.address.length != 0) out += ",\"address\":" + quote(text(e.address));
    if (e.kind == MqttBrokerClientEventKind::HandshakeFailed) out += ",\"detail\":" + quote(text(e.detail));
    return out + ",\"time\":" + std::to_string(timeMillis) + "}";
}

std::string BrokerEvents::startedDocument(uint64_t timeMillis) {
    return "{\"event\":\"started\",\"time\":" + std::to_string(timeMillis) + "}";
}

DiagnosticsEntry BrokerEvents::entryFor(const MqttBrokerClientEvent& e) {
    DiagnosticsEntry out;
    out.kind = topicFor(e.kind) + 15;     // after "$modbee/events/"
    out.clientId = text(e.clientId);
    out.login = text(e.userName);
    out.address = text(e.address);
    const std::string detail = text(e.detail);
    switch (e.kind) {
    case MqttBrokerClientEventKind::Connected:
        out.detail = e.resumed ? "its session resumed" : "a new session";
        break;
    case MqttBrokerClientEventKind::Disconnected:
        out.reasonCode = e.reasonCode;
        if (e.reasonCode != 0) {
            const char* meaning = e.reasonCode == MQ_REASON_SESSION_TAKEN_OVER
                                      ? "another connection logged in with the same client id"
                                  : e.reasonCode == MQ_REASON_KEEP_ALIVE_TIMEOUT
                                      ? "nothing heard from it for one and a half keep-alives"
                                  : e.reasonCode == MQ_REASON_NOT_AUTHORIZED
                                      ? "its login was changed, disabled or removed, or its key turned off"
                                  : e.reasonCode == MQ_REASON_SERVER_SHUTTING_DOWN ? "the server stopped"
                                                                                  : "";
            out.reason = "the server closed it - " + named(e.reasonCode, MQ_PKT_DISCONNECT, meaning);
        } else {
            out.reason = e.byClient ? "the client disconnected (DISCONNECT)"
                                    : "the connection closed without a DISCONNECT";
        }
        break;
    case MqttBrokerClientEventKind::Subscribed:
        out.detail = text(e.filter) + " (QoS " + std::to_string(e.qos) + ")";
        break;
    case MqttBrokerClientEventKind::Unsubscribed:
        out.detail = text(e.filter);
        break;
    case MqttBrokerClientEventKind::PublishRefused:
        out.reasonCode = e.reasonCode;
        out.reason = named(e.reasonCode, MQ_PKT_PUBACK,
                           e.reasonCode == MQ_REASON_NOT_AUTHORIZED ? "its login may not publish to this topic" : "");
        out.detail = text(e.filter);
        break;
    case MqttBrokerClientEventKind::SubscribeRefused:
        out.reasonCode = e.reasonCode;
        out.reason = named(e.reasonCode, MQ_PKT_SUBACK,
                           e.reasonCode == MQ_REASON_NOT_AUTHORIZED ? "its login may not subscribe to this filter" : "");
        out.detail = text(e.filter);
        break;
    case MqttBrokerClientEventKind::ConnectRefused:
        out.reasonCode = e.reasonCode;
        if (e.reasonCode == MQ_REASON_CLIENT_IDENTIFIER_NOT_VALID) {
            out.reason = named(e.reasonCode, MQ_PKT_CONNACK,
                               "the client id is held - a session of another login has it");
            out.detail = "held by login \"" + detail + "\"";
        } else if (e.reasonCode == MQ_REASON_NOT_AUTHORIZED) {
            out.reason = named(e.reasonCode, MQ_PKT_CONNACK, "its Will is on a topic its login may not publish to");
            out.detail = "Will topic " + detail;
        } else {
            out.reason = named(e.reasonCode, MQ_PKT_CONNACK,
                               e.reasonCode == MQ_REASON_SERVER_BUSY
                                   ? "the server is at its connections or sessions (max-connections, max-sessions)"
                               : e.reasonCode == MQ_REASON_QUOTA_EXCEEDED
                                   ? "the login holds all the sessions it may (max-sessions-per-login)"
                                   : "");
            out.detail = detail;
        }
        break;
    case MqttBrokerClientEventKind::HandshakeFailed:
        out.reason = detail.empty() ? std::string("the TLS handshake failed") : detail;
        break;
    }
    return out;
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
    if (DiagnosticsLog* log = shard.owner->_diagnostics.load(std::memory_order_acquire)) log->record(entryFor(event));
    if (!shard.owner->enabled()) return;
    // A connection refused before it had a session has no client to speak of - except one whose TLS handshake
    // failed, which never had a client id to give: its address is what there is.
    if (event.clientId.length == 0 && event.kind != MqttBrokerClientEventKind::HandshakeFailed) return;
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
        if (!enabled()) return;
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

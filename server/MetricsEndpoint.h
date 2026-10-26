// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// GET /metrics, in the Prometheus text format, on a thread of its own.
//
// Deliberately tiny: one request at a time, no keep-alive, no TLS, nothing
// but /metrics (and / and /healthz, which answer "ok"). It never touches a
// broker - it calls `render`, which reads the snapshots ShardedBroker and
// KafkaPipe already keep for exactly this, so a scrape costs the shards
// nothing. Put it behind the same network policy as any metrics port.
//
// With setRest(), /v1/... is the Application API's REST surface too, handed
// to `rest` whole (ApiServer::rest).

#include "ShardedBroker.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

class MetricsEndpoint {
public:
    typedef void (*RenderFn)(void* context, std::string& out);
    // Readiness: true when the server should be given clients; false with why.
    typedef bool (*ReadyFn)(void* context, std::string& why);
    // One /v1/ request; returns the HTTP status, `response` the JSON body.
    typedef int (*RestFn)(void* context, const std::string& method, const std::string& target,
                          const std::string& authorization, const std::string& body, std::string& response);

    MetricsEndpoint() = default;
    ~MetricsEndpoint() { stop(); }
    MetricsEndpoint(const MetricsEndpoint&) = delete;
    MetricsEndpoint& operator=(const MetricsEndpoint&) = delete;

    // Binds now (so a busy port is an error here, not later) and serves on a
    // thread until stop(). Port 0 picks a free one; port() says which.
    bool start(const std::string& address, uint16_t port, RenderFn render, void* context,
               std::string& error);
    void stop();
    // /readyz answers 200 "ready" or 503 with the reason; without this, "ready".
    void setReadiness(ReadyFn ready, void* context) { _ready = ready; _readyContext = context; }
    // Before start().
    void setRest(RestFn rest, void* context) { _rest = rest; _restContext = context; }
    uint16_t port() const { return _port; }

private:
    int _listen = -1;
    int _wake = -1;
    uint16_t _port = 0;
    RenderFn _render = nullptr;
    ReadyFn _ready = nullptr;
    void* _readyContext = nullptr;
    RestFn _rest = nullptr;
    void* _restContext = nullptr;
    void* _context = nullptr;
    std::atomic<bool> _stopping{false};
    std::thread _thread;
    void serve();
};

// The broker's counters, appended in the Prometheus text format.
void renderBrokerMetrics(const ShardedBrokerStatus& status, std::string& out);

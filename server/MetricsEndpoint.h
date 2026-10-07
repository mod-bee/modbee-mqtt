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

#include "ShardedBroker.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

class MetricsEndpoint {
public:
    typedef void (*RenderFn)(void* context, std::string& out);

    MetricsEndpoint() = default;
    ~MetricsEndpoint() { stop(); }
    MetricsEndpoint(const MetricsEndpoint&) = delete;
    MetricsEndpoint& operator=(const MetricsEndpoint&) = delete;

    // Binds now (so a busy port is an error here, not later) and serves on a
    // thread until stop(). Port 0 picks a free one; port() says which.
    bool start(const std::string& address, uint16_t port, RenderFn render, void* context,
               std::string& error);
    void stop();
    uint16_t port() const { return _port; }

private:
    int _listen = -1;
    int _wake = -1;
    uint16_t _port = 0;
    RenderFn _render = nullptr;
    void* _context = nullptr;
    std::atomic<bool> _stopping{false};
    std::thread _thread;
    void serve();
};

// The broker's counters, appended in the Prometheus text format.
void renderBrokerMetrics(const ShardedBrokerStatus& status, std::string& out);

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// THE APPLICATION API, SERVED (api: in modbee.yaml)
// =============================================================================
//
// gRPC on a local socket, and optionally on TCP with TLS and a token, for the
// services in proto/modbee/app/v1/app.proto. The only part of the server that
// includes gRPC: what it serves is plain C++ (Authority.h), and this header
// names no gRPC type, so nothing that includes it is built gRPC's way.
//
// gRPC runs its own threads. A call reaches a shard only through the shard's
// own queue (Authority::Shard), so a shard never waits on the API, and the API
// never touches a broker.

#include <memory>
#include <functional>
#include <string>
#include <vector>

class Authority;
class AppHub;
class AppConnections;
class AppAdmin;
class AuthGate;
class ShardedBroker;
class KafkaControl;

struct ApiServerConfig {
    std::vector<std::string> addresses;   // "unix:/run/modbee/api.sock", "tcp://0.0.0.0:7660"
    std::string tlsCertPem, tlsKeyPem;    // for tcp://
    std::string tlsClientCaPem;           // set: callers must present a certificate it signed
    std::string token;                    // set: every call carries "authorization: Bearer <token>"
    bool plainTcp = false;                // tcp:// without TLS anywhere (api.plain), not only on loopback
    std::string serverVersion;            // "modbee-mqtt 0.1.1"
    std::string serverId;                 // this server, stable across restarts
};

class ApiServer {
public:
    ApiServer();
    ~ApiServer();

    // What it serves. None is owned; each outlives stop(). One left null has
    // its service say so: Authority without an authority (logins are the
    // login database's), Admin's logins without a database, and so on.
    struct Services {
        Authority* authority = nullptr;
        AppHub* messages = nullptr;
        AppConnections* connections = nullptr;
        AppAdmin* admin = nullptr;           // sessions and retained messages, on the shards
        AuthGate* logins = nullptr;          // the login database, where logins are kept here
        ShardedBroker* server = nullptr;     // its listeners, TLS and status
        KafkaControl* kafka = nullptr;       // the kafka: section, changed live
        // Client events, on or off while it runs (server/events/BrokerEvents.h).
        std::function<bool()> clientEvents;
        std::function<void(bool)> setClientEvents;
        // Sparkplug Aware, read and changed while it runs (server/events/SparkplugAware.h).
        std::function<bool()> sparkplugAware;
        std::function<void(bool)> setSparkplugAware;
    };
    bool start(const ApiServerConfig& config, const Services& services, std::string& error);

    // THE REST SURFACE (internal/APP-API-SPEC.md section 5): one HTTP request,
    // answered with the same code as its gRPC call. `target` is the path with
    // its query; `authorization` the header. Returns the HTTP status; the body
    // is JSON. Callable from any thread; it waits for the answer.
    int rest(const std::string& method, const std::string& target, const std::string& authorization,
             const std::string& body, std::string& response);

    // Every stream is ended and every call answered or cancelled, within a
    // couple of seconds. Safe to call twice.
    void stop();

    // Where it listens, a TCP port resolved: "tcp://0.0.0.0:7660".
    const std::vector<std::string>& listening() const;

    // The gRPC this was built with, for `modbee-mqtt --version`.
    static std::string libraryVersion();

    struct Impl;

private:
    std::unique_ptr<Impl> _impl;
};

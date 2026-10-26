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
#include <string>
#include <vector>

class Authority;

struct ApiServerConfig {
    std::vector<std::string> addresses;   // "unix:/run/modbee/api.sock", "tcp://0.0.0.0:7660"
    std::string tlsCertPem, tlsKeyPem;    // for tcp://
    std::string tlsClientCaPem;           // set: callers must present a certificate it signed
    std::string token;                    // set: every call carries "authorization: Bearer <token>"
    std::string serverVersion;            // "modbee-mqtt 0.1.0"
    std::string serverId;                 // this server, stable across restarts
};

class ApiServer {
public:
    ApiServer();
    ~ApiServer();

    // `authority` may be null: logins are then the login database's, and the
    // Authority service says so (FAILED_PRECONDITION). Not owned; outlives stop().
    bool start(const ApiServerConfig& config, Authority* authority, std::string& error);

    // Every stream is ended and every call answered or cancelled, within a
    // couple of seconds. Safe to call twice.
    void stop();

    // Where it listens, a TCP port resolved: "tcp://0.0.0.0:7660".
    const std::vector<std::string>& listening() const;

    struct Impl;

private:
    std::unique_ptr<Impl> _impl;
};

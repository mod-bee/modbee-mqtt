// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// THE COMMAND LINE AND THE APPLICATION API, ONE TO ONE
// =============================================================================
//
// Every call in proto/modbee/app/v1/app.proto is a `modbee-mqtt <noun> <verb>`
// command, or is listed below as not one, with the reason. A command's flags
// are its request's fields, read from the contract itself (cmd_api.cpp), so a
// command cannot have a field the call lacks or lack one it has, and its
// answer is the call's, as JSON. test/cli_api_tests.cpp fails when a call has
// no row here, a row names no call, or docs/cli.md's table says otherwise.
//
// Plain data, so the test reads it without gRPC.

#include <cstddef>

struct ApiCommand {
    const char* noun;
    const char* verb;       // "" where the noun is the whole command
    const char* call;       // "Admin.ListSessions": the service and the call
    const char* rest;       // its REST route, "" for none
};

static const ApiCommand kApiCommands[] = {
    {"hello", "", "System.Hello", ""},

    {"authority", "revoke", "Authority.Revoke", ""},
    {"authority", "regrant", "Authority.Regrant", ""},
    {"authority", "forget", "Authority.Forget", ""},

    {"sparkplug", "decode", "Sparkplug.Decode", ""},
    {"sparkplug", "encode", "Sparkplug.Encode", ""},
    {"sparkplug-hosts", "put", "Sparkplug.PutHost", ""},
    {"sparkplug-hosts", "get", "Sparkplug.GetHost", ""},
    {"sparkplug-hosts", "list", "Sparkplug.ListHosts", ""},
    {"sparkplug-hosts", "delete", "Sparkplug.DeleteHost", ""},
    {"sparkplug", "nodes", "Sparkplug.ListNodes", ""},
    {"sparkplug", "rebirth", "Sparkplug.Rebirth", ""},
    {"sparkplug", "write", "Sparkplug.Write", ""},
    {"sparkplug-nodes", "put", "Sparkplug.PutEdgeNode", ""},
    {"sparkplug-nodes", "get", "Sparkplug.GetEdgeNode", ""},
    {"sparkplug-nodes", "list", "Sparkplug.ListEdgeNodes", ""},
    {"sparkplug-nodes", "delete", "Sparkplug.DeleteEdgeNode", ""},
    {"sparkplug-nodes", "set", "Sparkplug.SetValues", ""},
    {"sparkplug-nodes", "device", "Sparkplug.SetDeviceState", ""},
    {"sparkplug-nodes", "rebirth", "Sparkplug.RebirthEdgeNode", ""},
    {"sparkplug-nodes", "watch", "Sparkplug.WatchEdgeNodes", ""},
    {"sparkplug-devices", "put", "Sparkplug.PutDevice", ""},
    {"sparkplug-devices", "delete", "Sparkplug.DeleteDevice", ""},
    {"sparkplug-aware", "get", "Sparkplug.GetAware", ""},
    {"sparkplug-aware", "set", "Sparkplug.PutAware", ""},

    {"messages", "consume", "Messages.Consume", ""},
    {"messages", "publish", "Messages.Publish", "POST /v1/publish"},
    {"groups", "list", "Messages.ListGroups", "GET /v1/groups"},
    {"groups", "delete", "Messages.DeleteGroup", "DELETE /v1/groups/{group}?connection="},

    {"logins", "backup", "Admin.Backup", "POST /v1/backups"},
    {"events", "get", "Admin.GetEvents", "GET /v1/events"},
    {"events", "set", "Admin.PutEvents", "PUT /v1/events"},

    {"kafka-clusters", "put", "Kafka.PutCluster", "PUT /v1/kafka-clusters/{id}"},
    {"kafka-clusters", "get", "Kafka.GetCluster", "GET /v1/kafka-clusters/{id}"},
    {"kafka-clusters", "list", "Kafka.ListClusters", "GET /v1/kafka-clusters"},
    {"kafka-clusters", "delete", "Kafka.DeleteCluster", "DELETE /v1/kafka-clusters/{id}"},
    {"kafka-mappings", "put", "Kafka.PutMapping", "PUT /v1/kafka-mappings/{id}"},
    {"kafka-mappings", "get", "Kafka.GetMapping", "GET /v1/kafka-mappings/{id}"},
    {"kafka-mappings", "list", "Kafka.ListMappings", "GET /v1/kafka-mappings"},
    {"kafka-mappings", "delete", "Kafka.DeleteMapping", "DELETE /v1/kafka-mappings/{id}"},
    {"kafka", "status", "Kafka.Status", "GET /v1/kafka-status"},

    {"connections", "put", "Connections.Put", "PUT /v1/connections/{id}"},
    {"connections", "get", "Connections.Get", "GET /v1/connections/{id}"},
    {"connections", "list", "Connections.List", "GET /v1/connections"},
    {"connections", "delete", "Connections.Delete", "DELETE /v1/connections/{id}"},
    {"connections", "watch", "Connections.Watch", ""},
    {"connections", "reconnect", "Connections.Reconnect", "POST /v1/connections/{id}:reconnect"},

    {"logins", "list", "Admin.ListLogins", "GET /v1/logins"},
    {"logins", "get", "Admin.GetLogin", "GET /v1/logins/{name}"},
    {"logins", "put", "Admin.PutLogin", "PUT /v1/logins/{name}"},
    {"logins", "delete", "Admin.DeleteLogin", "DELETE /v1/logins/{name}"},
    {"logins", "reset", "Admin.ResetLogin", "POST /v1/logins/{name}:reset"},
    {"keys", "list", "Admin.ListKeys", "GET /v1/keys"},
    {"keys", "put", "Admin.PutKey", "PUT /v1/keys/{name}"},
    {"keys", "delete", "Admin.DeleteKey", "DELETE /v1/keys/{name}"},
    {"sessions", "list", "Admin.ListSessions", "GET /v1/sessions"},
    {"sessions", "kick", "Admin.Kick", "POST /v1/sessions/{clientId}:kick"},
    {"retained", "list", "Admin.ListRetained", "GET /v1/retained"},
    {"retained", "delete", "Admin.DeleteRetained", "DELETE /v1/retained?filter="},
    {"listeners", "get", "Admin.GetListeners", "GET /v1/listeners"},
    {"listeners", "set", "Admin.PutListeners", "PUT /v1/listeners"},
    {"tls", "set", "Admin.PutTls", "PUT /v1/listeners/tls"},
    {"stats", "", "Admin.Stats", "GET /v1/stats"},
    {"settings", "get", "Admin.GetSettings", "GET /v1/settings"},
    {"settings", "check", "Admin.CheckSettings", "POST /v1/settings:check"},
    {"settings", "set", "Admin.SetSettings", "PATCH /v1/settings"},
    {"settings", "apply", "Admin.ApplySettings", "PUT /v1/settings"},
    {"settings", "reset", "Admin.ResetSettings", "POST /v1/settings:reset"},
};

// Calls that are an application's own long-lived streams, not something a
// person runs and reads the answer of.
struct ApiNotACommand {
    const char* call;
    const char* why;
};

static const ApiNotACommand kApiNotCommands[] = {
    {"System.Attach", "an application instance's presence, held open for as long as it runs"},
    {"Authority.Serve", "the application answering CONNECTs, for as long as it runs"},
    {"Messages.PublishStream", "an outbox draining; `messages publish` sends one at a time"},
};

static const size_t kApiCommandCount = sizeof kApiCommands / sizeof kApiCommands[0];
static const size_t kApiNotCommandCount = sizeof kApiNotCommands / sizeof kApiNotCommands[0];

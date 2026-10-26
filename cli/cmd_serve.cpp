// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee serve - the multi-core broker as a daemon.
//
// One MqttBroker per core behind one port (server/ShardedBroker.h). Options
// are flags for now; a YAML file arrives with the Kafka pipe.

#include "Args.h"
#include "Common.h"

#include <csignal>
#include <cstdio>
#include <cstring>

#if defined(MODBEE_HAVE_SERVER)

#include "ShardedBroker.h"
#include "ServerConfig.h"
#include "MetricsEndpoint.h"
#include "RetainedKeeper.h"
#include "ShardAccounts.h"
#include "AuthGate.h"
#include "TlsSettings.h"
#include "BrokerEvents.h"
#include "SessionKeeper.h"
#if defined(MODBEE_HAVE_APP_API)
#include "Authority.h"
#include "ApiServer.h"
#include "AppHub.h"
#include "AppConnections.h"
#include "AppAdmin.h"
#endif
#if defined(MODBEE_HAVE_KAFKA)
#include "KafkaPipe.h"
#include "KafkaRetained.h"
#endif

#include <memory>

#include <fstream>
#include <sstream>
#include <thread>
#include <unistd.h>

namespace {

const std::vector<ArgSpec>& serveSpecs() {
    static const std::vector<ArgSpec> specs = {
        {"config", 'C', true, "modbee.yaml to run (flags given as well override it)"},
        {"address", 'a', true, "address to listen on (default 0.0.0.0)"},
        {"port", 'p', true, "port (default 1883)"},
        {"no-plain", 0, false, "no plain-TCP listener (TLS only)"},
        {"tls-port", 0, true, "also listen for TLS on this port (8883 by convention)"},
        {"cert", 0, true, "the broker's certificate, PEM file (with --tls-port)"},
        {"key", 0, true, "the broker's private key, PEM file (with --tls-port)"},
        {"client-ca", 0, true, "require client certificates signed by this CA, PEM file (mutual TLS)"},
        {"client-certificates", 0, true, "with --client-ca: required (default) or optional"},
        {"shards", 's', true, "brokers, one per core (default: one per core)"},
        {"max-connections", 'c', true, "connections, whole server (default 100000)"},
        {"max-sessions", 0, true, "sessions, whole server (default = connections)"},
        {"max-packet", 0, true, "largest packet accepted, bytes (default 65536)"},
        {"queue", 0, true, "messages held per session (default 64)"},
        {"subscriptions", 0, true, "subscriptions per session (default 32)"},
        {"max-retained", 0, true, "retained topics kept, whole server (default 10000)"},
        {"username", 0, true, "require this MQTT user name of every client (with --password)"},
        {"password", 0, true, "the password for --username"},
        {"retained-store", 0, true, "where retained messages are kept as well: memory (default), file, kafka"},
        {"retained-file", 0, true, "the file for --retained-store file (default /var/lib/modbee/retained.db)"},
        {"sessions-store", 0, true, "where persistent sessions are kept as well: memory (default), file"},
        {"sessions-directory", 0, true, "the directory for --sessions-store file (default /var/lib/modbee/sessions)"},
        {"reserve-up-front", 0, false, "reserve every slot at start instead of on use"},
        {"pin", 0, false, "pin each shard's thread to a core"},
        {"status-every", 0, true, "seconds between status lines (default 5, 0 off)"},
        {"metrics-port", 0, true, "serve Prometheus /metrics on this port (9464 by convention)"},
        {"help", 0, false, "show this help"},
    };
    return specs;
}

// What a /metrics scrape renders: the server's counters, and the pipe's.
struct MetricsSources {
    ShardedBroker* server = nullptr;
#if defined(MODBEE_HAVE_KAFKA)
    KafkaPipe* pipe = nullptr;
#endif
};

bool serverReady(void* context, std::string& why) {
    MetricsSources& sources = *static_cast<MetricsSources*>(context);
    if (sources.server->shardCount() == 0) { why = "the broker is not running"; return false; }
#if defined(MODBEE_HAVE_KAFKA)
    if (sources.pipe && !sources.pipe->status().kafkaReachable) { why = "Kafka cannot be reached"; return false; }
#endif
    return true;
}

#if defined(MODBEE_HAVE_APP_API)
// /v1/... on the metrics listener: the Application API's REST surface.
int restCall(void* context, const std::string& method, const std::string& target,
             const std::string& authorization, const std::string& body, std::string& response) {
    return static_cast<ApiServer*>(context)->rest(method, target, authorization, body, response);
}
#endif

void renderMetrics(void* context, std::string& out) {
    MetricsSources& sources = *static_cast<MetricsSources*>(context);
    renderBrokerMetrics(sources.server->status(), out);
#if defined(MODBEE_HAVE_KAFKA)
    if (sources.pipe) renderKafkaMetrics(sources.pipe->status(), out);
#endif
}

bool readWholeFile(const std::string& path, std::string& into) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return false;
    std::ostringstream text;
    text << file.rdbuf();
    into = text.str();
    return true;
}

} // namespace

int cmdServe(int argc, char** argv) {
    const std::vector<ArgSpec>& specs = serveSpecs();
    Args args;
    if (!args.parse(argc, argv, 2, specs)) {
        std::fprintf(stderr, "modbee serve: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp("modbee serve [-C modbee.yaml] [options]", specs);
        return 0;
    }

    // THE FILE FIRST, then any flag given on the command line on top of it -
    // so `modbee serve -c modbee.yaml -p 1884` is the file with another port.
    ServerConfig file;
    if (args.has("config")) {
        std::vector<ConfigProblem> problems;
        const std::string path = args.get("config");
        const bool ok = loadServerConfigFile(path, file, problems);
        for (const ConfigProblem& problem : problems) {
            std::fprintf(stderr, "%s\n", describe(problem, path).c_str());
        }
        if (!ok) {
            std::fprintf(stderr, "modbee serve: %s would not load (modbee check -c %s)\n",
                         path.c_str(), path.c_str());
            return 2;
        }
        // SETTINGS STORED IN THE DATABASE, over the file and under the
        // environment (and the flags below). They were validated when they were
        // stored; if they no longer load - a newer version, a changed file - the
        // server starts from the file alone and says so, rather than not at all.
        if (!file.authentication.database.empty()) {
            AuthStore stored;
            std::string why;
            if (stored.open(file.authentication.database, 0, why)) {
                StoredSettings settings;
                settings.hasDocument = stored.getSetting("config.document", settings.document);
                std::vector<std::pair<std::string, std::string>> rows;
                stored.listSettings("config.override.", rows);
                for (const auto& r : rows) settings.overrides.emplace_back(r.first.substr(16), r.second);
                stored.close();
                if (settings.hasDocument || !settings.overrides.empty()) {
                    std::string text;
                    readWholeFile(path, text);
                    ServerConfig effective;
                    std::vector<ConfigProblem> problems;
                    if (loadEffectiveConfig(text, path, settings, effective, problems)) {
                        file = effective;
                        std::printf("modbee serve: settings from the database (%s, %zu override(s))\n",
                                    settings.hasDocument ? "a stored document" : "the file", settings.overrides.size());
                    } else {
                        for (const ConfigProblem& problem : problems) {
                            std::fprintf(stderr, "%s\n", describe(problem, "stored settings").c_str());
                        }
                        std::fprintf(stderr, "modbee serve: the stored settings would not load - starting from %s alone\n",
                                     path.c_str());
                    }
                }
            }
        }
    }
    BrokerSection& b = file.broker;
    long long n = 0;
    const auto flag = [&](const char* name, long long fallback, long long lo, long long hi,
                          long long& out) {
        if (!args.has(name)) return true;
        if (!args.getInt(name, fallback, lo, hi, n)) return false;
        out = n;
        return true;
    };
    long long port = b.port, tlsPort = b.tlsPort, shards = (long long)b.shards,
              packet = b.maxPacket, queue = (long long)b.queue,
              subscriptions = (long long)b.subscriptions,
              retained = (long long)b.maxRetained;
    if (!flag("port", 1883, 0, 65535, port) || !flag("tls-port", 8883, 0, 65535, tlsPort) ||
        !flag("shards", 0, 0, 1024, shards) ||
        !flag("max-connections", 100000, 1, 100000000, b.maxConnections) ||
        !flag("max-sessions", 0, 0, 100000000, b.maxSessions) ||
        !flag("max-packet", 65536, 64, 268435455, packet) ||
        !flag("queue", 64, 1, 1000000, queue) ||
        !flag("subscriptions", 32, 1, 100000, subscriptions) ||
        !flag("max-retained", 10000, 0, 10000000, retained) ||
        !flag("status-every", 5, 0, 86400, b.statusEvery)) {
        std::fprintf(stderr, "modbee serve: %s\n", args.error().c_str());
        return 2;
    }
    b.port = (uint16_t)port;
    b.tlsPort = (uint16_t)tlsPort;
    b.shards = (size_t)shards;
    b.maxPacket = (uint32_t)packet;
    b.queue = (size_t)queue;
    b.subscriptions = (size_t)subscriptions;
    b.maxRetained = (size_t)retained;
    if (args.has("metrics-port")) {
        long long metricsPort = 0;
        if (!args.getInt("metrics-port", 9464, 0, 65535, metricsPort)) {
            std::fprintf(stderr, "modbee serve: %s\n", args.error().c_str());
            return 2;
        }
        file.metrics.enabled = true;
        file.metrics.port = (uint16_t)metricsPort;
    }
    if (args.has("username") != args.has("password")) {
        std::fprintf(stderr, "modbee serve: --username and --password go together\n");
        return 2;
    }
    if (args.has("username")) {
        file.authentication.accounts.push_back(MqttAccount{args.get("username"), args.get("password")});
    }
    if (args.has("retained-store")) {
        const std::string store = args.get("retained-store");
        if (store == "memory") file.retained.store = RetainedStore::Memory;
        else if (store == "file") file.retained.store = RetainedStore::File;
        else if (store == "kafka") file.retained.store = RetainedStore::Kafka;
        else {
            std::fprintf(stderr, "modbee serve: --retained-store is memory, file or kafka, not \"%s\"\n",
                         store.c_str());
            return 2;
        }
    }
    if (args.has("retained-file")) {
        file.retained.file = args.get("retained-file");
        if (!args.has("retained-store")) file.retained.store = RetainedStore::File;
    }
    if (args.has("sessions-store")) {
        const std::string store = args.get("sessions-store");
        if (store == "memory") file.sessions.store = SessionStore::Memory;
        else if (store == "file") file.sessions.store = SessionStore::File;
        else {
            std::fprintf(stderr, "modbee serve: --sessions-store is memory or file, not \"%s\"\n",
                         store.c_str());
            return 2;
        }
    }
    if (args.has("sessions-directory")) {
        file.sessions.directory = args.get("sessions-directory");
        if (!args.has("sessions-store")) file.sessions.store = SessionStore::File;
    }
    if (args.has("address")) b.address = args.get("address");
    if (args.has("no-plain")) b.plain = false;
    if (args.has("pin")) b.pinThreads = true;
    if (args.has("tls-port")) b.tls = true;
    if (args.has("cert")) b.certificateFile = args.get("cert");
    if (args.has("key")) b.keyFile = args.get("key");
    if (args.has("client-ca")) b.clientCaFile = args.get("client-ca");
    if (args.has("client-certificates")) {
        b.clientCertificates = args.get("client-certificates");
        if (b.clientCertificates != "required" && b.clientCertificates != "optional") {
            std::fprintf(stderr, "modbee serve: --client-certificates is required or optional, not \"%s\"\n",
                         b.clientCertificates.c_str());
            return 2;
        }
    }
    const bool pemPair = !b.certificatePem.empty() && !b.keyPem.empty();
    if (b.tls && !pemPair && (b.certificateFile.empty() || b.keyFile.empty())) {
        std::fprintf(stderr, "modbee serve: TLS needs a certificate and a key (--cert, --key)\n");
        return 2;
    }
    if (!b.tls && (args.has("cert") || args.has("key") || args.has("client-ca"))) {
        std::fprintf(stderr, "modbee serve: --cert, --key and --client-ca need --tls-port\n");
        return 2;
    }

    ShardedBrokerConfig config;
    config.address = b.address;
    config.port = b.port;
    config.plain = b.plain;
    // The certificate, its key and the client CA as PEM text, whether the
    // settings name files or hold the text itself - the form the listeners
    // serve and a live change replaces (server/auth/TlsSettings.h).
    {
        std::string why;
        if (!tlsFromSettings(b, config.tls, why)) {
            std::fprintf(stderr, "modbee serve: %s\n", why.c_str());
            return 2;
        }
    }
    config.shards = b.shards;
    config.pinThreads = b.pinThreads;
    const size_t count = b.shards > 0 ? b.shards
                                      : (size_t)std::max(1u, std::thread::hardware_concurrency());
    // Per shard, with a quarter more than an even share: the hash spreads
    // sessions evenly on average, not exactly.
    const size_t perShard = (size_t)(b.maxConnections + (long long)count - 1) / count;
    // One shard has nothing to spread: max-connections means what it says.
    config.broker.maxConnections = count == 1 ? perShard : perShard + perShard / 4 + 16;
    const long long totalSessions = b.maxSessions > 0 ? b.maxSessions : b.maxConnections;
    const size_t sessionsPerShard = (size_t)(totalSessions + (long long)count - 1) / count;
    config.broker.maxSessions = count == 1 ? sessionsPerShard : sessionsPerShard + sessionsPerShard / 4 + 16;
    config.broker.receiveBufferSize = b.maxPacket;
    config.broker.transmitBufferSize = b.maxPacket;
    config.broker.maximumPacketSize = b.maxPacket;
    config.broker.maxQueuedPerSession = b.queue;
    config.broker.maxQueuedBytesPerSession = (size_t)b.queueBytes;
    config.broker.maxQueuedBytesTotal = (size_t)((b.queueBytesTotal + (long long)count - 1) / (long long)count);
    config.broker.maxSubscriptionsPerSession = b.subscriptions;
    config.broker.maximumSessionExpiry = (uint32_t)b.maxSessionExpiry;
    config.broker.connectTimeoutMs = (mq_time_t)(b.connectTimeout * 1000);
    config.broker.serverKeepAliveMaximum = (uint16_t)b.maxKeepAlive;
    config.broker.maxRetained = b.maxRetained;
    config.broker.reserveUpFront = args.has("reserve-up-front");

    // THE KAFKA PIPE, when the file asks for it, and THE RETAINED STORE, when
    // retained messages are to outlive a restart. Both see every message.
    ShardExtensionChain extensions;
    const bool wantsKafka = !file.toKafka.empty() || !file.fromKafka.empty();
#if defined(MODBEE_HAVE_KAFKA)
    std::unique_ptr<KafkaPipe> pipe;
    if (wantsKafka) {
        pipe.reset(new KafkaPipe(file));
        extensions.add(pipe.get());
    }
#else
    if (wantsKafka || file.retained.store == RetainedStore::Kafka) {
        std::fprintf(stderr, "modbee serve: the file uses Kafka, but this build has no "
                             "Kafka support (librdkafka was not found when it was built)\n");
        return 2;
    }
#endif
    std::unique_ptr<RetainedKeeper> keeper;
    if (file.retained.store == RetainedStore::File) {
        keeper.reset(new RetainedKeeper(makeRetainedFile(file.retained.file)));
    }
#if defined(MODBEE_HAVE_KAFKA)
    if (file.retained.store == RetainedStore::Kafka) {
        const KafkaClusterConfig* cluster = file.cluster(file.retained.cluster);
        if (cluster == nullptr && file.clusters.size() == 1) cluster = &file.clusters[0];
        if (cluster == nullptr) {
            std::fprintf(stderr, "modbee serve: retained: store: kafka needs a Kafka cluster "
                                 "(kafka.clusters)\n");
            return 2;
        }
        keeper.reset(new RetainedKeeper(makeRetainedKafka(*cluster, file.retained.topic)));
    }
#endif
    if (keeper) extensions.add(keeper.get());
    // EVENTS BEFORE THE PIPE SEES THEM? Order does not matter: an event is a
    // message published from the shard's service pass, offered to every
    // extension - the Kafka pipe takes it when a mapping's filter matches.
    std::unique_ptr<BrokerEvents> events;
    if (file.events.enabled) {
        events.reset(new BrokerEvents());
        extensions.add(events.get());
    }
    // Logins: a live database of device logins (AuthGate), or a fixed list
    // from the file (ShardAccounts), or none.
    ShardAccounts accounts;
    std::unique_ptr<AuthGate> gate;
    // Declared before it is started, so the login gate can be told where it
    // is: config.* reports what its TLS listeners serve and changes their
    // certificate while it runs.
    ShardedBroker server;
#if defined(MODBEE_HAVE_APP_API)
    // LOGINS DECIDED BY THE APPLICATION, over the Application API: the fixed
    // logins from the file are still decided here - the application and the
    // modbee command log in with them - and every other is asked of it.
    std::unique_ptr<Authority> authority;
#else
    if (file.authentication.authority.enabled || !file.api.grpc.empty()) {
        std::fprintf(stderr, "modbee serve: the file uses the Application API (api:, authentication: "
                             "authority:), but this build has none (gRPC was not found when it was built)\n");
        return 2;
    }
#endif
    if (file.authentication.authority.enabled) {
#if defined(MODBEE_HAVE_APP_API)
        AuthorityConfig a;
        for (size_t i = 0; i < file.authentication.accounts.size(); i++) {
            const MqttAccount& account = file.authentication.accounts[i];
            (i == 0 && file.authentication.hasAdmin ? a.admins : a.logins)
                .push_back(std::make_pair(account.username, account.password));
        }
        const AuthoritySection& s = file.authentication.authority;
        a.timeoutMs = (uint32_t)s.timeoutMs;
        a.staleForSeconds = (uint32_t)s.staleForSeconds;
        a.cacheSize = (size_t)s.cacheSize;
        a.maxPending = (size_t)s.maxPending;
        config.broker.maxSessionsPerLogin = (uint32_t)file.authentication.maxSessionsPerLogin;
        config.broker.maxRetainedPerLogin = (uint32_t)file.authentication.maxRetainedPerLogin;
        authority.reset(new Authority(a));
        extensions.add(authority.get());
#endif
    } else if (!file.authentication.database.empty()) {
        AuthGateConfig g;
        g.database = file.authentication.database;
        g.configFile = args.has("config") ? args.get("config") : std::string();
        // config.* with "restart": true: the same graceful stop as a SIGTERM.
        // Under Docker the restart policy starts it again; without a supervisor
        // the server stays stopped, which config.set's reply says.
        g.restart = [](void*) { std::raise(SIGTERM); };
        g.certificateLogin = file.authentication.certificateLogin;
        g.noTopicsWithoutRules = file.authentication.defaultTopics == "none";
        // What one login may hold, set on every shard's broker; the fixed
        // admin and the registration login are not limited.
        config.broker.maxSessionsPerLogin = (uint32_t)file.authentication.maxSessionsPerLogin;
        config.broker.maxRetainedPerLogin = (uint32_t)file.authentication.maxRetainedPerLogin;
        config.broker.unlimitedLogins.push_back("$provision");
        if (file.authentication.hasAdmin && !file.authentication.accounts.empty()) {
            config.broker.unlimitedLogins.push_back(file.authentication.accounts[0].username);
        }
        g.server = &server;
        for (size_t i = 0; i < file.authentication.accounts.size(); i++) {
            const MqttAccount& a = file.authentication.accounts[i];
            (i == 0 && file.authentication.hasAdmin ? g.admins : g.logins)
                .push_back(std::make_pair(a.username, a.password));
        }
        gate.reset(new AuthGate(g));
        extensions.add(gate.get());
    } else {
        for (const MqttAccount& account : file.authentication.accounts) {
            accounts.add(account.username, account.password);
        }
        if (accounts.count() > 0) extensions.add(&accounts);
    }
    // SESSIONS, last: the logins are in place before a restored Will is
    // published, and it is told of every change the others cause.
    std::unique_ptr<SessionKeeper> sessions;
    if (file.sessions.store == SessionStore::File) {
        SessionKeeperConfig k;
        k.directory = file.sessions.directory;
        k.syncFull = file.sessions.syncFull;
        sessions.reset(new SessionKeeper(k));
        extensions.add(sessions.get());
    }
#if defined(MODBEE_HAVE_APP_API)
    // THE APPLICATION'S MESSAGES, wherever the Application API is served.
    std::unique_ptr<AppHub> hub;
    std::unique_ptr<AppAdmin> admin;
    if (!file.api.grpc.empty()) {
        AppHubConfig h;
        h.maxQueued = (size_t)file.api.maxQueued;
        h.eventsEnabled = file.events.enabled;
        hub.reset(new AppHub(h));
        extensions.add(hub.get());
        admin.reset(new AppAdmin());
        extensions.add(admin.get());
    }
#endif
    if (!extensions.empty()) config.extension = &extensions;

    cliInstallSignalHandlers();
    if (!server.start(config)) {
        std::fprintf(stderr, "modbee serve: %s\n", server.lastError().c_str());
        return 3;
    }
#if defined(MODBEE_HAVE_APP_API)
    // THE APPLICATION API, once every shard is running: its calls reach them -
    // and the MQTT connections it keeps for the application, opened again from
    // what was kept.
    std::unique_ptr<AppConnections> connections;
    ApiServer api;
    if (!file.api.grpc.empty()) {
        ApiServerConfig c;
        c.addresses = file.api.grpc;
        c.serverVersion = std::string("modbee-mqtt ") + MODBEE_VERSION;
        char host[256] = {0};
        c.serverId = ::gethostname(host, sizeof host - 1) == 0 ? host : "modbee";
        bool read = true;
        if (!file.api.tlsCert.empty()) read = readWholeFile(file.api.tlsCert, c.tlsCertPem) && read;
        if (!file.api.tlsKey.empty()) read = readWholeFile(file.api.tlsKey, c.tlsKeyPem) && read;
        if (!file.api.tlsClientCa.empty()) read = readWholeFile(file.api.tlsClientCa, c.tlsClientCaPem) && read;
        if (!file.api.tokenFile.empty()) {
            read = readWholeFile(file.api.tokenFile, c.token) && read;
            while (!c.token.empty() && (c.token.back() == '\n' || c.token.back() == '\r' || c.token.back() == ' ')) {
                c.token.pop_back();
            }
            if (c.token.empty()) read = false;
        }
        std::string why;
        if (!read) why = "api: a tls file or the token file could not be read, or the token is empty";
        connections.reset(new AppConnections(*hub, file.api.stateDirectory));
        if (read && !connections->start(why)) read = false;
        ApiServer::Services s;
        s.authority = authority.get();
        s.messages = hub.get();
        s.connections = connections.get();
        s.admin = admin.get();
        s.logins = gate.get();
        s.server = &server;
        if (!read || !api.start(c, s, why)) {
            std::fprintf(stderr, "modbee serve: %s\n", why.c_str());
            server.stop();
            return 3;
        }
    }
#endif
    std::printf("modbee serve: %zu shard(s) on %s", server.shardCount(), config.address.c_str());
    if (config.plain) std::printf(" port %u", server.port());
    if (config.tls.enabled) {
        std::printf(" tls %u (%s%s)", server.tlsPort(), MQTT_TLS_ENGINE_NAME,
                    config.tls.clientCaPem.empty() ? ""
                    : config.tls.clientCertificateOptional ? ", client certificates optional"
                                                          : ", client certificates required");
    }
    std::printf(", up to %lld connections\n", b.maxConnections);
    if (gate) {
        std::printf("modbee serve: MQTT clients must log in - device logins in %s (%zu), "
                    "admin \"%s\"; a login without rules uses %s\n", file.authentication.database.c_str(),
                    gate->store().userCount(), file.authentication.accounts[0].username.c_str(),
                    file.authentication.defaultTopics == "none" ? "no topic (default-topics: none)"
                                                                : "every topic (default-topics: all)");
    } else if (accounts.count() > 0) {
        std::printf("modbee serve: MQTT clients must log in (%zu user name(s))\n", accounts.count());
    } else if (file.authentication.authority.enabled) {
        // Said below, with the Application API.
    } else {
        std::printf("modbee serve: MQTT clients need no user name or password\n");
    }
    if (gate && file.authentication.certificateLogin) {
        std::printf("modbee serve: a verified client certificate logs in as the login its CN "
                    "names, without a password (certificate-login)\n");
    }
    if (events) std::printf("modbee serve: client events published on $modbee/events/...\n");
#if defined(MODBEE_HAVE_APP_API)
    for (const std::string& address : api.listening()) {
        std::printf("modbee serve: Application API (gRPC) on %s\n", address.c_str());
    }
    if (authority) {
        std::printf("modbee serve: MQTT logins are decided by the application (authority), asked once "
                    "and kept; fixed logins: %zu\n", file.authentication.accounts.size());
    }
#endif
    if (keeper) {
        std::printf("modbee serve: retained messages kept in %s: %zu restored",
                    keeper->where().c_str(), keeper->loaded() - (size_t)keeper->expired() -
                                             (size_t)keeper->refused());
        if (keeper->expired() > 0) std::printf(", %llu expired while stopped",
                                               (unsigned long long)keeper->expired());
        if (keeper->refused() > 0) std::printf(", %llu over max-retained and not restored",
                                               (unsigned long long)keeper->refused());
        std::printf("\n");
    }
    if (sessions) {
        const SessionKeeperStatus k = sessions->status();
        std::printf("modbee serve: sessions kept in %s (sync %s): %llu restored",
                    sessions->directory().c_str(), file.sessions.syncFull ? "full" : "normal",
                    (unsigned long long)k.restored);
        if (k.expired > 0) std::printf(", %llu expired while stopped", (unsigned long long)k.expired);
        if (k.refused > 0) std::printf(", %llu over max-sessions and not restored", (unsigned long long)k.refused);
        if (k.willsDue > 0) std::printf(", %llu Wills owed", (unsigned long long)k.willsDue);
        std::printf("\n");
    }
    MetricsSources sources;
    sources.server = &server;
#if defined(MODBEE_HAVE_KAFKA)
    sources.pipe = pipe.get();
#endif
    MetricsEndpoint metrics;
    metrics.setReadiness(&serverReady, &sources);
#if defined(MODBEE_HAVE_APP_API)
    if (file.api.rest && !file.api.grpc.empty()) metrics.setRest(&restCall, &api);
#endif
    if (file.metrics.enabled) {
        std::string why;
        if (!metrics.start(file.metrics.address, file.metrics.port, &renderMetrics, &sources, why)) {
            std::fprintf(stderr, "modbee serve: %s\n", why.c_str());
            server.stop();
            return 3;
        }
        std::printf("modbee serve: metrics on http://%s:%u/metrics\n", file.metrics.address.c_str(),
                    metrics.port());
#if defined(MODBEE_HAVE_APP_API)
        if (file.api.rest && !file.api.grpc.empty()) {
            std::printf("modbee serve: Application API (REST) on http://%s:%u/v1/\n", file.metrics.address.c_str(),
                        metrics.port());
        }
#endif
    }
#if defined(MODBEE_HAVE_KAFKA)
    if (pipe) {
        std::printf("modbee serve: kafka pipe (librdkafka %s): %zu cluster(s), %zu mqtt-to-kafka, "
                    "%zu kafka-to-mqtt\n", KafkaPipe::libraryVersion(), file.clusters.size(),
                    file.toKafka.size(), file.fromKafka.size());
    }
#endif

    const mq_time_t started = mq_millis();
    mq_time_t lastStatus = started;
    const long long every = b.statusEvery;
    while (!cliStopRequested()) {
        usleep(200000);
        if (every > 0 && mq_millis() - lastStatus >= (mq_time_t)every * 1000) {
            lastStatus = mq_millis();
            const ShardedBrokerStatus s = server.status();
            std::printf("t=%llus connections %zu sessions %zu published %llu delivered %llu "
                        "dropped %llu handed-off %u ring-full %llu",
                        (unsigned long long)((lastStatus - started) / 1000),
                        s.total.connections, s.total.sessions,
                        (unsigned long long)s.total.published,
                        (unsigned long long)s.total.delivered,
                        (unsigned long long)s.total.dropped, s.total.handedOff,
                        (unsigned long long)s.mailboxFull);
#if defined(MODBEE_HAVE_KAFKA)
            if (pipe) {
                const KafkaPipeStatus k = pipe->status();
                uint64_t sent = 0, ok = 0, failed = 0, large = 0, polled = 0, published = 0;
                for (const KafkaPipeStatus::Mapping& m : k.mappings) {
                    sent += m.send; ok += m.success; failed += m.failed; large += m.tooLarge;
                    polled += m.polled; published += m.published;
                }
                std::printf(" | kafka sent %llu ok %llu failed %llu too-large %llu held %llu "
                            "polled %llu published %llu",
                            (unsigned long long)sent, (unsigned long long)ok,
                            (unsigned long long)failed, (unsigned long long)large,
                            (unsigned long long)k.heldAcknowledgements,
                            (unsigned long long)polled, (unsigned long long)published);
            }
#endif
            if (accounts.count() > 0 && accounts.refused() > 0) {
                std::printf(" | logins refused %llu", (unsigned long long)accounts.refused());
            }
            if (gate) {
                const AuthGateStatus a = gate->status();
                std::printf(" | logins ok %llu refused %llu provisioned %llu",
                            (unsigned long long)a.accepted, (unsigned long long)a.refused,
                            (unsigned long long)a.provisioned);
                if (a.certificateAccepted + a.certificateRefused > 0) {
                    std::printf(" (by certificate: ok %llu refused %llu)",
                                (unsigned long long)a.certificateAccepted,
                                (unsigned long long)a.certificateRefused);
                }
            }
            if (keeper && keeper->failures() > 0) {
                std::printf(" | retained store failures %llu",
                            (unsigned long long)keeper->failures());
            }
#if defined(MODBEE_HAVE_APP_API)
            if (authority) {
                const AuthorityStatus a = authority->status();
                std::printf(" | authority: %zu instance(s), asked %llu cached %llu stale %llu unavailable %llu",
                            a.streams, (unsigned long long)a.asked, (unsigned long long)a.cached,
                            (unsigned long long)a.stale, (unsigned long long)a.unavailable);
            }
#endif
            if (sessions) {
                const SessionKeeperStatus k = sessions->status();
                std::printf(" | sessions kept %llu", (unsigned long long)k.kept);
                if (k.failures > 0) std::printf(" failures %llu", (unsigned long long)k.failures);
            }
            std::printf("\n");
        }
    }
    std::printf("modbee serve: stopping\n");
    metrics.stop();      // first: its REST calls reach the Application API
#if defined(MODBEE_HAVE_APP_API)
    api.stop();          // before the shards: its calls reach them
    if (connections) connections->stop();
#endif
    server.stop();
    return 0;
}

#else

int cmdServe(int, char**) {
    std::fprintf(stderr, "modbee serve: this build has no server tier (Linux only)\n");
    return 2;
}

#endif

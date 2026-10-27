// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee check - validate modbee.yaml without starting anything.
//
// Every error and warning, each with its line, then a one-line summary of
// what the file would run. Exit 0 if it would load, 1 if it would not, 2 for
// bad usage - so a CI step or a container's pre-start hook can gate on it.

#include "Args.h"

#include <cstdio>

#if defined(MODBEE_HAVE_SERVER)

#include "ServerConfig.h"

int cmdCheck(int argc, char** argv) {
    static const std::vector<ArgSpec> specs = {
        {"config", 'C', true, "the file to check (default modbee.yaml)"},
        {"quiet", 'q', false, "print problems only, not the summary"},
        {"help", 0, false, "show this help"},
    };
    Args args;
    if (!args.parse(argc, argv, 2, specs)) {
        std::fprintf(stderr, "modbee check: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp("modbee check [-C modbee.yaml]", specs);
        return 0;
    }
    const std::string path = args.get("config", "modbee.yaml");
    ServerConfig config;
    std::vector<ConfigProblem> problems;
    const bool ok = loadServerConfigFile(path, config, problems);
    size_t errors = 0, warnings = 0;
    for (const ConfigProblem& problem : problems) {
        std::fprintf(problem.warning ? stdout : stderr, "%s\n", describe(problem, path).c_str());
        if (problem.warning) warnings++; else errors++;
    }
    if (!ok) {
        std::fprintf(stderr, "%s: %zu error(s), %zu warning(s) - it would not load\n",
                     path.c_str(), errors, warnings);
        return 1;
    }
    if (!args.has("quiet")) {
        const BrokerSection& b = config.broker;
        std::printf("%s: OK", path.c_str());
        if (warnings > 0) std::printf(" (%zu warning(s))", warnings);
        std::printf(" - broker on %s", b.address.c_str());
        if (b.plain) std::printf(" port %u", b.port);
        if (b.tls) std::printf(" tls %u", b.tlsPort);
        std::printf(", %zu shard(s)%s, up to %lld connections", b.shards,
                    b.shards == 0 ? " (one per core)" : "", b.maxConnections);
        if (!config.authentication.database.empty()) {
            const bool fileAdmin = config.authentication.hasAdmin && !config.authentication.accounts.empty();
            std::printf("; MQTT: device logins in %s, admin %s%s%s, default-topics %s",
                        config.authentication.database.c_str(), fileAdmin ? "\"" : "",
                        fileAdmin ? config.authentication.accounts[0].username.c_str() : "from the database (modbee admin)",
                        fileAdmin ? "\"" : "", config.authentication.defaultTopics.c_str());
        } else if (config.authentication.authority.enabled) {
            std::printf("; MQTT: logins decided by the application (authority), %zu fixed",
                        config.authentication.accounts.size());
        } else if (config.authentication.accounts.empty()) std::printf("; MQTT: no login");
        else std::printf("; MQTT: fixed logins (%zu)", config.authentication.accounts.size());
        std::printf("; retained: %s",
                    config.retained.store == RetainedStore::File  ? ("file " + config.retained.file).c_str()
                  : config.retained.store == RetainedStore::Kafka ? ("kafka topic " + config.retained.topic).c_str()
                                                                  : "memory");
        std::printf("; sessions: %s",
                    config.sessions.store == SessionStore::File
                        ? ("file " + config.sessions.directory + (config.sessions.syncFull ? " (sync full)" : "")).c_str()
                        : "memory");
        if (!config.api.grpc.empty()) {
            std::printf("; Application API on");
            for (const std::string& address : config.api.grpc) std::printf(" %s", address.c_str());
        }
        std::printf("; kafka: %zu cluster(s), %zu mqtt-to-kafka, %zu kafka-to-mqtt mapping(s)\n",
                    config.clusters.size(), config.toKafka.size(), config.fromKafka.size());
    }
    return 0;
}

#else

int cmdCheck(int, char**) {
    std::fprintf(stderr, "modbee check: this build has no server tier (Linux only)\n");
    return 2;
}

#endif

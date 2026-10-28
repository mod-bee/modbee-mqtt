// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// CONNECTIONS WHOSE LINK IS KAFKA (ConnectionSpec.kafka).
//
// Each one gets a broker of its own, inside this server: one shard on
// 127.0.0.1, an ephemeral port, a login made for it at random - and a Kafka
// pipe of its own bridging that broker to the link's topics in the envelope
// (KafkaEnvelope.h):
//
//   out   every message published there (#) -> the link's Kafka topic,
//         headers: all, the PUBACK held until Kafka has the record - for as
//         long as Kafka is away (no delivery timeout: the record waits, in
//         order, and the client does not give up on its acknowledgement)
//   in    the link's Kafka topics -> that broker, from the headers: the same
//         topic, QoS, retain and properties - but never a record this link
//         wrote itself (mqtt-link: its consumer group), which is Kafka's No
//         Local
//
// The connection's MQTT client connects to it as to any broker, so Consume,
// Publish, store and forward, request/response, Wills and a Sparkplug host or
// edge node on it work unchanged; its topics are its own, not this server's.
// A link changed only in its MQTT settings keeps its broker.
//
// A Sparkplug host's STATE: with stateConnection set, spBv1.0/STATE/# is not
// written to Kafka - AppConnections publishes the host's STATE on that MQTT
// connection instead, its Will the offline one, since Kafka has no Will.

#include "AppConnections.h"
#include "KafkaControl.h"
#include "ServerConfig.h"

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class KafkaLinks : public AppKafkaLinks {
public:
    // `clusters`: where the links' clusters come from, now and as they change
    // (KafkaControl::share is set up here).
    explicit KafkaLinks(KafkaControl& clusters);
    ~KafkaLinks() override;

    KafkaLinks(const KafkaLinks&) = delete;
    KafkaLinks& operator=(const KafkaLinks&) = delete;

    bool check(const std::string& id, const AppKafkaLinkSpec& link, std::string& error) override;
    bool open(const std::string& id, const AppKafkaLinkSpec& link, AppLocalAddress& where,
              std::string& error) override;
    void close(const std::string& id) override;
    bool status(const std::string& id, AppKafkaLinkStatus& out) const override;

    // Every link stopped, each given a few seconds for what it holds.
    void closeAll();

    struct Link;

private:
    // The private pipe's section for one link, from the clusters as they are.
    bool sectionFor(const std::string& id, const AppKafkaLinkSpec& link, ServerConfig& out,
                    std::string& error) const;
    std::string usedBy(const std::string& cluster) const;
    void clustersChanged(const ServerConfig& section);
    // A link let go: stopped on a thread of its own - its pipe gives Kafka a
    // few seconds for what it holds, and the connections' thread, which calls
    // close(), must not wait them out.
    void reap(std::unique_ptr<Link> link);

    std::mutex _reaping;
    std::vector<std::thread> _reapers;                    // under _reaping

    mutable std::mutex _lock;
    std::vector<KafkaClusterConfig> _clusters;           // under _lock
    std::map<std::string, std::unique_ptr<Link>> _links;  // under _lock
};

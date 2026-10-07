// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// modbee.yaml: what `modbee serve` runs and `modbee check` validates.
//
// THE FILE IS THE DEFAULT; THE ENVIRONMENT WINS. Every scalar in the schema
// can be set by an environment variable named after its path - upper case,
// '-' and '.' as '_', list indices as numbers:
//
//     kafka.clusters[0].bootstrap-servers  ->  MODBEE_KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS
//
// which is how a Compose file or a Kubernetes manifest configures a container
// without rewriting the file. A list of scalars (topic filters) is set by one
// variable, comma-separated. A list of MAPPINGS comes from the file only: the
// environment can change a mapping, not invent one.
//
// SECRETS NEVER NEED TO BE IN THE FILE. Wherever a key is a secret
// (`password`), `<key>-file` names a file holding it, and the environment
// variable `<NAME>_FILE` does the same - Docker's convention for secrets.
//
// EVERY PROBLEM IS REPORTED, WITH ITS LINE. Loading does not stop at the first
// mistake: an operator fixing a file wants the whole list, each with where it
// is. An unknown key is an error, because a misspelt key that is silently
// ignored is a setting that silently is not set.

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

struct ConfigProblem {
    size_t line = 0;          // 1-based; 0 when it is not about one place in the file
    size_t column = 0;
    std::string path;         // "kafka.mqtt-to-kafka[0].kafka-acks"
    std::string message;
    bool warning = false;     // reported, but the file still loads
};

// --- broker: ---------------------------------------------------------------

struct BrokerSection {
    std::string address = "0.0.0.0";
    bool plain = true;                 // the plain-TCP listener
    uint16_t port = 1883;
    bool tls = false;                  // a `tls:` section is present
    uint16_t tlsPort = 8883;
    std::string certificateFile, keyFile, clientCaFile;
    size_t shards = 0;                 // 0: one per core
    long long maxConnections = 100000; // whole server
    long long maxSessions = 0;         // 0: = maxConnections
    uint32_t maxPacket = 65536;
    size_t queue = 64;                 // messages held per session
    size_t subscriptions = 32;         // per session
    // Retained topics the server keeps. Every shard holds all of them (a
    // retained message must reach a subscriber on any shard), so the memory
    // is this times the shard count: about 216 bytes a slot, reserved at
    // start, plus each message's topic and payload once stored.
    size_t maxRetained = 10000;
    bool pinThreads = false;
    long long statusEvery = 5;         // seconds between status lines; 0 off
};

// --- kafka: ----------------------------------------------------------------

struct KafkaClusterConfig {
    std::string id;
    std::string bootstrapServers;      // "host:port,host:port"
    bool tls = false;
    std::string caFile, certificateFile, keyFile;
    bool verifyHostname = true;
    std::string mechanism = "none";    // none | plain | scram-sha256 | scram-sha512 | oauthbearer
    std::string username, password;
    // Anything else, handed to librdkafka exactly as written ("linger.ms").
    std::vector<std::pair<std::string, std::string>> properties;
    size_t line = 0;
};

enum class KafkaKeyMode : uint8_t {
    Topic,      // the MQTT topic (default): one topic's messages stay in order
    ClientId,   // the publisher: one device's messages stay in order (Sparkplug)
    None,       // no key: spread over partitions
    Template,   // keyTemplate, with ${MQTT_TOPIC}, ${MQTT_CLIENTID}, ${MQTT_TOPIC_LEVEL_n}
};

// One Kafka header a mapping writes: a placeholder for part of the MQTT
// message ("mqtt-topic"), or a fixed name and value.
struct KafkaHeaderSpec {
    std::string name;
    std::string value;                 // fixed headers only
    bool fixed = false;
};

struct MqttToKafkaConfig {
    std::string id;
    std::string cluster;
    std::vector<std::string> filters;  // MQTT topic filters, wildcards allowed
    std::string kafkaTopic;
    int acks = -1;                     // -1 all, 0 zero, 1 one
    size_t maxRecordBytes = 1048576;
    KafkaKeyMode key = KafkaKeyMode::Topic;
    std::string keyTemplate;
    bool allHeaders = false;           // every mqtt-* placeholder: the lossless envelope
    std::vector<KafkaHeaderSpec> headers;
    bool uniform = false;              // partition-strategy: uniform (ignore the key)
    bool acknowledgeOnKafka = true;    // PUBACK when Kafka has it; false: when routed
    size_t line = 0;
};

struct KafkaToMqttConfig {
    std::string id;
    std::string cluster;
    std::vector<std::string> kafkaTopics;
    std::string kafkaTopicPattern;     // a librdkafka regex ("^plant\\..*"), instead of a list
    std::vector<std::string> mqttTopics;   // templates: ${KAFKA_TOPIC}, ${KAFKA_KEY}; several multicast
    bool fromHeaders = false;          // topic, QoS, retain and properties from the mqtt-* headers
    uint8_t qos = 1;
    bool retain = false;
    std::string group;                 // default modbee-<id>-group
    // MQTT 5 properties the messages are published with.
    std::string contentType;
    std::string responseTopic;
    bool hasPayloadFormat = false;
    uint8_t payloadFormat = 0;
    bool hasMessageExpiry = false;
    uint32_t messageExpiry = 0;
    std::vector<std::pair<std::string, std::string>> userProperties;
    size_t line = 0;
};

// --- metrics: ---------------------------------------------------------------

struct MetricsSection {
    bool enabled = false;              // a metrics: section turns it on
    std::string address = "0.0.0.0";
    uint16_t port = 9464;              // Prometheus's registry convention for exporters
};

// --- retained: ---------------------------------------------------------------
//
// Where retained messages are kept AS WELL AS memory, so a restart does not
// lose them. Memory alone is the default, as for every broker built on the
// library; `file` and `kafka` are for a server.
enum class RetainedStore : uint8_t {
    Memory,     // nothing outlives the process
    File,       // a snapshot file, written a few seconds after a change and at shutdown
    Kafka,      // a compacted Kafka topic, written as each change happens
};

struct RetainedSection {
    RetainedStore store = RetainedStore::Memory;
    std::string file = "/var/lib/modbee/retained.db";
    std::string topic = "modbee-retained";
    std::string cluster;               // empty: the only cluster
};

// --- authentication: ---------------------------------------------------------
//
// Who may connect over MQTT. Empty: anyone - right on a closed network, and
// the library's default. Any account at all: every CONNECT must log in as one
// of them, with its password.
struct MqttAccount {
    std::string username;
    std::string password;
};

struct AuthenticationSection {
    // From `username`/`password` (MQTT_USERNAME / MQTT_PASSWORD in .env) and
    // `users:`. With a database they are fixed logins beside it, and the
    // first - username/password - is the ADMIN.
    std::vector<MqttAccount> accounts;
    bool hasAdmin = false;               // accounts[0] came from username/password

    // Device logins in a SQLite file, changed live - added, removed,
    // provisioned - through the admin topics and `modbee user` / `modbee key`.
    std::string database;
};

struct ServerConfig {
    BrokerSection broker;
    MetricsSection metrics;
    RetainedSection retained;
    AuthenticationSection authentication;
    std::vector<KafkaClusterConfig> clusters;
    std::vector<MqttToKafkaConfig> toKafka;
    std::vector<KafkaToMqttConfig> fromKafka;

    const KafkaClusterConfig* cluster(const std::string& id) const {
        for (const KafkaClusterConfig& c : clusters) if (c.id == id) return &c;
        return nullptr;
    }
};

// Where environment overrides come from. getenv by default; tests supply
// their own.
typedef const char* (*ConfigEnvFn)(void* context, const char* name);

struct ConfigSource {
    std::string name = "modbee.yaml";  // for messages
    ConfigEnvFn env = nullptr;         // null: the process environment
    void* envContext = nullptr;
};

// Parses and validates. True when there are no errors (warnings allowed);
// `problems` lists every error and warning, in file order.
bool loadServerConfig(const std::string& yamlText, const ConfigSource& source,
                      ServerConfig& out, std::vector<ConfigProblem>& problems);

// Reads the file first; a file that cannot be read is one problem.
bool loadServerConfigFile(const std::string& path, ServerConfig& out,
                          std::vector<ConfigProblem>& problems);

// "modbee.yaml:12:5: kafka.mqtt-to-kafka[0].kafka-acks: expected ..."
std::string describe(const ConfigProblem& problem, const std::string& fileName);

// The placeholders a mqtt-to-kafka `headers:` list may name, and that
// `headers: all` writes. Null-terminated.
extern const char* const kMqttHeaderPlaceholders[];

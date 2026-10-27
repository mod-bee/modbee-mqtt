// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// modbee.yaml: what `modbee-mqtt serve` runs and `modbee-mqtt check` validates.
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
    // The TLS listener: on when the file has a `tls:` section, or when a
    // certificate and its key are given as PEM text (below).
    bool tls = false;
    uint16_t tlsPort = 8883;
    std::string certificateFile, keyFile, clientCaFile;
    // The same material as PEM TEXT rather than paths - cert-pem, key-pem,
    // client-ca-pem - set from the settings database (config.set) or the
    // environment. Text wins over a path, as every stored setting wins over
    // the file. While the server runs, a change to these is applied to the
    // listeners at once (server/auth/TlsSettings.h); nothing restarts.
    std::string certificatePem, keyPem, clientCaPem;
    // With a client CA: "required" - every client must present a certificate
    // it signed (the default) - or "optional": one is asked for and verified
    // when offered, and a client that offers none logs in as usual.
    std::string clientCertificates = "required";
    size_t shards = 0;                 // 0: one per core
    long long maxConnections = 100000; // whole server
    long long maxSessions = 0;         // 0: = maxConnections
    uint32_t maxPacket = 65536;
    size_t queue = 64;                 // messages held per session
    // And the bytes they may hold: one session's queue, and every session's
    // together (the whole server; split evenly between shards). 0 = no cap.
    long long queueBytes = 1048576;
    long long queueBytesTotal = 67108864;
    size_t subscriptions = 32;         // per session
    // The longest a disconnected session (and its queued messages) is kept, in
    // seconds, whatever an MQTT 5 client asks for. 4294967295 = until the
    // broker restarts (sessions are held in memory).
    long long maxSessionExpiry = 3600;
    // Seconds a connection has to send its CONNECT (a TLS handshake included)
    // before it is closed: how long a socket that never signs in holds a slot.
    long long connectTimeout = 10;
    // The longest Keep Alive a client may use, in seconds; 0 = whatever it
    // asks, including 0 - never timed out for silence.
    long long maxKeepAlive = 0;
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

// --- events: ------------------------------------------------------------------
//
// Publish what clients do - connect, go, subscribe, unsubscribe - on
// $modbee/events/... (server/events/BrokerEvents.h). A Kafka mapping with the
// filter "$modbee/events/#" makes them durable.
struct EventsSection {
    bool enabled = false;              // an events: section turns it on
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

// --- sessions: ----------------------------------------------------------------
//
// Where persistent sessions are kept AS WELL AS memory - their subscriptions,
// the QoS 1 and 2 messages owed to them, and the Wills of connected clients -
// so a restart, or the process being killed, loses none of them
// (server/sessions/SessionKeeper.h). Memory alone is the default.
enum class SessionStore : uint8_t {
    Memory,     // nothing outlives the process
    File,       // one SQLite file per shard, written as each change happens
};

struct SessionsSection {
    SessionStore store = SessionStore::Memory;
    std::string directory = "/var/lib/modbee/sessions";
    // sync: full - every commit reaches the disk before the PUBACK, which
    // survives the power going; normal (the default) survives the process
    // being killed.
    bool syncFull = false;
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

// --- authentication: authority: ----------------------------------------------
//
// Logins decided by the APPLICATION, over the Application API (api:), instead
// of a login database: every CONNECT not a fixed login is asked of it once and
// the answer kept (server/api/Authority.h). An authority: section turns it on.
struct AuthoritySection {
    bool enabled = false;
    long long timeoutMs = 2000;          // a CONNECT asked waits this long for an answer
    long long staleForSeconds = 86400;   // an expired answer still serves this long while the application is away
    long long cacheSize = 1000000;       // answers kept, whole server
    long long maxPending = 10000;        // CONNECTs waiting for an answer at once, whole server
};

struct AuthenticationSection {
    // From `username`/`password` (MQTT_USERNAME / MQTT_PASSWORD in .env) and
    // `users:`. With a database they are fixed logins beside it, and the
    // first - username/password - is the ADMIN.
    std::vector<MqttAccount> accounts;
    bool hasAdmin = false;               // accounts[0] came from username/password

    // Device logins in a SQLite file, changed live - added, removed,
    // provisioned - through the admin topics and `modbee-mqtt user` / `modbee-mqtt key`.
    std::string database;

    // A client certificate the TLS listener verified IS the login: the login
    // is its subject's CN, and no password is asked for (docs/logins.md).
    // Needs the database, whose logins it names.
    bool certificateLogin = false;

    // What a database login with no publish / subscribe rules may use:
    // "all" (the default) every topic but the $ ones, as a broker without
    // per-login rules; "none" nothing - a login uses only the topics its rules
    // name, for a broker shared by several customers.
    std::string defaultTopics = "all";

    // What one login may hold, admins excepted: sessions (client
    // identifiers), and retained topics. 0 = no limit. Per shard.
    long long maxSessionsPerLogin = 8;
    long long maxRetainedPerLogin = 1000;

    AuthoritySection authority;
};

// --- api: ---------------------------------------------------------------------
//
// The Application API (proto/modbee/app/v1/app.proto): how an application -
// not a device - talks to the server. gRPC on a local socket, and optionally on
// TCP, which must have TLS and a token.
// tcp://127.0.0.1:7660, tcp://localhost:..., tcp://[::1]:...: reachable from
// this machine only.
inline bool apiAddressIsLoopback(const std::string& address) {
    if (address.compare(0, 6, "tcp://") != 0) return false;
    const std::string host = address.substr(6, address.rfind(':') == std::string::npos ? std::string::npos
                                                                                      : address.rfind(':') - 6);
    return host.compare(0, 4, "127.") == 0 || host == "localhost" || host == "[::1]";
}

struct ApiSection {
    std::vector<std::string> grpc;       // "unix:/run/modbee/api.sock", "tcp://0.0.0.0:7660"
    std::string tlsCert, tlsKey, tlsClientCa;   // PEM files, for tcp://
    // A token every call must carry ("authorization: Bearer ..."): api.token,
    // or api.token-file - and MODBEE_API_TOKEN or MODBEE_API_TOKEN_FILE.
    std::string token;
    // TCP without TLS on an address other machines could reach, the token
    // still required: for a port only published on 127.0.0.1, or a private
    // network (a Docker network beside the platform).
    bool plain = false;
    long long maxQueued = 100000;        // per consumer group: past it, the group refuses
    std::string stateDirectory = "/var/lib/modbee";   // managed connections are kept here
    // The REST surface, /v1/..., on the metrics listener: off a loopback
    // address only with the token.
    bool rest = false;
};

struct ServerConfig {
    BrokerSection broker;
    MetricsSection metrics;
    EventsSection events;
    RetainedSection retained;
    SessionsSection sessions;
    AuthenticationSection authentication;
    ApiSection api;
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

// --- Settings kept in the database ---------------------------------------------
//
// THE FILE IS THE BOOTSTRAP; THE DATABASE CAN HOLD THE REST. A server with a
// login database keeps its settings beside the logins, changed over MQTT
// ($modbee/admin config.*): a whole document that replaces the file, and
// per-setting overrides named by path ("broker.port"). The ENVIRONMENT STILL
// WINS over both - a setting pinned by a variable is reported as pinned - and
// an override goes through exactly the loader and validator the file does.
struct StoredSettings {
    bool hasDocument = false;
    std::string document;                                        // YAML, like modbee.yaml
    std::vector<std::pair<std::string, std::string>> overrides;  // path -> value
};

struct EffectiveConfigInfo {
    // Environment variables that set something (they win over the database).
    std::vector<std::string> pinned;
};

// The file (or the stored document in its place), with the stored overrides
// under the environment. An override for a path the schema does not have is an
// error, like a misspelt key in the file.
bool loadEffectiveConfig(const std::string& fileText, const std::string& fileName,
                         const StoredSettings& stored, ServerConfig& out,
                         std::vector<ConfigProblem>& problems, EffectiveConfigInfo* info = nullptr);

// "broker.tls.port" -> "MODBEE_BROKER_TLS_PORT"
std::string configEnvironmentName(const std::string& path);

// The placeholders a mqtt-to-kafka `headers:` list may name, and that
// `headers: all` writes. Null-terminated.
extern const char* const kMqttHeaderPlaceholders[];

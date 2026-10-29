// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// MQTT CONNECTIONS THE SERVER KEEPS FOR THE APPLICATION (Application API:
// Connections)
// =============================================================================
//
// Another broker - a customer's, the platform's for a gateway's uplink, one a
// Sparkplug host or edge node lives on - reached by the server's own MQTT
// client, so the application needs no MQTT library and the connection stays
// up while the application restarts.
//
//   FAILOVER       `servers` in order of preference: an attempt that fails
//                  moves to the next; on any but the first, the first is
//                  checked every `primaryCheckSeconds` and gone back to once
//                  it answers
//   KEPT           the connections are written to a file in the server's data
//                  directory and opened again at start
//   IN             the messages it receives go to the consumer groups opened
//                  on it (AppHub), which subscribe it to their filters
//   OUT            a publish on it is answered when the remote broker has
//                  taken it - its PUBACK/PUBCOMP for CONFIRM_DELIVERED
//   WHEN ASKED     a connection that drops waits for reconnect() - with a new
//                  Will if it brings one (a Sparkplug edge node's NDEATH
//                  carries a new bdSeq each session)
//
// One thread owns every client (the library's MqttClient, single-threaded and
// non-blocking); calls from the API reach it through a queue.

#include "AppHub.h"
#include "ConnectionOutbox.h"
#include "sparkplug/MqttSparkplugTypes.h"
#include "sparkplug/MqttSparkplugValue.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct AppEndpoint {
    std::string host;
    uint16_t port = 0;           // 0: 1883, or 8883 with TLS
};

// A connection whose link is Kafka: its client is connected to a broker of
// its own, inside this server, which a Kafka pipe of its own bridges to these
// topics in the envelope (server/kafka/KafkaEnvelope.h) - so everything a
// connection does works on it unchanged. AppKafkaLinks makes that broker.
struct AppKafkaLinkSpec {
    std::string cluster;                   // "": the only one there is
    std::vector<std::string> inTopics;     // read from
    std::string inPattern;                 // a regular expression, instead
    std::string outTopic;                  // written to; "": nothing is
    std::string key;                       // topic ("") | clientid | none | a template
    std::string consumerGroup;             // "": modbee-conn-<id>
    // An MQTT connection carrying a Sparkplug host's STATE on this one -
    // Kafka has no Will. spBv1.0/STATE/# is then not written to Kafka.
    std::string stateConnection;
};

struct AppKafkaLinkStatus {
    bool reachable = false;
    uint64_t recordsOut = 0, recordsOutFailed = 0;   // Kafka has them; refused or given up
    uint64_t recordsIn = 0, recordsInInvalid = 0;    // read and handed on; could not be
    int64_t lag = -1;                                // records not read yet; -1 not known
    uint64_t held = 0;                               // publishes on it waiting for Kafka now
};

struct AppConnectionSpec {
    std::string id;
    std::vector<AppEndpoint> servers;
    // A connection to this server itself, as one of its own clients: no
    // servers needed (reachThisServer() says where, and with what login).
    // What a Sparkplug host or edge node on the server's own devices runs on.
    bool thisServer = false;
    // Kafka as the link instead (AppKafkaLinkSpec): no servers either.
    bool kafka = false;
    AppKafkaLinkSpec link;
    std::string clientId;
    bool hasUsername = false;
    std::string username;
    bool hasPassword = false;
    std::string password;        // never read back
    bool tls = false;
    std::string caPem;           // "" the system's trust store
    std::string certificatePem, keyPem;   // mutual TLS; the key never read back
    uint8_t protocol = 5;
    uint32_t keepaliveSeconds = 30;
    bool cleanStart = false;
    uint32_t sessionExpirySeconds = 0;
    bool hasWill = false;
    AppPublish will;
    bool reconnectWhenAsked = false;
    uint32_t primaryCheckSeconds = 60;
    // MQTT 5 CONNECT: the Will's delay (its properties are will.properties),
    // and what this end will take (0: the protocol's defaults).
    uint32_t willDelaySeconds = 0;
    uint16_t receiveMaximum = 0, topicAliasMaximum = 0;
    uint32_t maximumPacketSize = 0;
    // A slow link's timings (0: the defaults): how long a connect - DNS, TCP,
    // TLS and the CONNACK - may take (30 s), and how long an acknowledgement
    // is waited for before the message counts as lost (twice the keep-alive,
    // at least 120 s - a message waits behind others on a slow link, and
    // giving up early sends it again on a link that was only busy).
    uint32_t connectTimeoutSeconds = 0;
    uint32_t ackTimeoutSeconds = 0;
    // STORE AND FORWARD (ConnectionOutbox): every publish on it kept on disk
    // until the other broker has acknowledged it, whether it is up or not.
    bool storeAndForward = false;
    uint64_t storeMaxMessages = 100000;
    uint64_t storeMaxBytes = 64ull << 20;
    bool storeDropOldest = false;           // full: give up the oldest; otherwise refuse the new one (0x97)
};

enum class AppConnectionState : uint8_t { Connecting, Connected, Waiting, BackingOff, Refused };

struct AppConnectionInfo {
    AppConnectionSpec spec;      // as kept, the password and key left out
    AppConnectionState state = AppConnectionState::Connecting;
    uint32_t serverIndex = 0;
    bool sessionPresent = false;
    std::string lastError;
    int64_t sinceMs = 0;
    uint64_t messagesIn = 0, messagesOut = 0;
    uint64_t stored = 0, storedBytes = 0;   // held for the other broker now
    int64_t oldestStoredMs = 0;             // when the oldest of them was taken; 0: none
    uint64_t storedDropped = 0;             // given up to make room (store_and_forward.when_full)
    uint64_t storedExpired = 0;             // their Message Expiry Interval ran out while they waited
    AppKafkaLinkStatus link;                // spec.kafka: what its link is doing
};

// A Sparkplug B Host Application the server runs on one of its connections
// (the library's MqttSparkplugHost, sharing the connection's client): STATE
// born and died on it, every node's session followed - bdSeq, seq, aliases -
// and a rebirth asked for when a node's stream cannot be trusted: a gap that
// the reorder window did not close, data from a node with no birth, an alias
// no birth gave, a payload that does not decode, and every node when the
// host's own session begins - at most once a rebirthMinIntervalMs per node.
struct AppSparkplugHostSpec {
    std::string id;                        // this server's name for it
    std::string connection;                // the connection it runs on
    std::string hostId;                    // spBv1.0/STATE/<hostId>
    std::string group;                     // the group it follows; "": every group
    bool rebirthOnGap = true;
    uint32_t reorderTimeoutMs = 2000;      // how long a gap waits for the missing message
    uint32_t rebirthMinIntervalMs = 60000;
};

struct AppSparkplugHostInfo {
    AppSparkplugHostSpec spec;
    bool online = false;                   // its STATE is born on a connection that is up
    size_t nodes = 0;
    uint32_t decodeFailures = 0;
    uint64_t rebirthsRequested = 0;
};

// One metric a node's births declared, with the last value of a number or a
// bool (a host follows every metric's value; these are what it can say here).
struct AppSparkplugMetricInfo {
    std::string name, device;
    uint64_t alias = 0;
    bool hasAlias = false;
    uint32_t datatype = 0;
    enum Kind : uint8_t { None, Signed, Unsigned, Real, Boolean, Span } kind = None;
    int64_t i = 0;
    uint64_t u = 0;
    double d = 0;
    bool b = false;
    std::string bytes;                     // Span: text, bytes, an array's packed bytes, a DataSet or Template
    uint64_t at = 0;                       // the value's timestamp (ms since 1970)
    bool stale = false;                    // its node or device died since its birth
    uint64_t staleAt = 0;                  // when: the DDEATH's timestamp, or the host's time of the NDEATH
    int32_t quality = 0;                   // the last Quality its birth or data gave
    bool hasQuality = false;
};

struct AppSparkplugNodeInfo {
    std::string group, node;
    bool online = false;
    bool hasBdSeq = false, hasSeq = false;
    uint64_t bdSeq = 0;
    uint32_t lastSeq = 0;
    uint32_t messages = 0, gaps = 0, rebirthsRequested = 0;
    uint64_t offlineAt = 0;                // the host's UTC time of the NDEATH that took it offline
    std::vector<std::string> devices;
    std::vector<std::string> devicesOnline;
    std::vector<AppSparkplugMetricInfo> metrics;
    // The births its metrics come from, as they arrived: device "" the NBIRTH,
    // else that device's DBIRTH - every property they gave, for the API to read.
    std::vector<std::pair<std::string, std::string>> births;
};

// A Sparkplug B Edge Node the server runs on one of its connections (the
// library's MqttSparkplugNode, sharing the connection's client): NDEATH the
// connection's Will with a new bdSeq each session, births from what it
// declares (aliases its own), data as the application sets values -
// deadbands applied - a rebirth when a host asks, a primary host's STATE
// waited for. While it is not born, values set are kept on disk (the
// connections' outbox) and sent, oldest first, with is_historical once it is:
// Sparkplug's own store and forward. Every datatype, Templates' definitions
// in the NBIRTH. A declaration changed while it runs is published in the
// same session: a device's DDEATH and DBIRTH, or for the node's own metrics
// a rebirth.
struct AppSparkplugMetricDeclaration {
    std::string name, device;              // device "": the node's own
    uint32_t datatype = 0;
    bool writable = false;
    std::string units;
    bool hasRange = false;
    double low = 0, high = 0;
    double deadband = 0, deadbandPercent = 0;
    // What its births say beside engUnit, Quality and the range: the body of
    // an encoded PropertySet (keys, then values), as long as it is.
    std::string properties;
    bool historyFirst = false;             // what it kept goes before the rest
};

// A Template definition, published in the NBIRTH: `encoded` is its
// Payload.Template (is_definition true, no template_ref).
struct AppSparkplugTemplateDefinition {
    std::string name;
    std::string encoded;
};

struct AppSparkplugEdgeNodeSpec {
    std::string id, connection, group, node;
    std::string primaryHostId;             // "": born whenever connected
    std::vector<std::string> devices;
    std::vector<AppSparkplugMetricDeclaration> metrics;
    // What it keeps while not born - values, bytes, age; 0: no limit of that
    // kind. Past one, the oldest go (counted), samples before history_first's.
    uint64_t historyMax = 100000;
    uint64_t historyMaxBytes = 0, historyMaxAgeMs = 0;
    // Values a second sent as history once it is born (0: as fast as the link
    // takes); live values go at once, whatever history waits.
    uint32_t historyRate = 1000;
    std::vector<AppSparkplugTemplateDefinition> templates;
};

struct AppSparkplugEdgeNodeInfo {
    AppSparkplugEdgeNodeSpec spec;
    bool born = false;
    uint32_t bdSeq = 0;
    bool primaryHostOnline = false;
    uint32_t births = 0;
    uint64_t history = 0, historyLost = 0;
    uint64_t historyBytes = 0;
    bool historyDropping = false;          // a limit is dropping the oldest, since the history last emptied
    std::vector<std::string> devicesOnline;
    bool deleted = false;                  // a watcher's: DeleteEdgeNode
};

// A value the application sets: `value` at the metric's datatype, `text` the
// bytes of a span one - String, Text, UUID, Bytes, File, an array's packed
// bytes, a DataSet's or Template's encoded message (value points into it).
struct AppSparkplugSet {
    std::string name;
    MqttSparkplugValue value;
    std::string text;
    uint64_t timestamp = 0;                // 0: now
    bool hasQuality = false;               // its Quality changed with it: 192, 0 or 500
    int32_t quality = MQ_SP_QUALITY_GOOD;
};

// Where a connection's client connects when it is not to another broker:
// this server itself, or a Kafka link's own broker.
struct AppLocalAddress {
    std::string host = "127.0.0.1";
    uint16_t port = 0;
    bool tls = false;
    bool hasLogin = false;
    std::string user, password;
};

// What makes a Kafka link's broker (server/kafka/KafkaLinks), where Kafka is
// built; none, and a connection with `kafka` is refused. Called on the
// manager's thread.
class AppKafkaLinks {
public:
    virtual ~AppKafkaLinks() {}
    // Whether `link` can be run: its cluster exists, its topics are Kafka's.
    virtual bool check(const std::string& id, const AppKafkaLinkSpec& link, std::string& error) = 0;
    // Its broker running - made, or made again when `link` changed - and
    // where the connection's client reaches it.
    virtual bool open(const std::string& id, const AppKafkaLinkSpec& link, AppLocalAddress& where,
                      std::string& error) = 0;
    // Stopped: what it holds for Kafka given a few seconds to get there.
    virtual void close(const std::string& id) = 0;
    virtual bool status(const std::string& id, AppKafkaLinkStatus& out) const = 0;
};

class AppConnections {
public:
    // `stateDirectory` holds connections.json (and any TLS material given as
    // text), readable by the server's own user only. "" keeps nothing.
    // `syncFull`: what store and forward keeps is on the disk before the
    // publish is answered, power loss included.
    AppConnections(AppHub& hub, const std::string& stateDirectory, bool syncFull = false);
    ~AppConnections();

    // How a connection to this server itself (AppConnectionSpec::thisServer)
    // reaches it, asked at each attempt - its listeners may have moved: the
    // address and port, whether that port is TLS (loopback, so its certificate
    // is not checked), and the login the server keeps for its own clients.
    // False: it cannot be reached now. Set before start().
    typedef AppLocalAddress ThisServer;
    void reachThisServer(std::function<bool(ThisServer&)> where) { _thisServer = std::move(where); }
    // What makes Kafka links (not owned; outlives stop()). Set before start().
    void linkKafka(AppKafkaLinks* links) { _links = links; }
    bool kafkaLinks() const { return _links != nullptr; }

    // Opens what was kept, and starts the thread.
    bool start(std::string& error);
    void stop();

    // Create, or replace by id: a changed spec reconnects; an unchanged one
    // does nothing.
    bool put(const AppConnectionSpec& spec, AppConnectionInfo* info, std::string& error);
    bool get(const std::string& id, AppConnectionInfo& info) const;
    std::vector<AppConnectionInfo> list() const;
    bool remove(const std::string& id);
    // Connect again now - waiting (when asked) or not - with `will` from this
    // connect on, when given.
    bool reconnect(const std::string& id, const AppPublish* will, AppConnectionInfo* info, std::string& error);

    // Every change of state of these connections (all, when `ids` is empty),
    // starting with each one's state now. Called on the manager's thread.
    uint64_t watch(const std::vector<std::string>& ids, std::function<void(const AppConnectionInfo&)> changed);
    void unwatch(uint64_t watcher);

    // --- Sparkplug hosts (AppSparkplugHostSpec) ---
    bool putHost(const AppSparkplugHostSpec& spec, AppSparkplugHostInfo* info, std::string& error);
    bool getHost(const std::string& id, AppSparkplugHostInfo& info) const;
    std::vector<AppSparkplugHostInfo> hosts() const;
    // Its STATE dies first.
    bool removeHost(const std::string& id);
    // What the host knows of its nodes: `group` and `node` narrow it ("": all).
    bool nodes(const std::string& host, const std::string& group, const std::string& node,
               std::vector<AppSparkplugNodeInfo>& out, std::string& error) const;
    // Asked now, whatever the limits say (a person asked). `device` "": the node.
    bool rebirth(const std::string& host, const std::string& group, const std::string& node,
                 const std::string& device, std::string& error);
    // The connection a host runs on, to publish commands on.
    bool hostConnection(const std::string& host, std::string& connection) const;

    // --- Sparkplug edge nodes (AppSparkplugEdgeNodeSpec) ---
    bool putEdgeNode(const AppSparkplugEdgeNodeSpec& spec, AppSparkplugEdgeNodeInfo* info, std::string& error);
    bool getEdgeNode(const std::string& id, AppSparkplugEdgeNodeInfo& info) const;
    std::vector<AppSparkplugEdgeNodeInfo> edgeNodes() const;
    bool removeEdgeNode(const std::string& id);          // its NDEATH first
    // Born: set, and the changes published (NDATA or DDATA, deadbands applied).
    // Not born: kept, and sent as history once it is. `unknown`: names it never declared.
    // `refused`: "name: why" for a value its metric cannot take.
    bool setValues(const std::string& id, const std::string& device, std::vector<AppSparkplugSet>& values,
                   uint32_t& sent, uint32_t& kept, std::vector<std::string>& unknown,
                   std::vector<std::string>& refused, std::string& error);
    bool deviceState(const std::string& id, const std::string& device, bool online, std::string& error);
    bool rebirthEdgeNode(const std::string& id, std::string& error);
    // One device of an edge node declared again - `metrics` its own, `templates`
    // definitions they use that the node lacks - or added: its DDEATH if it was
    // born, and its DBIRTH, in the same session. A new or changed definition is
    // in the NBIRTH only, so the node is born again (same bdSeq). `online`:
    // whether it is born now. `notFound`: no such edge node (or device, to remove).
    bool putDevice(const std::string& id, const std::string& device,
                   const std::vector<AppSparkplugMetricDeclaration>& metrics,
                   const std::vector<AppSparkplugTemplateDefinition>& templates, bool& online,
                   bool& notFound, std::string& error);
    // Its DDEATH, and gone - with its metrics, and what was kept of them.
    bool removeDevice(const std::string& id, const std::string& device, bool& notFound, std::string& error);
    // Every change of these edge nodes (all, when `ids` is empty), starting
    // with each one's state now: born or not, the primary host, births,
    // devices online, history - its counts at most once a second. Without
    // metrics or templates (getEdgeNode has them). Called on the manager's thread.
    uint64_t watchEdgeNodes(const std::vector<std::string>& ids,
                            std::function<void(const AppSparkplugEdgeNodeInfo&)> changed);
    void unwatchEdgeNodes(uint64_t watcher);

    // From AppHub: a publish on a connection; a group on one opened or closed.
    void publish(const AppPublish& message, std::function<void(const AppPublishResult&)> done);
    void groupsChanged(const std::string& connection);

    struct Connection;
    struct Host;
    struct EdgeNode;

private:
    AppHub& _hub;
    std::string _directory;
    bool _syncFull = false;
    std::function<bool(ThisServer&)> _thisServer;
    AppKafkaLinks* _links = nullptr;
    ConnectionOutbox _outbox;                 // the manager's thread only
    mutable std::mutex _lock;                 // the maps and the queue
    std::condition_variable _wake;
    std::map<std::string, std::unique_ptr<Connection>> _connections;
    std::map<std::string, AppSparkplugHostSpec> _hosts;      // by id; each runs on its connection
    std::map<std::string, AppSparkplugEdgeNodeSpec> _edgeNodes;
    std::vector<std::function<void()>> _commands;   // run on the manager's thread
    struct Watcher {
        std::vector<std::string> ids;
        std::function<void(const AppConnectionInfo&)> changed;
    };
    std::map<uint64_t, Watcher> _watchers;
    struct EdgeWatcher {
        std::vector<std::string> ids;
        std::function<void(const AppSparkplugEdgeNodeInfo&)> changed;
    };
    std::map<uint64_t, EdgeWatcher> _edgeWatchers;
    std::atomic<bool> _edgeWatched{false};
    uint64_t _nextWatcher = 1;
    bool _quit = false;
    std::thread _thread;

    void run();
    void open(Connection& connection);
    void close(Connection& connection);
    void service(Connection& connection);
    void forward(Connection& connection);     // what store and forward holds, to the other broker
    void store(Connection& connection, const AppPublish& message,
               std::function<void(const AppPublishResult&)> done);
    void announce(Connection& connection);
    void save();
    bool load(std::string& error);
    void startHost(Connection& connection);   // the host its spec puts on it, if any
    void serviceHost(Connection& connection);
    void saveHosts();
    bool loadHosts(std::string& error);
    void startEdgeNode(Connection& connection);
    void serviceEdgeNode(Connection& connection);
    // What an edge node keeps across a restart (EdgeNode::savedBdSeq):
    // written when its bdSeq moves, and at most once a second as values do.
    void saveEdgeState(EdgeNode& edge, bool now);
    void loadEdgeState(EdgeNode& edge);
    // A spec stored and applied: where it runs, born anew when its session
    // must change (connection, group, node, primary host), else its
    // declarations changed in the session it has.
    bool applyEdgeNode(const AppSparkplugEdgeNodeSpec& spec, std::string& error);
    void redeclare(Connection& connection, const AppSparkplugEdgeNodeSpec& next);
    // Room for one more value of `bytes` in what it keeps, the oldest dropped
    // for it; and what is older than its age limit, dropped.
    void makeRoom(EdgeNode& edge, size_t bytes);
    void expireHistory(EdgeNode& edge);
    void sendHistory(Connection& connection);
    void announceEdge(EdgeNode& edge, bool now);
    void announceEdgeInfo(const AppSparkplugEdgeNodeInfo& info);
    // A host on a Kafka link: its STATE on the link's state connection.
    void serviceHostState(Connection& connection);
    void serviceLink(Connection& connection);
    void post(std::function<void()> command);
    // Runs `command` on the manager's thread and waits for it: every
    // connection is touched there and nowhere else.
    void call(std::function<void()> command) const;
    AppConnectionInfo infoOf(const Connection& connection) const;
    // The Kafka connection whose host's STATE `id` carries; "" none.
    std::string stateUser(const std::string& id) const;
};

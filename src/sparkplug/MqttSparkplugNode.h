// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// A SPARKPLUG EDGE NODE
// =============================================================================
//
// An L4 layer over MqttClient, the same level as MqttDevice and following the
// same rule from docs/ARCHITECTURE.md: A CONVENIENCE LAYER MAY CHOOSE A DEFAULT
// AND MAY NOT HIDE AN OUTCOME.
//
// What it chooses for you, because the specification does and a pin that can
// only hold one value is a lie on a rung:
//
//   - Clean Start true, Session Expiry 0 [tck-id-principles-persistence-clean-
//     session-50]. A configuration saying otherwise is REFUSED, not corrected.
//   - NBIRTH and NDATA at QoS 0, retain false [tck-id-topics-nbirth-mqtt],
//     [tck-id-topics-ndata-mqtt].
//   - The Will at QoS 1, retain FALSE [tck-id-payloads-ndeath-will-message-qos],
//     -retain. Note the difference from a Host Application's Will, which is
//     retain TRUE - the two are not the same and swapping them is a real bug.
//   - NCMD and DCMD subscribed at QoS 1, mandatory in both cases
//     [tck-id-message-flow-edge-node-ncmd-subscribe],
//     [tck-id-message-flow-device-dcmd-subscribe].
//
// WHAT IT WILL NOT DO. A metric that was not declared before the birth cannot
// be written: [tck-id-operational-behavior-data-publish-nbirth] requires the
// NBIRTH to carry every metric the node will ever publish, so the set is closed
// and a write to a name outside it is refused and counted. That is the API's
// central constraint and it is deliberate - the alternative is a node that
// publishes metrics a Host Application has never been told about, which a
// conformant one discards silently.
//
// THE SEQUENCE NUMBERS ARE PRIVATE. Exposing them would let a caller produce a
// non-conformant node, which is the thing this layer exists to prevent. They
// are readable - sequence(), birthDeathSequence() - because a function block
// needs to show them, and not writable.

#include "MqttSparkplugPayload.h"
#include "MqttSparkplugTopic.h"

#include "../MqttClient.h"
#include "../MqttFeatures.h"
#include "../MqttOwnedTransport.h"

#include <string>
#include <type_traits>

// How many servers a node can fail over through.
//
// [tck-id-operational-behavior-primary-application-state-with-multiple-servers-walk]
// and -host-offline-reconnect require a node to move to the NEXT server in its
// list when the primary host goes offline, so a list is a feature rather than a
// configuration convenience. Three is the specification's own worked example.
#ifndef MQ_SP_MAX_SERVERS
#define MQ_SP_MAX_SERVERS 4
#endif

// Devices under one node.
#ifndef MQ_SP_MAX_DEVICES
#define MQ_SP_MAX_DEVICES 8
#endif

// --- What a command looks like when it arrives ---

struct MqttSparkplugCommand {
    // Empty for an NCMD, the device id for a DCMD.
    const char* device = nullptr;

    // The name as WE declared it, not as it arrived - a command may carry only
    // an alias [tck-id-payloads-alias-data-cmd-requirement], and the caller
    // wants the name they wrote in their own code. Resolved through the alias
    // table before the handler sees it.
    const char* name = nullptr;

    uint64_t alias = 0;
    MqttSparkplugValue value;

    // True when this is Node Control/Rebirth or Device Control/Rebirth. Handled
    // internally before the handler runs - the node has already republished -
    // and surfaced so an application can log it.
    bool isRebirth = false;
};

typedef void (*MqttSparkplugCommandFn)(const MqttSparkplugCommand& command);
typedef void (*MqttSparkplugCommandContextFn)(void* context,
                                              const MqttSparkplugCommand& command);

// --- One declared metric ---

struct MqttSparkplugPoint {
    std::string name;
    MqttSparkplugDataType type = MqttSparkplugDataType::Unknown;

    uint64_t alias = 0;

    // Which device it belongs to, or -1 for a node-level metric. An index
    // rather than a name so the hot path is an integer compare.
    int16_t device = -1;

    MqttSparkplugValue value;

    // What was last actually published, so publishChanged() can tell. Separate
    // from `value` because a value written and not yet published is still
    // pending - comparing against the last scan instead would let a slow drift
    // accumulate and never go.
    MqttSparkplugValue published;
    bool everPublished = false;

    // Whether a DCMD or NCMD may write it. A command for a read-only metric is
    // refused and counted rather than applied: a Host Application that thinks
    // it can write a sensor reading should find out.
    bool writable = false;

    // Set when a command has written this point and the application has not
    // taken it yet. The pollable half of R4.
    bool commandPending = false;

    std::string units;
    int32_t quality = MQ_SP_QUALITY_GOOD;
    std::string description;
};

// --- The node ---

class MqttSparkplugNode {
public:
    // --- Construction: the network, three ways, as MqttDevice takes it ---

    // On a board: the sketch's network channel - WiFiClient, EthernetClient,
    // TinyGsmClient, or WiFiClientSecure / SSLClient for TLS. It is yours and
    // must outlive the node; the node points it at each server in turn.
    template <typename ChannelType,
              typename = typename std::enable_if<
                  !std::is_base_of<MqttTransport, ChannelType>::value>::type>
    explicit MqttSparkplugNode(ChannelType& channel) : MqttSparkplugNode() {
        _net.adopt(channel);
    }

    // On a host: nothing to supply. Plain TCP, or TLS after useTls(). (On a
    // board this has no network, and connect() returns false.)
    MqttSparkplugNode();

    // A transport you built and configured yourself. Never reconfigured here -
    // so it stays pointed where you pointed it, and a server LIST cannot walk
    // it. Use one of the constructors above to fail over between servers.
    explicit MqttSparkplugNode(MqttTransport& transport);

    ~MqttSparkplugNode();

    MqttSparkplugNode(const MqttSparkplugNode&) = delete;
    MqttSparkplugNode& operator=(const MqttSparkplugNode&) = delete;

    // --- Identity. All before connect() ---

    // [tck-id-topic-structure-namespace-valid-group-id] Refused rather than
    // sanitised if it contains + / or #.
    bool group(const char* groupId);
    bool node(const char* edgeNodeId);

    void credentials(const char* user, const char* password);
    void keepAlive(uint16_t seconds);

    // Which protocol version to ask for. Both are legal for Sparkplug 3.0 -
    // [tck-id-principles-persistence-clean-session-311] and -50 cover each.
    // 5.0 by default, falling back, because 5.0 lets an intentional disconnect
    // carry reason 0x04 and have the SERVER publish the NDEATH
    // [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt50].
    void only(MqttVersion version);

    // --- The clock, and it is not optional ---
    //
    // [tck-id-payloads-timestamp-in-UTC] This timestamp MUST be in UTC.
    // [tck-id-topics-nbirth-timestamp] and its siblings make a timestamp
    // mandatory on every message type, so it cannot be omitted either.
    //
    // mq_millis() IS NOT THIS. It is a monotonic counter since boot - correct
    // for every deadline in this library and wrong here by about fifty-six
    // years. The Eclipse TCK caught it: "payloads-timestamp-in-UTC: FAIL",
    // which is exactly the class of fault our own tests could not find,
    // because they were written from the same reading of the specification as
    // the code.
    //
    // ON A HOST there is a system clock and the default uses it. ON A BOARD
    // there may be no clock at all - an ESP32 with no NTP and no RTC knows
    // only how long it has been running - so the epoch has to be supplied.
    // Call this once, with milliseconds since 1970-01-01 UTC, from whatever
    // the device does have: an NTP block, an RTC, a value the SCADA system
    // wrote down. The node then adds its own uptime to it.
    //
    // Without it on such a board, timestamps are boot-relative and the node is
    // NOT CONFORMANT - which clockIsSet() reports rather than hiding.
    void epochNow(uint64_t millisecondsSinceEpoch);
    bool clockIsSet() const;

    // --- Servers. One, or a list to fail over through ---

    // Replaces the list with a single entry.
    bool server(const char* host, uint16_t port = 0);

    // Appends. The list is walked IN ORDER, and a valid offline STATE from the
    // primary host moves to the next entry rather than retrying this one.
    bool addServer(const char* host, uint16_t port = 0);

    size_t serverCount() const { return _serverCount; }
    size_t activeServer() const { return _serverIndex; }

    // --- TLS, on a host ---
    //
    // On the transport the node made for itself (the no-argument
    // constructor), and before connect(). Each returns false otherwise: on a
    // board TLS is the channel's - WiFiClientSecure or SSLClient - and a
    // transport you brought is configured by you.
    //
    // A server added with port 0 gets 8883 once TLS is on.
#if MQTT_TLS_SOCKETS
    // Verify the broker against the system's certificate store.
    bool useTls(bool enabled = true);
    // Verify it against this CA file instead.
    bool trustFile(const char* path);
    // Trust exactly one broker key: its SHA-256 public-key fingerprint, 64
    // hexadecimal characters. What a plant broker nobody signed for wants;
    // docs/security.md, "Learning the key the first time", says how to read
    // it off the broker.
    bool pinnedKey(const char* fingerprint);
#endif

    // --- The primary host, if there is one. OFF by default ---
    //
    // Configured, the node will not publish its birth sequence until this host
    // publishes online=true [tck-id-operational-behavior-edge-node-birth-
    // sequence-wait], and a valid offline moves it to the next server
    // [tck-id-operational-behavior-edge-node-termination-host-offline-reconnect].
    //
    // Not configured, the node births as soon as the session is up. That is the
    // default because it is what a single-broker plant wants, and because a
    // node waiting for a host that does not exist looks exactly like a node
    // that is broken.
    void primaryHost(const char* sparkplugHostId);
    bool hasPrimaryHost() const { return !_primaryHostId.empty(); }
    bool primaryHostOnline() const { return _primaryHostOnline; }

    // --- Declaring metrics ---
    //
    // Returns the alias, or 0 on refusal - a full table, an invalid name, an
    // unsupported datatype, or a name already declared.
    //
    // ALIASES ARE ASSIGNED HERE AND START AT 1. Zero is the "no alias" value,
    // and [tck-id-payloads-alias-uniqueness] requires uniqueness across the
    // node's ENTIRE set of metrics - including every device's - so they come
    // from one counter rather than one per device.
    uint64_t declare(const char* name, MqttSparkplugDataType type);
    uint64_t declare(const char* name, MqttSparkplugDataType type,
                     const char* units, int32_t quality = MQ_SP_QUALITY_GOOD);

    // Declared writable, so an NCMD may set it. Still published as a metric -
    // a setpoint the node does not report back is a setpoint nobody can verify.
    uint64_t declareWritable(const char* name, MqttSparkplugDataType type);

    // The typed-layer spelling, for the OpenPLC wrapper: every MqttValueType
    // has a Sparkplug datatype, so a STRUCT member's declared IEC type is
    // enough and nothing is inferred from a first sample.
    uint64_t declareTyped(const char* name, MqttValueType type);

    size_t pointCount() const { return _pointCount; }
    const MqttSparkplugPoint* pointAt(size_t index) const;
    const MqttSparkplugPoint* findPoint(const char* name) const;

    // --- Devices ---

    bool addDevice(const char* deviceId);
    size_t deviceCount() const { return _deviceCount; }

    // A metric belonging to a device. Same alias space as the node's own.
    uint64_t declareFor(const char* deviceId, const char* name,
                        MqttSparkplugDataType type);
    uint64_t declareWritableFor(const char* deviceId, const char* name,
                                MqttSparkplugDataType type);

    // DBIRTH. [tck-id-message-flow-device-birth-publish-nbirth-wait] A device
    // can publish a DBIRTH only once an NBIRTH has been sent, so this is
    // deferred until the node has birthed rather than refused.
    bool deviceOnline(const char* deviceId);

    // DDEATH. [tck-id-operational-behavior-device-ddeath] Required when the
    // EDGE NODE loses contact with a device - not only when the node itself
    // dies. A DDEATH carries a sequence number, unlike an NDEATH.
    bool deviceOffline(const char* deviceId);

    bool deviceIsOnline(const char* deviceId) const;

    // --- Connecting ---

    bool connect();

    // Intentional. At 3.1.1 an NDEATH is published first
    // [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311];
    // at 5.0 the DISCONNECT carries reason 0x04 and the SERVER publishes it
    // [-disconnect-mqtt50]. The version decides, not the caller.
    void disconnect();

    void poll();

    // --- Values out ---

    bool set(const char* name, const MqttSparkplugValue& value);
    bool setFor(const char* deviceId, const char* name,
                const MqttSparkplugValue& value);

    // One NDATA per node and one DDATA per device carrying only what moved.
    // [tck-id-principles-rbe-recommended] is the report-by-exception
    // recommendation, and this is it.
    bool publishChanged();

    // Every metric, changed or not. What a recipe download wants.
    bool publishAll();

    // Republish the whole birth sequence: NBIRTH at seq 0, then every online
    // device's DBIRTH. What a Node Control/Rebirth NCMD triggers, and callable
    // directly.
    bool rebirth();

    // --- Commands in. Callback AND pollable, because R4 ---

    void onCommand(MqttSparkplugCommandFn handler);
    void onCommand(MqttSparkplugCommandContextFn handler, void* context);

    // The pollable half. Generated IEC code cannot register a lambda.
    bool commandPending(const char* name) const;
    bool take(const char* name, MqttSparkplugValue& into);

    // --- Knowing what is going on ---

    bool isConnected() const { return _client.isConnected(); }

    // True once the NBIRTH has gone. Not the same as isConnected(): a node
    // waiting on a primary host is connected and not born.
    bool isBorn() const { return _born; }

    uint8_t sequence() const { return _seq; }
    uint8_t birthDeathSequence() const { return _bdSeq; }

    const MqttStatus& status() const { return _client.status(); }
    const char* lastErrorText() const;

    // WHY A CONNECTED NODE HAS NOT BIRTHED, in an operator's words - or "" when
    // it has, or when it is not connected (lastErrorText() covers that).
    //
    // Connected-and-not-born is ordinary for a moment and a fault if it lasts,
    // and the two look identical on a panel unless somebody says which: waiting
    // for a primary host that is down, a broker that refused the subscription
    // the node needs to hear that host, or a birth certificate that would not
    // go. All three used to show as a healthy link with nothing to say.
    const char* waitingFor() const;
    MqttSparkplugError lastPayloadError() const { return _lastPayloadError; }

    // Commands refused because the metric was unknown or read-only. Counted
    // rather than dropped: a Host Application writing to the wrong name shows
    // up here and nowhere else.
    uint32_t commandsRefused() const { return _commandsRefused; }
    uint32_t commandsApplied() const { return _commandsApplied; }

    // Births and deaths published, for a function block to show.
    uint32_t birthCount() const { return _birthCount; }

    // The whole library, when this is not enough.
    MqttClient& client() { return _client; }

private:
    struct Server {
        std::string host;
        uint16_t port = 0;
    };

    struct Device {
        std::string id;
        bool online = false;
        // Whether a DBIRTH is owed once the node births. deviceOnline() before
        // the node is born sets this rather than failing.
        bool wantsBirth = false;
    };

    // --- Session wiring ---
    void beginClient();
    void applyConnectionConfig();
    bool publishBirth();
    bool publishDeviceBirth(Device& device);
    bool publishData(int16_t device, bool onlyChanged);
    bool publishNodeDeath();
    bool subscribeCommands();

    void advanceServer();
    void nextSequence();

    // --- Payload assembly ---
    bool buildPayload(MqttSparkplugPayload& payload, MqttSparkplugType type,
                      int16_t device, bool onlyChanged, bool includeBirthExtras);
    bool sendPayload(MqttSparkplugType type, const char* deviceId,
                     const MqttSparkplugPayload& payload, uint8_t qos,
                     bool retain, bool asWill);

    // --- Inbound ---
    void handleMessage(const MqttMessage& message);
    void handleCommand(const MqttSparkplugTopicParts& parts,
                       const uint8_t* payload, size_t length);
    void handleState(const uint8_t* payload, size_t length);

    int findPointIndex(const char* name, int16_t device) const;
    int findPointByAlias(uint64_t alias) const;
    int findDeviceIndex(const char* deviceId) const;

    uint64_t declareInternal(const char* name, MqttSparkplugDataType type,
                             int16_t device, bool writable,
                             const char* units, int32_t quality);

    static void messageTrampoline(void* context, const MqttMessage& message);
    static void deathTrampoline(void* context, MqttResult result,
                                uint16_t packetId);
    static void sessionTrampoline(void* context, bool sessionPresent);
    static void stateSubscribed(void* context, MqttResult result, uint16_t packetId);
    static void commandSubscribed(void* context, MqttResult result, uint16_t packetId);
    static void stateTrampoline(void* context, MqttState state,
                                MqttResult reason);

    void sessionEstablished(bool sessionPresent);
    void stateChanged(MqttState state, MqttResult reason);

    MqttClient _client;
    MqttOwnedTransport _net;
    bool _begun = false;

    MqttConnectionConfig _connection;
    MqttClientConfig _behaviour;

    std::string _group;
    std::string _node;

    Server _servers[MQ_SP_MAX_SERVERS];
    size_t _serverCount = 0;
    size_t _serverIndex = 0;

    std::string _primaryHostId;
    bool _primaryHostOnline = false;

    // The last online=true timestamp seen from the primary host.
    //
    // THIS FIELD IS THE WHOLE OF [tck-id-operational-behavior-edge-node-
    // termination-host-offline-timestamp], and it is the subtlest requirement
    // in the specification. An offline STATE is only believed if its timestamp
    // is at or after this: an OLD host death certificate can be delivered after
    // a new host session is already up, and a node that believed it would tear
    // itself down every time the host reconnected.
    uint64_t _primaryHostOnlineAt = 0;
    bool _sawPrimaryHostOnline = false;

    // The last STATE timestamp accepted as current, of EITHER kind.
    //
    // A SEPARATE FIELD FROM _primaryHostOnlineAt ABOVE, and the difference is
    // not an oversight. The specification states two comparisons and they have
    // different right-hand sides:
    //
    //   [tck-id-message-flow-edge-node-birth-publish-phid-wait-timestamp]
    //   deciding the host IS online compares against "the previous STATE
    //   message timestamp value" - any STATE message, online or offline.
    //
    //   [tck-id-operational-behavior-edge-node-termination-host-offline]
    //   deciding to leave the session compares against "the previous ONLINE
    //   STATE message timestamp value" - only an online one.
    //
    // One field cannot serve both. This is the first; _primaryHostOnlineAt is
    // the second.
    uint64_t _lastStateAt = 0;
    bool _sawState = false;

    MqttSparkplugPoint _points[MQ_SP_MAX_METRICS];
    size_t _pointCount = 0;
    uint64_t _nextAlias = 1;

    Device _devices[MQ_SP_MAX_DEVICES];
    size_t _deviceCount = 0;

    // [tck-id-topics-nbirth-bdseq-increment] The bdSeq number MUST start at
    // zero and increment by one on each new MQTT CONNECT.
    uint8_t _bdSeq = 0;
    bool _bdSeqUsed = false;

    // [tck-id-topics-nbirth-seq-num] 0 in the NBIRTH, then +1 per message.
    uint8_t _seq = 0;

    bool _born = false;
    bool _aliasesEnabled = true;

    // OWED, AND RETRIED - not attempted once and forgotten.
    //
    // A birth that failed (queue full, a payload too large) or a subscription
    // the client refused locally used to be tried exactly once, at the moment
    // that triggered it. The node then sat connected and unborn for the life of
    // the session: a further online STATE changed nothing because the host was
    // already "online", and nothing else would ever try again. poll() now
    // retries both, no more than once a second.
    bool _birthOwed = false;
    bool _subscriptionsOwed = false;
    mq_time_t _owedTriedAt = 0;

    // A SUBACK refusal is the broker's answer, not ours, and retrying it would
    // only be refused again - so it is recorded for waitingFor() to report.
    // Topic-specific because "the broker refused the STATE subscription" sends
    // an engineer to the ACL; "something failed" sends them nowhere.
    // WHAT THE PRIMARY HOST HAS SAID, for waitingFor() to report. "Waiting
    // for the host" covers three different faults - its STATE never reached
    // us, it says offline, or what arrived was older than what we had - and
    // each one sends an engineer somewhere different.
    uint32_t _stateMessages = 0;
    uint32_t _stateStale = 0;
    bool _lastStateOnline = false;

    MqttResult _stateSubscribeRefused = MqttResult::Ok;
    MqttResult _commandSubscribeRefused = MqttResult::Ok;
    mutable char _waitText[160];

    // A 3.1.1 disconnect is TWO steps and they must not be reordered.
    //
    // [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311] says
    // the NDEATH must reach the server BEFORE the DISCONNECT packet, because a
    // DISCONNECT tells the server not to publish the Will. publishSeries()
    // QUEUES, so sending the DISCONNECT on the next line discarded the death
    // certificate - a conformance failure that looked like a clean shutdown.
    // So the publish carries a completion handler and the DISCONNECT goes when
    // the PUBACK arrives.
    bool _disconnectAfterDeath = false;

    // When the death certificate was asked for, so the deferred disconnect
    // above it has a bound. disconnect() has to mean disconnect even if the
    // acknowledgement never arrives.
    mq_time_t _deathAskedAt = 0;

    // A valid offline STATE means DISCONNECT AND GO SOMEWHERE ELSE, and the
    // second half needs driving.
    //
    // [tck-id-operational-behavior-edge-node-termination-host-offline-reconnect]
    // "it MUST attempt to connect to the NEXT MQTT Server in its connection
    // list to start the session establishment procedure over again."
    //
    // The client's own autoReconnect does not cover this: our disconnect is
    // INTENTIONAL, and an intentional disconnect concludes as Cancelled, which
    // the reconnect path deliberately does not retry - otherwise disconnect()
    // would not mean disconnect. So the node reconnects itself, and this flag
    // is how poll() knows to.
    //
    // Found by the Eclipse TCK's PrimaryHostTest, which watched the node go
    // offline and never come back.
    bool _reconnectAfterOffline = false;

    // A REBIRTH MEANS STOP TALKING FIRST.
    //
    // [tck-id-operational-behavior-data-commands-rebirth-action-1] "When an
    // Edge Node receives a Rebirth Request, it MUST IMMEDIATELY STOP SENDING
    // DATA MESSAGES." Only then -action-2: "it MUST send a complete BIRTH
    // sequence".
    //
    // Which is not automatic, because publishSeries() QUEUES. A data message
    // accepted a moment before the request arrives is already in the send
    // queue and drains after the NBIRTH, so the far end sees data between the
    // request and the birth. The Eclipse TCK's ReceiveCommandTest is what
    // noticed.
    //
    // So: this goes true the instant a rebirth request is seen, and no data is
    // accepted until the birth sequence has been acknowledged as sent. It
    // cannot retract what is already queued - nothing can - but it stops the
    // node adding to it, which is what "immediately stop" means for a layer
    // that does not own the queue.
    bool _rebirthPending = false;

    // The wall clock. `_epochAtBoot` is UTC milliseconds at the moment
    // mq_millis() read `_epochSetAt`, so now() is a subtraction away.
    uint64_t _epochAtBoot = 0;
    mq_time_t _epochSetAt = 0;
    bool _epochSet = false;

    // UTC milliseconds now, for a payload timestamp.
    uint64_t utcNow() const;

    MqttSparkplugError _lastPayloadError = MqttSparkplugError::None;
    uint32_t _commandsRefused = 0;
    uint32_t _commandsApplied = 0;
    uint32_t _birthCount = 0;

    MqttSparkplugCommandFn _commandHandler = nullptr;
    MqttSparkplugCommandContextFn _commandContextHandler = nullptr;
    void* _commandContext = nullptr;

    // The Will bytes have to survive from before the CONNECT until the server
    // uses them, so they are held here rather than built on the stack.
    std::vector<uint8_t> _willBytes;

    // One scratch buffer for outbound payloads. Sized for a full NBIRTH with
    // properties on every metric - about 120 bytes each, so 4 kB at a
    // board's 34 metrics; anything larger is refused by name.
    static const size_t kScratch = MQ_SP_MAX_METRICS * 120 > 4096 ? MQ_SP_MAX_METRICS * 120 : 4096;
    uint8_t _scratch[kScratch];

    // One decode payload, reused. A member rather than a stack local because it
    // carries its own name arena and is several kilobytes.
    MqttSparkplugPayload _inbound;

    // Assembled outbound payload, same reasoning.
    MqttSparkplugPayload _outbound;

    char _topic[MQ_SP_MAX_TOPIC];
};

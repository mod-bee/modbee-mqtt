// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// A SPARKPLUG HOST APPLICATION
// =============================================================================
//
// The other end. 55 of the specification's 295 statements are host-side, and
// they are a different job from the edge node's 233: a host announces itself on
// spBv1.0/STATE/{host_id} with JSON rather than protobuf, subscribes to a group
// or the whole namespace, tracks every node's sequence numbers, and sends
// commands.
//
// OFF BY DEFAULT, in the sense that nothing constructs one unless you do. There
// is no build flag - CMakeLists.txt:25-36 forbids feature options in this
// library, and the linker already does the job: a program that never
// instantiates MqttSparkplugHost does not link it.
//
// THREE THINGS HERE ARE THE OPPOSITE OF THE EDGE NODE and each one is a real
// conformance bug if copied across:
//
//   1. THE WILL IS RETAINED. [tck-id-payloads-state-will-message-retain] says
//      the Host Application MUST set the Will Retained flag to TRUE. An edge
//      node's Will must be retain FALSE
//      [tck-id-payloads-ndeath-will-message-retain]. Same field, opposite
//      requirement, because a host's state must be discoverable by a node that
//      connects later and a node's death must not linger.
//   2. THE BIRTH TIMESTAMP IS CHOSEN BEFORE CONNECTING.
//      [tck-id-payloads-state-birth-payload] The value MUST match the timestamp
//      value that was used in the immediately prior MQTT CONNECT packet Will
//      Message payload. So the number is picked once, put in the Will, and used
//      again in the birth. An implementation that called the clock twice is
//      non-conformant and would look fine in every test that did not compare
//      the two messages.
//   3. IT SUBSCRIBES TO ITS OWN STATE TOPIC. [tck-id-payloads-state-subscribe]
//      "This allows the Sparkplug Host Application handle timing issues around
//      STATE offline messages being published on it's behalf by the MQTT Server
//      when it is in fact online."

#include "MqttSparkplugPayload.h"
#include "MqttSparkplugTopic.h"

#include "../MqttClient.h"
#include "../MqttFeatures.h"
#include "../MqttOwnedTransport.h"

#include <string>
#include <type_traits>

// HOW MANY NODES THIS HOST TRACKS, AND METRICS PER NODE: a limit each, set at
// run time (nodeLimit(), metricLimit()), and memory. Both tables grow with the
// births that arrive, until a limit or memory refuses the next entry - counted
// (nodesRefused(), metricsRefused(), outOfMemory()), never fatal, and the
// message itself still reaches the application.
//
// THEY WERE LIMITED BY MEMORY ALONE, and that is a limit a peer chooses. A
// node sending DBIRTHs with new aliases, or a client publishing NBIRTHs under
// new node ids, grew them until the process died; and every message searched
// them in a line, so a birth of sixteen thousand aliases cost sixteen thousand
// times sixteen thousand comparisons. Now:
//
//   - On a host (MQ_SP_HOST_INDEX, on unless MQTT_LEAN) each table has a hash
//     index - a node by group and id, a metric by alias and by name - so a
//     message costs one lookup per metric whatever the plant's size. The
//     index is four bytes a slot at half load: about sixteen bytes a metric.
//   - On a board the tables are searched in a line, which at a board's size
//     is the fastest thing and costs no flash or RAM, and the limits keep that
//     line short: MQ_SP_HOST_NODE_LIMIT nodes, MQ_SP_HOST_METRIC_LIMIT metrics
//     a node.
//   - A DBIRTH REPLACES ITS DEVICE'S entries rather than adding to them: a
//     birth is a new picture of the device, and the old one's metrics stop
//     resolving. A device that rebirthed with new aliases each time grew the
//     table without end.
//
// Metrics are remembered per tracked node, so alias-only data can be read
// back as names.
//
// WHY A HOST CANNOT SKIP THIS. A conformant Edge Node puts names ONLY in its
// birth certificates - [tck-id-payloads-alias-data-cmd-requirement] forbids
// them in NDATA and NCMD, which is most of the bandwidth saving. So a host with
// no alias table receives a stream of numbers and cannot say what any of them
// measures. The birth is the dictionary and this is where it is kept.
//
// One table per NODE, not per device: [tck-id-payloads-alias-uniqueness] makes
// an alias unique across "this Edge Node's entire set of metrics", devices
// included, so a DBIRTH adds to the same table rather than needing its own.
#ifndef MQ_SP_HOST_INDEX
    #if MQTT_LEAN
        #define MQ_SP_HOST_INDEX 0
    #else
        #define MQ_SP_HOST_INDEX 1
    #endif
#endif

// The defaults for nodeLimit() and metricLimit(). A server watching a whole
// estate meets a million nodes; a board acting as a host watches a panel.
#ifndef MQ_SP_HOST_NODE_LIMIT
    #if MQTT_LEAN
        #define MQ_SP_HOST_NODE_LIMIT 32
    #else
        #define MQ_SP_HOST_NODE_LIMIT 1048576
    #endif
#endif
#ifndef MQ_SP_HOST_METRIC_LIMIT
    #if MQTT_LEAN
        #define MQ_SP_HOST_METRIC_LIMIT 1024
    #else
        #define MQ_SP_HOST_METRIC_LIMIT 65536
    #endif
#endif

// The longest broker name a host dials (one more for the terminator): a DNS
// name is at most 253 characters.
#ifndef MQ_SP_DIAL_HOST
    #if MQTT_LEAN
        #define MQ_SP_DIAL_HOST (MQTT_CONFIG_HOST + 1)
    #else
        #define MQ_SP_DIAL_HOST 256
    #endif
#endif

// A hash index of positions in a table: open addressing, four bytes a slot,
// kept at most half full, rebuilt when it would not be. Allocated with
// mq_allocLarge() and never throws; without the memory it is simply absent
// and the owner searches in a line. Only the host build's tables use it.
class MqttSparkplugHashIndex {
public:
    MqttSparkplugHashIndex() noexcept = default;
    ~MqttSparkplugHashIndex() noexcept;
    MqttSparkplugHashIndex(const MqttSparkplugHashIndex&) = delete;
    MqttSparkplugHashIndex& operator=(const MqttSparkplugHashIndex&) = delete;
    MqttSparkplugHashIndex(MqttSparkplugHashIndex&& other) noexcept { swap(other); }
    MqttSparkplugHashIndex& operator=(MqttSparkplugHashIndex&& other) noexcept {
        swap(other);
        return *this;
    }

    bool usable() const { return _slots != 0; }

    // The position whose entry `matches`, starting at `hash`; -1 for none.
    template <typename Match>
    long find(uint32_t hash, Match matches) const {
        if (_slots == 0) return -1;
        const size_t mask = _slots - 1;
        for (size_t slot = hash & mask;; slot = (slot + 1) & mask) {
            const uint32_t entry = _table[slot];
            if (entry == 0) return -1;
            if (matches((size_t)(entry - 1))) return (long)(entry - 1);
        }
    }

    // Adds a position. False when the index would pass half full: the owner
    // then rebuilds it (rebuild()) for the larger table.
    bool insert(uint32_t hash, size_t position);

    // Empty, with room for `count` positions at half load; each of the first
    // `count` positions inserted at `hashOf(position)`. False without the
    // memory, and the index is then absent.
    template <typename HashOf>
    bool rebuild(size_t count, HashOf hashOf) {
        if (!allocate(count)) return false;
        for (size_t position = 0; position < count; position++) {
            insert(hashOf(position), position);
        }
        return true;
    }

    void release() noexcept;

private:
    bool allocate(size_t count);
    void swap(MqttSparkplugHashIndex& other) noexcept {
        uint32_t* table = _table; _table = other._table; other._table = table;
        size_t slots = _slots; _slots = other._slots; other._slots = slots;
        size_t used = _used; _used = other._used; other._used = used;
    }
    uint32_t* _table = nullptr;
    size_t _slots = 0;
    size_t _used = 0;
};

// What a host knows about one edge node.
// One metric's entry in a tracked node's dictionary, learnt from a birth and
// kept current by the data that follows.
struct MqttSparkplugTrackedMetric {
    std::string name;
    uint64_t alias = 0;
    MqttSparkplugDataType type = MqttSparkplugDataType::Unknown;

    // Which device it belongs to, or empty for a node-level metric. Kept
    // because two devices on one node may well both have a metric called
    // "flow", and telling a caller which one it is looking at is the whole
    // value of the dictionary.
    std::string device;

    // --- THE CURRENT PICTURE, which is what a Host Application is for ---
    //
    // A host that tracks which nodes exist but not what they are reading is
    // half a host: every caller would have to keep its own copy from the
    // message callback, and each would get it subtly differently. The birth
    // seeds it - [tck-id-payloads-nbirth-metric-reqs] requires the NBIRTH to
    // carry every metric's value - and each NDATA updates it.
    //
    // `at` is the payload's own timestamp, not our clock: the reading happened
    // when the node says it happened, and on a slow link those differ by more
    // than a control decision should ignore. Whether that is too old to act on
    // is the caller's judgement, so both are offered rather than a `fresh`
    // flag with a threshold baked in.
    MqttSparkplugValue value;
    bool hasValue = false;
    uint64_t at = 0;

    // STALE: the last value is still here, and no longer current. Section
    // 12.47 (Host Application session termination): after an NDEATH a host
    // MUST mark every metric of the NBIRTH, and every device's, as STALE
    // "using the current Host Application's system UTC time"; after a DDEATH
    // the device's metrics "using the timestamp in the DDEATH payload". Only
    // a birth makes a metric current again - data from a node or device that
    // died without a new birth is not trusted to say otherwise.
    bool stale = false;
    uint64_t staleAt = 0;

    // A span value's bytes - text, bytes, an array, a dataset, a template -
    // and `value` points here. It pointed into the host's decode buffer, which
    // the next message reuses, so read() of a String could return another
    // message's bytes.
    MqttSparkplugBytes owned;
};

struct MqttSparkplugTrackedNode {
    std::string group;
    std::string node;

    // The dictionary, rebuilt from each NBIRTH; each DBIRTH replaces its own
    // device's part of it.
    MqttGrowList<MqttSparkplugTrackedMetric> metrics;
    size_t metricCount = 0;

    // Metrics a birth declared that the table could not take: past
    // metricLimit(), or out of memory. Their data arrives unnamed.
    uint32_t metricsRefused = 0;

    // The name this alias was born with, or nullptr if this node never
    // declared it. Nullptr is the honest answer: a host that invented a name
    // for an alias it never saw declared would be guessing.
    const char* metricName(uint64_t alias) const;

    // What the birth said this metric IS. Not inferred from the first sample,
    // which is the defect Sparkplug exists to remove.
    MqttSparkplugDataType metricType(uint64_t alias) const;

    // The dictionary entry for a metric by name, or nullptr. `device` is the
    // device id for a device metric and nullptr for a node-level one - the
    // same name can exist at both levels and they are different metrics.
    const MqttSparkplugTrackedMetric* find(const char* name,
                                           const char* device = nullptr) const;

    bool online = false;
    // When the node was marked offline by its NDEATH: the host's UTC time
    // ([tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-offline]).
    uint64_t offlineAt = 0;

    // Whether a device of this node is online: born (it has metrics from a
    // DBIRTH) and neither it nor its node has died since. False for a device
    // this node never birthed.
    bool deviceOnline(const char* device) const;

    // [tck-id-topics-nbirth-bdseq-matching] correlates a death with its birth.
    // The timestamp explicitly must NOT be used for this
    // [tck-id-payloads-ndeath-bdseq], so this is the only field that can.
    uint64_t bdSeq = 0;
    bool hasBdSeq = false;

    // The last sequence number seen. A gap means a message was lost, and
    // [tck-id-operational-behavior-host-reordering-rebirth] says what to do
    // about it.
    uint8_t lastSeq = 0;
    bool hasSeq = false;

    // When the reorder window opened, or 0 if it is not open.
    mq_time_t reorderStartedAt = 0;
    bool reordering = false;

    uint32_t messages = 0;
    uint32_t gaps = 0;
    uint32_t rebirthsRequested = 0;

    // Rebirth requests this host would have sent and did not, because one had
    // gone to this node within rebirthHoldoff(). A lossy link otherwise asks
    // for a birth on every gap - and every birth is the largest message the
    // node sends, on the link that is already losing messages.
    uint32_t rebirthsHeld = 0;
    // Messages whose sequence number had already been seen: a duplicate.
    uint32_t duplicates = 0;

    // --- Kept by the host; not for reading ---

    // The next sequence number expected, and which of the 255 after it have
    // already arrived. The window that tells a message that is merely LATE
    // from one that is lost: a gap opens the reorder window, the missing
    // message closes it, and a duplicate is recognised rather than counted
    // as a gap of 255. [tck-id-operational-behavior-host-reordering-start],
    // [-reordering-success]
    uint8_t expectedSeq = 0;
    uint32_t seqAhead[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    // The numbers that ARRIVED in the last 128: a message whose number is here
    // is a duplicate; any other that is not the next is a gap, however far
    // ahead (or behind) it looks - 256 numbers wrap, and a link that lost 128
    // in a row must still be noticed.
    uint32_t seqSeen[8] = {0, 0, 0, 0, 0, 0, 0, 0};

    // The last automatic rebirth request, for rebirthHoldoff().
    mq_time_t rebirthAskedAt = 0;
    bool rebirthAsked = false;
    // On the host's list of open reorder windows, which poll() walks instead
    // of every node: a million nodes walked on every poll is its own stall.
    bool reorderListed = false;

#if MQ_SP_HOST_INDEX
    MqttSparkplugHashIndex byAlias;
    MqttSparkplugHashIndex byName;
#endif
};

// Every inbound Sparkplug message, after decoding.
struct MqttSparkplugInbound {
    MqttSparkplugType type = MqttSparkplugType::NData;
    const char* group = nullptr;
    const char* node = nullptr;
    // Null for a node-level message.
    const char* device = nullptr;
    const MqttSparkplugPayload* payload = nullptr;
};

typedef void (*MqttSparkplugInboundFn)(const MqttSparkplugInbound& message);
typedef void (*MqttSparkplugInboundContextFn)(void* context,
                                              const MqttSparkplugInbound& message);

class MqttSparkplugHost : private MqttClientHook {
public:
    // --- Construction: the network, three ways, as MqttSparkplugNode ---

    // On a board: the sketch's network channel. Yours; must outlive this.
    template <typename ChannelType,
              typename = typename std::enable_if<
                  !std::is_base_of<MqttTransport, ChannelType>::value>::type>
    explicit MqttSparkplugHost(ChannelType& channel) : MqttSparkplugHost() {
        _net.adopt(channel);
    }

    // On a host: plain TCP, or TLS after useTls().
    MqttSparkplugHost() MQ_NOEXCEPT;

    // A transport you built and configured. Never reconfigured here.
    explicit MqttSparkplugHost(MqttTransport& transport) MQ_NOEXCEPT;

    // On a client somebody else owns, as MqttSparkplugNode takes one: the
    // owner keeps the connection - server(), credentials(), keepAlive(),
    // only() and the TLS calls do nothing here, and poll() does not poll the
    // client. The host takes the Will (its offline STATE, claimed - refused
    // if another layer owns it), Clean Start and no session expiry on every
    // CONNECT. connect() restarts a session opened without the Will;
    // disconnect() publishes the offline STATE and leaves the connection up.
    explicit MqttSparkplugHost(MqttClient& shared) MQ_NOEXCEPT;
    ~MqttSparkplugHost();

    MqttSparkplugHost(const MqttSparkplugHost&) = delete;
    MqttSparkplugHost& operator=(const MqttSparkplugHost&) = delete;

    // --- Identity ---
    //
    // [tck-id-operational-behavior-host-application-host-id] The
    // sparkplug_host_id MUST be unique to all other Sparkplug Host IDs in the
    // infrastructure. It is a topic element, so the reserved characters are out.
    bool hostId(const char* sparkplugHostId);
    const char* hostId() const { return _hostId.c_str(); }

    void credentials(const char* user, const char* password);
    void keepAlive(uint16_t seconds);
    void only(MqttVersion version);
    bool server(const char* host, uint16_t port = 0);

    // --- TLS, on a host, before connect() ---
    //
    // As MqttSparkplugNode: on the transport this made for itself only.
    // Port 0 in server() means 8883 once TLS is on.
#if MQTT_TLS_SOCKETS
    bool useTls(bool enabled = true);
    bool trustFile(const char* path);
    bool pinnedKey(const char* fingerprint);
#endif

    // --- What to watch ---
    //
    // [tck-id-host-topic-phid-birth-sub-required] ... MUST subscribe to its own
    // spBv1.0/STATE/sparkplug_host_id AND the appropriate spBv1.0 topic(s)
    // immediately after successfully connecting.
    //
    // No argument watches everything: spBv1.0/#. A group name narrows it to
    // spBv1.0/{group}/#, which the specification explicitly permits and which
    // a large plant wants.
    bool watch(const char* group = nullptr);

    // --- Whether to become the PRIMARY host ---
    //
    // A primary host is one that edge nodes gate their births on. Nothing in
    // the protocol marks a host as primary - the NODES are configured with its
    // id - so this only affects whether a rebirth request is sent on a
    // sequence gap: [tck-id-operational-behavior-host-reordering-rebirth]'s
    // own non-normative note says "In most cases a Primary Host Application
    // would send a Rebirth Request but a Non-Primary Host may not".
    void actAsPrimary(bool primary = true) { _primary = primary; }

    // --- The reorder window ---
    //
    // [tck-id-operational-behavior-host-reordering-param] Sparkplug Host
    // Applications SHOULD provide a configurable Reorder Timeout parameter.
    // -reordering-start opens the window on an out-of-order sequence number,
    // -reordering-success closes it if the gap fills, and -reordering-rebirth
    // sends a Node Control/Rebirth NCMD if it elapses.
    //
    // Zero disables it: a gap then asks for a rebirth immediately, which is
    // what a host on a reliable local broker wants.
    void reorderTimeout(mq_time_t milliseconds) { _reorderTimeout = milliseconds; }

    // --- How often a rebirth may be asked of one node ---
    //
    // The host asks for a rebirth when it cannot trust its picture of a node:
    // a sequence gap that did not fill, a payload it could not decode, data
    // from a node whose birth it never saw. Each of those can repeat on every
    // message of a bad link, and every answer is a full NBIRTH and DBIRTHs.
    // So one request goes to a node, and the next not before this many
    // milliseconds - the rest are counted in rebirthsHeld. Zero sends every
    // one. requestRebirth() itself, the application's own call, is never held.
    void rebirthHoldoff(mq_time_t milliseconds) { _rebirthHoldoff = milliseconds; }
    mq_time_t rebirthHoldoff() const { return _rebirthHoldoff; }

    // --- How much this host will track ---
    //
    // Nodes, and metrics a node. A birth past either is still delivered to
    // onMessage(); what it would have added is refused and counted.
    void nodeLimit(size_t nodes) { _nodeLimit = nodes; }
    size_t nodeLimit() const { return _nodeLimit; }
    void metricLimit(size_t metrics) { _metricLimit = metrics; }
    size_t metricLimit() const { return _metricLimit; }
    uint32_t nodesRefused() const { return _nodesRefused; }
    uint32_t metricsRefused() const { return _metricsRefused; }

    // --- Connecting ---
    bool connect();
    void disconnect();
    void poll();

    // --- Commanding a node ---
    //
    // An NCMD or DCMD. [tck-id-payloads-ncmd-qos] and [tck-id-payloads-dcmd-qos]
    // both fix the QoS at 0 and retain at false, so neither is a parameter.
    bool command(const char* group, const char* node, const char* name,
                 const MqttSparkplugValue& value);
    bool commandDevice(const char* group, const char* node, const char* device,
                       const char* name, const MqttSparkplugValue& value);

    // Node Control/Rebirth. What a host sends when it has missed something.
    bool requestRebirth(const char* group, const char* node);

    // --- Watching ---
    void onMessage(MqttSparkplugInboundFn handler);
    void onMessage(MqttSparkplugInboundContextFn handler, void* context);

    // --- Knowing what is going on ---
    bool isConnected() const { return _client.isConnected(); }
    bool isBorn() const { return _born; }

    size_t nodeCount() const { return _nodeCount; }
    // The tables grow with what arrives; this says one could not - a node, a
    // metric or a large birth went untracked for want of memory.
    bool outOfMemory() const { return _outOfMemory; }
    const MqttSparkplugTrackedNode* nodeAt(size_t index) const;
    const MqttSparkplugTrackedNode* findNode(const char* group,
                                             const char* node) const;

    // One metric's current value, by name, across the whole plant. Returns
    // false if the node is not tracked, the metric was never declared, or
    // nothing has carried a value for it yet - three different "no" answers
    // that all mean "do not act on this".
    bool read(const char* group, const char* node, const char* name,
              MqttSparkplugValue& into) const;
    bool readDevice(const char* group, const char* node, const char* device,
                    const char* name, MqttSparkplugValue& into) const;

    const MqttStatus& status() const { return _client.status(); }

    // Why the last thing that failed, failed.
    //
    // PARITY WITH MqttSparkplugNode, which has had this from the start. Its
    // absence here was not a deliberate asymmetry: the wrapper's host block
    // was written from the node's and simply had nothing to report, so a host
    // that lost its link said nothing at all - and an integrator debugging a
    // rejected password on the SCADA side got silence while the edge node
    // named the cause.
    const char* lastErrorText() const;
    MqttSparkplugError lastPayloadError() const { return _lastPayloadError; }
    uint32_t decodeFailures() const { return _decodeFailures; }

    MqttClient& client() { return _client; }
    bool sharesClient() const { return _shared; }
    // Another layer owns the shared client's Will, so connect() was refused.
    bool willRefused() const { return _willRefused; }
private:
    void beginClient();
    void applyConnectionConfig();
    bool connectInFlight() const;
    void retarget();
    bool publishBirth();
    // `awaited`: with the completion a disconnect waits for. A shared
    // client's host stands down without one - it may be destroyed before
    // the acknowledgement comes, and nothing waits for it.
    bool publishDeath(bool awaited = true);
    void serviceReorder();

    void handleMessage(const MqttMessage& message);

    // Learn a node's alias dictionary from a birth, and apply it to everything
    // else. See the comment on the tracked tables (top) for why a host cannot
    // do without this.
    void rememberMetrics(MqttSparkplugTrackedNode& tracked,
                         const MqttSparkplugPayload& payload,
                         const char* deviceId);
    void forgetDevice(MqttSparkplugTrackedNode& tracked, const char* deviceId);
    void compactMetrics(MqttSparkplugTrackedNode& tracked);
    void indexMetrics(MqttSparkplugTrackedNode& tracked);
    long findAlias(const MqttSparkplugTrackedNode& tracked, uint64_t alias) const;
    long findName(const MqttSparkplugTrackedNode& tracked, const char* name,
                  const char* device) const;
    void trackSequence(MqttSparkplugTrackedNode& tracked, uint8_t seq,
                       const char* group, const char* node);
    void askRebirth(MqttSparkplugTrackedNode& tracked, const char* group,
                    const char* node);
    bool decodeInbound(const uint8_t* data, size_t length);
    void resolveAliases(const MqttSparkplugTrackedNode& tracked,
                        MqttSparkplugPayload& payload, const char* deviceId) const;
    void recordValues(MqttSparkplugTrackedNode& tracked,
                      const MqttSparkplugPayload& payload,
                      const char* deviceId);
    int trackNode(const char* group, const char* node);

    static void deathTrampoline(void* context, MqttResult result,
                                uint16_t packetId);

    // --- The client's events, as a layer on it (MqttClientHook) ---
    void prepareConnect(MqttConnectionConfig& connection) override;
    void sessionEstablished(bool sessionPresent) override;
    bool message(const MqttMessage& message) override;

    // The client: the host's own, or a shared one.
    MqttClient _ownClient;
    MqttClient& _client;
    bool _shared = false;
    bool _active = false;       // connect() until disconnect()
    bool _willArmed = false;    // this session's CONNECT carried the Will
    bool _restartOwed = false;  // a shared session came up without it
    bool _willRefused = false;
    MqttOwnedTransport _net;
    bool _begun = false;

    // What server() was given: the port may still be 0, meaning "the default
    // for whether TLS is on", which is not known until connect().
    std::string _host;
    uint16_t _port = 0;
    // What the transport dials: fixed storage, see applyConnectionConfig().
    char _dialHost[MQ_SP_DIAL_HOST] = {0};
    bool _retargetOwed = false;
    bool _born = false;

    // A clean disconnect waits for the death certificate to CONCLUDE.
    //
    // publishDeath() queues; the bytes leave on a later poll(). Disconnecting
    // on the next line sent the DISCONNECT first and the certificate never went
    // at all - which is exactly the failure
    // [tck-id-host-topic-phid-death-payload-disconnect-clean] exists to
    // prevent, since a DISCONNECT tells the server not to publish the Will.
    // Every node gated on this host would have gone on publishing into nothing.
    //
    // Bounded, because an acknowledgement that never comes must not leave the
    // host half-disconnected for ever. disconnect() means disconnect.
    bool _disconnectAfterDeath = false;
    mq_time_t _deathAskedAt = 0;
    bool _primary = false;

    MqttConnectionConfig _connection;
    MqttClientConfig _behaviour;

    std::string _hostId;
    std::string _watchGroup;
    bool _watchSet = false;

    // Chosen once, before the CONNECT, and used in both the Will and the birth.
    // [tck-id-payloads-state-birth-payload] requires them to match.
    uint64_t _certificateTimestamp = 0;

    mq_time_t _reorderTimeout = 5000;

    MqttGrowList<MqttSparkplugTrackedNode> _nodes;
    bool _outOfMemory = false;
    size_t _nodeCount = 0;
#if MQ_SP_HOST_INDEX
    MqttSparkplugHashIndex _nodeIndex;
#endif
    size_t _nodeLimit = MQ_SP_HOST_NODE_LIMIT;
    size_t _metricLimit = MQ_SP_HOST_METRIC_LIMIT;
    uint32_t _nodesRefused = 0;
    uint32_t _metricsRefused = 0;
    mq_time_t _rebirthHoldoff = 5000;

    // The nodes whose reorder window is open (positions in _nodes).
    MqttGrowList<uint32_t> _reorderList;
    size_t _reorderCount = 0;

    // Messages in a row that fitted the default decode payload, once it has
    // grown past it; at kShrinkAfter it is given back.
    uint32_t _smallInARow = 0;
    static const uint32_t kShrinkAfter = 16;

    MqttSparkplugInboundFn _handler = nullptr;
    MqttSparkplugInboundContextFn _contextHandler = nullptr;
    void* _context = nullptr;

    MqttSparkplugError _lastPayloadError = MqttSparkplugError::None;
    uint32_t _decodeFailures = 0;

    std::vector<uint8_t> _willBytes;
    static const size_t kScratch = 4096;
    uint8_t _scratch[kScratch];
    char _topic[MQ_SP_MAX_TOPIC];

    MqttSparkplugPayload _inbound;
};

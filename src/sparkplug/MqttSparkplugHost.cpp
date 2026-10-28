// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugHost.h"

#include <string.h>
#include "../MqttNoExcept.h"


namespace {

// UTC milliseconds since the epoch, for a STATE certificate's timestamp.
//
// [tck-id-payloads-state-will-message-payload] requires "a numeric value
// representing the current UTC time in milliseconds since Epoch". mq_millis()
// is a monotonic counter since boot and is not that - a mistake the Eclipse
// TCK caught on the node side and which would have been the same here.
uint64_t hostUtcNow() {
    const uint64_t epoch = mq_epochMillis();
    return epoch != 0 ? epoch : (uint64_t)mq_millis();
}

}  // namespace

MqttSparkplugHost::MqttSparkplugHost() MQ_NOEXCEPT : _client(_ownClient) {
    memset(_scratch, 0, sizeof _scratch);
    _topic[0] = '\0';
}

MqttSparkplugHost::MqttSparkplugHost(MqttTransport& transport) MQ_NOEXCEPT
    : _client(_ownClient) {
    memset(_scratch, 0, sizeof _scratch);
    _topic[0] = '\0';
    _net.borrow(transport);
}

MqttSparkplugHost::MqttSparkplugHost(MqttClient& shared) MQ_NOEXCEPT
    : _client(shared), _shared(true) {
    memset(_scratch, 0, sizeof _scratch);
    _topic[0] = '\0';
}

MqttSparkplugHost::~MqttSparkplugHost() {
    if (_begun && !_shared) _client.end();
    _client.detach(*this);
}

#if MQTT_TLS_SOCKETS
bool MqttSparkplugHost::useTls(bool enabled) {
    return !_shared && !_begun && _net.useTls(enabled);
}

bool MqttSparkplugHost::trustFile(const char* path) {
    return !_shared && !_begun && _net.trustFile(path);
}

bool MqttSparkplugHost::pinnedKey(const char* fingerprint) {
    return !_shared && !_begun && _net.pin(fingerprint);
}
#endif

bool MqttSparkplugHost::hostId(const char* sparkplugHostId) {
    // The host id occupies a topic element, so the three reserved characters
    // are out even though [tck-id-payloads-state-will-message] says it "can be
    // any UTF-8 string".
    if (!mq_sparkplugValidId(sparkplugHostId)) return false;
    _hostId = sparkplugHostId;
    return true;
}

void MqttSparkplugHost::credentials(const char* user, const char* password) {
    if (user != nullptr) {
        _connection.userName = user;
        _connection.hasUserName = true;
    }
    if (password != nullptr) {
        const size_t length = strlen(password);
        _connection.password.assign((const uint8_t*)password,
                                    (const uint8_t*)password + length);
        _connection.hasPassword = true;
    }
}

void MqttSparkplugHost::keepAlive(uint16_t seconds) {
    _connection.keepAlive = seconds;
}

void MqttSparkplugHost::only(MqttVersion version) {
    _connection.version = version;
    _connection.fallbackEnabled = (version == MqttVersion::V5);
    _connection.fallbackVersion = MqttVersion::V311;
}

bool MqttSparkplugHost::connectInFlight() const {
    // The client's attempt, or the channel's dial on its task, which can outlast it.
    return (_begun && _client.state() == MqttState::Connecting) || _net.connecting();
}

// NOT WHILE A CONNECT IS READING IT: as MqttSparkplugNode::server().
bool MqttSparkplugHost::server(const char* host, uint16_t port) {
    if (host == nullptr || host[0] == '\0') return false;
    if (strlen(host) >= sizeof _dialHost) return false;
    if (connectInFlight()) return false;
    _host = host;
    _port = port;
    return true;
}

bool MqttSparkplugHost::watch(const char* group) {
    if (group == nullptr) {
        _watchGroup.clear();
        _watchSet = true;
        return true;
    }
    if (!mq_sparkplugValidId(group)) return false;
    _watchGroup = group;
    _watchSet = true;
    return true;
}

void MqttSparkplugHost::applyConnectionConfig() {
    _connection.host = _host;
    _connection.port = _port != 0 ? _port : _net.defaultPort();
    // THE TRANSPORT IS GIVEN A NAME IN STORAGE THAT DOES NOT MOVE. On a board
    // a stream transport keeps the pointer, and the client's own reconnects
    // dial it again without asking: _host is a std::string, and server()
    // assigning a longer name reallocated it, so the next reconnect read
    // freed memory. _dialHost is fixed and rewritten only here, and not while
    // a connect attempt (MqttConnectTask) may be reading it.
    if (connectInFlight()) {
        _retargetOwed = true;
    } else {
        retarget();
    }

    if (_connection.clientId.empty()) _connection.clientId = _hostId;
}

void MqttSparkplugHost::retarget() {
    _retargetOwed = false;
    const size_t length = _host.size() < sizeof _dialHost - 1 ? _host.size()
                                                               : sizeof _dialHost - 1;
    memcpy(_dialHost, _host.c_str(), length);
    _dialHost[length] = '\0';
    _net.target(_dialHost, _connection.port);
}

// EVERY CONNECT, the host's own client's or a shared one's, whoever starts it.
void MqttSparkplugHost::prepareConnect(MqttConnectionConfig& connection) {
    _willArmed = false;
    if (!_active) return;

    // [tck-id-message-flow-phid-sparkplug-clean-session-311] and -50: a Host
    // Application is as stateless as an edge node.
    connection.cleanStart = true;
    connection.sessionExpiryInterval = 0;
    // --- The Will: this host's own death certificate ---
    //
    // THE TIMESTAMP IS CHOSEN HERE AND REUSED IN THE BIRTH.
    // [tck-id-payloads-state-birth-payload] The value MUST match the timestamp
    // value that was used in the immediately prior MQTT CONNECT packet Will
    // Message payload. Reading the clock again at birth time would produce two
    // different numbers and a non-conformant host - and every test that did not
    // compare the two messages would pass.
    _certificateTimestamp = hostUtcNow();

    char json[64];
    const size_t jsonLength =
        mq_sparkplugWriteState(json, sizeof json, false, _certificateTimestamp);
    if (jsonLength == 0) return;

    if (mq_sparkplugBuildStateTopic(_topic, sizeof _topic, _hostId.c_str()) == 0) {
        return;
    }

    _willBytes.assign((const uint8_t*)json, (const uint8_t*)json + jsonLength);

    // [tck-id-payloads-state-will-message-qos] Will QoS 1.
    // [tck-id-payloads-state-will-message-retain] Will Retained TRUE.
    //
    // RETAINED, which is the OPPOSITE of an edge node's Will. A host's state
    // has to be discoverable by a node that connects later - that is the whole
    // mechanism [tck-id-operational-behavior-edge-node-birth-sequence-wait]
    // depends on - whereas a node's death certificate must not linger on the
    // broker after the node is back.
    connection.will.set(_topic, _willBytes.data(), _willBytes.size(),
                        MQ_QOS_1, true);
    connection.will.delayInterval = 0;
    _willArmed = true;
}

void MqttSparkplugHost::beginClient() {
    if (_begun || _net.get() == nullptr) return;
    _behaviour.autoReconnect = true;
    applyConnectionConfig();
    _client.begin(*_net.get(), _connection, _behaviour);
    _client.attach(*this);
    _client.claimWill(*this);
    _begun = true;
}

bool MqttSparkplugHost::connect() {
    if (_hostId.empty()) return false;
    if (_shared) {
        if (!_client.attach(*this) || !_client.claimWill(*this)) {
            _willRefused = true;
            return false;
        }
        _willRefused = false;
        _begun = true;
        _active = true;
        _born = false;
        // A session already up, or a CONNECT already sent, went without the
        // offline STATE: the next one carries it.
        if (_client.state() != MqttState::Idle &&
            _client.state() != MqttState::Reconnecting) {
            _client.restart();
        }
        return true;
    }
    if (_net.get() == nullptr) return false;
    if (_host.empty()) return false;

    _born = false;
    _active = true;
    if (!_begun) {
        beginClient();
    } else {
        // A fresh certificate timestamp for this connection, in the Will and
        // then in the birth. reconfigure() is what makes rewriting a Will
        // between connections possible at all.
        applyConnectionConfig();
        _client.reconfigure(_connection);
    }
    return mq_accepted(_client.connect());
}

void MqttSparkplugHost::disconnect() {
    if (!_begun) return;
    _active = false;
    _restartOwed = false;
    if (_shared) {
        // The connection stays up for everybody else, so the offline STATE
        // goes by hand and the next CONNECT goes without it.
        _disconnectAfterDeath = false;
        if (_willArmed && _client.isConnected()) publishDeath(false);
        _client.releaseWill(*this);
        _born = false;
        return;
    }
    // [tck-id-host-topic-phid-death-payload-disconnect-clean] If a Host
    // Application is disconnecting cleanly using MQTT DISCONNECT packet, the
    // Host Application MUST publish a Death Certificate payload BEFORE sending
    // the MQTT DISCONNECT packet.
    //
    // Same reasoning as an edge node's NDEATH at 3.1.1: a DISCONNECT tells the
    // server not to publish the Will, so without this a clean shutdown leaves
    // every node believing the host is still there - and a node gated on this
    // host would keep publishing into nothing.
    // AND THE ORDER IS THE WHOLE POINT. publishDeath() QUEUES - the bytes
    // leave on a later poll() - so disconnecting on the next line sent the
    // DISCONNECT first and the certificate never went at all. The disconnect
    // waits for the publish to CONCLUDE, which at QoS 1 means the server
    // acknowledged it rather than merely that we wrote it. Same shape as
    // MqttSparkplugNode's 3.1.1 path, and found the same way: by asking what
    // actually reached the peer instead of what the code said it did.
    _disconnectAfterDeath = true;
    _deathAskedAt = mq_millis();
    if (!publishDeath()) {
        // Nothing could be queued, so there is nothing to wait for and holding
        // the session open would be worse than a missing certificate.
        _disconnectAfterDeath = false;
        _client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
    }
    _born = false;
}

void MqttSparkplugHost::deathTrampoline(void* context, MqttResult result,
                                        uint16_t packetId) {
    (void)result;
    (void)packetId;
    MqttSparkplugHost* host = static_cast<MqttSparkplugHost*>(context);
    if (!host->_disconnectAfterDeath) return;
    host->_disconnectAfterDeath = false;
    // Whatever the result. A certificate the server refused is not a reason to
    // stay connected, and the registered Will still covers a cut link.
    host->_client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
}

void MqttSparkplugHost::poll() {
    if (!_begun) return;
    if (!_shared) _client.poll();   // a shared client's owner polls it
    if (_retargetOwed && !connectInFlight()) retarget();
    serviceReorder();

    // A shared client restarts from here, never from inside its own poll.
    if (_restartOwed) {
        _restartOwed = false;
        if (_active) _client.restart();
    }

    // The bound on the deferred disconnect above. Without it a server that
    // never acknowledges the death certificate would leave the host neither
    // connected nor disconnected - the client's own acknowledgementTimeoutMs
    // normally resolves this, but it can be set to zero.
    if (_disconnectAfterDeath &&
        mq_elapsed(_deathAskedAt, mq_millis()) > 10000) {
        _disconnectAfterDeath = false;
        _client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
    }
}

bool MqttSparkplugHost::message(const MqttMessage& message) {
    // Never taken: a node on the same client may watch the same namespace.
    handleMessage(message);
    return false;
}

void MqttSparkplugHost::sessionEstablished(bool sessionPresent) {
    (void)sessionPresent;
    if (!_active) return;   // a shared client's session; this host stood down
    if (_shared && !_willArmed) {
        // Opened before this host was there, so without its offline STATE.
        _restartOwed = true;
        return;
    }
    // [tck-id-payloads-state-subscribe] After establishing an MQTT connection,
    // the Sparkplug Host Application MUST subscribe on it's own
    // spBv1.0/STATE/[sparkplug_host_id] topic.
    //
    // Its own, first, and the reason is in the specification's non-normative
    // note: the server may publish this host's offline certificate on its
    // behalf - a retained one from a previous session - and the host needs to
    // see that happen so it can correct it.
    if (mq_sparkplugBuildStateTopic(_topic, sizeof _topic, _hostId.c_str()) > 0) {
        _client.subscribe(_topic, MQ_QOS_1);
    }

    // [tck-id-host-topic-phid-birth-sub-required] ... and the appropriate
    // spBv1.0 topic(s) immediately after successfully connecting.
    if (_watchSet) {
        size_t cursor = 0;
        const char* ns = MQ_SP_NAMESPACE;
        const size_t nsLength = strlen(ns);
        if (nsLength + 4 < sizeof _topic) {
            memcpy(_topic, ns, nsLength);
            cursor = nsLength;
            _topic[cursor++] = '/';
            if (!_watchGroup.empty()) {
                const size_t groupLength = _watchGroup.length();
                if (cursor + groupLength + 3 < sizeof _topic) {
                    memcpy(_topic + cursor, _watchGroup.c_str(), groupLength);
                    cursor += groupLength;
                    _topic[cursor++] = '/';
                }
            }
            _topic[cursor++] = '#';
            _topic[cursor] = '\0';
            // QoS 1. A host that missed an NBIRTH at QoS 0 would have no
            // alias table and would have to discard every NDATA that followed.
            _client.subscribe(_topic, MQ_QOS_1);
        }
    }

    // [tck-id-host-topic-phid-birth-message] The FIRST MQTT message a Host
    // Application MUST publish is a Birth Certificate - which is published
    // after the subscriptions above, so nothing is missed between the two.
    publishBirth();
}

bool MqttSparkplugHost::publishBirth() {
    char json[64];
    // The SAME timestamp that went into the Will.
    // [tck-id-payloads-state-birth-payload].
    const size_t jsonLength =
        mq_sparkplugWriteState(json, sizeof json, true, _certificateTimestamp);
    if (jsonLength == 0) return false;

    if (mq_sparkplugBuildStateTopic(_topic, sizeof _topic, _hostId.c_str()) == 0) {
        return false;
    }

    // [tck-id-host-topic-phid-birth-qos] QoS MUST be 1.
    // [tck-id-host-topic-phid-birth-retain] retain MUST be TRUE.
    const bool ok = mq_accepted(
        _client.publishSeries(_topic, (const uint8_t*)json, jsonLength,
                              MQ_QOS_1, true));
    if (ok) _born = true;
    return ok;
}

bool MqttSparkplugHost::publishDeath(bool awaited) {
    char json[64];
    const size_t jsonLength =
        mq_sparkplugWriteState(json, sizeof json, false, hostUtcNow());
    if (jsonLength == 0) return false;
    if (mq_sparkplugBuildStateTopic(_topic, sizeof _topic, _hostId.c_str()) == 0) {
        return false;
    }
    // [tck-id-host-topic-phid-death-qos] QoS 1.
    // [tck-id-host-topic-phid-death-retain] retain TRUE.
    // QoS 1 and a completion handler, so a disconnect that follows can wait
    // for the PUBACK. At QoS 0 there would be nothing to wait for.
    return mq_accepted(_client.publishSeries(_topic, (const uint8_t*)json,
                                             jsonLength, MQ_QOS_1, true,
                                             awaited ? &deathTrampoline : nullptr,
                                             awaited ? this : nullptr));
}

// =============================================================================
// READING THE PICTURE
// =============================================================================

const char* MqttSparkplugHost::lastErrorText() const {
    // The library's own result name, not the Sparkplug one: a connection that
    // failed failed for an MQTT reason, and lastPayloadError() is where a
    // codec fault shows up.
    return mq_resultName(_client.status().lastError);
}

bool MqttSparkplugHost::read(const char* group, const char* node,
                             const char* name,
                             MqttSparkplugValue& into) const {
    return readDevice(group, node, nullptr, name, into);
}

bool MqttSparkplugTrackedNode::deviceOnline(const char* device) const {
    if (!online || device == nullptr || *device == '\0') return false;
    bool any = false;
    for (size_t m = 0; m < metricCount; m++) {
        if (metrics[m].device != device) continue;
        if (metrics[m].stale) return false;
        any = true;
    }
    return any;
}

bool MqttSparkplugHost::readDevice(const char* group, const char* node,
                                   const char* device, const char* name,
                                   MqttSparkplugValue& into) const {
    const MqttSparkplugTrackedNode* tracked = findNode(group, node);
    if (tracked == nullptr) return false;
    const MqttSparkplugTrackedMetric* metric = tracked->find(name, device);
    if (metric == nullptr) return false;
    if (!metric->hasValue) return false;
    into = metric->value;
    return true;
}

// =============================================================================
// THE INDEXES
// =============================================================================

namespace {

// FNV-1a. A host's keys are short text and a 64-bit alias; nothing here is a
// secret, and a peer that chose names to collide would only make its own
// node's lookups walk a little further - the probe is bounded by the half
// load either way.
uint32_t fnv(uint32_t hash, const char* text) {
    if (text == nullptr) return hash;
    for (; *text != '\0'; text++) {
        hash ^= (uint8_t)*text;
        hash *= 16777619u;
    }
    return hash;
}

uint32_t nodeHash(const char* group, const char* node) {
    uint32_t hash = fnv(2166136261u, group);
    hash ^= 0xFFu;   // never part of a valid id: "a"+"bc" and "ab"+"c" differ
    hash *= 16777619u;
    return fnv(hash, node);
}

uint32_t nameHash(const char* name, const char* device) {
    uint32_t hash = fnv(2166136261u, device != nullptr ? device : "");
    hash ^= 0xFFu;
    hash *= 16777619u;
    return fnv(hash, name);
}

uint32_t aliasHash(uint64_t alias) {
    // A 64-bit mix (splitmix64's finaliser): aliases are usually 1, 2, 3...
    // and a plain truncation would put them in consecutive slots, which is
    // fine, but a node using 0x100000000, 0x200000000... would put every one
    // in slot zero.
    alias ^= alias >> 33;
    alias *= 0xff51afd7ed558ccdull;
    alias ^= alias >> 33;
    return (uint32_t)alias;
}

// The position of a tracked metric by alias, or by name and device: by the
// index on a host, in a line on a board (or where the index could not be
// built). The same answer either way.
// Where a metric sits in the alias index. One with no alias (0) is placed by
// its name, so a birth of a thousand of them does not pile into one probe
// chain; nothing looks one up by alias.
uint32_t aliasSlotHash(const MqttSparkplugTrackedMetric& metric) {
    return metric.alias != 0 ? aliasHash(metric.alias)
                             : nameHash(metric.name.c_str(), metric.device.c_str());
}

long trackedAlias(const MqttSparkplugTrackedNode& tracked, uint64_t alias) {
#if MQ_SP_HOST_INDEX
    if (tracked.byAlias.usable()) {
        return tracked.byAlias.find(aliasHash(alias), [&](size_t position) {
            return position < tracked.metricCount &&
                   tracked.metrics[position].alias == alias;
        });
    }
#endif
    for (size_t index = 0; index < tracked.metricCount; index++) {
        if (tracked.metrics[index].alias == alias) return (long)index;
    }
    return -1;
}

long trackedName(const MqttSparkplugTrackedNode& tracked, const char* name,
                 const char* device) {
    if (name == nullptr) return -1;
    // A node-level "flow" and a device's "flow" are different metrics, so
    // the device has to match too - including the empty case.
    const char* wanted = device != nullptr ? device : "";
#if MQ_SP_HOST_INDEX
    if (tracked.byName.usable()) {
        return tracked.byName.find(nameHash(name, wanted), [&](size_t position) {
            return position < tracked.metricCount &&
                   tracked.metrics[position].name == name &&
                   tracked.metrics[position].device == wanted;
        });
    }
#endif
    for (size_t index = 0; index < tracked.metricCount; index++) {
        if (tracked.metrics[index].name != name) continue;
        if (tracked.metrics[index].device != wanted) continue;
        return (long)index;
    }
    return -1;
}

}  // namespace

MqttSparkplugHashIndex::~MqttSparkplugHashIndex() noexcept { release(); }

void MqttSparkplugHashIndex::release() noexcept {
    mq_freeLarge(_table);
    _table = nullptr;
    _slots = 0;
    _used = 0;
}

bool MqttSparkplugHashIndex::allocate(size_t count) {
    size_t slots = 16;
    while (slots < count * 2) {
        if (slots > ((size_t)-1) / (2 * sizeof(uint32_t))) {
            release();
            return false;
        }
        slots *= 2;
    }
    uint32_t* table = (uint32_t*)mq_allocLarge(slots * sizeof(uint32_t));
    release();
    if (table == nullptr) return false;
    memset(table, 0, slots * sizeof(uint32_t));
    _table = table;
    _slots = slots;
    return true;
}

bool MqttSparkplugHashIndex::insert(uint32_t hash, size_t position) {
    if (_slots == 0 || (_used + 1) * 2 > _slots) return false;
    if (position >= 0xFFFFFFFFu) return false;
    const size_t mask = _slots - 1;
    size_t slot = hash & mask;
    while (_table[slot] != 0) slot = (slot + 1) & mask;
    _table[slot] = (uint32_t)(position + 1);
    _used++;
    return true;
}

// =============================================================================
// TRACKING NODES
// =============================================================================

int MqttSparkplugHost::trackNode(const char* group, const char* node) {
    const MqttSparkplugTrackedNode* known = findNode(group, node);
    if (known != nullptr) return (int)(known - &_nodes[0]);

    // A LIMIT FIRST, then memory: a client publishing births under ever new
    // node ids is refused at the limit, counted, and costs nothing more.
    if (_nodeCount >= _nodeLimit || _nodeCount >= 0x7FFFFFFFu) {
        _nodesRefused++;
        return -1;
    }
    if (!_nodes.reserve(_nodeCount + 1)) {
        _outOfMemory = true;
        return -1;
    }
    _nodes[_nodeCount].group = group;
    _nodes[_nodeCount].node = node;
    _nodeCount++;
#if MQ_SP_HOST_INDEX
    if (!_nodeIndex.insert(nodeHash(group, node), _nodeCount - 1)) {
        // Past half full: rebuilt for the larger table. Without the memory the
        // index goes and findNode() searches in a line - slower, never wrong.
        if (!_nodeIndex.rebuild(_nodeCount, [&](size_t position) {
                return nodeHash(_nodes[position].group.c_str(),
                                _nodes[position].node.c_str());
            })) {
            _outOfMemory = true;
        }
    }
#endif
    return (int)(_nodeCount - 1);
}

const MqttSparkplugTrackedNode* MqttSparkplugHost::nodeAt(size_t index) const {
    if (index >= _nodeCount) return nullptr;
    return &_nodes[index];
}

const MqttSparkplugTrackedNode*
MqttSparkplugHost::findNode(const char* group, const char* node) const {
    if (group == nullptr || node == nullptr) return nullptr;
#if MQ_SP_HOST_INDEX
    if (_nodeIndex.usable()) {
        const long at = _nodeIndex.find(nodeHash(group, node), [&](size_t position) {
            return position < _nodeCount && _nodes[position].group == group &&
                   _nodes[position].node == node;
        });
        return at >= 0 ? &_nodes[(size_t)at] : nullptr;
    }
#endif
    for (size_t index = 0; index < _nodeCount; index++) {
        if (_nodes[index].group == group && _nodes[index].node == node) {
            return &_nodes[index];
        }
    }
    return nullptr;
}

void MqttSparkplugHost::askRebirth(MqttSparkplugTrackedNode& tracked,
                                   const char* group, const char* node) {
    // A Non-Primary Host may not ask at all - the non-normative note on
    // [tck-id-operational-behavior-host-reordering-rebirth].
    if (!_primary) return;
    const mq_time_t now = mq_millis();
    if (_rebirthHoldoff != 0 && tracked.rebirthAsked &&
        mq_elapsed(tracked.rebirthAskedAt, now) < _rebirthHoldoff) {
        tracked.rebirthsHeld++;
        return;
    }
    if (requestRebirth(group, node)) {
        tracked.rebirthsRequested++;
        tracked.rebirthAsked = true;
        tracked.rebirthAskedAt = now;
    }
}

void MqttSparkplugHost::serviceReorder() {
    if (_reorderCount == 0) return;

    const mq_time_t now = mq_millis();
    for (size_t at = 0; at < _reorderCount;) {
        const size_t position = _reorderList[at];
        MqttSparkplugTrackedNode& tracked = _nodes[position];
        bool unlist = !tracked.reordering;

        // mq_isFuture handles the wrap of a 32-bit millisecond clock, which is
        // why it is used rather than a plain subtraction - the same reason
        // every other deadline in this library goes through it.
        if (!unlist && !mq_isFuture(now, tracked.reorderStartedAt + _reorderTimeout)) {
            // [tck-id-operational-behavior-host-reordering-rebirth] If ... the
            // Reorder Timeout elapses and the missing message(s) have not been
            // received, the Sparkplug Host Application MUST send an NCMD to
            // the Edge Node with a Node Control/Rebirth request.
            //
            // What did arrive is believed; the stream starts again from the
            // next message, so the gap is reported once rather than on every
            // message after it.
            tracked.reordering = false;
            tracked.hasSeq = false;
            askRebirth(tracked, tracked.group.c_str(), tracked.node.c_str());
            unlist = true;
        }
        if (unlist) {
            tracked.reorderListed = false;
            _reorderList[at] = _reorderList[_reorderCount - 1];
            _reorderCount--;
            continue;
        }
        at++;
    }
}

// --- Sequence numbers ---
//
// [tck-id-payloads-sequence-num-incrementing] one greater, wrapping at 255.
// A message that is not the next one is either AHEAD (one or more were lost,
// or are late) or BEHIND (already seen: a duplicate). The window says which.
namespace {

// Arrived: remembered, and the number 128 behind it forgotten - so "seen" is
// always the last 128 numbers, wherever the sequence has got to.
void markSeen(uint32_t* seen, uint8_t seq) {
    seen[seq >> 5] |= 1u << (seq & 31);
    const uint8_t old = (uint8_t)(seq + 128);
    seen[old >> 5] &= ~(1u << (old & 31));
}

bool wasSeen(const uint32_t* seen, uint8_t seq) { return (seen[seq >> 5] & (1u << (seq & 31))) != 0; }

} // namespace

void MqttSparkplugHost::trackSequence(MqttSparkplugTrackedNode& tracked,
                                      uint8_t seq, const char* group,
                                      const char* node) {
    tracked.lastSeq = seq;
    if (!tracked.hasSeq) {
        tracked.hasSeq = true;
        tracked.expectedSeq = (uint8_t)(seq + 1);
        for (uint32_t& word : tracked.seqAhead) word = 0;
        for (uint32_t& word : tracked.seqSeen) word = 0;
        markSeen(tracked.seqSeen, seq);
        return;
    }
    const uint8_t distance = (uint8_t)(seq - tracked.expectedSeq);
    if (distance == 0) {
        // In order: and whatever had arrived early behind it now follows.
        markSeen(tracked.seqSeen, seq);
        tracked.expectedSeq++;
        while (tracked.seqAhead[tracked.expectedSeq >> 5] &
               (1u << (tracked.expectedSeq & 31))) {
            tracked.seqAhead[tracked.expectedSeq >> 5] &=
                ~(1u << (tracked.expectedSeq & 31));
            tracked.expectedSeq++;
        }
        bool pending = false;
        for (uint32_t word : tracked.seqAhead) pending = pending || word != 0;
        // [-reordering-success] If the missing message(s) ... arrive before
        // the reordering timer elapses, the timer MUST be terminated and
        // normal operation MUST continue.
        if (!pending) tracked.reordering = false;
        return;
    }
    if (wasSeen(tracked.seqSeen, seq)) {
        // Arrived already, among the last 128: a duplicate, or one the
        // server delivered twice. Not a gap.
        tracked.duplicates++;
        return;
    }

    // Not the next, and not seen: a gap - messages lost, or a stream that
    // cannot be trusted (a number far from where the node was).
    (void)distance;
    tracked.seqAhead[seq >> 5] |= 1u << (seq & 31);
    markSeen(tracked.seqSeen, seq);
    if (tracked.reordering) return;   // the same gap, still open
    tracked.gaps++;
    if (_reorderTimeout == 0) {
        tracked.hasSeq = false;
        askRebirth(tracked, group, node);
        return;
    }
    // [tck-id-operational-behavior-host-reordering-start] ... the Host
    // Application MUST start a timer denoting the start of the Reorder
    // Timeout window.
    tracked.reordering = true;
    tracked.reorderStartedAt = mq_millis();
    if (!tracked.reorderListed) {
        if (_reorderList.reserve(_reorderCount + 1)) {
            _reorderList[_reorderCount++] = (uint32_t)(&tracked - &_nodes[0]);
            tracked.reorderListed = true;
        } else {
            // No room to time it: ask now rather than never.
            _outOfMemory = true;
            tracked.reordering = false;
            tracked.hasSeq = false;
            askRebirth(tracked, group, node);
        }
    }
}

// =============================================================================
// INBOUND
// =============================================================================

const char* MqttSparkplugTrackedNode::metricName(uint64_t alias) const {
    const long at = trackedAlias(*this, alias);
    return at >= 0 ? metrics[(size_t)at].name.c_str() : nullptr;
}

const MqttSparkplugTrackedMetric* MqttSparkplugTrackedNode::find(
    const char* name, const char* device) const {
    const long at = trackedName(*this, name, device);
    return at >= 0 ? &metrics[(size_t)at] : nullptr;
}

MqttSparkplugDataType MqttSparkplugTrackedNode::metricType(
    uint64_t alias) const {
    const long at = trackedAlias(*this, alias);
    return at >= 0 ? metrics[(size_t)at].type : MqttSparkplugDataType::Unknown;
}

void MqttSparkplugHost::indexMetrics(MqttSparkplugTrackedNode& tracked) {
#if MQ_SP_HOST_INDEX
    const bool aliases = tracked.byAlias.rebuild(tracked.metricCount, [&](size_t at) {
        return aliasSlotHash(tracked.metrics[at]);
    });
    const bool names = tracked.byName.rebuild(tracked.metricCount, [&](size_t at) {
        return nameHash(tracked.metrics[at].name.c_str(),
                        tracked.metrics[at].device.c_str());
    });
    if (!aliases || !names) _outOfMemory = true;
#else
    (void)tracked;
#endif
}

// A node that births smaller than it was gives the difference back: once its
// table is four times what it holds (and worth the trouble), the entries move
// to one of the right size. Once a birth, never on a data message - and the
// same rule as the decode payload's: a table that only grows is a table a
// peer can grow.
void MqttSparkplugHost::compactMetrics(MqttSparkplugTrackedNode& tracked) {
    const size_t capacity = tracked.metrics.capacity();
    if (capacity <= 64 || tracked.metricCount >= capacity / 4) return;
    MqttGrowList<MqttSparkplugTrackedMetric> sized;
    if (!sized.reserveExactly(tracked.metricCount < 4 ? 4 : tracked.metricCount)) {
        return;   // keeps the larger table: correct, only not given back
    }
    for (size_t index = 0; index < tracked.metricCount; index++) {
        sized[index] = static_cast<MqttSparkplugTrackedMetric&&>(tracked.metrics[index]);
    }
    tracked.metrics = static_cast<MqttGrowList<MqttSparkplugTrackedMetric>&&>(sized);
}

void MqttSparkplugHost::forgetDevice(MqttSparkplugTrackedNode& tracked,
                                     const char* deviceId) {
    // The device's entries go, the rest close up in order, and the index is
    // built again. Once a birth, never on a data message.
    const char* wanted = deviceId != nullptr ? deviceId : "";
    size_t kept = 0;
    for (size_t index = 0; index < tracked.metricCount; index++) {
        if (tracked.metrics[index].device == wanted) continue;
        if (kept != index) {
            tracked.metrics[kept] =
                static_cast<MqttSparkplugTrackedMetric&&>(tracked.metrics[index]);
        }
        kept++;
    }
    if (kept == tracked.metricCount) return;
    tracked.metricCount = kept;
    indexMetrics(tracked);
}

void MqttSparkplugHost::rememberMetrics(MqttSparkplugTrackedNode& tracked,
                                       const MqttSparkplugPayload& payload,
                                       const char* deviceId) {
    const char* device = deviceId != nullptr ? deviceId : "";
    for (size_t index = 0; index < payload.metricCount; index++) {
        const MqttSparkplugMetric& metric = payload.metrics[index];
        // A BIRTH METRIC WITHOUT AN ALIAS IS STILL A METRIC. Aliases are
        // optional - [tck-id-payloads-alias-birth-requirement] says only that
        // a birth that uses one carries the name too - and Eclipse Tahu's
        // JavaScript client never uses them, nor does its Java one unless
        // told to. These were skipped, so such a node's metrics could not be
        // read, and a write to one (Sparkplug.Write) was refused for want of
        // a datatype: the Eclipse TCK's own edge node births
        // TCK_metric/Int32 without one.
        if (metric.name == nullptr) continue;

        // An alias declared twice replaces the earlier entry rather than
        // adding a second. [tck-id-payloads-alias-uniqueness] says it cannot
        // happen on a conformant node, but a host is reading someone else's
        // traffic and two entries for one alias would make which name wins
        // depend on search order.
        long slot = metric.hasAlias ? trackedAlias(tracked, metric.alias)
                                    : trackedName(tracked, metric.name, deviceId);
        bool added = false;

        // A LIMIT, OUT OF MEMORY: said (metricsRefused(), outOfMemory()), not
        // fatal. The table is a convenience for reading data; refusing the
        // birth over it would lose the node entirely, and an unresolved alias
        // already has an honest answer - metricName() returns nullptr and the
        // caller sees a number it cannot name.
        if (slot < 0) {
            if (tracked.metricCount >= _metricLimit ||
                tracked.metricCount >= 0x7FFFFFFFu) {
                tracked.metricsRefused++;
                _metricsRefused++;
                continue;
            }
            if (!tracked.metrics.reserve(tracked.metricCount + 1)) {
                _outOfMemory = true;
                tracked.metricsRefused++;
                _metricsRefused++;
                continue;
            }
            slot = (long)tracked.metricCount++;
            added = true;
            MqttSparkplugTrackedMetric& fresh = tracked.metrics[(size_t)slot];
            fresh.hasValue = false;
            fresh.at = 0;
            fresh.value = MqttSparkplugValue();
        }

        MqttSparkplugTrackedMetric& entry = tracked.metrics[(size_t)slot];
        // Born: current again, whatever the slot held before.
        entry.stale = false;
        entry.staleAt = 0;
        const bool renamed = entry.name != metric.name || entry.device != device;
        entry.name = metric.name;
        entry.alias = metric.hasAlias ? metric.alias : 0;
        entry.hasAlias = metric.hasAlias;
        entry.type = metric.value.type;
        entry.device = device;
#if MQ_SP_HOST_INDEX
        // A new entry goes into both indexes; a redeclared alias under a new
        // name into the name index (the old name's slot no longer matches).
        // Either may find the index full, and it is then built again.
        bool ok = true;
        if (added) ok = tracked.byAlias.insert(aliasSlotHash(entry), (size_t)slot);
        if (ok && (added || renamed)) {
            ok = tracked.byName.insert(nameHash(metric.name, device), (size_t)slot);
        }
        if (!ok) indexMetrics(tracked);
#else
        (void)renamed;
        (void)added;
#endif
    }
}

void MqttSparkplugHost::recordValues(MqttSparkplugTrackedNode& tracked,
                                    const MqttSparkplugPayload& payload,
                                    const char* deviceId) {
    // Called AFTER the names have been filled in, so a data message carrying
    // nothing but an alias still lands in the right entry.
    for (size_t index = 0; index < payload.metricCount; index++) {
        const MqttSparkplugMetric& metric = payload.metrics[index];

        // Matched by alias first, because that is what a conformant node
        // sends and it cannot be ambiguous. The name is the fallback for a
        // node that sends names, which the specification permits.
        const long slot = metric.hasAlias
                              ? trackedAlias(tracked, metric.alias)
                              : trackedName(tracked, metric.name, deviceId);
        if (slot < 0) continue;   // never declared; nothing to update

        // A historical value is history, not the current picture: the
        // specification's flag says "this message should not be considered a
        // real time/current value", and a replayed hour must not overwrite a
        // live reading with an older one.
        if (metric.isHistorical) continue;

        MqttSparkplugTrackedMetric& entry = tracked.metrics[(size_t)slot];

        // A NULL metric is a positive statement that the value is not known,
        // so the entry stops offering a stale reading rather than keeping the
        // last good one.
        if (metric.value.isNull) {
            entry.hasValue = false;
            continue;
        }

        // Owned: `metric` points into _inbound's buffer, reused next message.
        entry.value = entry.owned.keep(metric.value);
        entry.hasValue = !entry.value.isNull;
        if (!entry.hasValue) {
            _outOfMemory = true;
            continue;
        }
        // The metric's own timestamp if it carried one, otherwise the
        // payload's - which is the usual case, since one payload timestamp
        // covering the batch is most of the bandwidth saving.
        entry.at = metric.hasTimestamp ? metric.timestamp
                                       : (payload.hasTimestamp ? payload.timestamp : 0);
    }
}

void MqttSparkplugHost::resolveAliases(
    const MqttSparkplugTrackedNode& tracked,
    MqttSparkplugPayload& payload, const char* deviceId) const {
    for (size_t index = 0; index < payload.metricCount; index++) {
        MqttSparkplugMetric& metric = payload.metrics[index];

        // A name that arrived wins. It is what the publisher actually said,
        // and overwriting it with our own record would hide a disagreement
        // rather than let the caller see it.
        //
        // Pointed at the dictionary's own storage, which outlives the callback
        // - the tracked node is a member of this host. Nothing is copied into
        // the payload, which is the same contract MqttSparkplugMetric::name
        // already has: the caller does not own it.
        long at = -1;
        if (metric.hasAlias) {
            at = trackedAlias(tracked, metric.alias);
            if (metric.name == nullptr && at >= 0) {
                metric.name = tracked.metrics[(size_t)at].name.c_str();
            }
        }

        // [tck-id-payloads-metric-datatype-not-req] Data SHOULD NOT carry a
        // datatype, so the birth's is applied - by alias, or by name.
        if (metric.datatypeAbsent) {
            if (!metric.hasAlias) at = trackedName(tracked, metric.name, deviceId);
            if (at >= 0) {
                const MqttSparkplugDataType declared = tracked.metrics[(size_t)at].type;
                if (declared != MqttSparkplugDataType::Unknown) {
                    mq_sparkplugApplyDatatype(metric, declared);
                }
            }
        }
    }
}

bool MqttSparkplugHost::decodeInbound(const uint8_t* data, size_t length) {
    // MEASURED, THEN DECODED ONCE, into a payload sized for this message.
    //
    // It decoded into whatever it had, and on TooManyMetrics doubled and tried
    // again - doubling the name arena too, when the real cause was a metric
    // with more properties than any payload holds, until the arena passed
    // the message's size. And it never gave anything back: one large birth
    // left about 208 bytes held for every byte that birth was, for the life
    // of the host.
    size_t metrics = 0;
    size_t names = 0;
    MqttSparkplugError error = mq_sparkplugMeasure(data, length, metrics, names);
    if (error == MqttSparkplugError::None) {
        // Past what this host would track of any node is refused here, before
        // anything is allocated for it - a megabyte of empty metrics is half
        // a million of them.
        const size_t most =
            (_metricLimit > MQ_SP_MAX_METRICS ? _metricLimit : MQ_SP_MAX_METRICS) + 64;
        const bool small = metrics <= MQ_SP_MAX_METRICS && names <= MQ_SP_NAME_BYTES;
        const bool grown = _inbound.capacity() > MQ_SP_MAX_METRICS ||
                           _inbound.nameCapacity() > MQ_SP_NAME_BYTES;
        if (grown && small) {
            // Given back once the large messages have stopped, not after the
            // first small one: a node whose births are large and whose data is
            // small would otherwise reallocate on every birth.
            if (++_smallInARow >= kShrinkAfter) {
                _smallInARow = 0;
                _inbound.resize(MQ_SP_MAX_METRICS, MQ_SP_NAME_BYTES);
            }
        } else if (!small) {
            _smallInARow = 0;
        }
        if (metrics > most) {
            error = MqttSparkplugError::TooManyMetrics;
        } else if ((metrics > _inbound.capacity() ||
                    names > _inbound.nameCapacity()) &&
                   !_inbound.reserve(metrics, names)) {
            _outOfMemory = true;
            error = MqttSparkplugError::TooManyMetrics;
        }
    }
    if (error == MqttSparkplugError::None) {
        error = mq_sparkplugDecode(data, length, _inbound);
    } else {
        _inbound.clear();
    }
    if (error != MqttSparkplugError::None) {
        _lastPayloadError = error;
        _decodeFailures++;
        return false;
    }
    return true;
}

void MqttSparkplugHost::handleMessage(const MqttMessage& message) {
    MqttSparkplugTopicParts parts;
    if (!mq_sparkplugParseTopic((const char*)message.topic.data,
                                message.topic.length, parts)) {
        return;
    }

    if (parts.isState()) {
        // Our own certificate, coming back. The specification's reason for
        // subscribing: if the server published our offline on our behalf while
        // we are in fact online, we correct it by birthing again.
        if (_hostId != parts.group) return;

        bool online = false;
        uint64_t timestamp = 0;
        if (!mq_sparkplugParseState(message.payload.data,
                                    message.payload.length, online,
                                    timestamp)) {
            return;
        }
        if (!online && _client.isConnected()) {
            // A stale death certificate from a previous session of ours, or one
            // the server delivered late. We are here, so we say so again.
            publishBirth();
        }
        return;
    }

    // Commands are ours to send, not to receive. A host that acted on an NCMD
    // would be acting on its own traffic.
    if (mq_sparkplugIsCommand(parts.type)) return;

    if (!decodeInbound(message.payload.data, message.payload.length)) {
        // [tck-id-operational-behavior-host-reordering-rebirth]'s sibling
        // guidance: a Host Application MAY send Node Control/Rebirth NCMD
        // messages if malformed payloads arrive. Being primary is the
        // condition, per that statement's own non-normative note - and the
        // node must be one this host already tracks: a malformed message is
        // not a reason to start tracking a node id, and a peer spraying
        // rubbish under new ids would otherwise fill the table with them.
        //
        // NOT for a payload past this host's own limits. That is ours, not
        // the node's, and a rebirth would only send the same birth again -
        // a request answered by the message that caused it, for ever.
        if (_lastPayloadError != MqttSparkplugError::TooManyMetrics) {
            const MqttSparkplugTrackedNode* known = findNode(parts.group, parts.node);
            if (known != nullptr) {
                askRebirth(_nodes[(size_t)(known - &_nodes[0])], parts.group,
                           parts.node);
            }
        }
        return;
    }

    const char* const deviceId = parts.hasDevice ? parts.device : nullptr;
    const int index = trackNode(parts.group, parts.node);
    if (index >= 0) {
        MqttSparkplugTrackedNode& tracked = _nodes[(size_t)index];
        tracked.messages++;

        // --- LEARNING THE DICTIONARY, AND THEN USING IT ---
        //
        // [tck-id-payloads-alias-birth-requirement] NBIRTH and DBIRTH MUST
        // include both a metric name and alias; the data messages then carry
        // the alias alone. So the births are the only place the names exist,
        // and a host that does not record them here can never read the data it
        // is subscribed to.
        //
        // An NBIRTH REPLACES the table - it is a new picture of the node, and
        // keeping entries from the old one would let a metric that has been
        // removed go on resolving. A DBIRTH REPLACES ITS DEVICE'S PART of it,
        // for the same reason one level down; the node's own metrics and the
        // other devices' are untouched.
        if (parts.type == MqttSparkplugType::NBirth) {
            tracked.metricCount = 0;
            indexMetrics(tracked);
        } else if (parts.type == MqttSparkplugType::DBirth) {
            forgetDevice(tracked, deviceId);
        }
        if (parts.type == MqttSparkplugType::NBirth ||
            parts.type == MqttSparkplugType::DBirth) {
            rememberMetrics(tracked, _inbound, deviceId);
            compactMetrics(tracked);
        } else {
            resolveAliases(tracked, _inbound, deviceId);
        }

        // Births carry values too - [tck-id-payloads-nbirth-metric-reqs]
        // requires it - so the picture is current from the birth onwards
        // rather than only once the first NDATA happens to arrive. A host that
        // waited for data would report every metric as unknown for however
        // long the slowest one takes to change.
        if (parts.type != MqttSparkplugType::NDeath &&
            parts.type != MqttSparkplugType::DDeath) {
            recordValues(tracked, _inbound, deviceId);
        }

        if (parts.type == MqttSparkplugType::NBirth) {
            tracked.online = true;
            tracked.hasSeq = false;
            tracked.reordering = false;
            tracked.rebirthAsked = false;   // answered
            if (_inbound.hasSeq) {
                trackSequence(tracked, (uint8_t)_inbound.seq, parts.group,
                              parts.node);
            }

            // [tck-id-topics-nbirth-bdseq-included] the bdSeq metric, which is
            // the only thing that will correlate the eventual NDEATH.
            const MqttSparkplugMetric* bdSeq =
                _inbound.findMetric(MQ_SP_METRIC_BDSEQ);
            if (bdSeq != nullptr) {
                tracked.bdSeq = (uint64_t)bdSeq->value.asNumber();
                tracked.hasBdSeq = true;
            }
        } else if (parts.type == MqttSparkplugType::NDeath) {
            // [tck-id-payloads-ndeath-bdseq] ... the bdSeq number used in the
            // NBIRTH and NDEATH messages MUST be used to determine that an
            // NDEATH matches a prior NBIRTH.
            //
            // So a death whose bdSeq does not match the birth we have is a
            // death for a session that already ended, and marking the node
            // offline on it would be wrong - the node may be connected again.
            const MqttSparkplugMetric* bdSeq =
                _inbound.findMetric(MQ_SP_METRIC_BDSEQ);
            const bool matches =
                bdSeq != nullptr && tracked.hasBdSeq &&
                (uint64_t)bdSeq->value.asNumber() == tracked.bdSeq;
            if (matches || !tracked.hasBdSeq) {
                tracked.online = false;
                tracked.hasSeq = false;
                tracked.reordering = false;
                // [tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-offline],
                // -ndeath-node-tags-stale, -ndeath-devices-offline and
                // -ndeath-devices-tags-stale: the node offline, and every
                // metric of it and of its devices stale, at the host's own
                // UTC time. The TCK asked; the host had only `online`.
                const uint64_t now = hostUtcNow();
                tracked.offlineAt = now;
                for (size_t m = 0; m < tracked.metricCount; m++) {
                    if (tracked.metrics[m].stale) continue;
                    tracked.metrics[m].stale = true;
                    tracked.metrics[m].staleAt = now;
                }
            }
        } else if (parts.type == MqttSparkplugType::DDeath) {
            // [tck-id-operational-behavior-edge-node-termination-host-action-ddeath-devices-offline]
            // and -ddeath-devices-tags-stale: the device offline and its
            // metrics stale, at the DDEATH's own timestamp.
            const uint64_t at = _inbound.hasTimestamp ? _inbound.timestamp : hostUtcNow();
            const char* wanted = deviceId != nullptr ? deviceId : "";
            for (size_t m = 0; m < tracked.metricCount; m++) {
                MqttSparkplugTrackedMetric& entry = tracked.metrics[m];
                if (entry.device != wanted || entry.stale) continue;
                entry.stale = true;
                entry.staleAt = at;
            }
            if (_inbound.hasSeq) {
                trackSequence(tracked, (uint8_t)_inbound.seq, parts.group,
                              parts.node);
            }
        } else if (parts.type != MqttSparkplugType::DDeath && !tracked.online) {
            // DATA, OR A DEVICE BIRTH, FROM A NODE WITH NO BIRTH ON RECORD: a
            // host that started after the node did, or a device born before
            // its node's birth was seen. Nothing it sends by alias can be
            // named, and nothing will change that until the node births again
            // - so a primary host asks, as Ignition's does, held like every
            // other request.
            askRebirth(tracked, parts.group, parts.node);
        } else if (_inbound.hasSeq) {
            trackSequence(tracked, (uint8_t)_inbound.seq, parts.group,
                          parts.node);
        }
    }

    if (_handler == nullptr && _contextHandler == nullptr) return;

    MqttSparkplugInbound inbound;
    inbound.type = parts.type;
    inbound.group = parts.group;
    inbound.node = parts.node;
    inbound.device = deviceId;
    inbound.payload = &_inbound;
    if (_handler) _handler(inbound);
    if (_contextHandler) _contextHandler(_context, inbound);
}

void MqttSparkplugHost::onMessage(MqttSparkplugInboundFn handler) {
    _handler = handler;
}

void MqttSparkplugHost::onMessage(MqttSparkplugInboundContextFn handler,
                                  void* context) {
    _contextHandler = handler;
    _context = context;
}

// =============================================================================
// COMMANDING
// =============================================================================

bool MqttSparkplugHost::command(const char* group, const char* node,
                                const char* name,
                                const MqttSparkplugValue& value) {
    if (!_client.isConnected()) return false;

    MqttSparkplugPayload payload;
    payload.clear();
    // [tck-id-payloads-ncmd-timestamp] An NCMD message MUST include a timestamp.
    payload.timestamp = hostUtcNow();
    payload.hasTimestamp = true;
    // [tck-id-payloads-ncmd-seq] An NCMD message MUST NOT include a sequence
    // number - the sequence belongs to the edge node's stream, and a host
    // injecting one would corrupt it.

    MqttSparkplugMetric metric;
    metric.name = payload.intern(name);
    if (metric.name == nullptr) return false;
    metric.value = value;
    if (!payload.addMetric(metric)) return false;

    MqttSparkplugError error = MqttSparkplugError::None;
    const size_t wrote = mq_sparkplugEncode(_scratch, sizeof _scratch, payload,
                                            MqttSparkplugType::NCmd, error);
    _lastPayloadError = error;
    if (wrote == 0) return false;

    if (mq_sparkplugBuildTopic(_topic, sizeof _topic, MqttSparkplugType::NCmd,
                               group, node) == 0) {
        return false;
    }

    // [tck-id-payloads-ncmd-qos] QoS 0. [tck-id-payloads-ncmd-retain] retain
    // false. A retained command would be re-delivered to a node on every
    // reconnect, which is how a pump gets started by a message from last week.
    return mq_accepted(
        _client.publishSeries(_topic, _scratch, wrote, MQ_QOS_0, false));
}

bool MqttSparkplugHost::commandDevice(const char* group, const char* node,
                                      const char* device, const char* name,
                                      const MqttSparkplugValue& value) {
    if (!_client.isConnected()) return false;

    MqttSparkplugPayload payload;
    payload.clear();
    payload.timestamp = hostUtcNow();
    payload.hasTimestamp = true;

    MqttSparkplugMetric metric;
    metric.name = payload.intern(name);
    if (metric.name == nullptr) return false;
    metric.value = value;
    if (!payload.addMetric(metric)) return false;

    MqttSparkplugError error = MqttSparkplugError::None;
    const size_t wrote = mq_sparkplugEncode(_scratch, sizeof _scratch, payload,
                                            MqttSparkplugType::DCmd, error);
    _lastPayloadError = error;
    if (wrote == 0) return false;

    if (mq_sparkplugBuildTopic(_topic, sizeof _topic, MqttSparkplugType::DCmd,
                               group, node, device) == 0) {
        return false;
    }
    return mq_accepted(
        _client.publishSeries(_topic, _scratch, wrote, MQ_QOS_0, false));
}

bool MqttSparkplugHost::requestRebirth(const char* group, const char* node) {
    // [tck-id-operational-behavior-data-commands-rebirth-verb] equivalent: the
    // request is an NCMD carrying Node Control/Rebirth set true.
    return command(group, node, MQ_SP_METRIC_NODE_REBIRTH, mq_spBool(true));
}

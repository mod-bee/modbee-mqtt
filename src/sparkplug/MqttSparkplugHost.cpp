// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugHost.h"

#include <string.h>


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

MqttSparkplugHost::MqttSparkplugHost() {
    memset(_scratch, 0, sizeof _scratch);
    _topic[0] = '\0';
}

MqttSparkplugHost::MqttSparkplugHost(MqttTransport& transport) {
    memset(_scratch, 0, sizeof _scratch);
    _topic[0] = '\0';
    _net.borrow(transport);
}

MqttSparkplugHost::~MqttSparkplugHost() {
    if (_begun) _client.end();
}

#if MQTT_TLS_SOCKETS
bool MqttSparkplugHost::useTls(bool enabled) {
    return !_begun && _net.useTls(enabled);
}

bool MqttSparkplugHost::trustFile(const char* path) {
    return !_begun && _net.trustFile(path);
}

bool MqttSparkplugHost::pinnedKey(const char* fingerprint) {
    return !_begun && _net.pin(fingerprint);
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

bool MqttSparkplugHost::server(const char* host, uint16_t port) {
    if (host == nullptr || host[0] == '\0') return false;
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
    _net.target(_host.c_str(), _connection.port);

    if (_connection.clientId.empty()) _connection.clientId = _hostId;

    // [tck-id-message-flow-phid-sparkplug-clean-session-311] and -50: a Host
    // Application is as stateless as an edge node.
    _connection.cleanStart = true;
    _connection.sessionExpiryInterval = 0;

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
    _connection.will.set(_topic, _willBytes.data(), _willBytes.size(),
                         MQ_QOS_1, true);
    _connection.will.delayInterval = 0;
}

void MqttSparkplugHost::beginClient() {
    if (_begun || _net.get() == nullptr) return;
    _behaviour.autoReconnect = true;
    applyConnectionConfig();
    _client.begin(*_net.get(), _connection, _behaviour);
    _client.onMessage(&messageTrampoline, this);
    _client.onSessionEstablished(&sessionTrampoline, this);
    _begun = true;
}

bool MqttSparkplugHost::connect() {
    if (_hostId.empty()) return false;
    if (_net.get() == nullptr) return false;
    if (_host.empty()) return false;

    _born = false;
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
    _client.poll();
    serviceReorder();

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

void MqttSparkplugHost::sessionTrampoline(void* context, bool sessionPresent) {
    static_cast<MqttSparkplugHost*>(context)->sessionEstablished(sessionPresent);
}

void MqttSparkplugHost::messageTrampoline(void* context,
                                          const MqttMessage& message) {
    static_cast<MqttSparkplugHost*>(context)->handleMessage(message);
}

void MqttSparkplugHost::sessionEstablished(bool sessionPresent) {
    (void)sessionPresent;

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

bool MqttSparkplugHost::publishDeath() {
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
                                             &deathTrampoline, this));
}

// =============================================================================
// TRACKING NODES
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

int MqttSparkplugHost::trackNode(const char* group, const char* node) {
    for (size_t index = 0; index < _nodeCount; index++) {
        if (_nodes[index].group == group && _nodes[index].node == node) {
            return (int)index;
        }
    }
    if (_nodeCount >= MQ_SP_MAX_TRACKED_NODES) return -1;
    _nodes[_nodeCount].group = group;
    _nodes[_nodeCount].node = node;
    _nodeCount++;
    return (int)(_nodeCount - 1);
}

const MqttSparkplugTrackedNode* MqttSparkplugHost::nodeAt(size_t index) const {
    if (index >= _nodeCount) return nullptr;
    return &_nodes[index];
}

const MqttSparkplugTrackedNode*
MqttSparkplugHost::findNode(const char* group, const char* node) const {
    if (group == nullptr || node == nullptr) return nullptr;
    for (size_t index = 0; index < _nodeCount; index++) {
        if (_nodes[index].group == group && _nodes[index].node == node) {
            return &_nodes[index];
        }
    }
    return nullptr;
}

void MqttSparkplugHost::serviceReorder() {
    if (_reorderTimeout == 0) return;

    const mq_time_t now = mq_millis();
    for (size_t index = 0; index < _nodeCount; index++) {
        MqttSparkplugTrackedNode& tracked = _nodes[index];
        if (!tracked.reordering) continue;

        // mq_isFuture handles the wrap of a 32-bit millisecond clock, which is
        // why it is used rather than a plain subtraction - the same reason
        // every other deadline in this library goes through it.
        if (mq_isFuture(now, tracked.reorderStartedAt + _reorderTimeout)) {
            continue;
        }

        // [tck-id-operational-behavior-host-reordering-rebirth] If ... the
        // Reorder Timeout elapses and the missing message(s) have not been
        // received, the Sparkplug Host Application MUST send an NCMD to the
        // Edge Node with a Node Control/Rebirth request.
        tracked.reordering = false;
        if (_primary) {
            requestRebirth(tracked.group.c_str(), tracked.node.c_str());
            tracked.rebirthsRequested++;
        }
    }
}

// =============================================================================
// INBOUND
// =============================================================================

const char* MqttSparkplugTrackedNode::metricName(uint64_t alias) const {
    for (size_t index = 0; index < metricCount; index++) {
        if (metrics[index].alias == alias) return metrics[index].name.c_str();
    }
    return nullptr;
}

const MqttSparkplugTrackedMetric* MqttSparkplugTrackedNode::find(
    const char* name, const char* device) const {
    if (name == nullptr) return nullptr;
    const char* wanted = device != nullptr ? device : "";
    for (size_t index = 0; index < metricCount; index++) {
        if (metrics[index].name != name) continue;
        // A node-level "flow" and a device's "flow" are different metrics, so
        // the device has to match too - including the empty case.
        if (metrics[index].device != wanted) continue;
        return &metrics[index];
    }
    return nullptr;
}

MqttSparkplugDataType MqttSparkplugTrackedNode::metricType(
    uint64_t alias) const {
    for (size_t index = 0; index < metricCount; index++) {
        if (metrics[index].alias == alias) return metrics[index].type;
    }
    return MqttSparkplugDataType::Unknown;
}

void MqttSparkplugHost::rememberMetrics(MqttSparkplugTrackedNode& tracked,
                                       const MqttSparkplugPayload& payload,
                                       const char* deviceId) {
    for (size_t index = 0; index < payload.metricCount; index++) {
        const MqttSparkplugMetric& metric = payload.metrics[index];
        if (!metric.hasAlias || metric.name == nullptr) continue;

        // An alias declared twice replaces the earlier entry rather than
        // adding a second. [tck-id-payloads-alias-uniqueness] says it cannot
        // happen on a conformant node, but a host is reading someone else's
        // traffic and two entries for one alias would make which name wins
        // depend on search order.
        size_t slot = tracked.metricCount;
        for (size_t seen = 0; seen < tracked.metricCount; seen++) {
            if (tracked.metrics[seen].alias == metric.alias) {
                slot = seen;
                break;
            }
        }

        // FULL IS FULL, and quietly. The table is a convenience for reading
        // data; refusing the birth over it would lose the node entirely, and
        // an unresolved alias already has an honest answer - metricName()
        // returns nullptr and the caller sees a number it cannot name.
        if (slot == tracked.metricCount) {
            if (tracked.metricCount >= MQ_SP_MAX_TRACKED_METRICS) return;
            tracked.metricCount++;
        }

        tracked.metrics[slot].name = metric.name;
        tracked.metrics[slot].alias = metric.alias;
        tracked.metrics[slot].type = metric.value.type;
        tracked.metrics[slot].device = deviceId != nullptr ? deviceId : "";
    }
}

void MqttSparkplugHost::recordValues(MqttSparkplugTrackedNode& tracked,
                                    const MqttSparkplugPayload& payload,
                                    const char* deviceId) {
    // Called AFTER the names have been filled in, so a data message carrying
    // nothing but an alias still lands in the right entry.
    const char* wanted = deviceId != nullptr ? deviceId : "";
    for (size_t index = 0; index < payload.metricCount; index++) {
        const MqttSparkplugMetric& metric = payload.metrics[index];

        // Matched by alias first, because that is what a conformant node
        // sends and it cannot be ambiguous. The name is the fallback for a
        // node that sends names, which the specification permits.
        int slot = -1;
        for (size_t seen = 0; seen < tracked.metricCount; seen++) {
            if (metric.hasAlias) {
                if (tracked.metrics[seen].alias == metric.alias) {
                    slot = (int)seen;
                    break;
                }
                continue;
            }
            if (metric.name == nullptr) continue;
            if (tracked.metrics[seen].name != metric.name) continue;
            if (tracked.metrics[seen].device != wanted) continue;
            slot = (int)seen;
            break;
        }
        if (slot < 0) continue;   // never declared; nothing to update

        // A NULL metric is a positive statement that the value is not known,
        // so the entry stops offering a stale reading rather than keeping the
        // last good one.
        if (metric.value.isNull) {
            tracked.metrics[slot].hasValue = false;
            continue;
        }

        tracked.metrics[slot].value = metric.value;
        tracked.metrics[slot].hasValue = true;
        // The metric's own timestamp if it carried one, otherwise the
        // payload's - which is the usual case, since one payload timestamp
        // covering the batch is most of the bandwidth saving.
        tracked.metrics[slot].at =
            metric.hasTimestamp ? metric.timestamp
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
        if (metric.name == nullptr && metric.hasAlias) {
            const char* name = tracked.metricName(metric.alias);
            if (name != nullptr) metric.name = name;
        }

        // [tck-id-payloads-metric-datatype-not-req] Data SHOULD NOT carry a
        // datatype, so the birth's is applied - by alias, or by name.
        if (metric.datatypeAbsent) {
            MqttSparkplugDataType declared = MqttSparkplugDataType::Unknown;
            if (metric.hasAlias) {
                declared = tracked.metricType(metric.alias);
            } else if (metric.name != nullptr) {
                const MqttSparkplugTrackedMetric* entry = tracked.find(metric.name, deviceId);
                if (entry != nullptr) declared = entry->type;
            }
            if (declared != MqttSparkplugDataType::Unknown) {
                mq_sparkplugApplyDatatype(metric, declared);
            }
        }
    }
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

    const MqttSparkplugError error =
        mq_sparkplugDecode(message.payload.data, message.payload.length,
                           _inbound);
    if (error != MqttSparkplugError::None) {
        _lastPayloadError = error;
        _decodeFailures++;

        // [tck-id-operational-behavior-host-reordering-rebirth]'s sibling
        // guidance: a Host Application MAY send Node Control/Rebirth NCMD
        // messages if malformed payloads arrive. Being primary is the
        // condition, per that statement's own non-normative note.
        if (_primary) {
            requestRebirth(parts.group, parts.node);
        }
        return;
    }

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
        // removed go on resolving. A DBIRTH EXTENDS it, because a device
        // birthing says nothing about the node's own metrics.
        if (parts.type == MqttSparkplugType::NBirth) {
            tracked.metricCount = 0;
        }
        if (parts.type == MqttSparkplugType::NBirth ||
            parts.type == MqttSparkplugType::DBirth) {
            rememberMetrics(tracked, _inbound,
                            parts.hasDevice ? parts.device : nullptr);
        } else {
            resolveAliases(tracked, _inbound, parts.hasDevice ? parts.device : nullptr);
        }

        // Births carry values too - [tck-id-payloads-nbirth-metric-reqs]
        // requires it - so the picture is current from the birth onwards
        // rather than only once the first NDATA happens to arrive. A host that
        // waited for data would report every metric as unknown for however
        // long the slowest one takes to change.
        if (parts.type != MqttSparkplugType::NDeath &&
            parts.type != MqttSparkplugType::DDeath) {
            recordValues(tracked, _inbound,
                         parts.hasDevice ? parts.device : nullptr);
        }

        if (parts.type == MqttSparkplugType::NBirth) {
            tracked.online = true;
            tracked.hasSeq = true;
            tracked.lastSeq = (uint8_t)_inbound.seq;
            tracked.reordering = false;

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
            }
        } else if (_inbound.hasSeq) {
            // [tck-id-payloads-sequence-num-incrementing] one greater, wrapping
            // at 255. Anything else is a gap.
            const uint8_t expected = (uint8_t)(tracked.lastSeq + 1);
            const uint8_t arrived = (uint8_t)_inbound.seq;

            if (tracked.hasSeq && arrived != expected) {
                tracked.gaps++;
                if (_reorderTimeout > 0) {
                    // [tck-id-operational-behavior-host-reordering-start] ...
                    // the Host Application MUST start a timer denoting the
                    // start of the Reorder Timeout window.
                    if (!tracked.reordering) {
                        tracked.reordering = true;
                        tracked.reorderStartedAt = mq_millis();
                    }
                } else if (_primary) {
                    requestRebirth(parts.group, parts.node);
                    tracked.rebirthsRequested++;
                }
            } else if (tracked.reordering) {
                // [-reordering-success] If the missing message(s) ... arrive
                // before the reordering timer elapses, the timer MUST be
                // terminated and normal operation MUST continue.
                tracked.reordering = false;
            }
            tracked.lastSeq = arrived;
            tracked.hasSeq = true;
        }
    }

    if (_handler == nullptr && _contextHandler == nullptr) return;

    MqttSparkplugInbound inbound;
    inbound.type = parts.type;
    inbound.group = parts.group;
    inbound.node = parts.node;
    inbound.device = parts.hasDevice ? parts.device : nullptr;
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

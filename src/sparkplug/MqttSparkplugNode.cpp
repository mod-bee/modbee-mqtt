// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugNode.h"

#include <stdio.h>
#include <string.h>


// =============================================================================
// CONSTRUCTION
// =============================================================================

MqttSparkplugNode::MqttSparkplugNode() {
    memset(_scratch, 0, sizeof _scratch);
    _topic[0] = '\0';
}

MqttSparkplugNode::MqttSparkplugNode(MqttTransport& transport) {
    memset(_scratch, 0, sizeof _scratch);
    _topic[0] = '\0';
    _net.borrow(transport);
}

MqttSparkplugNode::~MqttSparkplugNode() {
    // The client concludes what is outstanding before the transport it is
    // bound to goes away with _net.
    if (_begun) _client.end();
}

#if MQTT_TLS_SOCKETS
// Before the client is begun only: it binds the transport it is given, and
// swapping one underneath it would leave it talking to the other.
bool MqttSparkplugNode::useTls(bool enabled) {
    return !_begun && _net.useTls(enabled);
}

bool MqttSparkplugNode::trustFile(const char* path) {
    return !_begun && _net.trustFile(path);
}

bool MqttSparkplugNode::pinnedKey(const char* fingerprint) {
    return !_begun && _net.pin(fingerprint);
}
#endif

// =============================================================================
// THE CLOCK
// =============================================================================

void MqttSparkplugNode::epochNow(uint64_t millisecondsSinceEpoch) {
    if (millisecondsSinceEpoch == 0) {
        _epochSet = false;
        return;
    }
    _epochAtBoot = millisecondsSinceEpoch;
    _epochSetAt = mq_millis();
    _epochSet = true;
}

bool MqttSparkplugNode::clockIsSet() const {
    // Either the caller gave us one, or the platform has one. No guard here:
    // mq_epochMillis() answers zero where there is none, which is the whole
    // reason it is in the platform layer rather than behind an #if in this one
    // (R9 - a platform's habits stay behind the platform interface).
    return _epochSet || mq_epochMillis() != 0;
}

uint64_t MqttSparkplugNode::utcNow() const {
    // An explicitly supplied epoch wins, on every platform. A PLC whose SCADA
    // system hands it the time is more trustworthy than a container whose
    // clock nobody has looked at.
    if (_epochSet) {
        return _epochAtBoot +
               (uint64_t)mq_elapsed(_epochSetAt, mq_millis());
    }

    // The platform's own wall clock, which answers zero where there is none.
    const uint64_t epoch = mq_epochMillis();
    if (epoch != 0) return epoch;

    // No clock anywhere. Boot-relative, and NOT conformant - clockIsSet() says
    // so, and the alternative is omitting a field the specification makes
    // mandatory on every message type.
    return (uint64_t)mq_millis();
}

// =============================================================================
// IDENTITY
// =============================================================================

bool MqttSparkplugNode::group(const char* groupId) {
    if (!mq_sparkplugValidId(groupId)) return false;
    _group = groupId;
    return true;
}

bool MqttSparkplugNode::node(const char* edgeNodeId) {
    if (!mq_sparkplugValidId(edgeNodeId)) return false;
    _node = edgeNodeId;
    return true;
}

void MqttSparkplugNode::credentials(const char* user, const char* password) {
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

void MqttSparkplugNode::keepAlive(uint16_t seconds) {
    _connection.keepAlive = seconds;
}

void MqttSparkplugNode::only(MqttVersion version) {
    _connection.version = version;
    // A caller who names a version means it. Fallback stays available only when
    // 5.0 was asked for, which is the library's own rule - and note that the
    // Will here never needs 5.0 (no delay interval, no properties), so
    // fallbackWouldLoseTheWill() cannot refuse it. That was a real trap on the
    // plant: a Will Delay blocked the 3.1.1 fallback silently.
    _connection.fallbackEnabled = (version == MqttVersion::V5);
    _connection.fallbackVersion = MqttVersion::V311;
}

// =============================================================================
// SERVERS
// =============================================================================

bool MqttSparkplugNode::server(const char* host, uint16_t port) {
    _serverCount = 0;
    _serverIndex = 0;
    return addServer(host, port);
}

bool MqttSparkplugNode::addServer(const char* host, uint16_t port) {
    if (host == nullptr || host[0] == '\0') return false;
    if (_serverCount >= MQ_SP_MAX_SERVERS) return false;
    _servers[_serverCount].host = host;
    _servers[_serverCount].port = port;
    _serverCount++;
    return true;
}

void MqttSparkplugNode::advanceServer() {
    if (_serverCount <= 1) return;
    _serverIndex = (_serverIndex + 1) % _serverCount;
}

// =============================================================================
// PRIMARY HOST
// =============================================================================

void MqttSparkplugNode::primaryHost(const char* sparkplugHostId) {
    if (sparkplugHostId == nullptr || sparkplugHostId[0] == '\0') {
        _primaryHostId.clear();
        return;
    }
    _primaryHostId = sparkplugHostId;
}

// =============================================================================
// DECLARING METRICS
// =============================================================================

int MqttSparkplugNode::findPointIndex(const char* name, int16_t device) const {
    if (name == nullptr) return -1;
    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].device != device) continue;
        // Case SENSITIVE. [tck-id-case-sensitivity-metric-names]. A lookup that
        // folded case would let a PLC declare "Level" and write "LEVEL" and
        // silently hit the same metric - and then a Host Application shows one
        // tag where the engineer wrote two.
        if (_points[index].name == name) return (int)index;
    }
    return -1;
}

int MqttSparkplugNode::findPointByAlias(uint64_t alias) const {
    if (alias == 0) return -1;
    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].alias == alias) return (int)index;
    }
    return -1;
}

int MqttSparkplugNode::findDeviceIndex(const char* deviceId) const {
    if (deviceId == nullptr) return -1;
    for (size_t index = 0; index < _deviceCount; index++) {
        if (_devices[index].id == deviceId) return (int)index;
    }
    return -1;
}

uint64_t MqttSparkplugNode::declareInternal(const char* name,
                                            MqttSparkplugDataType type,
                                            int16_t device, bool writable,
                                            const char* units,
                                            int32_t quality) {
    if (name == nullptr || name[0] == '\0') return 0;
    if (_pointCount >= MQ_SP_MAX_METRICS) return 0;

    // A datatype this build cannot carry is refused HERE rather than at encode
    // time. [tck-id-topics-nbirth-templates] requires every Template definition
    // to be in the NBIRTH, so a Template metric declared and then refused at
    // publish time would leave a node that births successfully and then cannot
    // send data - which is a worse failure than not starting.
    if (!mq_sparkplugDataTypeSupported(type)) return 0;

    // Already declared. Refused rather than replaced: a second declaration with
    // a different type would change the meaning of a metric the NBIRTH has
    // already described.
    if (findPointIndex(name, device) >= 0) return 0;

    // UTF-8, and the specification discourages a list of punctuation in metric
    // names but forbids none of it - so this validates encoding only, and does
    // not second-guess what an engineer called their tag.
    if (mq_validateUtf8((const uint8_t*)name, strlen(name)) != MqttError::None) {
        return 0;
    }

    MqttSparkplugPoint& point = _points[_pointCount];
    point = MqttSparkplugPoint();
    point.name = name;
    point.type = type;
    point.device = device;
    point.writable = writable;
    point.quality = quality;
    if (units != nullptr) point.units = units;
    point.value = mq_spNull(type);

    // [tck-id-payloads-alias-uniqueness] ... MUST be a unique number across
    // this Edge Node's ENTIRE set of metrics. One counter for the node and
    // every device under it, which is why this is not per-device.
    point.alias = _aliasesEnabled ? _nextAlias++ : 0;

    _pointCount++;
    return point.alias;
}

uint64_t MqttSparkplugNode::declare(const char* name,
                                    MqttSparkplugDataType type) {
    return declareInternal(name, type, -1, false, nullptr, MQ_SP_QUALITY_GOOD);
}

uint64_t MqttSparkplugNode::declare(const char* name,
                                    MqttSparkplugDataType type,
                                    const char* units, int32_t quality) {
    return declareInternal(name, type, -1, false, units, quality);
}

uint64_t MqttSparkplugNode::declareWritable(const char* name,
                                            MqttSparkplugDataType type) {
    return declareInternal(name, type, -1, true, nullptr, MQ_SP_QUALITY_GOOD);
}

uint64_t MqttSparkplugNode::declareTyped(const char* name, MqttValueType type) {
    MqttSparkplugDataType sparkplugType = MqttSparkplugDataType::Unknown;
    if (!mq_sparkplugTypeForValueType(type, sparkplugType)) return 0;
    return declareInternal(name, sparkplugType, -1, false, nullptr,
                           MQ_SP_QUALITY_GOOD);
}

const MqttSparkplugPoint* MqttSparkplugNode::pointAt(size_t index) const {
    if (index >= _pointCount) return nullptr;
    return &_points[index];
}

const MqttSparkplugPoint* MqttSparkplugNode::findPoint(const char* name) const {
    const int index = findPointIndex(name, -1);
    return index < 0 ? nullptr : &_points[index];
}

// =============================================================================
// DEVICES
// =============================================================================

bool MqttSparkplugNode::addDevice(const char* deviceId) {
    if (!mq_sparkplugValidId(deviceId)) return false;
    if (_deviceCount >= MQ_SP_MAX_DEVICES) return false;
    // [tck-id-topic-structure-namespace-unique-device-id] The device_id MUST be
    // unique from other device_ids for this Edge Node.
    if (findDeviceIndex(deviceId) >= 0) return false;

    _devices[_deviceCount].id = deviceId;
    _devices[_deviceCount].online = false;
    _devices[_deviceCount].wantsBirth = false;
    _deviceCount++;
    return true;
}

uint64_t MqttSparkplugNode::declareFor(const char* deviceId, const char* name,
                                       MqttSparkplugDataType type) {
    const int device = findDeviceIndex(deviceId);
    if (device < 0) return 0;
    return declareInternal(name, type, (int16_t)device, false, nullptr,
                           MQ_SP_QUALITY_GOOD);
}

uint64_t MqttSparkplugNode::declareWritableFor(const char* deviceId,
                                               const char* name,
                                               MqttSparkplugDataType type) {
    const int device = findDeviceIndex(deviceId);
    if (device < 0) return 0;
    return declareInternal(name, type, (int16_t)device, true, nullptr,
                           MQ_SP_QUALITY_GOOD);
}

bool MqttSparkplugNode::deviceOnline(const char* deviceId) {
    const int index = findDeviceIndex(deviceId);
    if (index < 0) return false;

    Device& device = _devices[index];
    if (device.online) return true;

    // [tck-id-message-flow-device-birth-publish-nbirth-wait] A Device can
    // publish a DBIRTH as long as an NBIRTH has been sent previously. Before
    // that, the birth is OWED rather than refused: a caller bringing a device
    // up during start-up should not have to sequence itself against the
    // session coming up.
    if (!_born) {
        device.wantsBirth = true;
        return true;
    }

    device.online = true;
    return publishDeviceBirth(device);
}

bool MqttSparkplugNode::deviceOffline(const char* deviceId) {
    const int index = findDeviceIndex(deviceId);
    if (index < 0) return false;

    Device& device = _devices[index];
    device.wantsBirth = false;
    if (!device.online) return true;
    device.online = false;

    if (!_born || !_client.isConnected()) return true;

    // A DDEATH carries a sequence number.
    // [tck-id-payloads-ddeath-seq] Every DDEATH message MUST include a sequence
    // number - which is the opposite of NDEATH, where
    // [tck-id-topics-ndeath-seq] says it MUST NOT. Two death messages, opposite
    // rules, and the place that gets it wrong is right here.
    nextSequence();

    MqttSparkplugPayload& payload = _outbound;
    payload.clear();
    payload.timestamp = utcNow();
    payload.hasTimestamp = true;
    payload.seq = _seq;
    payload.hasSeq = true;

    return sendPayload(MqttSparkplugType::DDeath, device.id.c_str(), payload,
                       MQ_QOS_0, false, false);
}

bool MqttSparkplugNode::deviceIsOnline(const char* deviceId) const {
    const int index = findDeviceIndex(deviceId);
    return index >= 0 && _devices[index].online;
}

// =============================================================================
// SEQUENCE NUMBERS
// =============================================================================

void MqttSparkplugNode::nextSequence() {
    // [tck-id-payloads-sequence-num-incrementing] ... continually increasing by
    // one in each message from that Edge Node until a value of 255 is reached.
    // At that point, the sequence number of the following message MUST be zero.
    //
    // A uint8_t wraps 255 -> 0 by itself, which is the requirement exactly.
    // Written as an increment rather than a modulo so that is visible.
    _seq++;
}

// =============================================================================
// CONNECTING
// =============================================================================

void MqttSparkplugNode::applyConnectionConfig() {
    if (_serverCount > 0) {
        const Server& active = _servers[_serverIndex];
        _connection.host = active.host;
        _connection.port = active.port != 0 ? active.port : _net.defaultPort();

        // AND THE TRANSPORT, which is what actually dials. Setting only the
        // connection config used to be the whole of a server walk, so a node
        // told to move to the next server reconnected to the same one: the
        // index advanced and the socket did not. A transport the caller
        // brought is still left alone - see the constructor.
        _net.target(active.host.c_str(), _connection.port);
    }

    // The client identifier. Sparkplug does not specify one, but the Edge Node
    // Descriptor - group plus node - MUST be unique in the system
    // [tck-id-payloads-nbirth-edge-node-descriptor], so it is the obvious
    // identifier and it means two nodes cannot take each other's session.
    if (_connection.clientId.empty()) {
        _connection.clientId = _group;
        _connection.clientId += ":";
        _connection.clientId += _node;
    }

    // [tck-id-principles-persistence-clean-session-311] Clean Session true.
    // [tck-id-principles-persistence-clean-session-50] Clean Start true AND
    // Session Expiry Interval 0.
    //
    // NOT configurable, and not silently corrected either - these are the
    // library's defaults already, and a caller who changed them through
    // client() would get a node that is non-conformant in a way no test of
    // theirs would catch. Set here so that is impossible.
    _connection.cleanStart = true;
    _connection.sessionExpiryInterval = 0;

    // --- The Will: an NDEATH carrying this connection's bdSeq ---
    //
    // [tck-id-payloads-ndeath-bdseq] The NDEATH message MUST include the same
    // bdSeq number value that will be used in the associated NBIRTH message.
    // [tck-id-topics-ndeath-payload] ... MUST only include a single metric, the
    // bdSeq number.
    // [tck-id-topics-ndeath-seq] The NDEATH message MUST NOT include a
    // sequence number.
    MqttSparkplugPayload will;
    MqttSparkplugMetric bdSeqMetric;
    bdSeqMetric.name = MQ_SP_METRIC_BDSEQ;
    bdSeqMetric.value = mq_spInt64((int64_t)_bdSeq);
    will.addMetric(bdSeqMetric);
    // No seq. No timestamp either - an NDEATH MAY include one and the
    // specification warns that it is set at CONNECT time and so is not useful,
    // and that bdSeq MUST be used to match instead.

    MqttSparkplugError error = MqttSparkplugError::None;
    // Encoded as an NDEATH so the metric carries its NAME: the alias table does
    // not exist yet at CONNECT time, and a Host Application reading a death
    // certificate has nothing to resolve an alias against.
    const size_t willSize =
        mq_sparkplugSize(will, MqttSparkplugType::NDeath, error);
    if (error == MqttSparkplugError::None && willSize > 0 &&
        willSize <= sizeof _scratch) {
        const size_t wrote = mq_sparkplugEncode(_scratch, sizeof _scratch, will,
                                                MqttSparkplugType::NDeath,
                                                error);
        if (wrote > 0) {
            _willBytes.assign(_scratch, _scratch + wrote);

            char willTopic[MQ_SP_MAX_TOPIC];
            if (mq_sparkplugBuildTopic(willTopic, sizeof willTopic,
                                       MqttSparkplugType::NDeath,
                                       _group.c_str(), _node.c_str()) > 0) {
                // [tck-id-payloads-ndeath-will-message-qos] Will QoS 1.
                // [tck-id-payloads-ndeath-will-message-retain] Will Retained
                // FALSE - and note this is the OPPOSITE of a Host
                // Application's STATE Will, which must be retained true. The
                // two are different messages with different rules and swapping
                // them is a conformance failure in both directions.
                _connection.will.set(willTopic, _willBytes.data(),
                                     _willBytes.size(), MQ_QOS_1, false);

                // No Will Delay. A delay would need 5.0 and would mean a Host
                // Application waits to be told a node has gone - which is the
                // opposite of what a death certificate is for.
                _connection.will.delayInterval = 0;
            }
        }
    }
    _lastPayloadError = error;
}

void MqttSparkplugNode::beginClient() {
    if (_begun || _net.get() == nullptr) return;

    _behaviour.autoReconnect = true;
    applyConnectionConfig();

    _client.begin(*_net.get(), _connection, _behaviour);
    _client.onMessage(&messageTrampoline, this);
    _client.onSessionEstablished(&sessionTrampoline, this);
    _client.onStateChange(&stateTrampoline, this);
    _begun = true;
}

bool MqttSparkplugNode::connect() {
    if (_group.empty() || _node.empty()) return false;
    if (_net.get() == nullptr) return false;
    if (_serverCount == 0) return false;

    // [tck-id-topics-nbirth-bdseq-increment] The bdSeq number MUST start at
    // zero and increment by one on each new MQTT CONNECT.
    //
    // So the FIRST connect uses 0 and the second uses 1 - hence the flag rather
    // than incrementing unconditionally, which would start at 1.
    if (_bdSeqUsed) {
        _bdSeq++;
    }
    _bdSeqUsed = true;

    _born = false;
    _seq = 0;

    if (!_begun) {
        beginClient();
    } else {
        // The Will has to carry the NEW bdSeq, and the host may have changed if
        // we walked the server list. reconfigure() replaces both without
        // resetting the status counters or discarding queued operations - see
        // its comment in MqttClient.h, which exists because of this call.
        applyConnectionConfig();
        _client.reconfigure(_connection);
    }

    return mq_accepted(_client.connect());
}

void MqttSparkplugNode::disconnect() {
    if (!_begun) return;

    if (_client.activeVersion() == MqttVersion::V311) {
        // [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311]
        // If the Edge Node is using MQTT 3.1.1 and it sends an MQTT DISCONNECT
        // packet, the Edge Node MUST publish an NDEATH message to the MQTT
        // Server BEFORE it sends the MQTT DISCONNECT packet.
        //
        // Because a DISCONNECT tells the server NOT to publish the Will, so
        // without this a clean shutdown looks like a node that is still there.
        //
        // AND THE ORDER IS THE WHOLE POINT. publishSeries() queues; the bytes
        // leave on a later poll(). Calling disconnect() on the next line sent
        // the DISCONNECT first and the NDEATH never went at all. So the
        // disconnect waits for the publish to CONCLUDE - at QoS 1, which means
        // the server has acknowledged it, not merely that we wrote it.
        _disconnectAfterDeath = true;
        _deathAskedAt = mq_millis();
        if (!publishNodeDeath()) {
            // Nothing could be queued, so there is nothing to wait for and
            // hanging on to the session would be worse than a missing death
            // certificate.
            _disconnectAfterDeath = false;
            _client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
        }
    } else {
        // [-disconnect-mqtt50] If the Edge Node is using MQTT 5.0 ... the MQTT
        // v5.0 Disconnect with Will Message reason code MUST be set in the
        // DISCONNECT packet.
        //
        // Then the SERVER publishes the NDEATH, which is better: it is the one
        // already registered, carrying the right bdSeq, and it cannot be lost
        // between our publish and our disconnect.
        _client.disconnect(MQ_REASON_DISCONNECT_WITH_WILL);
    }
    _born = false;
}

bool MqttSparkplugNode::publishNodeDeath() {
    MqttSparkplugPayload& payload = _outbound;
    payload.clear();

    MqttSparkplugMetric bdSeqMetric;
    bdSeqMetric.name = MQ_SP_METRIC_BDSEQ;
    bdSeqMetric.value = mq_spInt64((int64_t)_bdSeq);
    payload.addMetric(bdSeqMetric);
    // No seq: [tck-id-topics-ndeath-seq].

    MqttSparkplugError error = MqttSparkplugError::None;
    const size_t wrote = mq_sparkplugEncode(_scratch, sizeof _scratch, payload,
                                            MqttSparkplugType::NDeath, error);
    _lastPayloadError = error;
    if (wrote == 0) return false;

    if (mq_sparkplugBuildTopic(_topic, sizeof _topic, MqttSparkplugType::NDeath,
                               _group.c_str(), _node.c_str()) == 0) {
        return false;
    }

    // QoS 1 and a completion handler, so the DISCONNECT that follows waits for
    // the PUBACK. At QoS 0 there would be nothing to wait for.
    return mq_accepted(_client.publishSeries(_topic, _scratch, wrote, MQ_QOS_1,
                                             false, &deathTrampoline, this));
}

void MqttSparkplugNode::deathTrampoline(void* context, MqttResult result,
                                        uint16_t packetId) {
    (void)result;
    (void)packetId;
    MqttSparkplugNode* node = static_cast<MqttSparkplugNode*>(context);
    if (!node->_disconnectAfterDeath) return;
    node->_disconnectAfterDeath = false;
    // Whatever the result. A death certificate the server refused is not a
    // reason to stay connected, and the registered Will still covers us if the
    // link is cut instead.
    node->_client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
}

void MqttSparkplugNode::poll() {
    if (!_begun) return;
    _client.poll();

    // A 3.1.1 DISCONNECT WAITS FOR THE NDEATH, AND NOT FOR EVER.
    //
    // The disconnect is deferred until the death certificate is acknowledged,
    // because [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311]
    // requires that order. If the acknowledgement never comes the node would
    // sit half-disconnected indefinitely - which the client's own
    // acknowledgementTimeoutMs normally resolves, but that can be set to zero.
    // So there is a bound here too, and disconnect() means disconnect.
    if (_disconnectAfterDeath &&
        mq_elapsed(_deathAskedAt, mq_millis()) > 10000) {
        _disconnectAfterDeath = false;
        _client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
    }

    // A BIRTH OR A SUBSCRIPTION THAT FAILED IS OWED, and retried - once a
    // second, while the session lasts. See _birthOwed in the header for the
    // node this was: connected, a primary host online, and never born.
    if ((_birthOwed || _subscriptionsOwed) && _client.isConnected() &&
        !_disconnectAfterDeath && !_reconnectAfterOffline &&
        mq_elapsed(_owedTriedAt, mq_millis()) >= 1000) {
        _owedTriedAt = mq_millis();
        if (_subscriptionsOwed) _subscriptionsOwed = !subscribeCommands();
        if (_birthOwed) {
            if (_born) {
                _birthOwed = false;
            } else if (!hasPrimaryHost() || _primaryHostOnline) {
                _birthOwed = !publishBirth();
            }
        }
    }

    // ANYTHING OWED GETS ANOTHER CHANCE, every poll.
    //
    // A birth or a device birth refused for want of queue room is owed, not
    // lost. Without this the only retry was the next reconnect, so a momentary
    // queue-full at the wrong instant cost a device until the link dropped.
    if (_born && _client.isConnected()) {
        for (size_t index = 0; index < _deviceCount; index++) {
            if (_devices[index].wantsBirth) {
                _devices[index].online = true;
                if (publishDeviceBirth(_devices[index])) {
                    _devices[index].wantsBirth = false;
                } else {
                    _devices[index].online = false;
                }
            }
        }
    }

    // The other half of a valid offline STATE: reconnect, on the next server.
    // Done here rather than inside handleState() because the disconnect it
    // follows has not finished at that point - the client is still draining -
    // and a connect() during it would be refused as Duplicate.
    if (_reconnectAfterOffline && !_client.isConnected() &&
        _client.state() == MqttState::Idle) {
        _reconnectAfterOffline = false;
        connect();
    }
}

// =============================================================================
// SESSION
// =============================================================================

void MqttSparkplugNode::sessionTrampoline(void* context, bool sessionPresent) {
    static_cast<MqttSparkplugNode*>(context)->sessionEstablished(sessionPresent);
}

void MqttSparkplugNode::stateTrampoline(void* context, MqttState state,
                                        MqttResult reason) {
    static_cast<MqttSparkplugNode*>(context)->stateChanged(state, reason);
}

void MqttSparkplugNode::messageTrampoline(void* context,
                                          const MqttMessage& message) {
    static_cast<MqttSparkplugNode*>(context)->handleMessage(message);
}

void MqttSparkplugNode::sessionEstablished(bool sessionPresent) {
    (void)sessionPresent;   // Always false: Clean Start is mandatory.

    // Subscriptions first. [tck-id-message-flow-edge-node-ncmd-subscribe] makes
    // the NCMD subscription mandatory "as Edge Nodes MUST be able to respond to
    // rebirth requests", and a birth published before the subscription exists
    // could be answered with a rebirth request we would not hear.
    _stateSubscribeRefused = MqttResult::Ok;
    _commandSubscribeRefused = MqttResult::Ok;
    _stateMessages = 0;
    _stateStale = 0;
    _lastStateOnline = false;
    _birthOwed = false;
    _owedTriedAt = mq_millis();
    _subscriptionsOwed = !subscribeCommands();

    if (hasPrimaryHost()) {
        // [tck-id-operational-behavior-edge-node-birth-sequence-wait] The Edge
        // Node MUST also wait to publish its BIRTH sequence until an
        // online=true STATE message is received by the Edge Node.
        //
        // The STATE message is retained, so subscribing is usually enough to
        // get it immediately - but not always, and the node simply waits.
        _primaryHostOnline = false;
        _sawPrimaryHostOnline = false;
        return;
    }

    _birthOwed = !publishBirth();
}

void MqttSparkplugNode::stateChanged(MqttState state, MqttResult reason) {
    (void)reason;
    // Reconnecting is the state the client enters once the link is gone and the
    // backoff is running - which is the moment the next CONNECT's Will has to
    // be rewritten, because the client will send it without asking again.
    if (state == MqttState::Reconnecting || state == MqttState::Idle ||
        state == MqttState::Connecting) {
        _birthOwed = false;
        _subscriptionsOwed = false;
        if (!_born) return;
        _born = false;
        for (size_t index = 0; index < _deviceCount; index++) {
            // A device is not offline - the LINK is. Its DBIRTH is owed again
            // once the node births, because a Host Application treats an
            // NDEATH as taking every device with it
            // [tck-id-operational-behavior-edge-node-termination-host-action-
            // ndeath-devices-offline].
            if (_devices[index].online) {
                _devices[index].wantsBirth = true;
                _devices[index].online = false;
            }
        }

        // The next CONNECT needs a new bdSeq in its Will, and the client's own
        // reconnect will send it. So the config is updated NOW, while the link
        // is down, and the client picks it up on its next attempt - which is
        // what reconfigure() is for and why autoReconnect can stay on.
        if (_begun) {
            _bdSeq++;
            _seq = 0;
            applyConnectionConfig();
            _client.reconfigure(_connection);
        }
    }
}

bool MqttSparkplugNode::subscribeCommands() {
    bool ok = true;

    // QoS 1, mandatory. [tck-id-message-flow-edge-node-ncmd-subscribe] "It MUST
    // subscribe on this topic with a QoS of 1." Not the library's QoS 0
    // default: a lost rebirth request is a node that never recovers.
    if (mq_sparkplugNodeCommandFilter(_topic, sizeof _topic, _group.c_str(),
                                      _node.c_str()) > 0) {
        ok = mq_accepted(_client.subscribe(_topic, MQ_QOS_1, &commandSubscribed,
                                           this)) && ok;
    } else {
        ok = false;
    }

    // One filter for every device, not one per device.
    // [tck-id-message-flow-device-dcmd-subscribe] applies "if the Device
    // supports writing to outputs", so this is registered only when something
    // is actually writable - a subscription nobody needs still costs a SUBSCRIBE
    // and broker-side matching on every message.
    bool anyDeviceWritable = false;
    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].device >= 0 && _points[index].writable) {
            anyDeviceWritable = true;
            break;
        }
    }
    if (anyDeviceWritable) {
        if (mq_sparkplugDeviceCommandFilter(_topic, sizeof _topic,
                                            _group.c_str(),
                                            _node.c_str()) > 0) {
            ok = mq_accepted(_client.subscribe(_topic, MQ_QOS_1)) && ok;
        } else {
            ok = false;
        }
    }

    // [tck-id-payloads-state-subscribe] equivalent for an edge node: it must
    // watch the primary host's STATE topic to know whether to birth at all.
    if (hasPrimaryHost()) {
        if (mq_sparkplugBuildStateTopic(_topic, sizeof _topic,
                                        _primaryHostId.c_str()) > 0) {
            ok = mq_accepted(_client.subscribe(_topic, MQ_QOS_1, &stateSubscribed,
                                               this)) && ok;
        } else {
            ok = false;
        }
    }

    return ok;
}

// =============================================================================
// PUBLISHING
// =============================================================================

bool MqttSparkplugNode::buildPayload(MqttSparkplugPayload& payload,
                                     MqttSparkplugType type, int16_t device,
                                     bool onlyChanged,
                                     bool includeBirthExtras) {
    payload.clear();
    payload.timestamp = utcNow();
    payload.hasTimestamp = true;

    const bool isBirth = mq_sparkplugIsBirth(type);

    for (size_t index = 0; index < _pointCount; index++) {
        MqttSparkplugPoint& point = _points[index];
        if (point.device != device) continue;

        if (onlyChanged && point.everPublished &&
            !point.value.differsFrom(point.published)) {
            continue;
        }

        MqttSparkplugMetric metric;
        // The name is interned into the payload's arena so the encoder has a
        // stable pointer even if the registry is edited between here and the
        // publish. std::string::c_str() would be stable too, and this also
        // keeps the payload self-contained for the tests to inspect.
        metric.name = payload.intern(point.name.c_str());
        if (metric.name == nullptr) {
            _lastPayloadError = MqttSparkplugError::TooManyMetrics;
            return false;
        }
        if (point.alias != 0) {
            metric.alias = point.alias;
            metric.hasAlias = true;
        }
        metric.value = point.value;

        // [tck-id-operational-behavior-data-publish-nbirth-values] For each
        // metric in the NBIRTH, the value MUST be included. A metric never
        // written yet is published as an explicit null rather than a made-up
        // zero - which is the difference between "not commissioned" and "zero".
        if (isBirth && point.units.length() > 0) {
            metric.addProperty(mq_spEngUnit(point.units.c_str()));
        }
        if (isBirth) {
            metric.addProperty(mq_spQuality(point.quality));
        }
        if (isBirth && point.description.length() > 0) {
            metric.addProperty(mq_spDescription(point.description.c_str()));
        }

        if (!payload.addMetric(metric)) {
            _lastPayloadError = MqttSparkplugError::TooManyMetrics;
            return false;
        }
    }

    if (includeBirthExtras) {
        // [tck-id-topics-nbirth-bdseq-included] A bdSeq number as a metric MUST
        // be included in the payload.
        // [tck-id-payloads-nbirth-bdseq-repeat] The value MUST match the one in
        // the Will registered on the CONNECT.
        MqttSparkplugMetric bdSeqMetric;
        bdSeqMetric.name = MQ_SP_METRIC_BDSEQ;
        bdSeqMetric.value = mq_spInt64((int64_t)_bdSeq);
        // [tck-id-payloads-alias-birth-requirement] NBIRTH and DBIRTH messages
        // MUST include BOTH a metric name and alias - and that is every metric
        // in the payload, not only the application's own. bdSeq and Node
        // Control/Rebirth went out with names and no aliases until an
        // independent host application in the docker rig counted them and
        // found two short.
        //
        // The aliases are reserved constants rather than taken from the
        // application's counter, so declaring a metric cannot collide with
        // them and the numbers do not shift when the metric set changes.
        bdSeqMetric.alias = MQ_SP_ALIAS_BDSEQ;
        bdSeqMetric.hasAlias = true;
        if (!payload.addMetric(bdSeqMetric)) {
            _lastPayloadError = MqttSparkplugError::TooManyMetrics;
            return false;
        }

        // [tck-id-operational-behavior-data-commands-rebirth] Every NBIRTH MUST
        // include a metric named Node Control/Rebirth.
        // -rebirth-datatype: Boolean. -rebirth-value: false in the NBIRTH.
        MqttSparkplugMetric rebirthMetric;
        rebirthMetric.name = MQ_SP_METRIC_NODE_REBIRTH;
        rebirthMetric.value = mq_spBool(false);
        rebirthMetric.alias = MQ_SP_ALIAS_NODE_REBIRTH;
        rebirthMetric.hasAlias = true;
        if (!payload.addMetric(rebirthMetric)) {
            _lastPayloadError = MqttSparkplugError::TooManyMetrics;
            return false;
        }
    }

    return true;
}

bool MqttSparkplugNode::sendPayload(MqttSparkplugType type,
                                    const char* deviceId,
                                    const MqttSparkplugPayload& payload,
                                    uint8_t qos, bool retain, bool asWill) {
    (void)asWill;

    MqttSparkplugError error = MqttSparkplugError::None;
    const size_t wrote = mq_sparkplugEncode(_scratch, sizeof _scratch, payload,
                                            type, error);
    _lastPayloadError = error;
    if (wrote == 0) return false;

    if (mq_sparkplugBuildTopic(_topic, sizeof _topic, type, _group.c_str(),
                               _node.c_str(), deviceId) == 0) {
        return false;
    }

    // publishSeries(), not publish().
    //
    // R6 - deterministic publish ordering - is what Sparkplug's seq field
    // depends on, and publishSeries() is the call that turns supersede off for
    // a message. Without it a later NDATA could replace an earlier one still in
    // the queue, and the far end would see a gap in the sequence and request a
    // rebirth.
    // mq_accepted(), not == Ok. MqttResult::Queued is the ACCEPTED result for
    // anything asynchronous, and Ok means "already completed" - so comparing
    // against Ok treats a perfectly good queued publish as a failure. Found by
    // a node that published its NBIRTH and then reported it had not birthed.
    return mq_accepted(
        _client.publishSeries(_topic, _scratch, wrote, qos, retain));
}

bool MqttSparkplugNode::publishBirth() {
    if (!_client.isConnected()) {
        _rebirthPending = false;
        return false;
    }

    // [tck-id-topics-nbirth-seq-num] The NBIRTH MUST include a sequence number
    // in the payload and it MUST have a value of 0.
    //
    // A second statement, [tck-id-payloads-sequence-num-zero-nbirth], says
    // "between 0 and 255 inclusive" - the two disagree, which is a known
    // inconsistency in the specification. Emitting 0 satisfies both.
    _seq = 0;

    if (!buildPayload(_outbound, MqttSparkplugType::NBirth, -1, false, true)) {
        // THE FLAG MUST NOT OUTLIVE THE ATTEMPT. _rebirthPending stops data
        // going out, and a birth that fails here with the flag still set would
        // silence the node for ever - a worse outcome than the gap in the
        // sequence a missing birth causes, and invisible.
        _rebirthPending = false;
        return false;
    }
    _outbound.seq = 0;
    _outbound.hasSeq = true;

    // [tck-id-topics-nbirth-mqtt] NBIRTH messages MUST be published with MQTT
    // QoS equal to 0 and retain equal to false. Not configurable.
    if (!sendPayload(MqttSparkplugType::NBirth, nullptr, _outbound, MQ_QOS_0,
                     false, false)) {
        _rebirthPending = false;   // see above
        return false;
    }

    _born = true;
    _birthCount++;
    // The birth is away, so data may flow again. Cleared here rather than in
    // rebirth() because the DBIRTHs below are part of the same sequence and
    // must not have data interleaved either.

    // Every metric now counts as published, because the NBIRTH carried all of
    // them. Without this, the first publishChanged() would resend the lot.
    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].device >= 0) continue;
        _points[index].published = _points[index].value;
        _points[index].everPublished = true;
    }

    // [tck-id-payloads-dbirth-order] All DBIRTH messages sent by an Edge Node
    // MUST be sent after the NBIRTH.
    for (size_t index = 0; index < _deviceCount; index++) {
        if (!_devices[index].wantsBirth) continue;
        _devices[index].online = true;
        // CLEARED ONLY ON SUCCESS. Clearing it first meant a DBIRTH refused
        // for want of queue room left the device marked online and never
        // birthed - so a Host Application had a device it would never hear
        // from, and the node had no record that it owed anything. The owed
        // birth now survives to the next poll.
        if (publishDeviceBirth(_devices[index])) {
            _devices[index].wantsBirth = false;
        } else {
            _devices[index].online = false;
        }
    }

    // The whole birth sequence is away. Data may flow again.
    _rebirthPending = false;
    return true;
}

bool MqttSparkplugNode::publishDeviceBirth(Device& device) {
    // Deliberately NOT gated on _rebirthPending: a DBIRTH is part of the birth
    // sequence the flag exists to protect, not traffic it should hold back.
    if (!_born || !_client.isConnected()) return false;

    const int deviceIndex = findDeviceIndex(device.id.c_str());
    if (deviceIndex < 0) return false;

    // [tck-id-payloads-dbirth-seq-inc] Every DBIRTH message MUST include a
    // sequence number value that is one greater than the previous sequence
    // number sent by the Edge Node. A DBIRTH is NOT seq 0 - only the NBIRTH is.
    nextSequence();

    if (!buildPayload(_outbound, MqttSparkplugType::DBirth,
                      (int16_t)deviceIndex, false, false)) {
        return false;
    }
    _outbound.seq = _seq;
    _outbound.hasSeq = true;

    if (!sendPayload(MqttSparkplugType::DBirth, device.id.c_str(), _outbound,
                     MQ_QOS_0, false, false)) {
        return false;
    }

    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].device != (int16_t)deviceIndex) continue;
        _points[index].published = _points[index].value;
        _points[index].everPublished = true;
    }
    _birthCount++;
    return true;
}

bool MqttSparkplugNode::set(const char* name,
                            const MqttSparkplugValue& value) {
    const int index = findPointIndex(name, -1);
    if (index < 0) return false;
    // The declared type wins. A caller handing an Int32 metric a float has made
    // a mistake, and coercing it would put a number on the wire that the
    // NBIRTH said was something else.
    if (value.type != _points[index].type) {
        _lastPayloadError = MqttSparkplugError::ValueTypeMismatch;
        return false;
    }
    _points[index].value = value;
    return true;
}

bool MqttSparkplugNode::setFor(const char* deviceId, const char* name,
                               const MqttSparkplugValue& value) {
    const int device = findDeviceIndex(deviceId);
    if (device < 0) return false;
    const int index = findPointIndex(name, (int16_t)device);
    if (index < 0) return false;
    if (value.type != _points[index].type) {
        _lastPayloadError = MqttSparkplugError::ValueTypeMismatch;
        return false;
    }
    _points[index].value = value;
    return true;
}

bool MqttSparkplugNode::publishData(int16_t device, bool onlyChanged) {
    if (!_born || !_client.isConnected()) return false;

    // [tck-id-operational-behavior-data-commands-rebirth-action-1] immediately
    // stop sending DATA messages. Not a failure - the values stay in the
    // registry and go out with the birth, and then normally afterwards.
    if (_rebirthPending) return true;

    // Nothing to say is not a failure, and publishing an empty NDATA would
    // still cost a sequence number and a message.
    bool anything = false;
    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].device != device) continue;
        if (!onlyChanged || !_points[index].everPublished ||
            _points[index].value.differsFrom(_points[index].published)) {
            anything = true;
            break;
        }
    }
    if (!anything) return true;

    const MqttSparkplugType type = device < 0 ? MqttSparkplugType::NData
                                              : MqttSparkplugType::DData;

    nextSequence();
    if (!buildPayload(_outbound, type, device, onlyChanged, false)) return false;
    _outbound.seq = _seq;
    _outbound.hasSeq = true;

    const char* deviceId =
        device < 0 ? nullptr : _devices[(size_t)device].id.c_str();

    // [tck-id-topics-ndata-mqtt] QoS 0, retain false. Not configurable.
    if (!sendPayload(type, deviceId, _outbound, MQ_QOS_0, false, false)) {
        // The sequence number has been spent. That is deliberate: a node that
        // reused it after a failed publish would send two different messages
        // with one number, and a Host Application tracking the sequence would
        // never detect the gap. A gap triggers a rebirth request, which
        // recovers; a duplicate does not.
        return false;
    }

    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].device != device) continue;
        _points[index].published = _points[index].value;
        _points[index].everPublished = true;
    }
    return true;
}

bool MqttSparkplugNode::publishChanged() {
    bool ok = publishData(-1, true);
    for (size_t index = 0; index < _deviceCount; index++) {
        if (!_devices[index].online) continue;
        ok = publishData((int16_t)index, true) && ok;
    }
    return ok;
}

bool MqttSparkplugNode::publishAll() {
    bool ok = publishData(-1, false);
    for (size_t index = 0; index < _deviceCount; index++) {
        if (!_devices[index].online) continue;
        ok = publishData((int16_t)index, false) && ok;
    }
    return ok;
}

bool MqttSparkplugNode::rebirth() {
    if (!_client.isConnected()) return false;

    // --- STOP SENDING DATA. ALL OF IT, INCLUDING WHAT IS ALREADY QUEUED. ---
    //
    // [tck-id-operational-behavior-data-commands-rebirth-action-1] "When an
    // Edge Node receives a Rebirth Request, it MUST IMMEDIATELY stop sending
    // DATA messages."
    //
    // Refusing to accept NEW data is not enough, and the reason is in the poll
    // order rather than in any race. A poll services RECEIVE before it services
    // the send queue - deliberately, because R7 needs an arriving session event
    // to be acted on before the queue drains behind it. So the rebirth request
    // is handled while an NDATA published earlier this scan is still sitting
    // unsent, and the queue then drains oldest first and puts that NDATA on the
    // wire AFTER the request. Every time there is data queued, not
    // occasionally: the only reason it can appear intermittent is that a run
    // with an empty queue at that instant looks like a pass.
    //
    // Withdrawing it is also the honest thing to do rather than a trick to
    // satisfy a checker. A rebirth means the far end's picture is stale and is
    // about to be replaced wholesale; the NBIRTH restates every metric's
    // current value, so a queued update carries nothing the birth does not
    // already carry, and it carries it with a sequence number from the old
    // series. Sending it would be redundant on a link and wrong in order.
    //
    // Anything already written is beyond reach and stays owed, which is why
    // this reduces the window rather than closing it absolutely - the one part
    // of "immediately" that no implementation can deliver.
    if (mq_sparkplugBuildTopic(_topic, sizeof _topic, MqttSparkplugType::NData,
                               _group.c_str(), _node.c_str(), nullptr) != 0) {
        _client.withdraw(_topic);
    }
    for (size_t index = 0; index < _deviceCount; index++) {
        if (mq_sparkplugBuildTopic(_topic, sizeof _topic,
                                   MqttSparkplugType::DData, _group.c_str(),
                                   _node.c_str(),
                                   _devices[index].id.c_str()) != 0) {
            _client.withdraw(_topic);
        }
    }

    // Every online device owes a DBIRTH again, because a Host Application
    // treats the NBIRTH as the start of a new picture.
    for (size_t index = 0; index < _deviceCount; index++) {
        if (_devices[index].online) {
            _devices[index].online = false;
            _devices[index].wantsBirth = true;
        }
    }
    _born = false;
    const bool birthed = publishBirth();
    if (!birthed && _client.isConnected()) {
        _birthOwed = true;
        _owedTriedAt = mq_millis();
    }
    return birthed;
}

// =============================================================================
// INBOUND
// =============================================================================

void MqttSparkplugNode::handleMessage(const MqttMessage& message) {
    MqttSparkplugTopicParts parts;
    if (!mq_sparkplugParseTopic((const char*)message.topic.data,
                                message.topic.length, parts)) {
        // Not a Sparkplug topic. Ignored rather than counted as an error: the
        // application may have subscribed to something of its own through
        // client().
        return;
    }

    if (parts.isState()) {
        // Only the host we are watching. Another host's STATE is not ours to
        // act on, and acting on it is how a node gets stranded.
        if (!hasPrimaryHost()) return;
        if (_primaryHostId != parts.group) return;
        handleState(message.payload.data, message.payload.length);
        return;
    }

    if (mq_sparkplugIsCommand(parts.type)) {
        // Ours only. A wildcard subscription can deliver a command for a
        // different node on the same group.
        if (_node != parts.node) return;
        if (_group != parts.group) return;
        handleCommand(parts, message.payload.data, message.payload.length);
    }
}

void MqttSparkplugNode::handleState(const uint8_t* payload, size_t length) {
    bool online = false;
    uint64_t timestamp = 0;
    if (!mq_sparkplugParseState(payload, length, online, timestamp)) return;
    _stateMessages++;

    // --- IS THIS MESSAGE THE CURRENT ONE, OR AN OLD ONE ARRIVING LATE? ---
    //
    // [tck-id-message-flow-edge-node-birth-publish-phid-wait-timestamp] "It
    // MUST validate the timestamp value is greater than or equal to the
    // previous STATE message timestamp value ... If no previous STATE message
    // timestamp value has been received by this Edge Node it MUST consider the
    // incoming STATE message to be the latest/valid."
    //
    // GREATER THAN OR EQUAL, and the equal half is load-bearing:
    // [tck-id-payloads-state-will-message-payload] requires a host's Will to
    // carry the SAME timestamp as its birth, so a host that dies publishes an
    // offline whose timestamp exactly equals the online we already have. A
    // strict > would discard the one death certificate the whole mechanism
    // exists to notice.
    //
    // A message that fails this is discarded WITHOUT being recorded. Recording
    // it would let one late arrival drag the comparison backwards and admit
    // every older message behind it.
    if (_sawState && timestamp < _lastStateAt) {
        _stateStale++;
        return;
    }
    _lastStateAt = timestamp;
    _sawState = true;
    _lastStateOnline = online;

    if (online) {
        // [tck-id-payloads-state-birth-payload] carries the timestamp the host
        // used in its Will. Remembering it is what makes the offline check
        // below possible.
        _primaryHostOnlineAt = timestamp;
        _sawPrimaryHostOnline = true;

        _primaryHostOnline = true;

        // [tck-id-operational-behavior-edge-node-birth-sequence-wait] The birth
        // sequence waits for this message, so this is where it goes.
        //
        // NOT ONLY ON A CHANGE FROM OFFLINE. That condition made a failed
        // birth permanent: the host was already "online", so every later
        // online STATE - including the one a host republishes - was ignored,
        // and the node stayed connected and unborn with nothing on its panel.
        // _born is what decides; a duplicate online on a born node does
        // nothing.
        if (!_born && _client.isConnected()) {
            _owedTriedAt = mq_millis();
            _birthOwed = !publishBirth();
        }
        return;
    }

    // --- WHEN AN OFFLINE STATE IS ACTIONABLE, AND WHEN IT IS JUST "NOT YET" ---
    //
    // [tck-id-operational-behavior-edge-node-termination-host-offline] It MUST
    // disconnect ... if the online JSON value is false AND the timestamp value
    // is GREATER THAN OR EQUAL TO THE PREVIOUS ONLINE STATE MESSAGE TIMESTAMP
    // VALUE.
    //
    // READ THE CONDITION CAREFULLY. It compares against the previous ONLINE
    // message. If there has never been one, there is no value to be greater
    // than or equal to and the condition CANNOT BE SATISFIED - so the node
    // must not disconnect. The sibling statement says the same thing from the
    // other side: -host-offline-reconnect governs a node that "disconnects
    // AFTER BEING IN A SPARKPLUG SESSION due to a valid offline STATE
    // message". This mechanism is about LEAVING a session, not about refusing
    // to join one.
    //
    // WHAT GETTING THIS WRONG LOOKS LIKE, because it is not obvious. A node
    // that acts on any offline STATE disconnects the moment it connects: the
    // host's last certificate is RETAINED, so the first thing a subscriber
    // receives is whatever the host last said - and if the host is down, that
    // is offline. The node disconnects, reconnects, receives the same retained
    // offline, disconnects again. A loop, with an NDEATH published each time
    // round, and the node never stays up long enough to see the host come
    // back. The Eclipse TCK's PrimaryHostTest is what caught it, and the
    // symptom read as "connected but never birthed" - which pointed at the
    // birth gate rather than at here.
    //
    // So before any online has been seen, an offline means exactly what
    // [tck-id-operational-behavior-edge-node-birth-sequence-wait] already
    // says: keep waiting.
    if (!_sawPrimaryHostOnline) {
        _primaryHostOnline = false;
        return;
    }

    // --- The stale-offline rule ---
    //
    // [tck-id-operational-behavior-edge-node-termination-host-offline-timestamp]
    // It MUST NOT disconnect if the timestamp value is less than the value
    // from the previous online STATE message.
    //
    // The specification's own explanation: "Due to how an MQTT connection can
    // be lost it is possible and likely that an old Host Application death
    // message could be delivered after a new Host Application MQTT session is
    // established." A node that simply believed online:false would tear itself
    // down every time the host reconnected, and the symptom would look exactly
    // like a network fault.
    if (timestamp < _primaryHostOnlineAt) return;

    _primaryHostOnline = false;
    _born = false;
    _birthOwed = false;

    // [-host-offline-reconnect] ... it MUST attempt to connect to the NEXT
    // MQTT Server in its connection list.
    //
    // Not this one again: the point of the whole mechanism is that a host
    // which has lost THIS server may still be reachable through another, and a
    // node that retried here would stay stranded.
    //
    // And the online flag is cleared so the next offline is "not yet" again
    // rather than another disconnect - which is what stops the loop above from
    // reappearing by a different route.
    _sawPrimaryHostOnline = false;

    advanceServer();

    // Disconnect, then come back - on the NEXT server. Both halves are
    // required, and the second one has to be driven because an intentional
    // disconnect concludes as Cancelled and the client's autoReconnect
    // correctly leaves that alone.
    _reconnectAfterOffline = true;
    _client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
}

void MqttSparkplugNode::handleCommand(const MqttSparkplugTopicParts& parts,
                                      const uint8_t* payload, size_t length) {
    const MqttSparkplugError error =
        mq_sparkplugDecode(payload, length, _inbound);
    if (error != MqttSparkplugError::None) {
        _lastPayloadError = error;
        _commandsRefused++;
        return;
    }

    int16_t device = -1;
    if (parts.hasDevice) {
        const int index = findDeviceIndex(parts.device);
        if (index < 0) {
            _commandsRefused++;
            return;
        }
        device = (int16_t)index;
    }

    for (size_t index = 0; index < _inbound.metricCount; index++) {
        MqttSparkplugMetric& metric = _inbound.metrics[index];

        // A command may carry only an alias
        // [tck-id-payloads-alias-data-cmd-requirement], so the alias table is
        // tried first and the name second.
        int point = -1;
        if (metric.hasAlias) point = findPointByAlias(metric.alias);
        if (point < 0 && metric.name != nullptr) {
            point = findPointIndex(metric.name, device);
        }

        // --- Rebirth ---
        //
        // BY NAME **OR BY ALIAS**, and the alias is not optional.
        // [tck-id-payloads-alias-data-cmd-requirement] says a command MUST
        // carry only an alias and MUST exclude the name - so once the birth
        // gives Node Control/Rebirth an alias, which
        // [tck-id-payloads-alias-birth-requirement] requires, a conformant
        // host sends the request with NO NAME AT ALL.
        //
        // Checked before the registry either way, because the rebirth metrics
        // are ones we publish in the birth and never store as points.
        const bool isRebirthRequest =
            (metric.name != nullptr &&
             (strcmp(metric.name, MQ_SP_METRIC_NODE_REBIRTH) == 0 ||
              strcmp(metric.name, MQ_SP_METRIC_DEVICE_REBIRTH) == 0)) ||
            (metric.hasAlias &&
             (metric.alias == MQ_SP_ALIAS_NODE_REBIRTH ||
              metric.alias == MQ_SP_ALIAS_DEVICE_REBIRTH));

        if (isRebirthRequest) {
            const bool asked = metric.value.type ==
                                       MqttSparkplugDataType::Boolean
                                   ? metric.value.as.boolean
                                   : metric.value.asNumber() != 0.0;
            if (asked) {
                // -action-1 first: stop sending data. Set BEFORE rebirth() so
                // nothing can slip in between, including from a handler this
                // call is about to run.
                _rebirthPending = true;
                // Then -action-2: a complete birth sequence.
                if (!rebirth()) {
                    // A FLAG WITH NOTHING TO PROTECT MUST NOT SURVIVE.
                    //
                    // publishBirth() clears it on every path it reaches,
                    // including failure - but rebirth() refuses before that
                    // when there is no session, and then nothing was withdrawn
                    // and no birth was queued. Left set, the flag would mute
                    // every subsequent publish until the next reconnect
                    // happened to birth again. Silence with the link up is the
                    // failure mode this whole audit is about.
                    _rebirthPending = false;
                }
                _commandsApplied++;

                MqttSparkplugCommand command;
                command.device = parts.hasDevice ? parts.device : nullptr;
                // The name we know it by, not the name it arrived with -
                // which, for a conformant host using aliases, is none.
                command.name = metric.name != nullptr
                                   ? metric.name
                                   : MQ_SP_METRIC_NODE_REBIRTH;
                command.isRebirth = true;
                command.value = metric.value;
                if (_commandHandler) _commandHandler(command);
                if (_commandContextHandler) {
                    _commandContextHandler(_commandContext, command);
                }
            }
            continue;
        }

        if (point < 0) {
            // A command for a metric we never declared. Counted, not applied -
            // a Host Application writing to the wrong name shows up here and
            // nowhere else.
            _commandsRefused++;
            continue;
        }
        if (!_points[point].writable) {
            // A command for a read-only metric. Also counted: a host that
            // thinks it can write a sensor reading should find out, and
            // applying it would let a stale command overwrite a live reading.
            _commandsRefused++;
            continue;
        }
        // [tck-id-payloads-metric-datatype-not-req] A command SHOULD NOT carry
        // a datatype; the one this node declared is applied, and a value that
        // cannot be that type is refused like a mismatch.
        if (metric.datatypeAbsent &&
            !mq_sparkplugApplyDatatype(metric, _points[point].type) &&
            !metric.valueUnsupported) {
            _commandsRefused++;
            continue;
        }
        if (metric.value.type != _points[point].type &&
            !metric.value.isNull) {
            _commandsRefused++;
            continue;
        }

        _points[point].value = metric.value;
        _points[point].commandPending = true;
        _commandsApplied++;

        MqttSparkplugCommand command;
        command.device = parts.hasDevice ? parts.device : nullptr;
        // The name as WE declared it, not as it arrived - a command carrying
        // only an alias has no name at all, and the caller wants the one they
        // wrote in their own code.
        command.name = _points[point].name.c_str();
        command.alias = _points[point].alias;
        command.value = metric.value;
        if (_commandHandler) _commandHandler(command);
        if (_commandContextHandler) {
            _commandContextHandler(_commandContext, command);
        }
    }
}

void MqttSparkplugNode::onCommand(MqttSparkplugCommandFn handler) {
    _commandHandler = handler;
}

void MqttSparkplugNode::onCommand(MqttSparkplugCommandContextFn handler,
                                  void* context) {
    _commandContextHandler = handler;
    _commandContext = context;
}

bool MqttSparkplugNode::commandPending(const char* name) const {
    const int index = findPointIndex(name, -1);
    return index >= 0 && _points[index].commandPending;
}

bool MqttSparkplugNode::take(const char* name, MqttSparkplugValue& into) {
    const int index = findPointIndex(name, -1);
    if (index < 0) return false;
    if (!_points[index].commandPending) return false;
    into = _points[index].value;
    // Cleared on the way out, so two callers cannot both act on one command -
    // the same rule MqttStatus follows for its edge counters.
    _points[index].commandPending = false;
    return true;
}

// THE BROKER'S ANSWER TO A SUBSCRIPTION, as opposed to the link going away
// with it outstanding - which is not a refusal and is retried with the session.
static bool mq_spBrokerRefused(MqttResult result) {
    switch (result) {
    case MqttResult::Refused:
    case MqttResult::NotAuthorized:
    case MqttResult::TopicNameInvalid:
    case MqttResult::QuotaExceededByPeer:
    case MqttResult::NotSupportedByPeer:
    case MqttResult::PacketIdentifierInUse:
        return true;
    default:
        return false;
    }
}

void MqttSparkplugNode::stateSubscribed(void* context, MqttResult result,
                                        uint16_t packetId) {
    (void)packetId;
    MqttSparkplugNode* node = static_cast<MqttSparkplugNode*>(context);
    node->_stateSubscribeRefused =
        mq_spBrokerRefused(result) ? result : MqttResult::Ok;
}

void MqttSparkplugNode::commandSubscribed(void* context, MqttResult result,
                                          uint16_t packetId) {
    (void)packetId;
    MqttSparkplugNode* node = static_cast<MqttSparkplugNode*>(context);
    if (mq_spBrokerRefused(result)) node->_commandSubscribeRefused = result;
}

// SHORT ON PURPOSE: these land on a 64-character STRING pin on a PLC panel,
// and a sentence cut off before its reason is worse than a terse one.
const char* MqttSparkplugNode::waitingFor() const {
    if (!_client.isConnected() || _born) return "";

    if (_stateSubscribeRefused != MqttResult::Ok) {
        snprintf(_waitText, sizeof _waitText,
                 "broker refused STATE/%s subscription (%s)",
                 _primaryHostId.c_str(), mq_resultName(_stateSubscribeRefused));
        return _waitText;
    }
    if (_commandSubscribeRefused != MqttResult::Ok) {
        snprintf(_waitText, sizeof _waitText,
                 "broker refused NCMD/DCMD subscription (%s)",
                 mq_resultName(_commandSubscribeRefused));
        return _waitText;
    }
    // BEFORE "waiting for the host": a STATE subscription that did not go is
    // the reason the host's STATE never arrives, and saying "waiting for the
    // host" over it would send an engineer to the wrong PLC.
    if (_subscriptionsOwed) return "subscribe refused locally - retrying";
    if (hasPrimaryHost() && !_primaryHostOnline) {
        // Which of the three, because each is a different fault: nothing has
        // arrived (the host has never connected to THIS broker, or its STATE
        // is not retained), the host says it is offline, or every STATE that
        // arrived was older than one already seen.
        const char* why = _stateMessages == 0 ? "no STATE from"
                          : !_lastStateOnline ? "offline STATE from"
                          : _stateStale > 0   ? "only stale STATE from"
                                              : "waiting for";
        snprintf(_waitText, sizeof _waitText, "%s primary host '%s'", why,
                 _primaryHostId.c_str());
        return _waitText;
    }
    if (_birthOwed) {
        if (_lastPayloadError != MqttSparkplugError::None) {
            snprintf(_waitText, sizeof _waitText,
                     "birth cannot be built: %s",
                     mq_sparkplugErrorName(_lastPayloadError));
            return _waitText;
        }
        return "birth could not be queued - retrying";
    }
    return "starting the session";
}

const char* MqttSparkplugNode::lastErrorText() const {
    // The library's own result name, not the Sparkplug one: a connection that
    // failed failed for an MQTT reason, and lastPayloadError() is where a
    // codec fault shows up.
    return mq_resultName(_client.status().lastError);
}

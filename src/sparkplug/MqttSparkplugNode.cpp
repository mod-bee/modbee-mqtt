// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MqttSparkplugNode.h"

#include <math.h>
#include <new>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include "../MqttNoExcept.h"


// =============================================================================
// CONSTRUCTION
// =============================================================================

MqttSparkplugNode::MqttSparkplugNode() MQ_NOEXCEPT : _client(_ownClient) {
    _topic[0] = '\0';
}

MqttSparkplugNode::MqttSparkplugNode(MqttTransport& transport) MQ_NOEXCEPT
    : _client(_ownClient) {
    _topic[0] = '\0';
    _net.borrow(transport);
}

MqttSparkplugNode::MqttSparkplugNode(MqttClient& shared) MQ_NOEXCEPT
    : _client(shared), _shared(true) {
    _topic[0] = '\0';
}

MqttSparkplugNode::~MqttSparkplugNode() {
    // The client concludes what is outstanding before the transport it is
    // bound to goes away with _net. A shared one is its owner's; the node
    // only leaves it, and its Will with it.
    if (_begun && !_shared) _client.end();
    _client.detach(*this);
    if (_points != nullptr) {
        for (size_t index = 0; index < _pointCount; index++) {
            freeQueue(_points[index].queue);
            freeText(_points[index].text);
        }
        for (size_t index = 0; index < _pointCapacity; index++) {
            _points[index].~MqttSparkplugPoint();
        }
        mq_freeLarge(_points);
    }
    mq_freeLarge(_stateBlock);
    mq_freeLarge(_pointIndex);
    mq_freeLarge(_scratch);
}

#if MQTT_TLS_SOCKETS
// Before the client is begun only: it binds the transport it is given, and
// swapping one underneath it would leave it talking to the other.
bool MqttSparkplugNode::useTls(bool enabled) {
    return !_shared && !_begun && _net.useTls(enabled);
}

bool MqttSparkplugNode::trustFile(const char* path) {
    return !_shared && !_begun && _net.trustFile(path);
}

bool MqttSparkplugNode::pinnedKey(const char* fingerprint) {
    return !_shared && !_begun && _net.pin(fingerprint);
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

void MqttSparkplugNode::clearCredentials() {
    _connection.userName.clear();
    _connection.hasUserName = false;
    _connection.password.clear();
    _connection.hasPassword = false;
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
#if MQTT_LEAN
    if (strlen(host) > _servers[_serverCount].host.capacity()) return false;
#endif
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

// The slot a name starts its search at: FNV-1a over the name, seeded with the
// device so one name on two devices lands apart.
static uint32_t mq_spPointHash(const char* name, int16_t device) {
    uint32_t hash = 2166136261u ^ (uint32_t)(uint16_t)device;
    for (; *name != '\0'; name++) {
        hash ^= (uint8_t)*name;
        hash *= 16777619u;
    }
    return hash;
}

int MqttSparkplugNode::findPointIndex(const char* name, int16_t device) const {
    if (name == nullptr) return -1;
    // Case SENSITIVE. [tck-id-case-sensitivity-metric-names]. A lookup that
    // folded case would let a PLC declare "Level" and write "LEVEL" and
    // silently hit the same metric - and then a Host Application shows one
    // tag where the engineer wrote two.
    if (_pointSlots != 0) {
        const size_t mask = _pointSlots - 1;
        for (size_t slot = mq_spPointHash(name, device) & mask;;
             slot = (slot + 1) & mask) {
            const uint32_t entry = _pointIndex[slot];
            if (entry == 0) return -1;
            const MqttSparkplugPoint& point = _points[entry - 1];
            if (point.device == device && point.name == name) {
                return (int)(entry - 1);
            }
        }
    }
    for (size_t index = 0; index < _pointCount; index++) {
        if (_points[index].device != device) continue;
        if (_points[index].name == name) return (int)index;
    }
    return -1;
}

// A point just declared goes into the name index - or the index is rebuilt,
// twice the size, when it would be more than half full. Without the memory
// for that the index is dropped and findPointIndex() searches instead: slower
// to declare, never wrong.
void MqttSparkplugNode::indexPoint(size_t index) {
    if (_pointCount * 2 > _pointSlots) {
        size_t slots = _pointSlots != 0 ? _pointSlots * 2 : 64;
        while (slots < _pointCount * 2) slots *= 2;
        uint32_t* grown = (uint32_t*)mq_allocLarge(slots * sizeof(uint32_t));
        mq_freeLarge(_pointIndex);
        _pointIndex = grown;
        _pointSlots = grown != nullptr ? slots : 0;
        if (grown == nullptr) return;
        memset(grown, 0, slots * sizeof(uint32_t));
        for (size_t i = 0; i < _pointCount; i++) {
            size_t slot = mq_spPointHash(_points[i].name.c_str(),
                                         _points[i].device) & (slots - 1);
            while (grown[slot] != 0) slot = (slot + 1) & (slots - 1);
            grown[slot] = (uint32_t)(i + 1);
        }
        return;
    }
    if (_pointSlots == 0) return;
    const size_t mask = _pointSlots - 1;
    size_t slot = mq_spPointHash(_points[index].name.c_str(),
                                 _points[index].device) & mask;
    while (_pointIndex[slot] != 0) slot = (slot + 1) & mask;
    _pointIndex[slot] = (uint32_t)(index + 1);
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
#if MQTT_LEAN
    if (strlen(name) > MQ_SP_POINT_NAME) return 0;   // a board's name field (MqttSparkplugNode.h)
#endif
    // The node's metric limit - small on a small board, see metricLimit().
    // The two the library adds to a birth (bdSeq, Node Control/Rebirth) are
    // not points and do not count against it.
    if (_pointCount >= metricLimit()) return 0;

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
    // Nor a node metric with a template definition's name: both are metrics
    // in the NBIRTH.
    if (device < 0 && findTemplate((const uint8_t*)name, strlen(name)) >= 0) {
        return 0;
    }

    // UTF-8, and the specification discourages a list of punctuation in metric
    // names but forbids none of it - so this validates encoding only, and does
    // not second-guess what an engineer called their tag.
    if (mq_validateUtf8((const uint8_t*)name, strlen(name)) != MqttError::None) {
        return 0;
    }

    // Room for one more, allocated now rather than at birth so a refusal is
    // a refused declaration, which the declaring block reports, and not a
    // birth that cannot be built.
    if (!growPoints(_pointCount + 1)) return 0;

    // What a span metric (text, bytes, array, dataset, template) owns, and
    // nothing for a number.
    MqttSparkplugValue probe;
    probe.type = type;
    MqttSparkplugPointText* text = nullptr;
    if (probe.isSpan()) {
        void* raw = mq_allocLarge(sizeof(MqttSparkplugPointText));
        if (raw == nullptr) {
            _outOfMemory = true;
            return 0;
        }
        text = new (raw) MqttSparkplugPointText();
    }

    MqttSparkplugPoint& point = _points[_pointCount];
    point = MqttSparkplugPoint();
    point.name = name;
    point.type = type;
    point.device = device;
    point.writable = writable;
    point.quality = quality;
    point.text = text;
    if (units != nullptr) point.units = units;

    // Not written yet: an explicit null, never a made-up zero (see the birth).
    PointState& state = _state[_pointCount];
    state = PointState();
    state.type = (uint8_t)type;
    state.device = device;
    state.flags = kValueNull;
    if (text != nullptr) state.flags |= kText;

    // [tck-id-payloads-alias-uniqueness] ... MUST be a unique number across
    // this Edge Node's ENTIRE set of metrics. One counter for the node and
    // every device under it, which is why this is not per-device.
    point.alias = _aliasesEnabled ? _nextAlias++ : 0;

    _pointCount++;
    indexPoint(_pointCount - 1);
    // Never published: on its list, so one declared after the birth goes in
    // the next message, as it did when every call looked at every metric.
    markChanged(_pointCount - 1);
    return point.alias;
}

bool MqttSparkplugNode::knownInstance(const MqttSparkplugValue& value) const {
    if (value.type != MqttSparkplugDataType::Template || value.isNull) return true;
    bool isDefinition = false;
    const uint8_t* ref = nullptr;
    size_t refLength = 0;
    return mq_sparkplugTemplateHeader(value.as.bytes.data, value.as.bytes.length,
                                      isDefinition, ref, refLength) &&
           !isDefinition && ref != nullptr && findTemplate(ref, refLength) >= 0;
}

int MqttSparkplugNode::findTemplate(const uint8_t* name, size_t length) const {
    for (size_t index = 0; index < _templateCount; index++) {
        const TemplateDefinition& definition = _templates[index];
        if (definition.name.size() == length &&
            memcmp(definition.name.c_str(), name, length) == 0) {
            return (int)index;
        }
    }
    return -1;
}

uint64_t MqttSparkplugNode::declareTemplate(const char* name,
                                           const uint8_t* definition,
                                           size_t length) {
    if (name == nullptr || *name == '\0' || definition == nullptr) return 0;
#if MQTT_LEAN
    if (strlen(name) > MQ_SP_POINT_NAME) return 0;
#endif
    if (mq_validateUtf8((const uint8_t*)name, strlen(name)) != MqttError::None) {
        return 0;
    }
    // A definition, and only that: is_definition true, no template_ref.
    bool isDefinition = false;
    const uint8_t* ref = nullptr;
    size_t refLength = 0;
    if (!mq_sparkplugTemplateHeader(definition, length, isDefinition, ref,
                                    refLength) ||
        !isDefinition || ref != nullptr) {
        _lastPayloadError = MqttSparkplugError::TemplateReference;
        return 0;
    }
    // Its name is a metric name in the NBIRTH: unique among them.
    if (findTemplate((const uint8_t*)name, strlen(name)) >= 0 ||
        findPointIndex(name, -1) >= 0) {
        return 0;
    }
    if (!_templates.reserve(_templateCount + 1)) {
        _outOfMemory = true;
        return 0;
    }
    TemplateDefinition& added = _templates[_templateCount];
    if (!added.bytes.assign(definition, length)) {
        _outOfMemory = true;
        return 0;
    }
    added.name = name;
    // [tck-id-payloads-alias-uniqueness]: from the node's one counter.
    added.alias = _aliasesEnabled ? _nextAlias++ : 0;
    _templateCount++;
    return added.alias != 0 ? added.alias : 1;
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

const MqttSparkplugPoint* MqttSparkplugNode::findPointFor(const char* deviceId,
                                                         const char* name) const {
    const int device = findDeviceIndex(deviceId);
    if (device < 0) return nullptr;
    const int index = findPointIndex(name, (int16_t)device);
    return index < 0 ? nullptr : &_points[index];
}

size_t MqttSparkplugNode::metricLimit() const {
    if (_metricLimit != 0) return _metricLimit;
    return (size_t)-1;   // what the program declares, memory permitting
}

bool MqttSparkplugNode::growPoints(size_t wanted) {
    // THE STATE TABLE IS ONE CACHE LINE A METRIC on an ESP32, and 32 bytes on
    // a 64-bit host too. Growing it is a design decision, not a detail.
    static_assert(sizeof(PointState) == 32, "PointState is one cache line");
    if (wanted <= _pointCapacity) return true;

    // Doubling, from eight, and never past the limit - so a node's table is at
    // most twice what it declared and usually far less.
    size_t capacity = _pointCapacity == 0 ? 8 : _pointCapacity * 2;
    if (capacity < wanted) capacity = wanted;
    const size_t limit = metricLimit();
    if (capacity > limit) capacity = limit < wanted ? wanted : limit;

    // Both tables, or neither. The state table on a 32-byte boundary: one
    // cache line an entry.
    void* raw = mq_allocLarge(capacity * sizeof(MqttSparkplugPoint));
    void* stateBlock = mq_allocLarge(capacity * sizeof(PointState) + 32);
    // No room for the doubled tables: exactly what is wanted, if that fits.
    if ((raw == nullptr || stateBlock == nullptr) && capacity > wanted) {
        mq_freeLarge(raw);
        mq_freeLarge(stateBlock);
        capacity = wanted;
        raw = mq_allocLarge(capacity * sizeof(MqttSparkplugPoint));
        stateBlock = mq_allocLarge(capacity * sizeof(PointState) + 32);
    }
    if (raw == nullptr || stateBlock == nullptr) {
        mq_freeLarge(raw);
        mq_freeLarge(stateBlock);
        _outOfMemory = true;
        return false;
    }
    PointState* state = reinterpret_cast<PointState*>(
        ((uintptr_t)stateBlock + 31) & ~(uintptr_t)31);
    for (size_t index = 0; index < capacity; index++) {
        new (&state[index]) PointState();
    }
    if (_state != nullptr) {
        memcpy(state, _state, _pointCount * sizeof(PointState));
    }
    mq_freeLarge(_stateBlock);
    _stateBlock = stateBlock;
    _state = state;

    MqttSparkplugPoint* grown = static_cast<MqttSparkplugPoint*>(raw);
    for (size_t index = 0; index < capacity; index++) {
        if (index < _pointCount) {
            new (&grown[index]) MqttSparkplugPoint(std::move(_points[index]));
        } else {
            new (&grown[index]) MqttSparkplugPoint();
        }
    }
    if (_points != nullptr) {
        for (size_t index = 0; index < _pointCapacity; index++) {
            _points[index].~MqttSparkplugPoint();
        }
        mq_freeLarge(_points);
    }
    _points = grown;
    _pointCapacity = capacity;
    return true;
}

bool MqttSparkplugNode::deadband(const char* name, double absolute,
                                 double percent) {
    const int index = findPointIndex(name, -1);
    if (index < 0) return false;
    if (!(absolute >= 0.0) || !(percent >= 0.0)) return false;
    MqttSparkplugValue probe;
    probe.type = _points[index].type;
    if (!probe.isNumeric()) return false;
    return deadbandAt((size_t)index, absolute, percent);
}

bool MqttSparkplugNode::deadbandFor(const char* deviceId, const char* name,
                                    double absolute, double percent) {
    if (deviceId == nullptr || deviceId[0] == '\0') {
        return deadband(name, absolute, percent);
    }
    const int device = findDeviceIndex(deviceId);
    if (device < 0) return false;
    const int index = findPointIndex(name, (int16_t)device);
    if (index < 0) return false;
    if (!(absolute >= 0.0) || !(percent >= 0.0)) return false;
    MqttSparkplugValue probe;
    probe.type = _points[index].type;
    if (!probe.isNumeric()) return false;
    return deadbandAt((size_t)index, absolute, percent);
}

bool MqttSparkplugNode::changePending(const char* deviceId,
                                      const char* name) const {
    int16_t device = -1;
    if (deviceId != nullptr && deviceId[0] != '\0') {
        const int index = findDeviceIndex(deviceId);
        if (index < 0) return false;
        device = (int16_t)index;
    }
    const int index = findPointIndex(name, device);
    return index >= 0 && pointChanged((size_t)index);
}

int MqttSparkplugNode::indexOf(const char* deviceId, const char* name) const {
    int16_t device = -1;
    if (deviceId != nullptr && deviceId[0] != '\0') {
        const int index = findDeviceIndex(deviceId);
        if (index < 0) return -1;
        device = (int16_t)index;
    }
    return findPointIndex(name, device);
}

// =============================================================================
// THE TWO TABLES - see TWO TABLES in the header
// =============================================================================

// A number's or a Boolean's value as the state table keeps it: the bits
// differsFrom() compares and no others - a Float's four, a Boolean as 0 or 1,
// the rest all eight - so two values differ exactly when their bits do, and a
// scan compares two integers instead of calling differsFrom().
static uint64_t mq_spBitsOf(const MqttSparkplugValue& value) {
    switch (value.type) {
        case MqttSparkplugDataType::Boolean:
            return value.as.boolean ? 1u : 0u;
        case MqttSparkplugDataType::Float: {
            uint32_t bits;
            memcpy(&bits, &value.as.float32, sizeof bits);
            return bits;
        }
        default:
            return value.as.unsignedValue;
    }
}

static void mq_spSetBits(MqttSparkplugValue& value, uint64_t bits) {
    switch (value.type) {
        case MqttSparkplugDataType::Boolean:
            value.as.boolean = bits != 0;
            break;
        case MqttSparkplugDataType::Float: {
            const uint32_t low = (uint32_t)bits;
            memcpy(&value.as.float32, &low, sizeof low);
            break;
        }
        default:
            value.as.unsignedValue = bits;
            break;
    }
}

// differsFrom(), for two values of one number metric kept as bits.
static inline bool mq_spBitsDiffer(bool aNull, uint64_t a, bool bNull,
                                   uint64_t b) {
    if (aNull != bNull) return true;
    return !aNull && a != b;
}

MqttSparkplugValue MqttSparkplugNode::stateValue(size_t index) const {
    const PointState& state = _state[index];
    MqttSparkplugValue value;
    value.type = (MqttSparkplugDataType)state.type;
    value.isNull = (state.flags & kValueNull) != 0;
    if (state.flags & kText) {
        const MqttSparkplugPointText& text = *_points[index].text;
        value.as.bytes.data = text.data;
        value.as.bytes.length = text.length;
    } else {
        mq_spSetBits(value, state.value);
    }
    return value;
}

MqttSparkplugValue MqttSparkplugNode::publishedValue(size_t index) const {
    const PointState& state = _state[index];
    MqttSparkplugValue value;
    value.type = (MqttSparkplugDataType)state.type;
    value.isNull = (state.flags & kPublishedNull) != 0;
    if (state.flags & kText) {
        const MqttSparkplugPointText& text = *_points[index].text;
        value.as.bytes.data = (const uint8_t*)text.published.data();
        value.as.bytes.length = (uint32_t)text.published.size();
    } else {
        mq_spSetBits(value, state.published);
    }
    return value;
}

void MqttSparkplugNode::storeValue(size_t index,
                                   const MqttSparkplugValue& value) {
    PointState& state = _state[index];
    if (value.isNull) {
        state.flags |= kValueNull;
    } else {
        state.flags &= (uint16_t)~kValueNull;
    }
    if (state.flags & kText) {
        MqttSparkplugPointText& text = *_points[index].text;
        text.data = value.isNull ? nullptr : value.as.bytes.data;
        text.length = value.isNull ? 0 : value.as.bytes.length;
    } else {
        state.value = value.isNull ? 0 : mq_spBitsOf(value);
    }
}

void MqttSparkplugNode::storeCommand(size_t index,
                                     const MqttSparkplugValue& value) {
    if ((_state[index].flags & kText) == 0 || value.isNull) {
        storeValue(index, value);
        return;
    }
    // A host's text lives in the receive buffer, or in a queue slot that is
    // reused: copied into the metric's own storage, and the span pointed there.
    MqttSparkplugPointText& text = *_points[index].text;
    const MqttSparkplugValue owned = text.command.keep(value);
    if (owned.isNull) _outOfMemory = true;
    storeValue(index, owned);
}

void MqttSparkplugNode::markPublished(size_t index) {
    PointState& state = _state[index];
    const bool isNull = (state.flags & kValueNull) != 0;
    if (state.flags & kText) {
        // Owned: the application's buffer moves on, the copy does not.
        MqttSparkplugPointText& text = *_points[index].text;
        if (!isNull && !text.published.assign(text.data, text.length)) {
            _outOfMemory = true;
        }
    } else {
        state.published = state.value;
    }
    if (isNull) {
        state.flags |= kPublishedNull;
    } else {
        state.flags &= (uint16_t)~kPublishedNull;
    }
    state.flags |= kEverPublished;
    state.flags &= (uint16_t)~kReportNext;
}

bool MqttSparkplugNode::putValue(size_t index, const MqttSparkplugValue& value,
                                 bool& changed) {
    changed = false;
    if (index >= _pointCount) return false;
    const PointState& state = _state[index];
    // The declared type wins. A caller handing an Int32 metric a float has
    // made a mistake, and coercing it would put a number on the wire that the
    // NBIRTH said was something else.
    if (value.type != (MqttSparkplugDataType)state.type) {
        _lastPayloadError = MqttSparkplugError::ValueTypeMismatch;
        return false;
    }
    // An instance of a type this node's birth describes, and nothing else
    // ([tck-id-payloads-template-instance-ref],
    // [tck-id-payloads-template-definition-nbirth]).
    if (!knownInstance(value)) {
        _lastPayloadError = MqttSparkplugError::TemplateReference;
        return false;
    }
    // WHETHER IT CHANGED. Text handed over in the same buffer as last time - a
    // PLC STRING - may have been rewritten in place, and a buffer compared
    // with itself never differs, so that is compared with the copy last
    // published instead.
    if (!(state.flags & kText)) {
        changed = mq_spBitsDiffer(value.isNull,
                                  value.isNull ? 0 : mq_spBitsOf(value),
                                  (state.flags & kValueNull) != 0, state.value);
    } else if (!value.isNull && !(state.flags & kValueNull) &&
               value.as.bytes.data != nullptr &&
               value.as.bytes.data == _points[index].text->data) {
        changed = !(state.flags & kEverPublished) ||
                  value.differsFrom(publishedValue(index));
    } else {
        changed = value.differsFrom(stateValue(index));
    }
    storeValue(index, value);
    if (changed) markChanged(index);
    return true;
}

// --- The change lists ---

MqttSparkplugNode::ChangeList& MqttSparkplugNode::changesOf(int16_t device) {
    return device < 0 ? _nodeChanges : _devices[(size_t)device].changes;
}

void MqttSparkplugNode::markChanged(size_t index) {
    PointState& state = _state[index];
    if (state.flags & kListed) return;
    state.flags |= kListed;
    state.nextChanged = 0;
    ChangeList& list = changesOf(state.device);
    const uint32_t link = (uint32_t)index + 1;
    if (list.tail == 0) {
        list.head = link;
    } else {
        _state[list.tail - 1].nextChanged = link;
        if (link < list.tail) list.sorted = false;
    }
    list.tail = link;
}

// Into index order - the order the metrics were declared in, and the order a
// message carried them in when every call walked every metric. A scan marks
// them in that order nearly always, so this is nearly always skipped. A merge
// sort of the linked list itself: nothing allocated, n log n.
void MqttSparkplugNode::sortChanges(ChangeList& list) {
    if (list.sorted) return;
    uint32_t head = list.head;
    for (size_t width = 1;; width *= 2) {
        uint32_t merged = 0, tail = 0;
        size_t merges = 0;
        uint32_t left = head;
        while (left != 0) {
            merges++;
            uint32_t right = left;
            size_t leftSize = 0;
            for (size_t i = 0; i < width && right != 0; i++) {
                leftSize++;
                right = _state[right - 1].nextChanged;
            }
            size_t rightSize = width;
            while (leftSize > 0 || (rightSize > 0 && right != 0)) {
                uint32_t take;
                if (leftSize == 0) {
                    take = right;
                    right = _state[right - 1].nextChanged;
                    rightSize--;
                } else if (rightSize == 0 || right == 0 || left < right) {
                    take = left;
                    left = _state[left - 1].nextChanged;
                    leftSize--;
                } else {
                    take = right;
                    right = _state[right - 1].nextChanged;
                    rightSize--;
                }
                if (tail != 0) {
                    _state[tail - 1].nextChanged = take;
                } else {
                    merged = take;
                }
                tail = take;
            }
            left = right;
        }
        if (tail != 0) _state[tail - 1].nextChanged = 0;
        head = merged;
        if (merges <= 1) {
            list.head = head;
            list.tail = tail;
            list.sorted = true;
            return;
        }
    }
}

void MqttSparkplugNode::settleChanges(ChangeList& list, bool published) {
    uint32_t head = 0, tail = 0;
    for (uint32_t link = list.head; link != 0;) {
        const size_t index = link - 1;
        const uint32_t next = _state[index].nextChanged;
        // WHAT WENT, AND ONLY WHAT WENT, becomes the new reference. A value
        // held back by its deadband keeps the old one, so the next comparison
        // is still against what the far end actually has - which is how a
        // slow drift adds up and is eventually reported rather than creeping
        // past for ever.
        if (published && pointChanged(index)) markPublished(index);
        if (pointOwed(index)) {
            _state[index].nextChanged = 0;
            if (tail != 0) {
                _state[tail - 1].nextChanged = link;
            } else {
                head = link;
            }
            tail = link;
        } else {
            _state[index].flags &= (uint16_t)~kListed;
            _state[index].nextChanged = 0;
        }
        link = next;
    }
    list.head = head;
    list.tail = tail;
}

void MqttSparkplugNode::forgetChanges(ChangeList& list) {
    for (uint32_t link = list.head; link != 0;) {
        PointState& state = _state[link - 1];
        link = state.nextChanged;
        state.flags &= (uint16_t)~kListed;
        state.nextChanged = 0;
    }
    list = ChangeList();
}

// --- By index ---

bool MqttSparkplugNode::setAt(size_t index, const MqttSparkplugValue& value) {
    // WHETHER IT CHANGED. This answered true for every value it took, so a
    // block counting what moved counted every member, every scan.
    bool changed = false;
    return putValue(index, value, changed) && changed;
}

MqttSparkplugValue MqttSparkplugNode::valueAt(size_t index) const {
    if (index >= _pointCount) return MqttSparkplugValue();
    return stateValue(index);
}

bool MqttSparkplugNode::changePendingAt(size_t index) const {
    return index < _pointCount && pointChanged(index);
}

bool MqttSparkplugNode::commandPendingAt(size_t index) const {
    return index < _pointCount && (_state[index].flags & kCommandPending);
}

void MqttSparkplugNode::queueWrites(size_t depth) {
    _writeDepth = depth > 16 ? 16 : depth;
}

// ONE BLOCK for a metric's write queue, in PSRAM where there is some: the
// header, the values, and a text metric's texts. See MqttSparkplugWriteQueue.
bool MqttSparkplugNode::makeQueue(size_t index) {
    MqttSparkplugPoint& point = _points[index];
    if (point.queue != nullptr) return true;
    const size_t depth = _writeDepth;
    if (depth == 0) return false;
    typedef MqttSparkplugBytes QueuedText;
    const size_t head = (sizeof(MqttSparkplugWriteQueue) + 15) & ~(size_t)15;
    const size_t values = (depth * sizeof(MqttSparkplugValue) + 15) & ~(size_t)15;
    const bool text = point.text != nullptr;
    uint8_t* raw = (uint8_t*)mq_allocLarge(
        head + values + (text ? depth * sizeof(QueuedText) : 0));
    if (raw == nullptr) return false;
    MqttSparkplugWriteQueue* queue = new (raw) MqttSparkplugWriteQueue();
    queue->values = reinterpret_cast<MqttSparkplugValue*>(raw + head);
    for (size_t slot = 0; slot < depth; slot++) {
        new (&queue->values[slot]) MqttSparkplugValue();
    }
    if (text) {
        queue->texts = reinterpret_cast<QueuedText*>(raw + head + values);
        for (size_t slot = 0; slot < depth; slot++) {
            new (&queue->texts[slot]) QueuedText();
        }
    }
    queue->depth = (uint8_t)depth;
    point.queue = queue;
    return true;
}

void MqttSparkplugNode::freeQueue(MqttSparkplugWriteQueue* queue) {
    if (queue == nullptr) return;
    typedef MqttSparkplugBytes QueuedText;
    if (queue->texts != nullptr) {
        for (size_t slot = 0; slot < queue->depth; slot++) {
            queue->texts[slot].~QueuedText();
        }
    }
    queue->~MqttSparkplugWriteQueue();
    mq_freeLarge(queue);
}

void MqttSparkplugNode::freeText(MqttSparkplugPointText* text) {
    if (text == nullptr) return;
    text->~MqttSparkplugPointText();
    mq_freeLarge(text);
}

#if MQTT_LEAN
// On a board every writable metric's queue is made here, when the node
// connects, so a command arriving later never allocates.
void MqttSparkplugNode::prepareWriteQueues() {
    if (_writeDepth == 0) return;
    for (size_t index = 0; index < _pointCount; index++) {
        if (!_points[index].writable || _points[index].queue) continue;
        makeQueue(index);
    }
}
#endif

void MqttSparkplugNode::acceptWrite(size_t index,
                                    const MqttSparkplugValue& value) {
    MqttSparkplugPoint& point = _points[index];
    PointState& state = _state[index];
    if (_writeDepth > 0 && point.queue == nullptr) makeQueue(index);
    if (_writeDepth == 0 || point.queue == nullptr) {
        // The latest write wins - by choice (no queue), or for want of the
        // memory for one, which still keeps the command rather than none.
        storeCommand(index, value);
        state.flags |= kCommandPending | kReportNext;
        markChanged(index);
        return;
    }
    MqttSparkplugWriteQueue& queue = *point.queue;
    const size_t depth = queue.depth;
    if (queue.count == depth) {
        // FULL: the oldest goes and the newest is kept - the host's latest
        // word is the one that matters - and the drop is counted, never
        // silent.
        queue.head = (uint8_t)((queue.head + 1) % depth);
        queue.count--;
        point.writesLost++;
        _writesLost++;
    }
    const size_t slot = (queue.head + queue.count) % depth;
    MqttSparkplugValue& kept = queue.values[slot];
    kept = value;
    if (queue.texts != nullptr) {
        // Its bytes owned by the slot, and the span pointed there.
        kept = queue.texts[slot].keep(value);
        if (kept.isNull && !value.isNull) _outOfMemory = true;
    }
    queue.count++;
    state.flags |= kCommandPending;
}

bool MqttSparkplugNode::takeAt(size_t index, MqttSparkplugValue& into) {
    if (index >= _pointCount || !(_state[index].flags & kCommandPending)) {
        return false;
    }
    MqttSparkplugPoint& point = _points[index];
    PointState& state = _state[index];
    if (point.queue && point.queue->count > 0) {
        // The OLDEST write. Its text moves to the metric, so the span handed
        // over stays good until the next take - the slot is reused.
        MqttSparkplugWriteQueue& queue = *point.queue;
        storeCommand(index, queue.values[queue.head]);
        queue.head = (uint8_t)((queue.head + 1) % queue.depth);
        queue.count--;
        if (queue.count == 0) state.flags &= (uint16_t)~kCommandPending;
        // Taken is applied: it goes back to the host, deadband or not.
        state.flags |= kReportNext;
        markChanged(index);
        point.writesTaken++;
        into = stateValue(index);
        return true;
    }
    into = stateValue(index);
    state.flags &= (uint16_t)~kCommandPending;
    point.writesTaken++;
    return true;
}

bool MqttSparkplugNode::peekAt(size_t index, MqttSparkplugValue& into) const {
    if (index >= _pointCount || !(_state[index].flags & kCommandPending)) {
        return false;
    }
    const MqttSparkplugPoint& point = _points[index];
    if (point.queue && point.queue->count > 0) {
        // Its span is the slot's own text already.
        into = point.queue->values[point.queue->head];
        return true;
    }
    into = stateValue(index);
    return true;
}

uint32_t MqttSparkplugNode::writesLostAt(size_t index) const {
    return index < _pointCount ? _points[index].writesLost : 0;
}

uint32_t MqttSparkplugNode::writesTakenAt(size_t index) const {
    return index < _pointCount ? _points[index].writesTaken : 0;
}

bool MqttSparkplugNode::deadbandAt(size_t index, double absolute,
                                   double percent) {
    if (index >= _pointCount) return false;
    if (!(absolute >= 0.0) || !(percent >= 0.0)) return false;
    MqttSparkplugValue probe;
    probe.type = _points[index].type;
    if (!probe.isNumeric()) return absolute == 0.0 && percent == 0.0;
    _points[index].deadband = absolute;
    _points[index].deadbandPercent = percent;
    if (absolute > 0.0 || percent > 0.0) {
        _state[index].flags |= kDeadband;
    } else {
        _state[index].flags &= (uint16_t)~kDeadband;
    }
    return true;
}

bool MqttSparkplugNode::holdAt(size_t index, bool held) {
    if (index >= _pointCount) return false;
    if (held) {
        _state[index].flags |= kHeld;
    } else {
        _state[index].flags &= (uint16_t)~kHeld;
    }
    return true;
}

bool MqttSparkplugNode::isHeldAt(size_t index) const {
    return index < _pointCount && (_state[index].flags & kHeld);
}

bool MqttSparkplugNode::reportAt(size_t index) {
    if (index >= _pointCount) return false;
    _state[index].flags |= kReportNext;
    markChanged(index);
    return true;
}

bool MqttSparkplugNode::unitsAt(size_t index, const char* units) {
    if (index >= _pointCount) return false;
    if (units == nullptr) units = "";
    if (_points[index].units != units) _points[index].units = units;
    return true;
}

bool MqttSparkplugNode::rangeAt(size_t index, double low, double high) {
    if (index >= _pointCount) return false;
    MqttSparkplugPoint& point = _points[index];
    point.hasRange = low < high;
    point.engLow = point.hasRange ? low : 0.0;
    point.engHigh = point.hasRange ? high : 0.0;
    return true;
}

bool MqttSparkplugNode::pointChanged(size_t index) const {
    const PointState& state = _state[index];
    if (!(state.flags & kEverPublished)) return true;
    // A host's own write goes back to it whatever the deadband says.
    if (state.flags & kReportNext) return true;
    // A held metric goes on its own clock - reportAt() - never for moving.
    if (state.flags & kHeld) return false;
    if (!(state.flags & kText)) {
        if (!mq_spBitsDiffer((state.flags & kValueNull) != 0, state.value,
                             (state.flags & kPublishedNull) != 0,
                             state.published)) {
            return false;
        }
    } else if (!stateValue(index).differsFrom(publishedValue(index))) {
        return false;
    }

    // No deadband, or nothing a deadband can measure: any change is a change.
    if (!(state.flags & kDeadband)) return true;
    const MqttSparkplugValue value = stateValue(index);
    const MqttSparkplugValue published = publishedValue(index);
    if (!value.isNumeric() || value.isNull || published.isNull) return true;

    // The deadband itself is in the metric's record: read only for a value
    // that has moved.
    const MqttSparkplugPoint& point = _points[index];
    const double now = value.asNumber();
    const double was = published.asNumber();
    const double moved = fabs(now - was);
    // NaN compares false with everything, so a value that became or stopped
    // being NaN is never held back by a deadband - it is reported.
    if (moved != moved) return true;
    if (point.deadband > 0.0 && moved <= point.deadband) return false;
    if (point.deadbandPercent > 0.0 &&
        moved <= fabs(was) * point.deadbandPercent / 100.0) {
        return false;
    }
    return true;
}

bool MqttSparkplugNode::pointOwed(size_t index) const {
    const PointState& state = _state[index];
    if (!(state.flags & kEverPublished)) return true;
    if (state.flags & kReportNext) return true;
    if (!(state.flags & kText)) {
        return mq_spBitsDiffer((state.flags & kValueNull) != 0, state.value,
                               (state.flags & kPublishedNull) != 0,
                               state.published);
    }
    return stateValue(index).differsFrom(publishedValue(index));
}

// =============================================================================
// DEVICES
// =============================================================================

bool MqttSparkplugNode::addDevice(const char* deviceId) {
    if (!mq_sparkplugValidId(deviceId)) return false;
    // [tck-id-topic-structure-namespace-unique-device-id] The device_id MUST be
    // unique from other device_ids for this Edge Node.
    if (findDeviceIndex(deviceId) >= 0) return false;
    // One more device, as far as memory goes (int16_t numbers them).
    if (_deviceCount >= 32767) return false;
    if (!_devices.reserve(_deviceCount + 1)) {
        _outOfMemory = true;
        return false;
    }

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
    //
    // ALREADY OWED: poll() births it - after the NBIRTH, or a second after a
    // refusal - and only poll(). Publishing here as well sent the device's
    // birth twice whenever a caller asked again while the node still owed it
    // (a program re-asking for its device each scan after a rebirth).
    if (!_born || device.wantsBirth) {
        device.wantsBirth = true;
        return true;
    }

    // ONLINE ONLY WITH ITS DBIRTH SENT. A DBIRTH refused for want of queue room
    // (a burst of large births on a slow link) left the device marked online
    // and owed nothing: its DDATA then went out for a device the host had
    // never seen born, and a Host Application asks for a rebirth of
    // everything when that happens. Refused, it is owed like any other
    // device birth, and poll() tries again a second later.
    device.online = true;
    if (publishDeviceBirth(device)) return true;
    device.online = false;
    device.wantsBirth = true;
    deviceBirthRefused(true);
    return true;
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

// THE SESSION'S BIRTH IS OVER, whatever ended it: the link, a valid offline
// STATE, disconnect() or a new connect(). Every one of those paths has to owe
// the online devices a DBIRTH, because a Host Application treats the node's
// death as taking every device with it
// [tck-id-operational-behavior-edge-node-termination-host-action-ndeath-
// devices-offline] - and only the link-down path used to. The others cleared
// _born first, so when the link went down a moment later its handler saw a
// node already "not born" and skipped the devices: the node birthed again
// when the host came back, and its devices never did. Found on a real broker
// in testing.
void MqttSparkplugNode::endBirth() {
    for (size_t index = 0; index < _deviceCount; index++) {
        if (_devices[index].online) {
            _devices[index].wantsBirth = true;
            _devices[index].online = false;
        }
    }
    _born = false;
}

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
}

// EVERY CONNECT, the node's own client's or a shared one's, whoever starts
// it: the first, an automatic reconnect, a 3.1.1 fallback. The client calls
// this just before it encodes the packet, so the Will always carries the bdSeq
// of the session it opens - there is no window in which a reconnect can go
// out with the last one's.
void MqttSparkplugNode::prepareConnect(MqttConnectionConfig& connection) {
    _willArmed = false;
    if (!_active) return;

    connection.cleanStart = true;
    connection.sessionExpiryInterval = 0;
    // --- The Will: an NDEATH carrying this connection's bdSeq ---
    //
    // [tck-id-payloads-ndeath-bdseq] The NDEATH message MUST include the same
    // bdSeq number value that will be used in the associated NBIRTH message.
    // [tck-id-topics-ndeath-payload] ... MUST only include a single metric, the
    // bdSeq number.
    // [tck-id-topics-ndeath-seq] The NDEATH message MUST NOT include a
    // sequence number.
    // One metric, whose name is a literal: no arena needed.
    MqttSparkplugPayload will(1, 0);
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
#if MQTT_LEAN
    if (willSize > _willBytes.capacity()) error = MqttSparkplugError::Overflow;
#endif
    if (error == MqttSparkplugError::None && willSize > 0) {
        _willBytes.resize(willSize);
        const size_t wrote = mq_sparkplugEncode(_willBytes.data(), willSize, will,
                                                MqttSparkplugType::NDeath,
                                                error);
        if (wrote > 0) {

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
                connection.will.set(willTopic, _willBytes.data(),
                                    _willBytes.size(), MQ_QOS_1, false);

                // No Will Delay. A delay would need 5.0 and would mean a Host
                // Application waits to be told a node has gone - which is the
                // opposite of what a death certificate is for.
                connection.will.delayInterval = 0;
                _willArmed = true;
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
    _client.attach(*this);
    _client.claimWill(*this);
    _begun = true;
}

void MqttSparkplugNode::nextSession() {
    // [tck-id-topics-nbirth-bdseq-increment] The bdSeq number MUST start at
    // zero and increment by one on each new MQTT CONNECT.
    //
    // So the FIRST connect uses 0 and the second uses 1 - hence the flag rather
    // than incrementing unconditionally, which would start at 1.
    if (_bdSeqUsed) _bdSeq++;
    _bdSeqUsed = true;
    endBirth();
    _seq = 0;
}

bool MqttSparkplugNode::startShared() {
    if (!_client.attach(*this) || !_client.claimWill(*this)) {
        _willRefused = true;
        return false;
    }
    _willRefused = false;
    _begun = true;
    _active = true;
    nextSession();
    if (!sizeBuffers()) _outOfMemory = true;
#if MQTT_LEAN
    prepareWriteQueues();
#endif
    // A session already up, or a CONNECT already sent, went without the
    // NDEATH: the next one carries it. Refused only while the owner keeps
    // the client down, and then the owner's connect() carries it anyway.
    if (_client.state() != MqttState::Idle &&
        _client.state() != MqttState::Reconnecting) {
        _client.restart();
        _restartOwed = false;   // that restart is the one sizeBuffers() asked for
    }
    return true;
}

bool MqttSparkplugNode::connect() {
    if (_group.empty() || _node.empty()) return false;
    if (_shared) return startShared();
    if (_net.get() == nullptr) return false;
    if (_serverCount == 0) return false;
    _active = true;
    nextSession();

    // Before the client exists the first time: it takes its transmit buffer
    // size from _behaviour when it is begun, and never again on a board.
    if (!sizeBuffers()) _outOfMemory = true;
#if MQTT_LEAN
    prepareWriteQueues();
#endif

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

bool MqttSparkplugNode::rebind(MqttTransport& transport) {
    if (_shared) return false;   // the transport is the client owner's
    // The client first: it closes what the old transport holds, and the old
    // transport may be the one _net owns and is about to release.
    if (_begun) _client.rebind(transport);
    _net.borrow(transport);
    _disconnectAfterDeath = false;
    _reconnectAfterOffline = false;
    endBirth();
    return true;
}

void MqttSparkplugNode::disconnect() {
    if (!_begun) return;
    _active = false;
    _restartOwed = false;
    _cycleOwed = false;
    _restartAfterDeath = false;
    if (_shared) {
        // The connection stays up for everybody else on it, so nothing makes
        // the broker publish the Will: the NDEATH goes by hand, at either
        // version, and the next CONNECT goes without it.
        _disconnectAfterDeath = false;
        _reconnectAfterOffline = false;
        if (_willArmed && _client.isConnected()) publishNodeDeath();
        _client.releaseWill(*this);
        endBirth();
        return;
    }
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
    endBirth();
}

bool MqttSparkplugNode::publishNodeDeath() {
    // Its own one-metric payload: bdSeq's name is a literal, so no arena.
    MqttSparkplugPayload payload(1, 0);

    MqttSparkplugMetric bdSeqMetric;
    bdSeqMetric.name = MQ_SP_METRIC_BDSEQ;
    bdSeqMetric.value = mq_spInt64((int64_t)_bdSeq);
    payload.addMetric(bdSeqMetric);
    // No seq: [tck-id-topics-ndeath-seq].

    MqttSparkplugError error = MqttSparkplugError::None;
    const size_t needed =
        mq_sparkplugSize(payload, MqttSparkplugType::NDeath, error);
    if (needed == 0 || !ensureScratch(needed)) {
        _lastPayloadError = error;
        return false;
    }
    const size_t wrote = mq_sparkplugEncode(_scratch, _scratchSize, payload,
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
    if (node->_shared) {
        node->_restartAfterDeath = true;   // from poll(), not inside the client's
    } else {
        node->_client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
    }
}

void MqttSparkplugNode::cycleSession() {
    if (!_shared) {
        _reconnectAfterOffline = true;
        disconnect();
        return;
    }
    // A shared client: the same goodbye in the same order, then the owner's
    // connection again rather than the node's own connect(). From poll():
    // this is reached from inside the client's poll (a STATE message), and a
    // DISCONNECT sent from there would go out ahead of that message's PUBACK.
    endBirth();
    _cycleOwed = true;
}

void MqttSparkplugNode::restartShared(uint8_t reasonCode) {
    // The Will registered for THIS session already carries its bdSeq; the
    // numbers move on for the next one before its CONNECT is prepared.
    nextSession();
    if (!sizeBuffers()) _outOfMemory = true;
    _restartOwed = false;
    _client.restart(reasonCode);
}
void MqttSparkplugNode::poll() {
    if (!_begun) {
        publishOwed();   // answered: not born, nothing went
        return;
    }
    if (!_shared) _client.poll();   // a shared client's owner polls it
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
        if (_shared) {
            restartShared(MQ_REASON_NORMAL_DISCONNECTION);
        } else {
            _client.disconnect(MQ_REASON_NORMAL_DISCONNECTION);
        }
    }

    // A shared client's session ends from here, never from inside its poll:
    // the NDEATH first at 3.1.1, the Will by reason code at 5.0.
    if (_cycleOwed && !_disconnectAfterDeath) {
        _cycleOwed = false;
        if (_client.activeVersion() == MqttVersion::V311) {
            _disconnectAfterDeath = true;
            _deathAskedAt = mq_millis();
            if (!publishNodeDeath()) {
                _disconnectAfterDeath = false;
                restartShared(MQ_REASON_NORMAL_DISCONNECTION);
            }
        } else {
            restartShared(MQ_REASON_DISCONNECT_WITH_WILL);
        }
    }
    if (_restartAfterDeath) {
        _restartAfterDeath = false;
        restartShared(MQ_REASON_NORMAL_DISCONNECTION);
    }

    // A shared client restarts from here, never from inside its own poll -
    // and a session the host has seen born ends as Sparkplug says one ends,
    // with its NDEATH and the next bdSeq after it.
    if (_restartOwed) {
        _restartOwed = false;
        if (_active && _born) {
            cycleSession();
        } else if (_active) {
            _client.restart();
        }
    }
    // A BIRTH OR A SUBSCRIPTION THAT FAILED IS OWED, and retried - once a
    // second, while the session lasts. See _birthOwed in the header for the
    // node this was: connected, a primary host online, and never born.
    if ((_birthOwed || _subscriptionsOwed) && _client.isConnected() &&
        !_disconnectAfterDeath && !_reconnectAfterOffline && !_cycleOwed &&
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

    // AN OWED DEVICE BIRTH GETS ANOTHER CHANCE - once a second after one was
    // refused, as the owed NBIRTH above does.
    //
    // A birth or a device birth refused for want of queue room is owed, not
    // lost. Without this the only retry was the next reconnect, so a momentary
    // queue-full at the wrong instant cost a device until the link dropped.
    // It used to try on every poll: a full queue stays full for a while, and
    // each try built the device's whole birth to be refused again.
    if (_born && _client.isConnected() &&
        (!_deviceBirthWaiting ||
         mq_elapsed(_deviceBirthTriedAt, mq_millis()) >= 1000)) {
        bool refused = false;
        for (size_t index = 0; index < _deviceCount; index++) {
            if (_devices[index].wantsBirth) {
                _devices[index].online = true;
                if (publishDeviceBirth(_devices[index])) {
                    _devices[index].wantsBirth = false;
                } else {
                    _devices[index].online = false;
                    refused = true;
                }
            }
        }
        deviceBirthRefused(refused);
    }

    // ONE NDATA, AND ONE DDATA A DEVICE, for everything the program marked
    // since the last poll (publishAtPoll()) - after the births above, never
    // before them.
    publishOwed();

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

bool MqttSparkplugNode::message(const MqttMessage& message) {
    // Never taken: a host on the same client may watch the same namespace,
    // and the application's handler still sees everything.
    handleMessage(message);
    return false;
}
void MqttSparkplugNode::sessionEstablished(bool sessionPresent) {
    (void)sessionPresent;   // Always false: Clean Start is mandatory.
    if (!_active) return;   // a shared client's session; this node stood down
    if (_shared && !_willArmed && _lastPayloadError == MqttSparkplugError::None) {
        // Opened by a CONNECT sent before this node was there, so without its
        // NDEATH. A session the host could not see die is not one to birth
        // into: the next CONNECT carries it.
        _restartOwed = true;
        return;
    }
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
    if (state == MqttState::Reconnecting || state == MqttState::Idle ||
        state == MqttState::Connecting) {
        _birthOwed = false;
        _subscriptionsOwed = false;
        if (!_born) return;
        // A device is not offline - the LINK is. See endBirth().
        endBirth();

        // The next CONNECT needs a new bdSeq in its Will, and the client's
        // own reconnect will send it - prepareConnect() writes it there, so
        // autoReconnect can stay on and a shared client needs nothing more.
        if (_begun) {
            _bdSeq++;
            _seq = 0;
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

bool MqttSparkplugNode::ensureScratch(size_t bytes) {
    if (bytes <= _scratchSize) return true;
    // A little over, so a text metric that grows by a character does not
    // reallocate on the next message.
    const size_t want = bytes + bytes / 8 + 64;
    uint8_t* grown = static_cast<uint8_t*>(mq_allocLarge(want));
    if (grown == nullptr) return false;
    mq_freeLarge(_scratch);
    _scratch = grown;
    _scratchSize = want;
    return true;
}

bool MqttSparkplugNode::sizeBuffers() {
    // --- What the biggest message must hold ---
    //
    // A birth certificate: the NBIRTH carries every node metric plus bdSeq and
    // Node Control/Rebirth, a DBIRTH every metric of its device. Counted, not
    // guessed, because the metric set is closed by the time anything births.
    // One pass per slot (the node, then each device) rather than a table
    // sized by a device limit, which there is not. Once per connect.
    size_t writable = 0, writableNames = 0;
    size_t mostMetrics = 0, mostNames = 0;
    for (size_t slot = 0; slot <= _deviceCount; slot++) {
        size_t metrics = slot == 0 ? 2 : 0;   // bdSeq and Node Control/Rebirth
        size_t nameBytes = 0;
        for (size_t index = 0; index < _pointCount; index++) {
            const MqttSparkplugPoint& point = _points[index];
            if ((point.device < 0 ? 0 : (size_t)point.device + 1) != slot) continue;
            metrics++;
            nameBytes += point.name.size() + 1;
            if (point.writable) {
                writable++;
                writableNames += point.name.size() + 1;
            }
        }
        if (slot == 0) {
            // The template definitions go in the NBIRTH too.
            metrics += _templateCount;
            for (size_t index = 0; index < _templateCount; index++) {
                nameBytes += _templates[index].name.size() + 1;
            }
        }
        if (metrics > mostMetrics) mostMetrics = metrics;
        if (nameBytes > mostNames) mostNames = nameBytes;
    }
    if (!_outbound.reserve(mostMetrics, mostNames + 1)) return false;

    // A command writes writable metrics, and Rebirth - so that many, with a
    // little room for names a host sent that this node never declared (they
    // are refused one by one rather than failing the whole message).
    if (!_inbound.reserve(writable + 8, writableNames + 256)) return false;

    // --- The bytes: the encode buffer, and the client's transmit buffer ---
    //
    // The client's buffer is allocated once, when it is begun, and on a board
    // it never grows - so a birth larger than its 4 kB default could not be
    // sent at all, and a hundred-metric node never birthed. It is told the
    // size of the real birth certificate before it exists, with room for the
    // topic and for text values that grow.
    size_t largest = 0;
    MqttSparkplugError error = MqttSparkplugError::None;
    if (buildPayload(_outbound, MqttSparkplugType::NBirth, -1, false, true)) {
        const size_t bytes =
            mq_sparkplugSize(_outbound, MqttSparkplugType::NBirth, error);
        if (bytes > largest) largest = bytes;
    }
    for (size_t index = 0; index < _deviceCount; index++) {
        if (buildPayload(_outbound, MqttSparkplugType::DBirth, (int16_t)index,
                         false, false)) {
            const size_t bytes =
                mq_sparkplugSize(_outbound, MqttSparkplugType::DBirth, error);
            if (bytes > largest) largest = bytes;
        }
    }
    _outbound.clear();
    if (largest > 0 && !ensureScratch(largest)) return false;

    const size_t packet = largest + largest / 4 + 512;
    if (_shared) {
        // A shared client grows its buffer only while the link is down. Room
        // to grow is asked for, and taken the next time the link is down
        // anyway; a new connection is owed only when the birth itself would
        // not go - not when it merely ate into the room to grow. A STRING
        // metric written longer since the last birth did exactly that, and
        // the reconnect it caused was one the host never saw die.
        _client.reserveTransmit(packet);
        if (_client.isConnected() && _client.transmitCapacity() < largest + 512) {
            _restartOwed = true;
        }
    } else if (!_begun) {
        if (packet > _behaviour.transmitBufferSize) {
            _behaviour.transmitBufferSize = packet;
        }
    }
    return true;
}

bool MqttSparkplugNode::buildPayload(MqttSparkplugPayload& payload,
                                     MqttSparkplugType type, int16_t device,
                                     bool onlyChanged,
                                     bool includeBirthExtras) {
    payload.clear();
    payload.timestamp = utcNow();
    payload.hasTimestamp = true;

    const bool isBirth = mq_sparkplugIsBirth(type);

    if (onlyChanged) {
        // THE CHANGE LIST, and nothing else: in index order, as when every
        // metric was walked.
        ChangeList& list = changesOf(device);
        sortChanges(list);
        for (uint32_t link = list.head; link != 0;
             link = _state[link - 1].nextChanged) {
            if (!pointChanged(link - 1)) continue;
            if (!addPointMetric(payload, link - 1, isBirth)) return false;
        }
    } else {
        for (size_t index = 0; index < _pointCount; index++) {
            if (_state[index].device != device) continue;
            if (!addPointMetric(payload, index, isBirth)) return false;
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
        // independent host application in a test setup counted them and
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

        // [tck-id-topics-nbirth-templates] Every template definition, in the
        // NBIRTH and only there. Named, and aliased like every birth metric.
        for (size_t index = 0; index < _templateCount; index++) {
            const TemplateDefinition& definition = _templates[index];
            MqttSparkplugMetric metric;
            metric.name = payload.intern(definition.name.c_str());
            if (metric.name == nullptr) {
                _lastPayloadError = MqttSparkplugError::TooManyMetrics;
                return false;
            }
            if (definition.alias != 0) {
                metric.alias = definition.alias;
                metric.hasAlias = true;
            }
            metric.value = mq_spTemplate(definition.bytes.data(),
                                         (uint32_t)definition.bytes.size());
            if (!payload.addMetric(metric)) {
                _lastPayloadError = MqttSparkplugError::TooManyMetrics;
                return false;
            }
        }
    }

    return true;
}

bool MqttSparkplugNode::addPointMetric(MqttSparkplugPayload& payload,
                                       size_t index, bool isBirth) {
    const MqttSparkplugPoint& point = _points[index];
    MqttSparkplugMetric metric;
    if (isBirth || point.alias == 0) {
        // The name is interned into the payload's arena so the encoder has a
        // stable pointer even if the registry is edited between here and the
        // publish. std::string::c_str() would be stable too, and this also
        // keeps the payload self-contained for the tests to inspect.
        metric.name = payload.intern(point.name.c_str());
        if (metric.name == nullptr) {
            _lastPayloadError = MqttSparkplugError::TooManyMetrics;
            return false;
        }
    } else {
        // A data message carries the alias alone
        // [tck-id-payloads-alias-data-cmd-requirement], so the name is never
        // read: pointed at, not copied, as publishRecords() does.
        metric.name = point.name.c_str();
    }
    if (point.alias != 0) {
        metric.alias = point.alias;
        metric.hasAlias = true;
    }
    metric.value = stateValue(index);

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
    if (isBirth && point.hasRange) {
        metric.addProperty(mq_spEngLow(point.engLow));
        metric.addProperty(mq_spEngHigh(point.engHigh));
    }
    if (isBirth && point.description.length() > 0) {
        metric.addProperty(mq_spDescription(point.description.c_str()));
    }

    if (!payload.addMetric(metric)) {
        _lastPayloadError = MqttSparkplugError::TooManyMetrics;
        return false;
    }
    return true;
}

bool MqttSparkplugNode::sendPayload(MqttSparkplugType type,
                                    const char* deviceId,
                                    const MqttSparkplugPayload& payload,
                                    uint8_t qos, bool retain, bool asWill) {
    (void)asWill;

    MqttSparkplugError error = MqttSparkplugError::None;
    // Sized exactly, then encoded - the buffer grows to the largest message
    // this node has sent, which after the first birth is almost never.
    const size_t needed = mq_sparkplugSize(payload, type, error);
    if (needed == 0) {
        _lastPayloadError = error;
        return false;
    }
    if (!ensureScratch(needed)) {
        _lastPayloadError = MqttSparkplugError::Overflow;
        return false;
    }
    const size_t wrote = mq_sparkplugEncode(_scratch, _scratchSize, payload,
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

    // A metric declared after connect() is in this birth too, so the payloads
    // grow to it. (The client's transmit buffer cannot - it was sized at the
    // first connect - which is why the blocks declare everything first.)
    if (!sizeBuffers()) _outOfMemory = true;

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
        if (_state[index].device >= 0) continue;
        markPublished(index);
    }
    forgetChanges(_nodeChanges);

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
            deviceBirthRefused(true);
        }
    }

    // The whole birth sequence is away. Data may flow again.
    _rebirthPending = false;
    return true;
}

// Whether an owed DBIRTH was just refused: the next try then waits a second.
void MqttSparkplugNode::deviceBirthRefused(bool refused) {
    _deviceBirthWaiting = refused;
    if (refused) _deviceBirthTriedAt = mq_millis();
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
    //
    // NOT SENT, NOT SPENT. A DBIRTH refused here goes nowhere and is owed, so
    // its number is the one the next message - this DBIRTH again - carries.
    // Each refused try used to spend one, and the gap that left told the
    // Host Application a message was lost: it asked for a rebirth, and the
    // node sent its whole birth again with nothing changed.
    const uint8_t sequenceBefore = _seq;
    nextSequence();

    if (!buildPayload(_outbound, MqttSparkplugType::DBirth,
                      (int16_t)deviceIndex, false, false)) {
        _seq = sequenceBefore;
        return false;
    }
    _outbound.seq = _seq;
    _outbound.hasSeq = true;

    if (!sendPayload(MqttSparkplugType::DBirth, device.id.c_str(), _outbound,
                     MQ_QOS_0, false, false)) {
        _seq = sequenceBefore;
        return false;
    }

    for (size_t index = 0; index < _pointCount; index++) {
        if (_state[index].device != (int16_t)deviceIndex) continue;
        markPublished(index);
    }
    forgetChanges(_devices[(size_t)deviceIndex].changes);
    _birthCount++;
    return true;
}

bool MqttSparkplugNode::set(const char* name,
                            const MqttSparkplugValue& value) {
    const int index = findPointIndex(name, -1);
    if (index < 0) return false;
    bool changed = false;
    return putValue((size_t)index, value, changed);
}

bool MqttSparkplugNode::setFor(const char* deviceId, const char* name,
                               const MqttSparkplugValue& value) {
    const int device = findDeviceIndex(deviceId);
    if (device < 0) return false;
    const int index = findPointIndex(name, (int16_t)device);
    if (index < 0) return false;
    bool changed = false;
    return putValue((size_t)index, value, changed);
}

bool MqttSparkplugNode::publishData(int16_t device, bool onlyChanged) {
    if (!_born || !_client.isConnected()) return false;

    // [tck-id-operational-behavior-data-commands-rebirth-action-1] immediately
    // stop sending DATA messages. Not a failure - the values stay in the
    // registry and go out with the birth, and then normally afterwards.
    if (_rebirthPending) return true;

    // Nothing to say is not a failure, and publishing an empty NDATA would
    // still cost a sequence number and a message.
    //
    // Report by exception looks at this slot's CHANGE LIST and nothing else:
    // what was marked since its last message (see publishChanged()).
    ChangeList& list = changesOf(device);
    bool anything = false;
    if (onlyChanged) {
        for (uint32_t link = list.head; link != 0;
             link = _state[link - 1].nextChanged) {
            if (pointChanged(link - 1)) {
                anything = true;
                break;
            }
        }
        if (!anything) {
            // What came back to the value last sent leaves the list.
            settleChanges(list, false);
            return true;
        }
    } else {
        for (size_t index = 0; index < _pointCount; index++) {
            if (_state[index].device == device) {
                anything = true;
                break;
            }
        }
        if (!anything) return true;
    }

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

    // WHAT WENT, AND ONLY WHAT WENT, becomes the new reference (see
    // settleChanges()). Everything went: the list is empty.
    if (onlyChanged) {
        settleChanges(list, true);
    } else {
        for (size_t index = 0; index < _pointCount; index++) {
            if (_state[index].device == device) markPublished(index);
        }
        forgetChanges(list);
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

bool MqttSparkplugNode::publishRecords(const MqttSparkplugRecord* records,
                                       size_t count, bool historical) {
    if (!_born || !_client.isConnected()) return false;
    if (_rebirthPending) return false;
    if (records == nullptr || count == 0) return true;

    if (records[0].index >= _pointCount) return false;
    const int16_t device = _points[records[0].index].device;
    for (size_t i = 0; i < count; i++) {
        if (records[i].index >= _pointCount) return false;
        const MqttSparkplugPoint& point = _points[records[i].index];
        if (point.device != device) return false;
        if (records[i].value.type != point.type) {
            _lastPayloadError = MqttSparkplugError::ValueTypeMismatch;
            return false;
        }
    }
    if (device >= 0 && !_devices[(size_t)device].online) return false;

    // Grown to the batch if it is larger than the birth the buffer was sized
    // for. Names are not interned: data messages carry aliases, and where a
    // node has none the registry's own strings outlive the encode below.
    if (_outbound.capacity() < count && !_outbound.reserve(count, 1)) {
        _lastPayloadError = MqttSparkplugError::Overflow;
        return false;
    }

    const MqttSparkplugType type = device < 0 ? MqttSparkplugType::NData
                                              : MqttSparkplugType::DData;
    _outbound.clear();
    _outbound.timestamp = utcNow();
    _outbound.hasTimestamp = true;
    for (size_t i = 0; i < count; i++) {
        const MqttSparkplugPoint& point = _points[records[i].index];
        MqttSparkplugMetric metric;
        metric.name = point.name.c_str();
        if (point.alias != 0) {
            metric.alias = point.alias;
            metric.hasAlias = true;
        }
        metric.value = records[i].value;
        metric.timestamp = records[i].timestamp;
        metric.hasTimestamp = true;
        metric.isHistorical = historical;
        if (!_outbound.addMetric(metric)) {
            _lastPayloadError = MqttSparkplugError::TooManyMetrics;
            return false;
        }
    }

    nextSequence();
    _outbound.seq = _seq;
    _outbound.hasSeq = true;
    const char* deviceId = device < 0 ? nullptr : _devices[(size_t)device].id.c_str();
    // [tck-id-topics-ndata-mqtt] QoS 0, retain false - as for every NDATA.
    if (!sendPayload(type, deviceId, _outbound, MQ_QOS_0, false, false)) return false;

    if (!historical) {
        // Live: what went is now the metric's value and its reference, so
        // report by exception does not send it a second time. (On a change
        // list it stays there until the next look, which finds it settled.)
        for (size_t i = 0; i < count; i++) {
            storeValue(records[i].index, records[i].value);
            markPublished(records[i].index);
        }
    }
    return true;
}

void MqttSparkplugNode::publishOwed() {
    if (!_publishOwed) return;
    _publishOwed = false;
    _pollPublishSent = publishChanged();
    _pollPublishes++;
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
        // --- A NEW HOST SESSION WHILE WE ARE BORN TO THE OLD ONE ---
        //
        // An online STATE NEWER than the one this node births against is a
        // different host MQTT session. That host's Will - its offline - carries
        // the SAME timestamp as its online
        // [tck-id-host-topic-phid-death-payload-connect]
        // [tck-id-payloads-state-will-message-payload], so the old session's
        // offline was stamped exactly the online we hold: valid under
        // [tck-id-operational-behavior-edge-node-termination-host-offline]
        // (section 5.5, Edge Node Session Termination), which requires the node to
        // publish its NDEATH, disconnect and start session establishment
        // over. We missed that offline - a host replaced quickly, its death
        // certificate overtaken by its successor's birth on the retained
        // topic, or ignored as stale behind it
        // [-host-offline-timestamp] - and the new online is the proof that it
        // happened. Staying born would send NDATA by aliases to a host that
        // has no birth certificate to decode them with.
        //
        // So the rule is applied now: NDEATH and disconnect (the intentional
        // one, as for a valid offline), then reconnect - to THIS server, not
        // the next: the host has just said it is here - and birth on its
        // retained online, with a new bdSeq [tck-id-topics-nbirth-bdseq-
        // increment]. A duplicate online (the same timestamp) is not a new
        // session and does nothing.
        if (_born && _sawPrimaryHostOnline && timestamp > _primaryHostOnlineAt) {
            _primaryHostOnlineAt = timestamp;
            _primaryHostOnline = true;
            endBirth();
            _birthOwed = false;
            cycleSession();
            return;
        }

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
    endBirth();
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
    //
    // AND THE DISCONNECT IS THE INTENTIONAL ONE, death certificate included:
    // reason 0x04 at 5.0, an NDEATH first at 3.1.1. It sent a plain 0x00
    // here, which tells the server to drop the Will - so every other host on
    // the broker kept a node that had gone. The rule binds every DISCONNECT
    // an edge node sends, not only the one an application asks for.
    cycleSession();
}

const char* mq_sparkplugCommandOutcomeName(MqttSparkplugCommandOutcome outcome) {
    switch (outcome) {
        case MqttSparkplugCommandOutcome::Applied:       return "applied";
        case MqttSparkplugCommandOutcome::Undecodable:   return "undecodable";
        case MqttSparkplugCommandOutcome::UnknownDevice: return "unknown device";
        case MqttSparkplugCommandOutcome::UnknownMetric: return "unknown metric";
        case MqttSparkplugCommandOutcome::ReadOnly:      return "read only";
        case MqttSparkplugCommandOutcome::WrongType:     return "wrong type";
        case MqttSparkplugCommandOutcome::NotARequest:   return "not a request";
    }
    return "?";
}

void MqttSparkplugNode::deliverCommand(MqttSparkplugCommand& command) {
    if (_commandHandler) _commandHandler(command);
    if (_commandContextHandler) {
        _commandContextHandler(_commandContext, command);
    }
}

void MqttSparkplugNode::refuseCommand(const char* device, const char* name,
                                      uint64_t alias,
                                      const MqttSparkplugValue& value,
                                      MqttSparkplugCommandOutcome outcome) {
    if (_refusedHandler == nullptr && _refusedContextHandler == nullptr) return;
    MqttSparkplugCommand command;
    command.device = device;
    command.name = name;
    command.alias = alias;
    command.value = value;
    command.outcome = outcome;
    if (_refusedHandler) _refusedHandler(command);
    if (_refusedContextHandler) _refusedContextHandler(_refusedContext, command);
}

void MqttSparkplugNode::handleCommand(const MqttSparkplugTopicParts& parts,
                                      const uint8_t* payload, size_t length) {
    const char* const deviceId = parts.hasDevice ? parts.device : nullptr;

    const MqttSparkplugError error =
        mq_sparkplugDecode(payload, length, _inbound);
    if (error != MqttSparkplugError::None) {
        _lastPayloadError = error;
        _commandsRefused++;
        // One report for the message: there are no metrics to report on, and
        // a host sending something that is not Sparkplug at all is a fault
        // worth seeing by itself.
        refuseCommand(deviceId, nullptr, 0, MqttSparkplugValue(),
                      MqttSparkplugCommandOutcome::Undecodable);
        return;
    }

    int16_t device = -1;
    if (parts.hasDevice) {
        const int index = findDeviceIndex(parts.device);
        if (index < 0) {
            // Counted once for the message, as it always was; reported once
            // per metric, so whoever is watching sees what was asked for.
            _commandsRefused++;
            for (size_t m = 0; m < _inbound.metricCount; m++) {
                const MqttSparkplugMetric& metric = _inbound.metrics[m];
                refuseCommand(deviceId, metric.name, metric.alias, metric.value,
                              MqttSparkplugCommandOutcome::UnknownDevice);
            }
            return;
        }
        device = (int16_t)index;
    }

    for (size_t index = 0; index < _inbound.metricCount; index++) {
        MqttSparkplugMetric& metric = _inbound.metrics[index];

        // A command may carry only an alias
        // [tck-id-payloads-alias-data-cmd-requirement], so the alias table is
        // tried first and the name second.
        //
        // AND THE ALIAS MUST BELONG TO THE TOPIC'S DEVICE. Aliases are unique
        // across the whole node [tck-id-payloads-alias-uniqueness], so an
        // alias alone names exactly one point - but a DCMD for pump1 carrying
        // the alias of a node-level setpoint is a host addressing the wrong
        // thing, and applying it would write a metric the topic never named.
        int point = -1;
        if (metric.hasAlias) {
            point = findPointByAlias(metric.alias);
            if (point >= 0 && _points[point].device != device) point = -1;
        }
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
            // The name we know it by, not the name it arrived with - which,
            // for a conformant host using aliases, is none.
            const char* const rebirthName =
                metric.name != nullptr ? metric.name
                : metric.alias == MQ_SP_ALIAS_DEVICE_REBIRTH
                    ? MQ_SP_METRIC_DEVICE_REBIRTH
                    : MQ_SP_METRIC_NODE_REBIRTH;
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
                command.device = deviceId;
                command.name = rebirthName;
                command.alias = metric.alias;
                command.isRebirth = true;
                command.value = metric.value;
                deliverCommand(command);
            } else {
                // Not counted either way, as before: false asks for nothing,
                // and nothing was refused. Reported, because a host that
                // sends it believes it asked for something.
                refuseCommand(deviceId, rebirthName, metric.alias, metric.value,
                              MqttSparkplugCommandOutcome::NotARequest);
            }
            continue;
        }

        if (point < 0) {
            // A command for a metric we never declared. Counted, not applied -
            // a Host Application writing to the wrong name shows up here and
            // nowhere else.
            _commandsRefused++;
            refuseCommand(deviceId, metric.name, metric.alias, metric.value,
                          MqttSparkplugCommandOutcome::UnknownMetric);
            continue;
        }
        if (!_points[point].writable) {
            // A command for a read-only metric. Also counted: a host that
            // thinks it can write a sensor reading should find out, and
            // applying it would let a stale command overwrite a live reading.
            _commandsRefused++;
            refuseCommand(deviceId, _points[point].name.c_str(),
                          _points[point].alias, metric.value,
                          MqttSparkplugCommandOutcome::ReadOnly);
            continue;
        }
        // [tck-id-payloads-metric-datatype-not-req] A command SHOULD NOT carry
        // a datatype; the one this node declared is applied, and a value that
        // cannot be that type is refused like a mismatch.
        if ((metric.datatypeAbsent &&
             !mq_sparkplugApplyDatatype(metric, _points[point].type) &&
             !metric.valueUnsupported) ||
            (metric.value.type != _points[point].type &&
             !metric.value.isNull) ||
            !knownInstance(metric.value)) {
            _commandsRefused++;
            refuseCommand(deviceId, _points[point].name.c_str(),
                          _points[point].alias, metric.value,
                          MqttSparkplugCommandOutcome::WrongType);
            continue;
        }

        acceptWrite((size_t)point, metric.value);
        _commandsApplied++;

        MqttSparkplugCommand command;
        command.device = deviceId;
        // The name as WE declared it, not as it arrived - a command carrying
        // only an alias has no name at all, and the caller wants the one they
        // wrote in their own code.
        command.name = _points[point].name.c_str();
        command.alias = _points[point].alias;
        command.value = metric.value;
        deliverCommand(command);
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

void MqttSparkplugNode::onCommandRefused(MqttSparkplugCommandFn handler) {
    _refusedHandler = handler;
}

void MqttSparkplugNode::onCommandRefused(MqttSparkplugCommandContextFn handler,
                                         void* context) {
    _refusedContextHandler = handler;
    _refusedContext = context;
}

bool MqttSparkplugNode::commandPending(const char* name) const {
    const int index = findPointIndex(name, -1);
    return index >= 0 && commandPendingAt((size_t)index);
}

bool MqttSparkplugNode::commandPendingFor(const char* deviceId,
                                          const char* name) const {
    if (deviceId == nullptr || deviceId[0] == '\0') return commandPending(name);
    const int device = findDeviceIndex(deviceId);
    if (device < 0) return false;
    const int index = findPointIndex(name, (int16_t)device);
    return index >= 0 && commandPendingAt((size_t)index);
}

bool MqttSparkplugNode::takeFor(const char* deviceId, const char* name,
                                MqttSparkplugValue& into) {
    const int index = indexOf(deviceId, name);
    return index >= 0 && takeAt((size_t)index, into);
}

bool MqttSparkplugNode::take(const char* name, MqttSparkplugValue& into) {
    // Cleared (or the oldest queued write removed) on the way out, so two
    // callers cannot both act on one command - the same rule MqttStatus
    // follows for its edge counters.
    const int index = findPointIndex(name, -1);
    return index >= 0 && takeAt((size_t)index, into);
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
    if (_willRefused) return "another layer owns the client's Will";
    if (!_client.isConnected() || _born) return "";
    if (_restartOwed) return "reconnecting to carry the NDEATH or a larger birth";
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

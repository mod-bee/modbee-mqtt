// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttDevice.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MqttNoExcept.h"

namespace {

// Case-insensitive compare for the handful of words a boolean payload is
// written as in practice. ASCII only, deliberately: a topic payload saying
// "ON" is ASCII, and a locale-aware fold would make the answer depend on where
// the gateway was installed.
bool sameWord(const char* text, const char* word) {
    while (*word) {
        const char a = *text++;
        const char b = *word++;
        const char folded = (a >= 'A' && a <= 'Z') ? (char)(a + 32) : a;
        if (folded != b) return false;
    }
    return *text == '\0';
}

// Whitespace at either end is ignored, because a payload that came from a
// shell, a file or somebody's copy and paste often has a newline on it and
// refusing "21.5\n" as "not a number" would be pedantry rather than rigour.
const char* skipSpace(const char* text) {
    while (*text == ' ' || *text == '\t' || *text == '\r' || *text == '\n') text++;
    return text;
}

bool consumedAll(const char* end) {
    return *skipSpace(end) == '\0';
}

} // namespace

// =============================================================================
// THE PAYLOAD
// =============================================================================

bool MqttPayload::equals(const char* other) const {
    if (other == nullptr) return false;
    const size_t length = strlen(other);
    if (length != _length) return false;
    if (_length == 0) return true;
    return memcmp(_message.payload.data, other, length) == 0;
}

const char* MqttPayload::responseTopic() const {
    if (_properties == nullptr || !_properties->hasResponseTopic) return nullptr;
    return _properties->responseTopic.c_str();
}

const uint8_t* MqttPayload::correlationData() const {
    if (_properties == nullptr || !_properties->hasCorrelationData) return nullptr;
    if (_properties->correlationData.empty()) return nullptr;
    return _properties->correlationData.data();
}

size_t MqttPayload::correlationLength() const {
    if (_properties == nullptr || !_properties->hasCorrelationData) return 0;
    return _properties->correlationData.size();
}

const char* MqttPayload::contentType() const {
    if (_properties == nullptr || !_properties->hasContentType) return nullptr;
    return _properties->contentType.c_str();
}

const char* MqttPayload::userProperty(const char* key) const {
    if (_properties == nullptr) return nullptr;
    return _properties->userProperty(key);
}

uint32_t MqttPayload::secondsUntilExpiry() const {
    if (_properties == nullptr || !_properties->hasExpiry) return 0;
    return _properties->expiryInterval;
}

bool MqttPayload::declaredAsText() const {
    if (_properties == nullptr || !_properties->hasPayloadFormat) return false;
    return _properties->payloadFormat == 1;
}

double MqttPayload::toNumber(double fallback) const {
    if (_length == 0) return fallback;
    char* end = nullptr;
    const char* start = skipSpace(_text);
    const double value = strtod(start, &end);

    // Nothing parsed, or something left over. "21.5 degC" is NOT 21.5 here:
    // the part that was not understood is where the fault usually is, and a
    // library that discards it hands back a plausible number with a problem
    // hidden inside it.
    if (end == start || !consumedAll(end)) return fallback;
    return value;
}

long MqttPayload::toInteger(long fallback) const {
    if (_length == 0) return fallback;
    char* end = nullptr;
    const char* start = skipSpace(_text);
    const long value = strtol(start, &end, 10);
    if (end == start || !consumedAll(end)) return fallback;
    return value;
}

bool MqttPayload::toBool(bool fallback) const {
    if (_length == 0) return fallback;
    const char* text = skipSpace(_text);

    if (sameWord(text, "1") || sameWord(text, "true") || sameWord(text, "on") ||
        sameWord(text, "yes") || sameWord(text, "open") || sameWord(text, "start") ||
        sameWord(text, "running")) {
        return true;
    }
    if (sameWord(text, "0") || sameWord(text, "false") || sameWord(text, "off") ||
        sameWord(text, "no") || sameWord(text, "closed") || sameWord(text, "stop") ||
        sameWord(text, "stopped")) {
        return false;
    }
    return fallback;
}

// =============================================================================
// CONSTRUCTION
// =============================================================================

#if defined(MQTT_PLATFORM_HOST)
MqttDevice::MqttDevice() {
    _transport = &_tcp;
    _ownTransport = true;
    applyDefaults();
}
#endif

MqttDevice::MqttDevice(MqttTransport& transport) {
    _transport = &transport;
    _ownTransport = false;
    applyDefaults();
}

MqttDevice::~MqttDevice() {
    // The client concludes everything outstanding on the way down, so a caller
    // holding a slot per publish gets every one of them back.
    if (_begun) _client.end();
    // A connect still running on its task is using the channel's transport,
    // so it is waited for (bounded by the connect timeout) before that goes.
#if MQTT_CONNECT_TASK
    if (_channel != nullptr) _task.settle();
#endif
    if (_destroy && _channel) _destroy(_channel);
}

void MqttDevice::connectInBackground(bool enabled) {
#if MQTT_CONNECT_TASK
    if (_channel == nullptr || _task.busy()) return;
    _transport = enabled ? static_cast<MqttTransport*>(&_task) : _channel;
#else
    (void)enabled;
#endif
}

bool MqttDevice::connecting() const {
#if MQTT_CONNECT_TASK
    return _channel != nullptr && _task.busy();
#else
    return false;
#endif
}

void MqttDevice::applyDefaults() {
    // 5.0, dropping to 3.1.1 if the broker turns out to be older.
    //
    // The long form defaults this OFF, so that a plant engineer reading a
    // config knows exactly what is on the wire. Here it is ON, because the
    // whole promise of this class is that it works against whatever is
    // installed - and activeVersion() still says what it settled on. Turn it
    // off with only(MqttVersion::V5).
    _connection.version = MqttVersion::V5;
    _connection.fallbackEnabled = true;
    _connection.fallbackVersion = MqttVersion::V311;
    _connection.keepAlive = 60;
    _connection.cleanStart = true;

    // Our own Maximum Packet Size, so a broker never sends something we cannot
    // assemble. Matched to the receive buffer rather than guessed.
    _connection.maximumPacketSize = (uint32_t)_behaviour.receiveBufferSize;

    _topicText.reserve(128);
    _payloadText.reserve(256);
}

// =============================================================================
// SETTINGS
// =============================================================================

void MqttDevice::clientId(const char* id) {
    _connection.clientId = id ? id : "";
}

void MqttDevice::credentials(const char* user, const char* password) {
    if (user) {
        _connection.userName = user;
        _connection.hasUserName = true;
    }
    if (password) {
        const size_t length = strlen(password);
        _connection.password.assign((const uint8_t*)password,
                                    (const uint8_t*)password + length);
        _connection.hasPassword = true;
    }
}

// =============================================================================
// PROVISIONING
// =============================================================================
//
// The protocol is a modbee broker's: CONNECT with user
// name "$provision" and the key as password, Client Identifier = the device's
// name; SUBSCRIBE $modbee/provision/<name>/reply; PUBLISH {} to
// $modbee/provision/<name>/request; the answer is JSON with "status" ok
// (with "username" and "password"), pending, or refused (with "reason").

namespace {

// How often a device waiting for approval asks again, and how long after a
// refused key it tries once more (an admin may have fixed it).
const mq_time_t kPendingAskEveryMs = 30000;
const mq_time_t kRefusedRetryMs = 300000;

// One string field of the broker's small, flat JSON answer - enough for
// "status", "username", "password" and "reason", which hold no escapes but \"
// and \\, handled anyway.
bool jsonField(const std::string& doc, const char* name, std::string& out) {
    const std::string key = std::string("\"") + name + "\"";
    size_t at = doc.find(key);
    if (at == std::string::npos) return false;
    at = doc.find(':', at + key.size());
    if (at == std::string::npos) return false;
    at = doc.find('"', at);
    if (at == std::string::npos) return false;
    out.clear();
    for (size_t i = at + 1; i < doc.size(); i++) {
        if (doc[i] == '\\' && i + 1 < doc.size()) { out.push_back(doc[++i]); continue; }
        if (doc[i] == '"') return true;
        out.push_back(doc[i]);
    }
    return false;
}

} // namespace

const MqttDevice::Provisioner MqttDevice::kProvisioner = {
    [](MqttDevice& d) { d.startProvisioning(); },
    [](MqttDevice& d) { d.provisionSession(); },
    [](MqttDevice& d, const MqttMessage& m) { d.provisionReply(m); },
    [](MqttDevice& d) { d.serviceProvisioning(); },
    [](MqttDevice& d, MqttState s) { d.provisionStateChanged(s); },
};

void MqttDevice::provision(const char* key, MqttDeviceLoadLoginFn load, MqttDeviceSaveLoginFn save,
                           void* context) {
    _provisioner = &kProvisioner;
    _provisionKey = key ? key : "";
    _loadLogin = load;
    _saveLogin = save;
    _loginContext = context;
    _provisionState = _provisionKey.empty() ? MqttProvisionState::Off : MqttProvisionState::Registering;
}

// At connect(): a stored login if there is one, the key if not.
void MqttDevice::startProvisioning() {
    _provisioning = false;
    if (_provisionKey.empty()) return;
    if (!_connection.hasUserName && !_reprovision) {
        char user[128] = {0};
        char password[128] = {0};
        if (_loadLogin && _loadLogin(_loginContext, user, sizeof user, password, sizeof password) &&
            user[0] != '\0') {
            credentials(user, password);
        }
    }
    if (_connection.hasUserName && !_reprovision) {
        // A held login being tried again is not known to be good yet: the
        // state the key's session last reported - Pending, Refused - stands
        // until the broker takes it (provisionStateChanged()), so a block's
        // STATE does not claim Provisioned every five minutes for one refused
        // CONNECT.
        if (!_retryingLogin) _provisionState = MqttProvisionState::Provisioned;
        return;
    }
    _retryingLogin = false;
    if (_connection.clientId.empty()) {
        _provisionState = MqttProvisionState::Refused;
        _provisionReason = "provisioning needs the device's name: connect(host, \"name\")";
        return;
    }
    // The key's own session: clean, no Will, no status - it is not the device
    // yet, and a "$provision" login may touch nothing but its two topics.
    _provisionConnection = _connection;
    _provisionConnection.hasUserName = true;
    _provisionConnection.userName = "$provision";
    _provisionConnection.hasPassword = true;
    _provisionConnection.password.assign(_provisionKey.begin(), _provisionKey.end());
    _provisionConnection.cleanStart = true;
    _provisionConnection.hasSessionExpiry = false;
    _provisionConnection.sessionExpiryInterval = 0;
    _provisionConnection.will = MqttWillConfig();
    _provisionConnection.registration = true;   // and none a layer adds at CONNECT
    _provisioning = true;
    _reprovision = false;
    _provisionState = MqttProvisionState::Registering;
    _provisionReason.clear();
}

void MqttDevice::provisionSession() {
#if MQTT_LEAN
    const std::string base = std::string("$modbee/provision/") + _connection.clientId.c_str();
#else
    const std::string base = "$modbee/provision/" + _connection.clientId;
#endif
    _client.subscribe((base + "/reply").c_str(), MQ_QOS_1);
    _client.publish((base + "/request").c_str(), "{}", MQ_QOS_1, false);
    _provisionAskedAt = mq_millis();
}

void MqttDevice::provisionReply(const MqttMessage& message) {
    const std::string doc((const char*)message.payload.data, message.payload.length);
    std::string status, user, password;
    jsonField(doc, "status", status);
    if (status == "ok" && jsonField(doc, "username", user) && jsonField(doc, "password", password)) {
        // THE ONLY PLACE A STORED LOGIN IS REPLACED: here, with one the broker
        // has just issued. Pending and refused below leave it as it was.
        if (_saveLogin) _saveLogin(_loginContext, user.c_str(), password.c_str());
        _retryingLogin = false;
        credentials(user.c_str(), password.c_str());
        _provisionState = MqttProvisionState::Provisioned;
        _provisionReason.clear();
        _switchToLogin = true;          // in poll(): not from inside the client's callback
    } else if (status == "pending") {
        _provisionState = MqttProvisionState::Pending;
        _provisionReason = "waiting for an admin to approve this device";
    } else {
        _provisionState = MqttProvisionState::Refused;
        if (!jsonField(doc, "reason", _provisionReason)) _provisionReason = "refused";
        _provisionAskedAt = mq_millis();
    }
}

void MqttDevice::serviceProvisioning() {
    // Provisioned, or the stored login was refused: reconnect, with the new
    // configuration, once the old connection is down.
    if (_switchToLogin || _reprovision) {
        if (_client.isConnected()) {
            if (!_switchDisconnected) {
                _switchDisconnected = true;
                _client.disconnect();
            }
            return;
        }
        if (_client.state() != MqttState::Idle && _client.state() != MqttState::Reconnecting) return;
        _switchDisconnected = false;
        // THE STORED LOGIN IS NOT FORGOTTEN HERE, and for a long time it was:
        // one refused CONNECT and it was wiped - from memory and through
        // _saveLogin, from the block's retained memory too - before the key
        // had been asked anything. A broker whose login store is lost, or put
        // back from last month's backup, refuses EVERY device's login at
        // once, and the whole fleet erased itself and sat in "pending" for an
        // admin to approve each one again, although the logins they held
        // were good the moment the store came back.
        //
        // So a refused login is only a reason to ASK. The device registers
        // with the key and keeps what it holds, in memory and in the store;
        // the login is replaced in provisionReply() when the broker has
        // actually issued a new one, and not before. Until then it is tried
        // again at the rejected-credentials pace (just below), so a store
        // that comes back lets the device back in without anyone approving
        // anything.
        _switchToLogin = false;
        startProvisioning();
        beginClient();
        _client.connect();
        return;
    }
    // Registering again with a login still held: the login is tried once
    // more at the pace a refused credential is always retried at
    // (rejectedCredentialsRetryMs, five minutes), whatever the key's session
    // is doing - up, waiting for approval, refused, or not reachable at all.
    // A broker whose store came back takes the old login and the device is
    // itself again; one that still refuses it sends the device back to the
    // key, and five minutes later the same again. Never faster: a password
    // repeated every second is how a device gets itself banned (MqttClient's
    // closeConnection() says why), and the pace is the one the client keeps
    // for a device with no key at all. Zero there means a refused credential
    // is never repeated, and then this does not repeat it either.
    if (_provisioning && _connection.hasUserName &&
        _provisionState != MqttProvisionState::Provisioned &&
        _behaviour.rejectedCredentialsRetryMs != 0 &&
        mq_elapsed(_loginRefusedAt, mq_millis()) >= _behaviour.rejectedCredentialsRetryMs) {
        _retryingLogin = true;
        _switchToLogin = true;          // the next poll() makes the switch
        return;
    }
    if (!_provisioning || !_client.isConnected()) return;
    const mq_time_t since = mq_elapsed(_provisionAskedAt, mq_millis());
    if ((_provisionState == MqttProvisionState::Pending && since >= kPendingAskEveryMs) ||
        (_provisionState == MqttProvisionState::Refused && since >= kRefusedRetryMs)) {
        provisionSession();
    }
}

void MqttDevice::provisionStateChanged(MqttState state) {
    if (state == MqttState::Connected) {
        // A held login tried again, and taken: the broker knows this device
        // after all, and whatever the key's session last said is history.
        if (!_provisioning && _retryingLogin) {
            _retryingLogin = false;
            _provisionState = MqttProvisionState::Provisioned;
            _provisionReason.clear();
        }
        return;
    }
    // 0x86 at 5.0, return code 4 at 3.1.1: neither means anything else in a
    // CONNACK.
    const uint8_t code = _client.status().connectReasonCode;
    if (!_provisioning) {
        // A provisioned login the broker no longer accepts - reset, removed,
        // the device replaced and the name registered again, or a login store
        // that was lost - sends the device to the key to register again. The
        // login itself is KEPT until a new one is issued: see
        // serviceProvisioning(). Only a refused PASSWORD: 0x87 (disabled, or
        // pending) is an admin's decision, which registering again would undo.
        if (code == MQ_REASON_BAD_USER_NAME_OR_PASSWORD || code == MQ_RETURN_BAD_CREDENTIALS) {
            _reprovision = true;
            _loginRefusedAt = mq_millis();
        }
    } else if (code != 0 && _provisionState == MqttProvisionState::Registering) {
        _provisionState = MqttProvisionState::Refused;
        _provisionReason = "the broker refused the provisioning key";
    }
}

void MqttDevice::keepAlive(uint16_t seconds) {
    _connection.keepAlive = seconds;
}

void MqttDevice::rememberSession(uint32_t seconds) {
    // Clean Start 0 is what asks the broker to resume whatever it kept
    // [V5:3.1.2.4]. The expiry says how long it should keep it for; at 3.1.1
    // there is no way to say, and the broker decides.
    _connection.cleanStart = false;
    _connection.hasSessionExpiry = seconds != 0;
    _connection.sessionExpiryInterval = seconds;
}

void MqttDevice::only(MqttVersion version) {
    _connection.version = version;
    _connection.fallbackEnabled = false;
}

void MqttDevice::allowOldBrokers(bool allowed) {
    _connection.fallbackEnabled = allowed;
}

void MqttDevice::statusTopic(const char* topic, const char* online,
                             const char* offline) {
    if (topic == nullptr) return;
    _statusTopic = topic;
    _statusOnline = online ? online : "online";
    _statusOffline = offline ? offline : "offline";
    _statusIsText = true;

    // Retained, and at QoS 1. A dashboard that connects tomorrow has to learn
    // this device is gone without waiting for it to change - which it never
    // will, because it is gone.
    _connection.will.set(topic, _statusOffline.c_str(), MQ_QOS_1, true);
}

void MqttDevice::will(const char* topic, const uint8_t* payload, size_t length,
                      uint8_t qos, bool retain) {
    if (topic == nullptr) return;
    _connection.will.set(topic, payload, length, qos, retain);

    // A Will of the caller's own replaces the status pair: publishing "online"
    // against a Will that says something else entirely would be this class
    // inventing a protocol on top of the one the caller chose.
    _statusTopic.clear();
    _statusOnline.clear();
    _statusOffline.clear();
    _statusIsText = false;
}

void MqttDevice::willDelay(uint32_t seconds) {
    _connection.will.delayInterval = seconds;
}

size_t MqttDevice::queueSize(size_t operations) {
    // Nothing caps it but memory: the queue is sized to it when the client is
    // begun, and if the memory is not there it keeps the size it had and
    // queueOutOfMemory() says so - never a quietly smaller queue. A remote
    // site told it could hold an hour of readings, and holding twenty
    // minutes, loses forty minutes of data and no warning.
    if (operations == 0) operations = 1;
    _behaviour.queueCapacity = operations;
    return operations;
}

void MqttDevice::bufferSize(size_t bytes) {
    _behaviour.receiveBufferSize = bytes;
    _behaviour.transmitBufferSize = bytes;
    _connection.maximumPacketSize = (uint32_t)bytes;
}

void MqttDevice::autoReconnect(bool enabled) {
    _behaviour.autoReconnect = enabled;
}

#if MQTT_TLS_SOCKETS

void MqttDevice::secure(const char* caFilePath) {
    _useTls = true;
    _transport = &_tls;
    if (caFilePath) _tls.setCaFile(caFilePath);
}

void MqttDevice::secureWithCertificate(const char* caPem) {
    _useTls = true;
    _transport = &_tls;
    if (caPem) _tls.setCaCertificate(caPem);
}

void MqttDevice::clientCertificate(const char* certificatePath, const char* keyPath) {
    _useTls = true;
    _transport = &_tls;
    _tls.setClientCertificate(certificatePath, keyPath);
}

void MqttDevice::insecureNoCertificateValidation() {
    _useTls = true;
    _transport = &_tls;
    _tls.setInsecureNoCertificateValidation();
}

#endif

uint16_t MqttDevice::defaultPort() const {
#if MQTT_TLS_SOCKETS
    if (_useTls) return MQ_PORT_TLS;
#endif
    return MQ_PORT_TCP;
}

// =============================================================================
// CONNECTING
// =============================================================================

bool MqttDevice::connect(const char* host, const char* id) {
    clientId(id);
    // Cast, because a bare 0 is a null pointer constant and would pick the
    // (host, clientId) overload we are standing in.
    return connect(host, (uint16_t)0);
}

bool MqttDevice::connect(const char* host, uint16_t port, const char* id) {
    clientId(id);
    return connect(host, port);
}

bool MqttDevice::connect(const char* host, uint16_t port) {
    if (host == nullptr || host[0] == '\0') return false;
    if (_transport == nullptr) return false;

    // NOT WHILE THE LAST ATTEMPT IS STILL OPENING THE CHANNEL. It is reading
    // the host string this would overwrite, on another task; and it could not
    // be followed by a new one until it returned anyway. It is bounded by the
    // connect timeout, and the client takes it from there.
    if (connecting()) return false;

    _host = host;
    if (port == 0) port = defaultPort();

    _connection.host = _host;
    _connection.port = port;

    // Only ours. A transport the caller brought is theirs to point at a host,
    // and reconfiguring it here would silently undo whatever they set.
    if (_ownTransport) {
#if defined(MQTT_PLATFORM_ARDUINO)
        if (_retarget) _retarget(_channel, _host.c_str(), port);
#elif defined(MQTT_PLATFORM_HOST)
#if MQTT_TLS_SOCKETS
        if (_useTls) _tls.configure(_host.c_str(), port);
        else _tcp.configure(_host.c_str(), port);
#else
        _tcp.configure(_host.c_str(), port);
#endif
#endif
    }

    _closing = false;
    _provisioning = false;
    if (_provisioner) _provisioner->start(*this);

    // Unconditionally, so that everything set since the last connect() - a new
    // client id, new credentials, a Will - is what actually goes on the wire.
    beginClient();

    // Nothing is re-issued here, deliberately. A subscription is held in the
    // session as a standing intent, and the client re-issues every one of them
    // when a session comes up that does not already have them - so issuing
    // them again from here would put a second SUBSCRIBE on the wire for each.
    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        _subscriptions[i].acknowledged = false;
    }

    const MqttResult result = _client.connect();
    return mq_accepted(result) || result == MqttResult::Duplicate;
}

void MqttDevice::beginClient() {
    _client.begin(*_transport, _provisioning ? _provisionConnection : _connection, _behaviour);
    _client.onMessage(&MqttDevice::messageTrampoline, this);
    _client.onSessionEstablished(&MqttDevice::sessionTrampoline, this);
    _client.onStateChange(&MqttDevice::stateTrampoline, this);
    _begun = true;
}

void MqttDevice::publishStatus(const std::string& payload) {
    // What this connection allows, not what we would prefer. A 5.0 broker may
    // cap Maximum QoS at 0 [V5:3.2.2.3.4] or set Retain Available to 0
    // [V5:3.2.2.3.5], and publishing at settings it refuses gets the publish
    // refused - so the online half of the availability topic would never go,
    // silently, against exactly the brokers that are most restrictive.
    const MqttCapabilities& allowed = _client.capabilities();
    const uint8_t qos = allowed.maximumQos < MQ_QOS_1 ? allowed.maximumQos : MQ_QOS_1;

    _client.publish(_statusTopic.c_str(),
                    payload.empty() ? nullptr : (const uint8_t*)payload.data(),
                    payload.size(), qos, allowed.retainAvailable);
}

void MqttDevice::disconnect() {
    if (!_begun) return;

    // Say offline before going, because a Will is NOT published on a graceful
    // disconnect [V5:3.1.2.5]. A device that shut down cleanly and never said
    // so reads as online for ever, which is the exact failure the availability
    // topic existed to prevent.
    if (_statusIsText && !_statusTopic.empty() && _client.isConnected()) {
        publishStatus(_statusOffline);
    }

    // The DISCONNECT waits for that to reach the wire. Sent now, it would go
    // first - MQTT has one ordered stream and the message behind a DISCONNECT
    // is a message that is never sent at all.
    _closing = true;
    _closingSince = mq_millis();
    poll();
}

void MqttDevice::disconnectNow() {
    _closing = false;
    if (_begun) _client.disconnect();
}

void MqttDevice::poll() {
    if (!_begun) return;

    // "Last heard" never ages past MQ_AGE_CEILING_MS, so isFresh() and
    // sinceMessage() cannot wrap round to young after 49.7 days of silence.
    const mq_time_t now = mq_millis();
    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        if (_subscriptions[i].everReceived) mq_ageCeiling(_subscriptions[i].lastMessageAt, now);
    }

    // Handled here rather than inside the state-change callback, so the client
    // is not restarted from within one of its own notifications.
    if (_retryWithoutWillRetain && !_client.isConnected()) {
        _retryWithoutWillRetain = false;
        _connection.will.retain = false;
        _willRetainRefused = true;
        beginClient();
        _client.connect();
        return;
    }

    // BEFORE the client, not after. The client's own poll() is what drains the
    // send queue, so a report queued after it would sit there until the next
    // call - one whole scan of latency on every reported value, for nothing.
    if (!_provisioning) serviceReports(false);

    _client.poll();
    if (_provisioner) _provisioner->service(*this);

    if (_closing) {
        // A queue that never drains must not hold the close open for ever: a
        // broker that has stopped acknowledging is exactly when an application
        // most wants to be able to shut down.
        const bool drained = _client.operations().empty();
        const bool waitedLongEnough = mq_elapsed(_closingSince, mq_millis()) > 2000;
        if (drained || waitedLongEnough || !_client.isConnected()) {
            _closing = false;
            _client.disconnect();
        }
    }
}

// =============================================================================
// PUBLISHING
// =============================================================================

bool MqttDevice::publish(const char* topic, const char* text, uint8_t qos, bool retain) {
    ensureBegun();
    const size_t length = text ? strlen(text) : 0;
    return mq_accepted(_client.publish(topic, (const uint8_t*)text, length, qos, retain));
}

bool MqttDevice::publish(const char* topic, const uint8_t* bytes, size_t length,
                         uint8_t qos, bool retain) {
    ensureBegun();
    return mq_accepted(_client.publish(topic, bytes, length, qos, retain));
}

bool MqttDevice::publishNumber(const char* topic, double value, uint8_t decimals,
                               uint8_t qos, bool retain) {
    char buffer[48];
    if (decimals > 17) decimals = 17;
    snprintf(buffer, sizeof buffer, "%.*f", (int)decimals, value);
    return publish(topic, buffer, qos, retain);
}

bool MqttDevice::publishSeries(const char* topic, const uint8_t* bytes, size_t length,
                               uint8_t qos, bool retain) {
    ensureBegun();
    return mq_accepted(_client.publishSeries(topic, bytes, length, qos, retain));
}

bool MqttDevice::publishSeries(const char* topic, const char* text,
                               uint8_t qos, bool retain) {
    const size_t length = text ? strlen(text) : 0;
    return publishSeries(topic, (const uint8_t*)text, length, qos, retain);
}

bool MqttDevice::publishAndConfirm(const char* topic, const char* text, uint8_t qos,
                                   MqttCompletionFn onConcluded, void* context,
                                   bool retain) {
    ensureBegun();
    const size_t length = text ? strlen(text) : 0;

    // publishSeries rather than publish: a caller that asked to be told what
    // happened to THIS message should not have it replaced by a newer one on
    // the same topic and be told "Superseded" for something it did not do.
    // That is a legitimate conclusion, but it is not what a confirmation is
    // for - a stop command must go, not be improved upon.
    return mq_accepted(_client.publishSeries(topic, (const uint8_t*)text, length,
                                             qos, retain, onConcluded, context));
}

bool MqttDevice::publish(const char* topic, const char* text,
                         const MqttOwnedProperties& properties,
                         uint8_t qos, bool retain) {
    const size_t length = text ? strlen(text) : 0;
    return publish(topic, (const uint8_t*)text, length, properties, qos, retain);
}

bool MqttDevice::publish(const char* topic, const uint8_t* bytes, size_t length,
                         const MqttOwnedProperties& properties,
                         uint8_t qos, bool retain) {
    ensureBegun();
    return mq_accepted(_client.publish(topic, bytes, length, qos, retain,
                                       properties));
}

bool MqttDevice::publishRequest(const char* topic, const char* text,
                                const char* replyTopic, const char* correlationId,
                                uint8_t qos) {
    if (replyTopic == nullptr || replyTopic[0] == '\0') return false;

    MqttOwnedProperties properties;
    properties.setResponseTopic(replyTopic);
    if (correlationId) properties.setCorrelationData(correlationId);

    const size_t length = text ? strlen(text) : 0;
    ensureBegun();

    // publishSeries, not publish: a request is not a value to be improved
    // upon. Two questions asked on one topic are two questions, and eliding
    // the first would leave whoever asked it waiting for ever.
    return mq_accepted(_client.publishSeries(topic, (const uint8_t*)text, length,
                                             qos, false, properties));
}

bool MqttDevice::reply(const MqttPayload& request, const char* text, uint8_t qos) {
    const size_t length = text ? strlen(text) : 0;
    return reply(request, (const uint8_t*)text, length, qos);
}

bool MqttDevice::reply(const MqttPayload& request, const uint8_t* bytes,
                       size_t length, uint8_t qos) {
    const char* where = request.responseTopic();

    // Not a request. Answering it would mean choosing a topic ourselves, and a
    // reply published where nobody is listening is worse than none: the caller
    // believes it answered.
    if (where == nullptr || where[0] == '\0') return false;

    MqttOwnedProperties properties;

    // [V5:3.3.2.3.6] The Correlation Data goes back UNALTERED. It is how the
    // requester tells this answer from the other four it is waiting on, and
    // the library never looks inside it.
    if (request.correlationData() != nullptr) {
        properties.setCorrelationData(request.correlationData(),
                                      request.correlationLength());
    }

    ensureBegun();
    return mq_accepted(_client.publishSeries(where, bytes, length, qos, false,
                                             properties));
}

// The five formats. Written out rather than left to a template, because the
// question "what does this number look like on the wire" should have an answer
// somebody can read.
void MqttDevice::formatNumber(bool value, char* out, size_t capacity) {
    snprintf(out, capacity, "%s", value ? "1" : "0");
}

void MqttDevice::formatNumber(float value, char* out, size_t capacity) {
    // %.7g, not %.9g. Nine digits round-trip a float exactly and seven do not,
    // but seven is the number a person would write: 4.2 rather than 4.19999981.
    // Where the exact bits matter, publish the bytes or use the typed layer -
    // docs/typed-values.md.
    snprintf(out, capacity, "%.7g", (double)value);
}

void MqttDevice::formatNumber(double value, char* out, size_t capacity) {
    snprintf(out, capacity, "%.15g", value);
}

void MqttDevice::formatNumber(long long value, char* out, size_t capacity) {
    snprintf(out, capacity, "%lld", value);
}

void MqttDevice::formatNumber(unsigned long long value, char* out, size_t capacity) {
    snprintf(out, capacity, "%llu", value);
}


// =============================================================================
// REPORTING A VARIABLE
// =============================================================================

bool MqttDevice::addReport(const char* topic, const void* source,
                           MqttReportType type, double deadband,
                           mq_time_t atLeastEveryMs, uint8_t qos, bool retain) {
    if (topic == nullptr || source == nullptr) return false;

    // [V5:MQTT-4.7.0-1] a Topic Name carries no wildcards. Checked at the call
    // that made the mistake rather than on the first poll() after it, so the
    // fault is reported where somebody can see which line caused it.
    if (!mq_topicNameIsValid(mq_topicSpan(topic))) return false;

    MqttDeviceReport* slot = nullptr;
    for (size_t i = 0; i < MQTT_DEVICE_MAX_REPORTS; i++) {
        if (_reports[i].inUse && _reports[i].topic == topic) { slot = &_reports[i]; break; }
    }
    if (slot == nullptr) {
        for (size_t i = 0; i < MQTT_DEVICE_MAX_REPORTS; i++) {
            if (_reports[i].inUse) continue;
            slot = &_reports[i];
            *slot = MqttDeviceReport();
            slot->topic = topic;
            break;
        }
    }

    // Full. Refused, and the caller is told - a variable that was accepted and
    // is never published is an application that believes it is reporting.
    if (slot == nullptr) return false;

    slot->inUse = true;
    slot->source = source;
    slot->type = type;
    slot->deadband = deadband < 0.0 ? -deadband : deadband;
    slot->atLeastEveryMs = atLeastEveryMs;
    slot->qos = qos;
    slot->retain = retain;
    slot->everPublished = false;
    return true;
}

bool MqttDevice::report(const char* topic, const bool* value,
                        mq_time_t atLeastEveryMs, uint8_t qos, bool retain) {
    // No deadband: a boolean either changed or it did not, and a deadband on
    // one could only ever suppress the change itself.
    return addReport(topic, value, MqttReportType::Bool, 0.0, atLeastEveryMs,
                     qos, retain);
}

bool MqttDevice::report(const char* topic, const int32_t* value, double deadband,
                        mq_time_t atLeastEveryMs, uint8_t qos, bool retain) {
    return addReport(topic, value, MqttReportType::Int32, deadband,
                     atLeastEveryMs, qos, retain);
}

bool MqttDevice::report(const char* topic, const uint32_t* value, double deadband,
                        mq_time_t atLeastEveryMs, uint8_t qos, bool retain) {
    return addReport(topic, value, MqttReportType::UInt32, deadband,
                     atLeastEveryMs, qos, retain);
}

bool MqttDevice::report(const char* topic, const float* value, double deadband,
                        mq_time_t atLeastEveryMs, uint8_t qos, bool retain) {
    return addReport(topic, value, MqttReportType::Float, deadband,
                     atLeastEveryMs, qos, retain);
}

bool MqttDevice::report(const char* topic, const double* value, double deadband,
                        mq_time_t atLeastEveryMs, uint8_t qos, bool retain) {
    return addReport(topic, value, MqttReportType::Double, deadband,
                     atLeastEveryMs, qos, retain);
}

bool MqttDevice::stopReporting(const char* topic) {
    if (topic == nullptr) return false;
    for (size_t i = 0; i < MQTT_DEVICE_MAX_REPORTS; i++) {
        if (_reports[i].inUse && _reports[i].topic == topic) {
            _reports[i].inUse = false;
            _reports[i].source = nullptr;
            return true;
        }
    }
    return false;
}

size_t MqttDevice::reportCount() const {
    size_t count = 0;
    for (size_t i = 0; i < MQTT_DEVICE_MAX_REPORTS; i++) {
        if (_reports[i].inUse) count++;
    }
    return count;
}

const MqttDeviceReport* MqttDevice::reportAt(size_t index) const {
    size_t seen = 0;
    for (size_t i = 0; i < MQTT_DEVICE_MAX_REPORTS; i++) {
        if (!_reports[i].inUse) continue;
        if (seen == index) return &_reports[i];
        seen++;
    }
    return nullptr;
}

double MqttDevice::readReport(const MqttDeviceReport& report) {
    // Read through the type it was registered as. The switch is what makes a
    // wrong type a compile error at the call rather than four bytes read as
    // eight here.
    switch (report.type) {
        case MqttReportType::Bool:   return *(const bool*)report.source ? 1.0 : 0.0;
        case MqttReportType::Int32:  return (double)*(const int32_t*)report.source;
        case MqttReportType::UInt32: return (double)*(const uint32_t*)report.source;
        case MqttReportType::Float:  return (double)*(const float*)report.source;
        case MqttReportType::Double: return *(const double*)report.source;
    }
    return 0.0;
}

void MqttDevice::publishReport(MqttDeviceReport& report, double value, mq_time_t now) {
    char buffer[40];
    switch (report.type) {
        case MqttReportType::Bool:
            formatNumber(value != 0.0, buffer, sizeof buffer);
            break;
        case MqttReportType::Int32:
            formatNumber((long long)*(const int32_t*)report.source,
                         buffer, sizeof buffer);
            break;
        case MqttReportType::UInt32:
            formatNumber((unsigned long long)*(const uint32_t*)report.source,
                         buffer, sizeof buffer);
            break;
        case MqttReportType::Float:
            formatNumber(*(const float*)report.source, buffer, sizeof buffer);
            break;
        case MqttReportType::Double:
            formatNumber(value, buffer, sizeof buffer);
            break;
    }

    const MqttResult result = _client.publish(report.topic.c_str(),
                                              (const uint8_t*)buffer, strlen(buffer),
                                              report.qos, report.retain);
    if (!mq_accepted(result)) {
        // Refused - the queue is full, or the peer will not take it at this
        // QoS. Left as NOT published, so the next poll() tries again rather
        // than recording a value that never went and going quiet for an hour.
        return;
    }

    report.lastValue = value;
    report.lastPublishedAt = now;
    report.everPublished = true;
    report.publishes++;
}

void MqttDevice::serviceReports(bool force) {
    // Nothing goes anywhere while the link is down. A publish would be QUEUED,
    // which is right for a caller that asked for one - but a change-driven
    // reporter would fill the queue with every intermediate value of an outage
    // and then send a history nobody wanted. What a subscriber needs after an
    // outage is the value NOW, and that is what reportAll() on the session
    // hook gives it.
    if (!force && !_client.isConnected()) return;

    const mq_time_t now = mq_millis();
    for (size_t i = 0; i < MQTT_DEVICE_MAX_REPORTS; i++) {
        MqttDeviceReport& report = _reports[i];
        if (!report.inUse || report.source == nullptr) continue;

        const double value = readReport(report);

        if (force || !report.everPublished) {
            publishReport(report, value, now);
            continue;
        }

        double moved = value - report.lastValue;
        if (moved < 0.0) moved = -moved;

        const bool farEnough = moved > report.deadband ||
                               (report.deadband == 0.0 && moved != 0.0);
        const bool quietTooLong = report.atLeastEveryMs != 0 &&
            mq_elapsed(report.lastPublishedAt, now) >= report.atLeastEveryMs;

        if (farEnough || quietTooLong) publishReport(report, value, now);
    }
}

void MqttDevice::reportAll() { serviceReports(true); }

// =============================================================================
// SUBSCRIBING
// =============================================================================

MqttDeviceSubscription* MqttDevice::findSlot(const char* filter) {
    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        if (_subscriptions[i].inUse && _subscriptions[i].filter == filter) {
            return &_subscriptions[i];
        }
    }
    return nullptr;
}

const MqttDeviceSubscription* MqttDevice::findSlot(const char* filter) const {
    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        if (_subscriptions[i].inUse && _subscriptions[i].filter == filter) {
            return &_subscriptions[i];
        }
    }
    return nullptr;
}

MqttDeviceSubscription* MqttDevice::slotFor(const char* filter, uint8_t qos) {
    MqttDeviceSubscription* existing = findSlot(filter);
    if (existing) {
        // REPLACED, not added. setup() run twice, or a handler changed at
        // runtime, must not end in every message being delivered twice.
        existing->qos = qos;
        existing->simple = nullptr;
        existing->withContext = nullptr;
        existing->context = nullptr;
        return existing;
    }

    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        if (_subscriptions[i].inUse) continue;
        MqttDeviceSubscription& slot = _subscriptions[i];
        slot = MqttDeviceSubscription();
        slot.inUse = true;
        slot.filter = filter;
        slot.qos = qos;
        return &slot;
    }

    // Full. Refused, and the caller is told - a subscription that was accepted
    // and never issued is an application that believes it is listening.
    return nullptr;
}

bool MqttDevice::issue(MqttDeviceSubscription& slot) {
    ensureBegun();

    // The slot is the completion's context, which is why the slots live in a
    // fixed array and never move. One caveat, stated rather than hidden: a
    // slot freed by unsubscribe() while its SUBACK is still outstanding, and
    // immediately reused for a different filter, records that acknowledgement
    // against the new filter. It costs a wrong diagnostic and nothing else.
    const MqttResult result = _client.subscribe(slot.filter.c_str(), slot.qos,
                                                &MqttDevice::subscribeTrampoline,
                                                &slot);
    slot.lastResult = result;
    slot.acknowledged = false;
    return mq_accepted(result);
}

bool MqttDevice::onMessage(const char* filter, MqttDeviceMessageFn handler, uint8_t qos) {
    if (filter == nullptr) return false;
    MqttDeviceSubscription* slot = slotFor(filter, qos);
    if (slot == nullptr) return false;
    slot->simple = handler;
    return issue(*slot);
}

bool MqttDevice::onMessage(const char* filter, MqttDeviceMessageContextFn handler,
                           void* context, uint8_t qos) {
    if (filter == nullptr) return false;
    MqttDeviceSubscription* slot = slotFor(filter, qos);
    if (slot == nullptr) return false;
    slot->withContext = handler;
    slot->context = context;
    return issue(*slot);
}

void MqttDevice::onMessage(MqttDeviceMessageFn handler) {
    _anySimple = handler;
}

void MqttDevice::onMessage(MqttDeviceMessageContextFn handler, void* context) {
    _anyContext = handler;
    _anyContextValue = context;
}

bool MqttDevice::subscribe(const char* filter, uint8_t qos) {
    if (filter == nullptr) return false;
    MqttDeviceSubscription* slot = slotFor(filter, qos);
    if (slot == nullptr) return false;
    return issue(*slot);
}

bool MqttDevice::unsubscribe(const char* filter) {
    if (filter == nullptr) return false;
    if (!_begun) return false;

    MqttDeviceSubscription* slot = findSlot(filter);
    if (slot) {
        slot->inUse = false;
        slot->simple = nullptr;
        slot->withContext = nullptr;
        slot->context = nullptr;
    }
    return mq_accepted(_client.unsubscribe(filter));
}

size_t MqttDevice::subscriptionCount() const {
    size_t count = 0;
    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        if (_subscriptions[i].inUse) count++;
    }
    return count;
}

const MqttDeviceSubscription* MqttDevice::subscriptionAt(size_t index) const {
    // Indexes the SUBSCRIBED ones in order, so a caller walking 0..count()
    // never has to know that the storage underneath has holes in it.
    size_t seen = 0;
    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        if (!_subscriptions[i].inUse) continue;
        if (seen == index) return &_subscriptions[i];
        seen++;
    }
    return nullptr;
}

const MqttDeviceSubscription* MqttDevice::subscriptionFor(const char* filter) const {
    return findSlot(filter);
}

bool MqttDevice::isFresh(const char* filter, mq_time_t withinMs) const {
    if (!_client.isConnected()) return false;
    const MqttDeviceSubscription* slot = findSlot(filter);
    if (slot == nullptr || !slot->everReceived) return false;
    return mq_elapsed(slot->lastMessageAt, mq_millis()) <= withinMs;
}

mq_time_t MqttDevice::sinceMessage(const char* filter) const {
    const MqttDeviceSubscription* slot = findSlot(filter);
    if (slot == nullptr || !slot->everReceived) return 0xFFFFFFFFu;
    return mq_elapsed(slot->lastMessageAt, mq_millis());
}

// =============================================================================
// EVENTS
// =============================================================================

void MqttDevice::onConnected(MqttDeviceEventFn handler) { _onConnectedSimple = handler; }

void MqttDevice::onConnected(MqttDeviceEventContextFn handler, void* context) {
    _onConnectedContext = handler;
    _onConnectedValue = context;
}

void MqttDevice::onDisconnected(MqttDeviceEventFn handler) {
    _onDisconnectedSimple = handler;
}

void MqttDevice::onDisconnected(MqttDeviceEventContextFn handler, void* context) {
    _onDisconnectedContext = handler;
    _onDisconnectedValue = context;
}

// =============================================================================
// DISPATCH
// =============================================================================

void MqttDevice::messageTrampoline(void* context, const MqttMessage& message) {
    ((MqttDevice*)context)->dispatch(message);
}

void MqttDevice::sessionTrampoline(void* context, bool sessionPresent) {
    ((MqttDevice*)context)->sessionEstablished(sessionPresent);
}

void MqttDevice::stateTrampoline(void* context, MqttState state, MqttResult reason) {
    ((MqttDevice*)context)->stateChanged(state, reason);
}

void MqttDevice::subscribeTrampoline(void* context, MqttResult result, uint16_t) {
    // The context is the slot that asked, so this is the one subscription the
    // answer belongs to - not a guess at which of several it might have been.
    MqttDeviceSubscription* slot = (MqttDeviceSubscription*)context;
    if (slot == nullptr) return;
    slot->lastResult = result;
    slot->acknowledged = mq_succeeded(result);
}

void MqttDevice::dispatch(const MqttMessage& message) {
    if (_provisioning) {
        // The key's session hears only its own reply.
        _provisioner->reply(*this, message);
        return;
    }

    // NUL-terminated copies, because the whole point of this layer is that a
    // handler can use strcmp and atoi without first learning what a span is.
    // The cost is one copy of the payload per message; an application moving
    // large payloads should use client().onMessage() and the spans, which copy
    // nothing at all. Said plainly in docs/api.md rather than left to be
    // measured.
    _topicText.assign((const char*)message.topic.data, message.topic.length);
    _payloadText.assign((const char*)message.payload.data, message.payload.length);

    // Copied out of the receive buffer, because the spans in MqttProperties
    // die with the next packet and a handler may keep the payload object for
    // the length of its own call. Only when there is something to copy: a
    // 3.1.1 message has no properties at all, and most 5.0 messages carry
    // none, so the ordinary path is one branch.
    _inboundHasProperties =
        message.properties != nullptr && message.properties->present != 0;
    if (_inboundHasProperties) _inbound.copyForForwarding(*message.properties);

    const MqttPayload payload(message, _payloadText.c_str(), message.payload.length,
                              _inboundHasProperties ? &_inbound : nullptr);
    const mq_time_t now = mq_millis();

    for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
        MqttDeviceSubscription& slot = _subscriptions[i];
        if (!slot.inUse) continue;
        // The filter's length is already known; c_str() would throw it away
        // and mq_topicSpan() would strlen() it back, once per slot per message.
        MqttSpan filter;
        filter.data = (const uint8_t*)slot.filter.data();
        filter.length = (uint32_t)slot.filter.size();
        if (!mq_topicMatches(filter, message.topic)) continue;

        slot.received++;
        slot.lastMessageAt = now;
        slot.everReceived = true;

        if (slot.simple) slot.simple(_topicText.c_str(), payload);
        if (slot.withContext) slot.withContext(_topicText.c_str(), payload, slot.context);
    }

    // The every-message handler runs as well as, not instead of. A logger that
    // stopped seeing what a specific handler took would be a logger with a
    // hole in it.
    if (_anySimple) _anySimple(_topicText.c_str(), payload);
    if (_anyContext) _anyContext(_topicText.c_str(), payload, _anyContextValue);
}

void MqttDevice::sessionEstablished(bool sessionPresent) {
    if (_provisioning) {
        _provisioner->session(*this);
        return;
    }
    _sessionWasResumed = sessionPresent;

    // "online", retained, ahead of anything the application queued while the
    // link was down (R7 - the client turns on priority mode around this hook).
    // A status that arrived after five minutes of backlog would be describing
    // a device that had been up for five minutes.
    if (_statusIsText && !_statusTopic.empty()) publishStatus(_statusOnline);

    // Everything reported goes out in full, whatever its deadband says. A
    // subscriber that has just arrived knows nothing, and waiting for the next
    // change could be hours - and on a retained topic it would be showing a
    // value from before the outage the whole time.
    reportAll();

    _wasConnected = true;
    if (_onConnectedSimple) _onConnectedSimple();
    if (_onConnectedContext) _onConnectedContext(_onConnectedValue);
}

void MqttDevice::stateChanged(MqttState state, MqttResult reason) {
    (void)reason;

    if (_provisioner) _provisioner->state(*this, state);

    // [V5:MQTT-3.1.2-11] - a Server with Retain Available 0 refuses a CONNECT
    // carrying a retained Will. Ours is retained because statusTopic() made it
    // so, and retrying the identical CONNECT would fail identically for ever.
    if (state != MqttState::Connected && _statusIsText &&
        _connection.will.retain && !_willRetainRefused &&
        _client.status().lastReasonCode == MQ_REASON_RETAIN_NOT_SUPPORTED) {
        _retryWithoutWillRetain = true;
    }

    if (state != MqttState::Connected && _wasConnected) {
        _wasConnected = false;

        // A subscription the broker acknowledged on a session that has gone is
        // not acknowledged any more. Left true, isFresh() and the counters
        // would describe a connection that no longer exists.
        for (size_t i = 0; i < MQTT_DEVICE_MAX_SUBSCRIPTIONS; i++) {
            _subscriptions[i].acknowledged = false;
        }

        if (_onDisconnectedSimple) _onDisconnectedSimple();
        if (_onDisconnectedContext) _onDisconnectedContext(_onDisconnectedValue);
    }
}

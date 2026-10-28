// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttClient.h"

#include "codec/MqttCodecV5.h"
#include "codec/MqttCodecV311.h"

#include <stdint.h>
#include <string.h>
#include "MqttNoExcept.h"

namespace {

// One of each, shared by every client in the process. They hold no state -
// every codec method is a pure function over the buffers it is handed - so
// sharing them costs nothing and saves one per connection on a gateway holding
// hundreds.
const MqttCodecV5 g_codecV5;
const MqttCodecV311 g_codecV311;

// How many whole packets one poll() will take off the wire.
//
// The same bound the broker applies, for the same reason: a peer that sends
// faster than this end consumes must not be able to hold poll() open for ever.
// On a PLC that is the scan cycle stopping.
const size_t kPacketsPerPoll = 8;

// MqttSpan::length IS 32 BITS, and these must not narrow it.
//
// A uint16_t cast here silently capped clientId, userName, password, the Will
// topic and the Will payload at 64 kB - and silently is the problem: the
// truncation happened BEFORE the encoder, so a 65,540-byte Will payload became
// a 4-byte one and went out looking perfectly well formed. MqttWriter::binary
// is built to REFUSE an oversized field, which is the honest answer; it never
// got the chance to.
MqttSpan spanOf(const std::string& text) {
    MqttSpan span;
    if (text.empty()) return span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

MqttSpan spanOf(const std::vector<uint8_t>& bytes) {
    MqttSpan span;
    if (bytes.empty()) return span;
    span.data = bytes.data();
    span.length = (uint32_t)bytes.size();
    return span;
}

#if MQTT_LEAN
// A board's fixed fields (MqttFixed.h).
MqttSpan spanOf(const MqttText& text) {
    MqttSpan span;
    if (text.empty()) return span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

MqttSpan spanOf(const MqttBytes& bytes) {
    MqttSpan span;
    if (bytes.empty()) return span;
    span.data = bytes.data();
    span.length = (uint32_t)bytes.size();
    return span;
}
#endif

// Deterministic jitter. A device has no entropy at start-up, and all that is
// needed is that forty PLCs restarting together land on different numbers -
// otherwise they present the broker with forty simultaneous connects, back off
// identically, and do it again.
//
// THE SEED MUST DIFFER BETWEEN DEVICES, and for a long time it did not: it was
// the reconnect count alone. Forty PLCs that lost the broker at the same moment
// all have the same count, so they all computed the same "spread" and came back
// in lockstep - the exact stampede this function exists to prevent. The count
// is still part of it, so one device's successive delays differ; deviceSeed()
// is what makes devices differ from each other.
mq_time_t spread(uint32_t seed, mq_time_t span) {
    if (span == 0) return 0;
    uint32_t hash = seed * 2654435761u;      // Knuth multiplicative
    hash ^= hash >> 13;
    hash *= 2246822519u;
    hash ^= hash >> 16;
    return (mq_time_t)(hash % (uint32_t)span);
}

// What makes one device's jitter differ from another's: a hash of its Client
// Identifier, which is unique per device by the protocol's own rule
// [V5:3.1.3.1]. FNV-1a, because it is four lines and spreads short similar
// strings ("pump-01", "pump-02") well enough for this.
//
// A client that leaves the identifier empty for the broker to assign has
// nothing unique of its own to hash at the moment it first needs a delay, so
// it falls back to its own address - different per instance in one process,
// and on a board at least not the same constant for every device.
uint32_t deviceSeed(const MQ_STRING_REF& clientId, const void* self) {
    if (clientId.empty()) {
        const uintptr_t address = (uintptr_t)self;
        return (uint32_t)(address ^ (address >> 16) ^ ((uint64_t)address >> 32));
    }
    uint32_t hash = 2166136261u;
    for (size_t index = 0; index < clientId.size(); index++) {
        hash ^= (uint8_t)clientId[index];
        hash *= 16777619u;
    }
    return hash;
}

} // namespace

const char* mq_stateName(MqttState state) {
    switch (state) {
        case MqttState::Idle:            return "idle";
        case MqttState::Connecting:      return "connecting";
        case MqttState::AwaitingConnAck: return "awaiting acknowledgement";
        case MqttState::Authenticating:  return "authenticating";
        case MqttState::Connected:       return "connected";
        case MqttState::Disconnecting:   return "disconnecting";
        case MqttState::Reconnecting:    return "reconnecting";
    }
    return "?";
}

MqttClient::MqttClient() MQ_NOEXCEPT {
    _status.state = MqttState::Idle;
#if MQTT_LEAN
    _session.bind(_area);
    _operations.bind(_area);
#endif
}

const MqttCodec& MqttClient::codecFor(MqttVersion version) const {
    return version == MqttVersion::V5 ? (const MqttCodec&)g_codecV5
                                      : (const MqttCodec&)g_codecV311;
}

void MqttClient::begin(MqttTransport& transport,
                       const MqttConnectionConfig& connection,
                       const MqttClientConfig& behaviour) {
    _transport = &transport;
    _connection = connection;
    _behaviour = behaviour;
    if (_transmitWanted > _behaviour.transmitBufferSize) {
        _behaviour.transmitBufferSize = _transmitWanted;
    }

    _activeVersion = connection.version;
    _fallbackTried = false;
    _restarting = false;
    _firstConnectAsked = false;
    _firstConnectHeld = false;
    _stable = false;

    _link.begin(transport, _behaviour.receiveBufferSize, _behaviour.transmitBufferSize,
                _behaviour.initialBufferSize);
#if MQTT_LEAN
    sizeArea();
#else
    _operations.setCapacity(behaviour.queueCapacity);
#endif
    _reconnectDelay = behaviour.reconnectInitialMs;
    _status = MqttStatus();
    _stayDown = false;
    _status.version = _activeVersion;
    enterState(MqttState::Idle);
}

#if MQTT_LEAN
void MqttClient::sizeArea() {
    const MqttClientConfig& behaviour = _behaviour;
    // A board's byte area, set aside once: room for the queue it is given -
    // MQTT_QUEUE_BYTES and MQTT_AREA_ENTRIES for the default queue, in
    // proportion for a larger one - or for any message the link can send,
    // whichever is more, twice over: queued, and its resend copy. It grows
    // only if a later begin() asks for more, keeping what it holds. When the
    // memory is not there the queue keeps the size it had, and
    // queueOutOfMemory() says so.
    const size_t operations = behaviour.queueCapacity ? behaviour.queueCapacity : 1;
    const size_t perOperation = MQTT_QUEUE_BYTES / MQTT_QUEUE_CAPACITY;
    size_t largest = MQTT_QUEUE_BYTES;
    size_t entries = MQTT_AREA_ENTRIES;
    if (operations > MQTT_QUEUE_CAPACITY) {
        largest = operations <= (size_t)-1 / 4 / perOperation
                      ? operations * perOperation : (size_t)-1 / 4;
        entries = operations <= MqttByteArea::kMaxEntries
                      ? operations * MQTT_AREA_ENTRIES / MQTT_QUEUE_CAPACITY
                      : MqttByteArea::kMaxEntries + 1;
    }
    if (behaviour.transmitBufferSize > largest) largest = behaviour.transmitBufferSize;
    const size_t areaSize = largest <= (size_t)-1 / 4 ? 2 * largest : 0;
    _areaOutOfMemory = false;
    if (areaSize == 0) {
        _areaOutOfMemory = true;
    } else if (areaSize > _area.capacity()) {
        if (_areaBytes.replace(areaSize)) {
            _area.move(_areaBytes.data(), areaSize);
            _areaBytes.releaseOld();
        } else {
            _areaOutOfMemory = true;
        }
    }
    if (!_area.entries(entries)) _areaOutOfMemory = true;
    if (!_areaOutOfMemory) _operations.setCapacity(behaviour.queueCapacity);
}
#endif
MqttResult MqttClient::reconfigure(const MqttConnectionConfig& connection) {
    // Nothing to reconfigure before begin() has been given a transport, and
    // accepting it would leave a config that the next begin() silently
    // overwrote.
    if (_transport == nullptr) return MqttResult::NotConnected;

    _connection = connection;

    // The version asked for may have changed, and the fallback state has to go
    // with it: _fallbackTried records that we already dropped to 3.1.1 on THIS
    // configuration. Carrying it across a reconfigure would mean a node that
    // failed over to a second broker never tried 5.0 there.
    //
    // Only reset when the asked-for version actually changed, so a reconfigure
    // that merely rotates a Will does not undo a fallback the current server
    // already forced.
    if (_activeVersion != connection.version && _status.state == MqttState::Idle) {
        _activeVersion = connection.version;
        _fallbackTried = false;
        _status.version = _activeVersion;
    }

    // Deliberately NOT touched: _status and its counters, _link and its
    // buffers, _operations and its queue, _session, and the state machine. A
    // caller reconfiguring mid-session keeps every one of them, which is the
    // whole difference between this and begin().
    return MqttResult::Ok;
}

MqttResult MqttClient::rebind(MqttTransport& transport) {
    if (_transport == nullptr) return MqttResult::NotConnected;
    if (_transport == &transport) return MqttResult::Ok;

    // Down until connect() - and first, so closing does not schedule a
    // reconnect on the transport that is going away.
    _stayDown = true;
    if (_status.state != MqttState::Idle) {
        // Concluded as a dropped connection is: what was sent stays in the
        // session (a clean start discards it anyway), what was not concludes.
        closeConnection(MqttResult::Cancelled);
    } else {
        _transport->end();
    }
    enterState(MqttState::Idle);

    _transport = &transport;
    _link.rebind(transport);
    return MqttResult::Ok;
}

void MqttClient::end() {
    if (_transport) _transport->end();

    // The contract does not lapse because the application stopped. Everything
    // outstanding concludes, so a caller holding a slot per publish gets every
    // one of them back.
    _operations.concludeAll(MqttResult::Cancelled);
    _operations.drainDeferred();
    _session.clear();
    enterState(MqttState::Idle);
    _status.connected = false;
}

void MqttClient::enterState(MqttState state, MqttResult reason) {
    if (_status.state == state) return;
    _status.state = state;
    _status.connected = (state == MqttState::Connected);
    _stateSince = mq_millis();
    if (reason != MqttResult::Ok) _status.lastError = reason;
    for (MqttClientHook* hook = _hooks; hook != nullptr;) {
        MqttClientHook* next = hook->_nextHook;
        hook->stateChanged(state, reason);
        hook = next;
    }
    if (_onState) _onState(_stateContext, state, reason);
}
void MqttClient::onMessage(MqttMessageFn handler, void* context) {
    _onMessage = handler;
    _messageContext = context;
}

void MqttClient::onSessionEstablished(MqttSessionFn handler, void* context) {
    _onSession = handler;
    _sessionContext = context;
}

void MqttClient::onStateChange(MqttStateFn handler, void* context) {
    _onState = handler;
    _stateContext = context;
}

// =============================================================================
// CONNECTING
// =============================================================================

MqttResult MqttClient::connect() {
    if (!_transport) return MqttResult::NotInitialised;
    // Asking to connect ends any intentional disconnect.
    _stayDown = false;
    if (_status.state == MqttState::Connected ||
        _status.state == MqttState::AwaitingConnAck) {
        return MqttResult::Duplicate;
    }

    // Clean Start means the peer discards any session it holds, so our stored
    // publishes go too: [V5:MQTT-4.4.0-1] only permits resending onto a
    // session that is present, and without one they have nowhere to go.
    //
    // The SUBSCRIPTIONS STAY. They are the application's standing intent, not
    // the peer's session state - asked for in setup() and still wanted - and
    // clearing them here would mean a Clean Start connection silently
    // delivering nothing the application subscribed to.
    if (_connection.cleanStart) _session.resetProtocolState();

    // A Will that needs 5.0 cannot be expressed at 3.1.1, and sending it
    // stripped is the one thing this library will not do: a Will Delay
    // silently lost turns a flapping link into a stream of false offline
    // alarms, which is the exact failure the delay was set to prevent.
    if (_connection.will.enabled && _connection.will.needsV5() &&
        _connection.version == MqttVersion::V311) {
        return MqttResult::NotSupportedByProtocol;
    }

    _fallbackTried = false;
    _activeVersion = _connection.version;
    _status.version = _activeVersion;

    // A new attempt has had no answer yet. See MqttStatus::connectReasonCode
    // for why this is not lastReasonCode.
    _status.connectReasonCode = 0;

    // THE FIRST CONNECT SINCE begin(), SPREAD - see firstConnectSpreadMs. It
    // waits exactly as a reconnect waits, in Reconnecting, and is started by
    // the same code when its moment comes; poll() never waits for it.
    const bool first = !_firstConnectAsked;
    _firstConnectAsked = true;
    if (!first) _firstConnectHeld = false;     // asked again: that is for now
    if (first && _behaviour.firstConnectSpreadMs != 0) {
        const uint32_t seed = deviceSeed(_connection.clientId, this) ^ 0x9E3779B9u;
        _reconnectAt = mq_millis() + spread(seed, _behaviour.firstConnectSpreadMs + 1);
        _firstConnectHeld = true;
        enterState(MqttState::Reconnecting);
        return MqttResult::Queued;
    }

    _transport->connectWithin(_behaviour.connectTimeoutMs);
    if (!_transport->begin()) {
        // Opening the medium failed outright, which is a different thing from
        // a peer refusing us and is reported as such.
        enterState(MqttState::Reconnecting, MqttResult::ConnectionLost);
        scheduleReconnect();
        return MqttResult::NotConnected;
    }

#if MQTT_LEAN
    _openFailure = MqttResult::ConnectionLost;
#endif
    enterState(MqttState::Connecting);
    return MqttResult::Queued;
}

void MqttClient::sendConnect() {
    for (MqttClientHook* hook = _hooks; hook != nullptr; hook = hook->_nextHook) {
        hook->prepareConnect(_connection);
    }

    MqttPacket packet;
    packet.header.type = MQ_PKT_CONNECT;

    MqttConnect& connect = packet.connect;
    connect.version = _activeVersion;
    connect.clientId = spanOf(_connection.clientId);
    connect.cleanStart = _connection.cleanStart;
    connect.keepAlive = _connection.keepAlive;

    if (_connection.hasUserName) {
        connect.hasUserName = true;
        connect.userName = spanOf(_connection.userName);
    }
    if (_connection.hasPassword) {
        connect.hasPassword = true;
        connect.password = spanOf(_connection.password);
    }

    if (_connection.will.enabled && !_connection.registration) {
        connect.hasWill = true;
        connect.will.topic = spanOf(_connection.will.topic);
        connect.will.payload = spanOf(_connection.will.payload);
        connect.will.qos = _connection.will.qos;
        connect.will.retain = _connection.will.retain;

        if (_activeVersion == MqttVersion::V5) {
            if (_connection.will.delayInterval != 0) {
                connect.will.properties.willDelayInterval = _connection.will.delayInterval;
                connect.will.properties.markPresent(MQ_PROP_WILL_DELAY_INTERVAL);
            }
            // The spans point into _connection, which outlives this encode.
            _connection.will.properties.describeTo(connect.will.properties);
        }
    }

    if (_activeVersion == MqttVersion::V5) {
        if (_connection.hasSessionExpiry) {
            MQ_PROPERTIES(packet, connect).sessionExpiryInterval = _connection.sessionExpiryInterval;
            MQ_PROPERTIES(packet, connect).markPresent(MQ_PROP_SESSION_EXPIRY_INTERVAL);
        }
        if (_connection.receiveMaximum != 0) {
            MQ_PROPERTIES(packet, connect).receiveMaximum = _connection.receiveMaximum;
            MQ_PROPERTIES(packet, connect).markPresent(MQ_PROP_RECEIVE_MAXIMUM);
        }
        // What the alias table can actually hold, not what was asked for: the
        // broker may use every alias up to the number advertised here, and one
        // above the table's size would be refused with 0x94 - a protocol error
        // this client had invited. See MqttAliases::offerable().
        const uint16_t aliases = MqttAliases::offerable(_connection.topicAliasMaximum);
        if (aliases != 0) {
            MQ_PROPERTIES(packet, connect).topicAliasMaximum = aliases;
            MQ_PROPERTIES(packet, connect).markPresent(MQ_PROP_TOPIC_ALIAS_MAXIMUM);
        }
        if (_connection.maximumPacketSize != 0) {
            MQ_PROPERTIES(packet, connect).maximumPacketSize = _connection.maximumPacketSize;
            MQ_PROPERTIES(packet, connect).markPresent(MQ_PROP_MAXIMUM_PACKET_SIZE);
        }
    }

    if (!sendPacket(packet)) {
        closeConnection(MqttResult::ConnectionLost);
        return;
    }
    enterState(MqttState::AwaitingConnAck);
}

// =============================================================================
// THE POLL
// =============================================================================

// GIVING UP ON AN OPERATION THE PEER NEVER ACKNOWLEDGED.
//
// Not a retransmission - see MqttClientConfig::acknowledgementTimeoutMs for
// the whole argument, and docs/ARCHITECTURE.md for why resending would be
// wrong. Nothing is resent; the operation is concluded and its slot and quota
// are released.
//
// This is the watchdog keepalive is not. Keepalive proves the peer's socket
// and event loop are alive, which a broker that answers every PINGREQ and
// acknowledges nothing also satisfies.
void MqttClient::serviceAcknowledgements() {
    const size_t given =
        _operations.concludeOverdue(mq_millis(),
                                    _behaviour.acknowledgementTimeoutMs,
                                    MqttResult::AckTimeout,
                                    (uint32_t)_link.bytesSent());
    if (given == 0) return;

    // Counted as publish failures, because that is what they are from the
    // application's side, and recorded as the last error so a function block
    // polling STATUS sees a reason rather than a silence.
    _status.publishFailures += (uint32_t)given;
    _status.lastError = MqttResult::AckTimeout;

    // THE QUOTA HAS TO COME BACK TOO, or freeing the slot achieves nothing:
    // the peer's Receive Maximum is what actually stops the next publish, and
    // it is tracked separately from the queue. This is the half that turns a
    // permanent stall into a recoverable one.
    for (size_t index = 0; index < given; index++) _flow.returnOne();
}

void MqttClient::poll() {
    if (!_transport) return;

    // Anything already owed is paid before new work is taken on. A conclusion
    // carried over from a handler that queued more work belongs to a caller
    // who has been waiting since the last pass.
    _operations.drainDeferred();

    switch (_status.state) {
    case MqttState::Idle:
        return;

    case MqttState::Reconnecting:
        serviceReconnect();
        return;

    case MqttState::Connecting:
        if (!_transport->isConnected()) {
#if MQTT_LEAN
            // An open that ran on a connect task and failed is answered as a
            // false begin() always was - by connect() or serviceReconnect(),
            // whichever started it: the backoff, and nothing concluded.
            if (_transport->openFailed()) {
                enterState(MqttState::Reconnecting, _openFailure);
                scheduleReconnect();
                return;
            }
#endif
            // Still handshaking. A TCP connect takes as long as it takes and
            // nothing here waits for it - but an attempt the transport has
            // already given up on is not waited out either.
            if (_transport->connectFailed() ||
                mq_elapsed(_stateSince, mq_millis()) > _behaviour.connectTimeoutMs) {
                closeConnection(MqttResult::ConnectionLost);
            }
            return;
        }
        sendConnect();
        return;

    case MqttState::AwaitingConnAck:
    case MqttState::Authenticating:
        serviceTransmit();
        serviceReceive();
        // A peer that accepts a TCP connection and then says nothing is
        // otherwise indistinguishable from a healthy one. This is NOT a
        // retransmission timer - see MqttOperations.h - it is the only way to
        // notice a broker that is wedged rather than gone.
        if (_status.state == MqttState::AwaitingConnAck &&
            mq_elapsed(_stateSince, mq_millis()) > _behaviour.connectTimeoutMs) {
            closeConnection(MqttResult::ConnectionLost);
        }
        return;

    case MqttState::Connected:
        if (!_stable && mq_elapsed(_stateSince, mq_millis()) >= (mq_time_t)MQ_RECONNECT_STABLE_MS) {
            _stable = true;
        }
        serviceTransmit();
        serviceReceive();
        if (_status.state != MqttState::Connected) return;   // receive may have closed us
        serviceKeepAlive();
        if (_status.state != MqttState::Connected) return;
        // BEFORE serviceQueue(), deliberately. An operation given up on here
        // frees a unit of the in-flight quota, and the very next thing this
        // pass does is look for something to send - so a recovered slot is
        // used on the same poll rather than the next one.
        serviceAcknowledgements();
        if (_status.state != MqttState::Connected) return;
        if (_reissueOwed) {
            _reissueOwed = false;
            reissueSubscriptions();
        }
        serviceQueue();
        return;

    case MqttState::Disconnecting:
        serviceTransmit();
        if (_link.transmitIdle()) {
            // V5:3.14.4 - after sending a DISCONNECT we MUST close the Network
            // Connection and MUST NOT send anything more on it.
            closeConnection(MqttResult::Ok);
        }
        return;
    }
}

// =============================================================================
// TRANSMIT
// =============================================================================

bool MqttClient::sendPacket(const MqttPacket& packet) {
    if (!_transport) return false;
    // V5:3.14.4 - after a DISCONNECT nothing more goes on the connection.
    // Reachable when a handler disconnects inside poll(): the PUBACK for the
    // message it was handed would otherwise follow the goodbye.
    if (_status.state == MqttState::Disconnecting) return false;
    if (!_link.send(codec(), packet)) return false;

    // Any packet at all resets the keepalive timer. [V5:3.1.2.10] measures the
    // interval from the last byte SENT, not from the last PINGREQ, so a busy
    // link never needs to ping at all.
    noteTransmitted();
    return true;
}

// "The point at which the Client finishes transmitting one MQTT Control
// Packet" [V5:3.1.2.10] - which is when its last byte has LEFT, not when it
// was queued. Marking it at the queue meant a packet that took three minutes
// to cross a slow link had "been sent" before its first byte went, and the
// keepalive measured from a moment that had not happened. So the keepalive is
// told only when the link has nothing left to send; serviceTransmit() tells it
// when a backlog finally drains.
void MqttClient::noteTransmitted() {
    if (_link.transmitIdle()) _keepAlive.noteSent(mq_millis());
}

void MqttClient::serviceTransmit() {
    if (!_transport) return;

    const size_t before = _link.transmitPending();
    _link.serviceTransmit();
    if (before > 0 && _link.transmitIdle()) _keepAlive.noteSent(mq_millis());

    // A transport that took nothing and is no longer connected has gone. The
    // link reports what it found and does not decide - closing a connection is
    // a role's decision, and a broker would drop one of hundreds rather than
    // reconnect.
    if (before > 0 && _link.transmitPending() == before &&
        !_transport->isConnected()) {
        closeConnection(MqttResult::ConnectionLost);
    }
}

// =============================================================================
// RECEIVE
// =============================================================================

void MqttClient::serviceReceive() {
    if (!_transport) return;

    // BOUNDED, for the same reason the broker bounds its own loop.
    //
    // This was `while (true)`, exited only when the peer stopped talking. A
    // broker publishing faster than this client drains means available() is
    // never zero and poll() NEVER RETURNS - which on a PLC is a scan cycle
    // that stops, with _onMessage re-entered the whole time. MqttBridge sits
    // on that callback, so a busy bridge inherited the stall directly.
    //
    // Whatever is left is read on the next poll, which is one scan away.
    for (size_t packets = 0; packets < kPacketsPerPoll; packets++) {
        _link.fill();

        MqttFixedHeader header;
        const MqttLinkStatus status = _link.inspect(header);

        switch (status) {
        case MqttLinkStatus::Idle:
            return;

        case MqttLinkStatus::Closed:
            closeConnection(MqttResult::ConnectionLost);
            return;

        case MqttLinkStatus::Incomplete:
            return;   // simply not all here yet

        case MqttLinkStatus::Malformed:
            // Five bytes and still no valid Remaining Length. There is no
            // delimiter to resynchronise on, so this cannot be recovered from.
            _status.protocolErrors++;
            failConnection(MqttResult::MalformedPacket, MQ_REASON_MALFORMED_PACKET);
            return;

        case MqttLinkStatus::TooLarge:
            // Bigger than we can hold. Our own limit, so the peer is told with
            // 0x95 rather than blamed for a malformed packet.
            failConnection(MqttResult::PacketTooLarge, MQ_REASON_PACKET_TOO_LARGE);
            return;

        case MqttLinkStatus::Packet:
            break;
        }

        // --- A whole packet is present ---
        //
        // The CONNACK is decoded with a VERSION-TOLERANT rule, because a peer
        // that speaks only 3.1.1 answers a 5.0 CONNECT with a 3.1.1-shaped
        // CONNACK. The discriminator is exact: a 3.1.1 CONNACK has a Remaining
        // Length of exactly 2, and a 5.0 one has a Property Length after the
        // reason code and so is 3 or more.
        const MqttCodec* useCodec = &codec();
        if (_status.state == MqttState::AwaitingConnAck && header.type == MQ_PKT_CONNACK) {
            useCodec = header.remainingLength == 2 ? (const MqttCodec*)&g_codecV311
                                                   : (const MqttCodec*)&g_codecV5;
        }

        MqttPacket packet;
        const MqttError error = _link.take(*useCodec, header, packet);

        if (error != MqttError::None) {
            _status.protocolErrors++;
            // THE STREAM NEVER RESYNCHRONISES. One stray octet desynchronises
            // for good and every packet after it is garbage that may still
            // parse, so the connection ends here.
            failConnection(MqttResult::MalformedPacket, mq_errorToReason(error));
            return;
        }

        handlePacket(header, packet);
        if (_status.state != MqttState::Connected &&
            _status.state != MqttState::AwaitingConnAck &&
            _status.state != MqttState::Authenticating) {
            return;
        }
    }
}

// =============================================================================
// DISPATCH
// =============================================================================

void MqttClient::handlePacket(const MqttFixedHeader& header, const MqttPacket& packet) {
    // Anything from the peer proves it is alive, so it counts against the
    // keepalive just as a PINGRESP does.
    _keepAlive.noteReceived(mq_millis());

    switch (header.type) {
    case MQ_PKT_CONNACK:
        handleConnAck(packet);
        break;

    case MQ_PKT_PUBLISH:
        handlePublish(packet);
        break;

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP:
        handleAck(header, packet);
        break;

    case MQ_PKT_SUBACK:
    case MQ_PKT_UNSUBACK:
        handleSubAck(header, packet);
        break;

    case MQ_PKT_PINGRESP:
        // Already counted above. Nothing else to do: its only job is to have
        // arrived.
        break;

    case MQ_PKT_DISCONNECT: {
        // V5 only; 3.1.1 has no peer-initiated DISCONNECT at all. The reason
        // code is what separates "the peer is shutting down cleanly" from
        // "another connection took our identifier", and an application
        // reconnecting blindly to the second would fight for the session for
        // ever.
        _status.lastReasonCode = packet.disconnect.reasonCode;
        const MqttResult reason =
            mq_reasonToResult(packet.disconnect.reasonCode, MQ_PKT_DISCONNECT);
        closeConnection(reason == MqttResult::Ok ? MqttResult::ConnectionLost : reason);
        break;
    }

    case MQ_PKT_AUTH:
        // The exchange itself needs a mechanism, and none ships in v1. The
        // state exists so adding one is a handler rather than a reshaped state
        // machine - retrofitting a round trip into a settled one is far worse.
        _status.lastReasonCode = packet.auth.reasonCode;
        failConnection(MqttResult::NotSupportedByProtocol, MQ_REASON_BAD_AUTHENTICATION_METHOD);
        break;

    default:
        // A packet only ever sent in the other direction. A conforming peer
        // does not send one, so this is a peer with a bug and the stream can
        // no longer be trusted.
        failConnection(MqttResult::ProtocolError, MQ_REASON_PROTOCOL_ERROR);
        break;
    }
}

// =============================================================================
// CONNACK
// =============================================================================

void MqttClient::handleConnAck(const MqttPacket& packet) {
    if (_status.state != MqttState::AwaitingConnAck &&
        _status.state != MqttState::Authenticating) {
        // A second CONNACK on one connection. V5:3.2 - one is sent per
        // connection, so this peer is not following the protocol.
        failConnection(MqttResult::ProtocolError, MQ_REASON_PROTOCOL_ERROR);
        return;
    }

    const MqttConnAck& connack = packet.connack;
    _status.lastReasonCode = connack.reasonCode;
    _status.connectReasonCode = connack.reasonCode;

    // THE VERSION IS WHAT THE ANSWER WAS SHAPED LIKE, not what we asked for.
    // This is how a fallback finds out: we sent a level 5 CONNECT and a
    // 3.1.1-only peer answered in its own dialect.
    const bool answeredAtV311 = connack.version == MqttVersion::V311;

    const MqttResult outcome = answeredAtV311
        ? mq_returnCodeToResult(connack.reasonCode)
        : mq_reasonToResult(connack.reasonCode, MQ_PKT_CONNACK);

    if (outcome != MqttResult::Ok) {
        // --- Fallback ---
        //
        // Only on evidence that the VERSION is the problem. Never on an
        // authentication failure, a TLS failure or a network error: treating a
        // network fault as a version problem hides the network fault, and
        // retrying a rejected password at another version is how a client gets
        // itself banned.
        const bool versionRejected =
            (answeredAtV311 && connack.reasonCode == MQ_RETURN_UNACCEPTABLE_PROTOCOL) ||
            (!answeredAtV311 && connack.reasonCode == MQ_REASON_UNSUPPORTED_PROTOCOL_VERSION);

        // A fallback that would land on a version which cannot express our
        // Will is refused rather than taken. Connecting anyway would mean a
        // device that believes it has a thirty-second Will Delay and has none.
        const bool fallbackWouldLoseTheWill =
            _connection.will.enabled && _connection.will.needsV5() &&
            _connection.fallbackVersion == MqttVersion::V311;

        if (versionRejected && fallbackWouldLoseTheWill) {
            failConnection(MqttResult::NotSupportedByProtocol,
                           MQ_REASON_UNSUPPORTED_PROTOCOL_VERSION);
            return;
        }

        if (versionRejected && _connection.fallbackEnabled && !_fallbackTried &&
            _activeVersion != _connection.fallbackVersion) {
            _fallbackTried = true;
            _activeVersion = _connection.fallbackVersion;
            _status.version = _activeVersion;

            // Stored publishes are ENCODED BYTES, so a 5.0 packet replayed
            // onto a 3.1.1 connection would be gibberish. A session cannot
            // span a version change.
            _session.clear();

            closeConnection(MqttResult::ConnectionRefused);
            // Immediately rather than after a backoff: this is a known answer
            // to a known question, not a failure to wait out.
            _reconnectAt = mq_millis();
            enterState(MqttState::Reconnecting);
            return;
        }

        _status.lastError = outcome;
        closeConnection(outcome);
        return;
    }

    // --- Accepted ---
    _activeVersion = connack.version;
    _status.version = _activeVersion;

    if (_activeVersion == MqttVersion::V5) {
        _capabilities.applyV5(MQ_PROPERTIES(packet, connack));
    } else {
        _capabilities.applyV311Defaults();
    }
    _capabilities.sessionPresent = connack.sessionPresent;

    // [V5:MQTT-3.2.2-4] If a Client sets Clean Start to 1 and receives
    // Session Present 1, it is a Protocol Error - the peer is telling us it
    // kept a session we asked it to discard.
    if (_connection.cleanStart && connack.sessionPresent) {
        failConnection(MqttResult::ProtocolError, MQ_REASON_PROTOCOL_ERROR);
        return;
    }

    // Re-initialised on EVERY connection, never carried across. V5:4.9 for the
    // quota, V5:3.3.2.3.4 for the aliases. Carrying an alias across a
    // reconnect means publishing to whatever it used to mean - a bug that
    // works, and puts the data somewhere else.
    _flow.reset(_capabilities.receiveMaximum);
    _aliases.reset(_capabilities.topicAliasMaximum, _connection.topicAliasMaximum);

    // V5:3.2.2.3.14 - a Server Keep Alive REPLACES ours as the interval we
    // are OBLIGED to keep, and an implementation that kept measuring against
    // what it asked for would ping too rarely for a broker that wants less.
    //
    // BUT IT DOES NOT GET TO SWITCH OFF OUR DEAD-LINK DETECTION. [V5:3.12.4]
    // "The Client can send PINGREQ at any time, irrespective of the Keep
    // Alive value, and check for a corresponding PINGRESP to determine that
    // the network and the Server are available." Taken literally, a Server
    // Keep Alive of 0 turned the keepalive off - and with it the only
    // watchdog a half-open cellular link has - and one of an hour let a
    // carrier's NAT, which forgets an idle mapping in minutes, drop the
    // connection silently in between. So the client runs at the SHORTER of
    // the two: never less often than the broker requires, never less often
    // than the application asked for. Only the application's own 0 turns it
    // off, because that is a decision somebody made (and the host sockets
    // keep the kernel's backstop under it - mq_socket::tuneDialledSocket).
    const uint16_t ours = _connection.keepAlive;
    const uint16_t theirs = _capabilities.keepAlive;
    uint16_t effective = ours;
    if (_capabilities.keepAliveOverridden && theirs != 0) {
        effective = (ours == 0 || theirs < ours) ? theirs : ours;
    }
    _keepAlive.begin(effective, mq_millis());
    _seenReceived = _link.bytesReceived();
    _seenRelieved = _link.bytesRelieved();

    _status.connects++;
    _stable = false;                 // earned again by this connection, on the poll
    // NOT a reset of the backoff. A CONNACK says the broker answered, not that
    // the connection works: one that accepts and then drops this client at
    // once would have it back every second for ever. The backoff starts again
    // only once the connection has stayed up for MQ_RECONNECT_STABLE_MS - see
    // closeConnection().
    enterState(MqttState::Connected);

    establishSession(connack.sessionPresent);
}

void MqttClient::establishSession(bool sessionPresent) {
    // A new connection: nothing has been sent on it yet.
    _session.clearSubscriptionsIssued();

    if (!sessionPresent) {
        // The peer has no session for us. Anything already SENT was relying on
        // one to carry it: [V5:MQTT-4.4.0-1] only permits retransmitting onto
        // a session that is present, so those have nowhere to go and
        // concluding them is the contract being kept rather than broken.
        //
        // WHAT WAS NEVER SENT IS UNTOUCHED. A publish made before connect() is
        // ordinary - it is what a scan-cycle application does in its first
        // scan - and concluding it here would discard the message the moment
        // the connection it was waiting for arrived.
        //
        // The subscriptions survive too, and are re-issued below. Clearing
        // them instead would mean every reconnect onto a fresh session
        // silently stopped delivering what the application asked for.
        _operations.concludeSent(MqttResult::SessionLost);
        _session.resetProtocolState();
    } else {
        // [V5:MQTT-4.4.0-1] THE ONE CIRCUMSTANCE IN WHICH RESENDING IS
        // REQUIRED: reconnecting with Clean Start 0 onto a session that is
        // present. Everything stored is offered again, with its original
        // Packet Identifier.
        _session.markAllUnsent();
        _operations.markAllUnsent();

        // BUT A PUBLISH THE SESSION IS REPLAYING IS NOT ALSO SENT AFRESH.
        //
        // The operation and the session's stored copy are two records of ONE
        // message, and the replay belongs to the copy: same bytes, same
        // Packet Identifier, DUP set. Putting the operation back as unsent as
        // well sent it a second time on the next drain - as a NEW message,
        // under a new identifier. Two deliveries at QoS 1, and at QoS 2 the
        // very duplicate exactly-once exists to prevent, with nothing at the
        // receiver able to tell them apart. Every reconnect onto a present
        // session did it, for every publish that was in flight.
        //
        // Only those the session actually holds: anything it does not (a
        // subscribe, a copy that could not be stored) goes again as before.
        //
        // Its acknowledgement deadline starts again now, not from the send on
        // the connection that was lost - and again when the replay leaves.
        const mq_time_t now = mq_millis();
        for (size_t index = 0;; index++) {
            MqttOperation* op = _operations.at(index);
            if (op == nullptr) break;
            if (!op->inUse() || op->kind != MqttOpKind::Publish || op->qos == MQ_QOS_0) continue;
            if (op->packetId == MQ_PACKET_ID_NONE) continue;
            if (_session.findOutbound(op->packetId) == nullptr) continue;
            op->sent = true;
            op->sentAt = now;
        }
    }

    // R7: THE HOOK RUNS BEFORE ANYTHING QUEUED IS SENT, AND WHAT IT QUEUES
    // GOES FIRST.
    //
    // A Sparkplug birth certificate has to be the first publication on a
    // connection, ahead of whatever the application queued while the link was
    // down. Running the hook early is only half of that: what it queues would
    // otherwise be appended BEHIND that traffic and go out last, introducing
    // data that had already gone past.
    //
    // So operations queued inside the hook are marked priority. The window is
    // exactly the hook's own execution - nothing else in the library ever sets
    // it, and the application has no way to.
    if (_onSession || _hooks) {
        _operations.setPriorityMode(true);
        for (MqttClientHook* hook = _hooks; hook != nullptr;) {
            MqttClientHook* next = hook->_nextHook;
            hook->sessionEstablished(sessionPresent);
            hook = next;
        }
        if (_onSession) _onSession(_sessionContext, sessionPresent);
        _operations.setPriorityMode(false);
    }
    reissueSubscriptions();
}

void MqttClient::reissueSubscriptions() {
    // A subscription is a standing intent the library owns. On a session that
    // was not present every one of them has to go out again; on one that was,
    // none may - and getting that backwards either loses them all silently or
    // subscribes twice to each.
    for (size_t i = 0; i < _session.subscriptionCount(); i++) {
        const MqttStoredSubscription* held = _session.subscriptionAt(i);
        // Already established, or already queued on THIS connection - by the
        // session hook, typically, which runs just before this.
        if (!held || held->established || held->issued) continue;

        MqttOperation op;
        op.kind = MqttOpKind::Subscribe;
        op.topic = held->filter;
        op.subscriptionOptions = held->options;
        op.supersedable = false;

        // The handler the caller gave when it subscribed, whether that was on
        // this connection or three reconnects ago. Without it, a subscribe
        // made before the link existed - which is every subscribe in a
        // setup() - would be accepted and never concluded.
        op.onComplete = held->onComplete;
        op.context = held->context;

        // Ahead of queued application traffic too. A publish that went out
        // before its own subscription was established would not come back,
        // which on a round-trip test looks like a lost message and on a plant
        // looks like a device that stopped reporting.
        op.priority = true;
        const MqttResult queued = _operations.add(op);
        if (mq_accepted(queued)) {
            _session.markSubscriptionIssued(held->filter.data(),
                                            (uint16_t)held->filter.size());
        } else if (queued == MqttResult::QueueFull) {
            // A program may hold more subscriptions than the queue holds
            // operations: the rest go as the queue drains (poll).
            _reissueOwed = true;
            break;
        }
    }
}

// =============================================================================
// INBOUND PUBLISH
// =============================================================================

void MqttClient::handlePublish(const MqttPacket& packet) {
    const MqttPublish& publish = packet.publish;

    MqttSpan topic = publish.topic;

    // --- Topic aliases, V5:3.3.2.3.4 ---
    if (_activeVersion == MqttVersion::V5 &&
        MQ_PROPERTIES(packet, publish).has(MQ_PROP_TOPIC_ALIAS)) {
        const uint16_t alias = MQ_PROPERTIES(packet, publish).topicAlias;

        if (!_aliases.inboundAllowed(alias)) {
            // Outside what we offered. We cannot receive an alias we never
            // agreed to accept.
            failConnection(MqttResult::ProtocolError, MQ_REASON_TOPIC_ALIAS_INVALID);
            return;
        }

        if (topic.length > 0) {
            // A name AND an alias: this establishes or reassigns the mapping.
            // Refused when a board cannot hold the topic in full, and then the
            // alias means nothing: this message still goes out under its own
            // full topic, and a later alias-only one is 0x94 just below -
            // never a delivery under a shortened or stale name.
            _aliases.rememberInbound(alias, topic.data, topic.length);
        } else {
            // An alias alone. It must already mean something, or the peer has
            // referred to a mapping that was never made.
            const uint8_t* known = nullptr;
            uint16_t knownLength = 0;
            if (!_aliases.lookupInbound(alias, known, knownLength)) {
                failConnection(MqttResult::ProtocolError, MQ_REASON_TOPIC_ALIAS_INVALID);
                return;
            }
            topic.data = known;
            topic.length = knownLength;
        }
    }

    if (!mq_topicNameIsValid(topic)) {
        failConnection(MqttResult::ProtocolError, MQ_REASON_TOPIC_NAME_INVALID);
        return;
    }

    bool deliver = true;

    if (publish.qos == MQ_QOS_2) {
        // [V5:MQTT-4.3.3-10] "Until it has received the corresponding PUBREL
        // packet, the receiver MUST acknowledge any subsequent PUBLISH packet
        // with the same Packet Identifier by sending a PUBREC. It MUST NOT
        // cause duplicate messages to be delivered."
        //
        // So a repeat is still acknowledged - it is just not delivered twice.
        // This is the whole of exactly-once, and skipping it makes QoS 2 an
        // expensive QoS 1.
        deliver = _session.rememberInbound(publish.packetId);
    }

    if (deliver) {
        MqttMessage message;
        message.topic = topic;
        message.payload = publish.payload;
        message.qos = publish.qos;
        message.retain = publish.retain;
        message.dup = publish.dup;
        message.packetId = publish.packetId;
        message.properties = _activeVersion == MqttVersion::V5
                                 ? &MQ_PROPERTIES(packet, publish) : nullptr;
        _status.received++;

        // Handles first, then the callback. A caller that polls and a caller
        // that registers a handler must see the same message, and the polled
        // copy has to be in place before application code runs - a handler
        // that reads its own handle would otherwise see the previous message.
        _topics.noteMessage(message.topic, message.payload);
        bool taken = false;
        for (MqttClientHook* hook = _hooks; hook != nullptr && !taken;
             hook = hook->_nextHook) {
            taken = hook->message(message);
        }
        if (!taken && _onMessage) _onMessage(_messageContext, message);
    }

    // --- Acknowledge ---
    if (publish.qos == MQ_QOS_1) {
        MqttPacket ack;
        ack.header.type = MQ_PKT_PUBACK;
        ack.ack.packetId = publish.packetId;
        sendPacket(ack);
    } else if (publish.qos == MQ_QOS_2) {
        // [V5:MQTT-4.3.3-8] respond with a PUBREC, having accepted ownership.
        MqttPacket ack;
        ack.header.type = MQ_PKT_PUBREC;
        ack.ack.packetId = publish.packetId;
        sendPacket(ack);
    }
}

// =============================================================================
// ACKNOWLEDGEMENTS
// =============================================================================

void MqttClient::handleAck(const MqttFixedHeader& header, const MqttPacket& packet) {
    const uint16_t packetId = packet.ack.packetId;
    const uint8_t reason = packet.ack.reasonCode;

    switch (header.type) {
    case MQ_PKT_PUBACK: {
        // QoS 1 is finished here.
        const MqttResult result = mq_reasonToResult(reason, MQ_PKT_PUBACK);
        if (mq_succeeded(result) || result == MqttResult::NoMatchingSubscribers) {
            _status.published++;
        } else {
            _status.publishFailures++;
            _status.lastReasonCode = reason;
        }
        _session.releaseOutbound(packetId);
        _flow.returnOne();
        _operations.concludeByPacketId(packetId, result);
        break;
    }

    case MQ_PKT_PUBREC: {
        // [V5:MQTT-4.4.0-2] A PUBREC with a reason of 0x80 or greater means
        // the PUBLISH IS ACKNOWLEDGED and MUST NOT be retransmitted. It ends
        // the exchange - no PUBREL follows - and the quota comes back.
        if (mq_reasonIsFailure(reason)) {
            _status.publishFailures++;
            _status.lastReasonCode = reason;
            _session.releaseOutbound(packetId);
            _flow.returnOne();
            _operations.concludeByPacketId(packetId, mq_reasonToResult(reason, MQ_PKT_PUBREC));
            break;
        }

        // Otherwise the second half begins. What a reconnect must resend from
        // here is the PUBREL, not the PUBLISH - resending the publish would
        // deliver the message twice, which is what QoS 2 exists to prevent.
        MqttPacket pubrel;
        pubrel.header.type = MQ_PKT_PUBREL;
        pubrel.ack.packetId = packetId;

        uint8_t encoded[64];
        size_t written = 0;
        if (codec().encode(encoded, sizeof encoded, pubrel, written) == MqttError::None) {
            _session.promoteToPubrel(packetId, encoded, written);
            // Marked sent here rather than left for the replay path, which
            // exists for reconnects and would otherwise send it twice.
            MqttStoredPublish* stored = _session.findOutbound(packetId);
            if (stored) stored->sent = true;
        }
        sendPacket(pubrel);
        break;
    }

    case MQ_PKT_PUBREL: {
        // An inbound QoS 2 exchange completing. [V5:MQTT-4.3.3-11] respond
        // with a PUBCOMP carrying the same Packet Identifier.
        _session.forgetInbound(packetId);
        MqttPacket pubcomp;
        pubcomp.header.type = MQ_PKT_PUBCOMP;
        pubcomp.ack.packetId = packetId;
        sendPacket(pubcomp);
        break;
    }

    case MQ_PKT_PUBCOMP: {
        // QoS 2 is finished here, and the identifier may be reused.
        _status.published++;
        _session.releaseOutbound(packetId);
        _flow.returnOne();
        _operations.concludeByPacketId(packetId, MqttResult::Ok);
        break;
    }

    default:
        break;
    }
}

void MqttClient::handleSubAck(const MqttFixedHeader& header, const MqttPacket& packet) {
    const MqttSubAck& ack = packet.suback;

    MqttOperation* op = _operations.findByPacketId(ack.packetId);
    MqttResult result = MqttResult::Ok;

    if (header.type == MQ_PKT_SUBACK) {
        // One reason code per filter, in the order the request listed them
        // [V5:MQTT-3.9.3-1]. A subscribe here carries exactly one, so the
        // first is the answer.
        if (ack.count > 0) {
            const uint8_t code = ack.reasons[0];
            if (_activeVersion == MqttVersion::V311) {
                result = code == MQ_SUBACK_FAILURE ? MqttResult::Refused : MqttResult::Ok;
            } else {
                result = mq_reasonToResult(code, MQ_PKT_SUBACK);
            }
            _status.lastReasonCode = code;
        }

        if (op && mq_succeeded(result)) {
            // Established, so a reconnect onto a present session will not
            // issue it again.
            _session.markSubscriptionEstablished(op->topic.data(),
                                                 (uint16_t)op->topic.size());
        }
    } else {
        // UNSUBACK. At 3.1.1 it carries no reason at all, so an unsubscribe
        // from a filter that was never subscribed looks exactly like one that
        // was - there is no way to tell, and nothing is gained by pretending.
        if (_activeVersion == MqttVersion::V5 && ack.count > 0) {
            result = mq_reasonToResult(ack.reasons[0], MQ_PKT_UNSUBACK);
            _status.lastReasonCode = ack.reasons[0];
        }
        if (op) {
            _session.removeSubscription(op->topic.data(), (uint16_t)op->topic.size());
        }
    }

    // The control reservation goes back FIRST. releasePacketId() looks at the
    // publish table only, and a SUBACK's identifier was never in it - so
    // without this the identifier stayed reserved for the life of the session.
    _session.releaseControlId(ack.packetId);
    _session.releasePacketId(ack.packetId);
    _operations.concludeByPacketId(ack.packetId, result);
}

// =============================================================================
// KEEPALIVE
// =============================================================================

void MqttClient::serviceKeepAlive() {
    const mq_time_t now = mq_millis();

    // SIGNS OF LIFE SHORT OF A WHOLE PACKET. Bytes of a large inbound packet
    // still arriving, and our own backlog draining after the transport had
    // refused it, both say the link is alive and merely slow. See
    // MqttKeepAlive for why counting only complete packets reconnected a slow
    // link in the middle of every large transfer, for ever.
    const uint64_t received = _link.bytesReceived();
    if (received != _seenReceived) {
        _seenReceived = received;
        _keepAlive.noteBytesReceived(now);
    }
    const uint64_t relieved = _link.bytesRelieved();
    if (relieved != _seenRelieved) {
        _seenRelieved = relieved;
        _keepAlive.noteProgress(now);
    }

    if (_keepAlive.responseOverdue(now)) {
        // THE ONLY WATCHDOG THERE IS. Nothing else in the library watches a
        // clock for the peer, because [V5:MQTT-4.4.0-1] forbids retransmitting
        // on a timer - so a peer that accepted the connection and then went
        // silent is invisible to everything but this.
        //
        // V5:4.1.1 - the peer closes the connection on a Keep Alive timeout,
        // and so do we rather than keeping a socket to nothing.
        _status.lastReasonCode = MQ_REASON_KEEP_ALIVE_TIMEOUT;
        closeConnection(MqttResult::KeepAliveTimeout);
        return;
    }

    // NOT BEHIND A BACKLOG. A PINGREQ queued behind a packet still leaving
    // cannot be answered before that packet has gone, so its deadline would
    // be measuring the link's bandwidth rather than the peer's health - and
    // the bytes leaving already prove what the ping would. A backlog that has
    // stopped moving is caught by the silence rule above instead.
    if (_link.transmitIdle() && _keepAlive.pingDue(now)) {
        MqttPacket ping;
        ping.header.type = MQ_PKT_PINGREQ;
        if (sendPacket(ping)) _keepAlive.notePingSent(now);
    }
}

// =============================================================================
// DRAINING THE QUEUE
// =============================================================================

void MqttClient::serviceQueue() {
    // Anything the session is holding from before the reconnect goes first.
    // [V5:MQTT-4.4.0-1] requires the original Packet Identifiers, so these are
    // replayed from their stored bytes rather than rebuilt.
    while (_link.transmitPending() == 0) {
        MqttStoredPublish* stored = _session.nextUnsent();
        if (!stored) break;

        // The DUP flag says this is a redelivery. [V5:MQTT-3.3.1-1] - "The DUP
        // flag MUST be set to 1 by the Client or Server when it attempts to
        // re-deliver a PUBLISH packet." A PUBREL has no DUP flag, so only a
        // PUBLISH is touched.
        if (!stored->awaitingPubcomp && !stored->encoded.empty()) {
            stored->encoded[0] |= MQ_PUBLISH_DUP;
        }
        if (!_link.queue(stored->encoded.data(), stored->encoded.size())) break;
        stored->sent = true;

        // The operation this copy belongs to - kept as sent across the
        // reconnect, see establishSession() - has its deadline started again
        // from this send, and from where these bytes end.
        MqttOperation* owner = _operations.findByPacketId(stored->packetId);
        if (owner != nullptr && owner->sent) {
            _operations.markSent(*owner, stored->packetId, _link.transmitMark());
        }
        serviceTransmit();
        noteTransmitted();
    }

    while (_link.transmitPending() == 0) {
        MqttOperation* op = _operations.nextUnsent();
        if (!op) break;

        MqttPacket packet;
        uint16_t packetId = MQ_PACKET_ID_NONE;

        if (op->kind == MqttOpKind::Publish) {
            // [V5:MQTT-4.9.0-2] At zero quota no more QoS>0 may be sent. QoS 0
            // still may, and this library sends it: a telemetry stream should
            // not stop because an acknowledged setpoint is outstanding.
            if (op->qos > MQ_QOS_0 && !_flow.canSendQos12()) break;

            if (op->qos > MQ_QOS_0) {
                packetId = _session.allocatePacketId();
                if (packetId == MQ_PACKET_ID_NONE) break;   // try again next poll
            }

            // A publish queued before the connection existed could not know
            // which version it would go out on. Now it is known, and 3.1.1 has
            // no properties: sending it stripped would lose a correlation id
            // or a response topic silently, so it concludes instead.
            //
            // The packet identifier allocated a moment ago is released, or a
            // 3.1.1 session would leak one per refused publish.
            if (!op->properties.empty() && _activeVersion == MqttVersion::V311) {
                if (packetId != MQ_PACKET_ID_NONE) {
                _session.releaseControlId(packetId);   // no-op unless it was one
                _session.releasePacketId(packetId);
            }
                _status.publishFailures++;
                _operations.concludeByOpId(op->opId,
                                           MqttResult::NotSupportedByProtocol);
                continue;
            }

            packet.header.type = MQ_PKT_PUBLISH;
            packet.publish.qos = op->qos;
            packet.publish.retain = op->retain;
            packet.publish.packetId = packetId;
            packet.publish.topic.data = op->topic.data();
            packet.publish.topic.length = (uint32_t)op->topic.size();
            packet.publish.payload.data = op->payload.empty() ? nullptr : op->payload.data();
            packet.publish.payload.length = (uint32_t)op->payload.size();

            // THE SPANS POINT INTO THE OPERATION, which is still in the queue
            // and stays there until this packet is encoded. That is the whole
            // reason the queue owns its properties rather than borrowing the
            // caller's.
            if (!op->properties.empty()) {
                op->properties.get()->describeTo(MQ_PROPERTIES(packet, packet.publish));
            }

        } else if (op->kind == MqttOpKind::Subscribe) {
            packetId = _session.allocatePacketId();
            if (packetId == MQ_PACKET_ID_NONE) break;
            // Recorded, so a later publish cannot be handed the same one.
            if (!_session.reserveControlId(packetId)) break;
            packet.header.type = MQ_PKT_SUBSCRIBE;
            packet.subscribe.packetId = packetId;
            packet.subscribe.count = 1;
            packet.subscribe.filters[0].filter.data = op->topic.data();
            packet.subscribe.filters[0].filter.length = (uint32_t)op->topic.size();
            packet.subscribe.filters[0].options = op->subscriptionOptions;

        } else {
            packetId = _session.allocatePacketId();
            if (packetId == MQ_PACKET_ID_NONE) break;
            if (!_session.reserveControlId(packetId)) break;
            packet.header.type = MQ_PKT_UNSUBSCRIBE;
            packet.unsubscribe.packetId = packetId;
            packet.unsubscribe.count = 1;
            packet.unsubscribe.filters[0].data = op->topic.data();
            packet.unsubscribe.filters[0].length = (uint32_t)op->topic.size();
        }

        const size_t needed = codec().size(packet);
        if (needed == 0) {
            // The connection cannot express it. Refused with a named reason
            // rather than sent with the offending part removed: a setpoint
            // whose expiry silently vanished is a stale command nobody can
            // account for, weeks later.
            if (packetId != MQ_PACKET_ID_NONE) {
                _session.releaseControlId(packetId);   // no-op unless it was one
                _session.releasePacketId(packetId);
            }
            if (op->kind == MqttOpKind::Publish) _status.publishFailures++;
            _operations.concludeByOpId(op->opId, MqttResult::NotSupportedByProtocol);
            continue;
        }
        if (!_capabilities.allowsPacketSize(needed)) {
            // The peer's Maximum Packet Size is a promise we must keep;
            // exceeding it earns a DISCONNECT with 0x95 rather than a refusal.
            if (packetId != MQ_PACKET_ID_NONE) {
                _session.releaseControlId(packetId);   // no-op unless it was one
                _session.releasePacketId(packetId);
            }
            if (op->kind == MqttOpKind::Publish) _status.publishFailures++;
            _operations.concludeByOpId(op->opId, MqttResult::PacketTooLarge);
            continue;
        }
#if MQTT_LEAN
        // On a board the resend copy goes in the byte area, into the room the
        // publish reserved when it was queued. One going out again after a
        // reconnect has none reserved, and waits for room.
        if (op->kind == MqttOpKind::Publish && op->qos > MQ_QOS_0 &&
            !_operations.makeRoomToStore(*op, needed)) {
            _session.releasePacketId(packetId);
            break;
        }
#endif

        // The bytes go out and are kept in one pass.
        //
        // A QoS>0 publish must be replayable after a reconnect
        // [V5:MQTT-4.4.0-1], so the session keeps a copy - but encoding it
        // twice would cost a second full pass over a 200 kB payload for
        // nothing. The link hands back what it queued.
        const uint8_t* encoded = nullptr;
        size_t encodedLength = 0;
        if (!_link.send(codec(), packet, &encoded, &encodedLength)) {
            // THE IDENTIFIER GOES BACK, WHATEVER THE REASON.
            //
            // Without this, every failed attempt consumed one. There are
            // MQTT_SESSION_MAX_INFLIGHT of them, so sixteen attempts drained
            // the pool and allocatePacketId() returned none for ever after -
            // QoS 1 and 2 stopped working entirely, on a healthy connection,
            // with nothing reported anywhere. One oversized message was enough
            // to silence a device's telemetry for good.
            if (packetId != MQ_PACKET_ID_NONE) {
                _session.releaseControlId(packetId);   // no-op unless it was one
                _session.releasePacketId(packetId);
            }

            // Two failures look identical here and must not be treated alike.
            //
            // If the packet is larger than our whole transmit buffer, it can
            // never be sent - retrying is a spin that concludes nothing and
            // holds the queue's head for ever. It is OUR limit rather than the
            // peer's, which is what PacketTooLarge means: raise bufferSize().
            if (needed > _link.transmitLimit()) {
                if (op->kind == MqttOpKind::Publish) _status.publishFailures++;
                _operations.concludeByOpId(op->opId, MqttResult::PacketTooLarge);
                continue;
            }

            // Otherwise the buffer simply has other bytes in it at this
            // instant. That clears on its own, so the operation keeps its
            // place and goes out on a later poll.
            break;
        }
        noteTransmitted();
        const uint32_t mark = _link.transmitMark();

        if (op->kind == MqttOpKind::Publish && op->qos > MQ_QOS_0) {
            _session.storeOutbound(packetId, op->qos, encoded, encodedLength);

            // The SESSION's copy is marked sent too, not just the operation's.
            //
            // Without this the replay path - which exists for reconnects - saw
            // a stored publish that had never been sent and offered it again
            // immediately, with the DUP flag set. The peer received every
            // QoS>0 message twice, the second marked as a redelivery. A live
            // broker's log showed it as "d1" on the very first exchange.
            MqttStoredPublish* stored = _session.findOutbound(packetId);
            if (stored) stored->sent = true;
        }

        _operations.markSent(*op, packetId, mark);

        if (op->kind == MqttOpKind::Publish) {
            if (op->qos == MQ_QOS_0) {
                // Nothing acknowledges a QoS 0 publish, so its conclusion is
                // reaching the wire. Reported here rather than left
                // outstanding for ever - which is the failure this whole
                // contract exists to prevent.
                _status.published++;
                _operations.concludeByOpId(op->opId, MqttResult::Ok);
            } else {
                _flow.takeOne();
            }
        }
    }
}

// =============================================================================
// FAILURE AND RECONNECTION
// =============================================================================

void MqttClient::failConnection(MqttResult reason, uint8_t disconnectReason) {
    _status.lastError = reason;
    _status.lastReasonCode = disconnectReason;

    // V5:4.13.1 - "In the case of an error in any other packet it SHOULD send
    // a DISCONNECT packet containing the reason code before closing the
    // Network Connection."
    //
    // Told rather than simply dropped, because a peer that is given the reason
    // can log it, and the alternative is an operator with two ends that both
    // say the other went away.
    if (_activeVersion == MqttVersion::V5 && _transport && _transport->isConnected()) {
        MqttPacket disconnect;
        disconnect.header.type = MQ_PKT_DISCONNECT;
        disconnect.disconnect.reasonCode = disconnectReason;

        // THROUGH THE LINK, like every other packet this client sends.
        //
        // This used to encode into a local buffer and write straight to the
        // transport, with a comment saying the transmit buffer "may hold a
        // half-sent packet" - which is exactly why it must not. When
        // _transmitCursor is part way through a packet, writing these bytes
        // to the socket splices them INTO it: the peer does not receive a
        // DISCONNECT, it receives a corrupt packet and reports a protocol
        // error, which is the opposite of the diagnosis this code exists to
        // provide. MQTT has no delimiter to resynchronise on, so there is no
        // recovering from it. The 8-attempt loop could also leave a truncated
        // DISCONNECT on the wire, with the same result.
        //
        // The link appends after whatever is in flight and its serviceTransmit
        // finishes both in order. If there is no room the DISCONNECT is simply
        // not sent - which is what the old code did too, having tried, but
        // without corrupting the stream on the way.
        _link.serviceTransmit();
        if (_link.send(g_codecV5, disconnect)) {
            _link.serviceTransmit();
        }
    }

    closeConnection(reason);
}

void MqttClient::closeConnection(MqttResult reason) {
    const bool wasUp = _status.state == MqttState::Connected;

    // A connection that stayed up long enough to have been working earns a
    // fresh backoff: the network failed, and the next attempt starts from
    // reconnectInitialMs. One that went down sooner keeps the backoff it had,
    // so a broker that accepts and immediately drops this device is retried
    // at a doubling interval rather than once a second. _stateSince is when
    // Connected was entered - enterState() sets it on every change.
    if (wasUp && (_stable || mq_elapsed(_stateSince, mq_millis()) >= (mq_time_t)MQ_RECONNECT_STABLE_MS)) {
        _reconnectDelay = _behaviour.reconnectInitialMs;
    }
    _stable = false;

    if (_transport) _transport->end();

    _link.reset();
    _keepAlive.stop();
    applyTransmitReservation();
    // Not session state, so neither survives. V5:4.9 and V5:3.3.2.3.4.
    _aliases.reset(0, 0);

    if (wasUp) _status.disconnects++;
    _status.lastError = reason;
    _status.connected = false;

    // What was SENT has moved into the session and will be retransmitted if
    // the session survives. What was not has nowhere to go, and is concluded
    // now rather than left outstanding for ever.
    _operations.concludeUnsent(MqttResult::ConnectionLost);

    // A session that cannot outlive the connection takes everything with it.
    const bool sessionSurvives =
        !_connection.cleanStart &&
        (_activeVersion == MqttVersion::V311 || _connection.hasSessionExpiry);

    if (!sessionSurvives) {
        _operations.concludeAll(MqttResult::SessionLost);
        _session.resetProtocolState();
    }

    // A CREDENTIAL THE BROKER HAS ALREADY REJECTED IS NOT RETRIED.
    //
    // Every other reason to reconnect is about the network, and the network
    // may come back. A password is not: it will not become correct by itself,
    // and sending it again every second is how a device gets itself banned -
    // by address, on some brokers, which takes the whole site with it.
    //
    // This is the same rule the version fallback already had, in the same
    // words, about the same packet: "retrying a rejected password at another
    // version is how a client gets itself banned". The reconnect path did not
    // have it, so with autoReconnect on - the default - a wrong password meant
    // forever. See MqttClientConfig::retryRejectedCredentials for when to turn
    // it back on, which is a decision rather than an oversight.
    // AT CONNECT TIME ONLY, and the `!wasUp` is what makes that precise.
    //
    // A CONNACK refusal means the credential was rejected: the connection
    // never came up. A 0x87 in a DISCONNECT halfway through a session means
    // something else - a publish to a topic this client may not have, most
    // likely - and that is not a reason to stop reconnecting, because the
    // connection itself was fine and the next one may be too.
    //
    // AND FROM THE CONNECT'S OWN ANSWER, not from lastReasonCode. That is the
    // last reason code from ANY packet and is never cleared, so a SUBACK of
    // 0x87 - a topic this client may not subscribe to, nothing to do with its
    // password - was still there when a later reconnect simply failed to reach
    // the broker, and the client stood down for good believing its credential
    // had been rejected. connectReasonCode is reset at the start of every
    // attempt and set only by a CONNACK, so a broker that never answered
    // cannot look like one that said no.
    const bool credentialRejected =
        !wasUp && !_behaviour.retryRejectedCredentials &&
        (_status.connectReasonCode == MQ_REASON_BAD_USER_NAME_OR_PASSWORD ||
         _status.connectReasonCode == MQ_REASON_NOT_AUTHORIZED ||
         (_activeVersion == MqttVersion::V311 &&
          (_status.connectReasonCode == MQ_RETURN_BAD_CREDENTIALS ||
           _status.connectReasonCode == MQ_RETURN_NOT_AUTHORIZED)));

    const bool restarting = _restarting;
    _restarting = false;

    if (restarting && !_stayDown) {
        // restart(): the owner wants to be connected and a layer wants a new
        // CONNECT - no backoff, whatever autoReconnect says. But not this
        // instant: the same client identifier back before the broker has
        // handled the goodbye is a session takeover, and a takeover may drop
        // the very Will the goodbye asked it to publish. Mosquitto did, one
        // run in three.
        _reconnectDelay = _behaviour.reconnectInitialMs;
        _reconnectAt = mq_millis() + MQ_RESTART_SETTLE_MS;
        enterState(MqttState::Reconnecting, reason);
    } else if (_behaviour.autoReconnect && !_stayDown &&
        reason != MqttResult::Cancelled && !credentialRejected) {
        scheduleReconnect();
        enterState(MqttState::Reconnecting, reason);
    } else if (_behaviour.autoReconnect && !_stayDown && credentialRejected &&
               _behaviour.rejectedCredentialsRetryMs != 0) {
        // Slowly, but always - see rejectedCredentialsRetryMs. The network
        // backoff is not advanced: the next NETWORK fault still starts from
        // where it was.
        _reconnectAt = mq_millis() + _behaviour.rejectedCredentialsRetryMs;
        enterState(MqttState::Reconnecting, reason);
    } else {
        enterState(MqttState::Idle, reason);
    }
}

void MqttClient::scheduleReconnect() {
    // Deterministic jitter, keyed on WHICH DEVICE this is and how many
    // reconnects it has already made.
    //
    // A plant where forty PLCs lose power together must not present the broker
    // with forty simultaneous connects - and then, having all backed off by
    // exactly the same amount, do it again. A device has no entropy at
    // start-up, so the spread is a hash rather than a random number - and the
    // hash must include something that differs between devices, or forty of
    // them compute the same delay. See deviceSeed().
    //
    // FULL JITTER: anywhere from MQ_RECONNECT_FLOOR_MS to the whole backoff,
    // uniformly. It used to be the backoff PLUS up to half of it again, and
    // that window is far too narrow for a fleet: a broker restart brought two
    // thousand devices back inside the same half second, every one of them a
    // TLS handshake and a session to restore. Drawing from [floor, base]
    // spreads the same fleet over the whole interval, and the doubling below
    // widens it on every failed round. The mean wait is shorter than it was -
    // half the backoff rather than one and a quarter - and the doubling is
    // what keeps a broker that stays away from being asked too often.
    const uint32_t seed = (_status.reconnects * 2654435761u + 1) ^
                          deviceSeed(_connection.clientId, this);
    const mq_time_t least = _reconnectDelay < (mq_time_t)MQ_RECONNECT_FLOOR_MS
                                ? _reconnectDelay : (mq_time_t)MQ_RECONNECT_FLOOR_MS;
    _reconnectAt = mq_millis() + least + spread(seed, _reconnectDelay - least + 1);

    _reconnectDelay *= 2;
    if (_reconnectDelay > _behaviour.reconnectMaximumMs) {
        _reconnectDelay = _behaviour.reconnectMaximumMs;
    }
}

void MqttClient::serviceReconnect() {
    if (mq_isFuture(mq_millis(), _reconnectAt)) return;

    // The held first connect is the first attempt, not a reconnect.
    const bool first = _firstConnectHeld;
    _firstConnectHeld = false;
    if (!first) _status.reconnects++;

    // A new attempt has had no answer yet. See MqttStatus::connectReasonCode
    // for why this is not lastReasonCode.
    _status.connectReasonCode = 0;
    _transport->connectWithin(_behaviour.connectTimeoutMs);
    if (!_transport->begin()) {
        scheduleReconnect();
        return;
    }
#if MQTT_LEAN
    // A first connect answers a failed open as connect() itself would have.
    _openFailure = first ? MqttResult::ConnectionLost : MqttResult::Ok;
#endif
    enterState(MqttState::Connecting);
}

// =============================================================================
// THE APPLICATION'S CALLS
// =============================================================================

MqttResult MqttClient::publish(const char* topic, const uint8_t* payload, size_t length,
                               uint8_t qos, bool retain,
                               MqttCompletionFn onComplete, void* context) {
    // Supersedable by default: a setpoint written every scan should send the
    // value it settled on rather than a history of everything it passed
    // through on the way.
    return publishInternal(topic, payload, length, qos, retain, true,
                           nullptr, onComplete, context);
}

MqttResult MqttClient::publish(const char* topic, const uint8_t* payload, size_t length,
                               uint8_t qos, bool retain,
                               const MqttOwnedProperties& properties,
                               MqttCompletionFn onComplete, void* context) {
    return publishInternal(topic, payload, length, qos, retain, true,
                           &properties, onComplete, context);
}

size_t MqttClient::withdraw(const char* topic) {
    if (topic == nullptr || *topic == '\0') return 0;
    const size_t length = strlen(topic);
    MQ_AREA_BYTES key(topic, topic + length);
    return _operations.withdraw(key, MqttResult::Superseded);
}

MqttResult MqttClient::publishSeries(const char* topic, const uint8_t* payload,
                                     size_t length, uint8_t qos, bool retain,
                                     const MqttOwnedProperties& properties,
                                     MqttCompletionFn onComplete, void* context) {
    return publishInternal(topic, payload, length, qos, retain, false,
                           &properties, onComplete, context);
}

MqttResult MqttClient::publishInternal(const char* topic, const uint8_t* payload,
                                       size_t length, uint8_t qos, bool retain,
                                       bool supersedable,
                                       const MqttOwnedProperties* properties,
                                       MqttCompletionFn onComplete, void* context) {
    if (!_transport) return MqttResult::NotInitialised;
    if (qos > MQ_QOS_2) return MqttResult::InvalidArgument;

    // No 65,535 limit here. A PUBLISH payload carries no length prefix - it is
    // whatever the Remaining Length leaves over - so what bounds it is the
    // packet itself, checked against the peer's Maximum Packet Size and our
    // own buffer when it comes to be sent. Refusing at 64 kB would report the
    // library's old internal width as the caller's mistake.
    if (length > MQ_VBI_MAX_VALUE) return MqttResult::InvalidArgument;

    // [V5:MQTT-4.7.0-1] a Topic Name carries no wildcards, and is at least one
    // character. Checked here rather than at the codec so a caller finds out
    // at the call that made the mistake.
    const MqttSpan name = mq_topicSpan(topic);
    if (!mq_topicNameIsValid(name)) return MqttResult::InvalidTopic;

    // Refused, never quietly downgraded. A publish that silently arrived at a
    // lower QoS than asked for is a delivery guarantee the application
    // believes it has and does not.
    if (!_capabilities.allowsQos(qos) && isConnected()) {
        return MqttResult::NotSupportedByPeer;
    }
    if (retain && !_capabilities.retainAvailable && isConnected()) {
        return MqttResult::NotSupportedByPeer;
    }

    if (properties && !properties->empty()) {
        // 3.1.1 HAS NO PROPERTIES AT ALL, so this cannot be sent stripped: a
        // publish that quietly lost its Correlation Data leaves the requester
        // waiting for an answer nothing can match to it any more.
        //
        // Refused here when the version is already settled. A publish queued
        // before connecting does not know yet which version it will go out on,
        // and serviceQueue() catches that case at the send.
        if (isConnected() && _activeVersion == MqttVersion::V311) {
            return MqttResult::NotSupportedByProtocol;
        }

        // [V5:3.3.2.3.5] the Response Topic is a Topic NAME, so the wildcard
        // rules apply to it exactly as they do to the topic being published
        // to. Checked at the call that made the mistake rather than at the
        // encode, where it would read as a library fault.
        if (properties->hasResponseTopic &&
            !mq_topicNameIsValid(mq_topicSpan(properties->responseTopic.c_str()))) {
            return MqttResult::InvalidTopic;
        }
    }

    MqttOperation op;
    op.kind = MqttOpKind::Publish;
    op.topic.assign(name.data, name.data + name.length);
    if (payload && length) op.payload.assign(payload, payload + length);
    op.qos = qos;
    op.retain = retain;
    op.supersedable = supersedable;
    if (properties) op.properties.set(*properties);
    op.onComplete = onComplete;
    op.context = context;
    return _operations.add(op);
}

MqttResult MqttClient::publish(const char* topic, const char* payload,
                               uint8_t qos, bool retain,
                               MqttCompletionFn onComplete, void* context) {
    const size_t length = payload ? strlen(payload) : 0;
    return publish(topic, (const uint8_t*)payload, length, qos, retain,
                   onComplete, context);
}

MqttResult MqttClient::publishSeries(const char* topic, const uint8_t* payload,
                                      size_t length, uint8_t qos, bool retain,
                                      MqttCompletionFn onComplete, void* context) {
    // Every message survives. For any stream that carries its own sequence
    // numbers - Sparkplug's seq field, a command log, anything a receiver
    // reconstructs order from - eliding a message leaves a gap the far end
    // reads as loss.
    //
    // Decided here rather than patched in after queueing, because an operation
    // marked unsupersedable a moment too late can already have been elided by
    // the publish that followed it.
    return publishInternal(topic, payload, length, qos, retain, false,
                           nullptr, onComplete, context);
}

MqttResult MqttClient::subscribe(const char* filter, uint8_t qos,
                                 MqttCompletionFn onComplete, void* context) {
    return subscribeWithOptions(filter, qos, onComplete, context);
}

MqttResult MqttClient::subscribeWithOptions(const char* filter, uint8_t options,
                                            MqttCompletionFn onComplete, void* context) {
    if (!_transport) return MqttResult::NotInitialised;

    const MqttSpan span = mq_topicSpan(filter);
    if (!mq_topicFilterIsValid(span)) return MqttResult::InvalidTopic;

    if ((options & MQ_SUBOPT_QOS_MASK) > MQ_QOS_2) return MqttResult::InvalidArgument;

    if (isConnected()) {
        if (!_capabilities.wildcardSubscriptionAvailable) {
            for (uint16_t i = 0; i < span.length; i++) {
                if (span.data[i] == (uint8_t)MQ_TOPIC_MULTI_WILDCARD ||
                    span.data[i] == (uint8_t)MQ_TOPIC_SINGLE_WILDCARD) {
                    return MqttResult::NotSupportedByPeer;
                }
            }
        }
        if (mq_isSharedFilter(span) && !_capabilities.sharedSubscriptionAvailable) {
            return MqttResult::NotSupportedByPeer;
        }
    }

    // RECORDED FIRST, whether or not there is a connection. A subscription is
    // a standing intent the library owns: a caller that has to watch for the
    // connection itself is one that has to know the protocol's own rules, and
    // only a caller polling every scan would get the behaviour at all.
    if (!_session.addSubscription(span.data, span.length, options,
                                  onComplete, context)) {
        return MqttResult::QueueFull;
    }

    if (!isConnected()) {
        // It will go out when the session is established. The caller is told
        // it was accepted, and the handler fires when it is acknowledged.
        return MqttResult::Queued;
    }

    MqttOperation op;
    op.kind = MqttOpKind::Subscribe;
    op.topic.assign(span.data, span.data + span.length);
    op.subscriptionOptions = options;
    op.supersedable = false;
    op.onComplete = onComplete;
    op.context = context;
    const MqttResult queued = _operations.add(op);
    if (mq_accepted(queued)) _session.markSubscriptionIssued(span.data, span.length);
    return queued;
}

MqttResult MqttClient::unsubscribe(const char* filter,
                                   MqttCompletionFn onComplete, void* context) {
    if (!_transport) return MqttResult::NotInitialised;

    const MqttSpan span = mq_topicSpan(filter);
    if (!mq_topicFilterIsValid(span)) return MqttResult::InvalidTopic;

    if (!isConnected()) {
        // Nothing to tell the peer. The standing intent simply goes.
        _session.removeSubscription(span.data, span.length);
        return MqttResult::Ok;
    }

    MqttOperation op;
    op.kind = MqttOpKind::Unsubscribe;
    op.topic.assign(span.data, span.data + span.length);
    op.supersedable = false;
    op.onComplete = onComplete;
    op.context = context;
    return _operations.add(op);
}

// DISCONNECT WITH WILL MESSAGE AT 3.1.1, which has no such reason code: the
// Will goes as an ordinary PUBLISH, written ahead of the DISCONNECT on the
// same link, so the broker has it before it learns the goodbye was deliberate
// - the same order [tck-id-payloads-ndeath-will-message-publisher-disconnect-
// mqtt311] asks of a Sparkplug node. QoS 0: nothing may follow the DISCONNECT,
// an acknowledgement included. At 5.0 the reason code does it and this is
// nothing.
void MqttClient::sendWillAt311(uint8_t reasonCode) {
    if (reasonCode != MQ_REASON_DISCONNECT_WITH_WILL ||
        _activeVersion != MqttVersion::V311 || !_connection.will.enabled) {
        return;
    }
    MqttPacket will;
    will.header.type = MQ_PKT_PUBLISH;
    will.publish.qos = MQ_QOS_0;
    will.publish.retain = _connection.will.retain;
    will.publish.topic = spanOf(_connection.will.topic);
    will.publish.payload = spanOf(_connection.will.payload);
    sendPacket(will);
}

MqttResult MqttClient::restart(uint8_t reasonCode) {
    if (!_transport) return MqttResult::NotInitialised;
    if (_stayDown || _status.state == MqttState::Idle) return MqttResult::NotConnected;

    if (_status.state == MqttState::Reconnecting) {
        _reconnectAt = mq_millis();
        return MqttResult::Ok;
    }
    _restarting = true;
    if (_status.state == MqttState::Disconnecting) return MqttResult::Queued;
    if (!isConnected()) {
        // Connecting or mid-handshake: nothing to say goodbye on.
        closeConnection(MqttResult::Cancelled);
        return MqttResult::Ok;
    }

    sendWillAt311(reasonCode);
    MqttPacket packet;
    packet.header.type = MQ_PKT_DISCONNECT;
    packet.disconnect.reasonCode = reasonCode;
    if (_activeVersion == MqttVersion::V311 &&
        reasonCode != MQ_REASON_NORMAL_DISCONNECTION) {
        packet.disconnect.reasonCode = MQ_REASON_NORMAL_DISCONNECTION;
    }
    if (!sendPacket(packet)) {
        closeConnection(MqttResult::Cancelled);
        return MqttResult::Ok;
    }
    enterState(MqttState::Disconnecting);
    return MqttResult::Queued;
}

// =============================================================================
// LAYERS
// =============================================================================

MqttClientHook::~MqttClientHook() = default;
void MqttClientHook::prepareConnect(MqttConnectionConfig& connection) { (void)connection; }
void MqttClientHook::sessionEstablished(bool sessionPresent) { (void)sessionPresent; }
void MqttClientHook::stateChanged(MqttState state, MqttResult reason) {
    (void)state;
    (void)reason;
}
bool MqttClientHook::message(const MqttMessage& message) {
    (void)message;
    return false;
}
bool MqttClient::attach(MqttClientHook& hook) {
    if (hook._attachedTo == this) return true;
    if (hook._attachedTo != nullptr) return false;
    MqttClientHook** tail = &_hooks;
    while (*tail != nullptr) tail = &(*tail)->_nextHook;
    hook._nextHook = nullptr;
    hook._attachedTo = this;
    *tail = &hook;
    return true;
}

void MqttClient::detach(MqttClientHook& hook) {
    if (hook._attachedTo != this) return;
    releaseWill(hook);
    for (MqttClientHook** link = &_hooks; *link != nullptr; link = &(*link)->_nextHook) {
        if (*link == &hook) {
            *link = hook._nextHook;
            break;
        }
    }
    hook._nextHook = nullptr;
    hook._attachedTo = nullptr;
}

bool MqttClient::claimWill(MqttClientHook& hook) {
    if (hook._attachedTo != this) return false;
    if (_willOwner != nullptr && _willOwner != &hook) return false;
    _willOwner = &hook;
    return true;
}

void MqttClient::releaseWill(MqttClientHook& hook) {
    if (_willOwner != &hook) return;
    _willOwner = nullptr;
    _connection.will = MqttWillConfig();
}

bool MqttClient::reserveTransmit(size_t bytes) {
    if (bytes > _transmitWanted) _transmitWanted = bytes;
    if (_status.state == MqttState::Idle || _status.state == MqttState::Reconnecting) {
        applyTransmitReservation();
    }
    return _link.transmitLimit() >= bytes;
}

void MqttClient::applyTransmitReservation() {
    // Only on a link that is down: nothing is half-written into the buffer
    // being replaced.
    if (_transport == nullptr || _transmitWanted <= _behaviour.transmitBufferSize) return;
    _behaviour.transmitBufferSize = _transmitWanted;
    _link.begin(*_transport, _behaviour.receiveBufferSize, _behaviour.transmitBufferSize,
                _behaviour.initialBufferSize);
#if MQTT_LEAN
    sizeArea();
#endif
}

MqttResult MqttClient::disconnect(uint8_t reasonCode) {
    if (!_transport) return MqttResult::NotInitialised;
    if (!isConnected()) {
        closeConnection(MqttResult::Cancelled);
        return MqttResult::Ok;
    }

    sendWillAt311(reasonCode);
    MqttPacket packet;
    packet.header.type = MQ_PKT_DISCONNECT;
    packet.disconnect.reasonCode = reasonCode;

    // At 3.1.1 a DISCONNECT carries no reason at all, so a caller asking for
    // one is told rather than having it dropped.
    if (_activeVersion == MqttVersion::V311 &&
        reasonCode != MQ_REASON_NORMAL_DISCONNECTION) {
        packet.disconnect.reasonCode = MQ_REASON_NORMAL_DISCONNECTION;
    }

    if (!sendPacket(packet)) {
        closeConnection(MqttResult::Cancelled);
        return MqttResult::Ok;
    }

    // Auto-reconnect is off for a deliberate disconnect: the application said
    // to stop, and reconnecting would be the library overruling it.
    // Down until connect() - see _stayDown. NOT autoReconnect = false, which
    // used to be here and outlived the disconnect it was meant for.
    _stayDown = true;
    enterState(MqttState::Disconnecting);
    return MqttResult::Queued;
}

// =============================================================================
// HANDLES
// =============================================================================
//
// The same machinery reached by a different name, for a caller that cannot
// hold strings. Nothing here is a second implementation: publishTo() builds
// the same operation publish() does.

// =============================================================================
// HANDLES
// =============================================================================
//
// Four thin calls over MqttTopicRegistry. The registry holds the table and the
// counters; these are the parts that need the client, because publishing and
// subscribing are the client's job.

MqttTopicHandle MqttClient::registerTopic(const char* topic) {
    return _topics.add(topic);
}

const char* MqttClient::topicOf(MqttTopicHandle handle) const {
    return _topics.topicOf(handle);
}

const MqttTopicState& MqttClient::topicState(MqttTopicHandle handle) const {
    return _topics.stateOf(handle);
}

bool MqttClient::takeMessage(MqttTopicHandle handle, uint8_t* buffer,
                             size_t capacity, size_t& length) {
    return _topics.takeMessage(handle, buffer, capacity, length);
}

MqttResult MqttClient::publishTo(MqttTopicHandle handle, const uint8_t* payload,
                                 size_t length, uint8_t qos, bool retain) {
    const char* topic = _topics.topicOf(handle);
    if (topic == nullptr) return MqttResult::InvalidArgument;

    // The completion context is the handle's own state block, so the outcome
    // lands where a polled reader will look for it - with no allocation, no
    // capture, and nothing a generated wrapper would have to express.
    return publishInternal(topic, payload, length, qos, retain, true, nullptr,
                           &MqttTopicRegistry::noteOutcome,
                           _topics.mutableStateOf(handle));
}

MqttResult MqttClient::subscribeTo(MqttTopicHandle handle, uint8_t qos) {
    const char* topic = _topics.topicOf(handle);
    if (topic == nullptr) return MqttResult::InvalidArgument;
    return subscribeWithOptions(topic, qos, &MqttTopicRegistry::noteOutcome,
                                _topics.mutableStateOf(handle));
}

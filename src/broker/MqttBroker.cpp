// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttBroker.h"


#include "../codec/MqttCodecV5.h"
#include "../codec/MqttCodecV311.h"

#include <string.h>
#include <stdio.h>

namespace {

const MqttCodecV5 g_brokerCodecV5;
const MqttCodecV311 g_brokerCodecV311;

// How many packets one connection may have processed in a single poll before
// the broker moves on to the next. See serviceConnection.
const size_t kPacketsPerPoll = 8;

// How many waiting connections one poll may accept, per listener. See
// acceptNew(): one at a time made a connection storm O(N squared), a drain
// would starve the devices already connected, and a bound is neither.
const size_t kAcceptsPerPoll = 16;

// Where a connection's buffers start when slots are not reserved up front:
// enough for a CONNECT, the acknowledgements and most telemetry, which is
// nearly everything most connections ever carry.
const size_t kLinkInitialBytes = 512;

#if defined(MQTT_PLATFORM_HOST)
// The readiness token a listener is registered under. A connection's token is
// its slot and generation, and no slot is 0xFFFFFFFF, so this cannot collide.
const uint64_t kListenerToken = 0xFFFFFFFFFFFFFFFFull;

// How often, at most, the event-driven broker revisits an idle connection to
// check its keepalive and connect deadlines. Those are measured in seconds,
// so a quarter of a second of lateness is not one a client can observe.
const mq_time_t kDeadlineSweepMs = 250;
#endif

MqttSpan asSpan(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

MqttSpan asSpan(const std::vector<uint8_t>& bytes) {
    MqttSpan span;
    span.data = bytes.empty() ? nullptr : bytes.data();
    span.length = (uint32_t)bytes.size();
    return span;
}

// Puts a stored property set back on the wire.
//
// The reverse of MqttOwnedProperties::copyForForwarding, and the spans point
// into the STORED strings - which live in the session for as long as the
// delivery does, and therefore outlive the encode. That is the whole reason
// the stored type exists.
void restoreProperties(const MqttOwnedProperties& from, MqttProperties& into) {
    if (from.hasPayloadFormat) {
        into.payloadFormatIndicator = from.payloadFormat;
        into.markPresent(MQ_PROP_PAYLOAD_FORMAT_INDICATOR);
    }
    if (from.hasContentType) {
        into.contentType = asSpan(from.contentType);
        into.markPresent(MQ_PROP_CONTENT_TYPE);
    }
    if (from.hasResponseTopic) {
        into.responseTopic = asSpan(from.responseTopic);
        into.markPresent(MQ_PROP_RESPONSE_TOPIC);
    }
    if (from.hasCorrelationData) {
        into.correlationData = asSpan(from.correlationData);
        into.markPresent(MQ_PROP_CORRELATION_DATA);
    }
    for (const MqttOwnedProperties::UserProperty& entry : from.user) {
        into.addUserProperty(asSpan(entry.key), asSpan(entry.value));
    }
}

} // namespace

void MqttBrokerConnection::recycle() {
    link.end();              // keeps the buffers, forgets the transport
    transport = nullptr;
    origin = nullptr;
    session = nullptr;
    connectSeen = false;
    closing = false;
    cleanStart = true;
    sessionExpiryRevised = false;
    version = MqttVersion::V5;
    toPeer = MqttFlow();
    inFlightFromPeer = 0;
    aliases.reset(0, 0);
    keepAliveSeconds = 0;
    keepAliveStated = false;
    lastPacketAt = 0;
    openedAt = 0;
    peerReceiveMaximum = 65535;
    peerMaximumPacketSize = 0;
    peerTopicAliasMaximum = 0;
    peerWantsProblemInformation = true;
#if defined(MQTT_PLATFORM_HOST)
    // A new occupant, so a new generation: any readiness event still in
    // flight for the old one no longer matches. `queued` is NOT reset - it
    // records that the SLOT is on the work list, which outlives the occupant,
    // and resetting it would let the slot be listed twice.
    generation++;
    watchedHandle = -1;
    watchingWrite = false;
#endif
}

MqttBroker::~MqttBroker() { end(); }

const MqttCodec& MqttBroker::codecFor(MqttVersion version) const {
    return version == MqttVersion::V5 ? (const MqttCodec&)g_brokerCodecV5
                                      : (const MqttCodec&)g_brokerCodecV311;
}

// =============================================================================
// LIFETIME
// =============================================================================

void MqttBroker::begin(const MqttBrokerConfig& config) {
    _config = config;

    if (_config.maxConnections == 0) _config.maxConnections = 1;
    if (_config.maxSessions < _config.maxConnections) {
        _config.maxSessions = _config.maxConnections;
    }
    // The routing index holds session indices in 32 bits. More sessions than
    // that cannot be addressed, so the count is clamped - and config() reports
    // the clamped value, as it does every other limit adjusted here - rather
    // than letting an index wrap and deliver to the wrong session.
    if ((uint64_t)_config.maxSessions > 0xFFFFFFFEull) {
        _config.maxSessions = (size_t)0xFFFFFFFEull;
    }
    if (_config.maxConnections > _config.maxSessions) {
        _config.maxConnections = _config.maxSessions;
    }

    // ADVERTISE ONLY WHAT WE CAN HOLD. A Maximum Packet Size larger than the
    // receive buffer would be a promise we break at the worst moment, which is
    // NanoMQ's D-1 with the numbers the other way round.
    if (_config.maximumPacketSize == 0 ||
        _config.maximumPacketSize > _config.receiveBufferSize) {
        _config.maximumPacketSize = (uint32_t)_config.receiveBufferSize;
    }

    if (_config.maximumQos > MQ_QOS_2) _config.maximumQos = MQ_QOS_2;

    // The routing index, sized once here like everything else. A power of two
    // at least twice the session count keeps the buckets shallow; the whole
    // structure is four small arrays, so a board configured for eight sessions
    // pays a few hundred bytes and a gateway for a thousand pays a few kB.
    size_t buckets = 16;
    while (buckets < _config.maxSessions * 2) buckets *= 2;
    _bucketStart.assign(buckets + 1, 0);
    _bucketSessions.clear();
    _wildcardSessions.clear();
    _routeMark.assign(_config.maxSessions, 0);
    _routeGeneration = 0;
    _subscriptionRevision = 0;
    _indexBuiltAt = 0xFFFFFFFFu;   // stale until the first route()


    // ONE MORE THAN CONFIGURED. The extra slot never serves; it is where a
    // connection arriving at a full broker is told 0x89 Server Busy instead of
    // being left to time out. See MqttBrokerConnection::refuseOnly.
    _connections.clear();
    _connections.resize(_config.maxConnections + 1);
    for (MqttBrokerConnection& connection : _connections) {
        connection = MqttBrokerConnection();
    }
#if defined(MQTT_PLATFORM_HOST)
    _freeConnections.clear();
    _freeConnections.reserve(_config.maxConnections);
    for (size_t index = _config.maxConnections; index > 0; index--) {
        _freeConnections.push_back((uint32_t)(index - 1));
    }
#endif

    _sessions.clear();
    _sessions.resize(_config.maxSessions);
#if defined(MQTT_PLATFORM_HOST)
    _sessionIndex.configure(_sessions.size(), _sessions.data(), &MqttBroker::identifierOf);
    _routeIndex.configure(buckets,
                          _config.reserveUpFront
                              ? _config.maxSessions * _config.maxSubscriptionsPerSession : 0);
    _routeScratch.clear();
    _sharePrunedAt = 0;
    _sharePrunedRevision = 0;
#endif
    for (MqttBrokerSession& session : _sessions) {
        session.configure(_config.maxSubscriptionsPerSession,
                          _config.maxQueuedPerSession, _config.reserveUpFront);
        session.attachSubscriptionRevision(&_subscriptionRevision);
#if defined(MQTT_PLATFORM_HOST)
        session.attachIndex(&_sessionIndex, (uint32_t)(&session - _sessions.data()));
        session.attachRoutes(&_routeIndex);
#endif
    }

    _retained.configure(_config.maxRetained);

    _status = MqttBrokerStatus();
#if defined(MQTT_PLATFORM_HOST)
    _work.clear();
    _working.clear();
    _liveConnections = 0;
    _timerCursor = 0;
    _timerSweptAt = 0;
    _wheel.reset(mq_millis());      // the sessions were just rebuilt, unarmed
    _housekeptAt = 0;
#endif
}

void MqttBroker::end() {
    for (MqttBrokerConnection& connection : _connections) {
        if (!connection.inUse()) continue;
        // [V5:3.14] 0x8B Server shutting down. A broker that simply dropped
        // its sockets would leave every client to find out by keepalive
        // timeout, one interval later - which on a plant is a minute of every
        // control loop talking to nothing.
        closeConnection(connection, MQ_REASON_SERVER_SHUTTING_DOWN, true, false);
    }

    // THE FAREWELLS HAVE TO GET OUT.
    //
    // closeConnection begins a lingering close; without this the process would
    // exit with the DISCONNECTs still in the transmit queue, which is exactly
    // the outcome the linger exists to prevent. Bounded, and it does not wait
    // for the peer - the peer may already be gone, and a shutdown that could
    // be held up by one unresponsive client is not a shutdown.
    for (size_t pass = 0; pass < 64; pass++) {
        bool anyLeft = false;
        for (MqttBrokerConnection& connection : _connections) {
            if (!connection.inUse()) continue;
            finishClosing(connection, mq_millis());
            if (connection.inUse()) anyLeft = true;
        }
        if (!anyLeft) break;
        mq_yield();
    }

    // Anything that still has not gone is released anyway. A client that
    // stopped reading does not get to keep a socket open through our shutdown.
    for (MqttBrokerConnection& connection : _connections) {
        if (!connection.inUse()) continue;
#if defined(MQTT_PLATFORM_HOST)
        unwatch(connection);     // before the descriptor closes, always
        releaseConnectionSlot(connection);
#endif
        if (connection.origin) connection.origin->release(connection.transport);
        else if (connection.transport) connection.transport->end();
        connection.recycle();
    }

    for (MqttBrokerSession& session : _sessions) session.release();
    _listeners.clear();
#if defined(MQTT_PLATFORM_HOST)
    _work.clear();
    _liveConnections = 0;
#endif
}

bool MqttBroker::addListener(MqttListener& listener) {
    if (_connections.empty()) begin(_config);
    // ONCE. A listener added twice is polled twice per pass and, worse, is
    // still in the list after whoever added it believes they replaced it -
    // which is how a restart path that re-added its listener ended up with
    // two entries for one socket.
    for (MqttListener* existing : _listeners) {
        if (existing == &listener) return true;
    }
    _listeners.push_back(&listener);
#if defined(MQTT_PLATFORM_HOST)
    if (_eventDriven && _readiness != nullptr) {
        _readiness->add(listener.waitHandle(), kListenerToken, false);
    }
#endif
    return true;
}

void MqttBroker::onAuthorise(MqttBrokerAuthFn handler, void* context) {
    _onAuthorise = handler;
    _authoriseContext = context;
}

void MqttBroker::onMessage(MqttBrokerTapFn handler, void* context) {
    _onTap = handler;
    _tapContext = context;
}

void MqttBroker::onConnection(MqttBrokerConnectionFn handler, void* context) {
    _onConnection = handler;
    _connectionContext = context;
}

// =============================================================================
// THE POLL
// =============================================================================

#if defined(MQTT_PLATFORM_HOST)

void MqttBroker::useReadiness(MqttPollSet& set) {
    _readiness = &set;
    _eventDriven = set.persistent();
    if (!_eventDriven) return;

    // Persistent membership: everything is registered once, here or as it
    // arrives, and removed as it leaves.
    for (MqttListener* listener : _listeners) {
        if (listener != nullptr) set.add(listener->waitHandle(), kListenerToken, false);
    }
    for (MqttBrokerConnection& connection : _connections) {
        if (!connection.inUse()) continue;
        watch(connection);
        connection.readable = true;
        queueWork(connection);
    }
    // Deadlines that were being found by walking are handed to the wheel.
    const mq_time_t now = mq_millis();
    _wheel.reset(now);
    for (MqttBrokerSession& session : _sessions) {
        if (!session.inUse() || session.connected) continue;
        if (session.will.scheduled) armWill(session, now);
        armExpiry(session, now);
    }
}

void MqttBroker::refreshReadiness() {
    if (_readiness == nullptr || !_readiness->isOpen()) return;
    if (_eventDriven) return;     // membership is kept, not rebuilt

    _readiness->clear();

    // The listeners, so an accept() that would find nothing is not made.
    for (MqttListener* listener : _listeners) {
        if (listener != nullptr && listener->isListening()) {
            _readiness->watch(listener->waitHandle(), false);
        }
    }

    // Every live connection. Writing is asked about only where something is
    // actually waiting to go out, because a socket is almost always writable
    // and watching for it otherwise would report every connection ready every
    // time - which is the state this exists to get out of.
    for (MqttBrokerConnection& connection : _connections) {
        if (!connection.inUse() || connection.transport == nullptr) continue;
        _readiness->watch(connection.transport->waitHandle(),
                          connection.link.transmitPending() != 0);
    }
}

// Whether this connection's socket is worth touching this pass.
//
// Three ways to be worth touching, and the last two are why a readiness set is
// not simply "skip what the kernel did not name":
//
//   the kernel says the socket has something,
//   the TRANSPORT is already holding bytes it has not been asked for - which
//     includes decrypted plaintext sitting inside an SSL object with nothing
//     left on the wire, and
//   the LINK is holding a part-assembled packet, so there may be a whole one
//     in there already without another byte arriving.
//
// Miss any of the three and a connection stalls with its data in the building.
bool MqttBroker::shouldRead(const MqttBrokerConnection& connection) const {
    if (_readiness == nullptr || !_readiness->isOpen()) return true;
    if (connection.transport == nullptr) return true;
    if (connection.transport->hasBufferedInput()) return true;
    if (connection.link.buffered() != 0) return true;
    if (_eventDriven) {
        // Nothing to watch means nothing will ever say it is ready: read.
        return connection.watchedHandle < 0 || connection.readable;
    }
    return _readiness->isReady(connection.transport->waitHandle());
}

// =============================================================================
// EVENT-DRIVEN SERVICING - persistent readiness only
// =============================================================================
//
// The sweep above visits every connection on every poll. At a plant's scale
// that is nothing; at twenty thousand connections it was 81 ms per poll, and
// the broker stopped accepting new connections because it could not get round
// to them (internal/STATE.md, 2026-10-04). Here a poll does work in proportion
// to the connections that HAVE work, and nothing in proportion to the ones
// that are merely connected.
//
// WHAT PUTS A CONNECTION ON THE WORK LIST, and what would go wrong without
// each, because a connection that is never listed is a connection that has
// silently stopped:
//
//   the kernel reports its socket ready          - it sent something
//   it was just accepted                         - its CONNECT may already be
//                                                  waiting
//   a message was queued for its session         - nothing on the socket will
//                                                  say so (wake())
//   it is closing                                - the linger has a deadline
//   it still holds buffered input, or has more   - the per-poll bounds left
//     to deliver with room to send it              work behind
//   its transport has no descriptor              - nothing can ever report it
//                                                  ready, so it is listed
//                                                  every poll, as before
//
// Output that is waiting for socket space is NOT relisted: write interest is
// registered instead, and the kernel says when there is room. Relisting would
// retry a full socket every poll.

void MqttBroker::queueWork(MqttBrokerConnection& connection) {
    if (!_eventDriven || connection.queued) return;
    connection.queued = true;
    _work.push_back(slotOf(connection));
}

void MqttBroker::wake(const MqttBrokerSession& session) {
    if (!_eventDriven || !session.connected) return;
    if (session.connectionSlot >= _connections.size()) return;
    MqttBrokerConnection& connection = _connections[session.connectionSlot];
    if (connection.inUse() && connection.session == &session) queueWork(connection);
}

void MqttBroker::watch(MqttBrokerConnection& connection) {
    connection.watchedHandle = -1;
    connection.watchingWrite = false;
    if (_readiness == nullptr || connection.transport == nullptr) return;
    const int handle = connection.transport->waitHandle();
    if (handle < 0) return;
    const uint64_t token =
        (uint64_t)slotOf(connection) | ((uint64_t)connection.generation << 32);
    // A registration that fails leaves the connection unwatched, which means
    // serviced every poll - slower, never silent.
    if (_readiness->add(handle, token, false)) connection.watchedHandle = handle;
}

void MqttBroker::unwatch(MqttBrokerConnection& connection) {
    if (connection.watchedHandle >= 0 && _readiness != nullptr) {
        _readiness->remove(connection.watchedHandle);
    }
    connection.watchedHandle = -1;
    connection.watchingWrite = false;
}

void MqttBroker::afterService(MqttBrokerConnection& connection) {
    connection.readable = false;
    if (!connection.inUse()) return;

    bool again = connection.closing || connection.watchedHandle < 0 ||
                 connection.link.buffered() != 0 ||
                 connection.transport->hasBufferedInput();

    const bool blocked = connection.link.transmitPending() != 0;
    if (!again && !blocked && connection.session != nullptr &&
        connection.session->nextUnsent(connection.toPeer.canSendQos12()) != nullptr) {
        again = true;    // deliverQueued stopped at its bound with room to go on
    }
    if (!again && !blocked && connection.session != nullptr &&
        connection.session->retainedOwed() != 0 && !connection.session->queueFull()) {
        again = true;    // retained messages still owed, and room to queue them
    }
    if (again) queueWork(connection);

    if (connection.watchedHandle >= 0 && blocked != connection.watchingWrite) {
        const uint64_t token =
            (uint64_t)slotOf(connection) | ((uint64_t)connection.generation << 32);
        if (_readiness->update(connection.watchedHandle, token, blocked)) {
            connection.watchingWrite = blocked;
        }
    }
}

// Keepalive and connect deadlines need no I/O, so they are checked by a
// rolling sweep that visits every slot about once per kDeadlineSweepMs -
// a quarter of a second, against deadlines measured in seconds. It replaces
// visiting every connection on every poll for the sake of two comparisons.
void MqttBroker::sweepDeadlines(mq_time_t now) {
    const size_t slots = _connections.size();
    if (slots == 0) return;
    if (_timerSweptAt == 0) { _timerSweptAt = now; return; }
    const mq_time_t elapsed = mq_elapsed(_timerSweptAt, now);
    uint64_t visits = (uint64_t)slots * elapsed / kDeadlineSweepMs;
    if (visits == 0) return;            // the fraction accumulates
    if (visits > slots) visits = slots;
    _timerSweptAt = now;

    for (uint64_t step = 0; step < visits; step++) {
        MqttBrokerConnection& connection = _connections[_timerCursor];
        _timerCursor = (_timerCursor + 1) % slots;
        // A listed connection is about to be serviced, which checks its
        // deadlines anyway.
        if (!connection.inUse() || connection.closing || connection.queued) continue;
        if (checkDeadlines(connection, now) && connection.inUse()) queueWork(connection);
    }
}

void MqttBroker::pollEvents(mq_time_t now) {
    // Ask the kernel - unless the daemon waiting on this set just did.
    if (!_readiness->takeFresh()) _readiness->wait(0);
    _readiness->takeFresh();
    const size_t slots = _connections.size();
    for (size_t index = 0; index < _readiness->readyCount(); index++) {
        const uint64_t token = _readiness->readyToken(index);
        if (token == kListenerToken) continue;   // acceptNew() looks every poll
        const uint32_t slot = (uint32_t)token;
        if (slot >= slots) continue;
        MqttBrokerConnection& connection = _connections[slot];
        // THE GENERATION CHECK. A token for a slot's previous occupant is
        // dropped here rather than serviced as the connection now in it.
        if (!connection.inUse() || connection.generation != (uint32_t)(token >> 32)) continue;
        connection.readable = true;
        queueWork(connection);
    }

    acceptNew();
    sweepDeadlines(now);

    // The pass in progress is separated from the next one, so whatever is
    // listed while servicing - by a delivery, a takeover, a close - waits
    // for the next pass instead of being serviced twice in this one.
    _working.swap(_work);
    for (uint32_t slot : _working) _connections[slot].queued = false;

    // ROTATED, for the same reason the sweep is (see _sweepStart). Whatever is
    // relisted goes back in the order it was serviced, so without this the
    // same connection would lead every pass and its acknowledgements would
    // always be first - test_broker_rules "the connection sweep rotates"
    // found exactly that the first time this ran.
    const size_t count = _working.size();
    const size_t first = count > 0 ? (_workRotation++ % count) : 0;
    for (size_t step = 0; step < count; step++) {
        const uint32_t slot = _working[(first + step) % count];
        MqttBrokerConnection& connection = _connections[slot];
        if (!connection.inUse()) continue;
        if (connection.closing) finishClosing(connection, now);
        else serviceConnection(connection);
        afterService(connection);
    }
    _working.clear();

    // Session deadlines from the wheel: only the due ones are visited.
    _wheel.advance(now, &MqttBroker::onTimer, this);

    // The retained store's message expiry, and the session count, once a
    // second rather than every poll. Both are whole-second quantities, and
    // the walks are what made an idle broker of 140,000 sessions cost 4 ms
    // a poll.
    if (_housekeptAt == 0 || mq_elapsed(_housekeptAt, now) >= 1000) {
        _housekeptAt = now;
        size_t live = 0;
        for (const MqttBrokerSession& session : _sessions) {
            if (session.inUse()) live++;
        }
        _status.sessions = live;
        _retained.sweep(now, &_status.retained);
    }
    _status.connections = _liveConnections;
}

// Remaining time, in milliseconds, of an interval given in SECONDS that began
// at `since` - computed in 64 bits, because a session expiry of a week is more
// milliseconds than a 32-bit clock can hold.
static uint64_t remainingMs(uint32_t intervalSeconds, mq_time_t since, mq_time_t now) {
    const uint64_t total = (uint64_t)intervalSeconds * 1000u;
    const uint64_t elapsed = mq_elapsed(since, now);
    return elapsed >= total ? 0 : total - elapsed;
}

void MqttBroker::armWill(MqttBrokerSession& session, mq_time_t now) {
    session.willTimer.owner = (uint32_t)(&session - _sessions.data());
    session.willTimer.kind = kWillTimer;
    uint64_t wait = remainingMs(session.will.delayInterval, session.will.scheduledAt, now);
    if (wait > MqttTimerWheel::kMaxAheadMs) wait = MqttTimerWheel::kMaxAheadMs;
    _wheel.arm(session.willTimer, (mq_time_t)(now + (mq_time_t)wait));
}

void MqttBroker::armExpiry(MqttBrokerSession& session, mq_time_t now) {
    // 0 ended with the connection; 0xFFFFFFFF never ends [V5:3.1.2.11.2].
    if (session.expiryInterval == 0 || session.expiryInterval == 0xFFFFFFFFu) return;
    session.expiryTimer.owner = (uint32_t)(&session - _sessions.data());
    session.expiryTimer.kind = kExpiryTimer;
    uint64_t wait = remainingMs(session.expiryInterval, session.offlineSince, now);
    if (wait > MqttTimerWheel::kMaxAheadMs) wait = MqttTimerWheel::kMaxAheadMs;
    _wheel.arm(session.expiryTimer, (mq_time_t)(now + (mq_time_t)wait));
}

// A timer firing is a reason to LOOK, never proof that the deadline has come.
// The session may have reconnected, been released and reused by another
// client, or had its interval capped to what the wheel can hold - so each
// case is decided again from the session's own state, by the same rules the
// walking broker uses, and a deadline still ahead is simply re-armed.
// =============================================================================
// ONE BROKER PER CORE - the seams a multi-core server uses
// =============================================================================

void MqttBroker::onEgress(MqttBrokerEgressFn handler, void* context) {
    _onEgress = handler;
    _egressContext = context;
}

void MqttBroker::onRouteKey(MqttRouteIndex::KeyObserver observer, void* context) {
    _routeIndex.observe(observer, context);
}

void MqttBroker::onShareMember(MqttRouteIndex::ShareObserver observer, void* context) {
    _routeIndex.observeShares(observer, context);
}

void MqttBroker::onShareGate(MqttBrokerShareGateFn gate, void* context) {
    _shareGate = gate;
    _shareGateContext = context;
}

void MqttBroker::onPlace(MqttBrokerPlaceFn place, MqttBrokerHandOffFn handOff, void* context) {
    _onPlace = place;
    _onHandOff = handOff;
    _placeContext = context;
}

uint32_t MqttBroker::egress(const MqttSpan& topic, const MqttSpan& payload, uint8_t qos,
                            bool retain, const MqttProperties& properties,
                            const MqttSpan& clientId, size_t matched, size_t dropped,
                            uint64_t deferral, const void* origin) {
    if (_onEgress == nullptr) return 0;
    MqttBrokerRouted message;
    message.topic = topic;
    message.payload = payload;
    message.qos = qos;
    message.retain = retain;
    message.properties = &properties;
    message.clientId = clientId;
    message.matched = matched;
    message.dropped = dropped;
    message.origin = origin;
    return _onEgress(_egressContext, message, deferral);
}

size_t MqttBroker::publishForwarded(const MqttBrokerRouted& message, size_t* droppedOut) {
    static const MqttProperties kNone;
    const MqttProperties& properties = message.properties ? *message.properties : kNone;
    // Stored here as well as where it was published: each broker of a
    // multi-core server keeps the whole retained set, so a SUBSCRIBE is
    // answered from its own store without asking the others.
    if (message.retain && _config.retainAvailable) {
        _retained.store(message.topic, message.payload, message.qos, properties, mq_millis());
    }
    size_t dropped = 0;
    const size_t matched = route(message.topic, message.payload, message.qos,
                                 message.retain, properties, nullptr, &dropped);
    if (droppedOut) *droppedOut = dropped;
    return matched;
}

uint64_t MqttBroker::openDeferred(MqttBrokerConnection& connection, uint16_t packetId,
                                  uint8_t qos, size_t matched, size_t dropped,
                                  bool retainedFull) {
    uint32_t index;
    if (_deferredFree != 0xFFFFFFFFu) {
        index = _deferredFree;
        _deferredFree = _deferred[index].nextFree;
    } else {
        index = (uint32_t)_deferred.size();
        _deferred.push_back(Deferred());
    }
    if (++_deferredStamp == 0) _deferredStamp = 1;
    Deferred& entry = _deferred[index];
    entry = Deferred();
    entry.slot = slotOf(connection);
    entry.generation = connection.generation;
    entry.stamp = _deferredStamp;
    entry.packetId = packetId;
    entry.qos = qos;
    entry.retainedFull = retainedFull;
    entry.matched = matched;
    entry.dropped = dropped;
    return (uint64_t)index | ((uint64_t)entry.stamp << 32);
}

void MqttBroker::closeDeferred(uint64_t deferral) {
    const uint32_t index = (uint32_t)deferral;
    if (index >= _deferred.size()) return;
    Deferred& entry = _deferred[index];
    if (entry.stamp == 0 || entry.stamp != (uint32_t)(deferral >> 32)) return;
    entry.stamp = 0;
    entry.nextFree = _deferredFree;
    _deferredFree = index;
}

void MqttBroker::expectParties(uint64_t deferral, uint32_t parties) {
    const uint32_t index = (uint32_t)deferral;
    if (index >= _deferred.size()) return;
    Deferred& entry = _deferred[index];
    if (entry.stamp == 0 || entry.stamp != (uint32_t)(deferral >> 32)) return;
    entry.expected = parties;
    if (entry.reported >= entry.expected) finishDeferred(deferral);
}

void MqttBroker::concludeDeferred(uint64_t deferral, size_t matched, size_t dropped,
                                  uint8_t veto) {
    const uint32_t index = (uint32_t)deferral;
    if (index >= _deferred.size()) return;
    Deferred& entry = _deferred[index];
    if (entry.stamp == 0 || entry.stamp != (uint32_t)(deferral >> 32)) return;
    if (veto != 0 && entry.veto == 0) entry.veto = veto;
    entry.matched += matched;
    entry.dropped += dropped;
    entry.reported++;
    // `expected` is 0 until the egress hook has returned. A party that
    // answers before that is counted, and the acknowledgement waits for the
    // number to be known.
    if (entry.expected != 0 && entry.reported >= entry.expected) finishDeferred(deferral);
}

// Every party has answered: the acknowledgement goes out with the merged
// outcome, by the same rules handlePublish() applies to a local one.
void MqttBroker::finishDeferred(uint64_t deferral) {
    const Deferred entry = _deferred[(uint32_t)deferral];
    closeDeferred(deferral);
    if (entry.slot >= _connections.size()) return;
    MqttBrokerConnection& connection = _connections[entry.slot];
    // The connection that published is gone, or the slot is somebody else's
    // now. Nothing to say and nobody to say it to; the client re-sends the
    // PUBLISH on its next session, which is what at-least-once means.
    if (!connection.inUse() || connection.closing ||
        connection.generation != entry.generation) {
        return;
    }

    uint8_t outcome = MQ_REASON_SUCCESS;
    if (entry.veto != 0) {
        // A party that had to take it could not: that, not the subscribers'
        // outcome, is what the publisher needs to know.
        outcome = entry.veto;
    } else if (entry.matched == 0) {
        outcome = MQ_REASON_NO_MATCHING_SUBSCRIBERS;
    } else if (entry.retainedFull || entry.dropped == entry.matched) {
        outcome = MQ_REASON_QUOTA_EXCEEDED;
    }
    const uint8_t type = entry.qos == MQ_QOS_1 ? MQ_PKT_PUBACK : MQ_PKT_PUBREC;
    if (!sendSimpleAck(connection, type, entry.packetId, outcome)) {
        closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
        return;
    }
    if (entry.qos == MQ_QOS_2) endRefusedQos2(connection, entry.packetId, outcome);
    if (entry.qos == MQ_QOS_1 && connection.inFlightFromPeer > 0) {
        connection.inFlightFromPeer--;
    }
    // Written outside a service pass, so make sure the connection gets one:
    // if the socket did not take it all, write interest must be registered.
    queueWork(connection);
}

void MqttBroker::handOff(MqttBrokerConnection& connection, int target) {
    MqttTransport* transport = connection.transport;
    MqttListener* origin = connection.origin;
    // Off this broker's books BEFORE the hand-off: the descriptor must leave
    // this epoll set before another broker registers it in its own, and the
    // slot is free here from this moment. The transport is NOT ended - it is
    // the receiver's now, and so is the job of handing it back to `origin`.
    unwatch(connection);
    if (_liveConnections > 0) _liveConnections--;
    releaseConnectionSlot(connection);
    connection.recycle();
    _status.handedOff++;
    if (_onHandOff && transport != nullptr) {
        _onHandOff(_placeContext, target, *transport, origin,
                   _handOffBytes.data(), _handOffBytes.size());
    }
}

bool MqttBroker::adopt(MqttTransport& transport, MqttListener* origin,
                       const uint8_t* bytes, size_t length) {
    if (_connections.empty()) return false;
    MqttBrokerConnection* slot = freeConnection();
    if (slot == nullptr) return false;

    slot->recycle();
    slot->transport = &transport;
    slot->origin = origin;
    if (!slot->refuseOnly && !_freeConnections.empty() &&
        _freeConnections.back() == slotOf(*slot)) {
        _freeConnections.pop_back();
    }
    slot->openedAt = mq_millis();
    slot->lastPacketAt = slot->openedAt;
    slot->link.begin(transport, _config.receiveBufferSize, _config.transmitBufferSize,
                     _config.reserveUpFront ? 0 : kLinkInitialBytes);
    if (!slot->link.prime(bytes, length)) {
        // More than this broker could ever hold; the caller keeps the
        // transport and decides what to do with it.
        slot->link.end();
        slot->transport = nullptr;
        slot->origin = nullptr;
        releaseConnectionSlot(*slot);
        return false;
    }
    _liveConnections++;
    _status.adopted++;
    if (_eventDriven) {
        watch(*slot);
        slot->readable = true;
        queueWork(*slot);
    }
    return true;
}

void MqttBroker::onTimer(void* context, MqttTimer& timer, mq_time_t now) {
    MqttBroker& broker = *static_cast<MqttBroker*>(context);
    if (timer.owner >= broker._sessions.size()) return;
    MqttBrokerSession& session = broker._sessions[timer.owner];
    if (!session.inUse() || session.connected) return;

    if (timer.kind == kWillTimer) {
        if (!session.will.scheduled) return;
        if ((mq_elapsed(session.will.scheduledAt, now) / 1000u) >= session.will.delayInterval) {
            broker.publishWill(session);
        } else {
            broker.armWill(session, now);
        }
        return;
    }

    if (timer.kind == kExpiryTimer) {
        if (session.hasExpired(now)) {
            // As expireSessions(): a Will still owed goes out as the session
            // ends [V5:3.1.3.2.2].
            if (session.will.scheduled) broker.publishWill(session);
            broker._wheel.cancel(session.willTimer);
            session.release();
            broker._status.sessionsExpired++;
        } else {
            broker.armExpiry(session, now);
        }
    }
}

#endif  // MQTT_PLATFORM_HOST

void MqttBroker::poll() {
    const mq_time_t now = mq_millis();

#if defined(MQTT_PLATFORM_HOST)
    if (_eventDriven && _readiness != nullptr && _readiness->isOpen()) {
        pollEvents(now);
        return;
    }

    // ONE syscall for the whole pool, replacing one per connection. The
    // timeout is zero because poll() promises never to wait for anything - a
    // PLC calls it once per scan. A daemon that wants to sleep between polls
    // waits on the same set itself, with a real timeout.
    if (_readiness != nullptr && _readiness->isOpen()) {
        refreshReadiness();
        _readiness->wait(0);
    }
#endif

    acceptNew();

    // Round-robin over the connections that are actually LIVE. See
    // _sweepStart.
    //
    // Advancing the start by one slot per poll is not enough on its own: with
    // two clients in a pool of thirty-two, slot 0 still leads thirty-one
    // passes out of thirty-two. So the next sweep starts after whichever
    // connection led this one, which gives each live connection its turn
    // regardless of how empty the pool is.
    // Counted on the way past, rather than by connectionCount() walking the
    // same pool again once the sweep is over.
    size_t liveConnections = 0;

    const size_t count = _connections.size();
    if (count > 0) {
        // THE START IS TAKEN ONCE, BEFORE THE LOOP. _sweepStart is updated
        // below, and the first version of this used it directly as the loop's
        // base - so moving it mid-sweep shifted every remaining index, visited
        // one connection twice and skipped another entirely. The starvation
        // test caught it immediately: a quiet client went unanswered while the
        // noisy one had sixteen packets processed in a single pass.
        const size_t start = _sweepStart;
        size_t next = start;
        bool ledYet = false;

        for (size_t step = 0; step < count; step++) {
            const size_t index = (start + step) % count;
            MqttBrokerConnection& connection = _connections[index];
            if (!connection.inUse()) continue;
            liveConnections++;

            if (!ledYet) {
                next = (index + 1) % count;
                ledYet = true;
            }

            if (connection.closing) finishClosing(connection, now);
            else serviceConnection(connection);
        }

        _sweepStart = ledYet ? next : 0;
    }

    // Wills and session expiry are checked after the connections, because a
    // connection serviced in this same pass may have just ended and scheduled
    // one.
    serviceWills(now);
    _status.sessions = expireSessions(now);
    _retained.sweep(now, &_status.retained);

    // COUNTED WHILE SWEEPING, not by sweeping again.
    //
    // These three were full walks of three pools on every poll - 32
    // connections, 64 sessions and 256 retained slots at the defaults, the
    // last of them walked TWICE because sweep() had just been over the same
    // array. That is ~350 slot visits per poll to populate three diagnostic
    // fields, and at 1000 connections it is far more. The sweeps above already
    // touch every live entry, so they count as they go.
    _status.connections = liveConnections;
}

MqttBrokerConnection* MqttBroker::freeConnection() {
    // The serving slots first. _connections holds maxConnections + 1; the last
    // is the reserve and is never handed out for ordinary service.
    const size_t serving = _connections.size() > 0 ? _connections.size() - 1 : 0;

#if defined(MQTT_PLATFORM_HOST)
    // From the free stack, not a walk (L13). The slot is LOOKED AT here and
    // taken in acceptNew(), only once a transport is actually put in it -
    // most polls find nobody waiting, and the slot must stay free for them.
    while (!_freeConnections.empty()) {
        const uint32_t index = _freeConnections.back();
        if (index < serving && !_connections[index].inUse()) {
            _connections[index].refuseOnly = false;
            return &_connections[index];
        }
        _freeConnections.pop_back();     // stale - never expected, and harmless
    }
#else
    for (size_t index = 0; index < serving; index++) {
        if (!_connections[index].inUse()) {
            _connections[index].refuseOnly = false;
            return &_connections[index];
        }
    }
#endif

    // Everything is busy. The reserve is handed out marked, so the CONNECT
    // that arrives on it is answered rather than ignored.
    MqttBrokerConnection& reserve = _connections[_connections.size() - 1];
    if (reserve.inUse()) return nullptr;    // already refusing somebody
    reserve.refuseOnly = true;
    return &reserve;
}

#if defined(MQTT_PLATFORM_HOST)
// A serving slot coming free goes back on the stack. The reserve never does.
void MqttBroker::releaseConnectionSlot(MqttBrokerConnection& connection) {
    const uint32_t index = slotOf(connection);
    if (_connections.empty() || index + 1 >= _connections.size()) return;
    _freeConnections.push_back(index);
}
#endif

void MqttBroker::acceptNew() {
    for (MqttListener* listener : _listeners) {
        if (listener == nullptr || !listener->isListening()) continue;

        // A BOUNDED BATCH per listener per poll - not one, and not a drain.
        //
        // This took one connection per poll, for a good reason badly applied:
        // draining a listener completely would let a burst of connections
        // starve the connections already being served, and every one of those
        // is a device waiting for an acknowledgement. That reasoning is sound.
        // Taking exactly ONE is not, because every poll ALSO services every
        // existing connection - so accepting N simultaneous connections costs
        // N + (N-1) + ... + 1 connection services, which is O(N squared).
        //
        // Measured, with mqtt-benchmark: 1000 clients arriving at once left
        // 300-odd of them unconnected. The client's CONNECT timed out, paho
        // fell back to MQTT 3.1 - which this broker does not implement and
        // correctly refuses - and the run stalled for ever. The SAME thousand
        // clients ramped over ten seconds connected perfectly, every message
        // delivered, which is what identified the accept rate rather than
        // anything about capacity: the limits were never reached.
        //
        // Sixteen keeps the fairness the original was protecting - a burst
        // still cannot monopolise a poll - while making a connection storm
        // linear instead of quadratic. A plant panel with a dozen devices
        // never reaches the bound and behaves exactly as before.
        for (size_t accepted = 0; accepted < kAcceptsPerPoll; accepted++) {
            MqttBrokerConnection* slot = freeConnection();
            if (slot == nullptr) return;

            MqttTransport* transport = listener->accept();
            if (transport == nullptr) break;

            slot->recycle();
            slot->transport = transport;
#if defined(MQTT_PLATFORM_HOST)
            // Taken now that it holds a connection; see freeConnection().
            if (!slot->refuseOnly && !_freeConnections.empty() &&
                _freeConnections.back() == slotOf(*slot)) {
                _freeConnections.pop_back();
            }
#endif
            slot->origin = listener;
            slot->openedAt = mq_millis();
            slot->lastPacketAt = slot->openedAt;
            // Full size now, or small and grown on demand - see
            // MqttBrokerConfig::reserveUpFront.
            slot->link.begin(*transport, _config.receiveBufferSize,
                             _config.transmitBufferSize,
                             _config.reserveUpFront ? 0 : kLinkInitialBytes);
            _status.accepted++;
#if defined(MQTT_PLATFORM_HOST)
            _liveConnections++;
            if (_eventDriven) {
                watch(*slot);
                slot->readable = true;     // its CONNECT may already be here
                queueWork(*slot);
            }
#endif
        }
    }
}

void MqttBroker::serviceConnection(MqttBrokerConnection& connection) {
    const mq_time_t now = mq_millis();

    connection.link.serviceTransmit();

    // Read and dispatch whole packets - but a BOUNDED number of them.
    //
    // Draining one connection completely before looking at the next lets a
    // client with a burst monopolise the whole poll: everybody else's
    // acknowledgements wait behind it, and on a busy line that is exactly the
    // client you least want to prioritise. It also reorders work across
    // connections far more than it needs to - a SUBSCRIBE that arrived first
    // on one socket can be processed after a PUBLISH that arrived second on
    // another, so a subscriber that had every right to see the message does
    // not. MQTT guarantees ordering WITHIN a connection, not across, so
    // neither order is wrong; this one is fairer.
    //
    // Whatever is left is read on the next poll, which is microseconds away.
#if defined(MQTT_PLATFORM_HOST)
    // The ONLY thing readiness changes. Everything below this point - the
    // keepalive deadline, the connect timeout, the queued deliveries, the
    // transmit cursor - runs for every connection every poll exactly as it
    // always did. What is skipped is a recv() on a socket the kernel has just
    // said has nothing, which was half of all the recv() calls this broker
    // made and the larger half of its CPU time.
    const bool mayRead = shouldRead(connection);
#else
    const bool mayRead = true;
#endif

    // ONE READ PER CONNECTION PER POLL, then drain what it produced.
    //
    // fill() used to be called at the top of every one of these iterations, so
    // a connection that yielded a single packet paid a second recv() to
    // discover the socket was empty - and with a bounded loop of eight, a busy
    // connection paid one wasted syscall for every packet it delivered. That
    // was most of the 20,373 EAGAIN returns out of 41,173 recvfrom calls
    // measured at 200 clients.
    //
    // A bulk read takes everything available in one call, so one call is all
    // there is to make: whatever it brought back is decoded from the link's
    // buffer without touching the socket again.
    if (mayRead) connection.link.fill();

    for (size_t packets = 0; packets < kPacketsPerPoll; packets++) {
        if (!connection.inUse() || connection.closing) break;

        MqttFixedHeader header;
        const MqttLinkStatus status = connection.link.inspect(header);

        if (status == MqttLinkStatus::Idle || status == MqttLinkStatus::Incomplete) {
            if (status == MqttLinkStatus::Idle && connection.link.peerGone()) {
                // Gone without a DISCONNECT. The Will is owed.
                closeConnection(connection, 0, false, true);
                return;
            }
            break;
        }
        if (status == MqttLinkStatus::Closed) {
            closeConnection(connection, 0, false, true);
            return;
        }
        if (status == MqttLinkStatus::Malformed) {
            _status.protocolErrors++;
            closeConnection(connection, MQ_REASON_MALFORMED_PACKET, true, true);
            return;
        }
        if (status == MqttLinkStatus::TooLarge) {
            // [V5:3.2.2.3.6] We told them the limit in CONNACK; exceeding it
            // is 0x95 rather than a malformed packet, because the bytes are
            // fine and the size is not.
            closeConnection(connection, MQ_REASON_PACKET_TOO_LARGE, true, true);
            return;
        }

        // THE VERSION IS NOT KNOWN UNTIL THE CONNECT IS READ, and it cannot
        // be learnt by decoding: each codec refuses a level it does not
        // implement, so trying one and falling back to the other would report
        // a 3.1.1 client as a malformed packet. The one byte that decides is
        // read on its own first.
        const MqttCodec* useCodec = &codecFor(connection.version);
        if (!connection.connectSeen && header.type == MQ_PKT_CONNECT) {
            uint8_t level = 0;
            const uint8_t* body = connection.link.rawPacket() + header.headerLength;
            if (!mq_peekProtocolLevel(body, header.remainingLength, level)) {
                _status.protocolErrors++;
                closeConnection(connection, MQ_REASON_MALFORMED_PACKET, false, false);
                return;
            }

            if (level == (uint8_t)MqttVersion::V5) {
                connection.version = MqttVersion::V5;
                useCodec = (const MqttCodec*)&g_brokerCodecV5;
            } else if (level == (uint8_t)MqttVersion::V311) {
                connection.version = MqttVersion::V311;
                useCodec = (const MqttCodec*)&g_brokerCodecV311;
            } else {
                // [V311:MQTT-3.1.2-2] "If the protocol level is not supported
                // by the Server ... the Server MUST respond to the CONNECT
                // Packet with a CONNACK return code 0x01 (unacceptable
                // protocol level) and then disconnect."
                //
                // Answered in the 3.1.1 shape, because a client speaking a
                // level we do not know cannot be assumed to read 5.0's. Its
                // two bytes are the same in every version of MQTT there has
                // ever been, which is the only reason this works at all.
                connection.version = MqttVersion::V311;
                connection.connectSeen = true;
                sendConnAck(connection, MQ_RETURN_UNACCEPTABLE_PROTOCOL, false, nullptr);
                _status.refused++;
                closeConnection(connection, (uint8_t)(MQ_RETURN_UNACCEPTABLE_PROTOCOL), false, false);
                return;
            }
        }

#if defined(MQTT_PLATFORM_HOST)
        // ONE BROKER PER CORE: the CONNECT decides which broker this client's
        // session lives on, so every byte read so far - the CONNECT and
        // anything pipelined behind it - is kept, in case the connection has
        // to move to that broker along with them.
        const bool placing = _onPlace != nullptr && !connection.connectSeen &&
                             header.type == MQ_PKT_CONNECT;
        if (placing) {
            _handOffBytes.assign(connection.link.rawPacket(),
                                 connection.link.rawPacket() + connection.link.buffered());
        }
#endif

        MqttPacket packet;
        const MqttError error = connection.link.take(*useCodec, header, packet);

        if (error != MqttError::None) {
            _status.protocolErrors++;
            // THE STREAM NEVER RESYNCHRONISES. One stray octet desynchronises
            // permanently and every packet after it is garbage that may still
            // parse, so the connection ends here rather than hunting for a
            // boundary.
            closeConnection(connection, mq_errorToReason(error), true, true);
            return;
        }

#if defined(MQTT_PLATFORM_HOST)
        if (placing) {
            const int target = _onPlace(_placeContext, packet.connect.clientId);
            if (target >= 0) {
                handOff(connection, target);
                return;
            }
        }
#endif

        connection.lastPacketAt = now;
        handlePacket(connection, header, packet);
    }

    if (!connection.inUse() || connection.closing) return;
    if (checkDeadlines(connection, now)) return;

    deliverQueued(connection);
    connection.link.serviceTransmit();
}

bool MqttBroker::checkDeadlines(MqttBrokerConnection& connection, mq_time_t now) {
    // [V5:3.1] A connection that has not sent a CONNECT is not a client yet
    // and is holding a slot every real client might need.
    if (!connection.connectSeen &&
        mq_elapsed(connection.openedAt, now) > _config.connectTimeoutMs) {
        closeConnection(connection, MQ_REASON_MAXIMUM_CONNECT_TIME, false, false);
        return true;
    }

    // [V5:MQTT-3.1.2-22] "If the Keep Alive value is non-zero and the Server
    // does not receive an MQTT Control Packet from the Client within one and a
    // half times the Keep Alive time period, it MUST close the Network
    // Connection to the Client as if the network had failed."
    //
    // "As if the network had failed" is the operative phrase: the Will is
    // published, because this is exactly the case the Will exists for.
    if (connection.keepAliveSeconds != 0) {
        const mq_time_t limit = (mq_time_t)connection.keepAliveSeconds * 1500u;
        if (mq_elapsed(connection.lastPacketAt, now) > limit) {
            _status.keepAliveTimeouts++;
            closeConnection(connection, MQ_REASON_KEEP_ALIVE_TIMEOUT, true, true);
            return true;
        }
    }
    return false;
}

// =============================================================================
// CLOSING
// =============================================================================

MqttBrokerConnection* MqttBroker::connectionForSession(const MqttBrokerSession& session) {
    // By the slot the session recorded when it was attached - one lookup
    // instead of a walk of every connection, which a takeover used to cost.
    if (session.connectionSlot < _connections.size()) {
        MqttBrokerConnection& connection = _connections[session.connectionSlot];
        if (connection.inUse() && connection.session == &session) return &connection;
    }
    // Never expected; kept so a slot that was not recorded costs time rather
    // than a missed takeover.
    for (MqttBrokerConnection& connection : _connections) {
        if (connection.inUse() && connection.session == &session) return &connection;
    }
    return nullptr;
}

void MqttBroker::closeConnection(MqttBrokerConnection& connection,
                                 uint8_t reasonCode, bool sendDisconnect,
                                 bool publishWillNow) {
    if (!connection.inUse()) return;
    connection.closing = true;

    bool wroteDisconnect = false;

    if (sendDisconnect && connection.version == MqttVersion::V5 &&
        connection.connectSeen) {
        // 3.1.1 has no server-to-client DISCONNECT at all, which is why this
        // is conditional rather than universal - the codec would refuse to
        // encode it and the connection would close with no explanation, which
        // is precisely what 3.1.1 clients have always had to live with.
        MqttPacket packet;
        packet.header.type = MQ_PKT_DISCONNECT;
        packet.disconnect.reasonCode = reasonCode;
        wroteDisconnect = connection.link.send(codecFor(connection.version), packet);

        // Pushed as far as the socket will take now. Nothing waits for it:
        // [V5:3.14.4] requires the connection to close, and a broker that
        // blocked to flush a farewell would stall every other client.
        connection.link.serviceTransmit();
    }

    MqttBrokerSession* session = connection.session;

    if (session != nullptr) {
        session->connected = false;
        session->offlineSince = mq_millis();

        if (!publishWillNow) {
            // A clean DISCONNECT DELETES the Will rather than delaying it.
            // [V5:MQTT-3.14.4-3] "On receipt of DISCONNECT with a Reason Code
            // of 0x00 (Normal disconnection) the Server MUST discard any Will
            // Message associated with the current Connection without
            // publishing it."
            session->will.clear();
        } else if (session->will.present) {
            // [V5:3.1.3.2.2] Scheduled, not sent. The delay is what lets a
            // device reboot inside its Will Delay Interval without every
            // dashboard in the plant flashing red on the way past.
            session->will.scheduled = true;
            session->will.scheduledAt = mq_millis();
        }

        if (_onConnection) {
            _onConnection(_connectionContext, asSpan(session->clientId()), false,
                          reasonCode);
        }

        // [V5:3.1.2.11.2] A Session Expiry Interval of 0 means the Session
        // ends when the Network Connection does. At 3.1.1 that is Clean
        // Session 1, stored the same way, so one rule covers both versions.
        if (session->expiryInterval == 0) {
            // "...until the Will Delay Interval has passed OR THE SESSION
            // ENDS, whichever happens first" [V5:3.1.3.2.2]. The session is
            // ending right now, so the delay is over however long it was.
            if (session->will.scheduled) publishWill(*session);
            session->release();
        }
#if defined(MQTT_PLATFORM_HOST)
        // The event-driven broker does not walk its sessions, so what the
        // walk would have found later is put on the wheel now.
        if (_eventDriven && session->inUse()) {
            const mq_time_t now = mq_millis();
            if (session->will.scheduled) armWill(*session, now);
            armExpiry(*session, now);
        }
#endif
    }

    // A REFUSED CONNECT HAS NO SESSION, and until now that meant no
    // notification either: the `refused` counter went up and nothing else
    // happened. An operator watching a plant fill up with clients that cannot
    // connect could see the count rise and had no way to learn the reason,
    // which is the one fact that would have told them what to fix.
    //
    // There is no client identifier to give - the session is what holds one,
    // and there is no session - so the span is empty and the REASON is the
    // payload. That is still the difference between a diagnosis and a number.
    if (session == nullptr && reasonCode != 0 && _onConnection) {
        _onConnection(_connectionContext, MqttSpan(), false, reasonCode);
    }

    if (session != nullptr) session->connectionSlot = MqttBrokerSession::kNoConnection;
    connection.session = nullptr;
    _status.disconnected++;

    // THE SOCKET IS NOT CLOSED YET.
    //
    // Closing it now would reset the connection if the peer is still writing,
    // and a reset discards the DISCONNECT that was just written - so the
    // client would learn nothing about why it was disconnected. Paho's
    // flow-control test found exactly that, as a BrokenPipeError raised while
    // it was still sending.
    // ONLY LINGER IF THERE IS SOMETHING TO READ. The wait exists so the peer
    // can read the DISCONNECT; where none was written - a 3.1.1 connection,
    // which has no server-to-client DISCONNECT at all, or a peer that has
    // already gone - there is nothing to wait for, and waiting would only
    // delay a slot coming back.
    // LINGER IF THERE IS ANYTHING AT ALL STILL TO GO, not only a DISCONNECT.
    //
    // This asked `wroteDisconnect`, which tracks one packet type. Every CONNECT
    // refusal writes a CONNACK and then closes with sendDisconnect = false -
    // so the reason the client needed was written into the transmit buffer and
    // the connection was released in the same call, discarding it if the
    // socket had not taken it yet. A refusal that gives no reason is the exact
    // failure this linger exists to prevent, and refusals are the case where a
    // reason matters most.
    const bool somethingToRead =
        wroteDisconnect || connection.link.transmitPending() != 0;
    connection.lingerUntil =
        mq_millis() + (somethingToRead ? _config.closeLingerMs : 0);
    finishClosing(connection, mq_millis());
#if defined(MQTT_PLATFORM_HOST)
    // Still lingering - and possibly closed from another connection's turn, by
    // a takeover - so it must be listed, or the linger never finishes.
    if (connection.inUse()) queueWork(connection);
#endif
}

void MqttBroker::finishClosing(MqttBrokerConnection& connection, mq_time_t now) {
    if (!connection.inUse() || !connection.closing) return;

    // Whatever is still owed goes out, and whatever the peer is still sending
    // is read and thrown away - both so that the close, when it comes, is a
    // FIN rather than a reset.
    connection.link.serviceTransmit();
    connection.link.fill();

    const bool peerGone = connection.link.peerGone();
    const bool timeUp = !mq_isFuture(now, connection.lingerUntil);
    const bool flushed = connection.link.transmitIdle();

    if (!peerGone && !timeUp) return;
    if (!flushed && !timeUp) return;    // still pushing the farewell out

#if defined(MQTT_PLATFORM_HOST)
    // BEFORE the descriptor is closed. A closed descriptor left in the
    // interest list is the stale registration MqttPollSet.h warns about.
    unwatch(connection);
    if (_liveConnections > 0) _liveConnections--;
    releaseConnectionSlot(connection);
#endif
    if (connection.origin) connection.origin->release(connection.transport);
    else if (connection.transport) connection.transport->end();

    connection.recycle();
}

// =============================================================================
// SENDING
// =============================================================================

bool MqttBroker::send(MqttBrokerConnection& connection, const MqttPacket& packet) {
    if (!connection.inUse()) return false;

    const MqttCodec& codec = codecFor(connection.version);

    // [V5:3.1.2.11.4] The client's Maximum Packet Size is a promise we must
    // keep: "The Server MUST NOT send packets exceeding Maximum Packet Size to
    // the Client" [V5:MQTT-3.2.2-15]. A packet over it is dropped rather than
    // truncated, because there is no such thing as half an MQTT packet.
    if (connection.peerMaximumPacketSize != 0) {
        const size_t needed = codec.size(packet);
        if (needed == 0 || needed > connection.peerMaximumPacketSize) return false;
    }

    if (connection.link.send(codec, packet)) return true;

    // IT DID NOT GO. Try once more with the queue flushed first.
    //
    // MqttLink::queue() refuses when the outbound buffer has no room, and the
    // buffer drains as fast as the socket will take it - so a flush is very
    // often the whole of the problem. deliverQueued() already does this before
    // encoding a PUBLISH; acknowledgements were emitted straight into the same
    // buffer with no such guard, and then their return value was discarded.
    if (connection.link.lastEncodeError() == MqttError::Overflow) {
        connection.link.serviceTransmit();
        if (connection.link.send(codec, packet)) return true;
    }
    return false;
}

bool MqttBroker::sendConnAck(MqttBrokerConnection& connection, uint8_t reasonCode,
                             bool sessionPresent, const MqttSpan* assignedId) {
    MqttPacket packet;
    packet.header.type = MQ_PKT_CONNACK;
    packet.connack.sessionPresent = sessionPresent;
    packet.connack.reasonCode = reasonCode;
    packet.connack.version = connection.version;

    if (connection.version == MqttVersion::V5) {
        MqttProperties& properties = packet.connack.properties;

        // EVERYTHING WE ADVERTISE HERE IS ENFORCED BELOW. That pairing is the
        // point: a limit stated and not kept teaches every client a lie, and a
        // limit kept and not stated leaves them all guessing.
        properties.receiveMaximum = _config.receiveMaximum;
        properties.markPresent(MQ_PROP_RECEIVE_MAXIMUM);

        properties.maximumPacketSize = _config.maximumPacketSize;
        properties.markPresent(MQ_PROP_MAXIMUM_PACKET_SIZE);

        if (_config.topicAliasMaximum > 0) {
            properties.topicAliasMaximum = _config.topicAliasMaximum;
            properties.markPresent(MQ_PROP_TOPIC_ALIAS_MAXIMUM);
        }

        // [V5:3.2.2.3.4] "If the Server does not include Maximum QoS in the
        // CONNACK ... the Client uses a Maximum QoS of 2." So it is sent only
        // when it is not 2 - sending the default is legal but is noise, and
        // [V5:MQTT-3.2.2-9] forbids the value 2 here in some readings. Absent
        // is unambiguous.
        if (_config.maximumQos < MQ_QOS_2) {
            properties.maximumQos = _config.maximumQos;
            properties.markPresent(MQ_PROP_MAXIMUM_QOS);
        }
        if (!_config.retainAvailable) {
            properties.retainAvailable = 0;
            properties.markPresent(MQ_PROP_RETAIN_AVAILABLE);
        }
        if (!_config.wildcardSubscriptionAvailable) {
            properties.wildcardSubscriptionAvailable = 0;
            properties.markPresent(MQ_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE);
        }
        if (!_config.subscriptionIdentifiersAvailable) {
            properties.subscriptionIdentifiersAvailable = 0;
            properties.markPresent(MQ_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE);
        }
        if (!_config.sharedSubscriptionAvailable) {
            properties.sharedSubscriptionAvailable = 0;
            properties.markPresent(MQ_PROP_SHARED_SUBSCRIPTION_AVAILABLE);
        }
        if (connection.keepAliveStated) {
            // [V5:MQTT-3.1.2-21] "If the Server sends a Server Keep Alive on
            // the CONNACK packet, the Client MUST use that value instead of
            // the value it sent as the Keep Alive."
            //
            // Sent ONLY when we actually changed it. A client that asked for
            // an interval we are happy with should hear nothing, and a client
            // that hears a value must be able to trust that it is the one
            // being enforced.
            properties.serverKeepAlive = connection.keepAliveSeconds;
            properties.markPresent(MQ_PROP_SERVER_KEEP_ALIVE);
        }
        if (connection.session != nullptr && connection.sessionExpiryRevised) {
            // [V5:3.2.2.3.2] "If the Session Expiry Interval is absent the
            // value in the CONNECT Packet used." So it is stated back ONLY
            // when we changed it. Echoing what was asked for is legal, and it
            // is five bytes on every CONNACK that say nothing.
            properties.sessionExpiryInterval = connection.session->expiryInterval;
            properties.markPresent(MQ_PROP_SESSION_EXPIRY_INTERVAL);
        }
        if (assignedId != nullptr && assignedId->length > 0) {
            // [V5:MQTT-3.2.2-16] "If the Client connects using a zero length
            // Client Identifier, the Server MUST respond with a CONNACK
            // containing an Assigned Client Identifier."
            properties.assignedClientIdentifier = *assignedId;
            properties.markPresent(MQ_PROP_ASSIGNED_CLIENT_IDENTIFIER);
        }
    }

    const bool sent = send(connection, packet);
    connection.link.serviceTransmit();
    return sent;
}

bool MqttBroker::sendSimpleAck(MqttBrokerConnection& connection, uint8_t packetType,
                               uint16_t packetId, uint8_t reasonCode) {
    MqttPacket packet;
    packet.header.type = packetType;
    packet.ack.packetId = packetId;

    // 3.1.1 has no reason code on these at all, and the codec refuses to
    // encode a non-success one rather than quietly dropping it - so a failure
    // that cannot be expressed becomes a success that can, which is what every
    // 3.1.1 broker has always done.
    packet.ack.reasonCode =
        connection.version == MqttVersion::V5 ? reasonCode : MQ_REASON_SUCCESS;

    // THE RETURN IS THE POINT. An acknowledgement that does not go has no
    // retransmission anywhere in MQTT - a lost SUBACK leaves the client
    // waiting on a connection this broker believes is healthy, and a lost
    // PINGRESP starts its keepalive watchdog. Dropping one silently is the
    // one outcome with no way back; the callers close instead, which the
    // client can at least recover from.
    return send(connection, packet);
}

// =============================================================================
// SESSIONS
// =============================================================================

#if defined(MQTT_PLATFORM_HOST)
const std::string& MqttBroker::identifierOf(const void* sessions, uint32_t index) {
    return static_cast<const MqttBrokerSession*>(sessions)[index].clientId();
}
#endif

MqttBrokerSession* MqttBroker::sessionFor(const MqttSpan& clientId) {
#if defined(MQTT_PLATFORM_HOST)
    if (_sessionIndex.configured()) {
        const uint32_t index = _sessionIndex.find(clientId.data, clientId.length);
        if (index == MqttSessionIndex::kNone || index >= _sessions.size()) return nullptr;
        MqttBrokerSession& session = _sessions[index];
        return session.matchesClientId(clientId) ? &session : nullptr;
    }
#endif
    for (MqttBrokerSession& session : _sessions) {
        if (session.matchesClientId(clientId)) return &session;
    }
    return nullptr;
}

MqttBrokerSession* MqttBroker::freeSession() {
#if defined(MQTT_PLATFORM_HOST)
    if (_sessionIndex.configured()) {
        const uint32_t index = _sessionIndex.takeFree();
        if (index == MqttSessionIndex::kNone || index >= _sessions.size()) return nullptr;
        return &_sessions[index];
    }
#endif
    for (MqttBrokerSession& session : _sessions) {
        if (!session.inUse()) return &session;
    }
    return nullptr;
}

MqttBrokerSession* MqttBroker::findSession(const char* clientId) {
    if (clientId == nullptr) return nullptr;
    MqttSpan span;
    span.data = (const uint8_t*)clientId;
    span.length = (uint32_t)strlen(clientId);
    return sessionFor(span);
}

bool MqttBroker::isConnected(const char* clientId) const {
    if (clientId == nullptr) return false;
    // strlen ONCE. It was inside the loop, so the same string was measured
    // again for every session in the pool.
    const size_t length = strlen(clientId);
    for (const MqttBrokerSession& session : _sessions) {
        if (!session.inUse() || !session.connected) continue;
        if (session.clientId().size() == length &&
            memcmp(session.clientId().data(), clientId, length) == 0) {
            return true;
        }
    }
    return false;
}

size_t MqttBroker::connectionCount() const {
    size_t used = 0;
    for (const MqttBrokerConnection& connection : _connections) {
        if (connection.inUse()) used++;
    }
    return used;
}

size_t MqttBroker::sessionCount() const {
    size_t used = 0;
    for (const MqttBrokerSession& session : _sessions) {
        if (session.inUse()) used++;
    }
    return used;
}

size_t MqttBroker::expireSessions(mq_time_t now) {
    for (MqttBrokerSession& session : _sessions) {
        if (!session.inUse() || !session.hasExpired(now)) continue;

        // A Will still owed when the session ends goes out now: "until the
        // Will Delay Interval has passed or the Session ends, whichever
        // happens first" [V5:3.1.3.2.2].
        if (session.will.scheduled) publishWill(session);

        session.release();
        _status.sessionsExpired++;
    }

    // Counted here rather than by sessionCount() walking the same pool again
    // immediately afterwards.
    size_t live = 0;
    for (const MqttBrokerSession& session : _sessions) {
        if (session.inUse()) live++;
    }
    return live;
}

void MqttBroker::publishWill(MqttBrokerSession& session) {
    MqttBrokerWill will = session.will;

    // Cleared BEFORE it is routed. Routing can re-enter this broker's own
    // delivery path, and a Will still marked scheduled at that moment would be
    // published a second time by the very next poll.
    session.will.clear();

    if (!will.present || will.topic.empty()) return;

    MqttProperties properties;
    restoreProperties(will.properties, properties);

    if (will.retain && _config.retainAvailable) {
        _retained.store(asSpan(will.topic), asSpan(will.payload), will.qos,
                        properties, mq_millis());
    }

    size_t dropped = 0;
    const size_t matched = route(asSpan(will.topic), asSpan(will.payload), will.qos,
                                 will.retain, properties, &session, &dropped);
#if defined(MQTT_PLATFORM_HOST)
    egress(asSpan(will.topic), asSpan(will.payload), will.qos, will.retain, properties,
           asSpan(session.clientId()), matched, dropped, 0);
#else
    (void)matched;
#endif
    _status.willsPublished++;
}

void MqttBroker::serviceWills(mq_time_t now) {
    for (MqttBrokerSession& session : _sessions) {
        if (!session.inUse() || !session.will.scheduled) continue;

        // A session that has reconnected cancels its own Will:
        // [V5:MQTT-3.1.2-8] excludes the case where "a new Network Connection
        // for the ClientID is opened before the Will Delay Interval has
        // elapsed". That is handled where the reconnect happens; this is only
        // the guard that keeps the two from disagreeing.
        if (session.connected) {
            session.will.scheduled = false;
            continue;
        }

        // mq_isFuture(now, deadline) asks whether the DEADLINE is still
        // ahead. Getting the arguments the wrong way round published every
        // Will the instant its connection dropped and then never published one
        // again, which looked exactly like a broker that ignores Will Delay -
        // and which the delay test caught on its first run.
        if ((mq_elapsed(session.will.scheduledAt, now) / 1000u)
                >= session.will.delayInterval) {
            publishWill(session);
        }
    }
}

// =============================================================================
// AUTHORISATION
// =============================================================================

bool MqttBroker::authorise(MqttBrokerConnection& connection, MqttBrokerAction action,
                           const MqttSpan& topic, uint8_t qos, bool retain,
                           const MqttSpan& clientId, const MqttSpan& userName,
                           const MqttSpan& password, uint8_t& reasonCode) {
    reasonCode = MQ_REASON_NOT_AUTHORIZED;

    if (action == MqttBrokerAction::Connect && !_config.allowAnonymous &&
        userName.length == 0) {
        // Refused before the hook runs, so a deployment that turns anonymous
        // access off is safe even with no hook installed at all. A policy that
        // depends on somebody remembering to write a callback is not a policy.
        reasonCode = MQ_REASON_BAD_USER_NAME_OR_PASSWORD;
        return false;
    }

    if (_onAuthorise == nullptr) return true;

    MqttBrokerRequest request;
    request.action = action;
    request.clientId = clientId;
    request.userName = userName;
    request.password = password;
    request.topic = topic;
    request.qos = qos;
    request.retain = retain;
    request.version = connection.version;
    request.peerName = connection.transport ? connection.transport->peerName() : "";

    return _onAuthorise(_authoriseContext, request, reasonCode);
}

// =============================================================================
// CONNECT
// =============================================================================

void MqttBroker::handleConnect(MqttBrokerConnection& connection,
                               const MqttPacket& packet) {
    const MqttConnect& request = packet.connect;

    // [V5:MQTT-3.1.0-2] "A Client can only send the CONNECT packet once over a
    // Network Connection. The Server MUST process a second CONNECT packet sent
    // from a Client as a Protocol Error and close the Network Connection."
    if (connection.connectSeen) {
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, true, true);
        return;
    }

    // THE BROKER IS FULL, AND SAYS SO.
    //
    // This connection came in on the reserve slot, which is handed out only
    // when every serving slot is busy. It is accepted and read purely so that
    // this answer can be given: 0x89 Server Busy at 5.0, 0x03 Server
    // Unavailable at 3.1.1 - a reason the far end can act on, rather than a
    // TCP connection that never completes and never explains itself.
    if (connection.refuseOnly) {
        connection.version = request.version;
        connection.connectSeen = true;
        const bool isBusyV5 = request.version == MqttVersion::V5;
        sendConnAck(connection,
                    isBusyV5 ? MQ_REASON_SERVER_BUSY : MQ_RETURN_SERVER_UNAVAILABLE,
                    false, nullptr);
        _status.refused++;
        closeConnection(connection,
                        (uint8_t)(isBusyV5 ? MQ_REASON_SERVER_BUSY
                                           : MQ_RETURN_SERVER_UNAVAILABLE),
                        false, false);
        return;
    }

    // The version was settled before the decode, by reading the Protocol
    // Level byte - see serviceConnection. By the time a CONNECT has decoded at
    // all, its level is one we implement, so there is nothing left to check
    // here. The CONNACK goes out at the version the CLIENT spoke, which is the
    // mirror of the client's own version-tolerant CONNACK rule and the reason
    // the two interoperate.
    connection.version = request.version;
    connection.connectSeen = true;
    connection.cleanStart = request.cleanStart;

    const bool isV5 = connection.version == MqttVersion::V5;

    // [V5:MQTT-4.12.0-1] "If the Server does not support the Authentication
    // Method supplied by the Client, it MAY send a CONNACK with a Reason Code
    // of 0x8C (Bad authentication method)."
    //
    // No enhanced authentication mechanism ships, so every method is
    // unsupported. Saying so is the whole point: a client that asked for
    // SCRAM-SHA-1 and was answered 0x00 would believe it had authenticated.
    if (isV5 && request.properties.has(MQ_PROP_AUTHENTICATION_METHOD)) {
        sendConnAck(connection, MQ_REASON_BAD_AUTHENTICATION_METHOD, false, nullptr);
        _status.refused++;
        closeConnection(connection, (uint8_t)(MQ_REASON_BAD_AUTHENTICATION_METHOD), false, false);
        return;
    }

    // --- What the client told us about itself ---
    if (isV5) {
        connection.peerReceiveMaximum = request.properties.receiveMaximum;
        connection.peerMaximumPacketSize = request.properties.maximumPacketSize;
        connection.peerTopicAliasMaximum = request.properties.topicAliasMaximum;
        connection.peerWantsProblemInformation =
            request.properties.requestProblemInformation != 0;
    } else {
        // 3.1.1 states none of this, so the defaults are its fixed answers -
        // no flow control, no packet limit, no aliases.
        connection.peerReceiveMaximum = 65535;
        connection.peerMaximumPacketSize = 0;
        connection.peerTopicAliasMaximum = 0;
        connection.peerWantsProblemInformation = true;
    }
    connection.toPeer.reset(connection.peerReceiveMaximum);

    // Inbound aliases are bounded by what WE offered; outbound by what THEY
    // offered. Two independent tables, cleared on every new connection
    // [V5:3.3.2.3.4], which is why this is here and not in the session.
    connection.aliases.reset(
        _config.useOutboundTopicAliases ? connection.peerTopicAliasMaximum : 0,
        _config.topicAliasMaximum);

    // --- Keep alive ---
    //
    // A CEILING, not an override. [V5:3.2.2.3.14] - the Server Keep Alive
    // tells the Client it will be disconnected "sooner than the Keep Alive
    // specified by the Client", so a client that asked for a SHORTER interval
    // keeps it. [V5:MQTT-3.1.2-21] then requires the client to use whatever we
    // state, which is why stating it and enforcing something else would be a
    // lie we told on purpose.
    connection.keepAliveSeconds = request.keepAlive;

    if (_config.serverKeepAliveMaximum != 0 &&
        (connection.keepAliveSeconds == 0 ||
         connection.keepAliveSeconds > _config.serverKeepAliveMaximum)) {
        connection.keepAliveSeconds = _config.serverKeepAliveMaximum;
        connection.keepAliveStated = true;
    }

    if (connection.keepAliveSeconds == 0) {
        connection.keepAliveSeconds = _config.defaultKeepAlive;
        if (connection.keepAliveSeconds != 0) connection.keepAliveStated = true;
    }

    // --- The client identifier ---
    MqttSpan clientId = request.clientId;
    char assigned[32] = {0};
    MqttSpan assignedSpan;
    bool didAssign = false;

    if (clientId.length == 0) {
        if (!_config.assignClientIdentifiers) {
            sendConnAck(connection,
                        isV5 ? MQ_REASON_CLIENT_IDENTIFIER_NOT_VALID
                             : MQ_RETURN_IDENTIFIER_REJECTED,
                        false, nullptr);
            _status.refused++;
            closeConnection(connection, (uint8_t)(isV5 ? MQ_REASON_CLIENT_IDENTIFIER_NOT_VALID : MQ_RETURN_IDENTIFIER_REJECTED), false, false);
            return;
        }

        // [V311:MQTT-3.1.3-8] "If the Client supplies a zero-byte ClientId
        // with CleanSession set to 0, the Server MUST respond to the CONNECT
        // Packet with a CONNACK return code 0x02 (Identifier rejected) and
        // then close the Network Connection." There is nowhere to keep a
        // session that has no name.
        if (!isV5 && !request.cleanStart) {
            sendConnAck(connection, MQ_RETURN_IDENTIFIER_REJECTED, false, nullptr);
            _status.refused++;
            closeConnection(connection, (uint8_t)(MQ_RETURN_IDENTIFIER_REJECTED), false, false);
            return;
        }

        // Per broker instance, never a process-wide counter - R5, and two
        // brokers in one test binary would otherwise hand out the same names.
        std::snprintf(assigned, sizeof assigned, "%.20s%08x",
                      _config.assignedIdPrefix ? _config.assignedIdPrefix : "auto-",
                      (unsigned)_nextAssignedId++);
        assignedSpan.data = (const uint8_t*)assigned;
        assignedSpan.length = (uint32_t)strlen(assigned);
        clientId = assignedSpan;
        didAssign = true;
    }

    // --- May they? ---
    uint8_t refusal = MQ_REASON_NOT_AUTHORIZED;
    MqttSpan noTopic;
    if (!authorise(connection, MqttBrokerAction::Connect, noTopic, 0, false,
                   clientId, request.hasUserName ? request.userName : MqttSpan(),
                   request.hasPassword ? request.password : MqttSpan(), refusal)) {
        if (!isV5) {
            // 3.1.1's value space is different and much smaller. Mapping 0x87
            // to 0x05 rather than sending it raw matters: 0x87 is "Granted
            // QoS 2" in no version at all, but a 3.1.1 client reading a byte
            // it does not recognise has no defined behaviour.
            refusal = (refusal == MQ_REASON_BAD_USER_NAME_OR_PASSWORD)
                          ? MQ_RETURN_BAD_CREDENTIALS : MQ_RETURN_NOT_AUTHORIZED;
        }
        sendConnAck(connection, refusal, false, nullptr);
        _status.refused++;
        closeConnection(connection, (uint8_t)(refusal), false, false);
        return;
    }

    // --- Session takeover ---
    //
    // [V5:MQTT-3.1.4-3] "If the ClientID represents a Client already connected
    // to the Server, the Server sends a DISCONNECT packet to the existing
    // Client with Reason Code of 0x8E (Session taken over)... and MUST close
    // the Network Connection of the existing Client."
    MqttBrokerSession* session = sessionFor(clientId);
    if (session != nullptr && session->connected) {
        MqttBrokerConnection* previous = connectionForSession(*session);
        if (previous != nullptr && previous != &connection) {
            _status.takeovers++;
            // The Will is NOT published: this is not the client vanishing, it
            // is the same client arriving again. [V5:MQTT-3.1.4-3] says the
            // existing connection is closed, and the Will belongs to the
            // session that is continuing.
            closeConnection(*previous, MQ_REASON_SESSION_TAKEN_OVER, true, false);
            session = sessionFor(clientId);   // the close may have released it
        }
    }

    // --- Clean Start ---
    bool sessionPresent = false;

    if (request.cleanStart) {
        // [V5:MQTT-3.1.2-4] "If a CONNECT packet is received with Clean Start
        // set to 1, the Client and Server MUST discard any existing Session
        // and start a new Session."
        if (session != nullptr) session->release();
        session = nullptr;
    } else if (session != nullptr && session->version != connection.version) {
        // A session created at one version cannot be resumed at the other. Its
        // subscriptions carry options 3.1.1 cannot express, and its queued
        // messages carry properties that would have to be silently dropped.
        // Discarding it and saying so is the only honest answer.
        session->release();
        session = nullptr;
    }

    // --- Is this CONNECT acceptable at all? ---
    //
    // ASKED BEFORE A SESSION IS TOUCHED, and that ordering is the whole point.
    // These two checks used to sit after the session had been adopted and
    // after will.clear() had run, which cost two separate things:
    //
    //   a refused CONNECT wiped the Will of a session it was RESUMING, and
    //
    //   a session adopted moments earlier was never released, because
    //   closeConnection() releases connection.session and that is not assigned
    //   until the CONNACK succeeds. The orphan kept _inUse set, and a 3.1.1
    //   Clean Session 0 has expiryInterval 0xFFFFFFFF so hasExpired() never
    //   returns true for it. A loop of CONNECTs with distinct client
    //   identifiers and a Will exhausted maxSessions permanently - a denial of
    //   service built entirely from valid packets.
    //
    // Neither can happen to a check that runs before anything is allocated.
    if (request.hasWill) {
        if (request.will.retain && !_config.retainAvailable) {
            // [V5:MQTT-3.1.2-11] "If the Server does not support retained
            // messages and Will Retain is set to 1 the Server MUST reject the
            // connection request."
            sendConnAck(connection,
                        isV5 ? MQ_REASON_RETAIN_NOT_SUPPORTED
                             : MQ_RETURN_SERVER_UNAVAILABLE,
                        false, nullptr);
            _status.refused++;
            closeConnection(connection, (uint8_t)(isV5 ? MQ_REASON_RETAIN_NOT_SUPPORTED : MQ_RETURN_SERVER_UNAVAILABLE), false, false);
            return;
        }
        if (request.will.qos > _config.maximumQos) {
            // [V5:MQTT-3.1.2-12] The same rule for Will QoS above Maximum QoS.
            sendConnAck(connection,
                        isV5 ? MQ_REASON_QOS_NOT_SUPPORTED
                             : MQ_RETURN_SERVER_UNAVAILABLE,
                        false, nullptr);
            _status.refused++;
            closeConnection(connection, (uint8_t)(isV5 ? MQ_REASON_QOS_NOT_SUPPORTED : MQ_RETURN_SERVER_UNAVAILABLE), false, false);
            return;
        }
    }

    if (session == nullptr) {
        session = freeSession();
        if (session == nullptr) {
            sendConnAck(connection,
                        isV5 ? MQ_REASON_SERVER_BUSY : MQ_RETURN_SERVER_UNAVAILABLE,
                        false, nullptr);
            _status.refused++;
            closeConnection(connection, (uint8_t)(isV5 ? MQ_REASON_SERVER_BUSY : MQ_RETURN_SERVER_UNAVAILABLE), false, false);
            return;
        }
        session->adopt(clientId);
        session->version = connection.version;
    } else {
        // [V5:MQTT-3.2.2-2] "If the Server accepts a connection with Clean
        // Start set to 0 and the Server has Session State for the ClientID, it
        // MUST set Session Present to 1 in the CONNACK packet."
        sessionPresent = true;

        // Arriving inside the Will Delay Interval cancels the Will
        // [V5:MQTT-3.1.2-8]. This is the case the delay exists for: a device
        // that rebooted and came back before anybody needed to be told.
        session->will.scheduled = false;
    }

    // --- Session expiry ---
    if (isV5) {
        const uint32_t asked = request.properties.has(MQ_PROP_SESSION_EXPIRY_INTERVAL)
                                 ? request.properties.sessionExpiryInterval : 0;
        uint32_t requested = asked;
        if (requested > _config.maximumSessionExpiry &&
            _config.maximumSessionExpiry != 0xFFFFFFFFu) {
            // [V5:3.2.2.3.2] A Server that revises the interval MUST state the
            // revised value in CONNACK. Silently shortening it would leave the
            // client expecting its queue to survive an outage it will not.
            requested = _config.maximumSessionExpiry;
        }
        session->expiryInterval = requested;
        connection.sessionExpiryRevised = requested != asked;
    } else {
        // 3.1.1 has two behaviours and no interval: Clean Session 1 discards
        // at close, Clean Session 0 keeps indefinitely [V311:3.1.2.4].
        session->expiryInterval = request.cleanStart ? 0 : 0xFFFFFFFFu;
    }

    // --- The Will ---
    session->will.clear();
    if (request.hasWill) {
        // [V5:MQTT-3.1.2-9] requires a Will Topic and Will Payload in the
        // payload when the Will Flag is set; the codec has already enforced
        // that, so this is a copy rather than a check.
        session->will.present = true;
        session->will.topic.assign((const char*)request.will.topic.data,
                                   request.will.topic.length);
        session->will.payload.assign(
            request.will.payload.data,
            request.will.payload.data + request.will.payload.length);
        session->will.qos = request.will.qos;
        session->will.retain = request.will.retain;
        session->will.properties.copyForForwarding(request.will.properties);
        session->will.delayInterval =
            isV5 ? request.will.properties.willDelayInterval : 0;
    }

    session->connected = true;
    session->userName.assign(request.hasUserName ? (const char*)request.userName.data : "",
                             request.hasUserName ? request.userName.length : 0);
    session->connectionSlot = (uint32_t)(&connection - _connections.data());
#if defined(MQTT_PLATFORM_HOST)
    // Back before its deadlines: the Will is cancelled by the reconnect
    // [V5:MQTT-3.1.2-8], and an attached session does not expire.
    _wheel.cancel(session->willTimer);
    _wheel.cancel(session->expiryTimer);
#endif
    connection.session = session;

    if (!sendConnAck(connection, isV5 ? MQ_REASON_SUCCESS : MQ_RETURN_ACCEPTED,
                     sessionPresent, didAssign ? &assignedSpan : nullptr)) {
        // THE CONNACK DID NOT GO, SO THERE IS NO CONNECTION.
        //
        // This used to run on regardless: connected was set, the session was
        // attached and _status.connected went up, for a client that never
        // heard it was accepted and was still waiting for its CONNACK. The
        // broker's own count of connected clients included one that was not.
        closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, false);
        return;
    }
    _status.connected++;

    if (_onConnection) {
        _onConnection(_connectionContext, asSpan(session->clientId()), true,
                      MQ_REASON_SUCCESS);
    }

    // A resumed session may have messages waiting from while the client was
    // away, and a half-finished QoS 2 exchange to complete [V5:4.4].
    if (sessionPresent) {
        session->markAllUnsent();
        deliverQueued(connection);
    }
}

// =============================================================================
// ROUTING
// =============================================================================

uint32_t MqttBroker::nextInGroup(const std::string& key, uint32_t members) {
    if (members == 0) return 0;
    for (ShareCursor& cursor : _shareCursors) {
        if (cursor.key == key) {
            const uint32_t chosen = cursor.next % members;
            cursor.next = chosen + 1;
            return chosen;
        }
    }
    ShareCursor cursor;
    cursor.key = key;
    cursor.next = 1;
    _shareCursors.push_back(cursor);
    return 0;
}

namespace {

// FNV-1a. Cheap, no table, and good enough for a bucket index - a collision
// costs one wasted mq_topicMatches() and nothing else.
uint32_t hashBytes(const uint8_t* data, size_t length) {
    uint32_t hash = 2166136261u;
    for (size_t index = 0; index < length; index++) {
        hash ^= data[index];
        hash *= 16777619u;
    }
    return hash;
}

bool filterHasWildcard(const std::string& filter) {
    for (size_t index = 0; index < filter.size(); index++) {
        if (filter[index] == '+' || filter[index] == '#') return true;
    }
    return false;
}

} // namespace

void MqttBroker::rebuildSubscriptionIndex() {
    _status.indexRebuilds++;
    const size_t buckets = _bucketStart.size() > 0 ? _bucketStart.size() - 1 : 0;
    if (buckets == 0) return;
    const uint32_t mask = (uint32_t)(buckets - 1);

    // COUNT, then FILL. Two passes over the subscriptions to lay them out
    // contiguously, which is what keeps the message path free of allocation.
    for (size_t index = 0; index < _bucketStart.size(); index++) _bucketStart[index] = 0;
    _wildcardSessions.clear();

    for (size_t si = 0; si < _sessions.size(); si++) {
        const MqttBrokerSession& session = _sessions[si];
        if (!session.inUse()) continue;
        bool anyWildcard = false;
        for (size_t index = 0; index < session.subscriptionCount(); index++) {
            const MqttBrokerSubscription* sub = session.subscriptionAt(index);
            if (sub == nullptr || !sub->inUse()) continue;
            if (filterHasWildcard(sub->effective)) { anyWildcard = true; continue; }
            const uint32_t bucket =
                hashBytes((const uint8_t*)sub->effective.data(), sub->effective.size()) & mask;
            _bucketStart[bucket + 1]++;
        }
        if (anyWildcard) _wildcardSessions.push_back((uint32_t)si);
    }

    for (size_t index = 1; index < _bucketStart.size(); index++) {
        _bucketStart[index] += _bucketStart[index - 1];
    }
    _bucketSessions.assign(_bucketStart.back(), 0);

    std::vector<uint32_t> cursor(_bucketStart.begin(), _bucketStart.end());
    for (size_t si = 0; si < _sessions.size(); si++) {
        const MqttBrokerSession& session = _sessions[si];
        if (!session.inUse()) continue;
        for (size_t index = 0; index < session.subscriptionCount(); index++) {
            const MqttBrokerSubscription* sub = session.subscriptionAt(index);
            if (sub == nullptr || !sub->inUse()) continue;
            if (filterHasWildcard(sub->effective)) continue;
            const uint32_t bucket =
                hashBytes((const uint8_t*)sub->effective.data(), sub->effective.size()) & mask;
            _bucketSessions[cursor[bucket]++] = (uint32_t)si;
        }
    }

    pruneShareCursors();

    _indexBuiltAt = _subscriptionRevision;
}

void MqttBroker::pruneShareCursors() {
    // SHARE CURSORS THAT NO LONGER NAME A GROUP ARE DROPPED HERE.
    //
    // A cursor is created for every "$share/<name>/<filter>" seen and nothing
    // ever removed one - not unsubscribe, not session release, not end(). A
    // client using a fresh group name per instance, which is the ordinary
    // pattern for a worker pool, added a permanent heap entry every time; and
    // because nextInGroup() is a linear scan with a string compare, the
    // routing cost only ever went up. It is unbounded growth driven entirely
    // by what a peer chooses to call itself.
    //
    // Run when subscriptions have changed, which is exactly when a group may
    // have gone away: from the rebuild on a board, and from route() - at most
    // once a second - where the index is kept incrementally instead.
    if (!_shareCursors.empty()) {
        size_t kept = 0;
        for (size_t index = 0; index < _shareCursors.size(); index++) {
            bool stillUsed = false;
            for (size_t si = 0; si < _sessions.size() && !stillUsed; si++) {
                const MqttBrokerSession& session = _sessions[si];
                if (!session.inUse()) continue;
                for (size_t k = 0; k < session.subscriptionCount(); k++) {
                    const MqttBrokerSubscription* sub = session.subscriptionAt(k);
                    if (sub == nullptr || !sub->inUse() || !sub->shared()) continue;
                    std::string key = sub->shareName;
                    key.push_back('\0');
                    key += sub->effective;
                    if (key == _shareCursors[index].key) { stillUsed = true; break; }
                }
            }
            if (stillUsed) {
                if (kept != index) _shareCursors[kept] = _shareCursors[index];
                kept++;
            }
        }
        _shareCursors.resize(kept);
    }

#if defined(MQTT_PLATFORM_HOST)
    _sharePrunedRevision = _subscriptionRevision;
    _sharePrunedAt = mq_millis();
#endif
}

// The sessions worth looking at for this topic, each listed once.
void MqttBroker::gatherCandidates(const MqttSpan& topic) {
    _routeCandidates.clear();
    if (_routeMark.size() != _sessions.size()) _routeMark.assign(_sessions.size(), 0);

#if defined(MQTT_PLATFORM_HOST)
    if (_routeIndex.configured()) {
        if (++_routeGeneration == 0) {
            for (size_t index = 0; index < _routeMark.size(); index++) _routeMark[index] = 0;
            _routeGeneration = 1;
        }
        _routeScratch.clear();
        _routeIndex.candidates(topic, _routeScratch);
        for (uint32_t si : _routeScratch) {
            if (si >= _routeMark.size() || _routeMark[si] == _routeGeneration) continue;
            _routeMark[si] = _routeGeneration;
            _routeCandidates.push_back(si);
        }
        return;
    }
#endif

    const size_t buckets = _bucketStart.size() > 0 ? _bucketStart.size() - 1 : 0;
    if (buckets == 0) return;

    // A GENERATION STAMP THAT WRAPS IS A STAMP THAT LIES. At 0 every mark
    // would read as "already visited this round" and the session would be
    // skipped - one message not delivered, every four billion routes, which is
    // about ten hours at the rates measured here. Cleared rather than risked.
    if (++_routeGeneration == 0) {
        for (size_t index = 0; index < _routeMark.size(); index++) _routeMark[index] = 0;
        _routeGeneration = 1;
    }

    const uint32_t mask = (uint32_t)(buckets - 1);
    const uint32_t bucket = hashBytes(topic.data, topic.length) & mask;
    for (uint32_t index = _bucketStart[bucket]; index < _bucketStart[bucket + 1]; index++) {
        const uint32_t si = _bucketSessions[index];
        if (_routeMark[si] == _routeGeneration) continue;   // one entry per session
        _routeMark[si] = _routeGeneration;
        _routeCandidates.push_back(si);
    }
    for (size_t index = 0; index < _wildcardSessions.size(); index++) {
        const uint32_t si = _wildcardSessions[index];
        if (_routeMark[si] == _routeGeneration) continue;
        _routeMark[si] = _routeGeneration;
        _routeCandidates.push_back(si);
    }
}

size_t MqttBroker::route(const MqttSpan& topic, const MqttSpan& payload,
                         uint8_t qos, bool retain,
                         const MqttProperties& properties,
                         const MqttBrokerSession* from, size_t* droppedOut) {
    size_t matched = 0;
    const mq_time_t now = mq_millis();

    // Shared subscriptions are collected first and resolved afterwards,
    // because the choice is across sessions: exactly one member of a group
    // receives each message, and that cannot be decided while walking one
    // session at a time.
    struct ShareCandidate {
        MqttBrokerSession* session;
        const MqttBrokerSubscription* subscription;
    };
    struct ShareGroup {
        std::string key;
        std::vector<ShareCandidate> members;
    };
    std::vector<ShareGroup> groups;

    // ONLY THE SESSIONS THAT COULD MATCH. Everything inside this loop is
    // exactly as it was - including the mq_topicMatches() check, which is what
    // makes a merely-conservative index safe.
#if defined(MQTT_PLATFORM_HOST)
    if (_routeIndex.configured()) {
        // Nothing to rebuild - the index is kept as subscriptions change.
        // Shared-group cursors are still pruned when groups may have gone,
        // which the rebuild used to do as a side effect.
        if (!_shareCursors.empty() && _subscriptionRevision != _sharePrunedRevision &&
            mq_elapsed(_sharePrunedAt, now) >= 1000) {
            pruneShareCursors();
        }
    } else if (_indexBuiltAt != _subscriptionRevision) {
        rebuildSubscriptionIndex();
    }
#else
    if (_indexBuiltAt != _subscriptionRevision) rebuildSubscriptionIndex();
#endif
    gatherCandidates(topic);

    for (size_t candidate = 0; candidate < _routeCandidates.size(); candidate++) {
        MqttBrokerSession& session = _sessions[_routeCandidates[candidate]];
        if (!session.inUse()) continue;

        // One copy per session, carrying the highest QoS of the matching
        // subscriptions [V5:MQTT-3.3.4-2] and the identifiers of all of them
        // [V5:3.3.4]. The alternative the standard also allows - one copy per
        // subscription - delivers the same message twice to a client that
        // subscribed to both "plant/#" and "plant/+/temp", which is never what
        // the person who wrote those two filters wanted.
        int bestQos = -1;
        const MqttBrokerSubscription* best = nullptr;
        uint32_t ids[MQTT_MAX_SUBSCRIPTION_IDS] = {0};
        uint8_t idCount = 0;

        for (size_t index = 0; index < session.subscriptionCount(); index++) {
            const MqttBrokerSubscription* subscription = session.subscriptionAt(index);
            if (subscription == nullptr || !subscription->inUse()) continue;

            MqttSpan effective = asSpan(subscription->effective);
            if (!mq_topicMatches(effective, topic)) continue;

            // [V5:3.8.3.1] No Local: "Application Messages MUST NOT be
            // forwarded to a connection with a ClientID equal to the ClientID
            // of the publishing connection." Per subscription, not per
            // session - a client with one No Local subscription and one
            // ordinary one still gets its own message through the second.
            if (subscription->noLocal() && from == &session) continue;

            if (subscription->shared()) {
                std::string key = subscription->shareName;
                key.push_back('\0');
                key += subscription->effective;

                ShareGroup* group = nullptr;
                for (ShareGroup& candidate : groups) {
                    if (candidate.key == key) { group = &candidate; break; }
                }
                if (group == nullptr) {
                    ShareGroup fresh;
                    fresh.key = key;
                    groups.push_back(fresh);
                    group = &groups.back();
                }
                ShareCandidate member;
                member.session = &session;
                member.subscription = subscription;
                group->members.push_back(member);
                continue;
            }

            if ((int)subscription->qos() > bestQos) {
                bestQos = (int)subscription->qos();
                best = subscription;
            }
            if (subscription->subscriptionId != 0 &&
                idCount < MQTT_MAX_SUBSCRIPTION_IDS) {
                ids[idCount++] = subscription->subscriptionId;
            }
        }

        if (best == nullptr) continue;

        // THE SLOT FIRST, THEN FILL IT.
        //
        // This built a MqttBrokerDelivery on the stack and handed it to
        // enqueue(), which copy-assigned the whole thing into a queue slot -
        // two copies of topic and payload per subscriber, with the temporary's
        // buffers allocated and freed each time round. And on a subscriber
        // whose queue was already full, all of that was done in full and then
        // thrown away.
        //
        // Reserving first makes a full queue free, and filling the slot in
        // place makes it one copy into a buffer the slot already owns. A
        // refused reservation is still counted as matched-and-dropped exactly
        // as before, so what the publisher is told does not change.
        MqttBrokerDelivery* slot = session.reserveDelivery();
        if (slot == nullptr) {
            matched++;
            _status.dropped++;
            if (droppedOut) (*droppedOut)++;
            continue;
        }
        MqttBrokerDelivery& delivery = *slot;
        delivery.topic.assign((const char*)topic.data, topic.length);
        if (payload.length > 0) {
            delivery.payload.assign(payload.data, payload.data + payload.length);
        }

        // [V5:MQTT-3.8.4-8] The QoS of the delivered message is the LOWER of
        // what was published and what was granted. Sending at a higher QoS
        // than the subscriber asked for would put acknowledgements on a client
        // that never agreed to handle them.
        delivery.qos = qos < (uint8_t)bestQos ? qos : (uint8_t)bestQos;

        // [V5:MQTT-3.3.1-9] "It MUST set the RETAIN flag to 0 when a PUBLISH
        // packet is sent to a Client because it matches an established
        // subscription regardless of how the flag was set in the message it
        // received" - unless Retain As Published is set [V5:MQTT-3.3.1-13].
        delivery.retain = best->retainAsPublished() ? retain : false;

        delivery.properties.copyForForwarding(properties);
        delivery.subscriptionIdCount = idCount;
        for (uint8_t index = 0; index < idCount; index++) {
            delivery.subscriptionIds[index] = ids[index];
        }
        delivery.queuedAt = now;

        // Counted as matched EVEN WHEN THE QUEUE REFUSES IT. The count
        // decides whether the publisher hears 0x10 No Matching Subscribers,
        // and "your message was dropped because a subscriber is too far
        // behind" is a completely different fault from "nobody is listening".
        // Reporting the second for the first would send somebody to check
        // their topic spelling for an afternoon.
        matched++;
        session.commitDelivery(delivery);
#if defined(MQTT_PLATFORM_HOST)
        wake(session);
#endif
    }

    for (ShareGroup& group : groups) {
        if (group.members.empty()) continue;
#if defined(MQTT_PLATFORM_HOST)
        // Several brokers, one server: the group's one copy may belong to
        // another broker's member this time. The key is "name\0filter".
        if (_shareGate) {
            const size_t nul = group.key.find('\0');
            MqttSpan name, filter;
            name.data = (const uint8_t*)group.key.data();
            name.length = (uint32_t)nul;
            filter.data = (const uint8_t*)group.key.data() + nul + 1;
            filter.length = (uint32_t)(group.key.size() - nul - 1);
            if (!_shareGate(_shareGateContext, name, filter)) continue;
        }
#endif
        const uint32_t chosen = nextInGroup(group.key, (uint32_t)group.members.size());
        ShareCandidate& member = group.members[chosen];

        // Filled in place, for the same reason as the ordinary path above.
        MqttBrokerDelivery* slot = member.session->reserveDelivery();
        if (slot == nullptr) {
            matched++;
            _status.dropped++;
            if (droppedOut) (*droppedOut)++;
            continue;
        }
        MqttBrokerDelivery& delivery = *slot;
        delivery.topic.assign((const char*)topic.data, topic.length);
        if (payload.length > 0) {
            delivery.payload.assign(payload.data, payload.data + payload.length);
        }
        delivery.qos = qos < member.subscription->qos() ? qos : member.subscription->qos();
        delivery.retain = member.subscription->retainAsPublished() ? retain : false;
        delivery.properties.copyForForwarding(properties);
        if (member.subscription->subscriptionId != 0) {
            delivery.subscriptionIds[0] = member.subscription->subscriptionId;
            delivery.subscriptionIdCount = 1;
        }
        delivery.queuedAt = now;

        matched++;
        member.session->commitDelivery(delivery);
#if defined(MQTT_PLATFORM_HOST)
        wake(*member.session);
#endif
    }

    return matched;
}

// =============================================================================
// DELIVERY
// =============================================================================

void MqttBroker::deliverQueued(MqttBrokerConnection& connection) {
    MqttBrokerSession* session = connection.session;
    if (session == nullptr || !connection.inUse() || connection.closing) return;

    const mq_time_t now = mq_millis();
    const bool isV5 = connection.version == MqttVersion::V5;

    // Retained messages owed to a new subscription, as far as there is room.
    continueRetained(*session);

    // Bounded per pass. A session holding sixty messages must not monopolise
    // the poll: every other connection in the plant is waiting behind it.
    for (size_t pass = 0; pass < 32; pass++) {
        // Nothing new goes out while the transmit queue still holds bytes.
        // Encoding a second packet into a full queue would fail halfway and
        // leave the delivery marked sent when it was not.
        if (connection.link.transmitPending() != 0) {
            connection.link.serviceTransmit();
            if (connection.link.transmitPending() != 0) return;
        }

        // A half-finished QoS 2 exchange owes a PUBREL, and that comes before
        // anything new: the peer is holding a packet identifier open until it
        // arrives. [V5:4.3.3]
        MqttBrokerDelivery* owed = nullptr;
        // queuedExtent(), not queueCapacity(): this runs first on every one of
        // up to 32 passes, before it is even known whether there is anything
        // to send, so at the default depth it was up to 2,048 slot visits per
        // connection per poll - almost all of them on empty slots.
        for (size_t index = 0; index < session->queuedExtent() && owed == nullptr; index++) {
            MqttBrokerDelivery* candidate = session->deliveryAt(index);
            if (candidate && candidate->inUse() && candidate->pubrelOwed) owed = candidate;
        }
        if (owed != nullptr) {
            MqttPacket pubrel;
            pubrel.header.type = MQ_PKT_PUBREL;
            pubrel.ack.packetId = owed->packetId;
            if (!send(connection, pubrel)) return;
            owed->pubrelOwed = false;
            continue;
        }

        // [V5:MQTT-4.9.0-2] At zero quota no further QoS > 0 may be sent - so
        // ask for the oldest entry that CAN go, rather than the oldest entry
        // and then giving up on finding it is a QoS 1.
        MqttBrokerDelivery* next =
            session->nextUnsent(connection.toPeer.canSendQos12());
        if (next == nullptr) {
            // Sending QoS 0 frees its slot at once, so a walk that stopped on
            // a full queue may now go on.
            if (continueRetained(*session)) continue;
            return;
        }

        // [V5:3.3.2.3.3] A message whose expiry has elapsed while it waited is
        // not delivered at all. Delivering it would hand a control system a
        // setpoint its own publisher had already declared stale.
        if (next->properties.hasExpiry) {
            const uint32_t waited = (uint32_t)(mq_elapsed(next->queuedAt, now) / 1000u);
            if (waited >= next->properties.expiryInterval) {
                next->release();
                _status.dropped++;
                continue;
            }
        }


        uint16_t packetId = MQ_PACKET_ID_NONE;
        if (next->qos > MQ_QOS_0) {
            packetId = next->packetId != MQ_PACKET_ID_NONE
                           ? next->packetId : session->allocatePacketId();
            if (packetId == MQ_PACKET_ID_NONE) return;   // try again next poll
        }

        MqttPacket packet;
        packet.header.type = MQ_PKT_PUBLISH;
        packet.publish.qos = next->qos;
        packet.publish.retain = next->retain;
        packet.publish.packetId = packetId;
        packet.publish.topic = asSpan(next->topic);
        packet.publish.payload = asSpan(next->payload);

        // [V5:MQTT-3.3.1-1] The DUP flag says this is a redelivery. It is set
        // only when this exact message has already been on the wire once - a
        // message queued while the client was offline is NOT a redelivery,
        // however long it waited.
        packet.publish.dup = next->packetId != MQ_PACKET_ID_NONE && next->sent;

        if (isV5) {
            restoreProperties(next->properties, packet.publish.properties);

            if (next->properties.hasExpiry) {
                const uint32_t waited =
                    (uint32_t)(mq_elapsed(next->queuedAt, now) / 1000u);
                packet.publish.properties.messageExpiryInterval =
                    next->properties.expiryInterval - waited;
                packet.publish.properties.markPresent(MQ_PROP_MESSAGE_EXPIRY_INTERVAL);
            }

            // [V5:3.3.4] The identifiers of every matching subscription travel
            // with the single copy, so a client that used them can tell which
            // of its own subscriptions produced this message.
            if (_config.subscriptionIdentifiersAvailable) {
                for (uint8_t index = 0; index < next->subscriptionIdCount &&
                                        index < MQTT_MAX_SUBSCRIPTION_IDS; index++) {
                    packet.publish.properties.subscriptionIds[index] =
                        next->subscriptionIds[index];
                    packet.publish.properties.subscriptionIdCount = (uint8_t)(index + 1);
                }
                if (next->subscriptionIdCount > 0) {
                    packet.publish.properties.markPresent(MQ_PROP_SUBSCRIPTION_IDENTIFIER);
                }
            }

            if (_config.useOutboundTopicAliases && connection.peerTopicAliasMaximum > 0) {
                const MqttSpan topicSpan = asSpan(next->topic);
                uint16_t alias = connection.aliases.lookupOutbound(
                    topicSpan.data, (uint16_t)topicSpan.length);
                if (alias != 0) {
                    // [V5:3.3.2.3.4] Once a mapping is established the topic
                    // may be sent zero-length. The alias is only ever used
                    // AFTER the full topic has gone once on this connection.
                    packet.publish.topic.length = 0;
                    packet.publish.properties.topicAlias = alias;
                    packet.publish.properties.markPresent(MQ_PROP_TOPIC_ALIAS);
                } else {
                    // false: lookupOutbound() has just missed on this exact
                    // topic, so searching again would be the same walk twice.
                    alias = connection.aliases.assignOutbound(
                        topicSpan.data, (uint16_t)topicSpan.length, false);
                    if (alias != 0) {
                        packet.publish.properties.topicAlias = alias;
                        packet.publish.properties.markPresent(MQ_PROP_TOPIC_ALIAS);
                    }
                }
            }
        }

        if (!send(connection, packet)) {
            // Three reasons, and only one of them is worth waiting out.
            //
            //   - the client said it would not accept a packet this large
            //     [V5:MQTT-3.2.2-15]
            //   - OUR OWN transmit buffer could never hold it
            //   - the transmit buffer has other bytes in it at this instant
            //
            // The first two cannot be fixed by waiting. Retrying either one
            // holds the head of this client's delivery queue for ever, so it
            // stops receiving anything at all - on a healthy connection, with
            // nothing reported but a rising `dropped`.
            const size_t needed = codecFor(connection.version).size(packet);

            const bool refusedByPeer =
                connection.peerMaximumPacketSize != 0 &&
                (needed == 0 || needed > connection.peerMaximumPacketSize);

            // An inbound packet is bounded by our receive buffer, but the
            // outbound copy is not the same size: subscription identifiers, a
            // recalculated expiry and a topic alias are all added on the way
            // out. A message that arrived just inside the limit can leave just
            // outside it.
            const bool tooBigForUs = needed == 0 ||
                                     needed > connection.link.transmitCapacity();

            if (refusedByPeer || tooBigForUs) {
                // The message is being DISCARDED, so its identifier goes back
                // whether this attempt allocated it or a previous one did -
                // there is nothing left to acknowledge it.
                if (packetId != MQ_PACKET_ID_NONE) session->releasePacketId(packetId);
                next->release();
                _status.dropped++;
                continue;
            }

            // Transient, and the message KEEPS ITS PLACE - so only an
            // identifier this attempt allocated goes back. A redelivery's
            // belongs to the stored message and is still awaiting its
            // acknowledgement; returning that one would let a second message
            // be sent under the same number.
            if (packetId != MQ_PACKET_ID_NONE && next->packetId == MQ_PACKET_ID_NONE) {
                session->releasePacketId(packetId);
            }
            return;
        }

        _status.delivered++;
        session->messagesDelivered++;

        if (next->qos == MQ_QOS_0) {
            // Nothing acknowledges a QoS 0 delivery, so reaching the wire is
            // its conclusion.
            next->release();
        } else {
            next->packetId = packetId;
            next->sent = true;
            connection.toPeer.takeOne();
        }
    }
}

// =============================================================================
// DISPATCH
// =============================================================================

void MqttBroker::handlePacket(MqttBrokerConnection& connection,
                              const MqttFixedHeader& header,
                              const MqttPacket& packet) {
    // [V5:MQTT-3.1.0-1] "After a Network Connection is established by a Client
    // to a Server, the first packet sent from the Client to the Server MUST be
    // a CONNECT packet." Anything else is a Protocol Error, and answering it
    // would let an unauthenticated peer use the broker.
    if (!connection.connectSeen && header.type != MQ_PKT_CONNECT) {
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, false, false);
        return;
    }

    switch (header.type) {
    case MQ_PKT_CONNECT:
        handleConnect(connection, packet);
        return;

    case MQ_PKT_PUBLISH:
        handlePublish(connection, packet);
        return;

    case MQ_PKT_PUBACK:
    case MQ_PKT_PUBREC:
    case MQ_PKT_PUBREL:
    case MQ_PKT_PUBCOMP:
        handleAck(connection, header, packet);
        return;

    case MQ_PKT_SUBSCRIBE:
        handleSubscribe(connection, packet);
        return;

    case MQ_PKT_UNSUBSCRIBE:
        handleUnsubscribe(connection, packet);
        return;

    case MQ_PKT_PINGREQ: {
        // [V5:MQTT-3.12.4-1] "The Server MUST send a PINGRESP packet in
        // response to a PINGREQ packet."
        MqttPacket response;
        response.header.type = MQ_PKT_PINGRESP;
        // A lost PINGRESP starts the CLIENT's keepalive watchdog against a
        // broker that thinks everything is fine, and it will disconnect on a
        // timer with no idea why.
        if (!send(connection, response)) {
            closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
            return;
        }
        return;
    }

    case MQ_PKT_DISCONNECT:
        handleDisconnect(connection, packet);
        return;

    case MQ_PKT_AUTH:
        // No enhanced authentication mechanism ships. [V5:4.12] makes an AUTH
        // outside an agreed exchange a Protocol Error, and there is no agreed
        // exchange because CONNECT refuses every Authentication Method.
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, true, true);
        return;

    default:
        // CONNACK, SUBACK, UNSUBACK, PINGRESP arriving FROM a client. The
        // codec decoded them perfectly well - R2 means it decodes everything
        // in both directions - but a client has no business sending one.
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, true, true);
        return;
    }
}

// =============================================================================
// PUBLISH, FROM A CLIENT
// =============================================================================

void MqttBroker::handlePublish(MqttBrokerConnection& connection,
                               const MqttPacket& packet) {
    const MqttPublish& message = packet.publish;
    MqttBrokerSession* session = connection.session;
    if (session == nullptr) return;

    const bool isV5 = connection.version == MqttVersion::V5;
    MqttSpan topic = message.topic;

    // --- Topic aliases, inbound [V5:3.3.2.3.4] ---
    if (isV5 && message.properties.has(MQ_PROP_TOPIC_ALIAS)) {
        const uint16_t alias = message.properties.topicAlias;

        // "A sender MUST NOT send a PUBLISH packet with a Topic Alias greater
        // than the Topic Alias Maximum value returned by the receiver"
        // [V5:MQTT-3.3.2-9]. Zero is never valid [V5:MQTT-3.3.2-8].
        if (!connection.aliases.inboundAllowed(alias)) {
            _status.protocolErrors++;
            closeConnection(connection, MQ_REASON_TOPIC_ALIAS_INVALID, true, true);
            return;
        }

        if (topic.length > 0) {
            // A topic WITH an alias sets the mapping for the rest of this
            // connection.
            connection.aliases.rememberInbound(alias, topic.data,
                                               (uint16_t)topic.length);
        } else {
            const uint8_t* mapped = nullptr;
            uint16_t mappedLength = 0;
            if (!connection.aliases.lookupInbound(alias, mapped, mappedLength)) {
                // An alias used before it was ever established. There is no
                // way to guess what it meant, and guessing would deliver a
                // message to the wrong topic.
                _status.protocolErrors++;
                closeConnection(connection, MQ_REASON_TOPIC_ALIAS_INVALID, true, true);
                return;
            }
            topic.data = mapped;
            topic.length = mappedLength;
        }
    }

    // [V5:MQTT-3.3.2-1] The Topic Name must be present, and must not contain a
    // wildcard [V5:MQTT-3.3.2-2].
    if (topic.length == 0 || !mq_topicNameIsValid(topic)) {
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_TOPIC_NAME_INVALID, true, true);
        return;
    }

    // [V5:MQTT-3.2.2-11] "If a Server receives a PUBLISH packet with a QoS
    // greater than the Maximum QoS it specified, it MUST ... disconnect with
    // Reason Code 0x9B (QoS not supported)."
    if (message.qos > _config.maximumQos) {
        closeConnection(connection, MQ_REASON_QOS_NOT_SUPPORTED, true, true);
        return;
    }

    // [V5:MQTT-3.2.2-14] The same rule for a retained message when Retain
    // Available is 0.
    if (message.retain && !_config.retainAvailable) {
        closeConnection(connection, MQ_REASON_RETAIN_NOT_SUPPORTED, true, true);
        return;
    }

    // [V5:MQTT-3.3.4-7] / [V5:4.9] Our own Receive Maximum. Exceeding it is
    // 0x93, and it is the only defence against one client with a runaway loop
    // filling the broker's memory.
    if (message.qos > MQ_QOS_0 && isV5 &&
        connection.inFlightFromPeer >= _config.receiveMaximum) {
        closeConnection(connection, MQ_REASON_RECEIVE_MAXIMUM_EXCEEDED, true, true);
        return;
    }

    const uint8_t ackType = message.qos == MQ_QOS_1 ? MQ_PKT_PUBACK : MQ_PKT_PUBREC;

    // [V5:3.3.2.3.2] "The Server MAY validate that the Payload is of the form
    // indicated ... If the Payload is not [valid UTF-8] it can send a PUBACK
    // or PUBREC with Reason Code 0x99." It does validate: a payload that
    // claims to be text and is not will be read as text by something
    // downstream, and that something is usually a dashboard.
    if (isV5 && message.properties.has(MQ_PROP_PAYLOAD_FORMAT_INDICATOR) &&
        message.properties.payloadFormatIndicator == 1) {
        if (mq_validateUtf8(message.payload.data, message.payload.length)
                != MqttError::None) {
            if (message.qos == MQ_QOS_0) {
                closeConnection(connection, MQ_REASON_PAYLOAD_FORMAT_INVALID, true, true);
            } else {
                sendSimpleAck(connection, ackType, message.packetId,
                              MQ_REASON_PAYLOAD_FORMAT_INVALID);
            }
            return;
        }
    }

    // --- May they publish here? ---
    uint8_t refusal = MQ_REASON_NOT_AUTHORIZED;
    MqttSpan none;
    if (!authorise(connection, MqttBrokerAction::Publish, topic, message.qos,
                   message.retain, asSpan(session->clientId()), asSpan(session->userName),
                   none, refusal)) {
        if (message.qos > MQ_QOS_0) {
            sendSimpleAck(connection, ackType, message.packetId, refusal);
        }
        // At QoS 0 there is nothing to answer with, and dropping the
        // connection over one refused topic would take out everything else
        // that client is doing. The refusal is counted, not shouted about.
        return;
    }

    _status.published++;

    // --- QoS 2, method A [V5:4.3.3] ---
    //
    // The identifier is recorded and the message forwarded ONCE. A repeat
    // carrying the same identifier is acknowledged and not forwarded again,
    // which is what "exactly once" means from this side.
    bool forward = true;
    bool countedInFlight = false;
    if (message.qos == MQ_QOS_2) {
        if (session->inboundKnown(message.packetId)) {
            // A DUP RETRANSMISSION IS NOT A NEW MESSAGE IN FLIGHT.
            //
            // The peer is required to resend when its PUBREC is lost or slow,
            // which is exactly what happens when this broker is loaded and its
            // acknowledgements are queued behind other traffic. Counting each
            // retransmission - while only PUBREL ever decrements - walked the
            // counter up to receiveMaximum and then disconnected a perfectly
            // conforming client with 0x93 and published its Will.
            forward = false;
        } else if (!session->rememberInbound(message.packetId)) {
            // No room to remember it, so exactly-once cannot be promised.
            // Saying so is better than forwarding it twice later.
            sendSimpleAck(connection, MQ_PKT_PUBREC, message.packetId,
                          MQ_REASON_QUOTA_EXCEEDED);
            return;
        }
        if (forward) {
            connection.inFlightFromPeer++;
            countedInFlight = true;
        }
    } else if (message.qos == MQ_QOS_1) {
        connection.inFlightFromPeer++;
        countedInFlight = true;
    }
    (void)countedInFlight;

    size_t matched = 0;
    size_t refusedByQueue = 0;
    bool retainedStoreFull = false;
    if (forward) {
        // [V5:MQTT-3.3.1-5] A retained PUBLISH replaces whatever was stored;
        // [V5:MQTT-3.3.1-6] a zero-length payload clears it. Both happen
        // inside store(), and either way the message is still DELIVERED to
        // whoever is subscribed right now [V5:MQTT-3.3.1-7].
        if (message.retain) {
            // A FULL RETAINED STORE IS NOT A SUCCESSFUL PUBLISH. The return
            // was discarded here, so a broker that had run out of retained
            // slots answered 0x00 and the publisher believed its setpoint had
            // been kept for the next subscriber to arrive. It had not.
            retainedStoreFull = !_retained.store(topic, message.payload,
                                                 message.qos, message.properties,
                                                 mq_millis());
        }

        matched = route(topic, message.payload, message.qos, message.retain,
                        message.properties, session, &refusedByQueue);

        if (_onTap) {
            _onTap(_tapContext, asSpan(session->clientId()), topic,
                   message.payload, message.qos, message.retain);
        }

#if defined(MQTT_PLATFORM_HOST)
        // Onward, to the other brokers of a multi-core server. At QoS 1 and
        // 2 the acknowledgement can wait for them, so that 0x10 and 0x97 mean
        // what they say across every broker and not just this one.
        if (_onEgress) {
            const uint64_t deferral =
                message.qos > MQ_QOS_0
                    ? openDeferred(connection, message.packetId, message.qos, matched,
                                   refusedByQueue, retainedStoreFull)
                    : 0;
            const uint32_t parties =
                egress(topic, message.payload, message.qos, message.retain,
                       message.properties, asSpan(session->clientId()), matched,
                       refusedByQueue, deferral);
            if (deferral != 0) {
                if (parties == 0) {
                    closeDeferred(deferral);
                } else {
                    expectParties(deferral, parties);   // may send it now
                    return;                              // otherwise later
                }
            }
        }
#endif
    }

    // --- Acknowledge ---
    // WHAT THE PUBLISHER IS ACTUALLY TOLD.
    //
    // Three outcomes, and they were previously two. A message that matched
    // subscribers and was then refused by every one of their queues used to be
    // acknowledged 0x00 Success - so a publisher whose QoS 1 contract says "at
    // least once" got an affirmative acknowledgement for a message that
    // reached nobody. MqttBrokerConfig.h has always said otherwise:
    //
    //   "the drop is counted, and at QoS > 0 the publisher is told with 0x97
    //    rather than being allowed to believe it was delivered"
    //
    // 0x97 was never sent from here. It is now, and the same applies when the
    // retained store had no room: the message was not kept, so saying it was
    // is the one answer that is definitely wrong.
    uint8_t outcome = MQ_REASON_SUCCESS;
    if (forward && matched == 0) {
        // [V5:3.4.2.1] 0x10 No matching subscribers. Real information: a
        // commissioning engineer publishing to a topic nobody is listening on
        // otherwise has no way at all to find that out. A full retained store
        // does NOT change this one - nobody was listening either way, and the
        // spelling of the topic is still what they need to check.
        outcome = MQ_REASON_NO_MATCHING_SUBSCRIBERS;
    } else if (retainedStoreFull || (matched > 0 && refusedByQueue == matched)) {
        outcome = MQ_REASON_QUOTA_EXCEEDED;
    }

    // THE QUOTA IS ONLY RETURNED IF THE ACKNOWLEDGEMENT WENT.
    //
    // inFlightFromPeer-- used to run whether or not the PUBACK was queued, so
    // the broker's own flow-control accounting said an exchange had completed
    // that the client was still waiting on. A client at its Receive Maximum
    // then had quota the broker had given back and it had not.
    //
    // These two DO recover by themselves - the client retransmits its PUBLISH
    // on reconnect - so closing is a recovery rather than a loss.
    if (message.qos == MQ_QOS_1) {
        if (!sendSimpleAck(connection, MQ_PKT_PUBACK, message.packetId, outcome)) {
            closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
            return;
        }
        if (connection.inFlightFromPeer > 0) connection.inFlightFromPeer--;
    } else if (message.qos == MQ_QOS_2) {
        const uint8_t said = forward ? outcome : MQ_REASON_SUCCESS;
        if (!sendSimpleAck(connection, MQ_PKT_PUBREC, message.packetId, said)) {
            closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
            return;
        }
        endRefusedQos2(connection, message.packetId, said);
    }
}

// [V5:4.3.3] A PUBREC with a Reason Code of 0x80 or greater ENDS the QoS 2
// exchange: the sender releases the packet identifier and never sends PUBREL.
// So must this side - otherwise the identifier stays "known" and the client's
// next message that reuses it is taken for a duplicate and silently dropped,
// and the in-flight count the PUBREL would have returned is never returned.
// At 3.1.1 the refusal was sent as success (there are no reason codes), the
// client WILL send PUBREL, and nothing is released here.
void MqttBroker::endRefusedQos2(MqttBrokerConnection& connection, uint16_t packetId,
                                uint8_t reasonCode) {
    if (reasonCode < 0x80 || connection.version != MqttVersion::V5) return;
    if (connection.session != nullptr) connection.session->forgetInbound(packetId);
    if (connection.inFlightFromPeer > 0) connection.inFlightFromPeer--;
}

// =============================================================================
// ACKNOWLEDGEMENTS
// =============================================================================

void MqttBroker::handleAck(MqttBrokerConnection& connection,
                           const MqttFixedHeader& header,
                           const MqttPacket& packet) {
    MqttBrokerSession* session = connection.session;
    if (session == nullptr) return;

    const uint16_t packetId = packet.ack.packetId;
    const uint8_t reason = packet.ack.reasonCode;

    switch (header.type) {
    case MQ_PKT_PUBACK: {
        // A QoS 1 delivery is complete. [V5:MQTT-4.4.0-2] - a reason code of
        // 0x80 or above STILL completes it: "it MUST NOT resend the PUBLISH
        // packet". A failure is a conclusion, not a retry trigger.
        MqttBrokerDelivery* delivery = session->findByPacketId(packetId);
        if (delivery == nullptr) return;   // unknown identifier; nothing owed
        delivery->release();
        connection.toPeer.returnOne();
        return;
    }

    case MQ_PKT_PUBREC: {
        MqttBrokerDelivery* delivery = session->findByPacketId(packetId);
        if (delivery == nullptr) {
            // [V5:MQTT-3.5.4-1] A PUBREC for an identifier we know nothing
            // about still needs a PUBREL, or the peer waits for ever. The
            // reason code says why.
            if (!sendSimpleAck(connection, MQ_PKT_PUBREL, packetId,
                               MQ_REASON_PACKET_IDENTIFIER_NOT_FOUND)) {
                closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
            }
            return;
        }

        if (reason >= 0x80) {
            // [V5:MQTT-4.4.0-2] A PUBREC carrying a failure ENDS the exchange.
            // No PUBREL follows, and the identifier is free again.
            delivery->release();
            connection.toPeer.returnOne();
            return;
        }

        delivery->awaitingPubcomp = true;
        delivery->pubrelOwed = true;
        // Sent on this pass rather than the next: the peer is holding the
        // identifier open until it arrives.
        deliverQueued(connection);
        return;
    }

    case MQ_PKT_PUBREL: {
        // The client finishing a QoS 2 it started. [V5:MQTT-3.6.4-1] "The
        // receiver ... MUST respond with a PUBCOMP packet containing the same
        // Packet Identifier as the PUBREL."
        const bool known = session->inboundKnown(packetId);
        // THE PUBCOMP GOES FIRST, and the state is only discarded if it went.
        // Forgetting the identifier and returning the quota before knowing
        // whether the peer would ever hear about it left the exchange half
        // finished at one end and complete at the other - and the peer holds
        // that identifier open until a PUBCOMP arrives.
        if (!sendSimpleAck(connection, MQ_PKT_PUBCOMP, packetId,
                           known ? MQ_REASON_SUCCESS
                                 : MQ_REASON_PACKET_IDENTIFIER_NOT_FOUND)) {
            closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
            return;
        }
        session->forgetInbound(packetId);
        if (connection.inFlightFromPeer > 0) connection.inFlightFromPeer--;
        return;
    }

    case MQ_PKT_PUBCOMP: {
        MqttBrokerDelivery* delivery = session->findByPacketId(packetId);
        if (delivery == nullptr) return;
        delivery->release();
        connection.toPeer.returnOne();
        return;
    }

    default:
        return;
    }
}

// =============================================================================
// SUBSCRIBE
// =============================================================================

bool MqttBroker::visitRetained(void* context, const MqttRetainedMessage& message,
                               uint32_t remainingExpiry, bool hasRemainingExpiry) {
    RetainedVisit* visit = (RetainedVisit*)context;
    MqttBroker& broker = *visit->broker;
    const MqttBrokerSubscription& subscription = *visit->subscription;
    MqttBrokerSession* session = visit->session;

    // Full: stop here, and pick this message up again when the queue drains.
    if (session->queueFull()) return false;

    MqttBrokerDelivery delivery;
    delivery.topic = message.topic;
    delivery.payload = message.payload;

    // [V5:MQTT-3.8.4-8] The lower of what was published and what was granted.
    delivery.qos = message.qos < subscription.qos() ? message.qos : subscription.qos();

    // [V5:MQTT-3.3.1-8] "When sending a PUBLISH Packet to a Client the Server
    // MUST set the RETAIN flag to 1 if a message is sent as a result of a new
    // subscription being made by a Client." Always 1 here, whatever Retain As
    // Published says - RAP governs ordinary delivery, not this.
    //
    // This is exactly where NanoMQ is wrong (internal/INTEROP.md D-2), which
    // is why ours is asserted in test_broker.cpp rather than assumed.
    delivery.retain = true;

    // One assignment, because both stores now use the same type. The version
    // that copied field by field is what left User Property behind.
    delivery.properties = message.properties;
    if (hasRemainingExpiry) {
        delivery.properties.hasExpiry = true;
        delivery.properties.expiryInterval = remainingExpiry;
    }

    if (subscription.subscriptionId != 0) {
        delivery.subscriptionIds[0] = subscription.subscriptionId;
        delivery.subscriptionIdCount = 1;
    }

    delivery.queuedAt = mq_millis();

    if (!session->enqueue(delivery)) return false;
    broker._status.retainedDelivered++;
    visit->queuedAny = true;
#if defined(MQTT_PLATFORM_HOST)
    broker.wake(*session);
#endif
    return true;
}

bool MqttBroker::continueRetained(MqttBrokerSession& session) {
    if (session.retainedOwed() == 0) return false;
    const mq_time_t now = mq_millis();
    bool queuedAny = false;
    for (size_t index = 0; index < session.subscriptionCount(); index++) {
        if (session.queueFull()) break;
        const MqttBrokerSubscription* subscription = session.subscriptionAt(index);
        if (subscription == nullptr || !subscription->retainedOwed) continue;

        RetainedVisit visit;
        visit.broker = this;
        visit.session = &session;
        visit.subscription = subscription;
        visit.queuedAny = false;
        const size_t next = _retained.visitMatchingFrom(
            asSpan(subscription->effective), now, subscription->retainedNext,
            &MqttBroker::visitRetained, &visit);
        queuedAny = queuedAny || visit.queuedAny;
        session.retainedProgress(index, next, next >= _retained.capacity());
    }
    return queuedAny;
}

void MqttBroker::handleSubscribe(MqttBrokerConnection& connection,
                                 const MqttPacket& packet) {
    MqttBrokerSession* session = connection.session;
    if (session == nullptr) return;

    const MqttSubscribe& request = packet.subscribe;
    const bool isV5 = connection.version == MqttVersion::V5;

    // [V5:MQTT-3.8.3-2] "The Payload MUST contain at least one Topic
    // Filter and Subscription Options pair." An empty SUBSCRIBE is a Protocol
    // Error, not an empty SUBACK.
    if (request.count == 0) {
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, true, true);
        return;
    }

    // [V5:MQTT-2.2.1-3] A SUBSCRIBE must carry a non-zero Packet Identifier.
    if (request.packetId == MQ_PACKET_ID_NONE) {
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, true, true);
        return;
    }

    uint32_t subscriptionId = 0;
    if (isV5 && request.properties.has(MQ_PROP_SUBSCRIPTION_IDENTIFIER)) {
        if (!_config.subscriptionIdentifiersAvailable) {
            // [V5:MQTT-3.2.2-12] A client that sends one to a server that said
            // they are unavailable gets a DISCONNECT, not a refused filter:
            // the identifier applies to the whole packet.
            closeConnection(connection,
                            MQ_REASON_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED,
                            true, true);
            return;
        }
        subscriptionId = request.properties.subscriptionIds[0];
    }

    MqttPacket response;
    response.header.type = MQ_PKT_SUBACK;
    response.suback.packetId = request.packetId;
    response.suback.count = request.count;

    // Which filters were accepted, so the retained messages go out AFTER the
    // SUBACK. [V5:3.8.4] allows either order, but a retained PUBLISH arriving
    // before the acknowledgement of the subscription that caused it confuses
    // every client that tracks its own subscriptions - including ours.
    bool accepted[MQTT_MAX_FILTERS_PER_PACKET] = {false};
    bool sendRetained[MQTT_MAX_FILTERS_PER_PACKET] = {false};

    for (uint8_t index = 0; index < request.count; index++) {
        const MqttSubscription& filter = request.filters[index];
        uint8_t granted = MQ_REASON_UNSPECIFIED_ERROR;

        const bool isShared = mq_isSharedFilter(filter.filter);

        if (!mq_topicFilterIsValid(filter.filter)) {
            granted = isV5 ? MQ_REASON_TOPIC_FILTER_INVALID : 0x80;
        } else if (isShared && !_config.sharedSubscriptionAvailable) {
            // [V5:MQTT-3.2.2-13] Advertised as unavailable, so refused here
            // rather than accepted and quietly never delivered.
            granted = isV5 ? MQ_REASON_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED : 0x80;
        } else if (!_config.wildcardSubscriptionAvailable &&
                   (memchr(filter.filter.data, MQ_TOPIC_MULTI_WILDCARD,
                           filter.filter.length) != nullptr ||
                    memchr(filter.filter.data, MQ_TOPIC_SINGLE_WILDCARD,
                           filter.filter.length) != nullptr)) {
            granted = isV5 ? MQ_REASON_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED : 0x80;
        } else {
            uint8_t refusal = MQ_REASON_NOT_AUTHORIZED;
            MqttSpan none;
            if (!authorise(connection, MqttBrokerAction::Subscribe, filter.filter,
                           filter.qos(), false, asSpan(session->clientId()),
                           asSpan(session->userName), none, refusal)) {
                granted = isV5 ? refusal : 0x80;
            } else {
                // [V5:MQTT-3.8.4-6] The granted QoS may be lower than the one
                // requested, and never higher than our Maximum QoS.
                uint8_t qos = filter.qos();
                if (qos > _config.maximumQos) qos = _config.maximumQos;

                bool replaced = false;
                uint8_t options = filter.options;
                if (qos != filter.qos()) {
                    options = (uint8_t)((options & ~MQ_SUBOPT_QOS_MASK) | qos);
                }

                if (session->addSubscription(filter.filter, options,
                                             subscriptionId, replaced)) {
                    granted = qos;
                    accepted[index] = true;

                    // [V5:3.8.3.1] Retain Handling:
                    //   0 - send retained messages at subscribe time
                    //   1 - send them only if the subscription is new
                    //   2 - do not send them
                    // 3.1.1 has no such option and always behaves as 0.
                    const uint8_t handling = isV5 ? filter.retainHandling()
                                                  : MQ_RETAIN_SEND_ALWAYS;
                    sendRetained[index] =
                        handling == MQ_RETAIN_SEND_ALWAYS ||
                        (handling == MQ_RETAIN_SEND_IF_NEW && !replaced);

                    // A shared subscription never receives retained messages
                    // at subscribe time [V5:4.8.2]: "Shared Subscriptions do
                    // not receive retained messages".
                    if (isShared) sendRetained[index] = false;
                } else {
                    granted = isV5 ? MQ_REASON_QUOTA_EXCEEDED : 0x80;
                }
            }
        }

        response.suback.reasons[index] = granted;
    }

    // A SUBACK HAS NO RETRANSMISSION ANYWHERE IN MQTT. Lose one and the client
    // waits on a subscription the broker thinks it granted, for ever, on a
    // connection that looks healthy from both ends. Closing is recoverable;
    // that is not.
    if (!send(connection, response)) {
        closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
        return;
    }

    // Owed, then sent as far as the queue has room. The rest follow as the
    // queue drains (deliverQueued), so none is dropped for want of space.
    for (uint8_t index = 0; index < request.count; index++) {
        if (!accepted[index] || !sendRetained[index]) continue;
        for (size_t scan = 0; scan < session->subscriptionCount(); scan++) {
            const MqttBrokerSubscription* candidate = session->subscriptionAt(scan);
            if (candidate == nullptr || !candidate->inUse()) continue;
            if (candidate->filter.size() == request.filters[index].filter.length &&
                memcmp(candidate->filter.data(), request.filters[index].filter.data,
                       candidate->filter.size()) == 0) {
                session->oweRetained(scan);
                break;
            }
        }
    }
    continueRetained(*session);

    deliverQueued(connection);
}

// =============================================================================
// UNSUBSCRIBE
// =============================================================================

void MqttBroker::handleUnsubscribe(MqttBrokerConnection& connection,
                                   const MqttPacket& packet) {
    MqttBrokerSession* session = connection.session;
    if (session == nullptr) return;

    const MqttUnsubscribe& request = packet.unsubscribe;

    // [V5:MQTT-3.10.3-2] "The Payload of an UNSUBSCRIBE packet MUST contain at
    // least one Topic Filter."
    if (request.count == 0 || request.packetId == MQ_PACKET_ID_NONE) {
        _status.protocolErrors++;
        closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, true, true);
        return;
    }

    MqttPacket response;
    response.header.type = MQ_PKT_UNSUBACK;
    response.suback.packetId = request.packetId;
    response.suback.count = request.count;

    for (uint8_t index = 0; index < request.count; index++) {
        const bool removed = session->removeSubscription(request.filters[index]);
        // [V5:3.11.3] 0x11 No subscription existed. 3.1.1 has no reason codes
        // on UNSUBACK at all, and the codec drops the list for that version -
        // so this is filled in either way and simply not encoded at 3.1.1.
        response.suback.reasons[index] =
            removed ? MQ_REASON_SUCCESS : MQ_REASON_NO_SUBSCRIPTION_EXISTED;
    }

    // As with SUBACK: nothing resends this, so a client that loses it waits
    // for ever on an unsubscribe the broker has already performed.
    if (!send(connection, response)) {
        closeConnection(connection, MQ_REASON_QUOTA_EXCEEDED, false, true);
    }
}

// =============================================================================
// DISCONNECT
// =============================================================================

void MqttBroker::handleDisconnect(MqttBrokerConnection& connection,
                                  const MqttPacket& packet) {
    MqttBrokerSession* session = connection.session;
    const bool isV5 = connection.version == MqttVersion::V5;

    // [V5:MQTT-3.14.4-3] A Normal disconnection DISCARDS the Will.
    // [V5:3.14.2.1] Reason 0x04 asks for the opposite: "Disconnect with Will
    // Message - The Client wishes to disconnect but requires that the Server
    // also publishes its Will Message."
    const bool willWanted =
        isV5 && packet.disconnect.reasonCode == MQ_REASON_DISCONNECT_WITH_WILL;

    if (isV5 && session != nullptr &&
        packet.disconnect.properties.has(MQ_PROP_SESSION_EXPIRY_INTERVAL)) {
        const uint32_t requested = packet.disconnect.properties.sessionExpiryInterval;

        // [V5:MQTT-3.14.4-2] "If the Session Expiry Interval in the CONNECT
        // packet was zero, then it is a Protocol Error to set a non-zero
        // Session Expiry Interval in the DISCONNECT packet sent by the
        // Client." A session that was never going to survive cannot be given a
        // lifetime on the way out.
        if (session->expiryInterval == 0 && requested != 0) {
            _status.protocolErrors++;
            closeConnection(connection, MQ_REASON_PROTOCOL_ERROR, true, false);
            return;
        }

        session->expiryInterval =
            requested > _config.maximumSessionExpiry &&
            _config.maximumSessionExpiry != 0xFFFFFFFFu
                ? _config.maximumSessionExpiry : requested;
    }

    // [V5:MQTT-3.14.4-1] "After sending a DISCONNECT packet the Client MUST
    // close the Network Connection", and the Server does not answer it. The
    // close here is ours, not a reply.
    closeConnection(connection, 0, false, willWanted);
}

// =============================================================================
// PUBLISHING FROM THE APPLICATION
// =============================================================================

#if defined(MQTT_PLATFORM_HOST)
bool MqttBroker::publishMessage(const MqttBrokerRouted& message) {
    if (!mq_topicNameIsValid(message.topic) || message.qos > MQ_QOS_2) return false;
    static const MqttProperties kNone;
    const MqttProperties& properties = message.properties ? *message.properties : kNone;
    bool kept = true;
    if (message.retain && _config.retainAvailable) {
        kept = _retained.store(message.topic, message.payload, message.qos, properties, mq_millis());
    }
    size_t refusedByQueue = 0;
    const size_t matched = route(message.topic, message.payload, message.qos, message.retain,
                                 properties, nullptr, &refusedByQueue);
    egress(message.topic, message.payload, message.qos, message.retain, properties,
           message.clientId, matched, refusedByQueue, 0, message.origin);
    _status.published++;
    // The same answer as publish(), for the same reasons.
    if (!kept) return false;
    if (matched > 0 && refusedByQueue == matched) return false;
    return true;
}
#endif

size_t MqttBroker::disconnectUser(const MqttSpan& userName, uint8_t reasonCode) {
    size_t closed = 0;
    for (MqttBrokerConnection& connection : _connections) {
        if (!connection.inUse() || connection.closing || connection.session == nullptr) continue;
        const std::string& name = connection.session->userName;
        if (name.size() != userName.length) continue;
        if (userName.length != 0 && memcmp(name.data(), userName.data, userName.length) != 0) continue;
        // Will published: to everyone else this device has gone, which is
        // what its Will is for.
        closeConnection(connection, reasonCode, true, true);
        closed++;
    }
    return closed;
}

bool MqttBroker::restoreRetained(const MqttSpan& topic, const MqttSpan& payload, uint8_t qos,
                                 const MqttProperties& properties) {
    if (!_config.retainAvailable || payload.length == 0 || qos > MQ_QOS_2) return false;
    if (!mq_topicNameIsValid(topic)) return false;
    return _retained.store(topic, payload, qos, properties, mq_millis());
}

bool MqttBroker::publish(const char* topic, const uint8_t* payload, size_t length,
                         uint8_t qos, bool retain) {
    if (topic == nullptr) return false;

    MqttSpan topicSpan;
    topicSpan.data = (const uint8_t*)topic;
    topicSpan.length = (uint32_t)strlen(topic);
    if (!mq_topicNameIsValid(topicSpan)) return false;

    MqttSpan payloadSpan;
    payloadSpan.data = payload;
    payloadSpan.length = (uint32_t)length;

    MqttProperties properties;

    bool kept = true;
    if (retain && _config.retainAvailable) {
        kept = _retained.store(topicSpan, payloadSpan, qos, properties, mq_millis());
    }

    // No originating session, so No Local matches nothing and every subscriber
    // receives it - including one that published the same topic a moment ago.
    size_t refusedByQueue = 0;
    const size_t matched = route(topicSpan, payloadSpan, qos, retain, properties,
                                 nullptr, &refusedByQueue);
#if defined(MQTT_PLATFORM_HOST)
    egress(topicSpan, payloadSpan, qos, retain, properties, MqttSpan(), matched,
           refusedByQueue, 0);
#endif
    _status.published++;

    // WHAT false MEANS HERE, AND WHY IT IS NOT "nobody was subscribed".
    //
    // This returned true unconditionally - including when the retained store
    // was full and nothing was kept, and when every matching subscriber's
    // queue refused it and nothing was delivered. An OpenPLC program calling
    // this could not tell complete success from complete failure, which makes
    // the return value worse than no return value at all.
    //
    // Having no subscribers is NOT a failure: a retained publish to a topic
    // nobody has asked for yet is the ordinary way to arm a setpoint, and the
    // message is stored and waiting. What is reported is the message going
    // nowhere AND being kept nowhere.
    //
    // Delivered on the next poll rather than here. A publish from inside an
    // application's own scan must not run the whole delivery path
    // re-entrantly: the tap handler that called it would be re-entered too.
    if (!kept) return false;
    if (matched > 0 && refusedByQueue == matched) return false;
    return true;
}


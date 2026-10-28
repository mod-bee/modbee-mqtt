// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// =============================================================================
// THE CLIENT
// =============================================================================
//
// The first file in the library that knows which end of a conversation it is.
// Everything below it - the codecs, the queue, the session, the matcher - is
// role-neutral, which is what lets a broker be a different L3 over the same
// code rather than a fork. That is enforced by scripts/check-layering.py, not
// by good intentions.
//
// NOTHING HERE BLOCKS, connect() included.
//
//   Idle -> Connecting -> AwaitingConnAck -> [Authenticating] -> Connected
//                                                                    |
//               Disconnecting <----------------------------- Reconnecting
//
// connect() queues the intent and returns. The acknowledgement arrives on a
// later poll(). That is the whole reason this library exists rather than
// wrapping one that spins waiting for it: an OpenPLC scan cycle and an Arduino
// loop() are the same constraint, and a call that waits for a network round
// trip cannot live inside either.
//
// EVERY EVENT IS AVAILABLE TWICE: as a callback, and as pollable state.
// Generated IEC 61131 function-block code cannot register a lambda, so a
// wrapper reads counters and compares them with what it saw last - and two
// readers cannot take each other's edges, because the counters are totals
// rather than flags.

#include "MqttTransport.h"
#include "MqttCodec.h"
#include "MqttLink.h"
#include "MqttConfig.h"
#include "MqttCapabilities.h"
#include "MqttFlow.h"
#include "MqttSession.h"
#include "MqttOperations.h"
#include "MqttKeepAlive.h"
#include "MqttAliases.h"
#include "MqttTopic.h"
#include "MqttTopicRegistry.h"

#include <vector>

enum class MqttState : uint8_t {
    Idle = 0,
    Connecting,        // the transport is opening
    AwaitingConnAck,   // CONNECT sent, waiting for the answer
    Authenticating,    // a 5.0 AUTH exchange is in progress
    Connected,
    Disconnecting,
    Reconnecting,      // waiting out the backoff
};

const char* mq_stateName(MqttState state);

// An inbound application message.
//
// The spans point into the receive buffer and are valid ONLY for the duration
// of the handler. Anything that must outlive it copies what it needs, and says
// so - which is the same rule the decoder follows, one layer down.
struct MqttMessage {
    MqttSpan topic;
    MqttSpan payload;
    uint8_t qos = MQ_QOS_0;
    bool retain = false;
    bool dup = false;
    uint16_t packetId = MQ_PACKET_ID_NONE;

    // Null on a 3.1.1 connection, where there are none.
    const MqttProperties* properties = nullptr;
};

typedef void (*MqttMessageFn)(void* context, const MqttMessage& message);

// Runs when a session is established, BEFORE anything queued is sent.
//
// R7. This is the hook a Sparkplug layer hangs its birth certificate on: an
// NBIRTH has to be the first publication on a connection, ahead of whatever
// the application queued while the link was down. A hook that ran after the
// queue drained would put the birth in the middle of the data it is supposed
// to introduce.
typedef void (*MqttSessionFn)(void* context, bool sessionPresent);

typedef void (*MqttStateFn)(void* context, MqttState state, MqttResult reason);

class MqttClient;

// =============================================================================
// A LAYER THAT SHARES A CLIENT
// =============================================================================
//
// ONE CONNECTION, SEVERAL PROTOCOLS ON TOP OF IT. A Sparkplug node, a firmware
// updater and an application's own topics all want the same broker session,
// and a device has one radio and one login to spend. The three handler slots
// further down are the application's; a layer that took one took it from
// everybody else.
//
// So a layer attaches instead. Any number may, each sees every event, in the
// order they attached, and the application's own handlers run after them.
//
//   prepareConnect      before EVERY CONNECT is encoded - the first, each
//                       automatic reconnect, a 3.1.1 fallback - with the
//                       config it is built from. Where a layer whose Will
//                       changes per connection (Sparkplug's bdSeq) writes it,
//                       because the client reconnects without asking anybody.
//   sessionEstablished  as MqttSessionFn: before anything queued is sent, and
//                       what is queued inside it goes first.
//   stateChanged        as MqttStateFn.
//   message             every inbound message, after the topic registry has
//                       it. True when it was this layer's: later layers and the
//                       application handler do not see it.
//
// NOTHING ATTACHED COSTS ONE NULL CHECK per event, which is the whole of what
// this adds to a client that does not use it.
//
// Callbacks run inside poll(), on whatever thread polls. A layer is attached
// to at most one client and is detached (or the client ended) before it is
// destroyed; attach() and detach() are never called from inside a callback.
// How long restart() leaves between the goodbye and the next CONNECT: long
// enough for a broker to have handled the one before the same client
// identifier returns, short enough that nobody waits for it.
#ifndef MQ_RESTART_SETTLE_MS
#define MQ_RESTART_SETTLE_MS 250
#endif

// How long a connection has to have stayed up before its loss starts the
// reconnect backoff again from reconnectInitialMs. A CONNACK alone is not a
// working connection: a broker that accepts and then drops a client at once -
// overloaded, a rule that closes it, a takeover loop between two devices with
// one identifier - would otherwise be answered with a fresh one-second backoff
// every time, for ever. See scheduleReconnect() in MqttClient.cpp.
#ifndef MQ_RECONNECT_STABLE_MS
#define MQ_RECONNECT_STABLE_MS 30000
#endif

// The shortest reconnect delay full jitter may draw. Zero would put some
// devices straight back on a broker that has only just gone away; this is
// small enough to change nothing else. Never more than the backoff itself,
// so a test or a LAN configured with a 1 ms backoff still gets 1 ms.
#ifndef MQ_RECONNECT_FLOOR_MS
#define MQ_RECONNECT_FLOOR_MS 100
#endif

class MqttClientHook {
public:
    // Out of line, in MqttClient.cpp: one vtable in the program, not one per
    // file that names a layer.
    virtual ~MqttClientHook();

    virtual void prepareConnect(MqttConnectionConfig& connection);
    virtual void sessionEstablished(bool sessionPresent);
    virtual void stateChanged(MqttState state, MqttResult reason);
    virtual bool message(const MqttMessage& message);
    // The client this layer is attached to, or null.
    MqttClient* attachedTo() const { return _attachedTo; }

private:
    friend class MqttClient;
    MqttClientHook* _nextHook = nullptr;
    MqttClient* _attachedTo = nullptr;
};

// Everything a caller can poll instead of registering a handler.
//
// Counters are RUNNING TOTALS that wrap, never flags. A caller compares one
// with what it saw last, so two callers cannot steal each other's edges - and
// a function block polled once a scan misses nothing.
struct MqttStatus {
    MqttState state = MqttState::Idle;
    MqttVersion version = MqttVersion::V5;
    bool connected = false;

    uint32_t connects = 0;
    uint32_t disconnects = 0;
    uint32_t published = 0;
    uint32_t received = 0;
    uint32_t publishFailures = 0;
    uint32_t reconnects = 0;
    uint32_t protocolErrors = 0;

    // Why the last connection ended, or the last operation failed. Kept so a
    // caller that polls rather than registering a handler can still find out.
    MqttResult lastError = MqttResult::Ok;

    // The last reason code the peer sent with ANY packet - CONNACK, SUBACK,
    // PUBACK, DISCONNECT. Zero when none has been sent.
    //
    // AT 3.1.1 THE NUMBERS MEAN SOMETHING ELSE. That version's CONNACK
    // carries a RETURN CODE (4 is a bad user name or password) and its SUBACK
    // a failure of 0x80, and both land here - a different numbering from
    // 5.0's reason codes, where 4 is "disconnect with Will". Read it with the
    // negotiated version, or use connectReasonCode below for the one question
    // most people are actually asking.
    uint8_t lastReasonCode = 0;

    // What the broker answered the MOST RECENT CONNECT attempt with, and
    // nothing else: 0 while an attempt is in progress, and 0 if the attempt
    // never got an answer at all - a broker that is down, a link that is cut.
    //
    // SEPARATE FROM lastReasonCode ON PURPOSE. That one is the last reason
    // code from ANY packet - a SUBACK, a PUBACK, a DISCONNECT - and is never
    // cleared. Deciding "was my password rejected?" from it meant a topic
    // refused hours earlier with 0x87 made an unreachable broker look like a
    // rejected credential, and the client stood down for good.
    uint8_t connectReasonCode = 0;
};

class MqttClient {
public:
    MqttClient() MQ_NOEXCEPT;

    // The transport is the caller's and must outlive this object. The library
    // does not own the network stack and cannot decide when it starts.
    void begin(MqttTransport& transport,
               const MqttConnectionConfig& connection,
               const MqttClientConfig& behaviour = MqttClientConfig());

    // Closes everything and concludes every outstanding operation. The
    // contract does not lapse because the application stopped.
    void end();

    // REPLACES WHAT THE NEXT CONNECT WILL USE, WITHOUT RESTARTING ANYTHING.
    //
    // Host, port, client identifier, credentials and Will - all of it comes
    // from the config handed to begin(), which is copied. There was no way to
    // change any of it afterwards, and two things need one:
    //
    //   - A ROTATING DEATH CERTIFICATE. The Will is a field of the CONNECT
    //     packet, so a payload that must differ per connection has to be
    //     rewritten between them. Sparkplug's bdSeq is exactly this: it
    //     increments on every CONNECT and the Will must carry the new value,
    //     because it is the only thing correlating a death with its birth.
    //   - A SERVER LIST. Failing over to a second broker means changing the
    //     host, and the transport is the caller's to re-point.
    //
    // begin() would do both and is the wrong tool: it resets the status
    // counters a function block reads every scan, re-initialises the link
    // buffers, and re-caps the operation queue WITHOUT concluding what is in
    // it - so outstanding publishes would vanish rather than fail. A reconnect
    // is not a restart.
    //
    // Takes effect at the next CONNECT. Accepted while connected - the
    // alternative is refusing it and making the caller race the reconnect
    // timer - and the current session is untouched, which is what "next"
    // means. Refused before begin(), because there is nothing to reconfigure.
    //
    // The TRANSPORT is not re-pointed here. It belongs to the caller, and a
    // library that reached into it would undo whatever they had set; see
    // MqttDevice::connect() for the pattern where the caller owns both.
    MqttResult reconfigure(const MqttConnectionConfig& connection);

    // MOVES THE CLIENT TO ANOTHER TRANSPORT, KEEPING EVERYTHING ELSE.
    //
    // For a settings change that needs a different medium, not just a
    // different host: plain to TLS, Wi-Fi to cellular, a new pinned key. The
    // topic registry, the subscriptions, the handles every other block holds,
    // the status counters and the operation queue all stay - begin() would
    // reset the counters and re-cap the queue without concluding it.
    //
    // Whatever the old transport held is closed first, without a DISCONNECT
    // (send one with disconnect() and let it go out before this if the peer
    // should see a graceful goodbye). The client is left Idle and down until
    // connect(), with the session's unsent work concluded exactly as a
    // dropped connection concludes it. The old transport is the caller's to
    // delete afterwards; this object never refers to it again.
    //
    // Refused before begin(), because there is nothing to move.
    MqttResult rebind(MqttTransport& transport);

    // The connection configuration in force - what begin() or the last
    // reconfigure() gave. Read it, change a field, hand it back to
    // reconfigure(): how a caller that did not keep its own copy (an OpenPLC
    // block that built the client on an earlier scan) changes one thing
    // without losing the rest.
    const MqttConnectionConfig& connection() const { return _connection; }

    // The whole contract. Call it often; it never waits for anything.
    void poll();

    // --- Connection ---
    //
    // Queues the intent and returns. Whether it succeeded arrives as a state
    // change on a later poll().
    MqttResult connect();

    // Sends a DISCONNECT and closes. V5:3.14.4 - after sending one we MUST
    // close the connection and MUST NOT send anything further on it.
    //
    // MQ_REASON_DISCONNECT_WITH_WILL has the broker publish the Will anyway:
    // what a layer whose Will is a death certificate needs on a deliberate
    // goodbye (see willOwner()). At 3.1.1, which has no such reason, the Will
    // is published by the client itself, just ahead of the DISCONNECT.
    MqttResult disconnect(uint8_t reasonCode = MQ_REASON_NORMAL_DISCONNECTION);

    // ENDS THIS CONNECTION AND STARTS THE NEXT ONE AT ONCE.
    //
    // For a layer that shares the client and needs a fresh CONNECT without
    // owning the decision to be connected: a Sparkplug node whose Will has to
    // be on the CONNECT, or whose primary host went offline. disconnect()
    // would leave the client down until its owner noticed; this sends the
    // DISCONNECT (reasonCode as disconnect() takes it), closes, and connects
    // again MQ_RESTART_SETTLE_MS later - no backoff, autoReconnect or not.
    //
    // Refused with NotConnected while the owner keeps the client down - never
    // connected, or disconnect() in force: that is its decision, not a
    // layer's. A client between connections stops waiting out its backoff.
    MqttResult restart(uint8_t reasonCode = MQ_REASON_NORMAL_DISCONNECTION);

    // --- Layers sharing this client: see MqttClientHook ---
    //
    // attach() appends. False if the hook is attached to another client;
    // true, and nothing changes, if it is already attached to this one.
    bool attach(MqttClientHook& hook);
    void detach(MqttClientHook& hook);

    // THE WILL HAS ONE OWNER. A CONNECT carries one Will, and two layers each
    // writing theirs in prepareConnect would leave whichever ran last - so a
    // layer that needs it claims it, and a second claimant is refused rather
    // than silently overwritten. Releasing clears the Will from the next
    // CONNECT; detaching releases it.
    bool claimWill(MqttClientHook& hook);
    void releaseWill(MqttClientHook& hook);
    const MqttClientHook* willOwner() const { return _willOwner; }

    // A TRANSMIT BUFFER AT LEAST THIS BIG, for a layer whose largest packet is
    // its own business - a Sparkplug birth certificate grows with what is
    // declared. The buffers never grow under a live connection (see
    // MqttLink), so this is applied while the link is down: at once if it is,
    // otherwise before the next CONNECT. True when the buffer already holds
    // `bytes`; false means it will after the next connection starts (a
    // restart() brings that forward), or that memory could not be found -
    // transmitCapacity() says which.
    bool reserveTransmit(size_t bytes);
    size_t transmitCapacity() const { return _link.transmitCapacity(); }

    // --- Operations ---
    MqttResult publish(const char* topic, const uint8_t* payload, size_t length,
                       uint8_t qos = MQ_QOS_0, bool retain = false,
                       MqttCompletionFn onComplete = nullptr, void* context = nullptr);

    MqttResult publish(const char* topic, const char* payload,
                       uint8_t qos = MQ_QOS_0, bool retain = false,
                       MqttCompletionFn onComplete = nullptr, void* context = nullptr);

    // Withdraw anything queued for this topic that has not yet been written.
    // Returns how many were withdrawn; each is concluded as Superseded,
    // because that is what happened to it - something made it irrelevant
    // before it went.
    //
    // WHAT THIS IS FOR. A publisher about to restate everything - a full
    // refresh, a re-announcement, a birth certificate - has already made any
    // queued update redundant. Letting it go anyway costs bytes on a metered
    // link, and where a protocol forbids ordinary data between a request and
    // the refresh answering it, sends a message that must not be sent.
    //
    // It cannot reach anything already written. Those bytes are gone and their
    // acknowledgement is still owed, so only unsent records are withdrawn -
    // which means this is a way to send less, never a way to undo.
    size_t withdraw(const char* topic);

    // One message of a SERIES, where every message matters.
    //
    // publish() above may be superseded: a newer value for the same topic
    // replaces one still waiting, which is what you want for a reading or a
    // setpoint - the value that arrives should be the value that is true.
    //
    // This one is never replaced. Use it where the messages are a sequence
    // rather than a value: a counter, an event log, a Sparkplug data stream.
    // Eliding a member of a sequence leaves a gap the receiver reads as loss,
    // which is precisely what a seq field is there to report.
    MqttResult publishSeries(const char* topic, const uint8_t* payload, size_t length,
                              uint8_t qos = MQ_QOS_0, bool retain = false,
                              MqttCompletionFn onComplete = nullptr,
                              void* context = nullptr);

    // --- With 5.0 properties ---
    //
    // A Response Topic and Correlation Data (the request/response pattern), a
    // Content Type, User Properties, a Message Expiry Interval. Build the set
    // with MqttOwnedProperties' named setters; it is COPIED into the queue, so
    // it may be a local.
    //
    // REFUSED WITH NotSupportedByProtocol ON A 3.1.1 CONNECTION, never sent
    // stripped. 3.1.1 has no properties at all, and a publish that quietly
    // lost its correlation id would leave the requester waiting for an answer
    // that can no longer be matched to it. The refusal happens at the call
    // when the version is already known, and at the send otherwise - because a
    // publish queued before connecting cannot know yet which version it will
    // go out on.
    //
    // Topic Alias is deliberately NOT settable: aliases are negotiated
    // per connection and MqttAliases owns them. An application that set one by
    // hand would be overwriting the library's own table.
    MqttResult publish(const char* topic, const uint8_t* payload, size_t length,
                       uint8_t qos, bool retain,
                       const MqttOwnedProperties& properties,
                       MqttCompletionFn onComplete = nullptr, void* context = nullptr);

    MqttResult publishSeries(const char* topic, const uint8_t* payload, size_t length,
                             uint8_t qos, bool retain,
                             const MqttOwnedProperties& properties,
                             MqttCompletionFn onComplete = nullptr,
                             void* context = nullptr);

    // Subscribing before the connection exists is RECORDED, not refused. A
    // caller that has to watch for the connection itself is a caller that has
    // to know the protocol's own rules, and only one that polls every scan
    // gets the behaviour at all.
    MqttResult subscribe(const char* filter, uint8_t qos = MQ_QOS_0,
                         MqttCompletionFn onComplete = nullptr, void* context = nullptr);

    // The 5.0 subscription options, as a byte. Build it with
    // mq_subscribeOptions() below rather than by hand.
    MqttResult subscribeWithOptions(const char* filter, uint8_t options,
                                    MqttCompletionFn onComplete = nullptr,
                                    void* context = nullptr);

    MqttResult unsubscribe(const char* filter,
                           MqttCompletionFn onComplete = nullptr, void* context = nullptr);

    // --- Handlers ---
    void onMessage(MqttMessageFn handler, void* context);
    void onSessionEstablished(MqttSessionFn handler, void* context);
    void onStateChange(MqttStateFn handler, void* context);

    // --- Handles, for a caller that cannot hold strings ---
    //
    // See MqttTopicRegistry.h. The registry is public, so a wrapper that wants
    // the whole thing has it, and these are the four calls that need to reach
    // into the client to do their work.

    MqttTopicHandle registerTopic(const char* topic);

    MqttResult publishTo(MqttTopicHandle handle, const uint8_t* payload, size_t length,
                         uint8_t qos = MQ_QOS_0, bool retain = false);
    MqttResult subscribeTo(MqttTopicHandle handle, uint8_t qos = MQ_QOS_0);

    const char* topicOf(MqttTopicHandle handle) const;

    // What a function block reads each scan. Never null, even for a handle
    // that does not exist.
    const MqttTopicState& topicState(MqttTopicHandle handle) const;

    // Copies the most recent payload out and clears the pending flag, so a
    // block can tell a new message from the same one read twice.
    bool takeMessage(MqttTopicHandle handle, uint8_t* buffer, size_t capacity,
                     size_t& length);

    // The registry itself, for a wrapper that wants more than the four calls
    // above - how many topics are registered, whether one has a message
    // waiting, or to clear the lot.
    MqttTopicRegistry& topics() { return _topics; }
    const MqttTopicRegistry& topics() const { return _topics; }

    // The topic and subscription tables grow with the program - on a host and
    // a board alike, nothing caps them but memory - and this says one of them
    // could not: a registration or a subscription was refused for want of
    // memory, not for anything about the topic.
    bool outOfMemory() const { return _topics.outOfMemory() || _session.outOfMemory(); }

    // The send queue could not be given the size begin() asked for
    // (MqttClientConfig::queueCapacity - a PLC's MAX_PENDING): its slots or,
    // on a board, the bytes its messages are kept in. It keeps the size it
    // had, and operations().capacity() says what that is.
    bool queueOutOfMemory() const {
#if MQTT_LEAN
        if (_areaOutOfMemory) return true;
#endif
        return _operations.outOfMemory();
    }

    // --- Polled state ---
    const MqttStatus& status() const { return _status; }
    MqttState state() const { return _status.state; }

    // True only when work will actually go: connected, acknowledged, and with
    // a session to carry it. Anything weaker would let a caller believe a
    // publish was on its way when it was not.
    bool isConnected() const { return _status.state == MqttState::Connected; }

    const MqttCapabilities& capabilities() const { return _capabilities; }
    MqttSession& session() { return _session; }
    const MqttSession& session() const { return _session; }
    MqttOperations& operations() { return _operations; }
    const MqttOperations& operations() const { return _operations; }
    MqttTransport* transport() { return _transport; }
    const MqttTransport* transport() const { return _transport; }

    // What is in force, which is not always what was asked for.
    MqttVersion activeVersion() const { return _activeVersion; }
    uint16_t activeKeepAlive() const { return _keepAlive.intervalSeconds(); }

private:
    MqttTransport* _transport = nullptr;
    MqttConnectionConfig _connection;
    MqttClientConfig _behaviour;

    MqttStatus _status;
    MqttCapabilities _capabilities;
    MqttFlow _flow;
#if MQTT_LEAN
    // A board's queued topics, payloads, properties, resend copies and
    // filters (MqttFixed.h), sized in begin(). Declared first so it outlives
    // what it holds.
    MqttLargeBytes _areaBytes;
    MqttByteArea _area;
    bool _areaOutOfMemory = false;
#endif
    // Subscriptions the queue could not take when the session came up, still
    // to go: a program may hold more subscriptions than the queue holds
    // operations, so they go as it drains.
    bool _reissueOwed = false;
    MqttSession _session;
    MqttOperations _operations;
    MqttKeepAlive _keepAlive;
    MqttAliases _aliases;

    // The version actually in use, which differs from what was configured
    // whenever a fallback has happened.
    MqttVersion _activeVersion = MqttVersion::V5;
    bool _fallbackTried = false;

    // An intentional disconnect() is in force: the link stays down until the
    // application calls connect() again. A FLAG OF ITS OWN, not a write to
    // _behaviour.autoReconnect - that is the configured behaviour, and
    // overwriting it meant every later lost link was abandoned for good.
    bool _stayDown = false;

    // Framing - reassembly, the append queue, partial-write resume. Lifted
    // out of this class when the broker needed exactly the same thing, which
    // is what R1 is for: none of it depends on which end of the connection
    // this is.
    MqttLink _link;

    mq_time_t _stateSince = 0;
    mq_time_t _reconnectAt = 0;
    mq_time_t _reconnectDelay = 0;

    // The connection has stayed up for MQ_RECONNECT_STABLE_MS. Decided WHILE
    // it is up, on the poll, rather than by subtracting at the moment it
    // drops: a connection that had been up for 49.7 days and a few seconds
    // subtracted to "a few seconds", looked unstable, and kept whatever
    // backoff it had before it connected - a minute's wait after seven weeks
    // of good service.
    bool _stable = false;

    // The link's byte counters as the keepalive last saw them: bytes that
    // moved since are a sign of life even when no packet has finished - see
    // MqttKeepAlive. Taken afresh when a connection comes up.
    uint64_t _seenReceived = 0;
    uint64_t _seenRelieved = 0;

    // The send queue's obligation is met when the bytes have LEFT, not when
    // they were queued: the keepalive is told only once the link is idle.
    void noteTransmitted();

    MqttMessageFn _onMessage = nullptr;
    void* _messageContext = nullptr;
    MqttSessionFn _onSession = nullptr;
    void* _sessionContext = nullptr;
    MqttStateFn _onState = nullptr;
    void* _stateContext = nullptr;

    // Layers, in attach order, and the one that writes the Will.
    MqttClientHook* _hooks = nullptr;
    MqttClientHook* _willOwner = nullptr;

    // reserveTransmit()'s request, applied while the link is down.
    size_t _transmitWanted = 0;
    void applyTransmitReservation();
#if MQTT_LEAN
    // The board's byte area and queue slots, from _behaviour: begin(), and a
    // reservation that grew the transmit buffer.
    void sizeArea();
#endif
    // restart() is under way: the close it causes reconnects at once.
    bool _restarting = false;

    // MqttClientConfig::firstConnectSpreadMs: whether the first connect()
    // since begin() has been asked for, and whether the attempt it scheduled
    // is still to come - which is not a reconnect, and is not counted as one.
    bool _firstConnectAsked = false;
    bool _firstConnectHeld = false;
#if MQTT_LEAN
    // The reason an open that failed on a connect task is answered with
    // (MqttTransport::openFailed()): the one connect() gives a false begin(),
    // and none after serviceReconnect(), which keeps the one the link was
    // lost with.
    MqttResult _openFailure = MqttResult::Ok;
#endif

    // disconnect()/restart() with MQ_REASON_DISCONNECT_WITH_WILL at 3.1.1.
    void sendWillAt311(uint8_t reasonCode);
    // Topics by number, and the state a polled reader watches. Its own type
    // since the API review: a registry of topics with counters attached is a
    // self-contained idea, and MqttClient had enough to do.
    MqttTopicRegistry _topics;

    // The one publish path. `supersedable` is decided by the caller rather
    // than patched in afterwards: marking an operation unsupersedable only
    // once it is already queued leaves a window in which a second publish can
    // elide it, and a stream carrying sequence numbers would show the gap.
    MqttResult publishInternal(const char* topic, const uint8_t* payload, size_t length,
                               uint8_t qos, bool retain, bool supersedable,
                               const MqttOwnedProperties* properties,
                               MqttCompletionFn onComplete, void* context);

    const MqttCodec& codecFor(MqttVersion version) const;
    const MqttCodec& codec() const { return codecFor(_activeVersion); }

    void enterState(MqttState state, MqttResult reason = MqttResult::Ok);

    // --- The poll steps, in the order poll() runs them ---
    void serviceTransmit();
    void serviceReceive();
    void serviceKeepAlive();

    // Concludes anything whose acknowledgement is overdue. NOT a
    // retransmission - nothing is resent. See
    // MqttClientConfig::acknowledgementTimeoutMs.
    void serviceAcknowledgements();
    void serviceQueue();
    void serviceReconnect();

    bool sendPacket(const MqttPacket& packet);

    void handlePacket(const MqttFixedHeader& header, const MqttPacket& packet);
    void handleConnAck(const MqttPacket& packet);
    void handlePublish(const MqttPacket& packet);
    void handleAck(const MqttFixedHeader& header, const MqttPacket& packet);
    void handleSubAck(const MqttFixedHeader& header, const MqttPacket& packet);

    void sendConnect();
    void establishSession(bool sessionPresent);
    void reissueSubscriptions();

    // Tears the connection down. MQTT has no resynchronisation: one stray
    // octet desynchronises for good, so a malformed packet ends the
    // connection rather than starting a search for a boundary.
    void failConnection(MqttResult reason, uint8_t disconnectReason);
    void closeConnection(MqttResult reason);
    void scheduleReconnect();
};

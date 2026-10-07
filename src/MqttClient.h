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
    MqttClient();

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
    MqttResult disconnect(uint8_t reasonCode = MQ_REASON_NORMAL_DISCONNECTION);

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

    MqttMessageFn _onMessage = nullptr;
    void* _messageContext = nullptr;
    MqttSessionFn _onSession = nullptr;
    void* _sessionContext = nullptr;
    MqttStateFn _onState = nullptr;
    void* _stateContext = nullptr;

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

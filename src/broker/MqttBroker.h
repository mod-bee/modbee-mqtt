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
// THE BROKER
// =============================================================================
//
// The other end of the same protocol, over the same L0-L2 as the client.
//
// Almost nothing below this directory changed to make this possible. The
// codecs already encoded and decoded every packet in both directions, the
// matcher already answered "does this filter match this name" without caring
// which end was asking, MqttLink already reassembled a stream without knowing
// who had opened it, and MqttSession was already per-connection rather than a
// singleton. That was R1, and test/extension/test_broker_shape.cpp had been
// proving it for as long as the client has existed.
//
// Two things were added, and both are additive rather than alterations:
// MqttListener, a second way a link begins; and MqttTransport::peerName(), a
// defaulted virtual that returns an empty string unless a medium can do
// better. No existing implementation had to change for either.
//
// NOTHING HERE BLOCKS EITHER. One poll() drives every connection. A broker
// that blocked on one slow client would stall every other client in the plant,
// which is a far worse failure than the one the client's non-blocking rule
// prevents.
//
// WHAT THIS IS FOR. A gateway broker: a few dozen devices, small messages, one
// box, no clustering and no persistence across a power cycle. It is not a
// replacement for EMQX and does not try to be. What it is, is a broker that
// runs in the same binary as an OpenPLC runtime, needs no external service,
// and behaves exactly as the standards require - which is what a plant panel
// actually needs and what a cloud broker cannot provide when the link is down.
//
// WHAT IT DOES NOT DO, stated plainly rather than discovered:
//   - No persistence. A restart loses sessions and retained messages.
//     [V5:4.1] permits this; docs/broker.md says so where an operator reads.
//   - No clustering, no bridging between listeners (MqttBridge does that).
//   - No enhanced authentication mechanism. AUTH is decoded and refused.
//   - No WebSocket listener. The transport interface is narrow enough that
//     one would be additive, but none ships.
//
// ROUTING IS NO LONGER A LINEAR SCAN, and this comment used to say it was. A
// hash over literal filters plus a short list of wildcard ones, chained
// through the existing subscription slots, with the bucket array sized from
// config and taken once in begin() - so nothing is allocated per message,
// which is what makes an index acceptable on an ESP32 where a dynamically
// allocated trie would not be. Rebuilt only when a subscription changes,
// which a revision counter decides.

#include "../MqttFeatures.h"


#include "../MqttListener.h"
#include "../MqttLink.h"
#include "../MqttCodec.h"
#include "../MqttFlow.h"
#include "../MqttAliases.h"

#include "MqttBrokerConfig.h"
#include "MqttBrokerSession.h"
#include "MqttRetainedStore.h"

#include <string>
#include <vector>

#if defined(MQTT_PLATFORM_HOST)
#include "../transport/MqttPollSet.h"
#endif

// --- What the application is asked ---

enum class MqttBrokerAction : uint8_t {
    Connect = 0,
    Publish,
    Subscribe,
};

// Everything known about the request being authorised.
//
// Spans point into the packet being processed and are valid ONLY for the
// duration of the call. A hook that wants to keep a client identifier copies
// it - the same contract the decoder states one layer down.
struct MqttBrokerRequest {
    MqttBrokerAction action = MqttBrokerAction::Connect;

    MqttSpan clientId;
    MqttSpan userName;
    MqttSpan password;

    // The topic name being published, or the filter being subscribed to.
    // Empty for a Connect.
    MqttSpan topic;

    uint8_t qos = MQ_QOS_0;
    bool retain = false;

    // Where the connection came from, as the transport describes it. Empty
    // when the transport cannot say.
    const char* peerName = "";

    // A client certificate the TLS handshake VERIFIED: its subject's Common
    // Name and the SHA-256 of its DER (64 lowercase hex). Empty when there was
    // none - see MqttTransport::peerCertificateName().
    const char* certificateName = "";
    const char* certificateFingerprint = "";

    // Connect only, and only for a hook that authenticated the client by
    // something OTHER than the user name it sent - a certificate. The hook
    // writes the login it accepted here, and the session carries that name
    // instead of the CONNECT's: in what the broker reports about the client,
    // in every later Publish and Subscribe request, and in
    // disconnectUser(). Null for every other action.
    std::string* login = nullptr;

    MqttVersion version = MqttVersion::V5;
};

// Return true to allow.
//
// `reasonCode` is what the client is told when the answer is no, and it starts
// at the sensible refusal for the action - 0x87 Not Authorized. A hook that
// wants to say something more precise - 0x86 Bad User Name or Password, 0x97
// Quota Exceeded - writes it there.
//
// A function pointer with a context, not std::function: this is on the path of
// every CONNECT, SUBSCRIBE and PUBLISH, and generated wrapper code has to be
// able to reach it.
typedef bool (*MqttBrokerAuthFn)(void* context, const MqttBrokerRequest& request,
                                 uint8_t& reasonCode);

// Told about every message that passes through, after routing. For a bridge, a
// recorder, or a plant's own audit trail.
typedef void (*MqttBrokerTapFn)(void* context, const MqttSpan& clientId,
                                const MqttSpan& topic, const MqttSpan& payload,
                                uint8_t qos, bool retain);

// Told when a connection arrives or goes, so an application can log it in its
// own words. `reasonCode` is what we sent, or 0 when the peer simply vanished.
typedef void (*MqttBrokerConnectionFn)(void* context, const MqttSpan& clientId,
                                       bool connected, uint8_t reasonCode);

// Told what a client DID that something outside the broker may need to know:
// it connected (with its user name), went (with the reason), was granted a
// subscription, or removed one. A server exports these (modbee serve's
// `events:`), so an application that reads them learns, for example, which
// command topics a device listens on - which only the broker ever sees.
// Spans are valid only during the call.
enum class MqttBrokerClientEventKind : uint8_t {
    Connected = 0,
    Disconnected,
    Subscribed,
    Unsubscribed,
};

struct MqttBrokerClientEvent {
    MqttBrokerClientEventKind kind = MqttBrokerClientEventKind::Connected;
    MqttSpan clientId;
    MqttSpan userName;
    MqttSpan filter;                   // Subscribed / Unsubscribed
    uint8_t qos = 0;                   // Subscribed: the QoS granted
    uint8_t reasonCode = 0;            // Disconnected: what we sent, or 0
    bool resumed = false;              // Connected: an existing session was resumed (CONNACK session present)
};

typedef void (*MqttBrokerClientEventFn)(void* context, const MqttBrokerClientEvent& event);

// --- Hooks for a server that runs one broker per core (host only) ---
//
// A broker is single-threaded and instance-scoped (R5), so a multi-core server
// runs SEVERAL - one per core - and these are the seams between them. Each is
// optional and unused by a board; see internal/CLOUD-PLAN.md, section 5.

// A message this broker has just routed to its own sessions, offered onward.
// `matched` and `dropped` are its local outcome, as route() counts them.
struct MqttBrokerRouted {
    MqttSpan topic;
    MqttSpan payload;
    uint8_t qos = MQ_QOS_0;
    bool retain = false;
    const MqttProperties* properties = nullptr;   // never null when offered
    MqttSpan clientId;                            // the publisher; empty for the broker's own
    MqttSpan userName;                            // the publisher's login; empty without one
    size_t matched = 0;
    size_t dropped = 0;
    // Where a message from outside any connection came from, as the host that
    // injected it through publishMessage() tagged it - opaque here. A pipe
    // uses it so a message from Kafka topic T is never written back to T.
    const void* origin = nullptr;
};

// Called for every message that ORIGINATED here - a client's PUBLISH, a Will,
// MqttBroker::publish() - after local routing, and never for one that arrived
// through publishForwarded(). `deferral` is non-zero for a client's QoS 1 or
// 2 PUBLISH: return how many parties will later report through
// concludeDeferred(deferral, ...), and the PUBACK/PUBREC waits for all of them.
// Return 0 and it is sent now, from the local outcome alone.
typedef uint32_t (*MqttBrokerEgressFn)(void* context, const MqttBrokerRouted& message,
                                       uint64_t deferral);

// Which broker owns the session for this Client Identifier: a value of -1
// keeps the connection here; anything else hands it to the hand-off hook.
typedef int (*MqttBrokerPlaceFn)(void* context, const MqttSpan& clientId);

// The connection leaves this broker: its transport, the listener it came from
// (whose release() must be called when it is finally finished with), and
// every byte already read from it, starting with the CONNECT. The receiver
// calls adopt(). Ownership of the transport passes with the call.
typedef void (*MqttBrokerHandOffFn)(void* context, int target, MqttTransport& transport,
                                    MqttListener* origin, const uint8_t* bytes,
                                    size_t length);

// Whether THIS broker delivers shared group `shareName` on `filter` for the
// message being routed. Asked once per group per message, and only for groups
// with a matching member here. Without the hook every group is delivered, by
// this broker's own rotation - the single-broker behaviour.
typedef bool (*MqttBrokerShareGateFn)(void* context, const MqttSpan& shareName,
                                      const MqttSpan& filter);

// --- Counters ---
//
// Running totals, never flags, for the same reason the client's are: two
// readers must not steal each other's edges, and a function block polled once
// a scan must miss nothing.
struct MqttBrokerStatus {
    uint32_t accepted = 0;        // connections accepted at the socket
    uint32_t connected = 0;       // CONNECTs that earned a success CONNACK
    uint32_t refused = 0;         // CONNECTs answered with a failure
    uint32_t disconnected = 0;
    uint32_t takeovers = 0;       // 0x8E sent to a displaced connection
    // SIXTY-FOUR BITS FOR THE THREE THAT COUNT MESSAGES.
    //
    // A "running total" that goes backwards breaks precisely the consumer this
    // comment describes: a function block computing `now - last` sees a huge
    // number, or a negative one if it widens to a signed type. At the rate
    // this broker was measured at, `delivered` wraps a uint32_t in about six
    // hours, and roughly five days at a sustained 10k msg/s - both well inside
    // the uptime of a plant.
    //
    // Only these three are widened. The rest count CONNECTs, refusals and
    // faults, which do not reach four billion in any plant that is working,
    // and 64-bit arithmetic is not free on a 32-bit MCU that reads these every
    // scan. Measured: the flash cost of widening these three is under 200
    // bytes.
    uint64_t published = 0;       // PUBLISHes accepted from clients
    uint64_t delivered = 0;       // PUBLISHes sent to clients
    uint64_t dropped = 0;         // deliveries a full queue could not take
    uint32_t protocolErrors = 0;
    uint32_t keepAliveTimeouts = 0;
    uint32_t willsPublished = 0;
    uint32_t sessionsExpired = 0;
    uint32_t retainedDelivered = 0;
    uint32_t handedOff = 0;       // connections given to another broker at CONNECT
    uint32_t adopted = 0;         // connections taken from another broker
    // Full rebuilds of the routing index. A board rebuilds when subscriptions
    // change; a host keeps the index incrementally and this stays at zero -
    // which is what the reconnect-storm test holds it to.
    uint32_t indexRebuilds = 0;

    size_t connections = 0;       // live now
    size_t sessions = 0;          // held now, connected or not
    size_t retained = 0;
};

// One network connection. Not one session: a session outlives this.
struct MqttBrokerConnection {
    MqttTransport* transport = nullptr;
    MqttListener* origin = nullptr;    // who to hand the transport back to

    // THIS SLOT EXISTS TO SAY NO.
    //
    // Taken only when every serving slot is busy. The connection is accepted,
    // its CONNECT is read, and it is answered 0x89 Server Busy and closed -
    // which is a diagnosis the client can act on, and is what
    // MqttTcpListener's own comment has always promised. Before this, a
    // connection arriving at a full broker was simply not accepted: it sat in
    // the kernel backlog until that filled too and then its SYN was dropped,
    // with nothing counted and nothing said.
    bool refuseOnly = false;

    MqttLink link;

    // The quota for messages going TOWARDS this client, bounded by the
    // Receive Maximum it sent us [V5:3.1.2.11.3]. This is what MqttFlow
    // models, so it is what MqttFlow is used for.
    MqttFlow toPeer;

    // How many QoS > 0 PUBLISHes this client has outstanding towards US,
    // against our own Receive Maximum [V5:3.2.2.3.3]. The other direction, and
    // deliberately NOT an MqttFlow: it is a different quantity with a
    // different owner, and sharing the type would invite sharing the value.
    uint16_t inFlightFromPeer = 0;

    MqttAliases aliases;

    // [V5:3.1.2.10] "If the Keep Alive value is non-zero and the Server does
    // not receive an MQTT Control Packet from the Client within one and a half
    // times the Keep Alive time period, it MUST close the Network Connection."
    // Tracked as a last-seen time rather than with MqttKeepAlive, which models
    // the other end of this: sending pings, not expecting them.
    uint16_t keepAliveSeconds = 0;

    // Whether we changed what the client asked for, and therefore whether the
    // CONNACK must say so [V5:MQTT-3.1.2-21].
    bool keepAliveStated = false;

    mq_time_t lastPacketAt = 0;

    MqttVersion version = MqttVersion::V5;

    // Null until a CONNECT has been accepted. Everything but CONNECT is a
    // Protocol Error before then [V5:MQTT-3.1.0-1].
    MqttBrokerSession* session = nullptr;

    bool connectSeen = false;

    // We have finished with this connection and are waiting for the peer to
    // notice. Nothing more is processed; whatever arrives is discarded.
    bool closing = false;
    mq_time_t lingerUntil = 0;

    // [V5:3.1.2.11.4] How many QoS > 0 messages we may have in flight TOWARDS
    // this client. Theirs, not ours - the two are independent.
    uint16_t peerReceiveMaximum = 65535;
    uint16_t inFlightToPeer = 0;

    // [V5:3.1.2.11.4] The largest packet this client will accept. Zero means
    // no limit was stated.
    uint32_t peerMaximumPacketSize = 0;

    // [V5:3.1.2.11.5] How many aliases the client will accept from us.
    uint16_t peerTopicAliasMaximum = 0;

    // [V5:3.1.2.11.7] When 0, we must not send Reason String or User Property
    // on anything but PUBLISH, CONNACK and DISCONNECT.
    bool peerWantsProblemInformation = true;

    // Set when this connection asked for a clean start, so the will and the
    // session are treated correctly if the CONNECT is then refused.
    bool cleanStart = true;

    // Whether we shortened the Session Expiry Interval the client asked for.
    // [V5:3.2.2.3.2] requires a revised value to be STATED in the CONNACK -
    // silently shortening it would leave the client expecting its queue to
    // survive an outage it will not.
    bool sessionExpiryRevised = false;

    mq_time_t openedAt = 0;

    // --- Event-driven servicing (MqttPollSet persistent mode) ---
    //
    // Bumped every time the slot is recycled, and carried in the readiness
    // token. An event collected for the slot's PREVIOUS occupant carries the
    // old generation and is ignored, so a descriptor closed and reopened as
    // somebody else can never be serviced as the connection it used to be.
#if defined(MQTT_PLATFORM_HOST)
    uint32_t generation = 0;
    // Already on the broker's work list for the next pass.
    bool queued = false;
    // The descriptor registered with the readiness set, and whether write
    // interest is currently registered for it.
    int watchedHandle = -1;
    bool watchingWrite = false;
    // The kernel reported this socket ready since it was last serviced, so a
    // read is worth a system call. A connection woken only because a message
    // was queued for it has nothing to read, and asking would be an EAGAIN.
    bool readable = false;
#endif

    bool inUse() const { return transport != nullptr; }

    // Returns the slot to the free pool WITHOUT giving back the link's
    // buffers.
    //
    // Assigning a fresh MqttBrokerConnection over this would destroy two
    // vectors of several kilobytes each and allocate them again on the next
    // accept - so a broker serving short-lived connections would allocate and
    // free its whole receive path over and over, which is exactly the
    // behaviour a fixed pool exists to avoid.
    void recycle();
};

class MqttBroker {
public:
    MqttBroker() = default;
    ~MqttBroker();

    MqttBroker(const MqttBroker&) = delete;
    MqttBroker& operator=(const MqttBroker&) = delete;

    // Allocates everything, once. Call before adding listeners.
    void begin(const MqttBrokerConfig& config = MqttBrokerConfig());

    // Closes every connection, sending DISCONNECT 0x8B Server Shutting Down
    // where the version has one, and releases every session. A broker that
    // simply dropped its sockets would leave every client to discover it by
    // keepalive timeout, one interval later.
    void end();

    // Listeners are the caller's and must outlive the broker. More than one is
    // ordinary: plain TCP on 1883 and TLS on 8883 are two listeners over the
    // same broker, and neither the sessions nor the routing can tell them
    // apart - which is exactly right, because a subscriber on one must receive
    // what a publisher on the other sends. Adding one twice is a no-op.
    //
    // END THE BROKER BEFORE ITS LISTENERS. A listener owns the sockets of the
    // connections it accepted, so ending it while the broker still holds
    // those connections leaves the broker with transports that no longer
    // exist, and the next poll() reads freed memory. end() hands every
    // connection back to its listener first; after that the listeners may go.
    // Found as a segfault in a PLC's network task.
    bool addListener(MqttListener& listener);

#if defined(MQTT_PLATFORM_HOST)
    // Ask the kernel which connections are ready, instead of asking each one.
    //
    // OPTIONAL, AND OFF BY DEFAULT. Without it the broker behaves exactly as
    // it always has - which is what a board gets, and what every existing
    // caller gets until it says otherwise. The set is the caller's: a
    // standalone daemon owns one so it can also BLOCK on it between polls,
    // which is a thing poll() itself must never do.
    //
    // It changes which connections have their socket read, and nothing else.
    // Every connection is still visited every poll, in the same rotated
    // order, so keepalive, connect timeouts, Wills, session expiry and the
    // transmit path are untouched.
    void useReadiness(MqttPollSet& set);

    // Re-declares every descriptor worth waiting on, so a caller can wait on
    // the set after poll() returns. Called by poll() as well. Does nothing for
    // a set in persistent mode, whose membership is kept as connections come
    // and go.
    void refreshReadiness();

    // WHEN THE SET IS IN PERSISTENT MODE (MqttPollSet::beginPersistent), the
    // broker stops visiting every connection on every poll. It services only
    // the connections with something to do: those the kernel reports ready,
    // those newly accepted, those with output still to drain or input already
    // buffered, those closing, and those a message was just queued for.
    // Keepalive and connect deadlines are checked by a rolling sweep that
    // makes no system calls. A transport with no descriptor to watch is
    // simply serviced every poll, as before.
    //
    // A daemon that waits on the set between polls must not sleep while this
    // is true: the work is not on any descriptor the kernel could wake it for.
    bool hasPendingWork() const { return !_work.empty(); }
#endif

    // The whole contract. Call it often; it never waits for anything.
    void poll();

    // --- Publishing from the application itself ---
    //
    // A broker that can only relay what clients send is a broker an OpenPLC
    // program cannot talk to without opening a loopback connection to itself.
    // This is the same routing path, with no connection behind it.
    bool publish(const char* topic, const uint8_t* payload, size_t length,
                 uint8_t qos = MQ_QOS_0, bool retain = false);

    // Puts a message into the retained store WITHOUT publishing it - nobody
    // is sent it, no hook sees it. For restoring retained messages kept
    // somewhere that outlives the process (a file, a Kafka topic), after
    // begin() and before clients connect. A Message Expiry Interval in
    // `properties` counts from now, so pass what remained of it.
    // False if the topic is invalid, the payload is empty, retained messages
    // are turned off, or the store is full.
    bool restoreRetained(const MqttSpan& topic, const MqttSpan& payload, uint8_t qos,
                         const MqttProperties& properties);

    // Closes every connection that logged in as `userName`, telling a 5.0
    // client why (0x87 Not authorized, say, for a login just revoked). Their
    // Wills are published. How many were closed.
    size_t disconnectUser(const MqttSpan& userName, uint8_t reasonCode);

    // --- Hooks ---
    void onAuthorise(MqttBrokerAuthFn handler, void* context);
    void onMessage(MqttBrokerTapFn handler, void* context);
    void onConnection(MqttBrokerConnectionFn handler, void* context);
    void onClientEvent(MqttBrokerClientEventFn handler, void* context);

#if defined(MQTT_PLATFORM_HOST)
    // --- One broker per core; see the hook types above ---
    void onEgress(MqttBrokerEgressFn handler, void* context);
    void onPlace(MqttBrokerPlaceFn place, MqttBrokerHandOffFn handOff, void* context);

    // Told every routing key as subscriptions come and go; see
    // MqttRouteIndex::observe. A multi-core server uses it to send a message
    // only to the brokers that might want it. Survives begin().
    void onRouteKey(MqttRouteIndex::KeyObserver observer, void* context);

    // Shared groups across brokers: told of every member joining and leaving
    // (see MqttRouteIndex::observeShares), and asked, per message, whether a
    // group is delivered here (MqttBrokerShareGateFn). Survive begin().
    void onShareMember(MqttRouteIndex::ShareObserver observer, void* context);
    void onShareGate(MqttBrokerShareGateFn gate, void* context);

    // A party's outcome for a deferred PUBACK. When the last one has
    // reported, the acknowledgement goes out with every party's outcome
    // merged into the local one - 0x10 only if nobody anywhere matched, 0x97
    // if everybody who matched refused. A deferral whose connection has gone
    // is concluded silently: the client re-sends on its next session.
    //
    // `veto`, when non-zero, is a reason code that decides the
    // acknowledgement whatever else matched: a party that HAD to take the
    // message and could not (the Kafka pipe: 0x97 Quota exceeded, 0x83
    // Implementation specific error for a record too large for Kafka). The
    // first veto wins. At MQTT 3.1.1, which has no reason codes on PUBACK,
    // the acknowledgement is sent anyway and the veto is only counted.
    void concludeDeferred(uint64_t deferral, size_t matched, size_t dropped,
                          uint8_t veto = 0);

    // A message from outside any connection - the Kafka pipe's - with its
    // MQTT 5 properties, routed exactly as publish() routes one, and offered
    // to the egress hook with `message.origin`. Same answer as publish().
    bool publishMessage(const MqttBrokerRouted& message);

    // A message routed by another broker, routed here to this broker's own
    // sessions and stored if retained. Never offered to the egress hook - it
    // has already been everywhere it needs to go. Returns how many sessions
    // matched; `dropped` how many of those a full queue refused.
    size_t publishForwarded(const MqttBrokerRouted& message, size_t* dropped);

    // A connection handed over by another broker, with the bytes it had
    // already read. False if there is no slot for it, in which case the
    // caller still owns the transport.
    bool adopt(MqttTransport& transport, MqttListener* origin,
               const uint8_t* bytes, size_t length);
#endif

    // --- Polled state ---
    const MqttBrokerStatus& status() const { return _status; }
    const MqttBrokerConfig& config() const { return _config; }

    MqttRetainedStore& retained() { return _retained; }
    const MqttRetainedStore& retained() const { return _retained; }

    size_t connectionCount() const;
    size_t sessionCount() const;

    // Whether a client identifier has a live connection right now. For a
    // status page, and for a test that must know the takeover happened.
    bool isConnected(const char* clientId) const;

    // The session for a client identifier, or null. Public because a plant's
    // own code has every right to ask what a device is subscribed to.
    MqttBrokerSession* findSession(const char* clientId);

private:
    MqttBrokerConfig _config;
    MqttBrokerStatus _status;

    std::vector<MqttListener*> _listeners;
    std::vector<MqttBrokerConnection> _connections;
    std::vector<MqttBrokerSession> _sessions;
    MqttRetainedStore _retained;

    MqttBrokerAuthFn _onAuthorise = nullptr;
    void* _authoriseContext = nullptr;
    MqttBrokerTapFn _onTap = nullptr;
    void* _tapContext = nullptr;
    MqttBrokerConnectionFn _onConnection = nullptr;
    void* _connectionContext = nullptr;
    MqttBrokerClientEventFn _onClientEvent = nullptr;
    void* _clientEventContext = nullptr;
    void clientEvent(MqttBrokerClientEventKind kind, const MqttBrokerSession& session,
                     const MqttSpan& filter, uint8_t qos, uint8_t reasonCode,
                     bool resumed = false);

    // Assigned client identifiers are "auto-<n>". Per broker instance, never a
    // process-wide counter - R5, and two brokers in one test binary.
    uint32_t _nextAssignedId = 1;

    // Where the next poll starts its sweep of the connections.
    //
    // WITHOUT THIS, CONNECTION 0 ALWAYS WINS. Servicing the pool in index
    // order every time means the client that connected first has its packets
    // processed before everybody else's on every single pass - so under load
    // its acknowledgements are always first and the last slot's are always
    // last. Rotating the start gives every connection its turn at the front.
    //
    // It is not an ordering GUARANTEE and cannot be: MQTT orders packets
    // within a connection, never across, and which of two packets on two
    // sockets was processed first is not something a broker can know without
    // kernel timestamps. This only removes a systematic bias.
    size_t _sweepStart = 0;

    // --- Event-driven servicing; see useReadiness() ---
    //
    // Host only, like the readiness set itself: a board has no epoll, and
    // what it never uses it should not carry.
#if defined(MQTT_PLATFORM_HOST)
    bool _eventDriven = false;
    std::vector<uint32_t> _work;       // slots to service on the next pass
    std::vector<uint32_t> _working;    // the pass in progress
    size_t _liveConnections = 0;
    size_t _timerCursor = 0;           // where the deadline sweep resumes
    size_t _workRotation = 0;          // where the next pass starts, for fairness

    // Session deadlines - Will Delay and Session Expiry - kept on a wheel,
    // so a poll visits the sessions that are due rather than all of them.
    // The retained store's expiry and the session count are swept once a
    // second instead of every poll: both are measured in whole seconds.
    MqttTimerWheel _wheel;
    mq_time_t _housekeptAt = 0;

    // Sessions by Client Identifier, and the free session slots: what every
    // CONNECT used to find by walking the whole session table (L11).
    MqttSessionIndex _sessionIndex;

    // Candidate sessions by topic, kept one subscription at a time instead of
    // rebuilt whenever anything changes (L5, L6). See MqttRouteIndex.h.
    MqttRouteIndex _routeIndex;
    std::vector<uint32_t> _routeScratch;
    mq_time_t _sharePrunedAt = 0;
    uint32_t _sharePrunedRevision = 0;

    // Free SERVING connection slots, lowest on top - what freeConnection()
    // used to find by walking the pool from slot 0 on every accept. At
    // 130,000 connections that walk was most of a 20 ms poll and the accept
    // rate fell to a few hundred a second (L13). The refuse-only reserve slot
    // is never on it.
    std::vector<uint32_t> _freeConnections;
    void releaseConnectionSlot(MqttBrokerConnection& connection);

    // --- One broker per core ---
    MqttBrokerEgressFn _onEgress = nullptr;
    void* _egressContext = nullptr;
    MqttBrokerPlaceFn _onPlace = nullptr;
    MqttBrokerHandOffFn _onHandOff = nullptr;
    void* _placeContext = nullptr;
    MqttBrokerShareGateFn _shareGate = nullptr;
    void* _shareGateContext = nullptr;
    std::vector<uint8_t> _handOffBytes;
    struct Deferred {
        uint32_t slot = 0;
        uint32_t generation = 0;
        uint32_t stamp = 0;          // 0 means free
        uint32_t expected = 0;       // parties, once the egress hook has said; 0 until then
        uint32_t reported = 0;       // conclusions received, which may come first
        uint32_t nextFree = 0xFFFFFFFFu;
        uint16_t packetId = 0;
        uint8_t qos = 0;
        bool retainedFull = false;
        uint8_t veto = 0;            // the first party's refusal reason, if any
        size_t matched = 0;
        size_t dropped = 0;
    };
    std::vector<Deferred> _deferred;
    uint32_t _deferredFree = 0xFFFFFFFFu;
    uint32_t _deferredStamp = 0;
    uint32_t egress(const MqttSpan& topic, const MqttSpan& payload, uint8_t qos,
                    bool retain, const MqttProperties& properties,
                    const MqttSpan& clientId, size_t matched, size_t dropped,
                    uint64_t deferral, const void* origin = nullptr,
                    const MqttSpan& userName = MqttSpan());
    uint64_t openDeferred(MqttBrokerConnection& connection, uint16_t packetId,
                          uint8_t qos, size_t matched, size_t dropped, bool retainedFull);
    void closeDeferred(uint64_t deferral);
    void expectParties(uint64_t deferral, uint32_t parties);
    void finishDeferred(uint64_t deferral);
    void handOff(MqttBrokerConnection& connection, int target);
    static const std::string& identifierOf(const void* sessions, uint32_t index);
    enum : uint8_t { kWillTimer = 1, kExpiryTimer = 2 };
    void armWill(MqttBrokerSession& session, mq_time_t now);
    void armExpiry(MqttBrokerSession& session, mq_time_t now);
    static void onTimer(void* context, MqttTimer& timer, mq_time_t now);
    mq_time_t _timerSweptAt = 0;
    void queueWork(MqttBrokerConnection& connection);
    void wake(const MqttBrokerSession& session);
    void watch(MqttBrokerConnection& connection);
    void unwatch(MqttBrokerConnection& connection);
    void pollEvents(mq_time_t now);
    void sweepDeadlines(mq_time_t now);
    void afterService(MqttBrokerConnection& connection);
    uint32_t slotOf(const MqttBrokerConnection& connection) const {
        return (uint32_t)(&connection - _connections.data());
    }
#endif

    // [V5:4.8.2] "A Shared Subscription is a subscription that may have
    // multiple subscribing Clients... Each message is sent to one of the
    // subscribing Clients." Which one is the Server's choice; this rotates, so
    // a group of four gateways each does a quarter of the work rather than one
    // doing all of it.
    struct ShareCursor {
        std::string key;      // group name, a NUL, then the inner filter
        uint32_t next = 0;
    };
    std::vector<ShareCursor> _shareCursors;

    // --- The routing index ---
    //
    // route() walked EVERY session for every message, and inside each one
    // every subscription it held. That is the cost the benchmark found: at a
    // thousand sessions it was the largest single item in the broker's budget
    // even after the per-session scan was tightened.
    //
    // THE INDEX ONLY HAS TO BE A SUPERSET, which is what makes it safe to add
    // to this path. A candidate it produces is still checked with
    // mq_topicMatches() exactly as before, so an index that offers too many
    // sessions merely wastes work; it is an index that offers too FEW that
    // would lose a message, and the structure below cannot:
    //
    //   a filter with no wildcard matches one topic and one only, so it is
    //     bucketed by its own hash and found by the topic's hash;
    //   a filter with + or # is in the wildcard list and is always a
    //     candidate.
    //
    // Flat arrays in CSR form rather than a vector per bucket: built once when
    // the index is stale, read without allocating on the message path.
    std::vector<uint32_t> _bucketStart;      // bucket -> offset into _bucketSessions
    //
    // SESSION INDICES ARE 32 BITS. They were uint16_t, and a broker configured
    // for more than 65,535 sessions wrapped them silently: session 65,536 was
    // stored as session 0, and its messages were offered to whoever held slot
    // 0 - delivered to the wrong client if that client's filter matched too.
    // test_broker_scale routes to session 69,999 of 70,000 to keep it fixed.
    std::vector<uint32_t> _bucketSessions;   // session indices, grouped by bucket
    std::vector<uint32_t> _wildcardSessions;
    std::vector<uint32_t> _routeMark;        // generation stamp, to visit once
    uint32_t _routeGeneration = 0;

    // Bumped by the SESSIONS themselves; see attachSubscriptionRevision().
    uint32_t _subscriptionRevision = 0;
    uint32_t _indexBuiltAt = 0xFFFFFFFFu;

    std::vector<uint32_t> _routeCandidates;  // scratch, reused every route()

    void rebuildSubscriptionIndex();
    // Drops cursors for shared groups nobody is in any more; run when
    // subscriptions have changed. See the comment in the .cpp.
    void pruneShareCursors();
    void gatherCandidates(const MqttSpan& topic);
#if defined(MQTT_PLATFORM_HOST)
    MqttPollSet* _readiness = nullptr;
    bool shouldRead(const MqttBrokerConnection& connection) const;
#endif
    uint32_t nextInGroup(const std::string& key, uint32_t members);

    const MqttCodec& codecFor(MqttVersion version) const;

    // --- Connections ---
    void acceptNew();
    void serviceConnection(MqttBrokerConnection& connection);
    // The keepalive and connect deadlines, with no I/O. True if it closed.
    bool checkDeadlines(MqttBrokerConnection& connection, mq_time_t now);
    // Ends the session side of a connection and begins a lingering close: the
    // DISCONNECT goes out, the write side is shut down, and the socket is kept
    // for config.closeLingerMs so the peer can actually READ the reason it was
    // given. See MqttBrokerConfig::closeLingerMs.
    void closeConnection(MqttBrokerConnection& connection, uint8_t reasonCode,
                         bool sendDisconnect, bool publishWill);

    // Drains a lingering connection and releases it once the peer has gone or
    // the linger has elapsed.
    void finishClosing(MqttBrokerConnection& connection, mq_time_t now);
    MqttBrokerConnection* freeConnection();
    MqttBrokerConnection* connectionForSession(const MqttBrokerSession& session);

    // --- Packets in ---
    void handlePacket(MqttBrokerConnection& connection,
                      const MqttFixedHeader& header, const MqttPacket& packet);
    void handleConnect(MqttBrokerConnection& connection, const MqttPacket& packet);
    void handlePublish(MqttBrokerConnection& connection, const MqttPacket& packet);
    void handleAck(MqttBrokerConnection& connection, const MqttFixedHeader& header,
                   const MqttPacket& packet);
    void handleSubscribe(MqttBrokerConnection& connection, const MqttPacket& packet);
    void handleUnsubscribe(MqttBrokerConnection& connection, const MqttPacket& packet);
    void handleDisconnect(MqttBrokerConnection& connection, const MqttPacket& packet);

    // --- Packets out ---
    bool send(MqttBrokerConnection& connection, const MqttPacket& packet);
    bool sendConnAck(MqttBrokerConnection& connection, uint8_t reasonCode,
                     bool sessionPresent, const MqttSpan* assignedId);
    bool sendSimpleAck(MqttBrokerConnection& connection, uint8_t packetType,
                       uint16_t packetId, uint8_t reasonCode);
    void endRefusedQos2(MqttBrokerConnection& connection, uint16_t packetId,
                        uint8_t reasonCode);

    // --- Routing ---
    // Returns how many sessions the message was queued for, so a PUBACK can
    // carry 0x10 No Matching Subscribers [V5:3.4.2.1] - which is information a
    // commissioning engineer actually wants and almost no broker provides.
    // `droppedOut`, when given, receives how many of those matches a full
    // queue refused. The two numbers are different questions and the publisher
    // needs both: nobody listening is 0x10, and everybody listening being too
    // far behind to take it is 0x97 - which the configuration header has
    // always promised and this broker never sent.
    size_t route(const MqttSpan& topic, const MqttSpan& payload, uint8_t qos,
                 bool retain, const MqttProperties& properties,
                 const MqttBrokerSession* from, size_t* droppedOut = nullptr);
    void deliverQueued(MqttBrokerConnection& connection);
    // Queues retained messages owed to the session's new subscriptions, as
    // far as its queue has room, and remembers where each walk stopped.
    // True if it queued anything.
    bool continueRetained(MqttBrokerSession& session);

    // --- Sessions ---
    MqttBrokerSession* sessionFor(const MqttSpan& clientId);
    MqttBrokerSession* freeSession();
    size_t expireSessions(mq_time_t now);
    void serviceWills(mq_time_t now);
    void publishWill(MqttBrokerSession& session);

    bool authorise(MqttBrokerConnection& connection, MqttBrokerAction action,
                   const MqttSpan& topic, uint8_t qos, bool retain,
                   const MqttSpan& clientId, const MqttSpan& userName,
                   const MqttSpan& password, uint8_t& reasonCode,
                   std::string* login = nullptr);

    // The static trampoline the retained store visits through, and what it
    // needs to know. A file-static would be process-wide state, which R5
    // forbids and which two brokers in one test binary would corrupt.
    struct RetainedVisit {
        MqttBroker* broker;
        MqttBrokerSession* session;
        const MqttBrokerSubscription* subscription;
        bool queuedAny;
    };
    // False when the session's queue is full: that message was not taken,
    // and the walk resumes at it once there is room.
    static bool visitRetained(void* context, const MqttRetainedMessage& message,
                              uint32_t remainingExpiry, bool hasRemainingExpiry);
};


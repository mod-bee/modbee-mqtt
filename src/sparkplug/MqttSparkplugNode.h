// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// A SPARKPLUG EDGE NODE
// =============================================================================
//
// An L4 layer over MqttClient, the same level as MqttDevice and following the
// same rule from docs/ARCHITECTURE.md: A CONVENIENCE LAYER MAY CHOOSE A DEFAULT
// AND MAY NOT HIDE AN OUTCOME.
//
// What it chooses for you, because the specification does and a pin that can
// only hold one value is a lie on a rung:
//
//   - Clean Start true, Session Expiry 0 [tck-id-principles-persistence-clean-
//     session-50]. A configuration saying otherwise is REFUSED, not corrected.
//   - NBIRTH and NDATA at QoS 0, retain false [tck-id-topics-nbirth-mqtt],
//     [tck-id-topics-ndata-mqtt].
//   - The Will at QoS 1, retain FALSE [tck-id-payloads-ndeath-will-message-qos],
//     -retain. Note the difference from a Host Application's Will, which is
//     retain TRUE - the two are not the same and swapping them is a real bug.
//   - NCMD and DCMD subscribed at QoS 1, mandatory in both cases
//     [tck-id-message-flow-edge-node-ncmd-subscribe],
//     [tck-id-message-flow-device-dcmd-subscribe].
//
// WHAT IT WILL NOT DO. A metric that was not declared before the birth cannot
// be written: [tck-id-operational-behavior-data-publish-nbirth] requires the
// NBIRTH to carry every metric the node will ever publish, so the set is closed
// and a write to a name outside it is refused and counted. That is the API's
// central constraint and it is deliberate - the alternative is a node that
// publishes metrics a Host Application has never been told about, which a
// conformant one discards silently.
//
// THE SEQUENCE NUMBERS ARE PRIVATE. Exposing them would let a caller produce a
// non-conformant node, which is the thing this layer exists to prevent. They
// are readable - sequence(), birthDeathSequence() - because a function block
// needs to show them, and not writable.

#include "MqttSparkplugPayload.h"
#include "MqttSparkplugTopic.h"

#include "../MqttClient.h"
#include "../MqttFeatures.h"
#include "../MqttOwnedTransport.h"

#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include "../MqttFixed.h"

// A board's text fields (MQTT_LEAN, MqttFixed.h). Ids are already at most
// MQ_SP_MAX_ID everywhere. A metric name longer than its field is refused at
// declare(); units and text values are cut to theirs - a text value comes from
// a PLC STRING, 126 characters unless declared longer.
#if MQTT_LEAN
    #ifndef MQ_SP_POINT_NAME
    #define MQ_SP_POINT_NAME 96
    #endif
    #ifndef MQ_SP_POINT_UNITS
    #define MQ_SP_POINT_UNITS 16
    #endif
    #ifndef MQ_SP_POINT_DESCRIPTION
    #define MQ_SP_POINT_DESCRIPTION 64
    #endif
    #ifndef MQ_SP_POINT_TEXT
    #define MQ_SP_POINT_TEXT 126
    #endif
    #ifndef MQ_SP_WILL_BYTES
    #define MQ_SP_WILL_BYTES 64       // the NDEATH: one bdSeq metric
    #endif
#endif

// How many servers a node can fail over through.
//
// [tck-id-operational-behavior-primary-application-state-with-multiple-servers-walk]
// and -host-offline-reconnect require a node to move to the NEXT server in its
// list when the primary host goes offline, so a list is a feature rather than a
// configuration convenience. Three is the specification's own worked example.
#ifndef MQ_SP_MAX_SERVERS
#define MQ_SP_MAX_SERVERS 4
#endif

// Devices under one node: no fixed number, on a host or a board. The device
// table grows with addDevice() until memory runs out (outOfMemory()), or the
// 32767 an int16_t can number. (It was MQ_SP_MAX_DEVICES, 8.)

// --- What a command looks like when it arrives ---

// What became of one metric in an NCMD or DCMD.
//
// Every refusal used to be a counter and nothing else: commandsRefused() went
// up and the application could not say WHICH write, from which host, for what
// reason. A commissioning engineer whose setpoint "does nothing" needs exactly
// that, so each reason is named - and they are different faults: an unknown
// metric is a host with the wrong tag list, a read-only one is a host that
// thinks it can write a sensor, a wrong type is a host that ignored the birth.
enum class MqttSparkplugCommandOutcome : uint8_t {
    Applied = 0,     // written to the metric, or a rebirth performed
    Undecodable,     // the payload was not a Sparkplug payload at all
    UnknownDevice,   // a DCMD for a device this node never added
    UnknownMetric,   // a name or alias this node never declared
    ReadOnly,        // declared, but not with declareWritable()
    WrongType,       // a value that is not the declared datatype
    NotARequest,     // a Rebirth metric whose value is false, which asks nothing
};

const char* mq_sparkplugCommandOutcomeName(MqttSparkplugCommandOutcome outcome);

struct MqttSparkplugCommand {
    // Empty for an NCMD, the device id for a DCMD.
    const char* device = nullptr;

    // The name as WE declared it, not as it arrived - a command may carry only
    // an alias [tck-id-payloads-alias-data-cmd-requirement], and the caller
    // wants the name they wrote in their own code. Resolved through the alias
    // table before the handler sees it.
    //
    // For a REFUSED command the name may be one this node never declared, as
    // the host sent it - or null, when the host sent only an alias nothing
    // here answers to, or a payload that would not decode.
    const char* name = nullptr;

    uint64_t alias = 0;
    MqttSparkplugValue value;

    // True when this is Node Control/Rebirth or Device Control/Rebirth. Handled
    // internally before the handler runs - the node has already republished -
    // and surfaced so an application can log it.
    bool isRebirth = false;

    // Always Applied in what onCommand() hands over; the reason otherwise, in
    // what onCommandRefused() does.
    MqttSparkplugCommandOutcome outcome = MqttSparkplugCommandOutcome::Applied;
    bool applied() const { return outcome == MqttSparkplugCommandOutcome::Applied; }
};

typedef void (*MqttSparkplugCommandFn)(const MqttSparkplugCommand& command);
typedef void (*MqttSparkplugCommandContextFn)(void* context,
                                              const MqttSparkplugCommand& command);

// --- Writes waiting to be taken ---
//
// The writes to one metric that the application has not taken, oldest first:
// a ring of queueWrites() slots, made the first time the metric is written (on
// a board, when the node connects) - a metric nobody writes pays a null
// pointer.
//
// ONE BLOCK, in PSRAM where the board has some (mq_allocLarge): the values,
// and beside them, for a TEXT metric only, the text each one owns - a decoded
// STRING or BYTES value is a span into the receive buffer, which the next
// packet overwrites, so a value kept past the message it came in cannot be
// one. A number's slots carry no text. They each carried 127 characters, four
// to every writable metric whatever its type, and as four small allocations
// they went to a board's internal RAM first (MQX-027).
struct MqttSparkplugWriteQueue {
    MqttSparkplugValue* values = nullptr;          // `depth` of them
    MQ_STRING(MQ_SP_POINT_TEXT)* texts = nullptr;  // a text metric's; else null
    uint8_t depth = 0;
    uint8_t head = 0;
    uint8_t count = 0;
};

// --- What a text metric owns ---
//
// A STRING, TEXT, UUID, BYTES or FILE metric, and only one of those: made when
// it is declared, in PSRAM where the board has some. A number used to carry
// these two 127-character buffers too, empty, for ever (MQX-027).
struct MqttSparkplugPointText {
    // The value: a span, as MqttSparkplugValue's - into the application's
    // buffer (a PLC STRING), or into `command`.
    const uint8_t* data = nullptr;
    uint32_t length = 0;

    // Owns the text of what was last published. A text value is a span into
    // the application's buffer, and a span kept as "what was last published"
    // moved with that buffer: the comparison was the buffer with itself, and
    // a change that kept the length was never reported.
    MQ_STRING(MQ_SP_POINT_TEXT) published;

    // Owns the text of the value last written (or taken), so the value never
    // points into a receive buffer that has since been reused.
    MQ_STRING(MQ_SP_POINT_TEXT) command;
};

// --- One declared metric ---

// One value for publishRecords(): a declared metric (by index, as indexOf()
// answers), the value in its declared type, and the moment it belongs to.
struct MqttSparkplugRecord {
    size_t index = 0;
    MqttSparkplugValue value;
    uint64_t timestamp = 0;   // UTC milliseconds
};

// WHAT A METRIC IS - its name, its alias, what its birth says about it - and
// what is read only now and then: a deadband once a value has moved, a write
// queue when a host writes. Allocated with the others in PSRAM where the board
// has some.
//
// ITS VALUE IS NOT HERE. What a scan reads and writes - the value, the one
// last published, and the marks that decide whether it goes - is in the node's
// small state table, a cache line a metric (see MqttSparkplugNode, "TWO
// TABLES"). Read it with valueAt(), changePendingAt(), commandPendingAt() and
// isHeldAt(). (It was all one record, 608 bytes on a board, and a scan touched
// five of its cache lines to set one value - MQX-027.)
struct MqttSparkplugPoint {
    MQ_STRING(MQ_SP_POINT_NAME) name;
    MqttSparkplugDataType type = MqttSparkplugDataType::Unknown;

    uint64_t alias = 0;

    // Which device it belongs to, or -1 for a node-level metric. An index
    // rather than a name so the hot path is an integer compare.
    int16_t device = -1;

    // Whether a DCMD or NCMD may write it. A command for a read-only metric is
    // refused and counted rather than applied: a Host Application that thinks
    // it can write a sensor reading should find out.
    bool writable = false;

    // The display range, engLow and engHigh in the birth. Only when hasRange.
    bool hasRange = false;
    double engLow = 0.0;
    double engHigh = 0.0;

    // REPORT BY EXCEPTION, AND WHAT COUNTS AS AN EXCEPTION. A numeric metric
    // whose value has moved from the one last PUBLISHED by no more than
    // `deadband` (in its own units) or no more than `deadbandPercent` of that
    // published value is not "changed" for publishChanged(). Both zero - the
    // default - means any change at all, as the specification's
    // [tck-id-principles-rbe-recommended] has it; set both and a change must
    // clear both. Measured against what was last published, never against the
    // last scan, so a slow drift still adds up and goes. Not applied to
    // Boolean or text metrics, and not to a birth or publishAll(): those carry
    // every value as it is.
    double deadband = 0.0;
    double deadbandPercent = 0.0;

    // The text it owns - a text metric's only; null for a number.
    MqttSparkplugPointText* text = nullptr;

    // The writes not taken yet, when queueWrites() is on; see there.
    MqttSparkplugWriteQueue* queue = nullptr;

    // Writes dropped because the queue was full: the oldest goes, the newest
    // is kept, and each one dropped is counted here and in writesLost().
    uint32_t writesLost = 0;

    // Writes taken so far. A program that looks at the oldest write before
    // taking it (peekAt) compares this to know another caller took the one it
    // looked at, and that what it sees now is the next write.
    uint32_t writesTaken = 0;

    int32_t quality = MQ_SP_QUALITY_GOOD;
    MQ_STRING(MQ_SP_POINT_UNITS) units;
    MQ_STRING(MQ_SP_POINT_DESCRIPTION) description;
};

// --- The node ---

class MqttSparkplugNode : private MqttClientHook {
public:
    // --- Construction: the network, three ways, as MqttDevice takes it ---

    // On a board: the sketch's network channel - WiFiClient, EthernetClient,
    // TinyGsmClient, or WiFiClientSecure / SSLClient for TLS. It is yours and
    // must outlive the node; the node points it at each server in turn.
    template <typename ChannelType,
              typename = typename std::enable_if<
                  !std::is_base_of<MqttTransport, ChannelType>::value>::type>
    explicit MqttSparkplugNode(ChannelType& channel) : MqttSparkplugNode() {
        _net.adopt(channel);
    }

    // On a host: nothing to supply. Plain TCP, or TLS after useTls(). (On a
    // board this has no network, and connect() returns false.)
    MqttSparkplugNode() MQ_NOEXCEPT;

    // A transport you built and configured yourself. Never reconfigured here -
    // so it stays pointed where you pointed it, and a server LIST cannot walk
    // it. Use one of the constructors above to fail over between servers.
    explicit MqttSparkplugNode(MqttTransport& transport) MQ_NOEXCEPT;

    // ON A CLIENT SOMEBODY ELSE OWNS - the connection a device already has,
    // shared with its own topics and its firmware updates. One radio, one
    // login, one session.
    //
    // The client's owner keeps everything about the connection: where, how,
    // as whom, and whether to be connected at all. server(), credentials(),
    // keepAlive(), only() and the TLS calls do nothing here, and poll() does
    // not poll the client - its owner does. What Sparkplug needs of the
    // session it takes for itself, on every CONNECT: the Will (the NDEATH,
    // claimed with MqttClient::claimWill - refused if another layer owns it),
    // Clean Start, no session expiry, and a transmit buffer that holds the
    // birth certificate.
    //
    // connect() starts the node: when the session in progress was opened
    // without its Will, it restarts the client once so the next CONNECT
    // carries it. disconnect() publishes the NDEATH and stands the node down,
    // leaving the connection to everybody else. A primary host going offline
    // restarts the connection, as Sparkplug requires; a server list is the
    // owner's business.
    explicit MqttSparkplugNode(MqttClient& shared) MQ_NOEXCEPT;
    ~MqttSparkplugNode();

    MqttSparkplugNode(const MqttSparkplugNode&) = delete;
    MqttSparkplugNode& operator=(const MqttSparkplugNode&) = delete;

    // --- Identity. All before connect() ---

    // [tck-id-topic-structure-namespace-valid-group-id] Refused rather than
    // sanitised if it contains + / or #.
    bool group(const char* groupId);
    bool node(const char* edgeNodeId);

    void credentials(const char* user, const char* password);
    // No user name and no password on the next CONNECT.
    void clearCredentials();
    void keepAlive(uint16_t seconds);

    // Which protocol version to ask for. Both are legal for Sparkplug 3.0 -
    // [tck-id-principles-persistence-clean-session-311] and -50 cover each.
    // 5.0 by default, falling back, because 5.0 lets an intentional disconnect
    // carry reason 0x04 and have the SERVER publish the NDEATH
    // [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt50].
    void only(MqttVersion version);

    // --- The clock, and it is not optional ---
    //
    // [tck-id-payloads-timestamp-in-UTC] This timestamp MUST be in UTC.
    // [tck-id-topics-nbirth-timestamp] and its siblings make a timestamp
    // mandatory on every message type, so it cannot be omitted either.
    //
    // mq_millis() IS NOT THIS. It is a monotonic counter since boot - correct
    // for every deadline in this library and wrong here by about fifty-six
    // years. The Eclipse TCK caught it: "payloads-timestamp-in-UTC: FAIL",
    // which is exactly the class of fault our own tests could not find,
    // because they were written from the same reading of the specification as
    // the code.
    //
    // ON A HOST there is a system clock and the default uses it. ON A BOARD
    // there may be no clock at all - an ESP32 with no NTP and no RTC knows
    // only how long it has been running - so the epoch has to be supplied.
    // Call this once, with milliseconds since 1970-01-01 UTC, from whatever
    // the device does have: an NTP block, an RTC, a value the SCADA system
    // wrote down. The node then adds its own uptime to it.
    //
    // Without it on such a board, timestamps are boot-relative and the node is
    // NOT CONFORMANT - which clockIsSet() reports rather than hiding.
    void epochNow(uint64_t millisecondsSinceEpoch);
    bool clockIsSet() const;

    // --- Servers. One, or a list to fail over through ---

    // Replaces the list with a single entry.
    bool server(const char* host, uint16_t port = 0);

    // Appends. The list is walked IN ORDER, and a valid offline STATE from the
    // primary host moves to the next entry rather than retrying this one.
    bool addServer(const char* host, uint16_t port = 0);

    size_t serverCount() const { return _serverCount; }
    size_t activeServer() const { return _serverIndex; }

    // --- TLS, on a host ---
    //
    // On the transport the node made for itself (the no-argument
    // constructor), and before connect(). Each returns false otherwise: on a
    // board TLS is the channel's - WiFiClientSecure or SSLClient - and a
    // transport you brought is configured by you.
    //
    // A server added with port 0 gets 8883 once TLS is on.
#if MQTT_TLS_SOCKETS
    // Verify the broker against the system's certificate store.
    bool useTls(bool enabled = true);
    // Verify it against this CA file instead.
    bool trustFile(const char* path);
    // Trust exactly one broker key: its SHA-256 public-key fingerprint, 64
    // hexadecimal characters. What a plant broker nobody signed for wants;
    // docs/security.md, "Learning the key the first time", says how to read
    // it off the broker.
    bool pinnedKey(const char* fingerprint);
#endif

    // --- The primary host, if there is one. OFF by default ---
    //
    // Configured, the node will not publish its birth sequence until this host
    // publishes online=true [tck-id-operational-behavior-edge-node-birth-
    // sequence-wait], and a valid offline moves it to the next server
    // [tck-id-operational-behavior-edge-node-termination-host-offline-reconnect].
    //
    // Not configured, the node births as soon as the session is up. That is the
    // default because it is what a single-broker plant wants, and because a
    // node waiting for a host that does not exist looks exactly like a node
    // that is broken.
    void primaryHost(const char* sparkplugHostId);
    bool hasPrimaryHost() const { return !_primaryHostId.empty(); }
    bool primaryHostOnline() const { return _primaryHostOnline; }

    // --- Declaring metrics ---
    //
    // Returns the alias, or 0 on refusal - a full table, an invalid name, an
    // unsupported datatype, or a name already declared.
    //
    // ALIASES ARE ASSIGNED HERE AND START AT 1. Zero is the "no alias" value,
    // and [tck-id-payloads-alias-uniqueness] requires uniqueness across the
    // node's ENTIRE set of metrics - including every device's - so they come
    // from one counter rather than one per device.
    uint64_t declare(const char* name, MqttSparkplugDataType type);
    uint64_t declare(const char* name, MqttSparkplugDataType type,
                     const char* units, int32_t quality = MQ_SP_QUALITY_GOOD);

    // Declared writable, so an NCMD may set it. Still published as a metric -
    // a setpoint the node does not report back is a setpoint nobody can verify.
    uint64_t declareWritable(const char* name, MqttSparkplugDataType type);

    // The typed-layer spelling, for the OpenPLC wrapper: every MqttValueType
    // has a Sparkplug datatype, so a STRUCT member's declared IEC type is
    // enough and nothing is inferred from a first sample.
    uint64_t declareTyped(const char* name, MqttValueType type);

    size_t pointCount() const { return _pointCount; }
    const MqttSparkplugPoint* pointAt(size_t index) const;
    const MqttSparkplugPoint* findPoint(const char* name) const;
    const MqttSparkplugPoint* findPointFor(const char* deviceId,
                                           const char* name) const;

    // HOW MANY METRICS THIS NODE MAY DECLARE, node and devices together.
    //
    // No limit but memory, on a host or a board - as many as the program
    // declares, in PSRAM where a board has some; a declaration memory cannot
    // take is refused (0) and outOfMemory() says so. (It was 1024 on a host
    // and on a board with PSRAM, MQ_SP_MAX_METRICS_LARGE.) Nothing is
    // reserved up front: the tables grow with the declarations, and the
    // payloads and encode buffer are sized to the birth certificate when the
    // node first connects.
    size_t metricLimit() const;
    // Override it - for a board whose application knows better. Before the
    // declarations; it never refuses a metric already declared.
    void metricLimit(size_t limit) { _metricLimit = limit; }

    // The tables (metrics, devices, the birth's buffers) grow with the
    // program, and this says one of them could not: something was refused
    // for want of memory. For SPARKPLUG_NODE's LAST_ERROR.
    bool outOfMemory() const { return _outOfMemory; }

    // A DEADBAND on a numeric metric: see MqttSparkplugPoint::deadband. False
    // when the metric is not declared, is not numeric, or a value is negative.
    bool deadband(const char* name, double absolute, double percent = 0.0);
    bool deadbandFor(const char* deviceId, const char* name, double absolute,
                     double percent = 0.0);

    // Whether this metric would go in the next publishChanged() - it has
    // moved past its deadband since it was last published. A null or empty
    // device means a node metric. False for an undeclared one.
    bool changePending(const char* deviceId, const char* name) const;

    // --- By index, for a block that binds many metrics ---
    //
    // A STRUCT of a hundred members looked every one up by name, every scan:
    // ten thousand string compares to say what had changed. The index is
    // stable for the life of the node - metrics are never removed - so a
    // block looks each one up once, at declaration, and keeps the number.
    // Every call refuses an index past pointCount() rather than trusting it.
    //
    // A null or empty device means a node metric. -1 when not declared.
    //
    // setAt() answers whether the value CHANGED: false for the same value
    // again, and false when refused - an index past pointCount(), or a value
    // of another type (lastPayloadError() then says ValueTypeMismatch).
    int indexOf(const char* deviceId, const char* name) const;
    bool setAt(size_t index, const MqttSparkplugValue& value);
    bool changePendingAt(size_t index) const;
    bool commandPendingAt(size_t index) const;
    bool takeAt(size_t index, MqttSparkplugValue& into);

    // THE VALUE a metric holds now - what the next message would carry. A
    // text value is a span: into the application's buffer, or one the node
    // owns, good until the metric is next written. An index past pointCount()
    // answers a value of type Unknown.
    MqttSparkplugValue valueAt(size_t index) const;

    // --- Every write, in order: the write queue ---
    //
    // OFF (depth 0) BY DEFAULT, and then a write replaces the one before it:
    // the metric holds the latest value, and a host that writes 1 and then 0
    // faster than the application takes them is seen to write 0 only - the
    // command in the 1 is lost. That is what the onCommand() callback, which
    // sees every write as it arrives, needs nothing more than.
    //
    // ON, every write to a metric waits in a queue of `depth` until taken,
    // and each take() hands over the OLDEST - so a program that takes one per
    // scan sees each written value, in order, for a scan each. What a PLC
    // block wants, and the OpenPLC blocks turn it on. When the queue is full
    // the oldest write is dropped, the newest kept, and the drop counted
    // (writesLost(), writesLostAt()) - never silently. A write's value is the
    // metric's value from when it is TAKEN, and it is reported back then.
    //
    // Depth 1..16. Set it before the first write; a metric's queue keeps the
    // depth it was made with.
    void queueWrites(size_t depth);
    size_t writeQueueDepth() const { return _writeDepth; }

    // The oldest write waiting, without taking it - for a program that looks
    // before it decides. Its text stays valid until it is taken.
    bool peekAt(size_t index, MqttSparkplugValue& into) const;
    uint32_t writesLostAt(size_t index) const;
    uint32_t writesTakenAt(size_t index) const;
    uint32_t writesLost() const { return _writesLost; }
    // A numeric metric's deadband. A non-numeric one takes only 0, 0.
    bool deadbandAt(size_t index, double absolute, double percent);

    // HELD, or not: a held metric goes only when reportAt() asks for it, in a
    // birth, or with publishAll() - never with publishChanged() just because
    // it moved. For a set published on its own clock (every PERIOD, on
    // request), whose values would otherwise ride along in every other
    // metric's report-by-exception message.
    bool holdAt(size_t index, bool held);
    bool isHeldAt(size_t index) const;

    // Puts this metric in the next publishChanged(), changed or not, held or
    // not. A set published on its own clock marks its members and publishes:
    // one message with them and whatever else moved, and nothing of anyone
    // else's that did not.
    bool reportAt(size_t index);

    // What the birth says about a metric beside its value: engUnit (null or
    // "" for none), and the engLow/engHigh display range (low >= high for
    // none). Kept, and sent with the next birth - a host only reads them
    // there, so a change made after the birth shows at the next one.
    bool unitsAt(size_t index, const char* units);
    bool rangeAt(size_t index, double low, double high);

    // --- Devices ---

    bool addDevice(const char* deviceId);
    size_t deviceCount() const { return _deviceCount; }

    // A metric belonging to a device. Same alias space as the node's own.
    uint64_t declareFor(const char* deviceId, const char* name,
                        MqttSparkplugDataType type);
    uint64_t declareWritableFor(const char* deviceId, const char* name,
                                MqttSparkplugDataType type);

    // DBIRTH. [tck-id-message-flow-device-birth-publish-nbirth-wait] A device
    // can publish a DBIRTH only once an NBIRTH has been sent, so this is
    // deferred until the node has birthed rather than refused.
    bool deviceOnline(const char* deviceId);

    // DDEATH. [tck-id-operational-behavior-device-ddeath] Required when the
    // EDGE NODE loses contact with a device - not only when the node itself
    // dies. A DDEATH carries a sequence number, unlike an NDEATH.
    bool deviceOffline(const char* deviceId);

    bool deviceIsOnline(const char* deviceId) const;

    // --- Connecting ---

    bool connect();

    // MOVES THE NODE TO ANOTHER TRANSPORT - a settings change that needs a
    // different medium (plain to TLS, Wi-Fi to cellular, a new broker on a
    // board link). Every declaration, alias, device and the write queues stay;
    // the client keeps its counters. Whatever the old transport held is closed
    // without a death certificate of its own, so call disconnect() and let it
    // go first when the old broker should see one (with 5.0 the broker sends
    // the NDEATH Will on that DISCONNECT). The node is down until connect(),
    // which births again with the next bdSeq. The transport is the caller's,
    // as with the MqttTransport constructor: it is never re-pointed here.
    bool rebind(MqttTransport& transport);

    // Intentional. At 3.1.1 an NDEATH is published first
    // [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311];
    // at 5.0 the DISCONNECT carries reason 0x04 and the SERVER publishes it
    // [-disconnect-mqtt50]. The version decides, not the caller.
    void disconnect();

    void poll();

    // --- Values out ---

    bool set(const char* name, const MqttSparkplugValue& value);
    bool setFor(const char* deviceId, const char* name,
                const MqttSparkplugValue& value);

    // One NDATA per node and one DDATA per device carrying only what moved.
    // [tck-id-principles-rbe-recommended] is the report-by-exception
    // recommendation, and this is it.
    //
    // IT LOOKS ONLY AT WHAT WAS MARKED. A metric is marked when its value
    // changes (set(), setAt()), when a host's write is applied, and by
    // reportAt(); it stays marked while it differs from what was last
    // published - a move inside its deadband, a held metric - and leaves the
    // mark when it goes. So a call walks the metrics that moved, not all of
    // them: it walked every metric of the node once, and again for every
    // online device, on every call (MQX-026).
    bool publishChanged();

    // ONE MESSAGE A POLL, for a program that sets its values in many places.
    // Owes one publishChanged() to the next poll(): every part of the program
    // with something to say asks, and the node then sends ONE NDATA, and one
    // DDATA per online device, carrying everything that moved - where each
    // asking part used to send its own (MQX-026). Asking twice before the poll
    // is asking once.
    //
    // pollPublishes() counts the ones poll() has made, and pollPublishSent()
    // says whether the last of them went (publishChanged()'s answer). A caller
    // that asked keeps the count from when it asked; once the count has moved,
    // its message has been made and pollPublishSent() is its result.
    void publishAtPoll() { _publishOwed = true; }
    uint32_t pollPublishes() const { return _pollPublishes; }
    bool pollPublishSent() const { return _pollPublishSent; }

    // Every metric, changed or not. What a recipe download wants.
    bool publishAll();

    // VALUES OUTSIDE REPORT BY EXCEPTION, EACH WITH ITS OWN TIMESTAMP.
    //
    // For what must reach the host even when its value repeats - an event,
    // a day's total - and for store-and-forward. All the records belong to one
    // device, or all to the node; that is what the first one says.
    //
    // `historical` sets is_historical on every metric, and that is the
    // specification's store-and-forward: the host files each value at its
    // own timestamp and does NOT update the live tag. Not historical, each
    // value becomes the metric's live value and its last published one, as if
    // publishChanged() had sent it.
    //
    // False when not born, while a rebirth is pending, for a record naming a
    // metric of another device or a value of the wrong type, or when nothing
    // could be queued. Published at QoS 0 like every NDATA/DDATA, so delivery
    // is the broker's best effort: a caller that must not lose a record keeps
    // it until it has seen the node stay born afterwards.
    bool publishRecords(const MqttSparkplugRecord* records, size_t count, bool historical);

    // The timestamp this node stamps a message with now: UTC milliseconds, or
    // uptime while no clock is set (see clockIsSet()).
    uint64_t timestamp() const { return utcNow(); }

    // Republish the whole birth sequence: NBIRTH at seq 0, then every online
    // device's DBIRTH. What a Node Control/Rebirth NCMD triggers, and callable
    // directly.
    bool rebirth();

    // --- Commands in. Callback AND pollable, because R4 ---

    void onCommand(MqttSparkplugCommandFn handler);
    void onCommand(MqttSparkplugCommandContextFn handler, void* context);

    // EVERY METRIC THAT WAS NOT APPLIED, one call each, with the reason in
    // command.outcome. Separate from onCommand() rather than folded into it,
    // so an application that acts on what its handler is given can never act
    // on a write the node refused. Between the two, every metric of every
    // NCMD and DCMD is seen exactly once - which is what a log, a test rig or
    // an audit trail wants. commandsRefused() still counts them.
    void onCommandRefused(MqttSparkplugCommandFn handler);
    void onCommandRefused(MqttSparkplugCommandContextFn handler, void* context);

    // The pollable half. Generated IEC code cannot register a lambda.
    //
    // A NODE metric. A command for a metric under a DEVICE (a DCMD) is held on
    // that device's metric and is reached with the ...For() pair - a name is
    // only unique within its device, so asking for it without one cannot be
    // answered. Before these existed a DCMD was applied, counted, and then
    // never handed to anyone.
    bool commandPending(const char* name) const;
    bool take(const char* name, MqttSparkplugValue& into);
    bool commandPendingFor(const char* deviceId, const char* name) const;
    bool takeFor(const char* deviceId, const char* name,
                 MqttSparkplugValue& into);

    // --- Knowing what is going on ---

    bool isConnected() const { return _client.isConnected(); }

    // True once the NBIRTH has gone. Not the same as isConnected(): a node
    // waiting on a primary host is connected and not born.
    bool isBorn() const { return _born; }

    uint8_t sequence() const { return _seq; }
    uint8_t birthDeathSequence() const { return _bdSeq; }

    const MqttStatus& status() const { return _client.status(); }
    const char* lastErrorText() const;

    // WHY A CONNECTED NODE HAS NOT BIRTHED, in an operator's words - or "" when
    // it has, or when it is not connected (lastErrorText() covers that).
    //
    // Connected-and-not-born is ordinary for a moment and a fault if it lasts,
    // and the two look identical on a panel unless somebody says which: waiting
    // for a primary host that is down, a broker that refused the subscription
    // the node needs to hear that host, or a birth certificate that would not
    // go. All three used to show as a healthy link with nothing to say.
    const char* waitingFor() const;
    MqttSparkplugError lastPayloadError() const { return _lastPayloadError; }

    // Commands refused because the metric was unknown or read-only. Counted
    // rather than dropped: a Host Application writing to the wrong name shows
    // up here and nowhere else.
    uint32_t commandsRefused() const { return _commandsRefused; }
    uint32_t commandsApplied() const { return _commandsApplied; }

    // Births and deaths published, for a function block to show.
    uint32_t birthCount() const { return _birthCount; }

    // The whole library, when this is not enough.
    MqttClient& client() { return _client; }

    // Built on a client somebody else owns.
    bool sharesClient() const { return _shared; }
private:
    struct Server {
        MQ_STRING(MQTT_CONFIG_HOST) host;
        uint16_t port = 0;
    };

    // --- TWO TABLES ---
    //
    // A metric is two records. MqttSparkplugPoint is what it IS - its name,
    // alias, units, range, deadband, write queue - read when it is declared,
    // when it births, when a host writes it, and when a moved value is
    // checked against its deadband. PointState is what a scan reads and
    // writes: the value, the one last published, and the marks that decide
    // whether it goes. 32 bytes, one cache line on an ESP32, where the old
    // single record spread those over five lines of 608 bytes in PSRAM.
    //
    // A text metric's value is in its MqttSparkplugPointText (kText): the
    // span does not fit in 8 bytes on a 64-bit host, and a text value is read
    // with its text anyway.
    struct PointState {
        // A number's or a Boolean's value as the bits differsFrom() compares
        // - a Float's four, a Boolean as 0 or 1, the rest all eight - so a
        // change is two integers that differ. 0 while null, and for a text
        // metric.
        uint64_t value = 0;
        uint64_t published = 0;
        // The next metric on its slot's change list, as index + 1; 0 ends it.
        uint32_t nextChanged = 0;
        int16_t device = -1;
        uint8_t type = 0;          // MqttSparkplugDataType
        uint8_t spare = 0;
        uint16_t flags = 0;
    };
    static constexpr uint16_t kValueNull = 1u << 0;
    static constexpr uint16_t kPublishedNull = 1u << 1;
    // What was last actually published is known, so publishChanged() can
    // tell. Not before the first birth.
    static constexpr uint16_t kEverPublished = 1u << 2;
    // A COMMANDED VALUE IS ALWAYS REPORTED BACK, deadband or not. Set when an
    // NCMD or DCMD writes the point (or reportAt() asks), cleared when it is
    // published. Without it a setpoint written 0.3 away from the last report,
    // under a 0.5 deadband, was never echoed - and the host, which watches for
    // the echo, decided the write had failed.
    static constexpr uint16_t kReportNext = 1u << 3;
    // HELD: reported only when asked (reportAt), in a birth, or by
    // publishAll() - never by publishChanged() just for having moved. See
    // holdAt().
    static constexpr uint16_t kHeld = 1u << 4;
    // A command has written it and the application has not taken it yet. The
    // pollable half of R4.
    static constexpr uint16_t kCommandPending = 1u << 5;
    // On its slot's change list.
    static constexpr uint16_t kListed = 1u << 6;
    // A deadband is set: only then is the cold record read for it.
    static constexpr uint16_t kDeadband = 1u << 7;
    // A text metric: its value is in its MqttSparkplugPointText.
    static constexpr uint16_t kText = 1u << 8;

    // THE METRICS THAT MAY HAVE TO GO, one list a slot - the node's own, and
    // each device's - threaded through PointState::nextChanged. A metric is
    // put on when its value changes, a host writes it or reportAt() asks, and
    // stays on while it differs from what was last published (a move inside
    // its deadband, a held metric), so publishChanged() looks at those and
    // nothing else. In order of index unless `sorted` says otherwise.
    struct ChangeList {
        uint32_t head = 0;   // index + 1; 0 for an empty list
        uint32_t tail = 0;
        bool sorted = true;
    };

    struct Device {
        MQ_STRING(MQ_SP_MAX_ID) id;
        bool online = false;
        // Whether a DBIRTH is owed once the node births. deviceOnline() before
        // the node is born sets this rather than failing.
        bool wantsBirth = false;
        ChangeList changes;
    };

    // --- Session wiring ---
    void beginClient();
#if MQTT_LEAN
    void prepareWriteQueues();
#endif
    void applyConnectionConfig();
    bool publishBirth();
    bool publishDeviceBirth(Device& device);
    void deviceBirthRefused(bool refused);
    bool publishData(int16_t device, bool onlyChanged);
    bool publishNodeDeath();
    bool subscribeCommands();

    void advanceServer();
    void nextSequence();
    void endBirth();

    // --- Payload assembly ---
    bool buildPayload(MqttSparkplugPayload& payload, MqttSparkplugType type,
                      int16_t device, bool onlyChanged, bool includeBirthExtras);
    bool addPointMetric(MqttSparkplugPayload& payload, size_t index,
                        bool isBirth);
    bool sendPayload(MqttSparkplugType type, const char* deviceId,
                     const MqttSparkplugPayload& payload, uint8_t qos,
                     bool retain, bool asWill);

    // --- Inbound ---
    void handleMessage(const MqttMessage& message);
    void handleCommand(const MqttSparkplugTopicParts& parts,
                       const uint8_t* payload, size_t length);
    void handleState(const uint8_t* payload, size_t length);
    void deliverCommand(MqttSparkplugCommand& command);
    void refuseCommand(const char* device, const char* name, uint64_t alias,
                       const MqttSparkplugValue& value,
                       MqttSparkplugCommandOutcome outcome);

    int findPointIndex(const char* name, int16_t device) const;
    void indexPoint(size_t index);
    int findPointByAlias(uint64_t alias) const;
    int findDeviceIndex(const char* deviceId) const;

    uint64_t declareInternal(const char* name, MqttSparkplugDataType type,
                             int16_t device, bool writable,
                             const char* units, int32_t quality);

    // The registry grows as metrics are declared - see metricLimit().
    bool growPoints(size_t wanted);

    // Whether a point is "changed" for report by exception: its value differs
    // from the one last published by more than its deadband.
    bool pointChanged(size_t index) const;
    // Whether it must stay on its change list: never published, a report
    // owed, or a value other than the one last published.
    bool pointOwed(size_t index) const;

    // --- The two tables (see TWO TABLES) ---
    MqttSparkplugValue stateValue(size_t index) const;
    MqttSparkplugValue publishedValue(size_t index) const;
    // A value into the state table as it is: a text value's span kept.
    void storeValue(size_t index, const MqttSparkplugValue& value);
    // A host's value: its text copied into the metric's own storage first.
    void storeCommand(size_t index, const MqttSparkplugValue& value);
    // The value becomes what was last published; the report owed is paid.
    void markPublished(size_t index);
    // The declared type wins; whether the value changed, in `changed`.
    bool putValue(size_t index, const MqttSparkplugValue& value, bool& changed);

    ChangeList& changesOf(int16_t device);
    void markChanged(size_t index);
    void sortChanges(ChangeList& list);
    // After a message (`published`: the changed ones went) or a look that
    // found nothing to send: what still differs stays, the rest leaves.
    void settleChanges(ChangeList& list, bool published);
    // A birth carried them all: the list is empty.
    void forgetChanges(ChangeList& list);

    bool makeQueue(size_t index);
    static void freeQueue(MqttSparkplugWriteQueue* queue);
    static void freeText(MqttSparkplugPointText* text);

    // publishAtPoll(), paid.
    void publishOwed();

    // The payloads, the encode buffer and the client's transmit buffer, sized
    // to this node's birth certificate. Called by connect(), before the client
    // exists the first time, and before every birth.
    bool sizeBuffers();
    bool ensureScratch(size_t bytes);

    // A host's write to a point: the latest value, or one more in its queue.
    void acceptWrite(size_t index, const MqttSparkplugValue& value);

    static void deathTrampoline(void* context, MqttResult result,
                                uint16_t packetId);
    static void stateSubscribed(void* context, MqttResult result, uint16_t packetId);
    static void commandSubscribed(void* context, MqttResult result, uint16_t packetId);
    // --- The client's events, as a layer on it (MqttClientHook) ---
    void prepareConnect(MqttConnectionConfig& connection) override;
    void sessionEstablished(bool sessionPresent) override;
    void stateChanged(MqttState state, MqttResult reason) override;
    bool message(const MqttMessage& message) override;

    // The numbers a new session starts with: bdSeq onwards, seq from zero.
    void nextSession();
    // Ends this session so the next one births afresh: the NDEATH, then a new
    // CONNECT - the node's own, or a restart of a shared client.
    void cycleSession();
    bool startShared();
    void restartShared(uint8_t reasonCode);
    // The client: the node's own, or a shared one (see the constructor).
    MqttClient _ownClient;
    MqttClient& _client;
    bool _shared = false;

    // The node wants to be online: connect() until disconnect().
    bool _active = false;
    // The CONNECT that opened this session carried the node's Will.
    bool _willArmed = false;
    // A shared client must restart, from poll(): the session came up without
    // the Will, or the birth certificate outgrew the transmit buffer.
    bool _restartOwed = false;
    // Another layer owns the shared client's Will.
    bool _willRefused = false;
    // A shared session to end from poll(): the primary host went offline, or
    // came back as a new session (cycleSession); the 3.1.1 NDEATH concluded.
    bool _cycleOwed = false;
    bool _restartAfterDeath = false;
    MqttOwnedTransport _net;
    bool _begun = false;

    MqttConnectionConfig _connection;
    MqttClientConfig _behaviour;

    MQ_STRING(MQ_SP_MAX_ID) _group;
    MQ_STRING(MQ_SP_MAX_ID) _node;

    Server _servers[MQ_SP_MAX_SERVERS];
    size_t _serverCount = 0;
    size_t _serverIndex = 0;

    MQ_STRING(MQ_SP_MAX_TOPIC) _primaryHostId;
    bool _primaryHostOnline = false;

    // The last online=true timestamp seen from the primary host.
    //
    // THIS FIELD IS THE WHOLE OF [tck-id-operational-behavior-edge-node-
    // termination-host-offline-timestamp], and it is the subtlest requirement
    // in the specification. An offline STATE is only believed if its timestamp
    // is at or after this: an OLD host death certificate can be delivered after
    // a new host session is already up, and a node that believed it would tear
    // itself down every time the host reconnected.
    uint64_t _primaryHostOnlineAt = 0;
    bool _sawPrimaryHostOnline = false;

    // The last STATE timestamp accepted as current, of EITHER kind.
    //
    // A SEPARATE FIELD FROM _primaryHostOnlineAt ABOVE, and the difference is
    // not an oversight. The specification states two comparisons and they have
    // different right-hand sides:
    //
    //   [tck-id-message-flow-edge-node-birth-publish-phid-wait-timestamp]
    //   deciding the host IS online compares against "the previous STATE
    //   message timestamp value" - any STATE message, online or offline.
    //
    //   [tck-id-operational-behavior-edge-node-termination-host-offline]
    //   deciding to leave the session compares against "the previous ONLINE
    //   STATE message timestamp value" - only an online one.
    //
    // One field cannot serve both. This is the first; _primaryHostOnlineAt is
    // the second.
    uint64_t _lastStateAt = 0;
    bool _sawState = false;

    // Allocated (mq_allocLarge, so in PSRAM where there is some) and grown as
    // metrics are declared. A node with five metrics pays for five. Two
    // tables, one entry each a metric: see TWO TABLES. The state table starts
    // on a 32-byte boundary, so each entry is one cache line on an ESP32;
    // _stateBlock is the allocation it is in.
    MqttSparkplugPoint* _points = nullptr;
    PointState* _state = nullptr;
    void* _stateBlock = nullptr;
    ChangeList _nodeChanges;
    size_t _pointCount = 0;
    size_t _pointCapacity = 0;
    // NAME TO INDEX, for findPointIndex(): open addressing on the name and
    // device, each slot a point's index + 1 (0 empty), at most half full, and
    // grown with the points. Declaring used to search every metric declared
    // before it, so a thousand-metric program made half a million name
    // compares in its first scan. Null (0 slots) when it could not be had, and
    // then the search is the plain one.
    uint32_t* _pointIndex = nullptr;
    size_t _pointSlots = 0;
    size_t _metricLimit = 0;   // 0: the platform's default, metricLimit()
    size_t _writeDepth = 0;    // queueWrites(); 0 = the latest write wins
    uint32_t _writesLost = 0;
    uint64_t _nextAlias = 1;

    MqttGrowList<Device> _devices;
    bool _outOfMemory = false;
    size_t _deviceCount = 0;

    // [tck-id-topics-nbirth-bdseq-increment] The bdSeq number MUST start at
    // zero and increment by one on each new MQTT CONNECT.
    uint8_t _bdSeq = 0;
    bool _bdSeqUsed = false;

    // [tck-id-topics-nbirth-seq-num] 0 in the NBIRTH, then +1 per message.
    uint8_t _seq = 0;

    bool _born = false;
    bool _aliasesEnabled = true;

    // publishAtPoll(): owed, how many poll() has made, and the last answer.
    bool _publishOwed = false;
    bool _pollPublishSent = false;
    uint32_t _pollPublishes = 0;

    // OWED, AND RETRIED - not attempted once and forgotten.
    //
    // A birth that failed (queue full, a payload too large) or a subscription
    // the client refused locally used to be tried exactly once, at the moment
    // that triggered it. The node then sat connected and unborn for the life of
    // the session: a further online STATE changed nothing because the host was
    // already "online", and nothing else would ever try again. poll() now
    // retries both, no more than once a second.
    bool _birthOwed = false;
    bool _subscriptionsOwed = false;
    mq_time_t _owedTriedAt = 0;

    // AN OWED DBIRTH WAITS between tries. A DBIRTH refused for want of queue
    // room was rebuilt - every metric of the device - and refused again on
    // every poll, which is every PLC scan, for as long as the queue stayed
    // full. Now, like the owed NBIRTH, no more than once a second.
    bool _deviceBirthWaiting = false;
    mq_time_t _deviceBirthTriedAt = 0;

    // A SUBACK refusal is the broker's answer, not ours, and retrying it would
    // only be refused again - so it is recorded for waitingFor() to report.
    // Topic-specific because "the broker refused the STATE subscription" sends
    // an engineer to the ACL; "something failed" sends them nowhere.
    // WHAT THE PRIMARY HOST HAS SAID, for waitingFor() to report. "Waiting
    // for the host" covers three different faults - its STATE never reached
    // us, it says offline, or what arrived was older than what we had - and
    // each one sends an engineer somewhere different.
    uint32_t _stateMessages = 0;
    uint32_t _stateStale = 0;
    bool _lastStateOnline = false;

    MqttResult _stateSubscribeRefused = MqttResult::Ok;
    MqttResult _commandSubscribeRefused = MqttResult::Ok;
    mutable char _waitText[160];

    // A 3.1.1 disconnect is TWO steps and they must not be reordered.
    //
    // [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311] says
    // the NDEATH must reach the server BEFORE the DISCONNECT packet, because a
    // DISCONNECT tells the server not to publish the Will. publishSeries()
    // QUEUES, so sending the DISCONNECT on the next line discarded the death
    // certificate - a conformance failure that looked like a clean shutdown.
    // So the publish carries a completion handler and the DISCONNECT goes when
    // the PUBACK arrives.
    bool _disconnectAfterDeath = false;

    // When the death certificate was asked for, so the deferred disconnect
    // above it has a bound. disconnect() has to mean disconnect even if the
    // acknowledgement never arrives.
    mq_time_t _deathAskedAt = 0;

    // A valid offline STATE means DISCONNECT AND GO SOMEWHERE ELSE, and the
    // second half needs driving.
    //
    // [tck-id-operational-behavior-edge-node-termination-host-offline-reconnect]
    // "it MUST attempt to connect to the NEXT MQTT Server in its connection
    // list to start the session establishment procedure over again."
    //
    // The client's own autoReconnect does not cover this: our disconnect is
    // INTENTIONAL, and an intentional disconnect concludes as Cancelled, which
    // the reconnect path deliberately does not retry - otherwise disconnect()
    // would not mean disconnect. So the node reconnects itself, and this flag
    // is how poll() knows to.
    //
    // Found by the Eclipse TCK's PrimaryHostTest, which watched the node go
    // offline and never come back.
    bool _reconnectAfterOffline = false;

    // A REBIRTH MEANS STOP TALKING FIRST.
    //
    // [tck-id-operational-behavior-data-commands-rebirth-action-1] "When an
    // Edge Node receives a Rebirth Request, it MUST IMMEDIATELY STOP SENDING
    // DATA MESSAGES." Only then -action-2: "it MUST send a complete BIRTH
    // sequence".
    //
    // Which is not automatic, because publishSeries() QUEUES. A data message
    // accepted a moment before the request arrives is already in the send
    // queue and drains after the NBIRTH, so the far end sees data between the
    // request and the birth. The Eclipse TCK's ReceiveCommandTest is what
    // noticed.
    //
    // So: this goes true the instant a rebirth request is seen, and no data is
    // accepted until the birth sequence has been acknowledged as sent. It
    // cannot retract what is already queued - nothing can - but it stops the
    // node adding to it, which is what "immediately stop" means for a layer
    // that does not own the queue.
    bool _rebirthPending = false;

    // The wall clock. `_epochAtBoot` is UTC milliseconds at the moment
    // mq_millis() read `_epochSetAt`, so now() is a subtraction away.
    uint64_t _epochAtBoot = 0;
    mq_time_t _epochSetAt = 0;
    bool _epochSet = false;

    // UTC milliseconds now, for a payload timestamp.
    uint64_t utcNow() const;

    MqttSparkplugError _lastPayloadError = MqttSparkplugError::None;
    uint32_t _commandsRefused = 0;
    uint32_t _commandsApplied = 0;
    uint32_t _birthCount = 0;

    MqttSparkplugCommandFn _commandHandler = nullptr;
    MqttSparkplugCommandContextFn _commandContextHandler = nullptr;
    void* _commandContext = nullptr;
    MqttSparkplugCommandFn _refusedHandler = nullptr;
    MqttSparkplugCommandContextFn _refusedContextHandler = nullptr;
    void* _refusedContext = nullptr;

    // The Will bytes have to survive from before the CONNECT until the server
    // uses them, so they are held here rather than built on the stack.
    MQ_BYTES(MQ_SP_WILL_BYTES) _willBytes;

    // One scratch buffer for outbound payloads, grown to the largest message
    // this node has had to encode - which is its birth certificate, sized
    // exactly by sizeBuffers() before the first one goes. It was a fixed
    // 120 bytes a metric inside the node, which is a guess, and a guess that
    // was 4 kB of internal RAM on every board.
    uint8_t* _scratch = nullptr;
    size_t _scratchSize = 0;

    // One decode payload, reused, sized for a command that writes every
    // writable metric at once. A member rather than a stack local because it
    // carries its own name arena.
    MqttSparkplugPayload _inbound{(size_t)0, (size_t)0};

    // Assembled outbound payload, sized to the birth certificate.
    MqttSparkplugPayload _outbound{(size_t)0, (size_t)0};

    char _topic[MQ_SP_MAX_TOPIC];
};

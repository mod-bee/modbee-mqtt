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
// MqttDevice - THE SIMPLE WAY IN
// =============================================================================
//
// Everything this library can do is reachable through MqttClient and a
// transport. That is the powerful form, it is fully documented in docs/api.md,
// and nothing here replaces it.
//
// This is the other form: ONE OBJECT, sensible defaults, and the fiddly parts
// done for you.
//
//     EthernetClient net;                       // or WiFiClient, or TinyGsmClient
//     MqttDevice mqtt(net);
//
//     void setup() {
//         Ethernet.begin(mac);
//         mqtt.statusTopic("plant/tank-01/status");        // online / offline, for you
//         mqtt.onMessage("plant/tank-01/pump", onPumpCommand);
//         mqtt.report("plant/tank-01/level", &level, 0.02, 30000);
//         mqtt.connect("192.168.1.10", "tank-01");
//     }
//
//     void loop() {
//         mqtt.poll();
//         level = readTheSensor();      // reported by the library, not here
//     }
//
// WHAT IT DOES FOR YOU, that the long form leaves to the caller:
//
//   - builds the transport, so there is one object rather than three
//   - talks 5.0 and falls back to 3.1.1 by itself, so it works with any broker
//   - routes each message to the handler for the filter it matched, instead of
//     handing you one callback and a topic to compare by hand
//   - hands you a NUL-TERMINATED payload and the conversions to read it
//   - publishes "online" retained when the session comes up, and registers
//     "offline" as the Will - the two halves of an availability topic that are
//     easy to get half-right
//   - says offline properly on a deliberate disconnect, which the Will never
//     does [V5:3.1.2.5], and waits for it to reach the wire before closing
//   - publishes a variable when it moves far enough, and anyway when it has
//     been quiet too long - report(), which is the ten lines of deadband and
//     timer every telemetry sketch was otherwise carrying
//   - answers "may I act on this value", which MQTT itself cannot: a connected
//     broker will serve a retained reading from a device that died last week
//
// WHAT IT DOES NOT DO. It does not block, and it does not hide anything:
// client() returns the real MqttClient, and everything you can reach from
// there works exactly as it does in the long form. Start here, and drop down
// when you need to; nothing you wrote has to be rewritten to do it.
//
// L4. The only other file besides MqttClient that knows which end of a
// conversation it is - see scripts/check-layering.py.

#include "MqttFeatures.h"   // MQTT_TLS_SOCKETS: an undefined macro is 0 in #if
#include "MqttClient.h"
#include "MqttConfig.h"
#include "MqttOwnedProperties.h"
#include "MqttTransport.h"
#include "transport/MqttStreamTransport.h"
#include "transport/MqttTcpTransport.h"
#include "transport/MqttTlsTransport.h"

#include <new>
#include <string>
#include <type_traits>

// How many distinct filters one device may subscribe to.
//
// A fixed array rather than a growing one: the slots must not move, because a
// subscription's acknowledgement is concluded against the slot that asked for
// it - and a device with no spare heap should not be allocating per
// subscription anyway. Raise it with a build flag if sixteen is not enough.
#ifndef MQTT_DEVICE_MAX_SUBSCRIPTIONS
#define MQTT_DEVICE_MAX_SUBSCRIPTIONS 16
#endif

// How many variables one device may report on change. Same reasoning as above:
// fixed, so nothing moves and nothing allocates per variable.
#ifndef MQTT_DEVICE_MAX_REPORTS
#define MQTT_DEVICE_MAX_REPORTS 16
#endif

// =============================================================================
// WHAT ARRIVED
// =============================================================================
//
// MQTT payloads are bytes and the protocol says nothing about what they mean.
// This wraps the bytes with the handful of readings an application actually
// wants, and refuses rather than guesses when the bytes are not that.
class MqttPayload {
public:
    MqttPayload(const MqttMessage& message, const char* text, size_t length,
                const MqttOwnedProperties* properties)
        : _message(message), _text(text), _length(length),
          _properties(properties) {}

    // The payload exactly as it arrived. Valid ONLY for the duration of the
    // handler - it points into the receive buffer, and the next poll() will
    // overwrite it. Copy anything that must outlive the call.
    const uint8_t* bytes() const { return _message.payload.data; }
    size_t length() const { return _length; }
    bool isEmpty() const { return _length == 0; }

    // The same bytes, NUL-terminated, so they can be handed to strcmp, atoi,
    // Serial.println or anything else that expects a C string. Never null.
    //
    // A payload containing a zero byte reads as ending there. bytes() and
    // length() are the whole of it, always - said plainly because a silent
    // half-payload is worse than no payload at all.
    const char* text() const { return _text; }

    uint8_t qos() const { return _message.qos; }

    // True when the broker served this from its retained store rather than
    // because somebody published it just now [V5:3.3.1.3]. A retained message
    // may be minutes or months old: it is the last thing said, not proof that
    // anybody is still there to say it.
    bool retained() const { return _message.retain; }

    // Everything else, including the 5.0 properties. Null properties on a
    // 3.1.1 connection, where there are none.
    const MqttMessage& message() const { return _message; }

    bool equals(const char* other) const;

    // --- What the sender said ABOUT the message, at 5.0 ---
    //
    // All null or zero on a 3.1.1 connection, where none of this exists, and
    // on any 5.0 message that simply did not carry them - which is most.

    // [V5:3.3.2.3.5] Where the sender wants the answer published. Null when
    // there is none, which is the same as "this is not a request".
    const char* responseTopic() const;

    // [V5:3.3.2.3.6] Bytes the requester will use to match the answer to the
    // question. Never interpreted here or by the broker - just carried back.
    const uint8_t* correlationData() const;
    size_t correlationLength() const;

    // [V5:3.3.2.3.9] A MIME type, "application/json" and the like. Null when
    // the sender did not say.
    const char* contentType() const;

    // [V5:3.3.2.3.7] The first value for this key, or null. Keys may repeat;
    // reach through message().properties when they do.
    const char* userProperty(const char* key) const;

    // [V5:3.3.2.3.3] What is LEFT of the sender's Message Expiry Interval, in
    // seconds, as the broker recalculated it. Zero when none was set.
    //
    // This is the one thing in MQTT that puts an age on a retained value - see
    // retained() above - and almost nothing sets it.
    uint32_t secondsUntilExpiry() const;

    // [V5:3.3.2.3.2] Whether the sender declared the payload to be UTF-8 text
    // rather than unspecified bytes. False when it said nothing.
    bool declaredAsText() const;

    // True when this message asks for an answer: it has a Response Topic.
    // MqttDevice::reply() is what sends one.
    bool isRequest() const { return responseTopic() != nullptr; }

    // --- Readings ---
    //
    // Each takes a FALLBACK, and each returns it rather than a wrong answer
    // when the payload is not a number. "21.5 degC" does not become 21.5:
    // the part that was not understood is usually where the fault is, and a
    // library that discards it hides the fault in a plausible number.

    double toNumber(double fallback = 0.0) const;
    long toInteger(long fallback = 0) const;

    // 1 / 0, true / false, on / off, yes / no, open / closed, start / stop -
    // any case. Anything else is the fallback.
    bool toBool(bool fallback = false) const;

private:
    const MqttMessage& _message;
    const char* _text;
    size_t _length;

    // Null when the message carried no properties at all, which is every
    // 3.1.1 message and most 5.0 ones. Owned by the device and valid only for
    // the duration of the handler, like everything else here.
    const MqttOwnedProperties* _properties;
};

// A handler for one filter, or for every message.
//
// A plain function pointer, not std::function: it costs nothing, it works with
// a capture-less lambda, and it is what generated wrapper code and an MCU
// toolchain with exceptions disabled can both use.
typedef void (*MqttDeviceMessageFn)(const char* topic, const MqttPayload& payload);
typedef void (*MqttDeviceMessageContextFn)(const char* topic, const MqttPayload& payload,
                                           void* context);

typedef void (*MqttDeviceEventFn)();
typedef void (*MqttDeviceEventContextFn)(void* context);

// One subscription and the handler that belongs to it.
struct MqttDeviceSubscription {
    bool inUse = false;
    std::string filter;
    uint8_t qos = MQ_QOS_0;

    MqttDeviceMessageFn simple = nullptr;
    MqttDeviceMessageContextFn withContext = nullptr;
    void* context = nullptr;

    // Running totals, for a caller that polls rather than handling.
    uint32_t received = 0;
    mq_time_t lastMessageAt = 0;
    bool everReceived = false;

    // Whether the broker has acknowledged it. A SUBACK can refuse, and a
    // subscription that was accepted by us and refused by the broker delivers
    // nothing while looking perfectly healthy.
    bool acknowledged = false;
    MqttResult lastResult = MqttResult::Queued;
};

// One variable the device publishes on change.
//
// FIXED-WIDTH TYPES ONLY, and the reason is not pedantry: `int` is 16 bits on
// AVR and 32 on ESP32, so binding one would put different bytes on the wire
// depending on which board was compiled - a fault that appears months later,
// when the second kind of device joins the plant.
enum class MqttReportType : uint8_t { Bool = 0, Int32, UInt32, Float, Double };

struct MqttDeviceReport {
    bool inUse = false;
    std::string topic;
    const void* source = nullptr;
    MqttReportType type = MqttReportType::Float;

    // How far it must move before it is worth saying. Zero publishes every
    // change, however small - which for a floating-point reading from a real
    // sensor is every single scan, and is almost never what was meant.
    double deadband = 0.0;

    // Say it anyway after this long, even if nothing moved.
    //
    // THIS IS THE HALF EVERYBODY LEAVES OUT. With a deadband alone, a tank
    // that has not moved for an hour and a transmitter that died an hour ago
    // look IDENTICAL to every subscriber - there is nothing to tell them apart
    // with. Zero means never, and means accepting that.
    mq_time_t atLeastEveryMs = 0;

    uint8_t qos = MQ_QOS_0;
    bool retain = true;

    double lastValue = 0.0;
    mq_time_t lastPublishedAt = 0;
    bool everPublished = false;
    uint32_t publishes = 0;
};

// --- Provisioning: a device that registers itself (a modbee broker) ---
//
// Where a provisioned device keeps its login between restarts - flash, NVS,
// a file - supplied by the application, because only it knows what storage
// the board has. Load fills both buffers (NUL-terminated) and returns true if
// a login was stored; save stores one, and is called with "" and "" to
// forget it. Neither is required: without them the device provisions again
// at every start, which works, at the cost of a new login each time.
typedef bool (*MqttDeviceLoadLoginFn)(void* context, char* user, size_t userSize,
                                      char* password, size_t passwordSize);
typedef void (*MqttDeviceSaveLoginFn)(void* context, const char* user, const char* password);

enum class MqttProvisionState : uint8_t {
    Off,            // provision() was not called
    Registering,    // connected with the key, asking for a login
    Pending,        // the broker wants an admin to approve this device first
    Provisioned,    // has its own login (stored, or just received)
    Refused,        // the key was refused; provisionReason() says why
};

class MqttDevice {
public:
    // --- Construction ---

#if defined(MQTT_PLATFORM_ARDUINO)
    // The sketch's network channel: WiFiClient, WiFiClientSecure,
    // EthernetClient, TinyGsmClient, SSLClient - anything that is an Arduino
    // Client. It is yours and must outlive this object, because the sketch
    // owns the network stack and this does not.
    //
    // THE CHANNEL IS NOT STARTED HERE. WiFi.begin(), Ethernet.begin() or the
    // modem's own attach sequence are the sketch's to run, and they are the
    // ones that take time - see docs/transports.md.
    template <typename ChannelType,
              typename = typename std::enable_if<
                  !std::is_base_of<MqttTransport, ChannelType>::value>::type>
    explicit MqttDevice(ChannelType& channel) {
        typedef MqttStreamTransport<ChannelType> Stream;
        static_assert(sizeof(Stream) <= sizeof(_storage),
                      "network channel transport does not fit - raise kStorageSize");
        static_assert(alignof(Stream) <= alignof(void*),
                      "network channel transport wants an alignment this storage "
                      "does not give it");
        _transport = new (_storage) Stream(channel);
        _retarget = &retargetStream<ChannelType>;
        _destroy = &destroyStream<ChannelType>;
        _ownTransport = true;
    }
#else
    // On a host there is no channel to supply: the sockets are already there.
    // Plain TCP unless secure() is called.
    MqttDevice();
#endif

    // Bring your own transport - a WebSocket one, a host TLS one you configured
    // yourself, or a test double. Configure its target before connect(); this
    // will not touch it.
    explicit MqttDevice(MqttTransport& transport);

    ~MqttDevice();

    MqttDevice(const MqttDevice&) = delete;
    MqttDevice& operator=(const MqttDevice&) = delete;

    // --- Settings. All optional, all before connect() ---

    // Identifies this device to the broker, and is what a session is kept
    // against. Leave it out and a 5.0 broker assigns one - which is fine until
    // you want a session to survive a reconnect, because next time it assigns
    // a different one.
    void clientId(const char* id);

    void credentials(const char* user, const char* password);

    // REGISTER ITSELF, with a provisioning key from the broker's admin
    // (`modbee key create`). Before connect(); the device's id - connect()'s
    // second argument - becomes its user name:
    //
    //   mqtt.provision("site-north.Xk3...", &loadLogin, &saveLogin);
    //   mqtt.connect("mqtt.example.com", "pump-07");
    //
    // With no login stored, it connects with the key, is given its own login,
    // stores it, and reconnects as itself - nobody types or records a
    // per-device password. If the broker later refuses that login as wrong
    // (it was reset, or the device was replaced and re-registered), the
    // device forgets it and registers again by itself. docs/provisioning.md.
    void provision(const char* key, MqttDeviceLoadLoginFn load = nullptr,
                   MqttDeviceSaveLoginFn save = nullptr, void* context = nullptr);
    MqttProvisionState provisionState() const { return _provisionState; }
    // Why it was refused, or what it is waiting for, in words; "" otherwise.
    const char* provisionReason() const { return _provisionReason.c_str(); }

    // Seconds between pings when nothing else is being said. This is the only
    // thing that notices a link that has gone away without saying so - a
    // firewall that dropped the flow, or a cellular bearer that vanished - so
    // it is also how long a failure can go unnoticed. A 5.0 broker may shorten
    // it; activeKeepAlive() is what is actually in force.
    void keepAlive(uint16_t seconds);

    // Ask the broker to hold our subscriptions and our in-flight QoS 1 and 2
    // messages while we are away, for this many seconds.
    //
    // This is what turns a dropped link into a pause rather than a loss: a
    // QoS 1 publish that was on the wire when the link died is retransmitted
    // when it comes back. Without it, every reconnect starts from nothing.
    //
    // Seconds are 5.0 only; a 3.1.1 broker keeps the session for as long as it
    // feels like and there is no way to ask. Any non-zero value gets the
    // 3.1.1 behaviour there, which is the honest approximation.
    void rememberSession(uint32_t seconds = 3600);

    // 5.0, dropping to 3.1.1 if the broker turns out to be older. ON by
    // default here - the whole point of this class is that it works against
    // whatever is installed - and the fallback happens only on evidence that
    // the VERSION is the problem, never on a bad password or a network fault.
    //
    // Turn it off with only(MqttVersion::V5) when you need to know exactly
    // what is on the wire.
    void only(MqttVersion version);
    void allowOldBrokers(bool allowed = true);

    // --- Saying whether you are here ---

    // The two halves of an availability topic, done as one call.
    //
    //   - "offline" is registered as the Will, so the broker publishes it if
    //     this device stops without saying goodbye: power, a cable, a crash
    //   - "online" is published the moment a session is established, ahead of
    //     any application traffic
    //   - both are RETAINED, so a dashboard that connects later still learns
    //     the state rather than waiting for the next change
    //   - a deliberate disconnect() publishes "offline" itself, because a Will
    //     is never sent on a graceful disconnect [V5:3.1.2.5] and a device that
    //     shut down cleanly would otherwise read as online for ever
    //
    // This is the single most-forgotten half of an MQTT deployment: the alarm
    // that never fires because the Will was set and nothing ever published the
    // "online" that would let anyone tell the difference.
    //
    // If the broker caps QoS below 1 or refuses retained messages, the pair is
    // published with whatever it does allow rather than not at all - and
    // capabilities().retainAvailable is where that is visible. An availability
    // topic without retain reaches only subscribers already connected, which is
    // worth knowing before it matters.
    //
    // A broker that refuses retained messages refuses the RETAINED WILL at the
    // CONNECT [V5:MQTT-3.1.2-11], which cannot be foreseen - Retain Available
    // arrives in the answer to the CONNECT that carried the Will. Rather than
    // retrying a rejected CONNECT for ever, the Will goes again without retain
    // and willRetainRefused() says so.
    void statusTopic(const char* topic, const char* online = "online",
                     const char* offline = "offline");

    // A Will of your own, when the payload is not text - a Sparkplug death
    // certificate, for instance. Mutually exclusive with statusTopic(): the
    // last one called is the one in force.
    void will(const char* topic, const uint8_t* payload, size_t length,
              uint8_t qos = MQ_QOS_1, bool retain = true);

    // 5.0 only. How long the broker waits after the link dies before publishing
    // the Will, so a device that reconnects inside the window never appears to
    // have gone at all. Nothing stops a flapping radio link filling a plant
    // with false alarms as well as this does.
    void willDelay(uint32_t seconds);

    // --- Connecting ---
    //
    // NOTHING HERE WAITS. connect() states the intent and returns; the link
    // comes up across later calls to poll(), and isConnected() is what says so.
    // A false return means the settings themselves are unusable - no host,
    // nowhere to send - not that the broker refused.
    //
    // You do not have to wait for it either: publish() and onMessage() before
    // the link exists are held and go out when it does.

    bool connect(const char* host, uint16_t port = 0);
    bool connect(const char* host, const char* clientId);
    bool connect(const char* host, uint16_t port, const char* clientId);

    // Says goodbye properly: publishes the offline status if there is one,
    // lets it reach the wire, then sends DISCONNECT and closes. Takes a few
    // calls to poll() and never blocks for any of them.
    //
    // Auto-reconnect stops: you asked to stop, and reconnecting would be the
    // library overruling you. connect() again when you want it back.
    void disconnect();

    // Closes now, with nothing said. For a device going down on a failing
    // supply, where a clean goodbye is a luxury there is no power for.
    void disconnectNow();

    // The whole contract. Call it every scan, or every time round loop().
    void poll();

    // --- Publishing ---
    //
    // Each returns true if the message was ACCEPTED - queued, and guaranteed to
    // reach exactly one conclusion. At QoS 0 that is as much as anyone can say.
    // At QoS 1 and 2, whether it was acknowledged is a later question: use
    // publishAndConfirm() when it matters.
    //
    // A publish while the link is down is HELD, not refused, up to the queue's
    // capacity - so a panel keeps working through an outage and catches up
    // afterwards.

    bool publish(const char* topic, const char* text, uint8_t qos = MQ_QOS_0,
                 bool retain = false);

    bool publish(const char* topic, const uint8_t* bytes, size_t length,
                 uint8_t qos = MQ_QOS_0, bool retain = false);

    // Any number, as decimal text.
    //
    // Text rather than raw bytes, because that is what the rest of a plant
    // already reads: Node-RED publishes numbers as text and Ignition infers a
    // tag's type from it, and neither reads a big-endian float without being
    // configured to. See docs/typed-values.md for the evidence and for the
    // binary option when two ends have agreed on one.
    //
    // A float prints as %.7g and a double as %.15g - the number a person would
    // write, not every digit the format can hold. Where the exact bits matter,
    // publish the bytes or use the typed layer.
    template <typename T,
              typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
    bool publish(const char* topic, T value, uint8_t qos = MQ_QOS_0,
                 bool retain = false) {
        char buffer[40];
        formatNumber(value, buffer, sizeof buffer);
        return publish(topic, buffer, qos, retain);
    }

    // A number with the decimal places fixed, for a reading a person reads:
    // 4.20 rather than 4.2000001.
    bool publishNumber(const char* topic, double value, uint8_t decimals,
                       uint8_t qos = MQ_QOS_0, bool retain = false);

    // One message of a SERIES, where every message matters and none may be
    // elided. An event log, a counter, a Sparkplug data stream.
    //
    // The publishes above SUPERSEDE: a newer value for a topic replaces one
    // still waiting to be sent, which is what you want for a reading or a
    // setpoint - what arrives should be what is true. For a sequence that is
    // exactly wrong, because the gap reads as loss.
    bool publishSeries(const char* topic, const uint8_t* bytes, size_t length,
                       uint8_t qos = MQ_QOS_0, bool retain = false);
    bool publishSeries(const char* topic, const char* text,
                       uint8_t qos = MQ_QOS_0, bool retain = false);

    // Publish, and tell me what happened to it.
    //
    // The handler runs once, on exactly one conclusion: acknowledged by the
    // broker, refused by it, superseded by a newer value, or lost with the
    // connection. This is the call for anything that must be confirmed - a
    // command to stop a pump, where "I sent it" and "it was received" are
    // different facts and only one of them is worth acting on.
    bool publishAndConfirm(const char* topic, const char* text, uint8_t qos,
                           MqttCompletionFn onConcluded, void* context = nullptr,
                           bool retain = false);

    // --- 5.0 properties, and the pattern they exist for ---

    // Publish with a property set of your own - a Content Type, User
    // Properties, a Message Expiry Interval. Build it with
    // MqttOwnedProperties' named setters.
    //
    // REFUSED on a 3.1.1 connection rather than sent stripped: a message that
    // quietly lost its correlation id leaves the requester waiting for an
    // answer nothing can match to it.
    bool publish(const char* topic, const char* text,
                 const MqttOwnedProperties& properties,
                 uint8_t qos = MQ_QOS_0, bool retain = false);
    bool publish(const char* topic, const uint8_t* bytes, size_t length,
                 const MqttOwnedProperties& properties,
                 uint8_t qos = MQ_QOS_0, bool retain = false);

    // ASK SOMETHING, AND SAY WHERE THE ANSWER GOES.
    //
    // This is MQTT 5.0's request/response pattern, which is otherwise three
    // properties a caller has to know the names of:
    //
    //     mqtt.onMessage("plant/gateway/replies", &onReply, MQ_QOS_1);
    //     mqtt.publishRequest("plant/pump-2/command", "stop",
    //                         "plant/gateway/replies", "stop-42");
    //
    // The answering device does not have to know any of it either - it calls
    // reply() and the library reads the topic and the correlation id off the
    // request.
    //
    // `correlationId` is what comes back with the answer, so it has to be
    // unique among the requests you have outstanding. A counter is enough.
    // Null sends none, and then an answer cannot be matched to its question.
    //
    // QoS 1 by default: a request nobody received is a caller waiting for
    // ever, and losing it is worse than seeing it twice.
    bool publishRequest(const char* topic, const char* text,
                        const char* replyTopic, const char* correlationId,
                        uint8_t qos = MQ_QOS_1);

    // ANSWER A REQUEST, on the topic it asked for, carrying its correlation
    // data back unaltered.
    //
    // False when the message was not a request - no Response Topic - or when
    // the connection is 3.1.1 and cannot carry the answer's correlation. Never
    // a reply sent somewhere nobody is listening.
    bool reply(const MqttPayload& request, const char* text,
               uint8_t qos = MQ_QOS_1);
    bool reply(const MqttPayload& request, const uint8_t* bytes, size_t length,
               uint8_t qos = MQ_QOS_1);

    // --- Reporting a variable, so the loop does not have to ---
    //
    // Point the device at a variable, say how far it has to move before anybody
    // cares, and how often to say so anyway. It is checked on every poll() and
    // published when either is true.
    //
    //     float tankLevel = 0.0f;
    //     mqtt.report("plant/tank-01/level", &tankLevel, 0.02f, 30000);
    //
    // That is a change-driven stream a subscriber can still tell apart from a
    // dead sensor, and it replaces the ten lines of deadband-and-timer that
    // every sketch would otherwise carry.
    //
    // RETAINED BY DEFAULT, unlike a plain publish(). What this reports is a
    // variable's current value - a state - so a dashboard connecting later
    // should learn it rather than wait for the next change. Say false where
    // what you have is a stream rather than a value.
    //
    // Everything reported is published again, in full, the moment a session is
    // established: a subscriber that has just arrived knows nothing, and
    // waiting for the next change could be hours.
    //
    // Numbers go out as decimal text - see publish() above. For binary
    // encodings, more types, and a quality that goes stale on schedule, the
    // typed layer is the full version: docs/typed-values.md.

    bool report(const char* topic, const bool* value,
                mq_time_t atLeastEveryMs = 0, uint8_t qos = MQ_QOS_0,
                bool retain = true);

    bool report(const char* topic, const int32_t* value, double deadband = 0.0,
                mq_time_t atLeastEveryMs = 0, uint8_t qos = MQ_QOS_0,
                bool retain = true);
    bool report(const char* topic, const uint32_t* value, double deadband = 0.0,
                mq_time_t atLeastEveryMs = 0, uint8_t qos = MQ_QOS_0,
                bool retain = true);
    bool report(const char* topic, const float* value, double deadband = 0.0,
                mq_time_t atLeastEveryMs = 0, uint8_t qos = MQ_QOS_0,
                bool retain = true);
    bool report(const char* topic, const double* value, double deadband = 0.0,
                mq_time_t atLeastEveryMs = 0, uint8_t qos = MQ_QOS_0,
                bool retain = true);

    // Anything else fails to compile rather than publishing the wrong number
    // of bytes. A pointer, a struct, a `long double`, an `int` whose width
    // differs per board - all of them are mistakes that would otherwise only
    // be found on the wire.
    template <typename T>
    bool report(const char*, const T*, double = 0.0, mq_time_t = 0,
                uint8_t = MQ_QOS_0, bool = true) = delete;

    bool stopReporting(const char* topic);

    size_t reportCount() const;
    const MqttDeviceReport* reportAt(size_t index) const;

    // Publishes every reported variable now, whatever its deadband says.
    void reportAll();

    // --- Subscribing ---
    //
    // onMessage(filter, handler) SUBSCRIBES as well as registering the handler.
    // One call, because the two are never wanted apart.
    //
    // Before the link exists is fine: it is recorded and issued when the
    // session comes up, and re-issued on any reconnect where the broker
    // reports no session of ours.
    //
    // Calling it twice for the same filter REPLACES the handler rather than
    // adding a second - so a reconnect, or a setup() run twice, cannot end up
    // delivering everything twice.

    bool onMessage(const char* filter, MqttDeviceMessageFn handler,
                   uint8_t qos = MQ_QOS_0);
    bool onMessage(const char* filter, MqttDeviceMessageContextFn handler,
                   void* context, uint8_t qos = MQ_QOS_0);

    // Every message, whichever filter it matched, in addition to any handler
    // for that filter. For logging, or for a gateway that treats them all the
    // same.
    void onMessage(MqttDeviceMessageFn handler);
    void onMessage(MqttDeviceMessageContextFn handler, void* context);

    // A subscription with no handler of its own - it will reach the
    // every-message handler above, if there is one.
    bool subscribe(const char* filter, uint8_t qos = MQ_QOS_0);

    // A null handler is refused HERE, with a message, rather than as an
    // ambiguous overload three lines into the compiler's output.
    //
    // onMessage(filter, nullptr, qos) is a reasonable thing to try and matches
    // both handler shapes equally well, so the compiler cannot choose. The
    // call that was meant is subscribe(filter, qos).
    bool onMessage(const char* filter, decltype(nullptr),
                   uint8_t qos = MQ_QOS_0) = delete;

    bool unsubscribe(const char* filter);

    // --- Knowing what is going on ---

    void onConnected(MqttDeviceEventFn handler);
    void onConnected(MqttDeviceEventContextFn handler, void* context);
    void onDisconnected(MqttDeviceEventFn handler);
    void onDisconnected(MqttDeviceEventContextFn handler, void* context);

    // True when the link is up, acknowledged, and work will actually go.
    bool isConnected() const { return _client.isConnected(); }

    // Whether a message has arrived on this filter within the time given, AND
    // the link is up.
    //
    // MQTT will hand you a retained reading from a device that died last
    // Tuesday and say nothing about it, so "I have a value" is not the same
    // question as "may I act on it". This is the second question, and the
    // interval is yours because only you know how often the value was promised:
    // a temperature sampled hourly and a pressure sampled ten times a second
    // cannot share a constant.
    //
    // The filter is matched as you wrote it in onMessage() or subscribe().
    //
    // PER FILTER, NOT PER TOPIC. A subscription to "plant/+/level" tracks when
    // anything matching it last arrived, not each device separately - tracking
    // every topic a wildcard matched would be an unbounded table on a device
    // with no spare memory, and only the application knows which devices it
    // expects to hear from at all. A panel watching several keeps its own
    // small table; examples/arduino/Alarm_On_Silence is that panel.
    bool isFresh(const char* filter, mq_time_t withinMs) const;

    // How long since a message arrived on this filter, in milliseconds.
    // 0xFFFFFFFF if none ever has.
    mq_time_t sinceMessage(const char* filter) const;

    // What is actually in force, which is not always what was asked for.
    MqttVersion activeVersion() const { return _client.activeVersion(); }
    uint16_t activeKeepAlive() const { return _client.activeKeepAlive(); }

    const MqttStatus& status() const { return _client.status(); }
    MqttState state() const { return _client.state(); }
    const char* stateText() const { return mq_stateName(_client.state()); }

    MqttResult lastError() const { return _client.status().lastError; }
    const char* lastErrorText() const { return mq_resultName(_client.status().lastError); }

    // How many messages are waiting to go out. Rises during an outage and
    // falls when the link comes back; if it stops falling, the queue is full
    // and publishes are being refused rather than lost.
    // True when this broker refused a retained Will, so the availability topic
    // is live but not retained: a dashboard connecting later will not learn
    // this device's state until it changes.
    bool willRetainRefused() const { return _willRetainRefused; }

    size_t queued() const { return _client.operations().pending(); }
    // What is in force: what queueSize() asked for, or - when the memory for
    // it was not there (queueOutOfMemory()) - the size the queue kept.
    size_t queueCapacity() const {
        return _client.queueOutOfMemory() ? _client.operations().capacity()
                                          : _behaviour.queueCapacity;
    }
    bool queueOutOfMemory() const { return _client.queueOutOfMemory(); }

    // How many filters are subscribed, and the state of each: how many
    // messages it has taken, when the last one came, and whether the broker
    // acknowledged it. A SUBACK can refuse, and a refused subscription
    // delivers nothing while looking perfectly healthy.
    size_t subscriptionCount() const;
    const MqttDeviceSubscription* subscriptionAt(size_t index) const;
    const MqttDeviceSubscription* subscriptionFor(const char* filter) const;

    // --- Tuning, for when a default is wrong ---

    // How many messages may be held while the link is down. Past this, a
    // publish is refused - never silently dropped.
    //
    // Any size: nothing caps it but memory (MQTT_QUEUE_CAPACITY, 32, is only
    // the default). The slots - and on a board the bytes their messages are
    // kept in, in proportion - are taken when the device connects, once, so
    // nothing grows while it runs. If the memory is not there the queue keeps
    // the size it had, and that is said rather than quietly given:
    //
    //     mqtt.queueSize(500);
    //     mqtt.connect(...);
    //     if (mqtt.queueOutOfMemory()) { /* queueCapacity() is what it kept */ }
    //
    // Returns what was asked for (0 is taken as 1).
    size_t queueSize(size_t operations);

    // The largest message that can be received or sent, in bytes. A message
    // larger than this cannot be received at all, so it is also what the
    // broker is told as our Maximum Packet Size.
    void bufferSize(size_t bytes);

    // Whether a lost link is reconnected automatically. On by default.
    void autoReconnect(bool enabled);

    // --- Host TLS ---
    //
    // On Arduino there is nothing here to call: a secure channel IS an Arduino
    // network channel, so you hand WiFiClientSecure or SSLClient to the
    // constructor and the transport is the same either way. See
    // docs/transports.md.
#if MQTT_TLS_SOCKETS
    // Turns on TLS and makes the default port 8883. With no argument, the
    // system certificate store is what is trusted.
    void secure(const char* caFilePath = nullptr);
    void secureWithCertificate(const char* caPem);
    void clientCertificate(const char* certificatePath, const char* keyPath);

    // Named at length on purpose - see MqttTlsTransport.h. This is the call
    // that turns a TLS connection into an encrypted conversation with whoever
    // answered.
    void insecureNoCertificateValidation();
#endif

    // --- Arduino String, everywhere a C string is taken ---
    //
    // A sketch builds topics and payloads with String because that is what the
    // core gives it. A library that only took const char* would be one the
    // user wrote .c_str() against on every line - which is exactly the kind of
    // ceremony this class exists to remove.
    //
    // Forwarding one-liners rather than a new parameter type, so no existing
    // signature changes and no overload resolves differently than it did.
#if defined(MQTT_PLATFORM_ARDUINO)
    bool connect(const String& host, const String& id) {
        return connect(host.c_str(), id.c_str());
    }
    bool connect(const String& host, uint16_t port, const String& id) {
        return connect(host.c_str(), port, id.c_str());
    }
    void clientId(const String& id) { clientId(id.c_str()); }
    void credentials(const String& user, const String& password) {
        credentials(user.c_str(), password.c_str());
    }
    void statusTopic(const String& topic) { statusTopic(topic.c_str()); }
    void statusTopic(const String& topic, const String& online, const String& offline) {
        statusTopic(topic.c_str(), online.c_str(), offline.c_str());
    }

    bool publish(const String& topic, const String& text,
                 uint8_t qos = MQ_QOS_0, bool retain = false) {
        return publish(topic.c_str(), text.c_str(), qos, retain);
    }
    bool publish(const char* topic, const String& text,
                 uint8_t qos = MQ_QOS_0, bool retain = false) {
        return publish(topic, text.c_str(), qos, retain);
    }
    bool publish(const String& topic, const char* text,
                 uint8_t qos = MQ_QOS_0, bool retain = false) {
        return publish(topic.c_str(), text, qos, retain);
    }
    template <typename T,
              typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
    bool publish(const String& topic, T value, uint8_t qos = MQ_QOS_0,
                 bool retain = false) {
        return publish(topic.c_str(), value, qos, retain);
    }

    bool publishSeries(const String& topic, const String& text,
                       uint8_t qos = MQ_QOS_0, bool retain = false) {
        return publishSeries(topic.c_str(), text.c_str(), qos, retain);
    }

    bool onMessage(const String& filter, MqttDeviceMessageFn handler,
                   uint8_t qos = MQ_QOS_0) {
        return onMessage(filter.c_str(), handler, qos);
    }
    bool onMessage(const String& filter, MqttDeviceMessageContextFn handler,
                   void* context, uint8_t qos = MQ_QOS_0) {
        return onMessage(filter.c_str(), handler, context, qos);
    }
    bool subscribe(const String& filter, uint8_t qos = MQ_QOS_0) {
        return subscribe(filter.c_str(), qos);
    }
    bool unsubscribe(const String& filter) { return unsubscribe(filter.c_str()); }

    bool isFresh(const String& filter, mq_time_t withinMs) const {
        return isFresh(filter.c_str(), withinMs);
    }
    mq_time_t sinceMessage(const String& filter) const {
        return sinceMessage(filter.c_str());
    }
    bool stopReporting(const String& topic) { return stopReporting(topic.c_str()); }
#endif

    // --- What the peer said about our session ---

    // Whether the broker resumed a session it already held for us, from the
    // last CONNACK. False after a Clean Start, and false the first time a
    // client identifier is seen.
    //
    // Worth acting on: a session that was NOT present means every subscription
    // has just been re-issued and anything that had been sent and not
    // acknowledged is gone - so a device that keeps its own state should
    // republish it.
    bool sessionWasResumed() const { return _sessionWasResumed; }

    // --- The whole library, when you outgrow this ---
    //
    // Not a wall. Everything below is the real object, doing the real work, and
    // anything you write against it keeps working alongside the calls above.
    MqttClient& client() { return _client; }
    const MqttClient& client() const { return _client; }
    MqttTransport* transport() { return _transport; }
    const MqttTransport* transport() const { return _transport; }
    const MqttCapabilities& capabilities() const { return _client.capabilities(); }

    // The configuration as it will be sent. Change anything here that this
    // class does not expose, before connect().
    MqttConnectionConfig& connection() { return _connection; }
    const MqttConnectionConfig& connection() const { return _connection; }
    MqttClientConfig& behaviour() { return _behaviour; }
    const MqttClientConfig& behaviour() const { return _behaviour; }

private:
#if defined(MQTT_PLATFORM_ARDUINO)
    // Big enough for MqttStreamTransport<anything>: a vtable pointer, a
    // reference, a string pointer and a port. Checked by a static_assert at
    // the point of construction rather than assumed.
    static const size_t kStorageSize = 64;
#endif

    MqttClient _client;
    MqttTransport* _transport = nullptr;

    // How to take an owned transport down again, and - for a sketch's channel
    // transport, the only kind a board builds here - how to point it at a
    // host, and where it lives. Null for a transport the caller brought.
    void (*_destroy)(void* transport) = nullptr;
#if defined(MQTT_PLATFORM_ARDUINO)
    void (*_retarget)(void* transport, const char* host, uint16_t port) = nullptr;
    alignas(void*) unsigned char _storage[kStorageSize];
#endif

    // False for a transport the caller brought and configured. We point ours
    // at a host in connect(); theirs we leave entirely alone, because a class
    // that reconfigured a transport it did not make would quietly undo
    // whatever the caller had set on it.
    bool _ownTransport = false;

#if defined(MQTT_PLATFORM_HOST)
    MqttTcpTransport _tcp;
#if MQTT_TLS_SOCKETS
    MqttTlsTransport _tls;
    bool _useTls = false;
#endif
#endif

    MqttConnectionConfig _connection;
    MqttClientConfig _behaviour;

    // Owned, because MqttStreamTransport keeps a pointer rather than a copy
    // and a host string that went out of scope is a dangling one at the exact
    // moment a reconnect needs it.
    std::string _host;

    MqttDeviceSubscription _subscriptions[MQTT_DEVICE_MAX_SUBSCRIPTIONS];
    MqttDeviceReport _reports[MQTT_DEVICE_MAX_REPORTS];

    MqttDeviceMessageFn _anySimple = nullptr;
    MqttDeviceMessageContextFn _anyContext = nullptr;
    void* _anyContextValue = nullptr;

    MqttDeviceEventFn _onConnectedSimple = nullptr;
    MqttDeviceEventContextFn _onConnectedContext = nullptr;
    void* _onConnectedValue = nullptr;
    MqttDeviceEventFn _onDisconnectedSimple = nullptr;
    MqttDeviceEventContextFn _onDisconnectedContext = nullptr;
    void* _onDisconnectedValue = nullptr;

    std::string _statusTopic;
    std::string _statusOnline;
    std::string _statusOffline;
    bool _statusIsText = false;

    // A graceful goodbye takes more than one poll(): the offline message has
    // to reach the wire before the DISCONNECT that follows it.
    bool _closing = false;
    mq_time_t _closingSince = 0;

    bool _begun = false;
    bool _wasConnected = false;
    bool _sessionWasResumed = false;

    // A broker that refuses retained messages refuses a RETAINED WILL at the
    // CONNECT [V5:MQTT-3.1.2-11], and there is no way to know that before
    // sending one: Retain Available arrives in the CONNACK, which is the
    // answer to the CONNECT we have already sent.
    //
    // So statusTopic() would leave such a device refused for ever, retrying the
    // same rejected CONNECT. Seen once, the Will goes again without retain and
    // the device connects. Recorded rather than silent - willRetainRefused()
    // says it happened, because an availability topic that is not retained is
    // worth knowing about.
    bool _retryWithoutWillRetain = false;
    bool _willRetainRefused = false;

    // --- Provisioning ---
    std::string _provisionKey;
    MqttDeviceLoadLoginFn _loadLogin = nullptr;
    MqttDeviceSaveLoginFn _saveLogin = nullptr;
    void* _loginContext = nullptr;
    MqttProvisionState _provisionState = MqttProvisionState::Off;
    std::string _provisionReason;
    bool _provisioning = false;          // the client is configured with the key, not a login
    bool _switchToLogin = false;         // provisioned: reconnect as the device
    bool _reprovision = false;           // the stored login was refused: register again
    bool _switchDisconnected = false;
    mq_time_t _provisionAskedAt = 0;
    MqttConnectionConfig _provisionConnection;
    void startProvisioning();
    void provisionSession();
    void provisionReply(const MqttMessage& message);
    void serviceProvisioning();
    void provisionStateChanged(MqttState state);

    // REACHED ONLY THROUGH provision(). poll(), dispatch() and the callbacks
    // call provisioning through this table, which provision() installs - so
    // a program that never calls provision() references none of it and the
    // linker drops it all (measured: about 5 kB of a short-form build).
    struct Provisioner {
        void (*start)(MqttDevice&);
        void (*session)(MqttDevice&);
        void (*reply)(MqttDevice&, const MqttMessage&);
        void (*service)(MqttDevice&);
        void (*state)(MqttDevice&, MqttState);
    };
    static const Provisioner kProvisioner;
    const Provisioner* _provisioner = nullptr;

    // Reused rather than reallocated per message. A gateway taking a thousand
    // messages a second would otherwise spend its time in the allocator.
    std::string _topicText;
    std::string _payloadText;

    // Filled only when a message actually carries properties, which is every
    // 3.1.1 message never and most 5.0 messages not at all - so the ordinary
    // path costs one branch rather than a copy.
    MqttOwnedProperties _inbound;
    bool _inboundHasProperties = false;

    // Hands the current configuration to the client. Safe to call again: it
    // resets the link and the status counters but NOT the queue or the
    // session, so a publish made before connect() and the subscriptions
    // recorded with it both survive - which is what lets settings be changed
    // between one connect() and the next.
    void beginClient();

    // The same, but only if it has not happened yet.
    void ensureBegun() { if (!_begun) beginClient(); }

    void applyDefaults();

    // The status pair, published with what THIS connection actually allows.
    // A broker may cap QoS at 0 or refuse retained messages entirely, and a
    // publish at settings it refuses is refused - which would mean the online
    // half of an availability topic silently never went.
    void publishStatus(const std::string& payload);
    uint16_t defaultPort() const;

    // Null when every slot is taken - never a silent overwrite of somebody
    // else's subscription.
    MqttDeviceSubscription* slotFor(const char* filter, uint8_t qos);
    MqttDeviceSubscription* findSlot(const char* filter);
    const MqttDeviceSubscription* findSlot(const char* filter) const;
    bool issue(MqttDeviceSubscription& slot);

    void dispatch(const MqttMessage& message);

    bool addReport(const char* topic, const void* source, MqttReportType type,
                   double deadband, mq_time_t atLeastEveryMs, uint8_t qos,
                   bool retain);
    static double readReport(const MqttDeviceReport& report);
    void serviceReports(bool force);
    void publishReport(MqttDeviceReport& report, double value, mq_time_t now);
    void sessionEstablished(bool sessionPresent);
    void stateChanged(MqttState state, MqttResult reason);

    static void messageTrampoline(void* context, const MqttMessage& message);
    static void sessionTrampoline(void* context, bool sessionPresent);
    static void stateTrampoline(void* context, MqttState state, MqttResult reason);
    static void subscribeTrampoline(void* context, MqttResult result, uint16_t packetId);

    static void formatNumber(bool value, char* out, size_t capacity);
    static void formatNumber(float value, char* out, size_t capacity);
    static void formatNumber(double value, char* out, size_t capacity);
    static void formatNumber(long long value, char* out, size_t capacity);
    static void formatNumber(unsigned long long value, char* out, size_t capacity);

    // Every other arithmetic type funnels into one of the five above, so a
    // short, an int and an int32_t all print the same digits on every board.
    template <typename T>
    static typename std::enable_if<std::is_integral<T>::value &&
                                   std::is_signed<T>::value &&
                                   !std::is_same<T, bool>::value>::type
    formatNumber(T value, char* out, size_t capacity) {
        formatNumber((long long)value, out, capacity);
    }

    template <typename T>
    static typename std::enable_if<std::is_integral<T>::value &&
                                   !std::is_signed<T>::value &&
                                   !std::is_same<T, bool>::value>::type
    formatNumber(T value, char* out, size_t capacity) {
        formatNumber((unsigned long long)value, out, capacity);
    }

#if defined(MQTT_PLATFORM_ARDUINO)
    template <typename ChannelType>
    static void retargetStream(void* transport, const char* host, uint16_t port) {
        ((MqttStreamTransport<ChannelType>*)transport)->target(host, port);
    }

    template <typename ChannelType>
    static void destroyStream(void* transport) {
        ((MqttStreamTransport<ChannelType>*)transport)->~MqttStreamTransport();
    }
#endif
};

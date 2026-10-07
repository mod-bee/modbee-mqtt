# API reference

Everything public, in the order you would meet it.

This is written from the headers. If something here disagrees with the code,
the code is right and this is a bug — say so.

---

## Contents

- [The one rule](#the-one-rule)
- [MqttDevice](#mqttdevice) — **the short form; most code needs only this**
- [MqttPayload](#mqttpayload) — reading what arrived
- [Request and response](#request-and-response) — asking, and answering
- [Properties](#properties) — `MqttOwnedProperties`, and what 3.1.1 does with them
- [MqttClient](#mqttclient) — connect, publish, subscribe
- [Configuration](#configuration) — `MqttConnectionConfig`, `MqttClientConfig`
- [Results](#results) — what `MqttResult` means, one value at a time
- [Handlers and polled state](#handlers-and-polled-state)
- [Handles](#handles) — topics by number, for generated code
- [Transports](#transports) — TCP, TLS, Arduino
- [MqttBroker](#mqttbroker)
- [MqttBridge](#mqttbridge)
- [Typed values](#typed-values) — `MqttTypedBindings`, `MqttPoller`
- [Underneath](#underneath) — the layers you can reach if you need to

---

## The one rule

**Nothing blocks.** No call in this library waits for the network, `connect()`
included. You call `poll()` often and outcomes arrive there.

```cpp
while (running) {
    mqtt.poll();
    // your own work
}
```

That is not a style preference. An OpenPLC scan cycle and an Arduino `loop()`
are the same constraint: a call that waits for a network round trip cannot live
inside either. Everything else in this API follows from it — which is why
`connect()` returns `MqttResult::Queued` rather than success or failure, and
why every operation concludes later rather than there and then.

---

## MqttDevice

```cpp
#include <ModBeeMqtt.h>
```

**One object, sensible defaults, and the fiddly parts done for you.** It is a
thin layer over `MqttClient` and a transport and hides neither: `client()` is
the real client, and anything you write against it works alongside everything
here.

Start here, drop down when you need to. Nothing you have written has to be
rewritten to do it.

### Making one

```cpp
// Arduino - the sketch owns the network stack, so it supplies the channel.
// WiFiClient, WiFiClientSecure, EthernetClient, TinyGsmClient, SSLClient -
// anything that is an Arduino Client.
WiFiClient net;
MqttDevice mqtt(net);

// A host - the sockets are already there.
MqttDevice mqtt;

// Or bring your own transport, on either.
MqttDevice mqtt(myTransport);
```

The channel must outlive the device. It is **not started here**: `WiFi.begin()`,
`Ethernet.begin()` or the modem's attach sequence are the sketch's to run.

### Settings, all optional, all before `connect()`

| | |
| --- | --- |
| `clientId(id)` | who you are. Needed for a session to survive a reconnect |
| `credentials(user, password)` | a user name and password for a broker that wants them. Either may be null. See [Credentials](#credentials) |
| `keepAlive(seconds)` | default 60. The only thing that notices a link that vanished |
| `rememberSession(seconds = 3600)` | an outage becomes a pause rather than a loss |
| `only(version)` | no fallback; exactly this version on the wire |
| `allowOldBrokers(bool)` | fallback to 3.1.1. **On by default here** |
| `statusTopic(topic, online, offline)` | the availability pair — see below |
| `will(topic, bytes, length, qos, retain)` | a Will of your own, for a payload that is not text |
| `willDelay(seconds)` | 5.0 only. A reconnect inside the window publishes no Will at all |
| `queueSize(operations)` | how much may be held while the link is down. Past this a publish is refused, never dropped. Default 32, and **it returns what is actually in force** — `MQTT_QUEUE_CAPACITY` is a hard compile-time ceiling, so asking for more gives you less and says so |
| `bufferSize(bytes)` | the largest message in or out. Default 4096 — see **What a large payload costs** below |
| `autoReconnect(bool)` | on by default. See [transports.md](transports.md) |

Every call above that takes a `const char*` also takes an Arduino `String`, on
Arduino. A sketch builds topics with `String` because that is what the core
gives it, and a library that made you write `.c_str()` on every line would be
one you fought.

**`allowOldBrokers` defaults ON here and OFF in the long form**, and that is
deliberate. The long form is configuration a plant engineer reads to know
exactly what is on the wire; this class exists to work against whatever is
installed. `activeVersion()` says what it settled on either way.

### `statusTopic()` — the availability pair

```cpp
mqtt.statusTopic("plant/tank-01/status");                 // "online" / "offline"
mqtt.statusTopic("plant/tank-01/status", "up", "down");   // your own words
```

Three things, because doing two of them is the common mistake:

1. registers the offline word as the **Will**, retained, at QoS 1 — the broker
   publishes it if this device stops without saying goodbye
2. publishes the online word, retained, the instant a session is established,
   **ahead of anything queued while the link was down**
3. publishes the offline word **itself** on a deliberate `disconnect()`,
   because a Will is never sent on a graceful disconnect `[V5:3.1.2.5]`

Mutually exclusive with `will()`: the last one called is the one in force.

### Connecting

```cpp
bool connect(const char* host, uint16_t port = 0);
bool connect(const char* host, const char* clientId);
bool connect(const char* host, uint16_t port, const char* clientId);
```

**Returns whether the settings were usable, not whether the broker accepted
you.** It does not wait — the link comes up across later calls to `poll()`, and
`isConnected()` is what says so. Port 0 means 1883, or 8883 when TLS was asked
for on a host.

```cpp
void disconnect();      // says offline, lets it reach the wire, then closes
void disconnectNow();   // closes now, with nothing said
void poll();            // the whole contract. Every scan, every loop
```

`disconnect()` takes a few calls to `poll()` and never blocks for any of them.
It also stops auto-reconnect: you asked to stop, and reconnecting would be the
library overruling you. `connect()` again when you want it back.

### Publishing

```cpp
bool publish(const char* topic, const char* text, uint8_t qos = 0, bool retain = false);
bool publish(const char* topic, const uint8_t* bytes, size_t length,
             uint8_t qos = 0, bool retain = false);

template <typename Number>
bool publish(const char* topic, Number value, uint8_t qos = 0, bool retain = false);

bool publishNumber(const char* topic, double value, uint8_t decimals,
                   uint8_t qos = 0, bool retain = false);
```

Each returns whether the message was **accepted** — queued, and guaranteed to
reach exactly one conclusion. A publish while the link is down is **held**, not
refused, up to `queueSize()`; past that it is refused and says so.

Numbers go out as **decimal text**, because that is what dashboards and
historians read without being configured. A `float` prints as `%.7g` and a
`double` as `%.15g` — the number a
person would write, not every digit the format can hold. Integers print
exactly; a `bool` prints `1` or `0`. Where the exact bits matter, publish the
bytes or use [the typed layer](typed-values.md).

One consequence worth knowing: **a `char` is a number in C++**, so
`publish("t", 'A')` publishes `"65"`, not `"A"`. Use `"A"` if that is what you
meant.

```cpp
bool publishSeries(const char* topic, const char* text, uint8_t qos = 0, bool retain = false);
bool publishSeries(const char* topic, const uint8_t* bytes, size_t length,
                   uint8_t qos = 0, bool retain = false);
```

The publishes above **supersede**: a newer value for a topic replaces one still
waiting to be sent. `publishSeries` never does — for a counter, an event log or
a Sparkplug data stream, where an elided member reads as loss.

```cpp
bool publishAndConfirm(const char* topic, const char* text, uint8_t qos,
                       MqttCompletionFn onConcluded, void* context = nullptr,
                       bool retain = false);
```

The handler runs **once**, on exactly one conclusion. Never superseded, because
a caller asking what happened to *this* message should not be told "Superseded"
for something it did not do.

### Reporting a variable

```cpp
bool report(const char* topic, const bool* value,
            mq_time_t atLeastEveryMs = 0, uint8_t qos = 0, bool retain = true);
bool report(const char* topic, const float* value, double deadband = 0.0,
            mq_time_t atLeastEveryMs = 0, uint8_t qos = 0, bool retain = true);
// and const double*, const int32_t*, const uint32_t*

bool stopReporting(const char* topic);
void reportAll();                       // everything now, deadbands ignored
size_t reportCount() const;
const MqttDeviceReport* reportAt(size_t index) const;
```

Point the device at a variable, say how far it has to move before anybody
cares, and how often to say so anyway. It is checked on every `poll()`.

```cpp
float tankLevel = 0.0f;
mqtt.report("plant/tank-01/level", &tankLevel, 0.02, 30000);
```

Both numbers are separate decisions:

- **the deadband** stops a noisy analogue input publishing every scan, because
  the last digit of a real sensor never sits still. Zero publishes every change.
- **the heartbeat** is what lets a subscriber tell a steady value from a dead
  publisher, which it cannot do at all without one. Zero means never, and means
  accepting that.

**Retained by default**, unlike `publish()`. What this reports is a variable's
current value — a state — so a dashboard connecting later should learn it
rather than wait for the next change. Pass `false` where what you have is a
stream.

Everything reported is published **in full** the moment a session is
established, whatever the deadbands say: a subscriber that has just arrived
knows nothing, and waiting for the next change could be hours.

**Nothing is queued while the link is down.** A publish is queued, which is
right for a caller that asked for one — but a change-driven reporter would fill
the queue with every intermediate value of an outage and then deliver a history
nobody wanted. What a subscriber needs afterwards is the value *now*, which is
what the session hook sends.

**Fixed-width types only**, and anything else fails to compile. `int` is 16
bits on AVR and 32 on ESP32, so reporting one would put different bytes on the
wire depending on which board was compiled — a fault that appears months later,
when the second kind of device joins the plant.

Bounded at `MQTT_DEVICE_MAX_REPORTS` (16 by default, a build flag). Past that,
and for an invalid topic name, `report()` returns false rather than accepting a
variable it will never publish.

For binary encodings, more types, and a quality that goes stale on schedule,
[the typed layer](typed-values.md) is the full version.

### Credentials

```cpp
mqtt.credentials("plant-operator", "s3cret");
mqtt.credentials("token-holder", nullptr);     // a user name and nothing else
```

Both arguments are optional and each sets its own flag on the wire. That
distinction is not cosmetic: the CONNECT carries a **User Name Flag** and a
**Password Flag**, and a present-but-empty password is a different packet from
an absent one. A broker doing token authentication usually wants the first form
above; one doing nothing at all wants neither.

On `MqttClient` the same thing is four fields of
[`MqttConnectionConfig`](#mqttconnectionconfig--what-you-send-and-the-broker-may-revise):
`userName`, `password`, `hasUserName` and `hasPassword`.

**The password is Binary Data at both versions** `[V5:3.1.3.6]`, which is why
the config holds a `std::vector<uint8_t>` and not a string — it may contain a
NUL, and a broker handing out generated tokens is the case most likely to.
The facade's convenience overload takes a `const char*` and measures it with
`strlen`, so a password with a NUL in it goes through `MqttClient` directly.

**Credentials are not encryption.** They travel in the CONNECT in the clear
unless the transport is TLS, so a password over plain TCP is a password
anybody on the path can read — `mqtt.secure()` first, then
`mqtt.credentials()`. [security.md](security.md) says what TLS does and does
not prove.

**A rejected credential is never retried and never falls back.** A refusal
arrives as `0x86 Bad User Name or Password` or `0x87 Not Authorized` in the
CONNACK, `status().lastReasonCode` holds it, and the client stops rather than
reconnecting — because a client that retries a rejected password is how a
device gets itself banned, and some brokers ban by address for the whole site.
Version fallback is also suppressed: retrying at 3.1.1 would tell an operator
the version is wrong when the credentials are.

### What a large payload costs

Stated in numbers, because "no hidden ceiling" is only half an answer.

A publish of **N** bytes occupies, at once:

| | |
| --- | --- |
| the queued operation's own copy | **N** bytes, freed when it concludes |
| the transmit buffer | `bufferSize()`, **fixed** |
| the receive buffer | `bufferSize()`, **fixed** — allocated whether or not you publish |
| the session store, at QoS 1 or 2 only | **N** bytes of encoded packet, until acknowledged |

So a 60 kB QoS 1 publish with `bufferSize(65536)` wants roughly **190 kB** of
RAM while it is in flight, and about 130 kB of that is the two fixed buffers
you asked for. At QoS 0 it is 130 kB and the copy.

There used to be a third buffer of the same size, an encode scratch area, and
every outbound packet was written into it and then copied again into the
transmit queue. Packets are now encoded straight into the queue's tail and
committed only if the encoder succeeded, so that buffer is gone: `bufferSize()`
costs two of itself, not three.

`bufferSize()` is a **hard ceiling**: a packet that does not fit is refused
with `PacketTooLarge` rather than fragmented, because MQTT has no
fragmentation and a half-written packet desynchronises the stream for good.

**There is no streaming publish** — no way to write a payload to the socket in
pieces as you produce it, and that is a decision rather than an omission. It
would cost two things this library will not spend: the write would **block**
when the socket filled, and a QoS 1 message streamed from a source could not be
replayed from the session on reconnect, because the bytes would be gone. If
you need hundreds of kilobytes through a microcontroller, this is not the
library for that message.

### When it allocates, and when it stops

Worth knowing before you leave something running for a year.

**At `begin()`**, once: the receive buffer and the transmit buffer, both sized
by `bufferSize()`. They are never grown afterwards — a packet that does not fit
is refused, not reallocated.

**During the first few operations**: a queue slot owns its topic and payload,
and the first publish through a slot allocates them. After that the slot
**keeps its capacity** and reuses it, so a device publishing similar messages
settles into allocating nothing at all. A payload substantially larger than
anything that slot has carried before will allocate again.

**Never on the receive path**, unless a message carries 5.0 properties — those
are copied only when present, which is every 3.1.1 message never and most 5.0
messages not at all.

The practical shape: a device allocates for the first minute and then stops.
That is what makes it safe to leave running; it is not a claim that the library
is allocation-free, because it is not.

**One consequence to be deliberate about.** The library is built with
exceptions off on a microcontroller, and there an allocation that cannot be
satisfied does not return null — it terminates, which on a board is a reset.
The defences against reaching that point are the ones you control:
`bufferSize()` and `queueSize()` bound everything the library will ask for, and
both are refusals rather than growth when they are reached.

### Request and response

MQTT 5.0's request/response pattern is three properties a caller would
otherwise have to know the names of. This is the whole of it:

```cpp
// The asking end
mqtt.onMessage("plant/gateway/replies", &onReply, MQ_QOS_1);
mqtt.publishRequest("plant/pump-2/command", "stop",
                    "plant/gateway/replies", "stop-42");

// The answering end - names neither the topic nor the correlation
void onCommand(const char* topic, const MqttPayload& request) {
    stopThePump();
    mqtt.reply(request, "stopped");
}
```

```cpp
bool publishRequest(const char* topic, const char* text,
                    const char* replyTopic, const char* correlationId,
                    uint8_t qos = MQ_QOS_1);
bool reply(const MqttPayload& request, const char* text, uint8_t qos = MQ_QOS_1);
bool reply(const MqttPayload& request, const uint8_t* bytes, size_t length,
           uint8_t qos = MQ_QOS_1);
```

`reply()` publishes to the request's **Response Topic** and carries its
**Correlation Data** back unaltered `[V5:3.3.2.3.6]` — which is how the
requester tells this answer from the other four it is waiting on. The library
never looks inside the correlation bytes.

`correlationId` has to be unique among the requests you have outstanding; a
counter is enough. Pass null and an answer cannot be matched to its question.

**QoS 1 by default**, because a request nobody received is a caller waiting for
ever, and seeing it twice is the lesser problem.

**`reply()` returns false** when the message was not a request — no Response
Topic — rather than inventing a topic to publish to. `request.isRequest()` is
the same question asked in advance.

Both are **refused on a 3.1.1 connection**: the correlation could not travel,
and an answer nothing can be matched to is worse than no answer.

### Properties

```cpp
MqttOwnedProperties properties;
properties.setContentType("application/json");
properties.setMessageExpiry(30);                  // seconds
properties.setPayloadFormat(true);                // the payload is UTF-8 text
properties.setResponseTopic("plant/gateway/replies");
properties.setCorrelationData("req-7");
properties.addUserProperty("issued-by", "panel-1");

mqtt.publish("plant/line3/batch", payload, properties, MQ_QOS_1);
```

Named setters rather than raw fields, because a property that is **absent** and
one that is **present and zero** are different things on the wire, and setting
a value without its flag is the quiet way to send neither.

**The set goes before the QoS on the facade and after `retain` on the client**
— `client.publish(topic, bytes, length, qos, retain, properties)`. Both orders
are the one that leaves the defaulted arguments last for that class, and a swap
is a compile error rather than a surprise, because `MqttOwnedProperties`
converts to nothing.

Reading them back off a message is on [`MqttPayload`](#mqttpayload).

**Topic Alias is not settable**, deliberately: aliases are negotiated per
connection and the library owns the table.

On 3.1.1 a publish carrying any of these is **refused with
`NotSupportedByProtocol`**, never sent stripped — see
[versions.md](versions.md#what-happens-to-properties-on-a-311-connection).

### Subscribing

```cpp
bool onMessage(const char* filter, MqttDeviceMessageFn handler, uint8_t qos = 0);
bool onMessage(const char* filter, MqttDeviceMessageContextFn handler,
               void* context, uint8_t qos = 0);
void onMessage(MqttDeviceMessageFn handler);              // every message
void onMessage(MqttDeviceMessageContextFn handler, void* context);
bool subscribe(const char* filter, uint8_t qos = 0);      // no handler of its own
bool unsubscribe(const char* filter);
```

```cpp
typedef void (*MqttDeviceMessageFn)(const char* topic, const MqttPayload& payload);
```

`onMessage(filter, handler)` **subscribes as well**, because the two are never
wanted apart. Each message goes to the handler for the filter it matched.

Before the link exists is fine — recorded and issued when the session comes up,
and re-issued on any reconnect where the broker reports no session.

Calling it twice for the same filter **replaces** the handler rather than
adding a second, so a `setup()` run twice cannot deliver everything twice.

Bounded at `MQTT_DEVICE_MAX_SUBSCRIPTIONS` (16 by default, a build flag). Past
that, `onMessage` returns false — never a silent overwrite.

A plain function pointer, not `std::function`: it costs nothing, works with a
capture-less lambda, and is what generated wrapper code and an
`-fno-exceptions` toolchain can both use.

### Events

```cpp
void onConnected(void (*handler)());
void onConnected(void (*handler)(void*), void* context);
void onDisconnected(void (*handler)());
void onDisconnected(void (*handler)(void*), void* context);
```

`onConnected` runs when a **session** is established, which is the moment work
will actually go — not when the socket opened.

### Knowing what is going on

```cpp
bool isConnected() const;
bool isFresh(const char* filter, mq_time_t withinMs) const;
mq_time_t sinceMessage(const char* filter) const;    // 0xFFFFFFFF if never

MqttVersion activeVersion() const;
uint16_t activeKeepAlive() const;
const MqttStatus& status() const;
MqttState state() const;
const char* stateText() const;
MqttResult lastError() const;
const char* lastErrorText() const;

bool willRetainRefused() const;   // see below
bool sessionWasResumed() const;   // the broker had a session for us

size_t queued() const;            // waiting to go out; rises during an outage
size_t queueCapacity() const;

size_t subscriptionCount() const;
const MqttDeviceSubscription* subscriptionAt(size_t index) const;
const MqttDeviceSubscription* subscriptionFor(const char* filter) const;
```

`isFresh()` is false when the link is down **however recent the value**, and
false when nothing has ever arrived. The filter is matched as you wrote it in
`onMessage()` or `subscribe()`.

**It is per filter, not per topic**, and that is deliberate. A subscription to
`plant/+/level` tracks when *anything* matching it last arrived, not each
device separately — tracking every topic a wildcard matched would mean an
unbounded table on a device with no spare memory, and only the application
knows which devices it actually expects to hear from. A supervisory panel
watching several devices keeps its own small table, which is what
`examples/arduino/Alarm_On_Silence` shows. Subscribe to one topic exactly and
the two questions are the same.

`willRetainRefused()` is true when this broker refused a **retained Will**
`[V5:MQTT-3.1.2-11]`. There is no way to avoid that in advance — Retain
Available arrives in the answer to the CONNECT that already carried the Will —
so the device sends the Will again without retain rather than retrying a
rejected CONNECT for ever. The availability topic then works, but is not
retained: a dashboard connecting later will not learn this device's state until
it changes.

`sessionWasResumed()` reports what the last CONNACK said. When it is **false**,
every subscription has just been re-issued and anything that had been sent and
not acknowledged is gone — so a device holding state of its own should
republish it.

`MqttDeviceSubscription` carries `received`, `lastMessageAt`, `acknowledged`
and `lastResult`. `acknowledged` is the one worth checking at commissioning: a
SUBACK can refuse, and a refused subscription delivers nothing while looking
perfectly healthy.

### Host TLS

```cpp
void secure(const char* caFilePath = nullptr);   // and the default port becomes 8883
void secureWithCertificate(const char* caPem);
void clientCertificate(const char* certificatePath, const char* keyPath);
void insecureNoCertificateValidation();
```

Built whenever `<openssl/ssl.h>` is present. On Arduino there is nothing here to call:
a secure channel *is* an Arduino channel, so you hand `WiFiClientSecure` or
`SSLClient` to the constructor instead. See [transports.md](transports.md).

### The whole library, when you outgrow this

```cpp
MqttClient& client();
MqttTransport* transport();
const MqttCapabilities& capabilities() const;
MqttConnectionConfig& connection();     // change anything, before connect()
MqttClientConfig& behaviour();
```

---

## MqttPayload

What a handler is given. The bytes, plus the readings an application actually
wants.

```cpp
const uint8_t* bytes() const;     // valid ONLY for this call
size_t length() const;
bool isEmpty() const;
const char* text() const;         // NUL-terminated, never null
uint8_t qos() const;
bool retained() const;
const MqttMessage& message() const;   // everything else, 5.0 properties included

bool equals(const char* other) const;
double toNumber(double fallback = 0.0) const;
long toInteger(long fallback = 0) const;
bool toBool(bool fallback = false) const;

// What the SENDER said about the message - 5.0 only, all null or zero
// otherwise and on a message that carried none.
const char* responseTopic() const;          // null when this is not a request
const uint8_t* correlationData() const;
size_t correlationLength() const;
const char* contentType() const;
const char* userProperty(const char* key) const;
uint32_t secondsUntilExpiry() const;        // what is LEFT of the expiry
bool declaredAsText() const;                // Payload Format Indicator == 1
bool isRequest() const;                     // it has a Response Topic
```

`bytes()` points into the receive buffer and the next `poll()` overwrites it.
Copy anything that must outlive the handler.

`text()` is a NUL-terminated copy, so the payload can go straight to `strcmp`,
`atoi` or `Serial.println`. A payload containing a zero byte reads as ending
there — `bytes()` and `length()` are the whole of it, always.

Every conversion takes a **fallback** and returns it rather than a wrong
answer. `"21.5 degC"` is refused, not read as 21.5: the part that was not
understood is usually where the fault is. Leading and trailing whitespace is
ignored, because a payload from a shell or a file often has a newline on it.

`toBool` accepts `1`/`0`, `true`/`false`, `on`/`off`, `yes`/`no`,
`open`/`closed`, `start`/`stop`, `running`/`stopped`, in any case. Anything
else is the fallback.

`secondsUntilExpiry()` is the one thing in MQTT that puts an **age** on a
retained value: a broker serving a retained message recalculates the expiry and
sends what is left of it `[V5:3.3.2.3.3]`. Almost nothing sets it, so treat it
as the exception rather than the protection — `isFresh()` is what you act on.

**What this layer costs:** one copy of the payload per message, so that
`text()` can exist. Properties are copied only when a message actually carries
some, which is every 3.1.1 message never and most 5.0 messages not at all. An application moving large payloads should use
`client().onMessage()` and the zero-copy spans instead.

---

## MqttClient

```cpp
#include <ModBeeMqtt.h>
```

### Starting and stopping

```cpp
void begin(MqttTransport& transport,
           const MqttConnectionConfig& connection,
           const MqttClientConfig& behaviour = MqttClientConfig());
```

The transport is **yours** and must outlive the client. The library does not
own your network stack and cannot decide when it starts.

Allocates its buffers here, once, and never grows them afterwards.

```cpp
void end();
```

Closes everything and **concludes every outstanding operation** with
`Cancelled`. The contract does not lapse because the application stopped.

```cpp
void poll();
```

Call it often. It never waits for anything.

### Connecting

```cpp
MqttResult connect();
MqttResult disconnect(uint8_t reasonCode = MQ_REASON_NORMAL_DISCONNECTION);
```

`connect()` returns `Queued` and the outcome arrives as a state change on a
later `poll()`. Watch it with `isConnected()`, `state()`, or an
`onStateChange` handler.

`disconnect()` sends a DISCONNECT and closes. On 5.0 you may give a reason;
`MQ_REASON_DISCONNECT_WITH_WILL` (0x04) asks the broker to publish your Will
anyway, which is how a device says "I am going down and I want that known".

### Publishing

```cpp
MqttResult publish(const char* topic, const uint8_t* payload, size_t length,
                   uint8_t qos = MQ_QOS_0, bool retain = false,
                   MqttCompletionFn onComplete = nullptr, void* context = nullptr);

MqttResult publish(const char* topic, const char* payload,
                   uint8_t qos = MQ_QOS_0, bool retain = false,
                   MqttCompletionFn onComplete = nullptr, void* context = nullptr);

MqttResult publishSeries(const char* topic, const uint8_t* payload, size_t length,
                         uint8_t qos = MQ_QOS_0, bool retain = false,
                         MqttCompletionFn onComplete = nullptr, void* context = nullptr);
```

**`publish()` may be superseded.** A newer value for the same topic replaces one
that is still waiting, and the replaced caller is told with
`MqttResult::Superseded`. That is what you want for a reading or a setpoint:
the value that arrives should be the value that is true.

**`publishSeries()` never is.** Use it where the messages are a sequence rather
than a value — a counter, an event log, a Sparkplug data stream. Eliding a
member of a sequence leaves a gap the receiver reads as loss.

Both are ordered: what you publish in order leaves in order.

```cpp
// A pressure reading. Only the latest matters.
mqtt.publish("plant/p1", "3.2", MQ_QOS_1);

// An event. Every one matters.
mqtt.publishSeries("plant/events", eventBytes, length, MQ_QOS_1);
```

```cpp
size_t withdraw(const char* topic);
```

**Taking back what has not gone yet.** `withdraw()` removes every publish to a
topic that is still queued and unwritten, concluding each as `Superseded`, and
returns how many went.

It is for a publisher that is about to restate everything — a full refresh, a
re-announcement, a birth certificate. The queued update is already redundant,
and on a metered link it costs bytes to say something twice. In Sparkplug it is
required: a rebirth request means stop sending data *immediately*, and a poll
services receive before it services the queue, so a reading published earlier in
the same scan is still sitting there when the request arrives.

It cannot reach anything already written. Those bytes are gone and their
acknowledgement is still owed, so this is a way to **send less, never a way to
undo**.

```cpp
// The host asked for everything again. What was queued is about to be
// restated anyway.
mqtt.withdraw("plant/line3/readings");
publishFullSnapshot();
```

**5.0 properties, in both directions.** There is an overload of each that
takes an `MqttOwnedProperties` after `retain`, and it is copied into the queue
so it may be a local:

```cpp
MqttOwnedProperties properties;
properties.setContentType("application/json");
properties.setResponseTopic("plant/line3/reply");
properties.setCorrelationData("batch-4471");
properties.setMessageExpiry(30);                  // seconds
properties.addUserProperty("issued-by", "panel-1");

mqtt.publish("plant/line3/batch", payload, length, MQ_QOS_1, false, properties);
```

On a **3.1.1** connection that publish is refused with
`NotSupportedByProtocol` rather than sent with its properties stripped — a
publish that quietly lost its correlation id would leave the requester waiting
for an answer nothing can be matched to. Topic Alias is deliberately not
settable: `MqttAliases` negotiates and owns that table per connection.

Inbound is complete too — every property is decoded and handed to you on
`MqttMessage::properties`. See
[versions.md](versions.md#properties-in-both-directions).

The payload is **copied** into the queue before `publish()` returns, so the
buffer you passed is yours again immediately.

### Subscribing

```cpp
MqttResult subscribe(const char* filter, uint8_t qos = MQ_QOS_0,
                     MqttCompletionFn onComplete = nullptr, void* context = nullptr);

MqttResult subscribeWithOptions(const char* filter, uint8_t options,
                                MqttCompletionFn onComplete = nullptr,
                                void* context = nullptr);

MqttResult unsubscribe(const char* filter,
                       MqttCompletionFn onComplete = nullptr, void* context = nullptr);
```

**Subscribing before the connection exists is recorded, not refused.** A
subscription is a standing intent the library owns: it is established when
possible and re-issued on reconnect whenever the broker reports no session.

For the 5.0 options, build the byte rather than remembering the bits:

```cpp
mqtt.subscribeWithOptions("plant/#",
    mq_subscribeOptions(MQ_QOS_1, /*noLocal=*/true));
```

```cpp
uint8_t mq_subscribeOptions(uint8_t qos,
                            bool noLocal = false,
                            bool retainAsPublished = false,
                            uint8_t retainHandling = MQ_RETAIN_SEND_ALWAYS);
```

At 3.1.1 those options do not exist, and the codec **refuses** rather than
dropping them — so you find out, instead of subscribing on terms nobody asked
for.

---

## Configuration

### MqttConnectionConfig — what you send, and the broker may revise

```cpp
std::string host;
uint16_t    port = MQ_PORT_TCP;         // 1883, or MQ_PORT_TLS = 8883
std::string clientId;

std::string          userName;
std::vector<uint8_t> password;          // Binary Data at both versions
bool hasUserName = false;
bool hasPassword = false;

MqttVersion version         = MqttVersion::V5;
bool        fallbackEnabled = false;
MqttVersion fallbackVersion = MqttVersion::V311;

bool     cleanStart = true;
uint16_t keepAlive  = 60;               // seconds; 0 disables it

uint32_t sessionExpiryInterval = 0;     // 5.0 only
bool     hasSessionExpiry      = false;

uint16_t receiveMaximum     = 0;        // 0 leaves the default of 65,535
uint16_t topicAliasMaximum  = 0;        // how many aliases WE will accept
uint32_t maximumPacketSize  = 0;        // 0 states no limit

MqttWillConfig will;
```

**On `cleanStart` and sessions.** At 3.1.1, Clean Session 0 keeps the session
indefinitely. At 5.0, Clean Start 0 **without** a Session Expiry Interval means
the session still ends when the connection does — so if you want a session to
survive a dropout at 5.0, set both:

```cpp
connection.cleanStart = false;
connection.sessionExpiryInterval = 3600;
connection.hasSessionExpiry = true;
```

**On fallback.** `fallbackEnabled` retries at 3.1.1 only on evidence that the
*version* was the problem: a 5.0 CONNACK with `0x84`, or a 3.1.1 CONNACK with
return code `0x01`. Never on an authentication failure, a TLS failure or a
network error — treating a network fault as a version problem hides the network
fault. At most one fallback per attempt, and the version that worked is
remembered.

### MqttWillConfig

```cpp
will.set("plant/plc-01/status", "offline", MQ_QOS_1, /*retain=*/true);
will.set("spBv1.0/.../NDEATH", bytes, length, MQ_QOS_1, false);  // binary
will.delayInterval = 30;   // 5.0: seconds before the broker publishes it
```

The payload is **bytes**, not a string, because a Will may be protobuf.

A Will Delay Interval is what lets a device reboot without every dashboard
flashing red: the broker waits, and cancels the Will if the client comes back
inside the interval.

### MqttClientConfig — what is yours alone

```cpp
size_t queueCapacity      = 32;      // operations outstanding at once
size_t receiveBufferSize  = 4096;    // ONE WHOLE PACKET
size_t transmitBufferSize = 4096;

mq_time_t reconnectInitialMs = 1000;
mq_time_t reconnectMaximumMs = 60000;
bool      autoReconnect      = true;
mq_time_t connectTimeoutMs   = 20000;
mq_time_t acknowledgementTimeoutMs = 60000;   // 0 = wait for ever
```

**`acknowledgementTimeoutMs` is not a retransmission timer.** MQTT sets no
deadline by which a server must acknowledge a PUBLISH, so a broker that accepts
a QoS 1 message and never PUBACKs it is conforming — and the operation would
stay outstanding for ever, holding a unit of the peer's Receive Maximum. After
`receiveMaximum` of them the client cannot publish at QoS > 0 at all, silently,
with the link up and keepalive passing. This bounds the wait: the operation is
**given up on** with `AckTimeout`, the caller is told, and the quota is
returned. Nothing is resent — [V5:MQTT-4.4.0-1] forbids that outside a session
resume. Set it to `0` to wait indefinitely.

**The buffers hold one whole packet each.** There is no hidden ceiling and no
silent fragmentation: publishing a 60 kB payload means setting
`transmitBufferSize` to at least that, and a packet that does not fit is
refused with `PacketTooLarge` rather than truncated. On a host that costs
nothing; on a board it is the single number that decides the largest message
the device can carry, which is why it is configuration rather than a constant.

---

## Results

Every call returns an `MqttResult`, and **every accepted operation reaches
exactly one conclusion.** Nothing is ever silently dropped.

```cpp
bool mq_accepted(MqttResult);      // Queued or Ok
bool mq_succeeded(MqttResult);     // Ok
bool mq_worthRetrying(MqttResult); // a full queue, yes; a bad topic, no
const char* mq_resultName(MqttResult);
```

| Value | What happened |
| --- | --- |
| `Queued` | Accepted. The outcome arrives on a later `poll()`. |
| `Ok` | Completed, and completed successfully. |
| `NotInitialised` | `begin()` has not been called. |
| `InvalidArgument` | |
| `InvalidTopic` | Failed 4.7 validation — a wildcard in a topic name, an empty filter. |
| `NotConnected` | And there is no session to queue into either. |
| `QueueFull` | Past `queueCapacity`. Never a silent drop. |
| `Duplicate` | The identical operation is already queued. |
| `Superseded` | Replaced by a later write to the same topic, or withdrawn. |
| `AckTimeout` | Sent, and the peer never acknowledged it. Nothing is resent. |
| `Cancelled` | You asked, or the client was ended. |
| `NotSupportedByProtocol` | 3.1.1 cannot express this at all. |
| `NotSupportedByPeer` | 5.0 can, and this broker said no. |
| `PacketTooLarge` | Over the peer's stated Maximum Packet Size. |
| `QuotaExceeded` | Our send quota is exhausted. |
| `NoPacketIdentifier` | Every identifier is in flight. |
| `Refused` | A failure with no more specific member. |
| `NoMatchingSubscribers` | 0x10 — delivered nowhere, **which may be fine**. |
| `TopicNameInvalid` | 0x90 |
| `NotAuthorized` | 0x87 |
| `PayloadFormatInvalid` | 0x99 |
| `QuotaExceededByPeer` | 0x97 |
| `PacketIdentifierInUse` | 0x91 |
| `ConnectionLost` | Dropped while this was outstanding. |
| `SessionLost` | Dropped, and the session did not survive it. |
| `ConnectionRefused` | CONNACK said no. |
| `ProtocolError` | The peer said something the standard does not allow. |
| `MalformedPacket` | The peer's bytes did not parse. |
| `KeepAliveTimeout` | No PINGRESP inside the interval. |
| `SessionTakenOver` | 0x8E — another connection took our identifier. |

`NotSupportedByProtocol` and `NotSupportedByPeer` are separate on purpose: they
send an engineer to different places. The first means "this broker never could";
the second means "this broker will not, today".

---

## Handlers and polled state

**Every event is available twice: as a callback, and as pollable state.**
Generated IEC 61131 function-block code cannot register a lambda, so everything
a handler tells you is also a number you can read.

```cpp
void onMessage(MqttMessageFn handler, void* context);
void onSessionEstablished(MqttSessionFn handler, void* context);
void onStateChange(MqttStateFn handler, void* context);
```

```cpp
typedef void (*MqttMessageFn)(void* context, const MqttMessage& message);
typedef void (*MqttSessionFn)(void* context, bool sessionPresent);
typedef void (*MqttStateFn)(void* context, MqttState state, MqttResult reason);
typedef void (*MqttCompletionFn)(void* context, MqttResult result, uint16_t packetId);
```

Function pointers with a context, not `std::function`: these are on the path of
every packet, and a wrapper needs a form that neither allocates nor captures.

### MqttMessage

```cpp
struct MqttMessage {
    MqttSpan topic;
    MqttSpan payload;
    uint8_t  qos;
    bool     retain;
    bool     dup;
    uint16_t packetId;
    const MqttProperties* properties;   // null on a 3.1.1 connection
};
```

**The spans point into the receive buffer and are valid only for the duration
of the handler.** Anything that must outlive it copies what it needs. This is
the same rule the decoder follows one layer down, and it is what lets a 200 kB
payload pass through without being copied twice.

### onSessionEstablished

Runs when a session comes up, **before anything queued is sent**, and what it
publishes goes out **first**. That is the hook a Sparkplug layer hangs its
birth certificate on: an NBIRTH has to precede the data it introduces, and a
hook that ran after the queue drained would put it in the middle.

### MqttStatus

```cpp
const MqttStatus& status() const;
```

```cpp
MqttState   state;
MqttVersion version;
bool        connected;

uint32_t connects, disconnects, reconnects;
uint32_t published, received, publishFailures;
uint32_t protocolErrors;

MqttResult lastError;
uint8_t    lastReasonCode;    // what the peer said; 0 at 3.1.1, which cannot
```

**Counters are running totals, never flags.** Compare one with what you saw
last, and two readers cannot take each other's edges.

---

## Handles

Topics by number, for code that cannot hold strings.

```cpp
MqttTopicHandle registerTopic(const char* topic);
MqttResult publishTo(MqttTopicHandle, const uint8_t* payload, size_t length,
                     uint8_t qos = MQ_QOS_0, bool retain = false);
MqttResult subscribeTo(MqttTopicHandle, uint8_t qos = MQ_QOS_0);
const char* topicOf(MqttTopicHandle) const;
const MqttTopicState& topicState(MqttTopicHandle) const;
bool takeMessage(MqttTopicHandle, uint8_t* buffer, size_t capacity, size_t& length);

MqttTopicRegistry& topics();    // the whole registry, if you want it
```

A function block passing a `STRING[255]` by value every scan is both a
performance problem and an ergonomic one — the block grows a 255-byte input
nobody wants to wire. Register once, publish by `INT`.

Registering the same text twice returns the **same** handle.

```cpp
struct MqttTopicState {
    uint32_t published, failed, received;
    MqttResult lastResult;
    uint8_t payload[MQTT_HANDLE_PAYLOAD_MAX];
    uint16_t payloadLength;
    bool truncated;
};
```

`takeMessage()` consumes the payload so you can tell a new message from the
same one read twice. The **counters** are not consumed: they are totals.

A handle may hold a **filter**, not just a name — a block subscribed to
`plant/+/pressure` sees what arrives on `plant/line3/pressure`.

---

## Transports

A transport is an ordered, reliable byte stream and nothing else. No platform
types, no knowledge of what the bytes mean, and no knowledge of which end of
the connection it is on.

**Six methods to write one.** `begin()`, `end()`, `write()` and `isConnected()`
are required; for reading you implement **either** `readable()` plus
`read(uint8_t*, size_t)` — the bulk pair, which is what the library calls — or
the older `available()` plus one-byte `read()`, which the base class loops for
you. A transport written against the byte-wise pair compiles and runs untouched;
`test/test_link.cpp` keeps one around as a standing check that it still does.

Six further methods are defaulted and can be ignored: `waitHandle()`,
`hasBufferedInput()`, `peerClosed()`, `peerName()`, and
`peerCertificateName()` / `peerCertificateFingerprint()` - the CN and SHA-256
of a client certificate a TLS listener verified, empty otherwise. The first two are how a
host broker asks the kernel which connections have something to say instead of
asking every connection in turn; a board overrides neither and polls exactly as
it always did. See [extending.md](extending.md) for why that pair exists and
what it measured.

### On a host

```cpp
MqttTcpTransport transport;
transport.configure("broker.example.com", 1883);
```

```cpp
MqttTlsTransport transport;             // hosts with wolfSSL or OpenSSL
transport.configure("broker.example.com", 8883);
transport.setCaFile("/etc/ssl/certs/ca-certificates.crt");   // or the system store
transport.setClientCertificate("client.crt", "client.key");  // optional
```

Or trust one key rather than an authority, for a broker nobody signed for:

```cpp
MqttTlsTransport transport;
transport.configure("10.0.0.10", 8883);
transport.setPinnedKeyFingerprint("23c3ca67edd8...");   // 64 hex characters
// transport.peerKeyFingerprint() is what it was actually shown, pinned or not
```

Never pin a cloud broker — it rotates every ninety days. See
[security.md](security.md#pinning-when-nobody-signed-for-the-broker).

### On Arduino

`MqttStreamTransport<ChannelType>` is a template over **any Arduino `Client`**.
It calls six methods — `connect`, `stop`, `available`, `read(buf, size)`,
`write`, `connected` — so every one of these works, with no change to the
library. `read` is the **bulk** `Client::read(uint8_t*, size_t)`, and never the
blocking `Stream::readBytes()`:

```cpp
WiFiClient plain;
MqttStreamTransport<WiFiClient> wifi(plain, "192.168.1.10", 1883);

EthernetClient wired;                       // what a plant panel usually is
MqttStreamTransport<EthernetClient> ethernet(wired, "192.168.1.10", 1883);

TinyGsmClient cellular(modem);              // a remote site, through TinyGSM
MqttStreamTransport<TinyGsmClient> gsm(cellular, "mqtt.example.com", 1883);

WiFiClientSecure secure;                    // native TLS, ESP32 and friends
MqttStreamTransport<WiFiClientSecure> tls(secure, "broker", 8883);

SSLClient wrapped(wired, TAs, TAs_NUM, A5); // TLS on a board that has none
MqttStreamTransport<SSLClient> ethernetTls(wrapped, "broker", 8883);
```

Or hand the channel to [`MqttDevice`](#mqttdevice) and skip the transport
entirely.

**TLS is a transport, not a feature.** Nothing above the transport changes, and
on Arduino it costs nothing extra because a secure channel already *is* an
Arduino `Client`.

**The one call in the library that can stall is `MqttTransport::begin()`, and
only on Arduino.** `Client::connect()` is synchronous on every core — seconds,
on a cellular modem. It is reached from `MqttClient::connect()` and from a
reconnect attempt inside `poll()`, and nowhere else.
[transports.md](transports.md) says what to do about that in a scan cycle.

Certificate validation is on by default and the opt-out is named so it cannot
be typed by accident: `setInsecureNoCertificateValidation()`. See
[security.md](security.md) for what "it connected" does and does not prove.

---

## MqttTlsListener

`MqttTcpListener`'s encrypted peer, and interchangeable with it: the broker
cannot tell them apart, so one broker can serve 1883 and 8883 at once.

```cpp
MqttTlsListener secure;
secure.configure("0.0.0.0", 8883, 32);          // address, port, connections
secure.useCertificateFile("plc.crt", "plc.key");   // or useCertificate(pem, pem)
secure.requireClientCertificate(caPem);            // optional, mutual TLS
if (!secure.begin()) {
    // lastError() is a sentence an operator can act on - "the private key does
    // not match the certificate", "port 8883 is already in use"
}
broker.addListener(secure);
```

`requestClientCertificate(caPem)` in place of `requireClientCertificate`
asks for a client certificate without demanding one: one that is offered must
verify, and a client that offers none is let through. Either way an accepted
connection's `peerCertificateName()` and `peerCertificateFingerprint()` are the
verified certificate's CN and SHA-256.

**Changing the certificate of a running listener.** The new context is made
first, away from the socket, so a pair that will not load is refused while the
old one is still served; then it is swapped in on the thread that calls
`accept()`. Connections already accepted keep the context they shook hands
with.

```cpp
MqttTlsListener maker;                               // never begun: it only builds
maker.useCertificate(newCertPem, newKeyPem);
maker.requireClientCertificate(caPem);
void* context = maker.makeContext();                 // null, with maker.lastError(), if it will not load
if (context) secure.useContext(context);             // on the accept() thread; secure owns it now
```

`shareContext()` gives another reference to the same context where the engine
shares one across threads (OpenSSL) and null where it does not (wolfSSL);
`dropContext()` frees one nobody took.

`boundPort()` reports what was actually bound, which differs from what was
asked for whenever port 0 was requested — how a test gets a free port without
racing. `active()` and `capacityRemaining()` say how many slots are in use; the
broker reads `capacityRemaining()` to decide whether an arriving connection gets
a session or a `0x89 Server Busy`.

### What a certificate says

`transport/MqttCertificate.h` reads a certificate the same way on both
engines - the engine supplies the DER, one parser reads it:

```cpp
MqttCertificateFacts facts;
std::string why;
if (mqttReadCertificate(pem, facts, why)) {
    // facts.fingerprint  SHA-256 of the DER, 64 lowercase hex
    // facts.commonName   "" unless the subject has exactly one CN
    // facts.subject      "O=Plant, CN=plc-01", in the certificate's order
    // facts.notAfter     "2036-10-04T09:33:00Z", and notAfterSeconds
    // facts.names        "DNS:plc-01", "IP:10.0.0.10"
}
mqttCertificateMatchesKey(certPem, keyPem, why);     // the key is this certificate's own
mqttCountCertificates(bundlePem);                    // a CA bundle's size
```

This fingerprint is of the whole certificate, where `MqttIdentity`'s is of the
key: a login bound to a certificate should not survive its reissue, a pin
should.

### Listening from a board

Two listeners are templates over an Arduino server type and its client type,
so the board itself can be the broker. Neither needs a descriptor, because a
`WiFiClient` has no number behind it.

```cpp
WiFiServer plainSocket(1883);
MqttStreamListener<WiFiServer, WiFiClient> plain(plainSocket);
plain.configure(4);                         // four served, plus the reserve
broker.addListener(plain);

WiFiServer tlsSocket(8883);
MqttStreamTlsListener<WiFiServer, WiFiClient> secure(tlsSocket);
secure.configure(4);
broker.addListener(secure);                 // certificate made on first start
Serial.println(secure.fingerprint().c_str());
```

`MqttStreamListener` needs nothing installed and has no guard around it.
`MqttStreamTlsListener` needs an engine, and no engine type appears in its
header — the TLS work is behind `MqttTlsAccept`, reached through
`MqttByteChannel`, so a sketch that never serves TLS does not pull an engine
into its translation unit. On a wired panel both take `EthernetServer` and
`EthernetClient` instead.

See [broker.md](broker.md#on-a-board),
[examples/arduino/Panel_Broker](../examples/arduino/Panel_Broker) and
[examples/arduino/Secure_Broker](../examples/arduino/Secure_Broker).

---

## MqttIdentity

A key pair and a certificate of the device's own, so a broker can serve TLS
without anybody having issued it anything.

```cpp
MqttIdentity identity;
identity.setCommonName("plc-01");
identity.setSubjectAltNames("DNS:plc-01,IP:10.0.0.10");
identity.loadOrGenerate("plc.crt", "plc.key");     // mint once, then find it

identity.fingerprint();        // 64 hex characters - what clients pin
identity.certificatePem();     // for a listener, or to hand to a client
identity.privateKeyPem();
```

EC P-256, because keygen is about a second on an ESP32 where RSA-2048 is
minutes. The certificate carries `CA:TRUE`, so a client that would rather
validate a chain than pin a key can load this very file as its trust anchor.

`loadFiles`, `saveFiles` and `loadOrGenerate` exist only where there is a file
system; on a board `load()` and `certificatePem()` are the whole interface, and
where the identity is kept — flash, a retained variable — is the caller's
business.

`mqttKeyFingerprint(pem)` computes the same 64 characters for a certificate the
caller did not make.

---

## MqttBroker

always compiled. See [broker.md](broker.md) for the whole thing.

`<ModBeeMqtt.h>` brings it in when the option is on, so there is no second
include to remember.

```cpp
MqttTcpListener listener;
MqttBroker broker;

listener.configure(nullptr, 1883, /*maxConnections=*/32);
listener.begin();

broker.begin(config);
broker.addListener(listener);

while (running) broker.poll();
```

```cpp
void begin(const MqttBrokerConfig& config = MqttBrokerConfig());
void end();
bool addListener(MqttListener& listener);
void poll();

bool publish(const char* topic, const uint8_t* payload, size_t length,
             uint8_t qos = MQ_QOS_0, bool retain = false);

void onAuthorise(MqttBrokerAuthFn handler, void* context);
void onMessage(MqttBrokerTapFn handler, void* context);
void onConnection(MqttBrokerConnectionFn handler, void* context);

const MqttBrokerStatus& status() const;
MqttRetainedStore& retained();
MqttBrokerSession* findSession(const char* clientId);
bool isConnected(const char* clientId) const;
size_t connectionCount() const;
size_t sessionCount() const;
```

---

## MqttBridge

always compiled. See [bridge.md](bridge.md).

```cpp
MqttResult begin(MqttTransport& local, MqttTransport& remote,
                 const MqttBridgeConfig& config);
MqttResult start();
void stop();
void poll();

const MqttBridgeStatus& status() const;
bool isUp() const;
const std::string& lastError() const;
```

`begin()` **refuses** a configuration that would loop, and `lastError()` says
which two filters would have done it. That refusal is the feature.

---

## Typed values

always compiled. See [typed-values.md](typed-values.md).

```cpp
MqttTypedBindings bindings;
bindings.begin(mqtt);

float pressure = 0.0f;
bindings.bind("plant/p1/pressure", &pressure,
              /*deadband=*/0.5, /*maxIntervalMs=*/60000, MQ_QOS_1);

// in your loop, after mqtt.poll()
bindings.poll();
```

```cpp
MqttPoller poller;
poller.begin(mqtt);
poller.track("plant/p1/pressure", MqttValueType::Float32, 60000, MQ_QOS_1);

const double reading = poller.number("plant/p1/pressure", /*fallback=*/-1.0);
const MqttQuality quality = poller.quality("plant/p1/pressure");
```

`number()` hands back the fallback unless the quality is `Good`, so a control
path cannot act on a stale reading by accident.

---

## Underneath

All of this is public. An application that outgrows the surface above keeps
everything it has already written.

| | |
| --- | --- |
| `capabilities()` | `MqttCapabilities` — what **this** connection can actually do |
| `session()` | `MqttSession` — what survives a reconnect |
| `operations()` | `MqttOperations` — the queue and its one-conclusion contract |
| `activeVersion()` | the version in force, which is not always the one configured |
| `activeKeepAlive()` | the interval in force, which the broker may have shortened |
| `transport()` | the transport you gave it |

And below those: `MqttCodecV5` / `MqttCodecV311`, `MqttPacket`, `MqttWire`,
`MqttTopic`, `MqttLink`. [ARCHITECTURE.md](ARCHITECTURE.md) explains what each
layer is for and why the boundaries are where they are.

**Ask `capabilities()`, never the version number.** That is what keeps version
handling out of application code, and what lets a layer written on top of this
work at both versions without knowing either.

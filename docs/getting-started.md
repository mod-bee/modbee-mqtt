# Getting started

If you have never used MQTT, start at the top. If you have, skip to
[Your first sketch](#your-first-sketch).

---

## Part 1 — what MQTT actually is

### The problem it solves

You have a tank level sensor. You want it on a screen in the control room, in a
log somewhere, and you want a pump to react to it.

The obvious way is for the sensor to connect to the screen, and to the logger,
and to the pump controller. Three connections, from a device with barely enough
memory for one. Now add a second screen, and a phone app, and a cloud service.
Every new thing that wants the reading means changing the sensor's program, and
a device in a plant room is the last place you want to be changing programs.

MQTT turns that inside out. **The sensor says the level once, to one place.
Anything that wants it asks that place.** The sensor never knows who is
listening, and does not change when somebody new starts listening.

### The broker

**The broker is the one place in the middle.** Think of it as a post office, or
a switchboard. Everything sends to it; everything receives from it; nothing
talks directly to anything else.

It is a program running on a server, a Raspberry Pi, an IoT gateway, or a cloud
service. Common ones are Mosquitto, EMQX and HiveMQ. You need exactly one, and
it needs to be reachable from everything that talks to it.

This library can be a broker too, if you want one inside your own program — see
[broker.md](broker.md) — but for a first system, install Mosquitto and point
everything at it.

### Clients

**A client is anything that talks to the broker.** Your Arduino is a client. So
is the screen in the control room, the historian, the phone app and the
engineer's laptop.

There is no "server side" and "device side" in MQTT. Every one of them is a
client, and they all speak exactly the same protocol. That is this library's
job: it makes your board one of them.

### Publish and subscribe

Two verbs, and that really is the whole protocol.

**Publish** — say something. *"The tank is at 4.2 metres."* You say it to the
broker and you are done. You do not know or care who hears it. Nobody may be
listening at all; that is not an error.

**Subscribe** — ask to be told. *"Tell me whenever anybody says anything about
the tank level."* From then on, the broker sends you each one as it happens.

A device usually does both: publishes what it measures, subscribes to what it
should do.

### Topics

**A topic is the subject line.** It is how the broker knows which message goes
to whom, and it is just text:

```
plant/tank-01/level
```

It looks like a file path and it works like one — levels separated by `/`,
going from general to specific. That is a convention, not a rule, but it is a
convention worth following because everything else assumes it.

**Decide your topic layout before you install the first device.** Changing it
later means changing every device and every screen. A layout that works:

```
site / area / device / what
plant/ north/ tank-01/ level
plant/ north/ tank-01/ status
plant/ north/ pump-2/  command
plant/ north/ pump-2/  state
```

There is no central registry. If you publish to `plant/tank-01/level` and
something subscribes to `plant/tank-01/level`, they are talking. If you spell
it `plant/tank01/level`, they are not, and nothing anywhere will tell you —
that is the single most common MQTT fault there is.

### Wildcards, so you do not subscribe a hundred times

When you subscribe you may use two wildcards:

| | | |
| --- | --- | --- |
| `+` | exactly one level | `plant/+/level` gets tank-01's level and pump-2's, but not `plant/tank-01/valve/level` |
| `#` | everything below, and must be last | `plant/#` gets every message under `plant/` |

Wildcards are for **subscribing only**. When you publish, the topic is exact —
`plant/+/level` is not a topic, it is a question.

### Retained messages

Normally a message goes only to whoever is subscribed **at that moment**.
Subscribe a minute later and you have missed it — you wait for the next one.

For a tank sampled every half hour, that is thirty minutes of a blank screen.

So a message can be marked **retained**. The broker keeps the last retained
message on each topic and hands it to anybody who subscribes afterwards.

```cpp
mqtt.publish("plant/tank-01/level", 4.2f, MQ_QOS_0, /*retain=*/true);
```

Rules of thumb:

- **retain a state** — a level, a setpoint, whether a pump is running
- **do not retain an event** — an alarm that happened, a button press, a
  reading in a stream

And the trap, because it catches everybody once: **a retained message usually
has no age on it.** The broker will hand you a level from a device that died
last Tuesday and say nothing about it. "I have a value" is not the same fact as
"this value is current" — see [Not acting on a stale value](#not-acting-on-a-stale-value).

(MQTT 5.0 has a partial answer: a publisher may set a Message Expiry Interval,
and a broker then serves the retained message with the time it has left. This
client can set one — `MqttOwnedProperties::setMessageExpiry()`, see
[versions.md](versions.md#properties-in-both-directions) — but it is optional
and most publishers do not, so treat it as the exception rather than the
protection.)

To delete a retained message, publish an **empty** payload to the same topic
with retain set. Publishing "cleared" instead leaves the old one there forever.

### QoS — how hard the protocol tries

Three levels. They are about *delivery effort*, and each costs something.

**QoS 0 — at most once.** Send it and forget it. If the packet is lost, it is
lost. No acknowledgement, no retry, one packet on the wire.

> Use it for: readings you publish repeatedly. If one 4.2 goes missing, another
> 4.2 is along in a second. This is the right default for almost all telemetry.

**QoS 1 — at least once.** The broker acknowledges. If the link dies before the
acknowledgement arrives, the message is sent again when it comes back — which
means **it can arrive twice**. (That resend needs a session to survive in; see
[Sessions](#sessions--surviving-an-outage). Without one there is nowhere for it
to be held, and it concludes as lost instead — reported, never silent.)

> Use it for: commands, state changes, alarms. Anything where losing it matters
> more than seeing it twice.
>
> And make what you do about it safe to repeat. `"stop"` twice is still
> stopped. `"toggle"` twice is a pump you did not want running. **Send states,
> not actions.**

**QoS 2 — exactly once.** A four-message handshake that guarantees one
delivery, once. Slowest, most memory, most round trips.

> Use it for: the rare thing that is neither safe to lose nor safe to repeat —
> a billing counter, a batch command. Most plants never need it.

**Both ends have a say.** You pick a QoS when you publish, and *separately*
when you subscribe. What you actually get is the lower of the two. Subscribing
at QoS 0 to a topic published at QoS 2 gets you QoS 0.

### The Will — how your absence gets noticed

A device that loses power does not get to say goodbye. Without help, it simply
stops publishing, and nothing can tell that apart from a quiet afternoon.

So when a client connects, it hands the broker a message to publish **on its
behalf** if the connection dies unexpectedly. That is the **Will**.

```cpp
mqtt.statusTopic("plant/tank-01/status");   // both halves, one call
```

That single line does three things this library would otherwise leave to you:

1. registers `"offline"` as the Will, retained
2. publishes `"online"`, retained, the instant the session comes up
3. publishes `"offline"` itself on a deliberate `disconnect()` — because a Will
   is **not** sent when you say goodbye properly, and a device that shut down
   cleanly would otherwise read as online for ever

Both halves matter. A Will with nothing ever publishing "online" is an alarm
that can never clear, and it is the most-forgotten half of most MQTT
deployments.

A broker that does not offer retained messages at all is handled too: the
device connects without retaining its Will rather than being refused for ever,
and `mqtt.willRetainRefused()` says that is what happened.

### Sessions — surviving an outage

By default, disconnecting throws everything away: your subscriptions, and any
QoS 1 or 2 message that was in flight. Reconnecting starts from nothing.

Ask the broker to keep it instead:

```cpp
mqtt.rememberSession(3600);      // keep my session for an hour
```

Now a dropped link is a pause rather than a loss. Your subscriptions are still
there when you come back, and a QoS 1 publish that was on the wire when the
link died is sent again.

This needs a **stable client identifier** — the session is kept against the
name you connect with, so a device that picks a new name each time gets a new
session each time.

### A bridge — two brokers, joined

A **bridge** connects two brokers so that some of the traffic on one appears on
the other.

Why you would want that: a site with a link to head office that is not
reliable. Put a broker **on the site**. Everything local talks to it, so the
plant keeps running whether or not the link is up. A bridge forwards the
subset that head office needs, and catches up when the link returns.

Without it, every device on site depends on a WAN connection to talk to the
device next to it.

See [bridge.md](bridge.md). The one thing to know now: **a bridge that forwards
both ways on overlapping topics is a loop**, and a loop will saturate both
sites with one message. This library refuses to build one rather than trying to
be clever about it.

---

## Part 2 — the code

### The one rule

**Nothing in this library waits for the network** — `connect()` included. One
object, one `poll()` in your loop.

That is why it exists. A call that waits for a network round trip cannot live
inside an OpenPLC scan cycle or an Arduino `loop()`, and most MQTT clients wait
somewhere. (The one honest exception is not ours and is documented plainly in
[transports.md](transports.md): Arduino's own `Client::connect()` is
synchronous on every core, and no library can change that.)

### What it needs from a board

A client wants roughly **4 kB of RAM** before any application data — a receive
buffer, a transmit buffer, an encode buffer, the send queue and the session
store — and `bufferSize()` is the number that moves it. TLS wants considerably
more again.

That rules out the 8-bit AVR boards, which is why `library.properties` does not
list them. ESP32, ESP8266, RP2040, STM32 and the mbed boards are all comfortable.
[footprint.md](footprint.md) has the measured numbers, flash and RAM.

**It needs C++14 and nothing newer**, which every core listed there provides —
the mbed-based cores compile at `gnu++14` and would reject a library that asked
for more. SAMD boards are not supported: their core still compiles at
`gnu++11`.

### Your first sketch

```cpp
#include <WiFi.h>
#include <ModBeeMqtt.h>

WiFiClient net;              // the sketch owns the network stack
MqttDevice mqtt(net);        // and this is everything else

void onLight(const char* topic, const MqttPayload& payload) {
    (void)topic;
    digitalWrite(LED_BUILTIN, payload.toBool() ? HIGH : LOW);
}

void setup() {
    pinMode(LED_BUILTIN, OUTPUT);
    WiFi.begin("plant-wifi", "");
    while (WiFi.status() != WL_CONNECTED) delay(250);

    mqtt.statusTopic("demo/board-1/status");        // online / offline, for you
    mqtt.onMessage("demo/board-1/light", &onLight); // subscribes AND handles
    mqtt.connect("192.168.1.10", "board-1");        // returns immediately
}

void loop() {
    mqtt.poll();                                    // the whole contract
    mqtt.publish("demo/board-1/reading", analogRead(A0));
}
```

That is a complete, working MQTT device. `examples/arduino/First_Sketch` is
this file with more comments.

Things worth noticing:

- **`connect()` does not connect.** It says where to go and returns. The link
  comes up across later calls to `poll()`, and `mqtt.isConnected()` is what
  says so. Nothing you can do will make it wait.
- **You do not have to wait for it.** Publishing while the link is down is
  held and sent when it comes back. Subscribing before it exists is recorded
  and issued when it does.
- **`poll()` is the only thing you must not forget.** Call it every time round
  the loop.

### Publishing

```cpp
mqtt.publish("plant/tank-01/level", 4.2f);                     // a float
mqtt.publish("plant/tank-01/count", 1704);                     // an integer
mqtt.publish("plant/pump-2/running", true);                    // "1" or "0"
mqtt.publish("plant/tank-01/mode", "auto");                    // text
mqtt.publish("plant/tank-01/level", 4.2f, MQ_QOS_1, true);     // QoS 1, retained
```

Numbers go out as **decimal text**, because that is what the rest of a plant
reads: a dashboard or a historian takes `"4.2"` without being configured, and a
raw binary float needs both ends to agree first.
[typed-values.md](typed-values.md) covers the binary option for when they have.

Bytes, when the payload is not text:

```cpp
const uint8_t frame[] = { 0x08, 0x01, 0x12, 0xFF };
mqtt.publish("plant/tank-01/raw", frame, sizeof frame, MQ_QOS_1);
```

Payloads are never interpreted and never validated. Only topics are.

#### Writes to one topic supersede

Publish a setpoint every scan and you will not send a hundred messages a
second. A publish to a topic that is **still waiting to be sent** replaces the
one that was waiting. What travels is the value the loop settled on, not a
history of everything it passed through.

That is what you want for a reading or a setpoint. It is exactly wrong for a
sequence — a counter, an event log, a Sparkplug data stream — where a missing
member reads as loss. For those:

```cpp
mqtt.publishSeries("plant/line3/events", "valve opened", MQ_QOS_1);
```

Nothing already **on the wire** is ever rewritten, whichever you use.

#### Let the library publish it for you

Most telemetry is the same shape: publish this variable when it moves enough,
and say it anyway if it has been quiet too long. That is one line:

```cpp
float tankLevel = 0.0f;

void setup() {
    mqtt.report("plant/tank-01/level", &tankLevel, /*deadband=*/0.02, /*at least every*/30000);
}

void loop() {
    mqtt.poll();                 // publishes it when it is worth publishing
    tankLevel = readTheSensor();
}
```

The deadband stops a noisy analogue input publishing every scan. The heartbeat
is what lets a dashboard tell a steady tank from a **dead transmitter** — with
a deadband alone, those two look identical to every subscriber, and that is the
half most systems leave out.

Retained by default, because a level is a state rather than an event. And
everything reported is published in full the moment the connection comes up, so
a screen opened at three in the morning sees real values immediately.

#### When a message must be confirmed

```cpp
void onStopConfirmed(void*, MqttResult result, uint16_t) {
    if (mq_succeeded(result)) { /* the broker has it */ }
    else { /* it did not go - mq_resultName(result) says why */ }
}

mqtt.publishAndConfirm("plant/pump-2/command", "stop", MQ_QOS_1,
                       &onStopConfirmed);
```

Exactly one conclusion arrives for every accepted message, always:
acknowledged, refused, superseded, or lost with the connection. Never silence.

### Asking something, and getting an answer back

Publish and subscribe are one-way. When you need an **answer** — "stop, and
tell me you stopped" — MQTT 5.0 has a pattern for it, and the library does the
bookkeeping:

```cpp
// The asking end
mqtt.onMessage("plant/gateway/replies", &onReply, MQ_QOS_1);
mqtt.publishRequest("plant/pump-2/command", "stop",
                    "plant/gateway/replies", "stop-42");

// The answering end - it names neither the reply topic nor the id
void onCommand(const char* topic, const MqttPayload& request) {
    stopThePump();
    mqtt.reply(request, "stopped");
}
```

The request carries **where to answer** and **an id to answer with**. `reply()`
reads both off it, so the answering device needs to know nothing about the
asker — which is what makes it work when there are ten of them.

The id (`"stop-42"`) comes back with the answer, so a gateway with five
requests outstanding knows which one was answered. Use a counter.

This is 5.0 only. On a 3.1.1 connection both calls are **refused** rather than
sent without the parts that make them work — an answer nothing can be matched
to is worse than no answer.

### Subscribing

```cpp
void onCommand(const char* topic, const MqttPayload& payload) {
    if (payload.equals("stop")) stopThePump();
}

mqtt.onMessage("plant/pump-2/command", &onCommand, MQ_QOS_1);
```

`onMessage(filter, handler)` **subscribes as well as registering the handler**,
because you never want one without the other. Each message goes to the handler
for the filter it matched, so you are not comparing topic strings by hand.

Every message, whichever filter matched — for a logger, or a gateway:

```cpp
mqtt.onMessage(&onAnything);     // as well as, never instead of
```

### Reading what arrived

The payload is bytes. `MqttPayload` gives you the readings you actually want:

```cpp
void onReading(const char* topic, const MqttPayload& payload) {
    const char* asText   = payload.text();          // NUL-terminated
    const double number  = payload.toNumber(-1.0);  // -1.0 if not a number
    const long counter   = payload.toInteger(0);
    const bool running   = payload.toBool(false);   // 1/0, true/false, on/off...

    const bool wasRetained = payload.retained();    // see below
    const size_t bytes     = payload.length();

    // What the sender said ABOUT the message - 5.0 only, null otherwise
    const char* type  = payload.contentType();       // "application/json"
    const char* who   = payload.userProperty("issued-by");
    const bool asking = payload.isRequest();         // it wants an answer
}
```

Each conversion takes a **fallback** and returns it rather than a wrong answer.
`"21.5 degC"` does **not** become 21.5 — the part that was not understood is
usually where the fault is, and quietly discarding it hides the fault inside a
plausible number.

### Not acting on a stale value

This is the one MQTT makes worse than most protocols. A connected broker will
serve a retained reading from a device that died last Tuesday and say nothing
about it.

```cpp
if (mqtt.isFresh("plant/+/level", 30000)) {
    // a message arrived within 30 seconds AND the link is up
    actOnIt();
} else {
    // do not act. mqtt.sinceMessage("plant/+/level") says how long it has been
}
```

The interval is yours, because only you know how often the value was promised.
A temperature sampled hourly and a pressure sampled ten times a second cannot
share a constant.

`payload.retained()` is the other half: it tells you the broker was repeating
the last thing said rather than somebody saying it now. A supervisory panel
that counted retained messages as signs of life would mark every device
healthy the moment it connected, dead ones included.
`examples/arduino/Alarm_On_Silence` is that panel, written correctly.

### Arduino `String` works everywhere

A sketch builds topics with `String` because that is what the core gives it, so
every call takes one:

```cpp
String base = "plant/" + deviceName;
mqtt.statusTopic(base + "/status");
mqtt.onMessage(base + "/command", &onCommand, MQ_QOS_1);
mqtt.publish(base + "/level", tankLevel);
```

### Which network — WiFi is one option, not the only one

```cpp
WiFiClient net;         MqttDevice mqtt(net);   // WiFi
EthernetClient net;     MqttDevice mqtt(net);   // wired
TinyGsmClient net(modem);  MqttDevice mqtt(net);  // cellular
```

Nothing else in the sketch changes. The transport is a template over any
Arduino network channel, so the library is the same on all of them.

What *does* differ is timing — a cellular modem's connect can take seconds, and
that is not something any library can make instant.
**[transports.md](transports.md) covers all of it**, including what to do about
that in a scan cycle.

### TLS

On Arduino, TLS is a property of the channel, so it costs nothing here:

```cpp
WiFiClientSecure net;              // ESP32 and friends: native TLS
net.setCACert(ROOT_CA);
MqttDevice mqtt(net);
mqtt.connect("broker.example.com", 8883, "tank-01");
```

On a board whose network chip has no TLS — most Ethernet chips, most cellular
modems — `SSLClient` wraps any channel and is itself a channel:
`examples/arduino/TLS_Over_Ethernet`.

On a host it is there whenever OpenSSL is - nothing to switch on:

```cpp
MqttDevice mqtt;
mqtt.secure("/etc/ssl/certs/ca.crt");            // port defaults to 8883
mqtt.clientCertificate("client.crt", "client.key");   // mutual TLS
mqtt.connect("broker.example.com", "gateway-1");
```

Certificate validation is **on by default**. The opt-out is
`insecureNoCertificateValidation()`, named at that length on purpose: it is the
one call that turns a TLS connection into an encrypted conversation with
whoever answered. See [security.md](security.md).

### A broker that wants a user name

Most cloud brokers do, and every hosted one does:

```cpp
MqttDevice mqtt;
mqtt.secure("/etc/ssl/certs/ca-certificates.crt");   // FIRST
mqtt.credentials("plant-operator", "s3cret");
mqtt.connect("mqtt.example.com", "gateway-1");
```

**That order is not decoration.** MQTT has no challenge and no hashing: the
password is bytes in the first packet you send. Without TLS it is a password
published to whoever is on the path, and nothing in the library warns you,
because it cannot know whether the segment is yours.

Either argument may be null, and each sets its own flag, so
`credentials("token", nullptr)` sends a user name and no password — which is
what a token-based broker usually wants, and is a different packet from one
with an empty password in it.

**If the broker says no, the client backs right off.** `lastErrorText()` says
why, and the client does not repeat the rejected password at its usual
backoff — a device doing that every second is how it gets itself banned, which
on some brokers is by address, so it takes everything else on your site with
it. It tries again every five minutes instead (`rejectedCredentialsRetryMs`),
so a broker fixed at its end brings a remote device back without anyone
visiting it. Call `connect()` to try at once.

### Talking to an old broker

`MqttDevice` asks for MQTT 5.0 and drops to 3.1.1 if the broker turns out to be
older. You do not have to do anything, and the fallback happens only on
evidence that the *version* is the problem — never on a bad password, a TLS
failure or a network error, because treating one of those as a version problem
hides it.

When you need to know exactly what is on the wire:

```cpp
mqtt.only(MqttVersion::V5);                  // no fallback
Serial.println(mq_versionName(mqtt.activeVersion()));   // what is in force
```

[versions.md](versions.md) says what 3.1.1 cannot do.

---

## When it does not work

**`connect()` returned true and `isConnected()` never becomes true.**
`connect()` returning true means the settings were usable, not that the broker
accepted you. Print `mqtt.stateText()` and `mqtt.lastErrorText()`. At 5.0 the
broker says why it refused; at 3.1.1 it says much less, and that is the
protocol rather than the library.

**A publish returned false.** Read `mqtt.lastErrorText()`. The usual ones:
`InvalidTopic` (a wildcard in a topic you published to — wildcards are for
subscribing), `QueueFull` (the link has been down long enough to fill the
queue; `mqtt.queued()` shows it filling), `NotSupportedByPeer` (the broker
refused retain, or your QoS — check `mqtt.capabilities()`).

**Messages are not arriving.** In order of likelihood:

1. The topic does not match. `plant/tank01` is not `plant/tank-01`, and nothing
   will tell you. Run MQTT Explorer or `mosquitto_sub -t '#' -v` and look at
   what is actually on the broker.
2. The subscription was refused. Accepted by us is not the same as granted by
   the broker — `mqtt.subscriptionFor(filter)->acknowledged` says which.
3. The publisher is publishing and nobody is retaining, and you subscribed
   afterwards.

**Everything works, then stops after a reconnect.** If the broker reported no
session, subscriptions are re-issued automatically — but anything that had
been *sent* and not acknowledged concludes as `SessionLost`, because there is
nowhere for it to go. `rememberSession()` is what prevents that.

**It works on the bench and not in the plant.** Nine times out of ten that is
port 1883 blocked, or DNS. `mqtt.stateText()` staying at `connecting` means
the TCP connection is not being made at all — nothing MQTT has happened yet.

---

## When you outgrow `MqttDevice`

Nothing is hidden. `MqttDevice` is a thin layer over `MqttClient` and a
transport, and both are right there:

```cpp
mqtt.client().publishSeries(topic, bytes, length, MQ_QOS_2, false, &onDone);
mqtt.connection().receiveMaximum = 8;      // before connect()
mqtt.capabilities().maximumPacketSize;
```

Everything you have already written keeps working alongside it. The long form
is in [api.md](api.md), and several examples use it deliberately so you can see
what it looks like.

---

## Next

- [recipes.md](recipes.md) — five jobs, done properly: a tank on a dashboard, a
  pump that confirms, a site that keeps running, a device whose silence raises
  an alarm, a value you must not act on
- [transports.md](transports.md) — WiFi, Ethernet, cellular, TLS, and the one
  place something genuinely blocks
- [api.md](api.md) — the reference, in the order you would meet it
- [versions.md](versions.md) — what 3.1.1 cannot do
- [security.md](security.md) — TLS, and what "it connected" does and does not prove
- [broker.md](broker.md) — a broker in your own binary
- [bridge.md](bridge.md) — two brokers, and the loop you must not build
- [typed-values.md](typed-values.md) — bind a variable, read it back with a quality
- [ARCHITECTURE.md](ARCHITECTURE.md) — the five layers and why each boundary is where it is
- [extending.md](extending.md) — building Sparkplug, or a PLC wrapper, on this

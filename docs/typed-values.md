# Typed values

Point the library at a variable, say how much it has to move before anybody
cares, and read it back on the other side with a quality you can act on.

```bash
cmake -S . -B build
```

Strictly **above** the raw API: everything here calls `publish()` and
`subscribe()` like any application would, and nothing here reaches into the
client. Use both if you like.

## Do you need this, or does `MqttDevice` already do it?

`MqttDevice::report()` covers the common half of this without a build option:

```cpp
mqtt.report("plant/tank-01/level", &tankLevel, 0.02, 30000);   // deadband, heartbeat
if (mqtt.isFresh("plant/tank-01/level", 30000)) { /* safe to act on */ }
```

**Use that** for a plain numeric reading published as text, which is most of
them. **Come here** when you need any of:

| | |
| --- | --- |
| a **binary** encoding | two ends that have agreed, and want the bytes rather than the digits |
| more types | `int8`…`uint64`, and text values |
| a real **quality** | `Unknown` / `Bad` / `Uncertain` / `Good`, not just fresh-or-not |
| a **fallback on bad quality** | so a control path cannot act on a reading the library already distrusts |
| **per-topic liveness** | wired to a Will or a birth topic |
| **pulling** a value when you want it | `poller.number(filter, fallback)` reads on demand, in a scan cycle, with no handler at all — `MqttDevice` pushes to a handler when a message arrives |
| **liveness** from another topic | a tracked value goes `Bad` when the publisher's Will says it is offline, however fresh the number is |

The two are independent and can be used together on one client.

---

## Publishing a variable

```cpp
#include <ModBeeMqtt.h>     // the typed layer comes with it when the option is on

MqttTcpTransport transport;
MqttClient mqtt;
MqttTypedBindings bindings;

float pressure = 0.0f;
float readSensor();

int main() {
    transport.configure("broker.example.com", 1883);

    MqttConnectionConfig connection;
    connection.host = "broker.example.com";
    connection.clientId = "plc-01";
    mqtt.begin(transport, connection);
    mqtt.connect();

    bindings.begin(mqtt);
    bindings.bind("plant/p1/pressure", &pressure,
                  /*deadband=*/0.5,
                  /*maxIntervalMs=*/60000,
                  MQ_QOS_1);

    for (;;) {
        mqtt.poll();
        pressure = readSensor();
        bindings.poll();   // publishes when it moved, or when the interval is up
    }
}
```

That publishes when the pressure moves by half a bar, **and at least once a
minute regardless** — which is what a plant actually wants: a change-driven
stream that a subscriber can still tell apart from a dead sensor.

### Why the interval matters

Without it, a subscriber cannot distinguish a steady value from a publisher
that died an hour ago. With it, silence means something, and
[`MqttPoller`](#reading-a-variable) can turn that silence into a quality.

Pass the **same number** to the publisher's `bind()` and the subscriber's
`track()`. That is the contract between the two ends.

### Types

Fixed width only:

```
bool
int8_t  uint8_t  int16_t  uint16_t  int32_t  uint32_t  int64_t  uint64_t
float   double
```

Anything else fails to compile rather than binding the wrong number of bytes.

> **One hazard C++ cannot prevent.** `int` is 16 bits on AVR and 32 on ESP32,
> and on a machine where `int` and `int32_t` are the same type the compiler
> cannot tell them apart. Binding an `int` therefore puts different bytes on
> the wire depending on which board was compiled — **in binary encoding**. In
> text, which is the default, it does not, because text is width-independent.
> That is one of the reasons text is the default.

---

## Encoding, and why text is the default

MQTT carries bytes and says nothing about what they mean. At 5.0 two properties
can say — Payload Format Indicator and Content Type — and at 3.1.1 there is
nothing at all. That gap cannot be closed by inventing a format; it is closed
by the two ends agreeing, and the useful thing a library can do is make the
agreement explicit and default it to whatever everything else already reads.

**Text is the default, at both versions.** A decimal string is what a dashboard
or a historian takes without being configured, it is readable in a broker log,
and it is what the rest of a plant is already publishing.

```
Text     "21.5"   "1"   "-7"
Binary   fixed width, big-endian - network byte order, like every length in
         MQTT itself. Smaller, and only for two ends that have agreed.
```

**Text has a second advantage on this library's targets: it is
width-independent.** `int` is 16 bits on AVR and 32 on ESP32, so a binary
binding to an `int` puts different bytes on the wire depending on which board
was compiled. The same binding in text puts `"70"` on the wire from both.

At 5.0 the choice is **stated on the wire** as well as assumed:

| | Payload Format Indicator | Content Type |
| --- | --- | --- |
| `MqttEncoding::Text` | 1 | `text/plain` |
| `MqttEncoding::Binary` | 0 | `application/octet-stream` |

Pick binary per binding when both ends have agreed on it:

```cpp
bindings.bind("plant/p1/pressure", &pressure, 0.5, 60000,
              MQ_QOS_0, false, MqttEncoding::Binary);
```

## Floating point round-trips exactly

`float` is printed with `%.9g` and `double` with `%.17g`, which are the
shortest formats that round-trip each exactly. A reading printed with fewer
digits and read back is a **different number**, and a comparison against a
setpoint then depends on the formatting rather than on the process.

### A payload that is not the number it claims to be is refused

```
"21.5 degC"  ->  refused, not read as 21.5
""           ->  refused
"-1"         ->  refused into an unsigned, not wrapped to 18 quintillion
"70000"      ->  refused into an int16, not truncated to 4464
"true"       ->  accepted as a boolean, because other tools publish it
"yes"        ->  refused
```

Salvaging the first of those would be the library quietly discarding the part
it did not understand — and that part is where the disagreement between the two
ends is.

---

## A simpler version, when that is all you need

`MqttDevice::isFresh()` answers the same question with no typed layer at all:

```cpp
if (mqtt.isFresh("plant/tank-01/level", 30000)) { /* safe to act on */ }
```

It knows two things and does not pretend to know a third: a message arrived
within the interval **you** promised, and the link is up. What it does not give
you is a typed value, a deadband, or the three-way `Unknown`/`Bad`/`Good`
distinction below — which is the point of this layer.

---

## Reading a variable

```cpp
MqttPoller poller;
poller.begin(mqtt);
poller.track("plant/p1/pressure", MqttValueType::Float32,
             /*maxIntervalMs=*/60000, MQ_QOS_1);

const double reading = poller.number("plant/p1/pressure", /*fallback=*/-1.0);
```

`track()` subscribes for you. A tracked topic nobody subscribed to reads as
`Unknown` for ever, and works out to be the commonest way to spend an
afternoon.

`begin()` installs its own message handler and **chains** any handler you had
already installed, so tracked values and raw messages are not a choice.

### Quality

```
never received                 ->  Unknown
the broker is not connected    ->  Bad
maxInterval is 0               ->  Good        (nothing was promised)
age <= maxInterval             ->  Good
maxInterval < age <= 2x        ->  Uncertain
age > 2x                       ->  Bad
```

Judged against **the interval the caller asked for**, not a constant. A
hard-coded "stale after five seconds" is wrong for a temperature sampled hourly
and wrong for a pressure sampled ten times a second. The publisher said how
often it would speak; that is what its silence is measured against.

`number()` returns the fallback unless the quality is `Good`, so a control path
cannot act on a reading the library has already said it distrusts. Use
`read()` if you want the value and the quality together and will decide
yourself.

**`Unknown` and `Bad` stay distinct** because they send you to different
places: `Unknown` is a commissioning error — a topic nobody is publishing — and
`Bad` is a cable.

### One honest limit

A connected broker will happily serve a retained message from a device that
died last Tuesday, and **nothing in MQTT lets a subscriber tell that from a
live one**.

So this reports what it knows — when *we* last received something, and whether
*our* link is up — and it does not pretend to know whether the publisher is
alive. For that, give it the publisher's Will topic:

```cpp
poller.trackLiveness("plant/device/reading", "plant/device/status");
```

A payload of `""`, `"0"`, `"false"` or `"offline"` on that topic means the
publisher is gone, and the tracked value goes `Bad` however fresh it is. The
plant's own knowledge becomes part of the answer, which is the only way it can
be right.

### A payload that will not decode is counted, not hidden

```cpp
const MqttTracked* entry = poller.find("plant/p1/pressure");
entry->received;      // messages that decoded
entry->undecodable;   // messages that did not
```

The previous value is kept **with its previous timestamp**, so it goes stale on
schedule rather than appearing fresh because something unreadable arrived.

---

## One thing that catches people out

A binding publishes its first value the moment the link is up. A subscriber
that has not subscribed yet will not see it — which is correct MQTT, and is
also the first thing to catch somebody out.

In a plant the answer is `retain`, so a late subscriber gets the current value
at subscribe time:

```cpp
bindings.bind("plant/p1/pressure", &pressure, 0.5, 60000, MQ_QOS_1,
              /*retain=*/true);
```

There is also `publishAll()`, which publishes every binding immediately
whatever the deadband says — the right thing to call from an
`onSessionEstablished` hook, because a subscriber that has just arrived has no
idea what any value is and waiting for the next change could be hours.

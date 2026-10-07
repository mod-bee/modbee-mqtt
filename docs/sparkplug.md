# Sparkplug B

Sparkplug B is a standard for running industrial devices over MQTT: the topic
names, the payload format, and the rules for saying "I am here" and "I have
gone". This library has both ends of it, conformant to Sparkplug 3.0:

| | |
| --- | --- |
| **`MqttSparkplugNode`** | An **edge node**: a device or gateway that publishes metrics and accepts writes. On a board or a host |
| **`MqttSparkplugHost`** | A **host application**: the SCADA end, which watches nodes, keeps their metric lists, and sends writes |

Both are in `#include <ModBeeMqtt.h>`, and neither costs anything in a program
that does not construct one.

Examples: [`Sparkplug_Edge_Node`](../examples/arduino/Sparkplug_Edge_Node)
(ESP32), [`sparkplug_node`](../examples/cpp/sparkplug_node) and
[`sparkplug_host`](../examples/cpp/sparkplug_host) (a host, and a pair that
talk to each other).

---

## What Sparkplug adds to MQTT

Plain MQTT moves bytes on topics anyone can invent. Sparkplug fixes the rest:

- **Births.** When a node connects, it publishes a *birth certificate*
  (NBIRTH) listing every metric it has, with its datatype, units and current
  value. A SCADA system such as Ignition builds its tag list from that, so
  nobody types tag names in by hand.
- **Deaths.** The node registers a *death certificate* (NDEATH) as its MQTT
  Will. If it drops off the network, the broker publishes it, and every value
  from that node is marked stale at once rather than left looking live.
- **Sequence numbers.** Every message is numbered. A host that sees a gap
  asks the node to publish its birth again (a *rebirth*).
- **Aliases.** After the birth, metrics are sent by number rather than by
  name, which is most of Sparkplug's bandwidth saving. A host keeps the birth
  to turn the numbers back into names; `MqttSparkplugHost` does that for you.
- **Report by exception.** Data messages (NDATA) carry only what changed.
- **A primary host.** A node can be told to stay quiet until a named SCADA
  host says it is online, and to move to another broker if that host goes
  offline.

The topics are `spBv1.0/<group>/<message type>/<node>[/<device>]`, for example
`spBv1.0/Plant/NDATA/Tank-01`. The payload is Protocol Buffers; this library
encodes and decodes it itself, with no protobuf library.

---

## An edge node

```cpp
#include <WiFi.h>
#include <ModBeeMqtt.h>

WiFiClient net;
MqttSparkplugNode node(net);   // or EthernetClient, TinyGsmClient, WiFiClientSecure

void setup() {
    WiFi.begin("plant-wifi", "");
    while (WiFi.status() != WL_CONNECTED) delay(250);

    node.epochNow(1767225600000ULL);   // UTC milliseconds - see "The clock"

    node.group("Plant");
    node.node("Tank-01");
    node.server("192.168.1.10");

    node.declare("Tank/Level", MqttSparkplugDataType::Float, "m");
    node.declare("Pump/Running", MqttSparkplugDataType::Boolean);
    node.declareWritable("Tank/Setpoint", MqttSparkplugDataType::Float);

    node.connect();                    // returns at once
}

void loop() {
    node.poll();
    node.set("Tank/Level", mq_spFloat(analogRead(A0) * 0.001f));
    node.set("Pump/Running", mq_spBool(digitalRead(5)));
    node.publishChanged();             // one NDATA, with only what moved
}
```

On a host there is no network object to pass: `MqttSparkplugNode node;` uses
the computer's own sockets.

### Declare everything first

The birth must list **every metric the node will ever publish**, so each one
is declared before `connect()`. Writing to a name that was not declared is
refused, because a SCADA system discards metrics it was never told about.
`declare()` returns the metric's alias, or 0 if it was refused: a duplicate
name, an unsupported datatype, or a full table.

`set()` must be given a value of the declared type: `mq_spFloat()` for a
`Float`, `mq_spDouble()` for a `Double`, and so on. A mismatch is refused
rather than converted, because the birth told the SCADA system what type to
expect. The makers are `mq_spBool`, `mq_spInt8` to `mq_spInt64`, `mq_spUInt8`
to `mq_spUInt64`, `mq_spFloat`, `mq_spDouble`, `mq_spString`, `mq_spText`,
`mq_spDateTime`, `mq_spBytes` and `mq_spNull(type)`.

### Sending

- **`publishChanged()`** sends one NDATA with the metrics whose value moved
  since they were last sent. Call it on a timer (once a second in the
  examples) or every loop; with nothing changed it sends nothing.
- **`publishAll()`** sends every metric, changed or not.
- **`rebirth()`** publishes the birth again. A host asks for this with a
  rebirth command, and the node does it by itself.

### The clock

Every Sparkplug message carries a UTC timestamp, and the specification
requires it. On a host the system clock is used and nothing needs doing. On a
board, which may only know how long it has been running, give the node the
time once, in milliseconds since 1970, from NTP or an RTC:

```cpp
configTime(0, 0, "pool.ntp.org");                // ESP32 and ESP8266
while (time(nullptr) < 1700000000) delay(250);
node.epochNow((uint64_t)time(nullptr) * 1000);
```

The node adds its own uptime from then on. Without it, `node.clockIsSet()` is
false and its timestamps count from boot, which a strict host will reject.

### Writes from SCADA

A metric declared with `declareWritable()` can be written by a host (an NCMD).
A write to any other metric is refused and counted in `commandsRefused()`. Take
writes with a callback:

```cpp
void onCommand(const MqttSparkplugCommand& command) {
    if (strcmp(command.name, "Tank/Setpoint") == 0) {
        setpoint = (float)command.value.asNumber();
    }
}

node.onCommand(&onCommand);
```

or by polling, which suits a PLC scan:

```cpp
MqttSparkplugValue value;
if (node.take("Tank/Setpoint", value)) setpoint = (float)value.asNumber();
```

The written value is the metric's value from then on, and the next
`publishChanged()` reports it back, so the SCADA system sees the write take
effect.

A write the node refuses is not given to `onCommand()` - so an application
that acts on what its handler is given never acts on one - but it can be seen,
with the reason, through `onCommandRefused()`:

```cpp
void onRefused(const MqttSparkplugCommand& command) {
    // command.outcome: UnknownDevice, UnknownMetric, ReadOnly, WrongType,
    // Undecodable or NotARequest - mq_sparkplugCommandOutcomeName() names it
    log(command.name, mq_sparkplugCommandOutcomeName(command.outcome));
}

node.onCommandRefused(&onRefused);
```

Between the two handlers every metric of every NCMD and DCMD is reported
exactly once; `command.applied()` says which side it came from.

### Devices

A node can speak for devices behind it, each with its own metrics, its own
birth (DBIRTH) and death (DDEATH):

```cpp
node.addDevice("Pump-2");
node.declareFor("Pump-2", "Speed", MqttSparkplugDataType::Float);
node.declareWritableFor("Pump-2", "Enable", MqttSparkplugDataType::Boolean);
// after connect():
node.deviceOnline("Pump-2");                     // DBIRTH, once the node has birthed
node.setFor("Pump-2", "Speed", mq_spFloat(1450.0f));
node.deviceOffline("Pump-2");                    // DDEATH, when the device stops answering
```

### A primary host, and more than one broker

```cpp
node.primaryHost("scada-1");                     // wait for this host before birthing
node.server("broker-a.plant");
node.addServer("broker-b.plant");                // up to four
```

With a primary host set, the node connects but does not birth until host
`scada-1` says it is online. If that host goes offline, the node disconnects
and connects to the **next** broker in the list, as the specification
requires. Without a primary host, the node births as soon as it connects,
which is what a single-broker site wants.

`node.waitingFor()` says, in words, why a connected node has not birthed yet:
for example `no STATE from primary host 'scada-1'`.

### TLS

- **On a board**, TLS belongs to the network object: pass a
  `WiFiClientSecure` (with its CA certificate set) or an `SSLClient`, and give
  the server port 8883: `node.server("mqtt.example.com", 8883)`.
- **On a host**, before `connect()`:

```cpp
node.useTls();                                   // verify against the system's certificates
node.trustFile("/etc/plant/ca.pem");             // or against this CA
node.pinnedKey("23c3ca67edd89db88be43bc88bd9806588f1766bd272e9e701c659365eb3ca1a");
```

Each makes the default port 8883. `pinnedKey()` trusts exactly one broker
key, which is what a plant broker with a self-made certificate wants;
[security.md](security.md#pinning-when-nobody-signed-for-the-broker) explains it and how to read the key off the
broker.

### Knowing what is going on

| | |
| --- | --- |
| `isConnected()` | The MQTT link is up |
| `isBorn()` | The birth has gone: SCADA can see the node |
| `waitingFor()` | Why a connected node has not birthed, or `""` |
| `lastErrorText()` | Why the link is down |
| `sequence()`, `birthDeathSequence()` | The current `seq` and `bdSeq` |
| `commandsApplied()`, `commandsRefused()` | Writes taken, and writes refused |
| `client()` | The `MqttClient` underneath, for anything else |

---

## A host application

```cpp
#include <ModBeeMqtt.h>

MqttSparkplugHost host;

void onMessage(const MqttSparkplugInbound& message) {
    // message.type: NBIRTH, NDATA, NDEATH, DBIRTH, ...
    // message.payload->metrics[i].name is filled in, even when the node sent an alias
}

int main() {
    host.hostId("scada-1");
    host.server("192.168.1.10");
    host.watch();                      // every group; or host.watch("Plant")
    host.onMessage(&onMessage);
    host.connect();

    for (;;) {
        host.poll();

        MqttSparkplugValue level;
        if (host.read("Plant", "Tank-01", "Tank/Level", level)) {
            // the latest value, by name
        }
    }
}
```

What the host does for you:

- **Announces itself** on `spBv1.0/STATE/<host id>`, with a retained Will
  that says offline, so nodes waiting for it as their primary host start and
  stop with it.
- **Keeps every node's metric list** from its birth, so data sent by alias
  arrives with names, and `read()` and `readDevice()` return the latest value
  of any metric by name. `findNode()` gives the whole picture for one node:
  online or not, its metrics, their types and the time of each value.
- **Checks sequence numbers.** With `actAsPrimary()`, a gap that does not fill
  within `reorderTimeout()` (5 seconds by default) makes it ask the node for a
  rebirth.

To write to a node:

```cpp
host.command("Plant", "Tank-01", "Tank/Setpoint", mq_spFloat(3.5f));
host.commandDevice("Plant", "Tank-01", "Pump-2", "Enable", mq_spBool(true));
host.requestRebirth("Plant", "Tank-01");
```

The value must be the metric's declared type; the birth says what that is
(`host.findNode(...)->find(name)->type`), and the
[`sparkplug_host`](../examples/cpp/sparkplug_host) example converts with it.

**Each host needs its own host id.** It is also the MQTT client id, so two
hosts sharing one keep taking over each other's connection.

On a host computer `MqttSparkplugHost host;` uses the system's sockets, and
`useTls()`, `trustFile()` and `pinnedKey()` work as for the node. On a board,
pass the network object as for the node.

---

## What is supported

**Datatypes:** `Int8` to `Int64`, `UInt8` to `UInt64`, `Float`, `Double`,
`Boolean`, `String`, `Text`, `UUID`, `DateTime`, `Bytes`, `File`, and metric
properties with scalar values (engineering units and quality live there).

**Not supported yet:** `DataSet`, `Template`, `PropertySetList` and the
array types. They are part of Sparkplug 3.0; this library does not carry
their values yet.

- **A node cannot declare one** - `declare()` refuses it.
- **A host still sees a node that sends one** (an Ignition UDT is a
  Template). The rest of the message is read as usual, and the metric itself
  is kept with its name, alias and datatype, `valueUnsupported` set and its
  value NULL - never a zero. The host's dictionary lists it; `read()` finds no
  value for it.

**Data without datatypes.** The specification says data and command
messages SHOULD NOT carry a metric's datatype, because the birth declared it.
This library's node includes it anyway (which is allowed), and both ends
accept messages without it: the host applies the type from the node's birth,
and the node the type it declared. A value that arrives in a field that
cannot hold that type is NULL rather than guessed.

**Limits**, fixed at build time so nothing is allocated while running:

| Macro | Default | What it bounds |
| --- | --- | --- |
| `MQ_SP_MAX_METRICS` | 34 on a board, 256 on a host | Metrics per node (all its devices included), and per message. A message with more cannot be read, so a host - which may watch nodes with hundreds of metrics - has the larger default |
| `MQ_SP_MAX_DEVICES` | 8 | Devices under one node |
| `MQ_SP_MAX_SERVERS` | 4 | Brokers a node can fail over through |
| `MQ_SP_MAX_PROPERTIES` | 4 | Properties per metric |
| `MQ_SP_MAX_TRACKED_NODES` | 16 | Nodes one host keeps track of, each with up to `MQ_SP_MAX_METRICS` metrics |

To change one, set it for the **whole build**, not with a `#define` in the
sketch: the library's own files are compiled separately and would not see it.
That means `-DMQ_SP_MAX_METRICS=64` in CMake, PlatformIO's `build_flags`, or
the Arduino IDE's `compiler.cpp.extra_flags` in `platform.local.txt`.

**What it costs on an ESP32:** the `Sparkplug_Edge_Node` example is about
30 KB of flash and 21 KB of RAM larger than `First_Sketch`. The RAM is the
node's fixed tables: its metrics, two payload buffers and a 4 KB send buffer.

---

## Sparkplug and the Kafka pipe

To Kafka, Sparkplug is MQTT like any other: the [pipe](kafka.md) passes the
topic and the protobuf bytes through untouched, in both directions. One
setting matters: key the records by **group and node**, so all of one node's
messages land in one Kafka partition and stay in order:

```yaml
    - id: sparkplug
      mqtt-topic-filters: ["spBv1.0/#"]
      kafka-topic: mqtt-sparkplug
      key: "${MQTT_TOPIC_LEVEL_1}/${MQTT_TOPIC_LEVEL_3}"
```

---

## How it is tested

Three independent checks, because tests written from the same reading of the
specification as the code would share its mistakes:

- **The Eclipse Sparkplug TCK**, the specification project's own test suite.
- **Other implementations' codecs:** Eclipse Tahu (Python) and the npm
  `sparkplug-payload` decode what this library encodes, and the reverse.
- **A live node and host**, on a real broker, judged by an independent Python
  host application.

Every requirement in the code names the statement of the specification it
implements, by its `tck-id`. [extending.md](extending.md#sparkplug-b) has the
design history.

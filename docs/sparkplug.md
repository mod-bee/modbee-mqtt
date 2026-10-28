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

**On a server**, `modbee-mqtt serve` runs the same host and edge node for an
application that has no Sparkplug code of its own, through the
[Application API](app-api.md#sparkplug-b): a host (`Sparkplug.PutHost`,
`modbee-mqtt sparkplug-hosts put`) or an edge node (`Sparkplug.PutEdgeNode`,
`modbee-mqtt sparkplug-nodes put`) on one of its connections - to another
broker, to the server itself, or [over Kafka](kafka.md#a-sparkplug-host-on-kafka-its-state-on-mqtt) -
payloads decoded and encoded for it, and Sparkplug Aware
(`serve --sparkplug-aware`). `modbee-mqtt sub -t 'spBv1.0/#' --sparkplug`
prints what goes by, decoded ([cli.md](cli.md#subscribing)).

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
  since they were last sent, and one DDATA for each online device that has
  some. Call it on a timer (once a second in the examples) or every loop;
  with nothing changed it sends nothing. It looks only at the metrics that
  were marked - by `set()`/`setAt()` changing a value, a host's write, or
  `reportAt()` - and a metric stays marked while it differs from what was
  last sent (inside its deadband, or held), so a call costs what moved, not
  what is declared. A text value rewritten in place in the same buffer is
  seen when it is `set()` again, as the OpenPLC blocks do every scan.
- **`publishAtPoll()`** asks for one `publishChanged()` at the next `poll()`.
  For a program that sets values in many places - the OpenPLC blocks: each
  asks, and the node sends one NDATA (and one DDATA a device) per poll for
  all of them. `pollPublishes()` counts those made and `pollPublishSent()`
  says whether the last one went; a caller keeps the count from when it
  asked to know its message has been made.
- **A deadband** decides what "moved" means for a noisy number:

  ```cpp
  node.deadband("Tank/Level", 0.01);          // ignore moves of 10 mm or less
  node.deadband("Line/Pressure", 0, 2.0);     // ignore moves of 2 % or less
  node.deadbandFor("Pump-2", "Speed", 5.0);   // a device's metric
  ```

  A move is measured from the value **last published**, not the last one set,
  so a slow drift still adds up and goes. A value a host **wrote** is always
  reported back, deadband or not - the host watches for that echo to know the
  write took. With both numbers set, a move must
  clear both. Only numeric metrics take one (a Boolean is refused - a
  deadband would freeze the bit), and a birth or `publishAll()` always
  carries every value as it is.
- **`publishAll()`** sends every metric, changed or not.
- **`rebirth()`** publishes the birth again. A host asks for this with a
  rebirth command, and the node does it by itself.

### The clock

Every Sparkplug message carries a UTC timestamp, and the specification
requires it. On a host the system clock is used and nothing needs doing.

On an ESP32, ESP8266 or RP2040 the board's own system clock is used as soon as
something has set it - so starting SNTP is enough, and the node needs no call:

```cpp
configTime(0, 0, "pool.ntp.org");                // ESP32 and ESP8266
```

A modem's network time, or an RTC read at start-up, sets it the same way:
`mq_setEpochMillis(ms)` (or `settimeofday()`). Until the clock reads a date
after 2024 it is treated as not set.

On any other board, give the node the time once, in milliseconds since 1970:

```cpp
node.epochNow(rtcMillisSince1970);
```

The node adds its own uptime from then on. Without a clock,
`node.clockIsSet()` is false and the timestamps count from boot - a date in
January 1970, which a host should read as "not synced" and replace with the
time it received the message.

### Units and ranges

The birth can say what a number means: `engUnit`, and the `engLow`/`engHigh`
range a SCADA gauge or trend should show (the property names Eclipse Tahu and
Ignition use).

```cpp
node.declare("Tank/Level", MqttSparkplugDataType::Float, "m");   // units at declaration
const int level = node.indexOf(nullptr, "Tank/Level");
node.unitsAt(level, "m");
node.rangeAt(level, 0.0, 4.5);
```

They travel only in the birth, so a change shows at the next one.

### Many metrics from one block

A wrapper binding a whole structure looks each metric up once and keeps its
index - `indexOf(device, name)` - then works by index every scan: `setAt()`,
`valueAt()`, `changePendingAt()`, `commandPendingAt()`, `takeAt()`,
`deadbandAt()`. The
index is stable for the life of the node; every call refuses one past
`pointCount()`. `setAt()` answers whether the value changed (false for the
same value again, and for a refusal). Names are found through a hash index
built as metrics are declared, so declaring a thousand costs a thousand
lookups, not half a million compares.

Two more by index, for a set sent on its own clock:

```cpp
node.holdAt(elapsed, true);       // only when asked: not with every other report
node.reportAt(elapsed);           // ask: it goes in the next publishChanged()
```

A **held** metric is left out of `publishChanged()` unless `reportAt()` marked
it - for a set sent on its own clock, every period or on request. Births and
`publishAll()` carry it as it is. `isHeldAt()` says whether one is held.

**What a metric is, and what it holds.** `pointAt(index)` (and `findPoint()`)
answer a metric's `MqttSparkplugPoint`: its name, type, alias, device, units,
range, deadband and write queue. Its value is not there - read it with
`valueAt(index)`. The value, the one last sent and the marks that decide
whether it goes are a separate table of 32 bytes a metric, one cache line on
an ESP32, so a scan that sets a thousand values reads a thousand lines and
not five thousand.

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

// A device's metric (a DCMD) - a name is only unique within its device:
if (node.takeFor("Pump-2", "Enable", value)) enable = value.as.boolean;
```

The written value is the metric's value from then on, and the next
`publishChanged()` reports it back, so the SCADA system sees the write take
effect.

By default a write replaces the one before it, so a host writing 1 and then 0
faster than the application polls is seen to write 0 only. A PLC wants every
write, in order:

```cpp
node.queueWrites(4);            // before the first write; 1..16 per metric
// each take() / takeAt() now hands over the OLDEST write, one at a time;
// peekAt() looks at it without taking it
```

Full, a metric's queue drops its oldest write and keeps the newest, and
counts the drop (`writesLost()`, `writesLostAt(index)`). A queued write
becomes the metric's value when it is taken, and is reported back then. Text
is copied, so a queued STRING survives the packets that follow it.

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

### On a connection you already have

A device rarely has only Sparkplug to say: its own topics and its firmware
updates want the same broker session. Give the node the client instead of a
network, and it becomes a layer on that connection (see "Layers sharing one
client" in api.md):

```cpp
MqttSparkplugNode node(client);    // client: begun, connected and polled by its owner
node.group("Plant1");
node.node("pump-01");
node.declare("level", MqttSparkplugDataType::Float);
node.connect();                    // starts the node on the shared session

// every pass, after client.poll():
node.poll();
```

The owner keeps the connection: `server()`, `credentials()`, `keepAlive()`,
`only()` and the TLS calls do nothing here, and `node.poll()` does not poll the
client. What Sparkplug needs of the session the node takes on every CONNECT:
the Will (its NDEATH - claimed, and refused while another layer owns it),
Clean Start, no session expiry, and a transmit buffer that holds the birth
certificate. Started on a session that is already up, it restarts the client
once so the next CONNECT carries the NDEATH; a primary host going offline
restarts it too. `disconnect()` publishes the NDEATH and leaves the connection
to everything else on it. `MqttSparkplugHost` takes a client the same way.

### A primary host, and more than one broker

```cpp
node.primaryHost("scada-1");                     // wait for this host before birthing
node.server("broker-a.plant");
node.addServer("broker-b.plant");                // up to four
```

With a primary host set, the node connects but does not birth until host
`scada-1` says it is online. If that host goes offline, the node disconnects
and connects to the **next** broker in the list, as the specification
requires. If the host is replaced and the node sees the new session's online
(a newer timestamp) without the old one's offline, it treats the missed
offline as valid: NDEATH, reconnect to the same broker, and a new NBIRTH for
the new host. Without a primary host, the node births as soon as it connects,
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

### Changing settings while running

`server()`, `credentials()` / `clearCredentials()`, `group()`, `node()`,
`primaryHost()`, `only()` and `keepAlive()` may be called again after a
`disconnect()`; the next `connect()` uses them and births with the next bdSeq.
`rebind(MqttTransport&)` moves the node to another transport you built - the
declarations, aliases, devices and write queues stay. With 5.0 the
`disconnect()` before it makes the broker publish the NDEATH.

### Arrays, structs and tables

A PLC's `ARRAY OF REAL` is a `FloatArray` metric, a `STRUCT` or UDT is a
`Template`, and a table is a `DataSet`. Each value travels as its own wire
bytes, so a program that uses none of them pays for none of it.

**An array** is packed from your own C array, little-endian whatever the
board, as the specification packs it:

```cpp
float curve[3] = {1.0f, 2.0f, 3.0f};
uint8_t packed[12];
node.declare("curve", MqttSparkplugDataType::FloatArray);
mq_spPackArray(MqttSparkplugDataType::FloatArray, curve, 3, packed, sizeof packed);
node.set("curve", mq_spArray(MqttSparkplugDataType::FloatArray, packed, 12));
```

The element types follow C: `int16_t` for an `Int16Array`, `bool` for a
`BooleanArray`, `const char*` for a `StringArray`, `uint64_t` milliseconds for
a `DateTimeArray`. With a null buffer `mq_spPackArray()` returns the size it
needs. Reading one back: `mq_spArrayCount(value, count)` and
`mq_spArrayAt(value, index, element)`, which also say when the bytes are not a
well-formed array.

**A struct** is a Template. Declare its *definition* once - the members with
their datatypes and starting values - and every metric of that type is an
*instance*. The definition goes in every NBIRTH; an instance must name it.

```cpp
// The type: a Pump has running and hours.
MqttSparkplugTemplate pump;
pump.isDefinition = true;
MqttSparkplugMetric running;  running.name = "running";  running.value = mq_spBool(false);
MqttSparkplugMetric hours;    hours.name = "hours";      hours.value = mq_spDouble(0);
pump.addMember(running);
pump.addMember(hours);
MqttSparkplugError error;
uint8_t definition[128];
size_t length = mq_sparkplugEncodeTemplate(definition, sizeof definition, pump, error);
node.declareTemplate("Pump", definition, length);

// A metric of that type, and a value for it.
node.declare("pump1", MqttSparkplugDataType::Template);
MqttSparkplugTemplate value;
value.templateRef = "Pump";            // an instance of Pump
hours.value = mq_spDouble(12.5);
value.addMember(hours);                // in data, only what changed
uint8_t bytes[128];
length = mq_sparkplugEncodeTemplate(bytes, sizeof bytes, value, error);
node.set("pump1", mq_spTemplate(bytes, (uint32_t)length));
```

A value whose `templateRef` names no definition this node declared is refused
(`lastPayloadError()` says `TemplateReference`), and so is a host's write of
one. A member can itself be a Template - a struct in a struct - an array or a
DataSet. Parameters go in `addParameter()`, as name, type and value.
`mq_sparkplugDecodeTemplate()` reads one back.

**A table** is a DataSet: named, typed columns (the integers, `Float`,
`Double`, `Boolean`, `String`, `DateTime`, `Text`) and rows with a cell for
each - `mq_sparkplugEncodeDataSet()` to write one from your cells,
`MqttSparkplugDataSetReader` to read one where it lies.

The node keeps its own copy of what it last published, as long as the value
is, so a value rewritten in place - a PLC writing into the same array - is
still seen to change. (On a board, a text value used to be cut to 126
characters; it is not any more.)

### Records with their own time, and store-and-forward

```cpp
struct MqttSparkplugRecord { size_t index; MqttSparkplugValue value; uint64_t timestamp; };
bool publishRecords(const MqttSparkplugRecord* records, size_t count, bool historical);
uint64_t timestamp() const;   // the stamp the node would use now
```

`publishRecords()` sends values outside report by exception, each metric with
its own timestamp, in one NDATA (or DDATA: all records belong to one device or
all to the node). With `historical` every metric carries `is_historical` - the
specification's store-and-forward flag: "it may be desirable to send metrics
after they were acquired ... for batching, store and forward, or sending local
backup data during network communication losses" - so the host files them at
their own time and does not update the live tags. Without it each value
becomes the metric's live value and its last published one, so report by
exception does not send it twice. QoS 0 like every data message: keep what
must not be lost until you have seen the node stay born afterwards.

`MqttSparkplugHistory` is the bounded ring that goes with it - **in RAM**:
a reboot or a power cut empties it; a program that must keep it across one
keeps it itself (on OpenPLC, in retained variables). `begin(depth)`
allocates `depth` events of 24 bytes with `mq_allocLarge()` (PSRAM first),
`add()` drops the oldest when full and counts it in `lost()`, `peek()` /
`drop()` take them oldest first, and `placeOnClock(utcNow, uptimeNow)` moves
events recorded before the clock was set onto UTC. The OpenPLC block
`SPARKPLUG_HISTORY` is built on the two.

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

**Datatypes: all of Sparkplug B 3.0's.** `Int8` to `Int64`, `UInt8` to
`UInt64`, `Float`, `Double`, `Boolean`, `String`, `Text`, `UUID`, `DateTime`,
`Bytes`, `File`, the thirteen array types, `DataSet` and `Template` (see
[Arrays, structs and tables](#arrays-structs-and-tables)); metric properties,
and a property whose value is a `PropertySet` or a `PropertySetList`
(`mq_spPropertySet()`, `mq_sparkplugDecodePropertySet()`).

- **A host keeps what it reads.** `read()` of a String, an array or a
  Template gives the host's own copy of the last value - not bytes in its
  receive buffer, which the next message reuses. An Ignition UDT arrives as a
  Template, its bytes readable with `mq_sparkplugDecodeTemplate()`.
- Only a datatype no metric can carry (`Unknown`, or a property's
  `PropertySet`) is kept without a value: `valueUnsupported` set and NULL,
  never a zero.

**Data without datatypes.** The specification says data and command
messages SHOULD NOT carry a metric's datatype, because the birth declared it.
This library's node includes it anyway (which is allowed), and both ends
accept messages without it: the host applies the type from the node's birth,
and the node the type it declared. A value that arrives in a field that
cannot hold that type is NULL rather than guessed.

**Limits.** A node's tables grow with what it declares, and its payloads and
send buffer are sized to its own birth certificate when it first connects -
so nothing is allocated in the steady state, and a node with five metrics pays
for five. On an ESP32 with PSRAM all of it is in PSRAM.

**No fixed number of metrics or devices**, on a host or a board: a node
declares as many metrics, under as many devices, as memory holds, and a
declaration memory cannot take is refused and `node.outOfMemory()` says so.
`node.metricLimit(n)` sets a limit of your own. A host likewise tracks as many
nodes, and metrics per node, as memory holds, and its decode payload grows to
the largest birth it meets (`host.outOfMemory()` when it could not).

| Macro | Default | What it bounds |
| --- | --- | --- |
| `MQ_SP_MAX_METRICS` | 34 on a board, 256 on a host | The size of a payload nobody sized, and where a host's decode payload starts before it grows |
| `MQ_SP_MAX_SERVERS` | 4 | Brokers a node can fail over through |
| `MQ_SP_MAX_PROPERTIES` | 4 | Properties per metric |

To change one, set it for the **whole build**, not with a `#define` in the
sketch: the library's own files are compiled separately and would not see it.
That means `-DMQ_SP_MAX_METRICS=64` in CMake, PlatformIO's `build_flags`, or
the Arduino IDE's `compiler.cpp.extra_flags` in `platform.local.txt`. An
OpenPLC project has no build flags of its own: there a define comes from the
board package.

**What it costs on an ESP32:** the node's tables are allocated as metrics are
declared, so its static RAM is a few hundred bytes. At run time a metric costs
320 bytes in the registry (288 for what it is, 32 for its value and state), a
text metric 280 more for the text it owns, and a writable one 80 for its write
queue (624 for a text metric) - all allocated with `mq_allocLarge()`, so in
PSRAM where the board has it. The outbound payload, the encode buffer and the
client's send buffer are sized to the birth certificate on top.

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

- **The Eclipse Sparkplug TCK**, the specification project's own test suite -
  its node under test declares a Template definition with a parameter, an
  instance and an array, so the template statements are graded too.
- **Other implementations' codecs:** Eclipse Tahu (Python) and the npm
  `sparkplug-payload` decode what this library encodes, and the reverse.
  Arrays, Templates, DataSets and nested PropertySets match Tahu byte for
  byte. `sparkplug-payload` 1.0.3 has no array datatypes and refuses a
  negative `Int32` in a DataSet cell - one more reason to decode through the
  server's API rather than in an application.
- **A live node and host**, on a real broker, judged by an independent Python
  host application.

Every requirement in the code names the statement of the specification it
implements, by its `tck-id`. [extending.md](extending.md#sparkplug-b) has the
design history.

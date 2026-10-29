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
(`serve --sparkplug-aware`). The server's edge node takes a quality with each
value, properties in its births, devices added, changed and removed in the
same session (`Sparkplug.PutDevice`, `DeleteDevice`), and keeps what it
cannot send within limits of count, bytes and age, sent at a set rate with
events first; its host reports every birth's properties and each metric's
latest quality ([app-api.md](app-api.md#sparkplug-b)).
`modbee-mqtt sub -t 'spBv1.0/#' --sparkplug` prints what goes by, decoded
([cli.md](cli.md#subscribing)).

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
- **Report by exception.** Data messages (NDATA) carry only what changed,
  each metric with its timestamp (the specification requires one on every
  metric of a birth or data message: about 7 bytes each).
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

### Quality, and other properties

A metric's **quality** says whether to trust its value: `MQ_SP_QUALITY_GOOD`
(192), `_BAD` (0) or `_STALE` (500), the three codes Sparkplug allows
([tck-id-payloads-propertyset-quality-value-value]; any other is refused).
A read that failed sets it Bad and the device stays born:

```cpp
node.qualityAt(level, MQ_SP_QUALITY_BAD);    // the PLC answered with an error
node.publishChanged();                       // an NDATA: the value as it was, Quality 0
```

Every birth carries each metric's quality. A change of quality puts the
metric in the next data message whatever its deadband says, and every data
message carries a `Quality` property while it is not Good - so a host that
reads no `Quality` as Good and a host that keeps the last one it read agree.
`MqttSparkplugRecord::quality` does the same for `publishRecords()`, and the
replay window keeps each value's.

**Other properties** - a description, a display format, read-only, a key of
your own, of any type, PropertySets too - go in the birth as an encoded
PropertySet (`mq_sparkplugEncodePropertySet`), copied:

```cpp
MqttSparkplugProperty more[2] = {mq_spDescription("Raw water tank level"), mq_spEngUnit("m")};
uint8_t encoded[128];
MqttSparkplugError error;
const size_t length = mq_sparkplugEncodePropertySet(encoded, sizeof encoded, more, 2, error);
node.propertiesAt(level, encoded, length);
```

They take one of a metric's four property slots (`MQ_SP_MAX_PROPERTIES`),
after engUnit, Quality and the range: with units and a range as well there is
no room, so put engUnit, engLow and engHigh in the encoded set too, as the
server's edge node does.

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
    // TooLarge, Undecodable or NotARequest - mq_sparkplugCommandOutcomeName()
    // names it
    log(command.name, mq_sparkplugCommandOutcomeName(command.outcome));
}

node.onCommandRefused(&onRefused);
```

Between the two handlers every metric of every NCMD and DCMD is reported
exactly once; `command.applied()` says which side it came from.

**A write is no larger than `commandLimit()`** - 4 kB on a board, 1 MB on a
host (`MQ_SP_COMMAND_LIMIT`) - for a text, bytes, array, DataSet or Template
metric. A longer one is refused as `TooLarge` before anything is copied: a
command's value is copied into the metric (and into each write-queue slot), so
without a limit anybody who can publish on NCMD could make a board allocate
whatever they sent. `node.commandLimit(bytes)` moves the line.

**Rebirth requests are held to one birth every `rebirthHoldoff()`** - 2
seconds by default (`MQ_SP_REBIRTH_HOLDOFF`). A request after a quiet spell
is answered at once. One that comes within the holdoff of the last birth stops
data at once, as the specification requires, and is answered by a single
birth when the holdoff is up - however many requests came in between
(`rebirthsHeld()` counts them). A host, a second host, or anybody who can
publish on NCMD can no longer make the node send its largest messages back to
back. `rebirthHoldoff(0)` answers every request with its own birth;
`rebirth()`, called by the application, is never held.

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

### Changing what it declares while it runs

A gateway adds a tag, a PLC connection comes and goes: the session stays.
The Sparkplug way to say a declaration changed is a device's DDEATH and a new
DBIRTH, or for the node's own metrics a rebirth - the same bdSeq, the same
connection, nothing else told the node died:

```cpp
node.deviceOffline("Pump-2");                    // DDEATH: the old declaration is over
node.removeMetrics("Pump-2");                    // the device stays, its metrics go
node.declareFor("Pump-2", "Speed", MqttSparkplugDataType::Double);
node.deviceOnline("Pump-2");                     // DBIRTH, with what it is now

node.removeDevice("Pump-3");                     // its DDEATH, and gone

node.removeMetrics(nullptr);                     // the node's own
node.removeTemplates();                          // ... and its Template definitions
node.declare("Mode", MqttSparkplugDataType::String);
node.rebirth();                                  // NBIRTH and every DBIRTH, the same bdSeq
```

A metric declared again takes a new alias from the node's one counter -
never one an old declaration used, so no host can read the old meaning
([tck-id-payloads-alias-uniqueness]). Indices after a removed metric move
down: look them up again with `indexOf()`. What the replay window held of a
removed metric is never sent. (A block that keeps indices, as the OpenPLC
blocks do, never removes.)

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

### A birth larger than the broker takes

At MQTT 5.0 the broker says how large a message it accepts. An NBIRTH (or
DBIRTH) larger than that is not sent, and the node stays unborn:
`lastPayloadError()` is `Overflow` and `tooLargeForServer()` counts it. The
birth grows with what is declared - about 65 bytes a metric, so a thousand
metrics is about 67 kB, past `modbee-mqtt serve`'s default `--max-packet` of
64 kB. Raise the broker's limit, or split the metrics across devices (each
DBIRTH is its own message). At 3.1.1 the broker cannot say, and a too-large
birth is dropped by it instead.

### Knowing what is going on

| | |
| --- | --- |
| `isConnected()` | The MQTT link is up |
| `isBorn()` | The birth has gone: SCADA can see the node |
| `waitingFor()` | Why a connected node has not birthed, or `""` |
| `lastErrorText()` | Why the link is down |
| `sequence()`, `birthDeathSequence()` | The current `seq` and `bdSeq` |
| `resumeBirthDeathSequence(last)` | Before `connect()`: the bdSeq the last session used, kept across a reboot (OpenPLC RETAIN memory, a file) - the next CONNECT uses the one after, where a node started again began at zero and two sessions in a row had the same number. Save `birthDeathSequence()` whenever it changes |
| `commandsApplied()`, `commandsRefused()` | Writes taken, and writes refused |
| `birthCount()` | NBIRTHs and DBIRTHs published |
| `rebirthsHeld()` | Rebirth requests answered by a birth already on its way |
| `replayHeld()`, `replayed()`, `replayLost()` | The replay window: values held, sent again, and not kept |
| `client()` | The `MqttClient` underneath, for anything else |

### A link that dies without saying so: the replay window

A cellular link can die with neither end told. Until the keepalive notices -
up to one and a half keepalive intervals - the node is born, and every NDATA it
writes goes into a socket that leads nowhere. Data is QoS 0, so nothing says it
was lost, and the NBIRTH after the reconnect carries only current values. Over
seven simulated hours of cellular faults (`test_sparkplug_soak`) that was 6,772
values of 240,704, gone without a trace.

```cpp
node.replayWindow(64);          // before connect(): 64 values, 1.5 kB
node.replayWindow(64, 600000);  // ... and nothing older than ten minutes
```

With a window the node keeps the last values it published until it has
**proof** the broker has them. Every so often while values are held (half the
keepalive by default, `replayProbe(ms)` to change it) it sends one UNSUBSCRIBE
for a filter it never subscribed to; the broker's UNSUBACK comes back on the
same connection after everything written before it, so every value published
before the probe has arrived, and is let go. If the session then ends without
the node meaning it to - the link lost, not `disconnect()` or a host's
offline STATE - whatever is still held is published again after the next
birth: NBIRTH, each device's DBIRTH, then the held values, oldest first, each
marked `is_historical` with the time it was first sent, and only then the
live data that waited behind them. A host files them in its history and does
not let them overwrite the live picture (this library's host does exactly
that). Some may have arrived the first time: a historian then sees the same
value at the same timestamp twice, which is a duplicate, not a lie.

It is the in-flight half of what Cirrus Link's MQTT Transmission calls store
and forward. The outage half - values recorded while the node cannot publish
at all - is `MqttSparkplugHistory` with `publishRecords()`, below.

**Off by default**, on a host and a board, because it changes what goes on the
wire after a reconnect. 24 bytes a value, allocated once with
`mq_allocLarge()` (PSRAM first). Numbers, booleans and dates only: a text or
other span value is not kept, and `replayLost()` counts it, as it counts a
value pushed out of a full window, one too old to send, and one for a device
that was taken offline. Each probe is about 40 bytes up and 5 down, at most
once per probe interval and only while values are held.


### Changing settings while running

`server()`, `credentials()` / `clearCredentials()`, `group()`, `node()`,
`primaryHost()`, `only()` and `keepAlive()` may be called again after a
`disconnect()`; the next `connect()` uses them and births with the next bdSeq.
`rebind(MqttTransport&)` moves the node to another transport you built - the
declarations, aliases, devices and write queues stay. `disconnect()`
publishes the NDEATH and waits for the broker's acknowledgement before the
DISCONNECT; with 5.0 the DISCONNECT also asks the broker to publish the Will,
so a host sees the same death twice (one bdSeq) and treats it as one.

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
struct MqttSparkplugRecord { size_t index; MqttSparkplugValue value; uint64_t timestamp; int32_t quality; };
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

**Keeping it across a restart** is the application's, and the ring gives it
a way out and a way back in:

```cpp
// Before power goes (or on every change, where the store is cheap):
MqttSparkplugEvent held[64];
const size_t count = history.save(held, 64);           // oldest first
for (size_t i = 0; i < count; i++) {
    mq_spEventWrite(record[i], held[i]);               // 24 bytes, fixed layout
}
store(record, count, history.lost());

// After the restart:
history.begin(64);
for (size_t i = 0; i < count; i++) mq_spEventRead(record[i], held[i]);
history.restore(held, count, lostBefore);
```

`mq_spEventWrite()` gives an event a fixed, versioned little-endian layout
(`MQ_SP_EVENT_BYTES`, 24), so a ring saved by one firmware reads back in
another whose struct is laid out differently; `mq_spEventRead()` refuses
bytes of any other layout. `restore()` takes the events as `add()` would and
carries the lost count over. An event still stamped with uptime
(`MQ_SP_EVENT_UPTIME`) is on the previous boot's counter, which no clock can
place, so `restore()` drops it and counts it lost rather than file it at a
time that never happened - call `placeOnClock()` before saving, once the clock
is set, to keep such events. On OpenPLC the record array is a `VAR RETAIN`
array of bytes; elsewhere, whatever the board keeps across a restart.

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
- **Checks sequence numbers.** A message that arrives late closes the gap it
  opened, and a duplicate is recognised as one rather than counted as a gap of
  255. With `actAsPrimary()`, a gap that does not fill within
  `reorderTimeout()` (5 seconds by default) makes it ask the node for a
  rebirth - and so does a payload it cannot decode from a node it tracks, and
  data from a node whose birth it never saw (a host started after the node, as
  Ignition does).
- **Asks again after its own outage.** When its session returns (a broker
  restart, a lost link), a primary host asks every node it knows to birth
  again: what happened while it was away is not known.
- **Asks once, not on every message.** Those requests are held to one per node
  every `rebirthHoldoff()` (5 seconds by default; `rebirthsHeld` counts the
  rest), so a lossy link gets a rebirth, not a storm of them - each answer is
  the node's largest message, on the link that is already losing messages.
  `requestRebirth()`, the application's own call, is never held.
- **Marks what died.** An NDEATH (whose bdSeq matches) makes the node
  offline (`offlineAt`, the host's UTC time) and every metric of it and its
  devices `stale` (`staleAt`); a DDEATH does it to the device's metrics, at
  the DDEATH's timestamp, and `deviceOnline(device)` turns false. The last
  values stay readable; only a new birth makes them current again.
- **Keeps history out of the live picture.** A metric marked `is_historical`
  reaches `onMessage()` but does not change what `read()` returns: an hour of
  replayed history must not overwrite the reading from a minute ago.

To write to a node:

```cpp
host.command("Plant", "Tank-01", "Tank/Setpoint", mq_spFloat(3.5f));
host.commandDevice("Plant", "Tank-01", "Pump-2", "Enable", mq_spBool(true));
host.requestRebirth("Plant", "Tank-01");
```

The value must be the metric's declared type; the birth says what that is
(`host.findNode(...)->find(name)->type`), and the
[`sparkplug_host`](../examples/cpp/sparkplug_host) example converts with it.

**Large births.** A host receives messages of up to 4 MB (`MQ_SP_HOST_PACKET`;
`host.maximumPacket(bytes)` before `connect()`), and tells the broker so; the
buffer starts at 4 kB and grows only for a message that needs it. A broker
with a lower limit drops a birth larger than its own: Mosquitto takes 256 MB,
`modbee-mqtt serve` 64 kB unless `--max-packet` says more - a thousand-metric
birth is about 67 kB.

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

**Compressed payloads are passed through, not decompressed.** A payload
whose `uuid` is `SPBV1.0_COMPRESSED` decodes as what it is on the wire - the
uuid, an `algorithm` metric and the compressed bytes in `body` - and reaches
`onMessage()` like that. Nothing in the library inflates it, so a node that
compresses is read by an application that does it itself.

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

**No fixed number of metrics or devices** on a node, on a host or a board: a
node declares as many metrics, under as many devices, as memory holds, and a
declaration memory cannot take is refused and `node.outOfMemory()` says so.
`node.metricLimit(n)` sets a limit of your own.

**A host's tables have limits, because a peer chooses what they hold.**
`host.nodeLimit(n)` (default `MQ_SP_HOST_NODE_LIMIT`: a million on a host, 32
on a board) and `host.metricLimit(n)` (`MQ_SP_HOST_METRIC_LIMIT`: 65,536 a
node on a host, 1,024 on a board). A birth past either is still delivered to
`onMessage()`; what it would have added is refused and counted
(`nodesRefused()`, `metricsRefused()`, and per node `metricsRefused`), and a
birth of more metrics than the host would track of any node is not decoded at
all. A DBIRTH replaces its device's part of the dictionary, so a device that
rebirths with new aliases does not grow it. On a host every lookup is through
a hash index - a node by group and id, a metric by alias and by name - so each
message costs one lookup per metric however large the plant; on a board the
limits keep a straight search short. The decode payload is sized to each
message before it is decoded (`mq_sparkplugMeasure()`), and given back once
sixteen small messages in a row have followed a large one; a node's table
shrinks when it births smaller. A metric with more than `MQ_SP_MAX_PROPERTIES`
properties keeps the first ones and counts the rest in the payload's
`propertiesDropped`, rather than failing the whole birth.

| Macro | Default | What it bounds |
| --- | --- | --- |
| `MQ_SP_MAX_METRICS` | 34 on a board, 256 on a host | The size of a payload nobody sized, and where a host's decode payload starts before it grows |
| `MQ_SP_MAX_SERVERS` | 4 | Brokers a node can fail over through |
| `MQ_SP_MAX_PROPERTIES` | 4 | Properties kept per metric; the rest are counted |
| `MQ_SP_HOST_NODE_LIMIT` | 32 on a board, 1,048,576 on a host | `host.nodeLimit()` to start with |
| `MQ_SP_HOST_METRIC_LIMIT` | 1,024 on a board, 65,536 on a host | `host.metricLimit()` to start with |
| `MQ_SP_COMMAND_LIMIT` | 4 kB on a board, 1 MB on a host | `node.commandLimit()` to start with |
| `MQ_SP_REBIRTH_HOLDOFF` | 2000 ms | `node.rebirthHoldoff()` to start with |

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
- **A hostile peer and a bad link**, in-process: `test_sparkplug_hardening`
  (every limit, linear time on large births and tables, the rebirth holdoffs,
  the replay window, timestamps past 49.7 days of uptime),
  `test_sparkplug_soak` (a node and its primary host through seven simulated
  hours of cellular faults - half-open links, cuts, FINs, latency, small MTUs,
  short writes - across the 32-bit millisecond wrap, checking bdSeq, seq, birth
  order, history order and that every value published arrives), and
  `test_sparkplug_fuzz` (the decoder, nested templates, datasets, property
  sets and compressed payloads, then the same bytes delivered to a live host
  and node), all under AddressSanitizer.

Every requirement in the code names the statement of the specification it
implements, by its `tck-id`. [extending.md](extending.md#sparkplug-b) has the
design history.

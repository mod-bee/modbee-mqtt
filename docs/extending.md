# Extending

The point of this library is what gets built on top of it — a broker, a
Sparkplug B layer, OpenPLC function blocks, whatever the plant needs in three
years. All of it has to be addable **without reopening the core**.

That is a constraint on the code as it stands, not a hope for later. This
document is the contract.

**The broker was the first thing built on that claim, and it held**: two
additive changes, nothing altered, and one thing moved out of `MqttClient`
because the broker needed exactly the same code. The section below says
precisely what it took, which is a more useful thing to read than a promise.

---

## The nine rules

| | Rule | What it buys |
| --- | --- | --- |
| **R1** | No layer below L3 knows what role it is playing | A broker is a *different L3* over the same L0–L2, not a fork |
| **R2** | Every packet type encodes and decodes in both directions, from day one | Half the codec is not dead-on-arrival when the broker arrives — and a decoder that has only ever parsed one direction has half its branches unexercised |
| **R3** | Raw bytes are the base API; the library never publishes a topic nobody asked for | Sparkplug's protobuf payloads and namespace are expressible; nothing we inject can corrupt someone else's topic space |
| **R4** | Every event is available as a callback **and** as pollable state | Generated IEC 61131 code cannot register a lambda |
| **R5** | Instance-scoped, always. No process-wide state | Several clients in one OpenPLC runtime |
| **R6** | Publish ordering is deterministic and documented | Sparkplug sequence numbers depend on it; so does any command/ack pattern |
| **R7** | Session-boundary hooks run before the send queue drains, **and what they queue goes first** | A layer above can inject its birth certificate ahead of application traffic |
| **R8** | No exceptions, no RTTI | Generated wrapper code, and MCU toolchains with both disabled |
| **R9** | No layer above the platform layer knows which platform it is on | The same files build for an ESP32 and for a Linux container, and a platform's habits stay behind the transport interface instead of being charged to every other platform |

R1 and R9 are enforced by a source check that runs in CI. It looks at
**identifiers**, not words, because `class MqttClient` contains no standalone
word "client" and a boundary-based grep sails straight past it. Protocol data
that both roles carry — the Client Identifier field, Server Keep Alive — is
allowlisted by exact name with the section that defines it.

R7 needed two halves and the first version only had one. Running the hook early
is not enough if what it queues is then appended *behind* the traffic it was
supposed to precede. So operations queued inside the hook are marked priority,
and that window is the hook's own execution and nothing else.

---

## The broker: what happened when this was built

This section used to describe what a broker *would* need. It exists now, so
this is what it actually took — which is the only honest measure of an
extensibility claim.

**Two things were added. Nothing was changed.**

- `MqttListener` (L0) — a second way a link begins. It adds a type; it does not
  alter `MqttTransport`, so every existing transport still compiles and behaves
  identically. **Four implementations of it ship now** — `MqttTcpListener` and
  `MqttTlsListener` on a host, `MqttStreamListener` and
  `MqttStreamTlsListener` as templates over an Arduino server type — and none
  of the three added after the first needed a change to the interface. That is
  the strongest evidence in the repository for the claim this section makes.
- `MqttTransport::peerName()` — a defaulted virtual returning an empty string,
  for diagnostics and for an authorisation hook that wants to know where a
  connection came from. No existing implementation had to notice.

**And then one thing WAS changed, later, and it is worth recording why.**

`MqttTransport`'s read side used to be `int available()` plus `int read()`
returning a single byte — Arduino's `Stream` idiom, in the interface every host
socket also had to implement. It was wrong in a way no test could catch:
`MqttTcpTransport` already did a bulk `recv()` into its own buffer and was then
obliged to hand those bytes back one at a time through a virtual call, while
`MqttLink` re-decoded the packet header after every single one of them.

The library now reads in bulk on both sides, matching `write()`:

```cpp
virtual size_t readable();                          // bytes available now
virtual size_t read(uint8_t* into, size_t length);  // take up to length
virtual size_t write(const uint8_t* from, size_t length) = 0;
```

**And it is not a breaking change**, which took a second pass to get right. The
byte-wise pair was removed first, on the grounds that leaving it would let the
idiom creep back — which was tidy and was the wrong trade. A transport is how
somebody attaches this library to a medium nobody here anticipated; breaking
that interface charges them for a change that benefits us.

So both pairs live on `MqttTransport`, neither is pure, and each has a default
written in terms of the other:

- implement the **bulk** pair if the medium can give you more than one byte at
  a time — a socket always can, it has already read them into a buffer;
- implement the **byte-wise** pair and the base class loops it for you.

A transport written against the old interface compiles and runs untouched.
`test/test_link.cpp` contains one, written exactly as it would have been
before, as a standing check that this stays true. The cost is two vtable slots:
**68 bytes** of flash across the whole library, measured.

Arduino's idiom now lives inside `MqttStreamTransport` where it belongs — and
that class uses `Client::read(buf, size)`, never `Stream::readBytes()`, because
the latter blocks for up to a second and nothing in this library may block.

Two defaulted virtuals were added at the same time and break nothing:
`waitHandle()`, returning `-1` where the platform has no waitable descriptor,
and `hasBufferedInput()`, returning false. A host broker uses both to ask the
kernel which connections have something to say instead of asking every
connection in turn; a board overrides neither and polls exactly as before.

**The lesson, since this section is about extensibility claims.** "Two things
were added, nothing was changed" was true and was still the wrong shape: an
interface can be correct, stable, and quietly impose one platform's habits on
every other. R9 exists because of this, and
`scripts/check-layering.py` enforces it — but it would **not** have caught this
one, because `int read()` names no platform. A shape can be wrong without
saying whose shape it is, and catching that still needs a person. What R9 does
catch is the next one along: an `#if defined(ARDUINO)` or an ESP32-only branch
appearing in a codec, in the link, or in the broker's protocol handling, where
the answer is always to put it behind the transport interface instead. It
caught one within the hour of being written — a host-only `std::string`
overload on `MqttTopic.h`.

**One thing moved, and that was the point.** The framing — reassembly, the
append queue, partial-write resume — came out of `MqttClient` into `MqttLink`,
because the broker needed exactly the same thing and none of it depends on
which end of the connection you are on. That is R1 paying for itself: the
reuse was already possible, it just had not been named yet.

**Everything else was new L3**, which is where it belongs: routing, the
retained store, the subscription registry, session takeover,
shared-subscription groups, will delay.

**One thing was deliberately NOT reused.** `MqttSession` and
`MqttBrokerSession` are different types. [V5:4.1] defines Session State
separately for each end and the lists are not the same — a client keeps its own
packets to replay, a server keeps the client's subscriptions and the messages
owed to it. Sharing a type there would have been reuse for its own sake, and
the broker would have inherited storage shaped for the wrong problem: the
client stores ENCODED bytes, because [V5:MQTT-4.4.0-1] requires the same
identifiers replayed unchanged, and a broker cannot do that at all — a session
may resume on a connection that negotiated a different version.

If you are building something else on this, that is the pattern: reuse what is
genuinely the same operation, and write a new type where the standard says the
two ends hold different things.

---

## Where a convenience layer goes

`MqttDevice` is L4 and is the shape to copy. It sits **above** `MqttClient`,
calls only its public API, and the layers below do not know it exists — which
is what lets it take liberties they may not: it defaults version fallback on,
copies payloads so a handler gets a C string, and keeps its own filter table.

The rule, from `docs/ARCHITECTURE.md`: **a convenience layer may choose a
default, and may not hide an outcome.** A Sparkplug layer, an OpenPLC wrapper
and a typed binding layer all live at the same level and follow the same rule.

---

## Sparkplug B

**Using it is [sparkplug.md](sparkplug.md).** This section is how it was
built, and why.

**It is in the library, at `src/sparkplug/`.** This section used to argue that
it belonged somewhere else — "it drags in protobuf and this one has no
dependencies" — and that reasoning is worth recording because it was half
right.

Protobuf does not mean a protobuf library. The Sparkplug payload uses eight
features of the wire format — varints, fixed32, fixed64, length-delimited
fields, nested messages, `repeated`, `oneof`, proto2 `optional` — which is a
few hundred lines of encoder and decoder, not a dependency. Vendoring nanopb
would have put a code generator, a third-party runtime and two pinned tool
versions into a library whose whole pitch is having none of those, and a
protobuf runtime on an ESP8266.

Eclipse Tahu's own C stack does vendor nanopb, and it also keeps its sequence
number in a file-scope variable — which breaks **R5**: two clients in one
OpenPLC runtime would share one counter and neither would be conformant. So its
structure is a model here and its code is not a source.

### What it is

`MqttSparkplugNode` is an edge node and `MqttSparkplugHost` a Host Application.
Both are **L4**, beside `MqttDevice`, and follow the same rule: *a convenience
layer may choose a default, and may not hide an outcome*. They choose what the
specification chooses — Clean Start true, NBIRTH at QoS 0 retain false, the
Will at QoS 1 retain false, NCMD subscribed at QoS 1 — and they refuse rather
than correct a configuration that would produce a non-conformant node.

The seven seams the old table listed are all still the reason this works, and
`test/extension/test_sparkplug_shape.cpp` still asserts them. It is worth
keeping even now the layer exists: it says the *core* does not obstruct a
Sparkplug layer, which is a different claim from the layer being correct.

### TLS, and the mode a plant actually uses

`MqttSparkplugNode` and `MqttSparkplugHost` reach a broker the same way
`MqttDevice` does, and the default worth naming is **pinning**.

A plant broker is a ModBee PLC broker or a site Mosquitto that nobody signed
for. There is no public authority to verify it against, so chain verification
has nothing to check and the only honest answer is to trust **one key** rather
than an authority: the first connection is unverified, the broker's public-key
fingerprint is recorded, and every later connection is checked against it. A
different key is refused with both fingerprints reported.

That matters for usability as much as for security. The alternative is placing
a CA file on every controller and naming its path in the program, which is
laborious to deploy and easy to get wrong — and on the OpenPLC wrapper the
pinned key lives in the block's own retained variable, so there is nothing to
wire, name or type at all.

`MQTT_TLS_VERIFY` with a CA remains the right answer for a **public** broker,
where an authority genuinely exists. Pinning deliberately skips chain and
expiry checking: a controller with no clock cannot judge an expiry date, and a
link that drops because a PLC booted believing it is 1970 is worse than one
that trusts a key it recognises.

### What a host must do that a node does not

A Host Application is not an edge node pointed the other way, and one thing in
particular has no counterpart: **it has to keep the birth certificate's
dictionary.**

A conformant node puts metric names in its NBIRTH and DBIRTH and then sends
data carrying the **alias alone** — that is where most of Sparkplug's bandwidth
saving comes from, an alias being one or two bytes where a name is thirty. So a
host that does not record the births receives a stream of numbers and cannot
say what any of them measures. `MqttSparkplugHost` keeps a per-node alias table
(as large as the births, memory permitting), rebuilt by each NBIRTH and extended by each
DBIRTH, and fills the names back in before handing a message to the
application. One table per **node**, not per device, because an alias is unique
across an edge node's entire set of metrics, devices included.

An alias the node never declared resolves to nothing rather than to a guessed
name, and a name that did arrive is left alone — overwriting it with our record
would hide a disagreement rather than let the caller see it.

### The two things the core needed

`MqttClient::reconfigure()`. The Will is a field of the CONNECT packet, and
Sparkplug's `bdSeq` increments on every CONNECT and must appear in both the
Will and the NBIRTH — so the Will has to be rewritten between connections.
`begin()` would do it and is the wrong tool: it resets the status counters a
function block reads every scan and re-caps the operation queue without
concluding it. A reconnect is not a restart.

Nothing in `reconfigure()` knows what Sparkplug is. Anyone with a rotating
death certificate or a failover server list needs exactly that method.

`MqttClient::withdraw()`, and it was found the same way — by a requirement the
core could not express. A rebirth request means *stop sending data
immediately*, and refusing new data is not enough: a poll services receive
before it services the send queue, so the request is handled while data
published earlier in the same scan is still queued, and the queue then drains
oldest-first and sends it afterwards. `withdraw()` takes back what has not been
written yet.

Nothing in it knows what Sparkplug is either. Anyone about to restate
everything has already made a queued update redundant.

### What is out, and why that is a real answer

`DataSet`, `Template`, `PropertySetList` and the fourteen array datatypes.
Thirty-seven of the specification's statements, and the three structured types
are its nested, mutually recursive messages — most of the codec's difficulty for
none of a plant's needs. `PropertySet` with scalar values **is** in, because
that is where engineering units and quality codes live.

An unsupported datatype is refused **by name** when decoded and at declaration
time when written, never defaulted. A metric silently decoded as zero is the
fault that survives commissioning.

### How it is known to be right

Three independent checks, because the library's own tests were written from the
same reading of the specification as the code and a misreading would be made
twice:

- **`docker/testing/run.sh --phase sparkplug-codec`** — our bytes decoded by Eclipse
  Tahu's Python and by the npm `sparkplug-payload`, and theirs decoded by us.
  There are no official byte vectors for Sparkplug anywhere, so this is the only
  evidence the encoder agrees with the schema rather than with itself.
- **`docker/testing/run.sh --phase sparkplug`** — a live node on Mosquitto with an
  independent Python Host Application judging ordering, sequence numbers and
  birth/death correlation.
- **`docker/testing/run.sh --phase sparkplug-tck`** — the Eclipse Sparkplug TCK, the
  specification project's own suite, driven headlessly over its control topics.
  It found the one fault nothing else did: payload timestamps were
  `mq_millis()`, a monotonic counter since boot, where
  `[tck-id-payloads-timestamp-in-UTC]` requires UTC milliseconds since the
  epoch.

Every requirement cited in the code names the statement that fixes it, by the
specification's own `tck-id`. The sources are pinned with hashes in
`internal/sparkplug/`.

---

## Building an OpenPLC wrapper

The wrapper is generated C++ inside a scan cycle, driven from IEC 61131
function blocks. Four things it needs:

**Nothing blocks, ever.** A scan cycle that stalls on a socket is a PLC missing
its deadline. `connect()` included.

**No lambdas.** A function block has inputs, outputs and static storage. It
cannot capture a closure. So every event has a polled twin:

```cpp
const MqttTopicState& state = client.topicState(handle);
// state.published, state.failed, state.received are RUNNING TOTALS.
// A block compares one with what it saw last scan.
```

Totals rather than flags, so nothing is missed between scans and two blocks
reading the same handle cannot take each other's edges.

**Handles, not strings.** A block passing `STRING[255]` by value every scan is
both a performance problem and an ergonomic one — the block grows a 255-byte
input nobody wants to wire.

```cpp
MqttTopicHandle h = client.registerTopic("plant/line3/pressure");
client.publishTo(h, data, length, 1, false);
client.subscribeTo(h, 1);
client.takeMessage(h, buffer, sizeof buffer, length);   // polled receive
```

A handle may hold a **filter** as well as a name, so one block can watch a
group of devices. The string API stays and is the primary one; this is the same
machinery reached by a different name.

**Stable enums.** Every result enum carries a `_COUNT` pinned by
`static_assert`, so a generated binding that casts rather than translates turns
a drift into a build error instead of a wrong reason code.

All of it works the way generated code is written — no lambdas, no
`std::function` on the path, edge-triggered blocks with one-scan `DONE` pulses
— and the whole library builds with `-fno-exceptions -fno-rtti`.

---

## What would break the contract

Concretely, so it is recognisable in a diff:

- a `std::function` parameter on `publish`, `subscribe` or anything reached from
  `poll()`
- an event only reachable by registering a handler
- any `static` holding connection state
- a topic the library publishes on its own initiative — a status topic, a
  diagnostic feed, anything
- reordering the send queue for any reason other than the session hook
- `if (weAreTheClient)` anywhere below `MqttClient.cpp`
- a call that waits for the network

Every one of those is checked before a release. The last is the hardest to
spot by reading, and the easiest to catch by running: drive the library on a
simulated clock, and a blocking call hangs rather than taking time.

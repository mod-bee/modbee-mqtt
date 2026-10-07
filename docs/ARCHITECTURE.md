# Architecture

Five layers, and why each boundary is where it is.

This is the document to read if you want to know *why* the library is shaped
the way it is, or if you are about to reach past the one-object API into the
layer underneath. If you just want to make it work, read
[Getting started](getting-started.md) instead.

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │ L4  ABOVE     MqttDevice      one object, the short form             │
 │               MqttTypedBindings · MqttPoller    optional, and built  │
 │               (a Sparkplug layer would live here)  on the raw API    │
 ├──────────────────────────────────────────────────────────────────────┤
 │ L3  ROLE      MqttClient      the state machine        ← the ONLY    │
 │               MqttBroker      the other end              layers that │
 │               MqttBridge      two clients and a mapping  know a role │
 ├──────────────────────────────────────────────────────────────────────┤
 │ L2  CORE      MqttLink        framing: reassembly, partial writes    │
 │               MqttOperations  the queue: one conclusion each         │
 │               MqttSession     what survives a connection             │
 │               MqttFlow        send quota, symmetric                  │
 │               MqttAliases     topic aliases, both directions         │
 │               MqttCapabilities what this connection can do           │
 │               MqttTopicRegistry  topics by number, polled state      │
 ├──────────────────────────────────────────────────────────────────────┤
 │ L1  PROTOCOL  MqttWire        primitives shared by both versions     │
 │               MqttCodecV311 · MqttCodecV5   complete, both directions│
 │               MqttProperties  5.0's 27 identifiers, typed            │
 │               MqttTopic       validation, matching, filter overlap   │
 ├──────────────────────────────────────────────────────────────────────┤
 │ L0  LINK      MqttTransport   one ordered reliable byte stream       │
 │               MqttListener    the other way a link begins            │
 └──────────────────────────────────────────────────────────────────────┘
```

Dependencies point downward only.

## L4 is a convenience, and it is allowed to be nothing else

`MqttDevice` is the object most code uses, and it is the only part of this
library whose job is ergonomics rather than protocol.

That distinction is enforced by where it sits. It is **above** `MqttClient` and
calls the same public API an application would — there is no back door, no
private access, and nothing in the layers below knows it exists. Delete the
file and the library still does everything it did.

Which is why it can take liberties the layers below may not. It defaults
version fallback ON, where `MqttConnectionConfig` defaults it off; it copies
every payload so a handler can have a NUL-terminated string, where the decoder
hands out spans that copy nothing; it holds a fixed table of filters, where the
client holds subscriptions the protocol's way. Every one of those is a
judgement about what is convenient, and every one of them would be wrong a
layer down.

The rule that keeps it honest: **a convenience layer may choose a default, and
may not hide an outcome.** So `statusTopic()` picks retained-QoS-1 for you, and
`publishAndConfirm()` still reports exactly one conclusion; `isFresh()` answers
a question the protocol cannot, and `client()` is right there when the answer is
not the one you want.

## The broker was the proof of this

The layers above were designed so that a broker could be a **different L3 over
the same L0-L2**, rather than a fork. That is easy to claim and easy to lose,
so it was held to from the start: a full exchange can be driven from the
SERVER side using only public API, and a role assumption creeping into L1 or L2
stops that compiling.

When the broker was finally written, **two things were added and nothing was
changed**:

- `MqttListener` — a second way a link begins. It adds a type; it does not
  alter `MqttTransport`.
- `MqttTransport::peerName()` — a defaulted virtual returning an empty string,
  for diagnostics and for an authorisation hook that wants to know where a
  connection came from. No existing implementation had to notice.

One thing moved: the framing — reassembly, the append queue, partial-write
resume — came out of `MqttClient` into `MqttLink`, because the broker needed
exactly the same thing and none of it depends on which end of the connection
you are. That is the outcome the rule was for.

The `MqttSession` the client keeps and the `MqttBrokerSession` the broker keeps
are deliberately **different types**. [V5:4.1] defines Session State separately
for each end and the two lists are not the same: a client keeps its own packets
to replay, a server keeps the client's subscriptions and the messages owed to
it. Sharing a type there would have been reuse for its own sake.

---

## The stream never resynchronises

This is the first thing to know, and the thing most likely to be got wrong by
somebody arriving from a fieldbus protocol.

Modbus RTU frames are delimited by silence. BeeBus frames begin with a flag
byte. Both can lose a byte, notice, discard the frame and carry on. **MQTT can
do none of that.** There is no delimiter, no checksum and no start byte — one
stray octet desynchronises the connection permanently, and every "packet" after
it is garbage that may still parse into something plausible.

So a codec here never searches for a boundary after a failure. It reports, and
the session **tears the connection down** — sending a DISCONNECT carrying the
reason code where the version has one (V5:4.13.1), then closing. A reconnect is
the only recovery there is.

---

## Nothing blocks, `connect()` included

Every other MQTT client we surveyed blocks somewhere, and nearly all of them
block in `connect()` waiting for the acknowledgement. FreeRTOS coreMQTT — the
best of them — spins for a caller-supplied timeout and cannot resume a partial
handshake across calls.

That single fact is why this library exists. An OpenPLC scan cycle and an
Arduino `loop()` are the same constraint: a call that waits for a network round
trip cannot live inside either. So `connect()` queues the intent and returns,
and the session is a state machine polled from the loop:

```
Idle → Connecting → AwaitingConnAck → [Authenticating] → Connected
                                                             ↓
                     Disconnecting ← ───────────────── Reconnecting
```

`Authenticating` exists from the start even though no SASL mechanism ships,
because retrofitting a handshake round trip into a settled state machine is far
worse than leaving a door in it.

---

## Two versions, two codecs, one capability set

3.1.1 and 5.0 are not one protocol with a version byte.

CONNACK carries a **Connect Return Code** (`0x00`–`0x05`) in 3.1.1 and a
**Reason Code** from a 44-value table in 5.0 — different value spaces entirely,
where `0x01` means "unacceptable protocol version" in one and "Granted QoS 1"
in the other. SUBACK in 3.1.1 permits four values and reserves the rest.
UNSUBACK is a packet identifier and nothing else. DISCONNECT has no body.
There are no properties anywhere, and no AUTH packet.

So there are **two complete codecs**, not one with `if (version == 5)` inside
every function. Scattered branches are where version bugs live: they pass
whichever version happened to be exercised and the other path is never run.
Two codecs means a branch that was never
written is a function that does not exist rather than a fall-through nobody
noticed.

What they share is `MqttWire` — the fixed header, the Variable Byte Integer,
the length-prefixed string, binary data. Those are identical in both versions
and live once.

**Above L2, nothing asks the version.** It asks `MqttCapabilities`: may I use a
topic alias, what is the highest QoS I may publish at, is retain available. On
5.0 that comes from the CONNACK properties; on 3.1.1 it is filled in with
3.1.1's fixed answers. That is what stops version handling leaking upward, and
it is what will let a Sparkplug layer or a broker work at both versions without
knowing either.

### The CONNACK is decoded version-tolerantly

A peer that speaks only 3.1.1, handed a level 5 CONNECT, answers with a
**3.1.1-shaped CONNACK** and closes. So the decoder cannot assume the version it
just asked for.

The discriminator is exact and needs no guessing: a 3.1.1 CONNACK has a
Remaining Length of exactly 2; a 5.0 one has a Property Length after the reason
code and so is 3 or more. Only after that is the session's version fixed.

---

## L1 has no I/O and no clock

`MqttWire`, `MqttProperties`, `MqttTopic` and both codecs are functions over
buffers. They never allocate, never read the clock and never touch a socket.
Everything they decide is decided by the bytes in front of them.

That is not tidiness. It is what makes the layer answerable **directly against
the standards' own worked byte examples** — the CONNECT variable header of
V5 Figure 3-6, byte for byte — and exercisable in isolation, which is where
parser defects live.

The class of bug this catches is "a declared length was trusted as a bound".
MQTT hands a decoder **four independently declared lengths**: the Remaining
Length, the Property Length, each two-byte string or binary prefix, and whatever
payload is left over. Every one of them is a number the peer chose. Deriving any
one from another, or trusting one as a bound on the buffer, is how a parser
reads someone else's memory.

### One of them is not what it looks like

Every *length-prefixed* field is bounded at 65,535 by its own prefix. **A
PUBLISH payload is not one of them** — it has no length field at all, and the
standard permits it up to 268,435,455 bytes. An earlier version of this library
held span lengths in sixteen bits, which silently capped every payload at 64 kB
and reported a larger one as the *caller's* invalid argument. Everything built
on a 60 kB message worked. The live broker rig found it.

---

## The queue, and the timer that is not there

`MqttOperations` holds what the application asked for until the link can carry
it. Three properties are worth stating on their own.

**Supersede.** A publish to a topic already queued and not yet sent *replaces*
the queued value rather than stacking behind it, and the superseded caller is
**told**. A setpoint written every scan sends the value it settled on, not a
history. The guards are subtle: an operation already **on the wire** is never
rewritten, because the acknowledgement would then confirm a value that was never
transmitted. And it is **opt-out per publish** — `publishSeries()` turns it
off, because a stream carrying its own sequence numbers would show an elided
message as a gap.

**Exactly one conclusion.** Every accepted operation reaches one, always. An
operation that is accepted and silently never concluded does not look like a
bug; it looks like a control loop that quietly stopped publishing a few hours
in, which is far harder to find.

**There is no retransmission timer, and that is not an omission.**
[V5:MQTT-4.4.0-1]:

> *"This is the only circumstance where a Client or Server is REQUIRED to resend
> messages. Clients and Servers MUST NOT resend messages at any other time."*

The only circumstance is reconnecting with Clean Start 0 onto a session that is
present. So nothing is ever **resent**: a timer that resent would emit duplicate
application messages and violate the standard. That absence is deliberate and is
held to, because adding one looks like an improvement.

**But there is a deadline, and it does not resend — it gives up.** The standard
forbids resending; it says nothing about how long a client must wait before
deciding an operation has failed, and the two are not the same thing.

`MqttClientConfig::acknowledgementTimeoutMs`, sixty seconds by default,
concludes a sent QoS>0 operation whose acknowledgement is overdue with
`AckTimeout`. The handler is told, the queue slot is freed, and — the half that
matters — the unit of the peer's Receive Maximum it was holding is released.

**This paragraph used to say keepalive was the only watchdog there is, and that
was the hole.** Keepalive proves a peer's socket and event loop are alive. It
proves nothing about whether acknowledgements are flowing, and a broker can
answer every PINGREQ while acking nothing. Without a deadline such a broker
leaves every QoS>0 operation outstanding for ever, and once `receiveMaximum` of
them are stuck the client cannot publish at QoS>0 again — silently, with the
link up and no status field saying why.

That is not hypothetical. A plant ran for five hours with TLS up, keepalive
answered, `MQTT_STATUS.PENDING` stuck at 6 and `PUBLISHED` still climbing,
because the QoS 0 traffic was unaffected. Bouncing the broker never reproduced
it, because a bounce *drops the link* and a dropped link concludes everything —
which is why the deadline, not the drop path, was the missing piece.

Setting it to zero restores waiting for ever, for an application that would
rather stall than see a message reported failed when it may well have been
delivered. That is a real preference and it is a decision, not a default.
`test_client` pins all four behaviours: the stall recovering, the drop
concluding without a deadline, zero waiting for ever, and QoS 0 never being
caught by a deadline it was never waiting on.

---

## The session is what survives a connection

V5:4.1 defines the membership precisely, and the membership *is* the design.

**In the session** — QoS 1 and 2 publishes sent but unacknowledged; QoS 2
publishes received but not completely acknowledged; PUBRELs sent but
unacknowledged; the subscriptions.

**Not in the session, re-initialised every connection** — the send quota and
Receive Maximum (V5:4.9), topic aliases (V5:3.3.2.3.4), and every other CONNACK
negotiation.

Getting a member of the second list into the first is a silent bug that
only appears after a reconnect. Carrying a topic alias across one means
publishing to whatever it used to mean — a bug that works, and puts the data
somewhere else.

**Subscriptions are the application's standing intent, not the peer's session
state.** A subscribe made before the connection exists is recorded and
established when possible. Clean Start clears the protocol state and keeps them;
clearing them too would mean a Clean Start connection silently delivering
nothing the application asked for.

---

## Flow control is negotiated, not configured

V5:4.9. `maxInFlight` is not a setting — it is the peer's Receive Maximum, read
from CONNACK. The rules are three lines and one of them is a trap:

- initial quota is non-zero and does not exceed the peer's Receive Maximum
- at zero, no more QoS>0 PUBLISH may be sent
- **everything else is still processed and answered**

The third is the trap. A naive "the queue is full, stop the world" also stops
acknowledging inbound publishes, so the peer's quota drains too and neither end
can make progress. The deadlock looks like a dead link and is entirely
self-inflicted.

Real brokers differ by more than three orders of magnitude here - some allow
ten QoS>0 publishes in flight, some sixty-five thousand. A client that assumed
any one of those numbers would be wrong against the others.

---

## Nothing is silently dropped

The rule that separates this library from most: **anything the connection
cannot carry is refused with a named reason.**

| Result | Means |
| --- | --- |
| `NotSupportedByProtocol` | 3.1.1 cannot express this at all — a message expiry, a user property, a topic alias |
| `NotSupportedByPeer` | 5.0 can express it and this peer said no — retain unavailable, maximum QoS 1 |

Two results, because they send an engineer to different places. The alternative
— send what fits and discard the rest — means a setpoint's expiry interval
silently vanishes on the way to an older broker, and the symptom appears weeks
later as a stale command nobody can account for.

---

## Where to go next

- [extending.md](extending.md) — the nine rules as a contract, and what a
  Sparkplug layer or a PLC wrapper would hang off
- [api.md](api.md) — everything public, including the layers below `MqttDevice`
  that this document describes

# Versions

This library speaks MQTT **3.1.1** (Protocol Level 4) and **5.0** (Level 5).
The same application code works against both; the version is a property of the
connection rather than a branch in your program.

This document is what differs, and what happens when you ask for something the
connection cannot carry.

---

## Choosing

Explicit, because a plant engineer should be able to read the config and know
what is on the wire:

```cpp
connection.version = MqttVersion::V5;
```

Fallback is opt-in and off by default:

```cpp
connection.fallbackEnabled = true;
connection.fallbackVersion = MqttVersion::V311;
```

**`MqttDevice` reverses that default, on purpose.** The long form is
configuration a plant engineer reads to know exactly what is on the wire; the
short form exists to work against whatever broker is installed, so it asks for
5.0 and drops to 3.1.1 by itself. `mqtt.only(MqttVersion::V5)` turns it off,
and `mqtt.activeVersion()` says what it settled on either way.

It triggers **only** on evidence that the version is the problem:

- a 5.0 CONNACK with reason code `0x84` (Unsupported Protocol Version), or
- a 3.1.1-shaped CONNACK with return code `0x01` (unacceptable protocol level)

Never on an authentication failure, a TLS failure or a network error. Retrying
a rejected password at another version is how a client gets itself banned, and
it tells an operator the version is wrong when the credentials are.

At most one fallback per connect attempt. The version that worked is
remembered, so a reconnect does not pay for the discovery twice.

```cpp
mqtt.activeVersion();      // what is in force, not what was asked for
```

---

## What 3.1.1 cannot do

Ask for any of these on a 3.1.1 connection and the call returns
`MqttResult::NotSupportedByProtocol`. **Nothing is silently dropped.**

| | Why it matters |
| --- | --- |
| Properties of any kind | Content Type, Response Topic, Correlation Data, User Property, Payload Format Indicator — all 5.0. Asking for one on a 3.1.1 connection is refused, never dropped: see below |
| Message Expiry Interval | A command that should not be acted on if it arrives late has no way to say so |
| Topic Alias | A long topic is sent in full on every publish |
| Session Expiry Interval | Clean Session alone decides; there is no "keep it for an hour" |
| Will Delay Interval | The Will is published the moment the connection drops, so a flapping link produces false offline alarms |
| Subscription options beyond QoS | No Local, Retain As Published and Retain Handling are 5.0 |
| Reason codes on acknowledgements | A publish the broker refused is indistinguishable from one it accepted |
| Reason codes on DISCONNECT | No way to say why, in either direction |
| Enhanced authentication (AUTH) | Packet type 15 is reserved |
| Subscription Identifiers | Nothing links a delivered message to the subscription that matched it |
| Shared Subscriptions | `$share/...` has no meaning |

### Properties, in both directions

**Inbound**: every property on a PUBLISH is decoded and handed to you.
`MqttMessage::properties` carries Content Type, Response Topic, Correlation
Data, User Properties, Payload Format Indicator, Subscription Identifier and
Message Expiry Interval, and is null only on a 3.1.1 connection where there are
none. Through the facade: `payload.responseTopic()`, `payload.contentType()`,
`payload.userProperty("key")`, `payload.secondsUntilExpiry()`.

**Outbound**: build an `MqttOwnedProperties` and hand it to `publish()`. It is
copied into the queue, so it may be a local.

```cpp
MqttOwnedProperties properties;
properties.setContentType("application/json");
properties.setMessageExpiry(30);                 // seconds; the broker discards it after
properties.addUserProperty("issued-by", "panel-1");

// MqttDevice - the set goes BEFORE the QoS, because qos and retain are
// defaulted on the facade and a caller usually wants neither.
device.publish("plant/line3/batch", payload, length, properties, MQ_QOS_1);

// MqttClient - the set goes AFTER retain, because the completion callback and
// its context come last and qos/retain are already positional there.
client.publish("plant/line3/batch", payload, length, MQ_QOS_1, false, properties);
```

The two argument orders are not an oversight and are worth reading twice: the
compiler catches a swap, because `MqttOwnedProperties` converts to nothing.

**A Will may carry them too** — `MqttWillConfig::properties` — which is what
lets a death certificate say what it is rather than only that it happened.

**Topic Alias is deliberately not settable.** Aliases are negotiated per
connection and `MqttAliases` owns the table; an application setting one by hand
would be overwriting it.

### What happens to properties on a 3.1.1 connection

**Refused, never sent stripped.** A publish carrying properties returns
`NotSupportedByProtocol` — at the call when the version is already known, and
at the send when it is not, because a publish queued before connecting cannot
know which version it will go out on. Either way exactly one conclusion
arrives and nothing goes on the wire.

That matters more than it looks. A publish that quietly lost its Correlation
Data leaves the requester waiting for an answer nothing can match to it, and
there is no error anywhere to explain why.

The same rule applies to the **Will**: one that needs 5.0 — a Will Delay
Interval, or any property — makes `connect()` return
`NotSupportedByProtocol` at 3.1.1, **and refuses a fallback that would land
there**. Connecting anyway would mean a device that believes it has a
thirty-second Will Delay and has none, filling a plant with the false offline
alarms the delay was set to prevent.

If you want a device that works against either version, do not set 5.0-only
Will fields. That is a deliberate choice you can read in the configuration,
which is the whole point.

### The one that surprises people

**A 3.1.1 acknowledgement carries no reason.** A QoS 1 publish that the broker
accepted and a QoS 1 publish it refused produce the same PUBACK — two bytes and
a packet identifier. So on 3.1.1 an operation can only ever conclude as `Ok` or
with a connection-level result. There is no `NotAuthorized` on a publish, and
the library does not invent one.

Likewise UNSUBACK: at 3.1.1 it is a packet identifier and nothing else, so
unsubscribing from a filter that was never subscribed looks exactly like one
that was.

---

## What is the same

More than the list above suggests. All of this behaves identically:

- QoS 0, 1 and 2, including the full four-packet QoS 2 handshake
- retained messages
- Wills, and Clean Session / Clean Start
- keepalive, PINGREQ and PINGRESP
- session resumption across a reconnect, and redelivery with the DUP flag
- topic names, topic filters, and wildcard matching — V5:4.7 and V311:4.7 are
  the same rules
- the supersede behaviour, the queue, and the one-conclusion contract

---

## The trap: the same number means different things

`0x01` on a CONNACK is **"Connection Refused, unacceptable protocol version"**
in 3.1.1 and **"Granted QoS 1"** in 5.0. They are entirely different value
spaces — 3.1.1 has a six-value Connect Return Code table, 5.0 has a 44-value
Reason Code table.

SUBACK is worse, because they overlap. 3.1.1 permits exactly `0x00`, `0x01`,
`0x02` and `0x80` and **reserves everything else**; 5.0 uses the full reason
code table, where `0x87` is Not authorized and `0x03` is not valid on a SUBACK
at all.

This is why the library carries two complete codecs rather than one with a
version flag, and why every specification citation in the source is namespaced
`V5:` or `V311:` — **119 of 3.1.1's 139 normative statement identifiers also
appear in 5.0, meaning different things.** `[MQTT-3.1.2-2]` in 3.1.1 says the
Server *MUST* answer with return code `0x01`; in 5.0 it says the Server *MAY*
answer with `0x84` and then MUST close.

An unqualified citation silently tracks the wrong rule.

---

## How the fallback actually works

Worth knowing, because it is the one place the two versions meet.

When a peer that speaks only 3.1.1 is handed a level 5 CONNECT, it answers with
a **3.1.1-shaped CONNACK** and closes the connection. So the decoder cannot
assume the version it just asked for — if it did, it would fail to parse the
very packet that says what to try instead.

The discriminator is exact and needs no guessing:

| | Remaining Length |
| --- | --- |
| 3.1.1 CONNACK | exactly **2** — acknowledge flags and return code |
| 5.0 CONNACK | **3 or more** — a Property Length always follows the reason code |

Only after reading that is the session's version fixed.

A session cannot span a version change. Stored publishes are kept as encoded
bytes so they can be replayed exactly, and a 5.0 packet replayed onto a 3.1.1
connection would be gibberish — so a fallback clears the session.

---

## Capabilities, not version checks

Above the codec, nothing asks which version is in force. It asks what this
connection can do:

```cpp
const MqttCapabilities& caps = mqtt.capabilities();
caps.allowsQos(MQ_QOS_2);
caps.retainAvailable;
caps.allowsTopicAliases();
caps.allowsPacketSize(bytes);
caps.maximumPacketSize;
caps.receiveMaximum;
caps.sharedSubscriptionAvailable;
```

On 5.0 these come from the CONNACK properties. On 3.1.1 they are filled in with
3.1.1's fixed answers — no aliases, no subscription identifiers, no shared
subscriptions, QoS 2, retain available — because 3.1.1 has no way to say
anything else.

That is what keeps version handling out of your program — and it is what let
the broker be written at both versions without a single `if (version == 5)`
above the codec.

**A broker's stated limits are promises you must keep, and real brokers differ
enormously.** Maximum Packet Size ranges from a megabyte to not stated at all;
Receive Maximum from ten to sixty-five thousand; Topic Alias Maximum from five
to sixty-five thousand. A client that assumed any one of those numbers would be
wrong against the next broker it met — which is why `MqttCapabilities` takes
every one of them from the CONNACK rather than from your configuration.

**What ours states**, and every one of them is enforced as well as stated:

| | ModBee broker (default) |
| --- | --- |
| Maximum Packet Size | 8,192, clamped to the receive buffer |
| Receive Maximum | 32 |
| Topic Alias Maximum | 8 |

`maximumPacketSize` is clamped to the receive buffer rather than trusted,
because advertising more than you can hold is a promise broken at the worst
possible moment.


---

## MQTT-SN

Asked often enough to answer here: **this library does not speak it, and
MQTT-SN is not a version of MQTT.**

It is a separate protocol for links where TCP does not exist — ZigBee, LoRa,
802.15.4, plain UDP. Almost nothing is shared: topics travel as a 2-byte id
registered in advance rather than as a string, there is a gateway in the middle
rather than a direct connection to a broker, the packet set is different, and
sleeping clients are a first-class idea MQTT has no equivalent of.

So it is not a feature that could be added here. It would be a second protocol
implementation sharing almost nothing below the role layer.

**If you have devices on a non-IP link**, run an MQTT-SN **gateway** and point
it at a broker. The gateway speaks MQTT-SN to the sensors and ordinary MQTT to
the broker — which is what this library talks to, from the other side. That is
the architecture MQTT-SN was designed around, not a workaround.

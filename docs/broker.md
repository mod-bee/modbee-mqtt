# The broker

A broker that runs in the same binary as your application, needs no external
service, and behaves as the standards require.

```bash
cmake -S . -B build
cmake --build build -j
```

**There is no flag to turn it on.** The broker is compiled into the library
always, and a program that never names `MqttBroker` does not carry it — the
linker drops a class nothing references. That is measured rather than asserted;
[footprint.md](footprint.md) has the numbers and the check that fails if it ever
stops being true.

---

## What it is for

A plant panel that must keep working when the site link is down.

The broker runs in the panel, every device on the line talks to it, and a
[bridge](bridge.md) carries the handful of topics head office needs upstream.
When the site link drops, the line keeps running; when it comes back, the
bridge reconnects on its own.

That is a case a cloud broker cannot serve, and it is the reason this exists.
[recipes.md §3](recipes.md#3-a-site-that-keeps-running-when-the-link-drops)
walks the whole scenario through, from what the devices do differently
(nothing) to what survives the outage.

## What it is not

Stated plainly, so nobody discovers it at an awkward moment. This is the
library's broker, with the library's defaults; `modbee serve`, the server
built on it, sets its own (below the Capacity table) and can keep retained
messages.

- **No persistence.** A restart loses sessions and retained messages.
  [V5:4.1] permits that, and this broker takes the permission. A program that
  keeps its retained messages elsewhere puts them back after `begin()` with
  `restoreRetained()`, which stores without publishing; `modbee serve` does
  that with a file or Kafka, and keeps sessions and their queued messages too
  ([docker.md](docker.md#where-messages-are-kept)).
- **No clustering.** One process, one box.
- **No enhanced authentication.** AUTH is decoded and refused with `0x8C`. A
  client that asks for SCRAM is told no rather than told nothing.
- **No persistence of the subscription index across a restart** — it is rebuilt
  from the sessions, which are in RAM, so see the first point. Routing itself
  is *not* a linear scan any more: a hash over literal filters plus a short
  list of wildcard ones, with the bucket array sized from config and taken once
  in `begin()`, so nothing is allocated per message. Measured at 1000 clients
  it is flat rather than growing with the session count.
- **No WebSocket listener.** The transport interface is narrow enough that one
  would be additive — see [extending.md](extending.md) — but none ships.

---

## Twenty lines

```cpp
#include <ModBeeMqtt.h>     // the broker comes with it when the option is on

MqttTcpListener listener;
MqttBroker broker;
static volatile bool running = true;     // your signal handler clears this

int main() {
    MqttBrokerConfig config;
    config.maxConnections = 32;
    config.maximumPacketSize = 65536;
    config.receiveBufferSize = 65536;
    config.transmitBufferSize = 65536;

    listener.configure(nullptr, 1883, config.maxConnections);
    if (!listener.begin()) {
        std::fprintf(stderr, "%s\n", listener.lastError().c_str());
        return 1;
    }

    broker.begin(config);
    broker.addListener(listener);

    while (running) broker.poll();

    broker.end();   // every client is told 0x8B Server shutting down
}
```

`examples/cpp/broker/main.cpp` is the same thing with argument handling and a
signal handler, and it builds with the library.

**More than one listener is ordinary.** Plain TCP on 1883 and TLS on 8883 are
two listeners over one broker, and neither the sessions nor the routing can
tell them apart — which is right, because a subscriber on one must receive what
a publisher on the other sends.

---

## Serving TLS

A listener is a listener. `MqttTlsListener` and `MqttTcpListener` are
interchangeable to the broker, so both at once is ordinary — and the sessions
and the routing cannot tell them apart, which is exactly right: a subscriber on
1883 must receive what a publisher on 8883 sends.

```cpp
MqttIdentity identity;
identity.setCommonName("plc-01");
identity.setSubjectAltNames("DNS:plc-01,IP:10.0.0.10");
identity.loadOrGenerate("plc.crt", "plc.key");

MqttTlsListener secure;
secure.configure("0.0.0.0", 8883, 32);
secure.useCertificate(identity.certificatePem().c_str(),
                      identity.privateKeyPem().c_str());
secure.begin();

MqttBroker broker;
broker.begin();
broker.addListener(plain);      // 1883, for what is already on the site
broker.addListener(secure);     // 8883, for everything new
```

### Where the certificate comes from

| the broker holds | how it got there | clients use |
| --- | --- | --- |
| a generated self-signed key | `loadOrGenerate`, zero configuration | the key fingerprint, pinned |
| a real CA-signed certificate | files named on the listener | ordinary validation |
| an enterprise-CA certificate | the same, from the site's own CA | ordinary validation |

**Self-signed is the zero-configuration default, not a ceiling.** The obstacle
was never the engine — it was that a certificate does not fit on a pin, and a
path does. A public certificate is worth having and painful to keep: Let's
Encrypt renews every ninety days and a PLC is a poor host for a renewal agent.
The realistic shapes are a long-lived certificate from the site's own CA, or
the self-signed default.

`identity.fingerprint()` is the number a client pins against. Print it, write it
on the panel, and it stays true — it is a fingerprint of the **key**, so it
survives the certificate being reissued.

### On a board

`MqttBroker` has never been host-only — it carries no platform guard and runs
on an ESP32 today. What kept a board from being a broker at all was that both
host listeners accept BSD sockets, and a board has none. There are now two
that do not.

**Plain, on 1883** — `MqttStreamListener`. This is the ordinary case on an
isolated line: a machine with twenty sensors on it, no route off it, and
nothing between them holding a certificate store or a clock to check an expiry
against.

```cpp
WiFiServer listenSocket(1883);
MqttStreamListener<WiFiServer, WiFiClient> listener(listenSocket);

listener.configure(4);            // four served, plus the reserve that refuses
listener.begin();
broker.addListener(listener);
```

It needs **nothing installed** — no engine, no certificate, no guard around it
— so a board with no TLS at all can still be the broker. The full sketch is
`examples/arduino/Panel_Broker`.

It was also written *second*, which was the wrong order and is worth recording:
a board could serve 8883 before it could serve 1883, because the encrypted half
was the one somebody asked for and the easy half was never noticed missing.

Encrypt the connection that leaves the line rather than the ones on it:
[MqttBridge](bridge.md) is one client, and it is the one that should be
carrying TLS upstream.

**Encrypted, on 8883** — `MqttStreamTlsListener`, the same shape with a TLS
session over each accepted channel:

```cpp
WiFiServer listenSocket(8883);
MqttStreamTlsListener<WiFiServer, WiFiClient> listener(listenSocket);

listener.configure(4);            // four served, plus the reserve that refuses
listener.begin();                 // makes a certificate if it has none
broker.addListener(listener);

Serial.println(listener.fingerprint().c_str());   // what clients pin
```

On a wired panel the two template arguments are `EthernetServer` and
`EthernetClient`; nothing else changes. The full sketch is
`examples/arduino/Secure_Broker`.

**The certificate is made on the board**, on first start, by `MqttIdentity`
using wolfSSL's `wc_MakeCert` — an EC P-256 key, about a second on an ESP32,
where RSA-2048 would be minutes. That is also why wolfSSL is the engine rather
than BearSSL: BearSSL can *read* an X.509 and cannot write one, so a board
using it could never have an identity of its own.

Clients pin the fingerprint rather than validating a chain, and that is the
right way round here: pinning does not expire, and a PLC with no clock cannot
check an expiry date anyway.

**Two things it needs that a host does not.** wolfSSL must be installed
alongside this library — without an engine the class compiles to nothing and
the example says so in one `#warning` rather than fifty errors. And the engine
reaches the network through `MqttByteChannel` rather than a descriptor, because
a `WiFiClient` has no number behind it; that indirection is also what lets the
whole class be instantiated against a test double on a host, so the TLS serving
path is exercised in CI rather than only on hardware.

### One thing a server transport must get right

`MqttLink::peerGone()` is `peerClosed() || !isConnected()`, and
`serviceConnection` turns that straight into
`closeConnection(..., publishWill = true)`. So an accepted TLS transport
reports `isConnected() == true` for the **whole of its handshake**, not from
the moment it completes. A server transport written the way the client one is —
which reports false while handshaking — would be torn down on its first poll,
and would publish a Will for a session that never connected.

This is not a detail of the implementation; it is the reason `test_tls_server`
exists, and the reason one of its checks is "a failed handshake publishes NO
Will".

---

## Configuration

Every limit here is something the broker **tells the client in CONNACK** and
then **enforces**. That pairing is the point: a limit stated and not kept
teaches every client a lie, and a limit kept and not stated leaves them all
guessing.

### Capacity

| | Default | |
| --- | ---: | --- |
| `maxConnections` | 32 | Served at once. The listener's pool must be at least this. |
| `maxSessions` | 64 | Including clients currently away. Larger than `maxConnections` on purpose: offline sessions are the normal state of a plant at 3 a.m. |
| `maxSubscriptionsPerSession` | 32 | |
| `maxQueuedPerSession` | 64 | Messages held for one client. The retained messages a new subscription is sent go a queue's worth at a time, as the client takes them - all of them, however many [MQTT-3.3.1-9]. |
| `maxQueuedBytesPerSession`, `maxQueuedBytesTotal` | 0 (no cap) | The same in bytes - one client's queue, and every client's together. Each queued message is its own copy of topic and payload, and an offline session queues too, so the depth alone does not bound memory. A message past either is refused for that subscriber and counted as dropped. `modbee serve` sets 1 MB and 64 MB. |
| `maxRetained` | 256 | Retained topics. A new one past this is delivered but not kept, and refused with `0x97`. From 1024 up, on a host, topics are found through a hash index rather than a walk. |
| `receiveBufferSize` / `transmitBufferSize` | 8192 | One whole packet each. |
| `reserveUpFront` | true | Reserve every slot and buffer at `begin()`. False reserves a session's slots on first use and starts each connection's buffers at 512 bytes, growing them only when a packet needs it. The limits are unchanged. |

These are the library's defaults. `modbee serve` sets its own, from
`modbee.yaml` or its flags: `max-packet` 65536 (both buffers and the
advertised Maximum Packet Size), `max-retained` 10000, `queue` 64,
`max-session-expiry` 3600 (`maximumSessionExpiry`, below) - and it can
keep retained messages across a restart with `retained: store: file` or
`kafka` ([kafka.md](kafka.md#every-key-and-the-environment)).

Everything is allocated once at `begin()` and never grows, so the memory the
broker will use is knowable before it starts rather than discovered on a busy
afternoon.

**One slot more than you asked for, in three places.** The connection pool and
each listener's pool hold `maxConnections + 1`. The extra one never serves: it
is where a connection arriving at a full broker is accepted, read, and answered
`0x89 Server Busy` — a reason the client can act on. Without it a full broker
simply stopped accepting, and the client saw a TCP connection that never
completed and never explained itself, which is indistinguishable from a broker
that has crashed.

### What a publisher is told

Three answers, and the difference between them matters when something is wrong:

| PUBACK / PUBREC | means |
| --- | --- |
| `0x00` Success | queued for at least one subscriber |
| `0x10` No matching subscribers | nobody is subscribed to that topic — check the spelling |
| `0x97` Quota exceeded | subscribers matched, and **every one of their queues was full**, so the message was dropped |

The third used to be reported as `0x00 Success`. A publisher whose QoS 1
contract says "at least once" got an affirmative acknowledgement for a message
that reached nobody — the one failure a publisher cannot detect or recover
from. `0x97` is also what a retained publish gets when the retained store is
full, because the message was not kept either.

`MqttBroker::publish()`, the application-side call, returns `false` in the same
two cases. Having no subscribers is **not** a failure there: a retained publish
to a topic nobody has asked for yet is the ordinary way to arm a setpoint.

### Waiting on the kernel instead of asking every connection

`poll()` never waits — a PLC calls it once per scan and it must return. On a
host, a standalone broker can additionally hand it a readiness set so that it
reads only the connections that have something to say:

```cpp
MqttPollSet readiness;                  // host only; a board has no epoll to ask
if (readiness.begin()) broker.useReadiness(readiness);

while (running) {
    broker.poll();                      // still never waits
    readiness.wait(20);                 // the daemon sleeps; the library does not
}
```

Optional, and off unless asked for. Without it the broker polls exactly as it
always has, which is what a board does. With it, a broker at 200 clients went
from 41,173 `recvfrom` calls per 20,000 messages — over twenty thousand of them
returning "nothing there" — to 20,818, of which 18 were empty.

**At scale, the persistent set.** `begin()` rebuilds the descriptor list
every pass, which is right for a plant broker and costs O(connections) per
poll at any size. At 21,000 connections that was 81 ms a poll, and the broker
stopped keeping up with arrivals. On Linux, `beginPersistent()` uses epoll
instead:
- Each socket is registered once.
- The broker services only the connections that have work: input, output
  still to drain, a message just queued for them, or a close in progress.
- Keepalive and connect deadlines are swept without a system call, and Will
  delays and session expiry sit on a timer wheel.

```cpp
MqttPollSet readiness;
const bool waiting = readiness.beginPersistent() || readiness.begin();
if (waiting) broker.useReadiness(readiness);

while (running) {
    broker.poll();
    // Not while work is waiting that no socket will announce - a message
    // queued for a connection, say. Sleeping then would add the whole wait
    // to that delivery.
    readiness.wait(broker.hasPendingWork() ? 0 : 20);
}
```

`beginPersistent()` returns false where there is no epoll, and the line above
falls back to `begin()`.

**Measured on one core:**
- 250,000 connections held, with `reserveUpFront = false`.
- 2.6 KB of broker memory per connection.
- QoS 1 answered in 2.6 ms at p50 and 4.0 ms at p99.
- Throughput with 1,000 busy clients unchanged from the rebuilt set, within
  run-to-run noise.

### What CONNACK advertises

| | Default | Enforced with |
| --- | ---: | --- |
| `receiveMaximum` | 32 | `0x93` Receive Maximum exceeded |
| `maximumPacketSize` | 8192 | `0x95` Packet too large |
| `maximumQos` | 2 | `0x9B` QoS not supported; SUBACK never grants above it |
| `retainAvailable` | true | `0x9A` Retain not supported |
| `topicAliasMaximum` | 8 | `0x94` Topic Alias invalid |
| `wildcardSubscriptionAvailable` | true | `0xA2` in the SUBACK |
| `subscriptionIdentifiersAvailable` | true | `0xA1` |
| `sharedSubscriptionAvailable` | always built; set it false to refuse | `0x9E` |
| `serverKeepAliveMaximum` | 0 (no ceiling) | closes at 1.5× |
| `maximumSessionExpiry` | 3600 | stated back when revised |

`maximumPacketSize` is **clamped to `receiveBufferSize`** at `begin()` rather
than trusted. Advertising more than you can hold is a promise broken at the
worst possible moment.

### Server Keep Alive is a ceiling, not an override

```cpp
config.serverKeepAliveMaximum = 60;
```

A client asking for 120 is brought down to 60 and **told**. A client asking for
5 keeps 5 and hears nothing — because [V5:3.2.2.3.14] says the property exists
to warn a client it will be cut off *sooner* than it asked, and this one will
not be.

Replacing the client's value unconditionally is legal, and is also how a broker
ends up holding a wedged device's connection open for a minute when the device
asked to be cut off after seven seconds.

### Policy

```cpp
config.allowAnonymous = false;          // refuse a client with no user name
config.assignClientIdentifiers = true;  // give a zero-length id one
config.closeLingerMs = 250;             // see below
```

`allowAnonymous = false` is enforced **before any hook runs**, so a deployment
that turns anonymous access off is safe even with no hook installed at all. A
policy that depends on somebody remembering to write a callback is not a
policy.

---

## The hooks

```cpp
typedef bool (*MqttBrokerAuthFn)(void* context, const MqttBrokerRequest& request,
                                 uint8_t& reasonCode);
```

Called for every CONNECT, PUBLISH and SUBSCRIBE. Return `false` to refuse;
`reasonCode` starts at `0x87` Not authorized and you may set something more
precise.

```cpp
static bool authorise(void*, const MqttBrokerRequest& request, uint8_t& reason) {
    if (request.action == MqttBrokerAction::Subscribe &&
        mq_spanEquals(request.topic, "safety/commands")) {
        reason = MQ_REASON_NOT_AUTHORIZED;
        return false;
    }
    return true;
}
broker.onAuthorise(authorise, nullptr);
```

`MqttBrokerRequest` carries the client identifier, user name, password, the
topic or filter, the QoS, the retain flag, the protocol version, and
`peerName` — where the connection came from, for an allowlist.

Over TLS with a client CA it also carries `certificateName` and
`certificateFingerprint`: the Common Name and SHA-256 (64 lowercase hex, of
the DER) of a client certificate the handshake **verified** - empty when there
was none. A hook that takes the login from the certificate rather than from
the user name writes it to `*request.login` on a Connect, and the session
carries that name from then on: in every later request, in client events,
and in `disconnectUser()`. With `allowAnonymous` off, a connection with a
verified certificate and no user name reaches the hook instead of being
refused before it - with no hook installed it is still refused.

```cpp
static bool byCertificate(void*, const MqttBrokerRequest& request, uint8_t& reason) {
    if (request.action != MqttBrokerAction::Connect) return true;
    if (request.certificateName[0] == '\0') return false;     // no verified certificate
    if (request.login) *request.login = request.certificateName;
    return true;
}
```

**Its spans are valid only for the call.** Copy anything you keep.

```cpp
typedef void (*MqttBrokerTapFn)(void* context, const MqttSpan& clientId,
                                const MqttSpan& topic, const MqttSpan& payload,
                                uint8_t qos, bool retain);

typedef void (*MqttBrokerConnectionFn)(void* context, const MqttSpan& clientId,
                                       bool connected, uint8_t reasonCode);
```

A tap sees every message after routing — for a recorder, an audit trail, or
your own historian.

---

## Publishing from your own code

```cpp
broker.publish("plant/status", bytes, length, MQ_QOS_0, /*retain=*/true);
```

A broker that could only relay what clients send would be one your OpenPLC
program could not talk to without opening a loopback connection to itself.

Delivery happens on the next `poll()`, not inside the call — so publishing from
inside a tap handler does not re-enter the delivery path.

---

## What it implements

| | |
| --- | --- |
| **Sessions** | per Client Identifier, surviving disconnection, expiring on their interval |
| **Takeover** | a second connection with the same identifier displaces the first, with `0x8E` - when it is the same login. A session belongs to the login that made it: another login naming its identifier is refused (`0x85`, or `0x02` at 3.1.1) while it is connected or when asking to resume it, and with Clean Start on a session nobody is connected to it gets a new one. An authorise hook can let a login stand in for the owner (`MqttBrokerRequest::replaceSession`): the session is then replaced, never resumed |
| **Resumed sessions** | held to the login's rights as they are now: each subscription, and each message queued while it was away, is asked of the authorise hook again; a Will is asked again when it is published |
| **Retained** | stored, replaced, cleared by a zero-length payload, served at subscribe time with RETAIN=1 |
| **Retain Handling** | 0, 1 and 2 |
| **Retain As Published** | per subscription |
| **No Local** | per subscription |
| **Subscription Identifiers** | collected across matching subscriptions, delivered on one copy |
| **QoS 0/1/2** | including exactly-once with method A |
| **Wills** | stored at CONNECT, delayed, cancelled by a reconnect, discarded on a clean DISCONNECT, published on a reset or a keepalive timeout |
| **Keepalive** | closed at 1.5× the interval in force |
| **Flow control** | both directions, with the peer's Receive Maximum honoured |
| **Topic aliases** | inbound always, outbound on request |
| **Shared subscriptions** | `$share/group/filter`, round-robin |
| **Message expiry** | remaining interval recomputed per subscriber |
| **Payload format** | validated when the publisher says it is UTF-8 |

---

## One thing to know about closing

When the broker has finished with a connection it sends the DISCONNECT, shuts
down the write side, and keeps the socket for `closeLingerMs` — reading and
discarding whatever else arrives — before closing.

That is not politeness. A DISCONNECT is the only way a client learns **why** it
was disconnected, and closing the instant after writing one throws it away.

The linger only happens when a DISCONNECT was actually written. At 3.1.1 there
is no server-to-client DISCONNECT at all, so the socket closes immediately and
the slot comes back at once.

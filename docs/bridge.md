# The bridge

Two brokers, a topic mapping, and loop prevention that **refuses rather than
guesses**.

```bash
cmake -S . -B build
```

---

## What it is for

A panel keeps its own broker so the line runs when the site link is down. The
bridge carries the handful of topics head office actually needs upstream, and
brings commands back down.

It is two `MqttClient`s and a mapping. Nothing more, deliberately: a bridge
that was its own protocol implementation would be a third place for the same
bugs to live.

[recipes.md §3](recipes.md#3-a-site-that-keeps-running-when-the-link-drops)
puts it in context: the wrong shape, the right shape, and what each device on
the site has to do differently.

---

## Loop prevention, which is the whole problem

A bridge publishes what it receives. **If what it publishes comes back, it
publishes it again**, and the two brokers spend the afternoon passing one
message between them as fast as the link allows.

This is not theoretical. It is the normal outcome of a bridge built without
thinking about it, and it takes a plant network down.

There are exactly two honest answers.

### At 5.0: No Local

[V5:3.8.3.1]: *"Application Messages MUST NOT be forwarded to a connection with
a ClientID equal to the ClientID of the publishing connection."*

The bridge subscribes with No Local set, so what the bridge published is never
handed back to the bridge. **This is exact.** Nothing is guessed and nothing is
dropped, and a bidirectional rule over one topic space is safe.

### At 3.1.1: disjoint filters, checked

There is no No Local, and there is no other mechanism in the protocol. So the
only way to be sure is that **what the bridge publishes on a side is not
matched by what the bridge subscribes to on that side**.

The bridge works that out for itself, with `mq_filtersOverlap`, and refuses the
configuration if it cannot be sure:

```cpp
MqttResult result = bridge.begin(local, remote, config);
// MqttResult::InvalidArgument
// bridge.lastError():
//   what the bridge publishes on the remote side ("shared/#") is matched by
//   what it subscribes to there ("shared/#"), so every message would come
//   straight back and loop. 3.1.1 has no No Local subscription option, so the
//   two directions need filters that cannot match each other's output - for
//   example readings up under "data/#" and commands down under "cmd/#"
```

### A prefix alone does NOT fix it

This is the obvious first answer, and it is wrong.

A bidirectional rule mapping `panel/` to `site/` publishes into `site/...` on
the remote **and subscribes to `site/...` on the remote**. The message comes
straight back.

What works is two rules whose filters cannot match each other's output:

```cpp
MqttBridgeTopic up;
up.filter = "data/#";
up.direction = MqttBridgeDirection::Out;
up.remotePrefix = "site7/";       // publishes into site7/data/...

MqttBridgeTopic down;
down.filter = "cmd/#";
down.direction = MqttBridgeDirection::In;   // subscribes to cmd/... only
```

That is how a bridge should be written anyway, because it says out loud what is
allowed to travel in each direction.

### Why not a heuristic

The tempting alternative — remember what was just published and drop anything
that looks like it — discards **real** messages. Two identical readings a
second apart are completely ordinary in a plant, and a bridge that silently ate
the second one would be a fault nobody could ever reproduce.

---

## Using it

```cpp
#include <ModBeeMqtt.h>     // the bridge comes with it when the option is on

MqttTcpTransport localLink, remoteLink;
MqttBridge bridge;

int main() {
localLink.configure("127.0.0.1", 1883);
remoteLink.configure("site.example.com", 1883);

MqttBridgeConfig config;
config.name = "panel-7";
config.local.clientId  = "panel-7-bridge-local";
config.remote.clientId = "panel-7-bridge-remote";   // MUST differ
config.local.version = config.remote.version = MqttVersion::V5;
config.behaviour.autoReconnect = true;

MqttBridgeTopic up;
up.filter = "plant/#";
up.direction = MqttBridgeDirection::Out;
up.qos = MQ_QOS_1;
up.remotePrefix = "site7/";
config.topics.push_back(up);

MqttBridgeTopic down;
down.filter = "cmd/#";
down.direction = MqttBridgeDirection::In;
down.qos = MQ_QOS_1;
config.topics.push_back(down);

if (!mq_accepted(bridge.begin(localLink, remoteLink, config))) {
    std::fprintf(stderr, "%s\n", bridge.lastError().c_str());
    return 1;
}
bridge.start();

for (;;) bridge.poll();
}
```

`examples/cpp/bridge/main.cpp` is the same thing with argument handling and a
signal handler, and it builds with the library.

### The two ends need different client identifiers

Enforced, not suggested. [V5:MQTT-3.1.4-3] means a bridge using one identifier
for both ends would take over its own session the moment the two brokers turned
out to be the same box — which happens in more plants than anybody admits.

### MqttBridgeTopic

| | |
| --- | --- |
| `filter` | the topic filter **without** either prefix |
| `direction` | `Out` (local → remote), `In`, or `Both` |
| `qos` | the most this rule will carry; a message crosses at the lower of this and what it arrived at |
| `localPrefix` / `remotePrefix` | what each side actually sees |
| `forwardRetain` | whether a retained message stays retained across. **Off by default**: a retained value outlives the link that carried it, and a stale reading on a head-office dashboard is worse than no reading. **5.0 only** — see below |

### Forwarding retained messages is 5.0 only, and the bridge says so

A broker **clears the RETAIN flag** on an ordinary delivery
[V5:MQTT-3.3.1-9]. So a bridge subscribing normally never sees that a message
was retained — it would forward every one of them unretained and
`forwardRetain` would silently do nothing.

The subscription has to ask for **Retain As Published**
[V5:MQTT-3.3.1-13] to observe the flag at all, and 3.1.1 has no such option.
So `begin()` refuses a `forwardRetain` rule at 3.1.1 with
`MqttResult::NotSupportedByProtocol` rather than accepting it and doing
nothing.

The bridge sets Retain As Published for you on those rules. You do not have to
know about it — but if you are reading the wire and wondering why the bridge's
subscription options differ from the ones you configured, that is why.

---

## State

```cpp
enum class MqttBridgeState { Idle, Connecting, Up, Degraded };
```

**`Degraded` is not `down`**, and saying so matters. One end down means the
panel still works and only what was crossing is lost. An operator told "bridge
down" goes looking for the wrong fault.

```cpp
struct MqttBridgeStatus {
    MqttBridgeState state;
    uint32_t forwardedOut, forwardedIn;
    uint32_t refusedOut, refusedIn;      // counted, never ignored
    uint32_t localConnects, remoteConnects;
    bool localUp, remoteUp;
};
```

A refusal — the far side down, its queue full, a payload larger than it will
accept — is a **number**, because inferring it from missing data on a trend is
not something anybody should have to do.

---

## Getting at the clients

```cpp
MqttClient& localClient();
MqttClient& remoteClient();
```

Public, because a bridge that hid them would force an application to choose
between the bridge and everything else the client can do.

---

## Kafka at one end

A board has no Kafka client, and will not get one: librdkafka is host-only,
and Kafka's own protocol wants a stable network and a big machine. So a board
reaches Kafka the way it reaches anything - **over MQTT, to a server with
the [Kafka pipe](kafka.md)**: the board's bridge carries its topics to that
server, and the pipe's mappings carry them on to Kafka - in the envelope with
`headers: all` - and back. A [connection whose link is
Kafka](kafka.md#kafka-as-a-connections-link) is not reachable from a board:
its broker listens on the server's loopback only.

On a **server**, the server's own topics reach Kafka through the pipe, which
never writes a record back to the topic it read it from. To carry another
broker's topics to Kafka, an application relays two of the server's
connections - one to that broker, subscribed with No Local, and one [over
Kafka](kafka.md#kafka-as-a-connections-link), which never reads back a record
it wrote - and the loop prevention above holds across it. Tested both ways
between two sites sharing one Kafka topic: each message arrives once on the
other site, and none comes back.

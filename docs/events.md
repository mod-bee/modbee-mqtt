# Client events

`modbee-mqtt serve` can publish **what its clients do** - each one that connects,
goes, subscribes or unsubscribes - as small JSON messages on
`$modbee/events/...`. Anything that can read MQTT, or Kafka through the
[pipe](kafka.md), can then follow it.

**Why.** Some facts exist only in the broker. That a device is connected
*right now* is one. Which topics it listens on is another: a device that
subscribes to `plant/pump-07/cmd/SETPOINT` says, in that subscription and
nowhere else, that it accepts a SETPOINT. An application that is not an MQTT
client - one that reads Kafka, say - otherwise has no way to learn either.

---

## Turning it on

```yaml
events:
  enabled: true
```

or `MODBEE_EVENTS_ENABLED=true` in the environment; `enabled: false` keeps
it off with the section in place. Off is the default. `modbee-mqtt serve` says so
when it is on:

```
modbee-mqtt serve: client events published on $modbee/events/...
```

---

## The messages

| Topic | When | Fields |
| --- | --- | --- |
| `$modbee/events/started` | The server has started | `event`, `time` |
| `$modbee/events/connected` | A client has connected | `event`, `clientid`, `username`, `resumed`, `time` |
| `$modbee/events/disconnected` | Its connection has gone | the same, and `reason` |
| `$modbee/events/subscribed` | It was granted a subscription | the same, and `filter`, `qos` |
| `$modbee/events/unsubscribed` | It removed a subscription it had | the same, and `filter` |
| `$modbee/events/publish-refused` | A PUBLISH its rules do not allow was refused | `event`, `clientid`, `username`, `topic`, `reason` (what it was told, e.g. 135 Not authorized), `time` |
| `$modbee/events/subscribe-refused` | A SUBSCRIBE its rules do not allow was refused | the same, with `filter` instead of `topic` |
| `$modbee/events/connect-refused` | The server turned a CONNECT away itself: it is full (`reason` 137 Server busy), or the login already holds all its sessions (151 Quota exceeded) | `event`, `clientid`, `username`, `reason`, `time` |

```json
{"event":"subscribed","clientid":"pump-07","username":"pump-07","filter":"plant/pump-07/cmd/SETPOINT","qos":1,"time":1791278042000}
```

- **`time`** is milliseconds since 1970 (UTC), when the broker saw it.
- **`username`** is the login the client connected with, or `""` without one.
  A device that logged in by its client certificate has the login its
  certificate named, whatever its CONNECT said
  ([logins.md](logins.md#logging-in-with-a-certificate)).
- **`reason`** is why the broker closed the connection, as a decimal number:
  the reason code it sends an MQTT 5 client - `141` (0x8D) when a keep-alive
  ran out, `142` (0x8E) when another connection took the session over, `139`
  (0x8B) when the server shuts down. It is `0` when the client went by itself:
  with a DISCONNECT, or by simply vanishing - a dropped network, a power cut.
- **`qos`** is the QoS the broker **granted**, which may be lower than the one
  asked for.
- **`resumed`** is `true` when the client picked up a session the broker
  still held (CONNACK "session present"), and `false` for a new one - a clean
  start, or a session that had expired. A new session has no subscriptions
  until it makes them again.
- **`started`** is published once each time the server starts, before any
  client can be served. Nobody is connected any more. Without
  `sessions: store: file` every subscription from before it is gone too: a
  reader keeping who is connected and what they listen on starts over from
  here. With it, the sessions are back - a client that returns is
  `resumed: true` and keeps the subscriptions it had. It has no client, so
  through the pipe it is a record with no key.
- **One event per filter.** A SUBSCRIBE with three filters is three
  `subscribed` events. A filter that was refused has none, and an UNSUBSCRIBE
  of a filter the client did not have has none.
- **A subscription that ends with its session has no `unsubscribed` event** -
  a session that expires, or one a client replaces by connecting with a clean
  start. A client that resumes its session keeps its subscriptions without
  sending them again, so there are no new `subscribed` events either. To know
  what a client listens on, keep its subscriptions from its events, start
  over for it when it connects with `resumed: false`, and start over for every
  client at `started`.
- A connection refused before it had a session - a wrong password, say - is
  not an event.

Each event is published at **QoS 1**, not retained, with the client's own id
as its publisher. A client's events are published in the order they
happened.

---

## Reading them over MQTT

An **admin** login may subscribe to `$modbee/events/#`. A device login may
not: no login but an admin may use the broker's own `$` topics
([logins.md](logins.md#what-a-device-may-do)). Without a login database there
are no topic permissions at all, so any client may.

```bash
docker compose exec modbee modbee-mqtt sub -t '$modbee/events/#' -v
```

**A `#` subscription never sees them.** MQTT does not match a topic that
starts with `$` against a filter that starts with a wildcard
[MQTT-4.7.2-1], so the events reach only a subscription that names
`$modbee/events/...` - never a catch-all subscriber, and never a catch-all
Kafka mapping, by accident.

---

## Keeping them in Kafka

An MQTT subscriber that is down misses what happened meanwhile. A Kafka
mapping keeps every event, for the topic's retention time, so an application
that was down reads what it missed:

```yaml
events:
  enabled: true

kafka:
  clusters:
    - id: main
      bootstrap-servers: kafka:9092

  mqtt-to-kafka:
    - id: events
      mqtt-topic-filters: ["$modbee/events/#"]
      kafka-topic: modbee-events
      key: clientid        # one client's events in one partition, in order
```

- **`key: clientid`** keys each record by the client the event is about, so
  all of one client's events land in one partition and are read in the order
  they happened: a `connected` is never read after the `subscribed` that
  followed it.
- **Create the topic first**, with the partitions you want - a topic Kafka
  creates by itself has the cluster's default, and some clusters do not
  create topics at all. The `*-kafka` setups in `docker/examples/` already
  create `modbee-events` and map the events to it.
- A mapping with `["#"]`, like the examples' `messages` one, does not carry them: that is
  the `$` rule above. They need a mapping of their own.

What a record looks like, and how to read one from your own app, is in
[kafka-apps.md](kafka-apps.md).

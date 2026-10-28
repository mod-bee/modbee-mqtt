# The Kafka pipe

`modbee-mqtt serve` can pass MQTT messages to Apache Kafka and Kafka records back to
MQTT, **untouched**. The payload bytes are the record's value. The MQTT topic,
QoS, retain flag and every MQTT 5 property can travel as Kafka headers. A
message that goes MQTT → Kafka → MQTT comes back as the identical message.

There is no transformation, no schema registry and no scripting. Sparkplug B is
MQTT to the pipe: topic and protobuf bytes go through as they are.

```
 MQTT clients ──MQTT──► modbee-mqtt serve ══ mqtt-to-kafka ══► Kafka topics
                        (one broker   ◄═ kafka-to-mqtt ══
                         per core)
```

It needs a build with librdkafka (2.x) installed; `modbee-mqtt serve --help` and the
first line it prints say whether the pipe is in. Everything else is
`modbee.yaml` - or the command line and the Application API, while it runs
([below](#changing-it-while-it-runs)):

```bash
modbee-mqtt check -C modbee.yaml      # every problem, with its line; exit 0 if it would load
modbee-mqtt serve -C modbee.yaml      # flags given as well override the file
```

---

**Writing an app that uses this?** [kafka-apps.md](kafka-apps.md) is the
step-by-step guide: what a record from your devices looks like, how to send
them commands, and a working Node.js app.

## Running it in Docker

The image `ghcr.io/mod-bee/modbee-mqtt` has the pipe built in.
`docker/compose.yml` runs it beside a single Kafka node to try it.
The `*-kafka` folders of [`docker/examples/`](../docker/examples) run modbee
and Kafka together on a server - on your own network, or on the internet behind
Traefik or Nginx Proxy Manager - with nothing to set up but passwords and a
domain name.
[docker.md](docker.md) walks through each, through connecting your own
programs to that Kafka, and through using a Nginx Proxy Manager you already
have.

```bash
docker compose -f docker/compose.yml up -d
modbee-mqtt pub -t plant/tank-01/level -m 4.25 -q 1     # the PUBACK means Kafka has it
```

---

## A complete example

```yaml
broker:
  port: 1883
  shards: 0                # one per core
  max-connections: 200000

metrics:
  port: 9464               # Prometheus: http://host:9464/metrics

kafka:
  clusters:
    - id: main
      bootstrap-servers: [kafka-1:9092, kafka-2:9092]
      tls: { ca: /certs/kafka-ca.pem }
      authentication:
        mechanism: scram-sha512
        username: modbee
        password-file: /run/secrets/kafka-password

  mqtt-to-kafka:
    - id: sparkplug
      mqtt-topic-filters: ["spBv1.0/#"]
      kafka-topic: mqtt-sparkplug
      key: "${MQTT_TOPIC_LEVEL_1}/${MQTT_TOPIC_LEVEL_3}"   # group/node: one node, one partition
      headers: all

    - id: telemetry
      mqtt-topic-filters: ["plant/+/data/#"]
      kafka-topic: mqtt-telemetry

  kafka-to-mqtt:
    - id: commands
      kafka-topics: [mqtt-commands]
      mqtt-topic: "plant/${KAFKA_KEY}/cmd"
      qos: 1
```

With one cluster, a mapping's `cluster:` can be left out.

---

## What an acknowledgement means

This is the part to read before relying on the pipe.

| The publisher sent | It is told | When |
| --- | --- | --- |
| QoS 1 or 2, to a topic a mapping takes | **Success** | Once **Kafka** has acknowledged the record, at the mapping's `kafka-acks`. Not before. |
| … and Kafka refused it, or could not take it within 30 s | **`0x97` Quota exceeded** | When the producer gives up (`message.timeout.ms`, 30 s by default). |
| … and the producer's queue is full | **`0x97`** | At once. A full queue is backpressure, and it is answered, not waited out. |
| … and the record is larger than `max-record-bytes` | **`0x83`** | At once. Never sent, never truncated. |
| QoS 0 | Nothing (QoS 0 has no answer) | The record is still sent, and its outcome counted. |
| QoS 1/2 to a mapping with `acknowledge: routed` | Success | As soon as the record is handed to the producer, without waiting for Kafka. |

- **A Kafka refusal decides the answer**, even when MQTT subscribers received
  the message. Telling the publisher "success" for a message that is missing
  from Kafka would make the loss invisible.
- **`0x10` No matching subscribers is never sent** when a mapping took the
  message. The data reached Kafka; the publisher is told success.
- **MQTT 3.1.1 has no reason codes on PUBACK.** A 3.1.1 publisher is
  acknowledged anyway, and the refusal is only counted in the metrics.
- **While Kafka is slow, publishers slow down.** A held acknowledgement holds
  one of the client's in-flight slots (its Receive Maximum), so a client that
  keeps publishing fills its own window and waits. Nothing is dropped
  silently on the way.

Tested by stopping Kafka for 12 seconds in the middle of a 3,000-message run:
every message acknowledged with success was in Kafka afterwards, exactly once.

Idempotence (`enable.idempotence`) is on for a cluster whenever every one of
its mappings uses `kafka-acks: all`, the default. A retried send is then never
written twice, and retries never reorder a partition. Setting a mapping to
`one` or `zero` turns it off for that cluster.

---

## MQTT → Kafka: the record

- **Value:** the MQTT payload, byte for byte.
- **Key** (`key:`):

  | `key:` | The key | Keeps in order |
  | --- | --- | --- |
  | `topic` (default) | The MQTT topic | Each topic's messages |
  | `clientid` | The publisher's client identifier; no key for a message that came from Kafka | Each device's messages, across all its topics |
  | `none` | No key | Nothing; records spread over partitions |
  | a template | e.g. `"${MQTT_TOPIC_LEVEL_1}/${MQTT_TOPIC_LEVEL_3}"` | Whatever the template groups |

  Templates may use `${MQTT_TOPIC}`, `${MQTT_CLIENTID}` and
  `${MQTT_TOPIC_LEVEL_n}` (levels counted from 0). Partitioning is murmur2,
  the Java client's, so a key lands on the same partition whichever client
  wrote it. `partition-strategy: uniform` ignores the key for partitioning.

  For **one mapping that carries everything** (`mqtt-topic-filters: ["#"]`),
  `clientid` is the one to choose: it keeps the order MQTT itself promises,
  each publisher's messages as it sent them, which `topic` does not once one
  device publishes on several topics. The `docker/examples` ship this way. The
  topic is then read from the `mqtt-topic` header (`headers: all`).

- **Headers** (`headers:`): none by default. `headers: all` writes every one
  below; a list writes the ones named, and `name: value` entries add fixed
  headers.

  | Header | Value |
  | --- | --- |
  | `mqtt-topic` | The topic |
  | `mqtt-qos` | `0`, `1` or `2` |
  | `mqtt-retain` | `true` or `false` |
  | `mqtt-clientid` | The publisher. A Will carries the client id of the session it belonged to. Absent only for a message that came from Kafka, or one the broker publishes itself (its replies to admin and provisioning requests) |
  | `mqtt-username` | The login the publisher connected with, as the broker checked it - so a reader knows whose message it is without trusting the payload or the topic. For a device that logged in by its client certificate it is the login the certificate named ([logins.md](logins.md#logging-in-with-a-certificate)), not whatever user name the CONNECT carried. A Will carries its session's. Absent without a login, and for the same messages as `mqtt-clientid` |
  | `mqtt-content-type` | MQTT 5 Content Type, if present |
  | `mqtt-correlation-data` | MQTT 5 Correlation Data, raw bytes, if present |
  | `mqtt-payload-format-indicator` | `0` or `1`, if present |
  | `mqtt-response-topic` | MQTT 5 Response Topic, if present |
  | `mqtt-message-expiry-interval` | Seconds, if present |
  | `mqtt-user-properties` | Each MQTT 5 user property as a header of its own name, in order, repeats kept. A property whose name starts `mqtt-` is written as `mqtt-user-property-<name>`, so it cannot be mistaken for one of these. |

  The packet identifier, DUP flag, topic alias and subscription identifiers
  are not headers: they belong to one hop of one connection, not to the
  message.

### The envelope: one MQTT message, one Kafka record

`headers: all` is **the envelope**, and it is a contract: every part of modbee
that puts MQTT into Kafka or takes it out - the pipe, a [connection whose link
is Kafka](#kafka-as-a-connections-link), and `modbee-mqtt pub|sub|req --kafka` -
reads and writes exactly this, from one piece of code (`server/kafka/KafkaEnvelope`).

- The value is the payload, untouched: binary, empty, whatever it was.
- The headers are the table above, plus **`mqtt-envelope: 1`**, the version of
  this contract. A connection over Kafka also writes **`mqtt-link`**: its
  consumer group, so it never reads its own records back.
- Every property survives the round trip: correlation data as raw bytes, user
  properties in order with their repeats, a payload format indicator, an
  expiry (what is left of it, as MQTT forwards it), an empty payload, the
  retain flag. Tested in `internal/test/test_kafka_link.cpp`.
- **Versions.** A reader refuses a record whose `mqtt-envelope` is a version
  it does not know - counted as `invalid`, not guessed at. A record with no
  `mqtt-envelope` at all (written by another producer, or before the header
  existed) is read as version 1. The version goes up only when a header's
  meaning changes; adding a header does not change it.
- **Not in it**, deliberately: the packet identifier, DUP, a topic alias,
  subscription identifiers - one hop's, not the message's.

- **Each message is produced once**, by the broker it was published on,
  however many shards there are.

---

## Kafka → MQTT

```yaml
  kafka-to-mqtt:
    - id: commands
      kafka-topics: [mqtt-commands]          # or kafka-topic-pattern: "^plant\\..*"
      mqtt-topic: "plant/${KAFKA_KEY}/cmd"   # or a list: one message per topic
      qos: 1                                 # default 1
      retain: false
      properties:
        content-type: application/json
        user-properties: [{origin: kafka}]
```

- **`mqtt-topic`** defaults to `${KAFKA_TOPIC}`. `${KAFKA_KEY}` is the record's
  key. A record that makes an invalid MQTT topic is skipped and counted
  (`invalid`) - and so is one that makes a topic starting with `$`: those are
  the server's own (`$modbee/admin/request`, `$modbee/events/...`), and
  writing to a Kafka topic must not be a way to send the broker admin
  requests or forge events.
- **`from-headers: true`** takes the topic, QoS, retain flag and properties
  from the `mqtt-*` headers a `headers: all` mapping wrote, and every other
  header becomes a user property. The message republished is the one that was
  produced.
- **Consumer groups:** one per mapping, `modbee-<id>-group` unless
  `consumer-group:` says otherwise. Several `modbee-mqtt serve` processes with the
  same file share the group, so Kafka spreads the partitions between them -
  and each record is then published on **only one** of those servers. A device
  connected to another of them does not receive it: servers do not pass MQTT
  messages between each other (one `modbee-mqtt serve` is one process; see "Not a
  multi-machine cluster" in the [README](../README.md#what-it-is-not)). Give
  each server its own `consumer-group:` for every server to publish every
  record.
- **At-least-once:** a record's offset is stored for commit once the record
  has been handed to the broker. After a crash, a record may be published
  twice; it is never skipped. Handed over is not the same as delivered: a
  record on a topic no session subscribes to reaches nobody and is still
  committed - MQTT keeps nothing for a client that has no session (a retained
  record is kept as the retained message, as it would be from any publisher).
  One the broker could not take - a subscriber's queue, or the retained store,
  was full - is counted as `refused`.
- **A new group starts at the latest offset**, not the beginning of the topic.
  To replay, set `auto.offset.reset: earliest` under the cluster's
  `properties:`.
- **Create a topic before writing to it.** If Kafka creates the topic on its
  first record, modbee joins it after that record, which is then never read.
  The `docker/examples` create their topics before modbee starts.

**No loops.** A message read from Kafka topic T is never written back to T by
an mqtt-to-kafka mapping, even when its filters match. It is still written to
any other topic a mapping sends it to. `modbee-mqtt check` warns when a file has
such an overlap, because it is rarely intended.

---

## Client events

Which devices are connected now, and which topics each one listens on, are
known only to the broker. `events: enabled: true` publishes them - connected,
disconnected, subscribed, unsubscribed, and what was refused - on
`$modbee/events/...`, and a
mapping makes them durable in Kafka:

```yaml
  mqtt-to-kafka:
    - id: events
      mqtt-topic-filters: ["$modbee/events/#"]
      kafka-topic: modbee-events
      key: clientid
```

A `["#"]` mapping does not carry them, because `#` never matches a topic
starting with `$`. [events.md](events.md) has the messages.

---

## Sparkplug B

Births, data, deaths (through Wills), commands and STATE all pass as they
are. With a mapping keyed by publisher - `key: clientid`, as
the `docker/examples` ship - nothing Sparkplug-specific is needed: a node's
NBIRTH, NDATA and DDATA, and its Will (NDEATH), all carry its client id, so
they share a partition and are read in the order they were published, which
keeps Sparkplug's `seq` in order.

With the default `key: topic` they have different topics, can land in
different partitions, and are read out of order.[^sparkplug-key]

[^sparkplug-key]: A template giving group and node,
    `"${MQTT_TOPIC_LEVEL_1}/${MQTT_TOPIC_LEVEL_3}"`, also keeps one node's
    messages together, and does not depend on the node's client id.

---

## Kafka as a connection's link

The pipe joins **this broker** to Kafka. A **connection whose link is Kafka**
is the other half: one of the MQTT connections the server keeps for an
application (`Connections.Put`, [app-api.md](app-api.md#connections-to-other-brokers)) with
Kafka in the middle instead of another broker - so an application, a
Sparkplug host or an edge node that works over an MQTT connection works over
Kafka unchanged.

```
 application ──API──► modbee-mqtt serve
                       └ connection "plant" ──MQTT──► its own broker ══ envelope ══► Kafka topics
                                                      (inside serve,   ◄══ envelope ══
                                                       127.0.0.1 only)
```

Each such connection gets **a broker of its own** inside the server, reached
over loopback with a login made for it at random, and a Kafka pipe of its own
between that broker and the connection's Kafka topics. The connection's MQTT
client connects to it like to any broker, and everything a connection does
works as it does elsewhere: Consume and Publish, store and forward, request
and response, per-filter subscription options, a Sparkplug host or edge node.
Its topics are its own - nothing published on it reaches this server's
clients, and nothing of theirs reaches it.

```bash
modbee-mqtt kafka-clusters put --id main --bootstrap-servers kafka-1:9092
modbee-mqtt connections put --id plant --kafka.cluster main \
    --kafka.kafka-topic plant-up --kafka.kafka-topics plant-down
```

| `kafka.` | |
| --- | --- |
| `cluster` | A cluster from `Kafka.PutCluster` (TLS and login are the cluster's); `""` the only one there is. A cluster a connection uses cannot be deleted. A change to it is taken live. |
| `kafka_topic` | Written to: every message published on the connection, one record each, in the envelope |
| `kafka_topics`, `kafka_topic_pattern` | Read from: each record becomes a message on the connection, as its headers say (a record with none comes on the Kafka topic's own name). A pattern is a regular expression starting with `^`, as Kafka's clients take one |
| `key` | The record key: `topic` (default), `clientid`, `none`, or a template - as for a mapping |
| `consumer_group` | Default `modbee-conn-<id>`. Starts at the latest offset, like a mapping's |
| `state_connection` | An MQTT connection for a Sparkplug host's STATE (below) |

What it means:

- **An acknowledgement means Kafka has it.** A QoS 1 or 2 publish on the
  connection is acknowledged once Kafka has the record (`CONFIRM_DELIVERED`
  says `DELIVERED` then). While Kafka cannot be reached nothing is given up
  on: records wait, in order, and go when it is back. The connection reports
  `BACKING_OFF`, "Kafka cannot be reached", in `Get` and `Watch` meanwhile, and
  a publish without store and forward is answered `NOT_CONNECTED`.
- **Store and forward** works as on any connection, and is what to use for
  messages that must survive a restart: kept on disk until Kafka has them.
  Tested with Kafka stopped and started mid-stream: every message in Kafka
  once, in order.
- **One Kafka topic both ways** is allowed (`kafka_topic` among
  `kafka_topics`): many servers can share one topic as one bus. A connection
  never reads back a record it wrote itself - Kafka has no No Local, so the
  `mqtt-link` header does that job - and a record read from a topic is never
  written back to it. Its own messages reach its own subscriptions once, from
  its broker, as on one MQTT broker.
- **`ConnectionInfo.kafka`**: whether Kafka can be reached, records written
  and refused, records read and refused, the lag (records not read yet,
  every 5 s), publishes waiting for Kafka now.
- **Wills.** A Will is sent when a session ends without a clean disconnect.
  A Kafka connection's session is inside the server, so that does not happen
  while it runs, and **a crash of the whole server sends nothing: Kafka has
  no Will.** What needs to know that a party went away should use the
  Sparkplug STATE below, or its own heartbeat. A Sparkplug edge node on a
  Kafka connection sends its NDEATH into Kafka when it is deleted or the
  server stops cleanly - by hand, as Sparkplug does on a shared connection.
- **Its messages are up to 1 MiB**, Kafka's own default record size, and
  it keeps up to 1,000 retained messages (records with `mqtt-retain: true`).
- **A record that is not an MQTT message is refused and counted**
  (`records_in_invalid`), never handed on: a topic or a string property that
  is not UTF-8 or is longer than MQTT allows - the pipe's mappings too. A
  client given one would drop the connection, and get it again.
- **Each costs about 4 MB of memory** while idle - its broker and two Kafka
  clients (measured on a release build) - so it is a link, not a per-device
  object: tens per server, not thousands.

### A Sparkplug host on Kafka, its STATE on MQTT

A Sparkplug Host Application announces itself with a retained STATE message
whose offline form is its MQTT Will - and Kafka has no Will. So a host whose
data comes through Kafka keeps its STATE on a real MQTT connection: name one
as the Kafka connection's `state_connection` - a connection to the broker the
edge nodes are on, or `this_server` - and the server publishes the host's
online STATE there, with its Will the offline one carrying the same
timestamp [tck-id-payloads-state-birth-payload]. One owner, the server, so
no lock is needed between instances. The STATE topic (`spBv1.0/STATE/#`) is
then not written to Kafka. The state connection is an ordinary MQTT
connection with no Will, host or edge node of its own.

```bash
modbee-mqtt connections put --id state --this-server
modbee-mqtt connections put --id scada-in --kafka.kafka-topics spb-data --kafka.kafka-topic spb-commands \
    --kafka.state-connection state
modbee-mqtt sparkplug-hosts put --id scada --connection scada-in --host-id scada1
```

Its births, data, aliases, `ListNodes` and rebirth requests (NCMD, written to
Kafka) work as on MQTT; tested with an edge node on another Kafka connection.
The host is online while its link runs and the state connection is up; if
the server crashes, the state connection's Will says offline. Without a
`state_connection` the STATE goes into Kafka like any message, with no Will.

An **edge node** on a Kafka connection needs nothing special. One gated on a
primary host (`primary_host_id`) waits for that host's STATE to come in
through Kafka.

### A bridge with Kafka at one end

On a **board**, Kafka is reached the way a board reaches anything: MQTT, to
a server whose pipe carries its topics on to Kafka. A board never has a Kafka
client (librdkafka is host-only), and a connection's own broker listens on the
server's loopback only, see [bridge.md](bridge.md#kafka-at-one-end).

On a **server**, there are two ways, and both keep the loop prevention a
bridge needs. This broker's own topics to and from Kafka are **the pipe**: a
message read from a Kafka topic is never written back to it. Another broker's
topics to and from Kafka are an application **relaying two connections** -
Consume on one, Publish on the other - one of them over Kafka: subscribe the
MQTT side with `no_local`, and on the Kafka side a connection never reads back
a record it wrote. A connection's own broker is not reachable from outside the
server (loopback, a login made at random), so nothing else connects to it.
Tested both ways between two sites sharing one Kafka topic, with the
library's bridge on each site's side of a link: each message arrives once on
the other site, nothing comes back, Kafka holds each once.

### Testing a link from the command line

`pub`, `sub` and `req` take `--kafka <bootstrap>` and talk to Kafka directly,
in the envelope - what a person needs to test the far side of a link:

```bash
modbee-mqtt sub --kafka kafka-1:9092 --kafka-topic plant-up -t 'plant/#' -v      # records, printed as MQTT messages
modbee-mqtt sub --kafka kafka-1:9092 --kafka-topic spb-data --sparkplug          # Sparkplug decoded, JSON lines
modbee-mqtt pub --kafka kafka-1:9092 --kafka-topic plant-down -t plant/cmd -m start -q 1
modbee-mqtt req --kafka kafka-1:9092 --kafka-topic svc-in --kafka-reply-topic svc-out -t svc/time -m now
```

`sub` reads every partition from its end (`--kafka-from-start` from its
start) with no consumer group unless `--kafka-group` names one; `pub` writes
one record per message as the pipe would; `req` reads the reply topic before
it writes the request, and prints the record carrying its correlation data.
TLS and SASL as a cluster takes them: `--kafka-tls`, `--kafka-ca`,
`--kafka-cert`/`--kafka-key-file`, `--kafka-insecure`, `--kafka-sasl
plain|scram-sha256|scram-sha512`, `--kafka-user`, `--kafka-password` or
`--kafka-password-file`, and `--kafka-set name=value` for any librdkafka
setting. A build without librdkafka refuses `--kafka`, saying so.

**Compared with TBMQ.** TBMQ 2.3's Kafka integration goes one way, MQTT to
Kafka; it has no Kafka to MQTT, so nothing like a connection, a Sparkplug
host, request/response or store and forward over Kafka.

---

## Clusters

```yaml
  clusters:
    - id: main
      bootstrap-servers: kafka:9092         # or a list
      tls:                                  # omit for plaintext
        ca: /certs/ca.pem
        cert: /certs/client.pem             # client certificate, optional
        key: /certs/client.key
        verify-hostname: true
      authentication:
        mechanism: scram-sha512             # none | plain | scram-sha256 | scram-sha512 | oauthbearer
        username: modbee
        password-file: /run/secrets/kafka
      properties:                           # passed to librdkafka exactly as written
        linger.ms: 10
        message.timeout.ms: 60000
```

The pipe sets `linger.ms: 5`, `compression.type: lz4` and
`message.timeout.ms: 30000`; `properties:` overrides any of them. Topic-level
librdkafka settings work here too. Over the API (`Kafka.PutCluster`) two
kinds are different: a setting whose value is a secret (a password, a key,
JAAS or OAUTHBEARER configuration) is never given back - an answer has it
empty, and a Put with it empty keeps it - and `plugin.library.paths` and
`ssl.engine.location`, which load a library into the server, are refused:
they belong in the file, which only whoever runs the server writes.

**TLS and SCRAM are tested end to end** against Kafka 4.3 with a SASL_SSL
listener and SCRAM-SHA-512 (`docker/testing/kafka/run-sasl-ssl.sh`):

- `tls.ca` is the CA that signed the Kafka brokers' certificates.
- Host names are verified by default. Each broker's certificate must name
  the host modbee connects to - the bootstrap address and every advertised
  listener - or the handshake fails (`certificate verify failed` in the log).
  `verify-hostname: false` turns this off.
- A wrong password or a certificate that does not verify is never silent:
  the log names it, `/readyz` answers `503 not ready: Kafka cannot be
  reached` within a second, QoS 1 and 2 publishers are answered `0x97` once
  `message.timeout.ms` has passed, and nothing reaches Kafka.
- `properties:` reach both the producing and the consuming side, so one
  that only a consumer knows - `auto.offset.reset` - makes the producer log a
  harmless CONFWARN line.

Kafka's TLS is librdkafka's, which is OpenSSL in the published image,
whichever engine serves MQTT ([docker.md](docker.md#building-the-image-yourself)).

---

## Changing it while it runs

Clusters and mappings can be added, changed and removed while the server
runs - from the command line, the Application API (`Kafka.PutCluster` ...
`Kafka.Status`, [app-api.md](app-api.md#kafka)) or its REST surface - with
**the same fields as this file**, named as it names them, and checked by the
same rules. Each change is all or nothing:

```sh
modbee-mqtt kafka-clusters put --id main --bootstrap-servers kafka-1:9092
modbee-mqtt kafka-mappings put --id telemetry \
    --mqtt-to-kafka.mqtt-topic-filters 'plant/#' --mqtt-to-kafka.kafka-topic telemetry
modbee-mqtt kafka-mappings put --id commands \
    --kafka-to-mqtt.kafka-topics commands --kafka-to-mqtt.mqtt-topic 'cmd/${KAFKA_KEY}'
modbee-mqtt kafka status
```

- **Only what a change touches restarts.** A change to a mapping's MQTT side
  (its filters, key, headers, how it acknowledges) restarts nothing. A
  producer is replaced when its cluster's settings change, or a Kafka topic
  it writes to is asked for with other acks or partitioning; a consumer when
  its cluster, Kafka topics or group change. A producer that is replaced takes
  no more records and lets go only once Kafka has answered every one it holds
  - so **a PUBACK already waiting on Kafka is still sent**, when Kafka
  acknowledges the record, as it would have been.
- **Refused before anything changes:** a setting the file would refuse, a
  mapping on a cluster that does not exist, a librdkafka setting librdkafka
  refuses, a cluster a mapping or the retained store still uses.
- **Kept in the state directory** (`kafka.json` beside `connections.json`),
  so a server started from flags alone keeps its mappings. The file's
  `kafka:` is where they start: once the API has changed them, `kafka.json`
  wins - as EMQX keeps what its API changed over its configuration file - and
  a file edited later does not quietly undo a change made live. Delete
  `kafka.json` to start from the file again.
- **A password is never given back**: an answer says `password_set`, and a
  change with no password keeps the one the cluster has.
- **Counters carry through a change** to a mapping, and start again from zero
  when it is deleted and made anew.

## Every key, and the environment

| Section | Keys |
| --- | --- |
| `broker:` | `address`, `plain`, `port`, `tls: {port, cert, key, client-ca, client-certificates, cert-pem, key-pem, client-ca-pem}` ([below](#tls-for-mqtt)), `shards`, `max-connections`, `max-connect-rate`, `max-sessions`, `max-session-expiry`, `connect-timeout`, `max-keepalive`, `max-packet`, `queue-bytes`, `queue-bytes-total`, `queue`, `subscriptions`, `max-retained`, `pin-threads`, `status-every` |
| `authentication:` | `database`: device logins in SQLite, managed live ([logins.md](logins.md)), with `username`/`password` (or `password-file`) as the admin. Without `database`: `username`/`password` and `users:` are a fixed set of logins. Any login makes logging in required. `certificate-login`: a verified client certificate logs in as the login its CN names ([logins.md](logins.md#logging-in-with-a-certificate)). `default-topics`: `all` (default) or `none` - what a database login with no rules may use ([logins.md](logins.md#what-a-device-may-do)) |
| `authentication: authority:` | Logins decided by your application over the Application API: `timeout-ms` (2000), `stale-for` (86400 s), `cache-size`, `max-pending` ([app-api.md](app-api.md#logins-decided-by-your-application)) |
| `api:` | The Application API: `grpc` (`unix:<path>`, `tcp://<host>:<port>`), `tls: {cert, key, client-ca}`, `token` or `token-file`, `plain` (TCP without TLS where only this machine or a private network reaches it, the token still required), `max-queued` (100000 per consumer group), `state-directory` (`/var/lib/modbee`, where managed connections, and Kafka changes made live, are kept), `rest` (the Admin calls as JSON on the metrics listener; off loopback only with a token) ([app-api.md](app-api.md)) |
| `retained:` | `store` (`memory`, `file` or `kafka`), `file` (default `/var/lib/modbee/retained.db`), `topic` (default `modbee-retained`), `cluster` |
| `sessions:` | `store` (`memory` or `file`), `directory` (default `/var/lib/modbee/sessions`), `sync` (`normal` or `full`) ([docker.md](docker.md#sessions)) |
| `metrics:` | `enabled`, `address`, `port` (default 9464) |
| `events:` | `enabled`: publish what clients do on `$modbee/events/...` ([events.md](events.md)) |
| `sparkplug:` | `aware`: keep every Sparkplug birth on `$sparkplug/certificates/...` ([app-api.md](app-api.md#sparkplug-b)) |
| `kafka.clusters[]` | `id`, `bootstrap-servers`, `tls`, `authentication`, `properties` |
| `kafka.mqtt-to-kafka[]` | `id`, `cluster`, `mqtt-topic-filters`, `kafka-topic`, `kafka-acks` (`zero`, `one`, `all`), `max-record-bytes` (1 MB), `key`, `headers`, `partition-strategy` (`key-based`, `uniform`), `acknowledge` (`kafka`, `routed`) |
| `kafka.kafka-to-mqtt[]` | `id`, `cluster`, `kafka-topics` or `kafka-topic-pattern`, `mqtt-topic`, `from-headers`, `qos`, `retain`, `consumer-group`, `properties: {content-type, response-topic, payload-format-indicator, message-expiry-interval, user-properties}` |

- **`broker: max-session-expiry`** is the longest a disconnected session, and
  the messages queued for it, is kept: in seconds, whatever an MQTT 5 client
  asks for, and for an MQTT 3.1.1 persistent session too; 3600 by default.
  `4294967295` keeps it until the broker restarts, since sessions are held in
  memory - or for good, with `sessions: store: file`.
- **`broker: queue-bytes`** and **`queue-bytes-total`** bound the memory queued
  messages hold - each is its own copy of topic and payload, and a session
  that is offline queues as well: 1 MB for one session, 64 MB for all of them
  together by default (0 = no cap). A message that would go past either is
  not queued for that subscriber, and is counted as dropped, as with a full
  `queue`; a QoS 1 or 2 publisher whose every subscriber refused it hears
  0x97.
- **`broker: connect-timeout`** is how long, in seconds, a connection has to
  send its CONNECT - a TLS handshake included - before it is closed; 10 by
  default. It bounds how long a socket that never signs in holds a slot.
- **`broker: max-keepalive`** is the longest Keep Alive a client may use, in
  seconds; a client asking for more, or for 0, is given this (MQTT 5 tells it
  so in CONNACK). 0, the default, lets every client choose - including never
  being timed out for silence.
- **Any value can come from the environment**, named after its path:
  `kafka.clusters[0].bootstrap-servers` is
  `MODBEE_KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS`. A list of values is one
  variable, comma-separated. The environment can change a mapping in the
  file, not add one.
- **Secrets need never be in the file:** `password-file:` names a file holding
  the password, and so does `MODBEE_..._PASSWORD_FILE` in the environment,
  Docker's convention for secrets.
- **An unknown key is an error**, with the nearest real key suggested. A
  misspelt setting is a setting that is silently not set.

### TLS for MQTT

```yaml
broker:
  tls:
    port: 8883
    cert: /etc/modbee/broker.pem         # the certificate, or its full chain
    key: /etc/modbee/broker.key
    client-ca: /etc/modbee/devices-ca.pem   # optional: verify client certificates
    client-certificates: required        # or optional
```

| Key | |
| --- | --- |
| `port` | 8883 by default |
| `cert`, `key` | PEM files: the certificate (a full chain is presented whole) and its unencrypted key |
| `client-ca` | One or more PEM certificates. Set, client certificates are verified against it |
| `client-certificates` | `required` (the default): every client must present a certificate the CA signed. `optional`: one is asked for and verified when offered - a bad one still fails the handshake - and a client that offers none logs in with a user name and password as usual |
| `cert-pem`, `key-pem`, `client-ca-pem` | The same material as PEM **text**, for the settings database (`config.set`) or the environment. Text wins over a path. A certificate and its key given this way turn the listener on by themselves, with no `tls:` section in the file |

A change to the material - `cert-pem`, `key-pem`, `client-ca-pem` or
`client-certificates` - is applied to the running server at once, with no
restart: new handshakes see it, connections already up carry on
([settings.md](settings.md#the-tls-material-applied-while-it-runs)).
- **With a login database, settings can be stored in it** and changed over
  MQTT - `modbee-mqtt config set broker.max-session-expiry=86400` - applied at
  the next restart; the environment still wins over them
  ([settings.md](settings.md)).

---

## Metrics

`metrics:` (or `--metrics-port`) serves `GET /metrics` in the Prometheus text
format, `GET /healthz` and `GET /readyz`:

| | Answers | Means |
| --- | --- | --- |
| `/healthz` | `200 ok` | **Liveness**: the process answers. Restart it only if this fails |
| `/readyz` | `200 ready`, or `503 not ready: <why>` | **Readiness**: give it clients. Not ready while the broker is not running, or while Kafka cannot be reached |

Kafka holds publishers' acknowledgements, so a server that cannot reach it
cannot finish a QoS 1 or 2 publish - it is alive, and not ready.
`/readyz` turns `503 not ready: Kafka cannot be reached` the moment
librdkafka reports that every broker is down, and back to ready when its
statistics (every 5 seconds, `statistics.interval.ms`) show a broker up again (or Kafka acknowledges a
record). Only producers report this, so a server whose only mappings are
`kafka-to-mqtt` always reads ready; so does one with no Kafka at all.

Besides the broker's own counters
(`modbee_connections`, `modbee_messages_published_total`, …), each mapping has:

| Metric | |
| --- | --- |
| `modbee_kafka_mqtt_to_kafka_send_total{mapping}` | Records handed to the producer |
| `modbee_kafka_mqtt_to_kafka_success_total{mapping}` | Acknowledged by Kafka |
| `modbee_kafka_mqtt_to_kafka_failed_total{mapping}` | Refused by Kafka or the producer |
| `modbee_kafka_mqtt_to_kafka_too_large_total{mapping}` | Over `max-record-bytes` |
| `modbee_kafka_mqtt_to_kafka_latency_seconds_sum{mapping}` | Produce-to-acknowledgement time; divide by `success_total` |
| `modbee_kafka_kafka_to_mqtt_polled_total{mapping}` | Records read |
| `modbee_kafka_kafka_to_mqtt_published_total{mapping}` | MQTT messages published |
| `modbee_kafka_kafka_to_mqtt_invalid_total{mapping}` | Records that made no valid MQTT message |
| `modbee_kafka_kafka_to_mqtt_refused_total{mapping}` | Records the broker could not take: a subscriber's queue was full, or the retained store was full |
| `modbee_kafka_held_acknowledgements` | PUBACK/PUBRECs waiting on Kafka now |

---

## What Kafka keeps

Kafka is not a queue that empties as it is read: each topic is a log on disk,
and a record stays there for the topic's retention time - **seven days** by
default (`log.retention.hours`) - however many consumers have read it. A
consumer, or a new mapping, can start from the beginning.

**Retained messages** can be kept in Kafka too, so they survive a restart of
modbee - `retained: store: kafka`:

```yaml
retained:
  store: kafka
  topic: modbee-retained     # the default; created, compacted, if missing
  cluster: main              # needed only with more than one cluster
```

The topic is *compacted* (`cleanup.policy=compact`): Kafka keeps the newest
record for each key, which is the MQTT topic, so it holds exactly the current
retained set - one record per topic, a cleared topic removed in time. Each
record is the payload, with the QoS and MQTT 5 properties as `mqtt-*` headers,
as the pipe writes them, so anything that reads Kafka can read it. modbee
reads it all back when it starts, before it accepts a client.
[docker.md](docker.md#where-messages-are-kept) compares it with `memory` and
`file`.

## Limits

- **User properties:** a message published from Kafka carries at most the
  library's `MQTT_MAX_USER_PROPERTIES` (8 by default); further ones are
  dropped.
- **Throughput:** measured on one PC (8 shards, a single Kafka broker in a
  container): 400,000 QoS 1 messages a second from 1,000 publishers, every
  PUBACK held for `acks: all`, none refused. That was the highest rate tried,
  not a ceiling that was found.

# The Kafka pipe

`modbee serve` can pass MQTT messages to Apache Kafka and Kafka records back to
MQTT, **untouched**. The payload bytes are the record's value. The MQTT topic,
QoS, retain flag and every MQTT 5 property can travel as Kafka headers. A
message that goes MQTT → Kafka → MQTT comes back as the identical message.

There is no transformation, no schema registry and no scripting. Sparkplug B is
MQTT to the pipe: topic and protobuf bytes go through as they are.

```
 MQTT clients ──MQTT──► modbee serve ══ mqtt-to-kafka ══► Kafka topics
                        (one broker   ◄═ kafka-to-mqtt ══
                         per core)
```

It needs a build with librdkafka (2.x) installed; `modbee serve --help` and the
first line it prints say whether the pipe is in. Everything else is
`modbee.yaml`:

```bash
modbee check -C modbee.yaml      # every problem, with its line; exit 0 if it would load
modbee serve -C modbee.yaml      # flags given as well override the file
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
modbee pub -t plant/tank-01/level -m 4.25 -q 1     # the PUBACK means Kafka has it
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
  `consumer-group:` says otherwise. Several `modbee serve` processes with the
  same file share the group, so Kafka spreads the partitions between them -
  and each record is then published on **only one** of those servers. A device
  connected to another of them does not receive it: servers do not pass MQTT
  messages between each other (one `modbee serve` is one process; see "Not a
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
any other topic a mapping sends it to. `modbee check` warns when a file has
such an overlap, because it is rarely intended.

---

## Client events

Which devices are connected now, and which topics each one listens on, are
known only to the broker. `events: enabled: true` publishes them - connected,
disconnected, subscribed, unsubscribed - on `$modbee/events/...`, and a
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
librdkafka settings work here too.

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

## Every key, and the environment

| Section | Keys |
| --- | --- |
| `broker:` | `address`, `plain`, `port`, `tls: {port, cert, key, client-ca, client-certificates, cert-pem, key-pem, client-ca-pem}` ([below](#tls-for-mqtt)), `shards`, `max-connections`, `max-sessions`, `max-session-expiry`, `connect-timeout`, `max-keepalive`, `max-packet`, `queue-bytes`, `queue-bytes-total`, `queue`, `subscriptions`, `max-retained`, `pin-threads`, `status-every` |
| `authentication:` | `database`: device logins in SQLite, managed live ([logins.md](logins.md)), with `username`/`password` (or `password-file`) as the admin. Without `database`: `username`/`password` and `users:` are a fixed set of logins. Any login makes logging in required. `certificate-login`: a verified client certificate logs in as the login its CN names ([logins.md](logins.md#logging-in-with-a-certificate)). `default-topics`: `all` (default) or `none` - what a database login with no rules may use ([logins.md](logins.md#what-a-device-may-do)) |
| `authentication: authority:` | Logins decided by your application over the Application API: `timeout-ms` (2000), `stale-for` (86400 s), `cache-size`, `max-pending` ([app-api.md](app-api.md#logins-decided-by-your-application)) |
| `api:` | The Application API: `grpc` (`unix:<path>`, `tcp://<host>:<port>`), `tls: {cert, key, client-ca}`, `token-file`, `max-queued` (100000 per consumer group), `state-directory` (`/var/lib/modbee`, where managed connections are kept) ([app-api.md](app-api.md)) |
| `retained:` | `store` (`memory`, `file` or `kafka`), `file` (default `/var/lib/modbee/retained.db`), `topic` (default `modbee-retained`), `cluster` |
| `sessions:` | `store` (`memory` or `file`), `directory` (default `/var/lib/modbee/sessions`), `sync` (`normal` or `full`) ([docker.md](docker.md#sessions)) |
| `metrics:` | `enabled`, `address`, `port` (default 9464) |
| `events:` | `enabled`: publish what clients do on `$modbee/events/...` ([events.md](events.md)) |
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
  MQTT - `modbee config set broker.max-session-expiry=86400` - applied at
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

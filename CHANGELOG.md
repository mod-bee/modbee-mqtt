# Changelog

What changed in each release. The release workflow publishes a version's
section here as its release notes, and refuses a tag without one - so a
change worth knowing about is written here when it is made, under
**Unreleased**, and the heading becomes the version when it ships.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/). Until 1.0.0 a
minor version may change the API or the configuration, and says how here.

## [Unreleased]

A Sparkplug host answers for a list of groups, and only those. `SparkplugHostSpec.group` (one group, or every one) is
now `groups`: the groups it answers for, none for every group. A host never acted on its group, only subscribed to
it: whatever else reached its connection - through `spBv1.0/#` an application's consumer group asked for, or a Kafka
link that carries a whole broker - was tracked, and a node of another group with no birth seen was asked to rebirth.
On a broker several hosts share, that was a command to somebody else's node, paid for on its link. Now a message of a
group the host does not answer for is not its own: never tracked, never asked to rebirth, never handed on with names
(the library's `MqttSparkplugHost::watching()`, with `watchAlso()` / `unwatch()` beside `watch()`). And a host Put
again with only its groups or limits changed keeps its session: a group added is subscribed and its nodes asked as
they speak, a group removed unsubscribed and its nodes no longer followed - no new STATE, no rebirth of the rest. So
ONE host can answer for many groups and take one more without its nodes seeing it go. Proven in `test_connections`
("a host answers only for its groups") and `test_sparkplug_host`.

Changes on upgrade: `PutHost` / `ListHosts` carry `groups` (a list) in place of `group`; on the wire the field is
the same (4), so a single group from an older client reads as a list of one. A state directory an earlier release
wrote is read with its one group.

## [0.1.7]

Two fixes for a Sparkplug host on a Kafka link, found by the platform on 0.1.6 against a site broker.

A Kafka link no longer echoes what it reads. A connection whose link is Kafka wrote EVERY message on its broker to its
out-topic - those its in-topic had just delivered too. The guard only skipped a record going back to the very topic
it came from, and a link reads one topic and writes another (`mqtt-messages` in, `mqtt-commands` out). Joined to a
broker that copies every message it carries into Kafka - its published commands included - each record went round
without end: a Sparkplug host on such a link sent its node's commands back to the node hundreds of times a second,
and the node answered each with a birth. A link's out-mapping now carries only what a client publishes on the
link's broker, never a message read from Kafka (`skipFromKafka`). Proven in `test_kafka_link` ("NO ECHO"): with a
far broker configured as a site runs one, a command reaches the device once, a reading reaches the link once, and
neither Kafka topic grows when nothing is published.

Changes on upgrade: none to configure.

A Sparkplug host on a Kafka link says online only once it can hear. Its online STATE went out as soon as its
session on the link's broker was up, while the link was still joining its consumer group - and a link starts at the
end of its topic. A node waiting for that host answers the online STATE with its birth at once, so the birth went
by unread and the host never learned the node (it showed offline until something else made it birth again). The
online STATE now waits until the link fetches its partitions at a position (`fetch_state` active in librdkafka's
statistics, kept as `AppKafkaLinkStatus.reading`); the Will is armed as before. Proven in `test_kafka_link`: put
in the order a platform puts them (host, STATE connection, link), the host now learns the node from its one birth.

Changes on upgrade: none to configure. A host on a Kafka link comes online up to a statistics interval (5 s) later.

## [0.1.6]

A fix found by the platform on 0.1.5: a server edge node's live values that had not been proven delivered were lost
when the server was killed, or the power cut, before it noticed its link had died. They are now kept on disk and
sent after the restart, as history.

Changes on upgrade: none to configure. `outbox.db` gains a table (an older server ignores it). An edge node proves
delivery every second while values flow (an UNSUBSCRIBE of a filter it never subscribed, about 40 bytes, and its
UNSUBACK); with `MODBEE_SESSIONS_SYNC=full`, each `SetValues` waits for one disk sync.

### Fixed

- **A server edge node's live values lost when the server was killed on a link that had died unnoticed.** Values
  published into a dead link before the keep-alive noticed (up to a keep-alive and a half) were held only in memory,
  the replay window: a SIGKILL or a power cut in that time lost them, and nothing sent them after the restart. The
  window is now written to `outbox.db` as it changes - in the same file, so under the same sync setting
  (`MODBEE_SESSIONS_SYNC=full` survives a power cut) - before `SetValues` answers, and put back when the server
  starts: after the next birth it goes oldest first, `is_historical`, at the times the values were first sent. A
  server stopped on purpose (a service stop, a node replaced) waits up to 2 s for proof that what it holds arrived,
  and keeps what is not proven the same way; it used to let it go.
- **Values that had arrived sent again after a link that died.** Delivery is now proven every second while values
  flow, not every half keep-alive, so after a link dies at most the last second's values may arrive twice; and a
  value sent as history from the outbox is no longer also held by the replay window, which sent it twice
  (`MqttSparkplugNode::replayKeepsHistory(false)`; a node on a board still holds its history by default). The
  window also follows the edge node's `history_max_age`, as the outbox does.

### Added

- `MqttSparkplugNode::replayJournal()`, `replayRestore()`, `replayProveNow()`, `replayKeepsHistory()` and `deviceOf()`: the replay window
  told to a store that outlives the process, and given back from it. A replay window entry is 40 bytes (was 32;
  the docs said 24).

## [0.1.5]

Fixes found by the platform on 0.1.4: a Sparkplug command to something not born is refused instead of sent nameless,
history is no longer lost to a rebirth while it is being sent, a host's STATE no longer goes before its consumers'
subscriptions, and a revoked login's kept allowance goes with it.

Changes on upgrade: `Sparkplug.Write` answers `FAILED_PRECONDITION` (`sent: false`) for a node or device that is not
born now, where it sent the command and said `sent: true`; a Sparkplug id may be 127 bytes on the server, was 47;
`ClientTls.server_name` is now the name checked.

### Added

- **`SparkplugEdgeNodeSpec.rebirth_on_host_online`** (and `MqttSparkplugNode::rebirthOnHostOnline()`): an edge node
  serving several hosts, none of them its primary, is born again (the same session and bdSeq) when a host's STATE
  comes online after its birth - at most once a second, hosts arriving together sharing one. A host that came online
  later saw no birth and, with no data to decode, never asked for one. Off by default; the retained STATE of a host
  already online asks nothing.

### Fixed

- **`Sparkplug.Write` sent a command its edge node could not read, and said it was delivered.** A write to a device
  that had died, or to a node or device this host had no live birth of, went with the old session's alias and no
  name - or, for a device never born, by a name it did not have - and answered `sent: true`. It is now refused,
  `FAILED_PRECONDITION` and `sent: false`, naming the node, device or metric that is not born, so the caller can
  lease or rebirth it and write again. A write's group, node and device are checked as Sparkplug ids first.
- **History lost to a rebirth while it was being sent.** A rebirth - a host's request, `RebirthEdgeNode`, a
  redeclaration, a host coming online - withdraws the data still queued, as Sparkplug requires, history the server
  had already counted as sent among it; two keep-alives later that history left the disk, never sent and not counted
  in `history_lost`. In a test of 3,000 kept values and five rebirths while they went, 320 never arrived. What was
  sent and not yet confirmed now goes again after the new birth, in its order: a value may arrive twice, never not at
  all.
- **A host's STATE birth before its consumers' subscriptions.** On a managed connection, a host following one group
  subscribed that group and published its STATE birth as the session began; the `spBv1.0/#` its consumers' groups
  wanted went after, so the Eclipse Sparkplug TCK's SessionEstablishmentTest failed three assertions ("Namespace topic
  filter is missing"). The groups' filters are now subscribed first in every session, before any host or edge node
  on the connection.
- **"Not a Sparkplug id" did not say why, and the server's limit was a board's.** Group, node, device and host ids
  were refused past 47 bytes - a UUID and a prefix is a natural id, and Sparkplug sets no limit - with a message
  naming only `/`, `+` and `#`. On the server an id may now be 127 bytes (a board's build keeps 47), and every refusal
  - `PutHost`, `PutEdgeNode`, `PutDevice`, `Write`, `Rebirth` - names the id and says why: its length and the limit,
  the character it has, empty, or not UTF-8 (`mq_sparkplugIdProblem()` in the library).
- **`ClientTls.server_name` was not used.** The name checked, and sent as SNI, was always the endpoint's host, so a
  panel reaching its gateway at a site address needed the gateway's certificate to name that address. It is now
  `server_name` when set (`MqttTlsTransport::setExpectedHostname()`), the host when not.
- **`Authority.Revoke` left a kept allowance of a login a decision named.** A decision kept for a CONNECT was filed
  under the CONNECT's user name only, so revoking the login the decision gave the session - a certificate's, say -
  left it to let the next CONNECT in without asking. It is now kept under both, for `Revoke`, `Regrant` and `Forget`.

## [0.1.4]

Every value `Sparkplug.SetValues` is given goes, not only the last of each metric; and the server is several
times faster and holds publishers back instead of dropping their messages - ahead of MonsterMQ Edge and MonsterMQ
in every case of their own fan-out benchmark, on the same machine - and an idle server takes about 40 MB, was 216.

Changes on upgrade: `SparkplugValuesReply.sent` counts only what was published; a full inter-shard ring or a
subscriber that is behind slows its publishers (TCP) rather than losing messages, and `modbee_mailbox_full_total`
counts messages that waited; `serve` builds its sessions and slots as clients arrive (`--reserve-up-front` as before).

### Changed

- **The server holds a publisher back instead of dropping its messages.** Publishers and subscribers on different
  shards dropped most of a fast stream - 40 to 90% in a fan-out benchmark, QoS 1 included, after the publisher had
  been acknowledged - because a shard took up to 4,096 forwarded messages at a time into subscriber queues of 1,000.
  Now a forwarded message waits in its ring while a subscriber it is for is behind but keeping up, a full ring stops
  the publishing shard taking PUBLISHes (acknowledgements and pings still go), and a PUBLISH a subscriber that is
  behind would have to drop is left unread until it has room: TCP slows the publisher. A subscriber that has taken
  nothing for 250 ms is a slow consumer and holds nobody back; its messages are refused at its queue limit, as before.
  `modbee_mailbox_full_total` now counts messages that waited for a ring, not ones dropped.
- **Faster delivery.** A session's queue is no longer walked for each message - to store it, find the next to send,
  give it a packet identifier, find it again for its acknowledgement and look for an owed PUBREL - and up to 256
  messages go to a connection a pass, not 32; each packet decoded resets only its own kind. In a fan-out benchmark
  (N publishers x M subscribers, 64-byte payloads) on 4 shards: QoS 0 4x4 683K -> 1.0-1.3M deliveries a second and 1x16
  720K -> 1.5-1.6M, QoS 1 4x4 367K -> 650K, with nothing dropped where 40-65% was.
- **One write per connection per pass.** Every packet the server sent - each delivery, each PUBACK - was a system
  call of its own. A connection's packets of a pass now go in one write. The same benchmark, 4 CPUs, against
  MonsterMQ Edge 0.1.18 and MonsterMQ (Kotlin) on the same machine and limits, deliveries a second: QoS 0 1x1 1.14M
  (676K, 249K), 4x4 4.5-5.0M (2.79M, 881K), 8x1 1.5-1.6M with none dropped (278K with 88% dropped, 342K), 1x16 4.6-4.9M
  (1.41M, 293K); QoS 1 1x1 300K (190K, 157K), 4x4 1.26-1.45M (435K, 758K).
- **The MQTT client writes once a poll** - every connection the server makes, the Sparkplug edge node and host
  included. The acknowledgements a poll answers with and the publishes it sends go in one write, where each was a
  write of its own: on TLS one record instead of one a packet (about 22 bytes each), and one TCP segment where several
  fit - a gateway sending a DDATA for each of its devices in the same second sends them together.
- **Memory grows with use.** `serve` built every session and connection slot it might ever hold at start - 1.2 kB
  each, about 150 MB for its default 100,000 - and every retained slot (10,000 a shard), and sized its session index
  and routing marks to the limit. They are now built as clients and retained topics arrive, never past the limits: an
  idle `serve` on 4 shards takes about 40 MB, was 216 MB. `--reserve-up-front` builds them all, as before.

### Fixed

- **`Sparkplug.SetValues` sent only the last of several values of one metric.** A gateway that gathers a second of
  values into one call lost every value of a tag that changed more than once in that second - on the host's live view
  and in its history - while the reply counted them all as sent. Each value now goes, in order, with its own
  timestamp, in one NDATA/DDATA where they fit (the same metric more than once, by its alias); the deadband and "the
  same value again is not sent" hold for each against the one before it; the metric's current value, which the next
  birth carries, is the last one given. One that cannot go live at that moment (the replay window first, the link's
  queue full) is kept and sent as history rather than dropped. Not born, every value was already kept.
- **`SparkplugValuesReply.sent` counts what was published**: a value held back by its deadband, or the same as the one
  before it, is no longer counted (it was "set and published, deadbands may hold some back").
- Setting a text metric to a new value could read the text it replaced after freeing it.
- A Sparkplug edge node or host removed while one of its subscriptions was unanswered could still be called after a
  reconnect: the client kept each subscription's completion to reissue it, and handed the removed one back. Forgotten
  too now.

## [0.1.3]

The client sends MQTT 5 topic aliases: on a cellular link a Sparkplug topic was most of every small message.

Defaults that change on upgrade: the server offers 64 topic aliases a connection (was 8).

### Changed

- **The MQTT client uses topic aliases** whenever the broker's CONNACK allows them (Topic Alias Maximum above 0) -
  every connection the server makes (`Connections.Put`, `modbee-mqtt connect`), its Sparkplug edge node and host
  included. The first publish to a topic on a connection carries the topic and an alias, every later one the alias
  alone: a value alone in a message on a Sparkplug topic of 62 bytes is 62 bytes smaller. When every alias the broker
  allows is in use, the one sent least recently is given the new topic. The table starts empty on every connection,
  a resumed session included; a QoS 1 or 2 publish resent after a reconnect carries its full topic; at MQTT 3.1.1
  nothing changes. Nothing to configure, and nothing changes for a broker that offers no aliases.
- **The server offers 64 topic aliases** a connection (`serve --topic-alias-maximum`, `broker: topic-alias-maximum`,
  0 to 256; the library's `MqttBrokerConfig::topicAliasMaximum`), was 8: a gateway sends a data topic for itself and
  one for each device. A host's alias table holds up to 256 (`MQTT_MAX_ALIASES`, a board's 8) and grows only as
  aliases are used, so a connection that uses none costs less memory than before.

### Fixed

- With `useOutboundTopicAliases` on, a broker delivery that had to wait for room in the transmit buffer was sent
  again by its alias alone, though the topic had never gone with it; the subscriber closed the connection with 0x94.
  An alias is now recorded only once the publish carrying it is queued.
- A Sparkplug edge node or host removed from a connection (`DeleteEdgeNode`, `DeleteHost`) while one of its
  subscriptions was still unanswered could crash the server when that connection later closed: the connection's
  client finished the subscription by calling into the removed node. A node or host now has the client forget it
  first (`MqttClient::forgetCompletions`).

## [0.1.2]

A gateway's broker closed to devices listens on nothing.

### Changed

- The server may run with **no listener** when its Application API is on: `serve --no-plain --api ...` (no
  `--tls-port`) starts with none, and `Admin.PutListeners` may turn the last one off and open one again later. A
  gateway keeps its broker closed to devices this way instead of on an unused port. Without the API, no listener is
  still refused (`serve`, and a config file with `plain: false`, no `tls` and no `api`).

## [0.1.1]

Reliability, for cellular links and Sparkplug: dropouts, outages, dead links, a broker restart and a failed DNS on the
cellular rig lose no value; the Sparkplug edge node and host pass the Eclipse Sparkplug TCK and interoperate with
Tahu (Java, JavaScript), Node-RED, Mosquitto, EMQX and HiveMQ; the MQTT client and server were tested against
Mosquitto, EMQX, HiveMQ, NanoMQ and RabbitMQ and the Paho conformance tests. Builds on Windows (MSVC), macOS and
Linux without warnings. The server's Sparkplug edge node is ready for a gateway to run on: a quality with each
value, properties in its births, devices added, changed and removed without a new session, store and forward limited
by count, bytes and age and sent at a set rate with events first, and a watch of its state; its host keeps what every
birth said. Each is a feature `System.Hello` lists.

Defaults that change on upgrade: `serve` holds up to 1000 messages a client (`--queue`, was 64), states a Receive
Maximum of 1024 (`--receive-maximum`) and takes up to 1000 new connections a second (`--max-connect-rate`); a host
socket with nothing acknowledged for 300 s is closed as dead (`TCP_USER_TIMEOUT`); every Sparkplug metric carries its own
timestamp, and a Sparkplug host asks every node to rebirth when its own session comes back.

### Added

- **What a gateway needs of the server's Sparkplug edge node.** Each one is a
  Hello feature, a `modbee-mqtt` command and in [app-api.md](docs/app-api.md#sparkplug-b):
  - **A quality with each value** (`sparkplug.edge.quality`): `SetValues` takes a `Quality` property (192 Good, 0
    Bad, 500 Stale - the codes Sparkplug allows) and publishes it with the value - a change of quality alone is news,
    whatever the deadband - in every data message while it is not Good, with the value in history, in the next birth,
    and across a restart. A read that failed goes Bad and its device stays born. Any other property, `metadata`, or
    another code is `refused`; they were dropped without a word.
  - **Properties in the births** (`sparkplug.edge.properties`): `SparkplugMetricDeclaration.properties` -
    Description, Format, readOnly, keys of your own, any type, as long as they are - beside engUnit, the range and
    Quality.
  - **Devices changed in the session** (`sparkplug.edge.devices`): `Sparkplug.PutDevice` (`modbee-mqtt
    sparkplug-devices put`) adds a device or replaces its metrics with its DDEATH and a new DBIRTH - the same bdSeq,
    new aliases from the node's one counter, values carried over - and `DeleteDevice` sends its DDEATH. `PutEdgeNode`
    again with the same connection, group, node and primary host now changes a running node the same way, and a
    change to its own metrics or templates is a rebirth in the session: it was a new session, an NDEATH and every
    device born again, for any change.
  - **Store and forward's limits** (`sparkplug.edge.history-limits`): `history_max_bytes` and `history_max_age_ms`
    beside `history_max`; the oldest go, samples first, counted in `history_lost`, a line in the log when dropping
    starts, `history_bytes` and `history_dropping` in its info.
  - **History at a set rate** (`sparkplug.edge.history-rate`): `history_rate`, 1000 values a second by default (0:
    as fast as the link takes it); live values never wait behind it. **Events before samples**
    (`sparkplug.edge.history-first`): a declaration's `history_first` drains ahead of the rest.
  - **`Sparkplug.WatchEdgeNodes`** (`sparkplug.edge.watch`, `modbee-mqtt sparkplug-nodes watch`): every change of
    an edge node as it happens - born, its primary host, births, devices online, history.
- **The server's Sparkplug host keeps what the births said** (`sparkplug.host.properties`): `ListNodes` gives each
  metric every property its birth had - engUnit, ranges, Description, Format, readOnly, a node's own keys - and its
  latest `quality` (the birth's, then the last a data message gave), so an application restarted while the host
  stayed up asks no node to be born again.
- **In the library**: `MqttSparkplugNode::qualityAt()` / `qualityOf()`, `MqttSparkplugRecord::quality` (the replay
  window keeps it too), `propertiesAt()` (an encoded PropertySet in the birth, `mq_spEncodedProperties()`), and
  `removeMetrics()`, `removeDevice()`, `removeTemplates()` to declare again in the session;
  `MqttSparkplugTrackedMetric::quality` on the host. On an ESP32 (`Sparkplug_Edge_Node`): 496 bytes more flash,
  the same static RAM; on the heap a pointer more a metric and 8 bytes more a replay-window entry.

- **New connections a second, whole server** (`serve --max-connect-rate`, `broker: max-connect-rate`, default 1000;
  `Admin.PutListeners` `connect_rate`, live; `0` no limit). A reconnect storm - the broker restarted, a tower back
  after an outage, a fleet's power returning - is taken at that rate, a second's worth at once, and the rest wait in
  the kernel's backlog: a slower connect, not a refusal a client would back off from, and TLS handshakes and session
  restores at a pace the server can serve. One budget for every shard; the shards stop watching their listeners while
  it is spent, so nothing spins. As EMQX's `max_conn_rate` and HiveMQ's overload protection.
- **Cellular-grade connections, in the library.** Every address of a host name is raced (RFC 8305: families
  interleaved, 250 ms head start), so a broken IPv6 route or a dead A record costs 250 ms, not the connect timeout.
  **The last good DNS answer** for each name is used when a fresh lookup fails or takes over 1.8 s (RFC 8767) - for
  at most 24 hours (`setStaleAddressLimit()`), a fresh answer always wins and replaces it at once, and a remembered
  address that is refused is forgotten - so a carrier DNS outage does not stop a reconnect, and a server that moves
  is followed. Host sockets get `SO_KEEPALIVE` and `TCP_USER_TIMEOUT` (300 s, `setDeadLinkTimeout()`; 120 s ended live connections on a lossy link).
  `MqttClientConfig::firstConnectSpreadMs` spreads a fleet's first connect after a power cut (default off).
  `MqttDevice::connecting()` / `connectInBackground()`, `MqttConnectTask::settle()`, `MqttTransport::connectWithin()`.
- **Sparkplug B, production-sized.** The host's node and metric tables are hash-indexed on a host (one lookup a
  metric at any plant size - 84.7 s to 0.9 s in the test), with limits (`nodeLimit()`, `metricLimit()`) and counted
  refusals; its decode buffer is sized to each message (`mq_sparkplugMeasure()`) and given back. A sequence window
  tells a late or duplicate message from a gap. Rebirth requests are held to one a node a `rebirthHoldoff()` - on
  the host (5 s) and the node (2 s: a burst of 50 NCMDs made 54 births, now 2). The node's opt-in **replay
  window** (`replayWindow()`) sends again, as `is_historical` after the next birth, values written into a link that
  died without either end noticing (an UNSUBSCRIBE probe proves what was delivered): a 7-hour flaky-link soak lost
  6,772 values before, none now. `MqttSparkplugHistory::save()` / `restore()` keep the ring across a restart.
  `commandLimit()`, `mq_spArrayNext()`, linear-time DataSet reading. Each managed connection has
  `connect_timeout_seconds` (30) and `ack_timeout_seconds` (twice the keep-alive, at least 120) for slow links.

- **Kafka as a connection's link** (`ConnectionSpec.kafka`: `cluster`, `kafka_topic`, `kafka_topics` or
  `kafka_topic_pattern`, `key`, `consumer_group`, `state_connection`): a managed connection with Kafka in the middle
  instead of another broker. Its client talks to a broker of its own inside the server - loopback, a login made for
  it at random - bridged to its Kafka topics by a pipe of its own, so Consume, Publish, store and forward,
  request/response and a Sparkplug host or edge node work on it unchanged, and its topics are not this server's. A
  publish is acknowledged once Kafka has the record, and while Kafka is away nothing is given up: records wait, in
  order (tested: Kafka stopped mid-stream, every message in Kafka once, in order). `BACKING_OFF` "Kafka cannot be
  reached" in `Get` and `Watch` meanwhile; `ConnectionInfo.kafka` says records in and out, refusals, the lag and the
  publishes waiting. One Kafka topic both ways works as one bus: a link never reads back a record it wrote (the
  `mqtt-link` header - Kafka has no No Local). A **Sparkplug host** on it keeps its STATE on the MQTT connection
  `state_connection` names, that connection's Will the offline STATE, since Kafka has no Will; `spBv1.0/STATE/#` is
  then not written to Kafka. A cluster a connection uses cannot be deleted, and a change to it is taken live. About
  4 MB of memory per link, idle. Hello says `connections.kafka`; a build without Kafka answers `FAILED_PRECONDITION`.
- **The envelope, as a contract** (docs/kafka.md): one MQTT message is one Kafka record - the payload untouched, the
  topic, QoS, retain and every MQTT 5 property in `mqtt-*` headers - now with **`mqtt-envelope: 1`** on every
  `headers: all` record, and read by one piece of code everywhere. A record in a version this cannot read is refused
  and counted (`invalid`); one without the header is read as version 1.
- **`modbee-mqtt pub|sub|req --kafka <bootstrap> --kafka-topic T`**: records written and read in the envelope, for
  testing the far side of a link - `sub` prints them as the MQTT messages they carry (`--json`, `--sparkplug`), with
  no consumer group unless `--kafka-group`; `req` reads the reply topic before it writes the request. TLS and SASL
  (`--kafka-tls`, `--kafka-ca`, `--kafka-cert`, `--kafka-sasl scram-sha512`, `--kafka-password-file`, ...) and any
  librdkafka setting (`--kafka-set`).
- **Consumer lag** of each kafka-to-mqtt mapping in `Kafka.Status` (`lag`), from librdkafka's statistics.

- **`Admin.Backup`** (`modbee-mqtt logins backup`, `POST /v1/backups`): a copy of the login database made in its
  backups folder, as `modbee-mqtt backup` does over MQTT, and named; with `contents`, its bytes too. Hello says
  `admin.backup`.
- **Client events turned on and off live** (`Admin.GetEvents` / `PutEvents`, `modbee-mqtt events get|set`,
  `GET`/`PUT /v1/events`): published on `$modbee/events/...` and to Consume streams only while on. Not kept, as
  `PutListeners` is not; `--events` and `events.enabled` still set it at start. Hello says `events.live`.
- **Kafka through the Application API and the CLI, live** (`Kafka.PutCluster` ... `DeleteMapping`, `Kafka.Status`;
  `modbee-mqtt kafka-clusters|kafka-mappings put|get|list|delete`, `modbee-mqtt kafka status`; REST
  `/v1/kafka-clusters`, `/v1/kafka-mappings`, `/v1/kafka-status`): clusters and mappings with the fields of the file's
  `kafka:` section, checked by the same loader, applied all or nothing while the server runs. Only what a change
  touches restarts - a producer when its cluster or a topic's acks or partitioning change, a consumer when its
  cluster, topics or group do - and a replaced producer is let go only once Kafka has answered every record it held,
  so a PUBACK already waiting on Kafka is still sent. Kept in the state directory (`kafka.json`), which wins over the
  file once the API has changed anything, as EMQX does; passwords never come back out. The pipe is now there in
  every build with librdkafka, mappings or not.
- **Sparkplug B arrays, structs and tables in the library** - every metric datatype, on a board too. The thirteen
  array types (`mq_spPackArray` from a program's own C array, `mq_spArrayCount` / `mq_spArrayAt` to read one),
  Templates - a PLC `STRUCT` or UDT: `MqttSparkplugTemplate`, `mq_sparkplugEncodeTemplate` / `DecodeTemplate`, and on
  the edge node `declareTemplate()`, its definition published in every NBIRTH and each value of a Template metric
  checked to be an instance of a declared one (`TemplateReference` otherwise) - DataSets (`mq_sparkplugEncodeDataSet`,
  `MqttSparkplugDataSetReader`), and a property whose value is a PropertySet or a PropertySetList. A value is carried
  as its own wire bytes, so a program that uses none of them links none of it: the ESP32 Sparkplug example grew by
  1.4 kB of flash and 16 bytes of RAM. They were refused by name until now - a choice made in the library's first
  commit, not a limit of the boards. Checked byte for byte against Eclipse Tahu both ways, and by the Sparkplug TCK
  (its template statements included). A PropertySet is now written keys first, then values, as generated encoders
  write it - the same message, now byte-identical with theirs.
- **The server's edge node takes them too**: `SparkplugEdgeNodeSpec.templates` (definitions, kept with the node),
  arrays, DataSets and Template instances in `SetValues`, and `SparkplugValuesReply.refused` ("name: why") for a value
  its metric cannot take. `ListNodes` reports the last value of every metric - text, bytes, arrays, DataSets and
  Templates as well as numbers and bools. Hello says `sparkplug.structures`.
- **`sub --sparkplug`**: Sparkplug B messages printed decoded, as JSON lines - the same codec the Application API
  uses.
- **A Sparkplug B Edge Node in the server** (`Sparkplug.PutEdgeNode` ... `DeleteEdgeNode`, `SetValues`,
  `SetDeviceState`, `RebirthEdgeNode`; `modbee-mqtt sparkplug-nodes ...`): the library's TCK-tested node on one of the
  server's connections - births from what it declares, data as values are set, a primary host waited for - and
  Sparkplug's store and forward: values set while it is not born kept on disk and sent `is_historical`, in order,
  once it is. A host's writes reach the application on the connection, names filled in.
- **Sparkplug Aware** (Sparkplug B 3.0 section 10; `serve --sparkplug-aware`, `sparkplug: aware`, `Sparkplug.PutAware`
  live): every NBIRTH and DBIRTH kept retained on `$sparkplug/certificates/...`, cleared by its death; every login may
  read them, none may write them.
- **A Sparkplug B Host Application in the server** (`Sparkplug.PutHost` ... `DeleteHost`, `ListNodes`, `Rebirth`,
  `Write`; `modbee-mqtt sparkplug-hosts ...`, `sparkplug nodes|rebirth|write`): the library's TCK-tested host on one
  of the server's connections, its STATE that connection's Will. It follows every node's session and asks for
  rebirths with limits (a gap past the reorder window, data with no birth, an unknown alias, a payload that does
  not decode, every node when its own session begins); data delivered on the connection carries the names and
  datatypes the births gave its aliases, and says `stale` (a death of a replaced session) and `gap`. Writes go by
  alias at the birth's datatype, any datatype. Kept across restarts.
- **A connection to this server itself** (`ConnectionSpec.this_server`): over loopback, with a login the server makes
  at each start - what a Sparkplug host or edge node on the server's own devices runs on.
- **Sparkplug B through the Application API, every part of it**: `ConsumeOpen.sparkplug` delivers `spBv1.0/...`
  messages decoded (`Delivery.sparkplug`) - every datatype, arrays, datasets, templates, property sets and their
  lists, metadata, compressed payloads (DEFLATE, GZIP) - `PublishRequest.sparkplug` publishes a typed payload, and
  `Sparkplug.Decode` / `Encode` convert one on their own (`modbee-mqtt sparkplug decode|encode`). The server's codec
  is its own (proto/modbee/sparkplug/v1/wire.proto), typed through; the library's, for boards, carries the same
  datatypes as their wire bytes (above).
- **Store and forward on a managed connection** (`ConnectionSpec.store_and_forward`): every publish on it is kept on
  disk until the other broker acknowledges it - a connection that is down, a restart or a crash loses none, and
  they are sent in order, at QoS 1 or more. `PUBLISH_STORED` while it is down; bounded in messages and bytes, full
  it refuses (0x97) or drops the oldest; expired messages are not sent. `ConnectionInfo` says what it holds.
- **A managed connection's CONNECT, all of MQTT 5's**: the Will's properties (they were accepted and not sent) and
  delay, `receive_maximum`, `topic_alias_maximum`, `maximum_packet_size`; a connection kept across a restart now
  keeps its Will's properties too.
- **`pub`, `sub` and `req` take a Will** (`--will-topic`, `--will-message`, `--will-qos`, `--will-retain`,
  `--will-delay`) **and MQTT 5's connect limits** (`--receive-maximum`, `--topic-alias-maximum`,
  `--maximum-packet-size`).
- **The Application API from the command line, one command for each call**: `modbee-mqtt hello`, `authority
  revoke|regrant|forget`, `messages publish|consume`, `groups list|delete`, `connections
  put|get|list|delete|watch|reconnect`, `logins ...`, `keys ...`, `sessions list|kick`, `retained list|delete`,
  `listeners get|set`, `tls set`, `stats`, `settings get|check|set|apply|reset`. A command's flags are its call's
  request fields, read from the contract compiled into the program (`--client-id-prefix`, `--message.topic`,
  `--set k=v`, `@file`, `--json`), its output is the call's answer as JSON, its errors the call's status. `--api`,
  `--token-file` (or `MODBEE_API`, `MODBEE_API_TOKEN_FILE`). One table in docs/cli.md maps each command to its call
  and REST route, and `cli_api_tests` fails when the commands, the contract and that table disagree.
- **`serve --set path=value`**: any setting by its place in modbee.yaml, as a flag - over the environment, checked
  as the file is - so no setting needs the file.
- **`POST /v1/connections/{id}:reconnect`**, the REST route for `Connections.Reconnect`.
- **`serve` takes everything an application needs at start as flags**, so it needs no file: `--api unix:<path>` /
  `tcp://host:port` (repeatable), `--api-token-file`, `--authority`, `--authority-timeout-ms`, `--events`,
  `--queue-bytes`, `--queue-bytes-total`.
- **Queue caps change live** (`Admin.PutListeners`, `PUT /v1/listeners`): `queue` (messages per session),
  `queue_bytes` (per session) and `queue_bytes_total` (the whole server), beside max connections and max packet, all
  or nothing; `Listeners` says them. A queue already over a lowered cap keeps what it holds and refuses new
  messages, counted as dropped, until it drains below it; raised, a queue grows at once.
- **`Stats` says what is queued now**: `queued_messages` and `queued_bytes`, every session together.
- **The stored settings over the Application API**: `Admin.GetSettings`, `CheckSettings`, `SetSettings`,
  `ApplySettings`, `ResetSettings` (`GET`/`PATCH`/`PUT /v1/settings`, `POST /v1/settings:check`, `:reset`) - what
  `modbee-mqtt config` does over MQTT, wherever a login database keeps them.
- **Per-session counters** (Application API, `Admin.ListSessions`): `messages_in`, `messages_out`, `bytes_in`,
  `bytes_out`, `last_activity_ms` and `connects` for each session, the connection that is up included - so an
  application can say whether a device is talking. They carry over when the same client starts clean while its
  session is held; 48 bytes a session. `ListSessions` now filters by client id and login on each shard before it
  gathers, rather than describing every session on every call.
- **A failed TLS handshake is a client event** (platform builds): `$modbee/events/handshake-failed`, and
  `CLIENT_EVENT_HANDSHAKE_FAILED` over the Application API, with the address and the TLS engine's own words
  (`ClientEvent.detail`) - a client certificate missing or untrusted, a protocol neither end shares, HTTP on the TLS
  port - so an application can see a device that never got as far as CONNECT. A port check that sends nothing is
  not one.
- **Listeners and limits changed live** (Application API, `Admin.PutListeners`, `PUT /v1/listeners`): the plain and
  TLS ports, either listener on or off (TLS turned on with its certificate), max connections and max packet - all or
  nothing, without a restart. A listener that moves keeps its connections; one turned off closes them (`0x98`).
  Max connections goes as far as the room kept for sessions at start (`--max-sessions`; `Listeners.
  max_connections_ceiling`), which costs no memory until used; lowered, it closes nobody. A connection already up
  keeps its packet size. `GetListeners` now also says the limits and the plain listener's address.
- **Subscription options on a managed connection** (Application API): `ConsumeOpen.subscriptions` sets each
  filter's QoS (0 to 2; a filter in `filters` alone stays QoS 1) and MQTT 5's no-local, retain-as-published and
  retain handling, for how the connection subscribes on the other broker. Groups asking for one filter share one
  subscription (the higher QoS); opening a group again with other options re-subscribes with them; a named group
  keeps them across a restart; `GroupInfo.subscriptions` lists them. Additive: an application that sends none sees
  no change.
- **Refusals are client events** (platform builds): `$modbee/events/publish-refused`, `subscribe-refused` and
  `connect-refused` (the server full, or a login at its session limit), and over the Application API as
  `CLIENT_EVENT_PUBLISH_REFUSED` / `_SUBSCRIBE_REFUSED` / `_CONNECT_REFUSED` with the topic or filter and the reason
  code — so an application can say why a device is not getting through. A board build compiles none of it.

### Changed

- **The server's edge node refuses a declaration it would have dropped**: a device id that is not a Sparkplug id or
  is given twice, and a metric declared twice, are `INVALID_ARGUMENT`. Every NCMD and DCMD reaches the application,
  to a metric declared `writable` or not, as before - now said in app-api.md: check `writable` before acting.

### Fixed

- **A kept value of a metric declared again as another datatype stopped the edge node's history for good**: it
  could never be sent and stayed at the head. It is dropped and counted in `history_lost`, as one of a metric
  removed now is (that one was dropped without being counted).

- **The release rebuilt gRPC from source (40 minutes a Linux build) after a few pushes**: CI saved a new 2 GB copy
  of its Windows packages on every run, and GitHub, which keeps 10 GB a repository, dropped the release's Linux
  packages to make room. Each set of packages is now saved once, named by what is in it.

- **The tree builds again on Windows, macOS, and Linux without the Application API**: a variable named `small` (a
  macro in the Windows headers) broke MSVC, an unused field broke clang's `-Werror`, and two names used only by the
  Application API were unused warnings - errors under `-Werror` - in a build without gRPC. On Windows, `req` and the
  Kafka options included `unistd.h`, and `cmd_api.cpp` called protobuf `pb`, a name protobuf itself now declares. With
  protobuf 6 (what vcpkg builds, for the Windows build and the Linux release) `modbee-mqtt api` did not compile: a
  descriptor's name is a `string_view` there, not a `std::string`.

- **MQTT.js (Node-RED's client) was disconnected (0x93) by `serve` when it pipelined QoS 1 publishes**: it does not
  keep to the server's Receive Maximum, and on a server of several shards an acknowledgement waits for the others, so
  a burst of 300 was really in flight against the library's 32. `serve` states 1024 now (`--receive-maximum`,
  `broker: receive-maximum`), still enforced.
- **A message larger than `--queue-bytes` (1 MiB) reached nobody**, idle subscribers included, once `--max-packet` was
  raised past it - a 60,000-row Sparkplug DataSet birth was lost this way. A session holding nothing now takes one
  message whatever its size (still bounded by the max packet and the whole server's `--queue-bytes-total`).
- **One node with a non-UTF-8 metric name broke `Sparkplug.ListNodes` for every node.** Sparkplug strings are proto2
  strings, never checked; the API's are proto3 and must be UTF-8, so the reply could not be sent ("No message
  returned") for as long as that node was known. Names, devices and text values in it now have invalid sequences
  replaced with U+FFFD, as `Consume` and `Sparkplug.Decode` already did.
- **The server's edge node stayed on a server its primary host had left.** On a connection with several servers, a
  valid offline STATE ended the node's session and the connection reconnected to the same server, where the host was
  still offline; the Eclipse Sparkplug TCK (MultipleBrokerTest) failed it. The node now reports the walk
  (`MqttSparkplugNode::serverWalks()`), the connection moves to the next server, and it does not probe back to the
  first while an edge node on it follows a primary host.
- **A connection took packets of at most 64 KiB, so the server's Sparkplug host never received a large birth.** A
  1500-metric NBIRTH (123 kB) was dropped by the other broker for this connection, and the host asked for it again for
  ever. Connections now take up to 16 MiB (`maximum_packet_size` lowers it), with buffers that start at 64 KiB and
  grow only while a large packet needs them.
- **Sparkplug node: a birth larger than the server takes no longer counts as a birth.** At MQTT 5.0 the server states
  its Maximum Packet Size; a larger NBIRTH was queued, dropped by the client, and the node counted itself born and sent
  data no host could name while every host asked for the birth again. It is refused now: the node stays unborn,
  `lastPayloadError()` says `Overflow`, `tooLargeForServer()` counts it. `modbee-mqtt serve` takes 64 kB unless
  `--max-packet` says more; a thousand-metric birth is about 67 kB.
- **Sparkplug host: a birth over 4 kB never arrived.** `MqttSparkplugHost` kept the client's 4 kB receive buffer, so an
  NBIRTH of more than about seventy metrics failed the connection (or, at 5.0, was never sent), the host asked for it
  again for ever and could name none of the node's data. It now takes up to `MQ_SP_HOST_PACKET` (4 MB on a host,
  16 kB on a board; `host.maximumPacket()`), says so as its Maximum Packet Size, and holds the memory only while a
  large message needs it (`MqttClientConfig::initialBufferSize`, new: buffers that start small and grow).
- **On a board, a message over 64 kB went out cut to its last 16 bits of length.** The board build's byte area and its
  views (`MqttByteArea`, `MqttAreaBytes`, `MQTT_LEAN`) kept a length in 16 bits: an NBIRTH of a thousand metrics
  (67,093 bytes) was published as its first 1,557 bytes, which no host could decode, and every rebirth repeated it.
  Lengths are 32-bit now (4 bytes more per queue entry).
- **A Sparkplug edge node or host put on a connection that reconnects only when asked never birthed, and said
  nothing**: putting its Will on the connection ends the session, and that connection waited to be asked. Refused
  now, saying why - and such a connection is not changed to reconnect only when asked while one runs on it.
- **Sparkplug host: after a broker restart, nodes stayed "online" after they died.** Nodes that reconnected before
  the host did birthed unseen; their data read as duplicates and their NDEATH, with a bdSeq the host never saw born, was
  not believed - 157 of 500 nodes in a load test. A primary `MqttSparkplugHost` whose session returns now forgets each
  node's sequence, no longer takes it to be online until it births, and asks it to birth again - on its next message if
  the request could not be queued at once - as the server's host already did.
- **Application API: `Sparkplug.Write` to a metric the birth gave no alias went by alias 0.** A birth may alias some
  metrics and not others (the Eclipse TCK's edge node does); the server took "any alias" to mean "all", so the NCMD
  carried alias 0 and no name. The host now keeps, per metric, whether it had an alias, and such a write goes by name.
- **Application API: `Sparkplug.ListNodes` says what died.** Each node's `offline_at` and `devices_online`, and each
  metric's `stale` and `stale_at` - what the server's host now keeps (above). The Eclipse Sparkplug TCK's Host profile
  passes through the API with them: 128 assertions, none failed.
- **Sparkplug host: birth metrics without an alias were ignored.** Aliases are optional, and Eclipse Tahu's JavaScript
  client (and its Java one by default) never sends them; the host tracked only aliased metrics, so such a node's
  values could not be read and `Sparkplug.Write` refused a write to one ("DATATYPE_UNKNOWN is not a scalar") - the TCK's
  own edge node births `TCK_metric/Int32` without one. They are tracked by name now.
- **Sparkplug host: a death marks the device or node offline and its metrics stale.** The Eclipse Sparkplug TCK
  (Host profile, EdgeSessionTerminationTest) failed five statements: after a DDEATH the device MUST be offline and
  its metrics STALE at the DDEATH's timestamp, after an NDEATH the node, its devices and every metric at the host's
  UTC time. `MqttSparkplugTrackedMetric::stale` / `staleAt`, `MqttSparkplugTrackedNode::offlineAt` and
  `deviceOnline()`; only a birth makes a metric current again.
- **Sparkplug: at MQTT 5.0 an intentional disconnect publishes its NDEATH first.** It sent only DISCONNECT with the
  Will reason code and left the NDEATH to the broker; the Eclipse Sparkplug TCK (SessionTerminationTest) failed it on
  [tck-id-operational-behavior-edge-node-intentional-disconnect-ndeath] - an NDEATH MUST be published before the
  connection ends, at either version. The DISCONNECT still carries 0x04, so the broker publishes the Will as well.
- **Sparkplug: UInt32 is written in `long_value`.** It went in `int_value`, which the specification's table ("uint32")
  allows a reading of, but the Eclipse Sparkplug TCK fails (its value check takes UInt32 from `long_value` only), and
  Eclipse Tahu's Java and C libraries and npm `sparkplug-payload` write it there - the last reads it from nowhere
  else, so our UInt32 metrics arrived as null in Node-RED. Metrics, properties, template parameters and DataSet cells;
  every decoder here still reads either field.
- **Sparkplug: a DataSet from Ignition with a UInt32 column was refused whole.** Eclipse Tahu's Java (Ignition's)
  writes a UInt32 cell in `long_value`; the DataSet reader took `int_value` alone. It takes either now.
- **Sparkplug: every metric carries a timestamp.** The node put a timestamp on the payload only; the Eclipse Sparkplug
  TCK failed it on [tck-id-payloads-name-birth-data-requirement] (the timestamp MUST be on every metric of NBIRTH,
  DBIRTH, NDATA and DDATA). Each metric now carries the message's time - about 7 bytes a metric on the wire.
- **Sparkplug: `Node Control/Rebirth` is born without an alias.** The Eclipse Sparkplug TCK failed the node on
  [tck-id-operational-behavior-data-commands-rebirth-name-aliases]: an NBIRTH MUST NOT give it one, so a host that never
  saw the birth can still ask for a rebirth by name. A request by the old alias (0xFFFFFFFE) is still honoured.
- **`modbee-mqtt sub` could not show a message over 4 KiB** - a Sparkplug birth, often. Its client kept the library's
  4 KiB receive buffer, so at 3.1.1 such a message ended the command ("packet too large") and at 5.0 the broker
  discarded it unseen; `--maximum-packet-size` was stated to the broker without sizing the buffer. `pub`, `sub` and
  `req` now take messages up to 1 MiB, or what `--maximum-packet-size` says.
- **The client lost QoS 2 messages under load, silently.** With more inbound QoS 2 messages awaiting their PUBREL
  than its table holds (`MQTT_SESSION_MAX_INBOUND_QOS2`, 16) - which a broker may send, the client allowing 65,535 in
  flight - each new one was taken for a duplicate: acknowledged with a PUBREC and never delivered. 200 publishers at
  50 msg/s QoS 2 into one subscriber lost 8% against our broker and 6% against Mosquitto. Every one is delivered now;
  only a known identifier is a duplicate. And at 5.0 the client states the table's size as its Receive Maximum
  (255 on a host, 16 on a board; a smaller `receiveMaximum` is kept), so a broker never has more in flight than it can
  tell apart and each stays exactly once.
- **A client with a small receive buffer could be shut out by one large message.** At MQTT 5 the client left Maximum
  Packet Size out of CONNECT unless told one, while its receive buffer (4 KiB by default) is a hard limit: a larger
  PUBLISH ended the connection with `0x95`, and a retained message, or one queued in its session, came again on
  every connect - a device that never stayed up. The client now states its receive buffer as its Maximum Packet
  Size (or `MqttConnectionConfig::maximumPacketSize` if smaller), and the broker discards what is too big for it.
- **A connection displaced by a takeover never had its Will published.** A second CONNECT with the same Client
  Identifier closed the first and discarded its Will, so a device whose dead link the broker only noticed when it came
  back never reported going away. [V5:3.1.4] says the Will is published as for any other close: now it is, unless the
  new connection resumes the session inside the Will Delay - the behaviour of EMQX and HiveMQ, found by the interop
  rig's `will-takeover-matrix` (`docker/testing/interop-mqtt/`).
- **A server edge node's birth carried old values after an outage, and nulls after a restart.** A value set while it
  was not born went only into its history, which a host files and never shows, so the NBIRTH said the value from
  before the outage (265 s stale in the cellular rig) and the host kept showing it. It is now the metric's current
  value as well, and a value set while the node is born but still sending history goes live at once. The bdSeq and
  every current value are kept in the state directory (`sparkplug-node-<hash>.json`), so a server killed and started
  again births with what the application last set - and **its bdSeq moves on from the last session's**: it began at
  zero in every process, so two sessions in a row had the same number and a late Will could kill the new one. The
  library has `MqttSparkplugNode::resumeBirthDeathSequence()` for a board to do the same from RETAIN memory. The
  server's edge node now has a replay window of 4096 values.
- **A burst to one subscriber past 64 messages was dropped.** `modbee-mqtt serve` held 64 messages per session, so a
  subscriber taking in a whole site - a Sparkplug host on `spBv1.0/#` - lost what arrived in a burst: 2,662 of 4,614
  values in one link flap in the cellular rig, 0.28% in the 10K benchmark. The default is now 1000, as Mosquitto,
  EMQX and HiveMQ, and on a host a session's queue grows in blocks of sixteen as a backlog needs them, never moving
  what is queued - so a device that hardly ever receives holds one block (4.5 kB) where it held 64 slots (18 kB).
- **A long transfer over a slow link is no longer ended by the keep-alive and retried for ever**: bytes moving
  either way are signs of life, no PINGREQ waits behind a backlog, and "sent" - for the keep-alive and for the
  acknowledgement deadline - means its last byte has left. A Server Keep Alive of 0 no longer turns off dead-link
  detection, and a long one no longer outlasts NAT timeouts: the client pings at the shorter of the two.
- **A resumed session sent each in-flight QoS 1/2 publish twice** - as the DUP replay and again as a new message.
  Once now.
- **Day 50**: a connection up 49.7 days counted as unstable when it dropped; `MqttDevice::isFresh()` and
  `MqttPoller` called a value silent for 49.7 days fresh; a Sparkplug node's timestamps jumped back 49.7 days.
- **ESP32: a reconnect no longer stalls `loop()`**: `MqttDevice` and the Sparkplug classes open an adopted channel
  on a task of their own (inline if there is no memory for one), bounded by the connect timeout where the channel
  allows (TinyGSM's own default is 75 s); and a channel write is at most 1 KB a call, never into a full socket, at
  most 20 ms a poll - `WiFiClient::write` waited up to 10 s on a dead link. Node and host never retarget the
  transport while a connect is reading the host name. The cellular example no longer sends AT commands while a
  connect is dialling.
- **A host TLS connection that moved to a second address ran its handshake over the socket it had just closed.**
- **Sparkplug host**: repeated DBIRTHs grew its tables for ever (a DBIRTH now replaces its device's metrics); a
  birth with more than four properties on a metric lost the node (the extra properties are counted now); a STATE
  timestamp that overflowed read as 0; a primary host never asked a node it had no birth for; a late or duplicate
  message counted as a gap. Node: command values grew without limit (`TooLarge` past `commandLimit()`), a reused
  property slot kept old data, alias lookup was linear.

- **A Kafka mapping changed while records were being read could crash the server** (use after free): a record
  published into the broker reached the pipe's own offer() on the same thread, which took the new plan there and
  freed the mapping and consumer the record came from. A change is now taken between passes. Tested: 579 changes
  while 50,000 records were read, under AddressSanitizer.
- **A Kafka record that is not an MQTT message is refused and counted** (`invalid`): a topic or a string property
  that is not UTF-8, or longer than MQTT allows, was handed to every subscriber, whose client dropped the connection
  as malformed - and got it again on the next session.
- **Store and forward lost what was on its way at every stop**: up to 64 messages sent and not yet acknowledged were
  deleted when the server stopped or a connection was changed. They are kept. A message the other broker refuses
  with `0x97` quota exceeded is kept and sent again after a pause; one that can never be sent on the connection (a
  retained message to a broker without retain, a QoS it does not offer) is given up and answered, instead of holding
  up everything behind it; a stored row that cannot be read any more is dropped and logged.
- **A managed connection's reconnects**: a broker that accepted and dropped it was asked again every 200 ms, and a
  site's gateways backed off in step. Now as the library's client: full jitter, 1 s doubling to 60 s, started afresh
  only after 30 s up; `0x97` and `0x9F` wait at least a minute. Only a refused login (`0x86`, `0x87`, `0x8A`, `0x8C`)
  is `REFUSED` - and tried again every five minutes, never given up on; `0x9F` connection rate exceeded, `0x97`,
  `0x80` and the rest used to leave a connection `REFUSED` until a person stepped in. Going back to the first server
  after it failed to last checks it half as often each time.
- **A managed connection says the largest packet it takes** (its 64 KiB buffer, 1 MiB over Kafka): the other broker
  could send more than the buffer held, and the connection failed on the same message every session.
- **A connection's id is a name**: letters, digits and `. _ - : @`. An id with `../` wrote its TLS material outside
  the state directory. Its files are removed with it.
- **Kafka clusters over the API**: `plugin.library.paths` and `ssl.engine.location` (which load a library into the
  server) are refused - the file may still set them - and a secret setting (a password, a key, JAAS) is never
  given back.
- **A Sparkplug payload can no longer decode into gigabytes**: 8 KiB of DEFLATE inflated to an eight-million-string
  array. Every decode has a budget of elements, Templates and PropertySets nest at most 16 deep, and invalid UTF-8
  in names and strings becomes U+FFFD (proto3 strings must be UTF-8, or a client's stream breaks).
- **The library's edge node spent a sequence number on an NDATA, DDATA or DDEATH its client refused** (queue full,
  on a slow link): the host saw a gap and asked for a rebirth - a large NBIRTH on the link that was already full.
  Not sent, not spent, as the DBIRTH already was. And **bdSeq moves on with every CONNECT**
  [tck-id-topics-nbirth-bdseq-increment], also after a session that never birthed (waiting for its primary host).
  The Sparkplug TCK edge profile passes, 6 of 6. 84 bytes.
- **The server's edge node deleted its history once it was queued**: sent QoS 0, it was lost if the link dropped
  then. It stays on disk until the node has stayed born for two keep-alives, and goes again after the next birth
  otherwise.
- **Connections.Watch**: a stream that ended while a change was being announced could be called after it was
  freed. The API token is compared in constant time; `sub -v` prints a topic's control characters as `?`.
- **A Kafka link's broker kept room for 100,000 retained messages** (26 MB each, idle): 1,000 now, about 4 MB a link.
  A link stopped is let go on a thread of its own, so the others' keep-alives do not wait for its Kafka. Two links
  cannot share one state connection (each would take its STATE in turn, for ever). A Kafka pipe that failed half way
  through starting left its librdkafka clients writing to a closed descriptor.

- **A Sparkplug host or edge node sharing a connection could be used after it was freed.** Standing down (a server's
  `DeleteHost`, `DeleteEdgeNode`, or a host put again) it published its offline STATE or NDEATH with a completion
  callback pointing at itself, and was then destroyed; the acknowledgement arriving later called into freed memory.
  It shows once the acknowledgement takes longer - over a Kafka link, it waits for Kafka. Nothing waits on it there,
  so it now goes without one. 59 bytes in the library.
- **The Docker image ran the server for the Application API's commands**: `docker run <image> connections list` (and
  `messages`, `kafka-clusters`, ...) started a broker instead, the entrypoint knowing only the older commands. Any
  first word that is not an option is now a command.

- **A Sparkplug host could read another message's text**: `MqttSparkplugHost` kept a String, Text or Bytes value as a
  pointer into its receive buffer, which the next message reuses, so `read()` could return the wrong bytes. It keeps
  its own copy now (`MqttSparkplugTrackedMetric::owned`).
- **A board's Sparkplug text was cut to 126 characters**: a node's copies of a metric's last-published and written
  values were fixed 127-character fields; they are as long as the value now, and only a text metric has them.
- **The server's edge node**: a String it was given kept pointing into the request after the call returned, so a
  rebirth could read freed memory - it keeps its own copy now; and `SetValues` with an unchanged value while born
  kept it as history and later sent it again, `is_historical`.
- A named group kept on disk lost a handshake-failed event's `detail` across a restart.
- **Client events say where a client came from**: `connected` and `connect-refused` carry `address`, and so does
  `ClientEvent.address` over the Application API, which the contract promised for CONNECTED and was always empty.
- **Managed connections say why they cannot connect**: `ConnectionInfo.last_error` reads "cannot connect to plant.local:
  connection refused" (timed out, no route to host, a name that does not resolve, a TLS handshake that failed) — the
  transport's own words, from the socket's error — instead of "could not connect".
- **Application API:** `Messages.Publish` with `CONFIRM_ACCEPTED` now says how many subscribers it matched
  (`matched`) and answers `PUBLISH_NO_SUBSCRIBERS` when none did, as the contract promised; before, it always answered
  `PUBLISH_ACCEPTED` with `matched` 0. The publish is followed across the shards until each has routed it — no
  acknowledgement is awaited.

## [0.1.0]

The first release: the library for boards and hosts, and `modbee-mqtt`, the server
built from it, as downloads for Linux and Windows and as a Docker image.

### The library

- MQTT 5.0 and 3.1.1 in C++14, with nothing that waits on the network: a
  client, a broker and a bridge, the same source on a board and a host.
- `MqttDevice`, typed values, Sparkplug B (an edge node and a host
  application, tested with the Eclipse Sparkplug TCK).
- Boards: ESP32, ESP8266, mbed (Portenta, Opta, Nano 33), RP2040, STM32 - over
  Wi-Fi, Ethernet or cellular, with TLS on each.

### The server

- `modbee-mqtt serve`: one broker per CPU core behind one port, configured with
  one `modbee.yaml`; sessions and retained messages that survive a restart;
  device logins and provisioning.
- The Kafka pipe: MQTT to Kafka and back, messages passed through untouched,
  a PUBACK only once Kafka has the message.
- The Application API, gRPC with a REST admin surface: consumer groups that
  share a device's messages, publishing to devices, connections, sessions,
  retained messages and logins - on a Unix socket, or TCP with a token.
- Linux (x86-64, ARM64) and Windows (x86-64), as a service on both.

### Ready to run

- `.deb` and `.rpm` packages, x86-64 and ARM64.
- Set up with the `modbee-mqtt` command alone, the same on Linux and Windows:
  `modbee-mqtt service install` runs it as a service (systemd, or a Windows
  service) keeping everything in one folder, and `modbee-mqtt admin` sets the
  admin login. No configuration file.
- `modbee-mqtt` for Linux as one static binary that runs on any distribution, and
  `modbee-mqtt.exe` for Windows as one file - each with the broker, the Kafka pipe,
  the Application API and the CLI inside; `modbee-mqtt --version` lists them.
- Each attested by GitHub, with a CycloneDX SBOM, and a checksum list signed
  with Sigstore. [docs/releases.md](https://github.com/mod-bee/modbee-mqtt/blob/main/docs/releases.md)
  shows how to check one.
- The Docker image: `ghcr.io/mod-bee/modbee-mqtt:0.1.0`, amd64 and arm64.

[Unreleased]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.6...HEAD
[0.1.6]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mod-bee/modbee-mqtt/releases/tag/v0.1.0

# Changelog

What changed in each release. The release workflow publishes a version's
section here as its release notes, and refuses a tag without one - so a
change worth knowing about is written here when it is made, under
**Unreleased**, and the heading becomes the version when it ships.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/). Until 1.0.0 a
minor version may change the API or the configuration, and says how here.

## [Unreleased]

### Added

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

### Fixed

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

[Unreleased]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mod-bee/modbee-mqtt/releases/tag/v0.1.0

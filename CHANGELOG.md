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

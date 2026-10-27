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

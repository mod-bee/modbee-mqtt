# Changelog

What changed in each release. The release workflow publishes a version's
section here as its release notes, and refuses a tag without one - so a
change worth knowing about is written here when it is made, under
**Unreleased**, and the heading becomes the version when it ships.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/). Until 1.0.0 a
minor version may change the API or the configuration, and says how here.

## [Unreleased]

## [0.1.0]

The first release: the library for boards and hosts, and `modbee`, the server
built from it, as downloads for Linux and Windows and as a Docker image.

### The library

- MQTT 5.0 and 3.1.1 in C++14, with nothing that waits on the network: a
  client, a broker and a bridge, the same source on a board and a host.
- `MqttDevice`, typed values, Sparkplug B (an edge node and a host
  application, tested with the Eclipse Sparkplug TCK).
- Boards: ESP32, ESP8266, mbed (Portenta, Opta, Nano 33), RP2040, STM32 - over
  Wi-Fi, Ethernet or cellular, with TLS on each.

### The server

- `modbee serve`: one broker per CPU core behind one port, configured with
  one `modbee.yaml`; sessions and retained messages that survive a restart;
  device logins and provisioning.
- The Kafka pipe: MQTT to Kafka and back, messages passed through untouched,
  a PUBACK only once Kafka has the message.
- The Application API, gRPC with a REST admin surface: consumer groups that
  share a device's messages, publishing to devices, connections, sessions,
  retained messages and logins - on a Unix socket, or TCP with a token.
- Linux (x86-64, ARM64) and Windows (x86-64), as a Windows service too.

### Ready to run

- `modbee` for Linux as one static binary that runs on any distribution, and
  `modbee.exe` for Windows as one file - each with the broker, the Kafka pipe,
  the Application API and the CLI inside; `modbee --version` lists them.
- Each signed with Sigstore, attested by GitHub, and listed in a CycloneDX
  SBOM. [docs/releases.md](https://github.com/mod-bee/modbee-mqtt/blob/main/docs/releases.md)
  shows how to check one.
- The Docker image: `ghcr.io/mod-bee/modbee-mqtt:0.1.0`, amd64 and arm64.

[Unreleased]: https://github.com/mod-bee/modbee-mqtt/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mod-bee/modbee-mqtt/releases/tag/v0.1.0

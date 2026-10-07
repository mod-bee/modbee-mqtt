# ModBee MQTT

**MQTT 5.0 and 3.1.1 in C++14: a client, a broker and a bridge. It runs on a
microcontroller and on a Linux server, and nothing in it ever waits on the
network.**

It was written for industrial control. There, a network call that blocks is a
scan cycle that overran, and a message that vanishes quietly is a pump that did
not start. Those two constraints shaped everything else in it.

```
 library:  esp32 · esp8266 · mbed (Portenta, Opta, Nano 33) · rp2040 · stm32 · Linux · macOS
 server:   Linux on x86-64 and ARM64, as a Docker image
```

---

## What is in it

### The library: for a board, or for your own program

`src/`, included with `#include <ModBeeMqtt.h>`. It has no dependencies, and is
the same source on every platform.

| | |
| --- | --- |
| **`MqttDevice`** | The short form: one object, per-topic handlers, an online/offline status topic, a variable published when it changes, and a staleness check |
| **Client** | QoS 0/1/2, sessions, retained messages, wills, keepalive, reconnect with backoff, credentials, topic aliases, flow control, and MQTT 5 properties both ways, including request/response |
| **Broker** | Sessions, retained messages, wills, shared subscriptions, an authorisation hook, and TLS from a certificate it can generate for itself. It runs on a board as well as a host |
| **Bridge** | Two brokers and a topic mapping, with loop prevention |
| **Typed values** | Bind a variable, publish it on change, and read it back with a quality that goes stale on schedule |
| **Sparkplug B** | An edge node and a host application (the SCADA end), conformant to Sparkplug 3.0 and tested with the Eclipse TCK |
| **Transports** | TCP and TLS on a host. On a board, any Arduino `Client`: WiFi, Ethernet, or cellular through TinyGSM, with TLS on all three |

Nothing is behind a build flag. A part your program never uses is code nothing
references, and the linker drops it; [footprint.md](docs/footprint.md) measures
that.

TLS uses whatever is there: wolfSSL or OpenSSL on a host, and
`WiFiClientSecure` or `SSLClient` on a board.

### The server: the broker at scale

`server/`, `cli/` and `docker/`. Linux only, and not part of the Arduino
library.

| | |
| --- | --- |
| **`modbee serve`** | The broker as a server: one broker per CPU core, behind one port. Configured with one `modbee.yaml` |
| **Kafka pipe** | MQTT to Apache Kafka and back, with messages passed through untouched. A publisher's PUBACK comes back only once Kafka has the message |
| **Device logins** | A login per device, added, reset and removed while it runs - or devices that register themselves with a provisioning key - with per-device topic permissions |
| **Retained messages that survive a restart** | Kept in memory as always, and in a file or a compacted Kafka topic as well, with one setting |
| **`modbee` command** | Publish, subscribe, request/response and benchmark from a terminal |
| **Docker image** | `ghcr.io/mod-bee/modbee-mqtt`: the server and the `modbee` command, for amd64 and arm64 |

---

## Quick start

### On a board

Put this repository in your Arduino `libraries/` folder, or use **Sketch →
Include Library → Add .ZIP Library**. This is a complete device: it announces
itself, registers a Will so its absence is noticed, and reconnects by itself.
It publishes the level only when it changes, and holds what you send while
the link is down.

```cpp
#include <Ethernet.h>            // or <WiFi.h>, or <TinyGsmClient.h>
#include <ModBeeMqtt.h>

byte MAC[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0x01 };
const uint8_t PUMP = 5;
float tankLevel = 0.0f;

EthernetClient net;
MqttDevice mqtt(net);

void onCommand(const char*, const MqttPayload& payload) {
    digitalWrite(PUMP, payload.toBool() ? HIGH : LOW);
}

void setup() {
    Ethernet.begin(MAC);

    mqtt.statusTopic("plant/tank-01/status");       // online / offline
    mqtt.onMessage("plant/tank-01/pump", &onCommand);

    // Publish the level when it moves 20 mm, and at least every 30 seconds.
    mqtt.report("plant/tank-01/level", &tankLevel, 0.02, 30000);

    mqtt.connect("192.168.1.10", "tank-01");        // returns immediately
}

void loop() {
    mqtt.poll();                                    // the whole contract
    tankLevel = analogRead(A0) * (10.0f / 1023.0f);
}
```

Only the network lines differ between links:

```cpp
WiFiClient net;             MqttDevice mqtt(net);   // WiFi
EthernetClient net;         MqttDevice mqtt(net);   // wired
TinyGsmClient net(modem);   MqttDevice mqtt(net);   // cellular
WiFiClientSecure net;       MqttDevice mqtt(net);   // TLS, ESP32
SSLClient net(wired, TAs, TAs_NUM, A5);             // TLS where the chip has none
```

New to MQTT? [Getting started](docs/getting-started.md) explains brokers,
topics and QoS before any code.

### In your own C++ program

```cmake
add_subdirectory(modbee-mqtt)
target_link_libraries(my-app PRIVATE modbee-mqtt)
```

The same `MqttDevice` on a host needs no network object:

```cpp
MqttDevice mqtt;
mqtt.secure("/etc/ssl/certs/ca-certificates.crt");
mqtt.connect("broker.example.com", "gateway-1");
```

A broker in your own program:

```cpp
MqttBrokerConfig config;
config.maxConnections = 64;

MqttTcpListener plain;
plain.configure(nullptr, 1883, config.maxConnections);   // address, port, slots
if (!plain.begin()) { /* plain.lastError() says why */ }

MqttBroker broker;
broker.begin(config);                      // allocates everything, once
broker.addListener(plain);

while (running) broker.poll();             // never waits
```

### As a server, in Docker

```bash
docker run -d --name modbee -p 1883:1883 --restart unless-stopped ghcr.io/mod-bee/modbee-mqtt
```

That is an MQTT broker on port 1883. For more, [docker.md](docs/docker.md) has
ready-to-run compose files:

| | |
| --- | --- |
| [`docker/compose.yml`](docker/compose.yml) | modbee and Kafka on this computer, to try the Kafka pipe |
| [`docker/stack/`](docker/stack) | modbee on the internet, with a free Let's Encrypt certificate from Traefik |
| [`docker/all-in-one/`](docker/all-in-one) | The same, with Kafka included, in one file |

Each needs, at most, a domain name, an email address and a password for your
devices, all in one `.env` file. You never handle a certificate file.

Until the image is published, `docker compose up -d --build` builds it from
this checkout, and `docker build -t ghcr.io/mod-bee/modbee-mqtt -f docker/Dockerfile .`
builds it for the `docker run` commands ([docker.md](docs/docker.md)).

### From a terminal

The `modbee` command is in the Docker image:

```bash
docker run --rm ghcr.io/mod-bee/modbee-mqtt pub -h broker.example.com -t test/hello -m hi -q 1
#1 test/hello: delivered to the broker
```

It is also built with this repository on Linux and macOS. It is **not** part of
the Arduino library. [cli.md](docs/cli.md) covers both.

---

## How it behaves

- **Nothing blocks, including `connect()`.** You call `poll()`, and outcomes
  arrive there. That is what lets it live inside a scan cycle or an Arduino
  `loop()`. The one call that can stall is the Arduino core's own
  `Client::connect()`, and [transports.md](docs/transports.md) says what to do
  about it.
- **Nothing is silently dropped.** Every operation ends in exactly one stated
  outcome: sent, refused, superseded or failed. A publish that will not fit is
  refused with a reason before it is accepted. A broker whose subscribers are
  backed up answers `0x97 Quota Exceeded`, not a PUBACK that claims success.
- **One core, every role.** The client, the broker and the bridge share the
  same framing, codecs, sessions and flow control. A plant panel can run its
  own broker, keep working when the site link drops, and bridge the topics
  head office needs.
- **One source, every platform.** The same files build for an ESP32 and for a
  Linux container. Nothing above the transport layer knows which platform it
  is on.
- **The source says why.** Constants, lengths and reason codes cite the clause
  of the MQTT standard that requires them.

---

## The codebase

| | |
| --- | --- |
| `src/` | The library, and the only folder the Arduino IDE compiles. `ModBeeMqtt.h` is the single include |
| `src/broker/`, `bridge/`, `sparkplug/`, `typed/` | The broker, the bridge, Sparkplug B, and typed values |
| `src/codec/`, `src/transport/` | The MQTT 3.1.1 and 5.0 codecs, and TCP, TLS, the Arduino `Client` adapter and the listeners |
| `examples/arduino/` | Fifteen sketches, each a real application |
| `examples/cpp/` | Publish, subscribe, a gateway, a broker, a bridge, a broker serving TLS, and a Sparkplug node and host |
| `server/` | The server tier: one broker per core (`ShardedBroker`), `modbee.yaml`, the Kafka pipe and metrics. Linux |
| `cli/` | The `modbee` command |
| `docker/` | The image (`docker/Dockerfile`) and the compose files |
| `docs/` | Everything below |

The library has five layers, and dependencies point downward only:

```
 L4  ABOVE     MqttDevice, typed values, Sparkplug B    conveniences over the raw API
 L3  ROLE      MqttClient, MqttBroker, MqttBridge       the only layers that know which end they are
 L2  CORE      MqttLink, MqttOperations, MqttSession    framing, the operation queue, what survives a connection
 L1  PROTOCOL  MqttWire, the codecs, MqttProperties     functions over buffers: no I/O, no clock, no allocation
 L0  LINK      MqttTransport, MqttListener              one ordered, reliable byte stream
```

Two rules are checked in CI: no layer below L3 knows which role it is playing,
and no layer above the platform layer knows which platform it is on.
[ARCHITECTURE.md](docs/ARCHITECTURE.md) explains why each boundary is where it
is.

---

## Building

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build
```

That builds the library, the examples and the tests, and, on a host, the
`modbee` command (`build/cli/modbee`) and the server. TLS is used if wolfSSL or
OpenSSL is installed. The Kafka pipe is built if the librdkafka development
package (`librdkafka-dev`) is installed. Neither is required.

When another CMake project adds this one with `add_subdirectory()`, it gets the
library alone.

**C++14 is all the library needs**, because the mbed-based Arduino cores and
OpenPLC's STruC++ compile at C++14. It builds clean with
`-fno-exceptions -fno-rtti -Werror`.

---

## Examples

`examples/arduino/`:

| | |
| --- | --- |
| `First_Sketch` | The shortest thing that works |
| `Tank_Level_Ethernet` | A level on a dashboard, over a wired link |
| `Publish_On_Change` | A variable published when it moves, and not otherwise |
| `Pump_Confirm_Stop` | A command that must be confirmed, read back from the contactor |
| `Remote_Site_Cellular` | An outstation on LTE-M, reconnecting on its own terms |
| `Alarm_On_Silence` | A panel that notices when a device stops speaking |
| `Request_And_Reply` | Ask a device something, and get an answer back |
| `Subscribe_With_Quality` | A reading that goes stale on schedule rather than going quiet |
| `Version_Fallback` | MQTT 5.0 where it is offered, 3.1.1 where it is not |
| `TLS_Client` | An encrypted connection from a board with a secure client |
| `TLS_Over_Ethernet` | TLS on a board whose network chip has none |
| `Panel_Broker` | The board **is** the broker, on port 1883 |
| `Secure_Broker` | The same, serving TLS on 8883 from a certificate it made itself |
| `Sparkplug_Edge_Node` | A Sparkplug B edge node that SCADA finds by itself, with a setpoint SCADA can write |
| `Provisioned_Device` | A device that registers itself with the broker and keeps its own login - nobody types a password into it |

`examples/cpp/`: `publish`, `subscribe`, `gateway`, `broker`, `bridge`,
`secure_broker`, and `sparkplug_node` with `sparkplug_host`, which talk to each
other.

`examples/kafka/nodejs/`: a Node.js app on the Kafka pipe - it reads every MQTT
message from Kafka and sends a pump command back
([kafka-apps.md](docs/kafka-apps.md)).

---

## What it is not

- **Not MQTT-SN.** That is a separate protocol for links without TCP; run an
  MQTT-SN gateway in front of a broker ([versions.md](docs/versions.md#mqtt-sn)).
- **Not a WiFi manager or an OTA updater.** It does MQTT over the network
  connection you already have.
- **Not a multi-machine cluster.** `modbee serve` uses every core of one
  machine. Kafka is how several servers share data.
- **No streaming publish.** A payload is a buffer. A streamed write could block,
  and a QoS 1 message could not be resent after a reconnect
  ([api.md](docs/api.md#what-a-large-payload-costs)).

---

## Documentation

| | |
| --- | --- |
| [getting-started.md](docs/getting-started.md) | **New to MQTT? Start here.** Brokers, topics and QoS, then a working device |
| [recipes.md](docs/recipes.md) | Five real jobs, done properly |
| [transports.md](docs/transports.md) | WiFi, Ethernet, cellular, and TLS on all three |
| [api.md](docs/api.md) | The reference: everything public |
| [broker.md](docs/broker.md) | A broker in your own program |
| [bridge.md](docs/bridge.md) | Two brokers and a mapping |
| [typed-values.md](docs/typed-values.md) | Bind a variable; read it back with a quality |
| [sparkplug.md](docs/sparkplug.md) | Sparkplug B: an edge node, a host application, and what each does for you |
| [versions.md](docs/versions.md) | MQTT 3.1.1 and 5.0: what differs |
| [security.md](docs/security.md) | TLS, and what "it connected" does and does not prove |
| [footprint.md](docs/footprint.md) | Flash and RAM, measured |
| [docker.md](docs/docker.md) | **Running the server**: one command, a stack with a certificate, or that stack with Kafka |
| [kafka.md](docs/kafka.md) | `modbee.yaml` and the Kafka pipe |
| [logins.md](docs/logins.md) | **Device logins**: a login per device, devices that register themselves, permissions, backups |
| [settings.md](docs/settings.md) | The server's settings, changed over MQTT and applied at a restart |
| [events.md](docs/events.md) | What clients do - connect, go, subscribe - as messages, and in Kafka |
| [kafka-apps.md](docs/kafka-apps.md) | **Your own app on Kafka** - reading device data and sending commands, from Node.js, Python or anything else |
| [cli.md](docs/cli.md) | The `modbee` command |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | The five layers, and why |
| [extending.md](docs/extending.md) | Building on it: a PLC wrapper, a new transport, and the design rules |

---

## Licence

GNU Affero General Public License, Version 3, **with additional ModBee terms**
(see [LICENSE](LICENSE)). Use is non-commercial; **commercial use requires a
separate licence**, and the LICENSE says how to ask for one.

To report a security defect, see [SECURITY.md](SECURITY.md).

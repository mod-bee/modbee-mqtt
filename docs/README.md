# Documentation

Everything here is written from the source. **If a number appears in this
documentation, it appears in the code too** — and if the two disagree, the code
is right and the documentation is a bug.

## Start here

| | |
| --- | --- |
| [getting-started.md](getting-started.md) | **New to MQTT? Start here.** What a broker is, what publish and subscribe mean, and then a working device in twenty lines |
| [recipes.md](recipes.md) | Five real jobs, done properly, with the parts that are easy to get half-right called out |
| [transports.md](transports.md) | WiFi, wired Ethernet, cellular, TLS on all three — and the one place something genuinely blocks |
| [api.md](api.md) | The reference. Everything public, in the order you would meet it |

## The other roles

None of these is behind a build flag — they are compiled always, and a program
that never names one does not carry it, because the linker drops a class
nothing references. [footprint.md](footprint.md) measures that rather than
claiming it.

| | |
| --- | --- |
| [broker.md](broker.md) | A broker in your own binary — what it does, what it does not, and how it is configured |
| [bridge.md](bridge.md) | Two brokers and a mapping, with loop prevention that refuses rather than guesses |
| [typed-values.md](typed-values.md) | Bind a variable; read it back with a quality that goes stale on schedule |
| [sparkplug.md](sparkplug.md) | Sparkplug B: an edge node SCADA finds by itself, and a host application to watch and command nodes |
| [footprint.md](footprint.md) | What each option costs, in flash and in RAM |

## Running a server

The broker as a Linux server, one broker per CPU core, with a pass-through
pipe to Apache Kafka. Published as a Docker image; none of it is part of the
Arduino library.

| | |
| --- | --- |
| [docker.md](docker.md) | **Running it**: one command, a stack with a Let's Encrypt certificate, or that stack with Kafka included |
| [kafka.md](kafka.md) | `modbee.yaml`, and the Kafka pipe: MQTT to Kafka and back, untouched, and exactly what an acknowledgement means |
| [kafka-apps.md](kafka-apps.md) | **Your own app on Kafka**: read your devices' MQTT messages and send them commands, from Node.js, Python or any language - with a working example |
| [logins.md](logins.md) | **Device logins**: a login per device, managed live; devices that register themselves with a key; what each may publish and subscribe to; backups |
| [cli.md](cli.md) | The `modbee` command: publish, subscribe, request/response, benchmark, serve, and manage logins. In the Docker image; built from source on a computer |

## Understanding it

| | |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | The five layers, and why each boundary is where it is |
| [versions.md](versions.md) | 3.1.1 and 5.0: what differs, and the traps |
| [security.md](security.md) | TLS, and what "it connected" does and does not prove |
| [extending.md](extending.md) | Building a Sparkplug layer, a PLC wrapper, or a transport on this |

## Working examples

Real applications rather than API demonstrations.

| | |
| --- | --- |
| [`First_Sketch`](../examples/arduino/First_Sketch) | the shortest thing that works |
| [`Tank_Level_Ethernet`](../examples/arduino/Tank_Level_Ethernet) | a level on a dashboard, over a **wired** link |
| [`Publish_On_Change`](../examples/arduino/Publish_On_Change) | a variable published when it moves, and not otherwise |
| [`Pump_Confirm_Stop`](../examples/arduino/Pump_Confirm_Stop) | a command that must be confirmed, read back from the contactor |
| [`Remote_Site_Cellular`](../examples/arduino/Remote_Site_Cellular) | an outstation on **cellular**, reconnecting on its own terms |
| [`Alarm_On_Silence`](../examples/arduino/Alarm_On_Silence) | a panel that notices when a device stops speaking |
| [`Request_And_Reply`](../examples/arduino/Request_And_Reply) | ask a device something, and get an answer back |
| [`Subscribe_With_Quality`](../examples/arduino/Subscribe_With_Quality) | a reading that goes stale on schedule rather than going quiet |
| [`Version_Fallback`](../examples/arduino/Version_Fallback) | 5.0 where it is offered, 3.1.1 where it is not |
| [`TLS_Client`](../examples/arduino/TLS_Client) | an encrypted connection from a board with a secure client |
| [`TLS_Over_Ethernet`](../examples/arduino/TLS_Over_Ethernet) | TLS on a board whose network chip has none |
| [`Panel_Broker`](../examples/arduino/Panel_Broker) | the board **is** the broker, on 1883, with nothing installed |
| [`Secure_Broker`](../examples/arduino/Secure_Broker) | the same, serving TLS from a certificate it made itself |
| [`Sparkplug_Edge_Node`](../examples/arduino/Sparkplug_Edge_Node) | a **Sparkplug B** edge node, with a setpoint SCADA can write |
| [`Provisioned_Device`](../examples/arduino/Provisioned_Device) | a device that **registers itself** with the broker and keeps its own login |
| [`examples/cpp/gateway`](../examples/cpp/gateway) | the same class on a host: a rule that will not act on a stale reading |

`TLS_Client`, `Panel_Broker` and `Secure_Broker` are written in the **full
form** — `MqttClient` or `MqttBroker` plus a transport — and each says so at
the top. They show what the short form is built on, and in the broker's case
there is no short form to use.

Everything here is about the library: what it does, how to use it, what it
costs and what it will not do. How it is built and proven to ourselves is not
published, and is not something using it requires.

---

## If you read one thing

**Nothing in this library blocks**, `connect()` included. You call `poll()` and
outcomes arrive there. Everything else in the design follows from that, because
an OpenPLC scan cycle and an Arduino `loop()` are the same constraint: a call
that waits for a network round trip cannot live inside either.

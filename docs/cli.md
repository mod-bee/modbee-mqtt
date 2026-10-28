# The `modbee-mqtt` command-line tool

One binary that publishes, subscribes, benchmarks and runs the broker, built
on this library's own client. It reports what the **broker said** about every
operation, not just that something was sent.

## Getting it

| Where you are | The `modbee-mqtt` command |
| --- | --- |
| **The Docker image** | **Included.** Nothing to install - see below |
| **This repository, built on Linux or macOS** | Built by default, as `build/cli/modbee-mqtt` |
| **The Arduino library** | **Not included.** It is a computer program, not part of what a board runs |
| **Your own CMake project** that adds this library | Not built - your project gets the library alone. `-DMQTT_BUILD_CLI=ON` builds it |

**From the Docker image**, with nothing installed:

```bash
docker run --rm ghcr.io/mod-bee/modbee-mqtt pub -h broker.example.com -t test -m hi -q 1
docker run --rm -it ghcr.io/mod-bee/modbee-mqtt sub -h broker.example.com -t 'test/#' -v
```

Given a command, the image runs `modbee-mqtt <command>`; given none, it runs the
broker. Inside a running compose stack, `docker compose exec modbee modbee-mqtt ...`
reaches that stack's broker on `localhost`. To reach a broker on your own
machine from `docker run`, add `--network host`.

**From source:**

```bash
cmake -S . -B build && cmake --build build -j
./build/cli/modbee-mqtt --help
sudo cmake --install build          # optional: puts modbee-mqtt in /usr/local/bin
```

On Linux every command is built; on Windows every one but `bench`
([windows.md](windows.md), with `modbee-mqtt service` to run `serve` as a Windows
service). On macOS, `pub`, `sub` and `req` are, and `bench`, `serve` and
`check` are not.
`serve` includes the [Kafka pipe](kafka.md) when the librdkafka development
package (`librdkafka-dev`) is installed. Without it, `serve` is the broker
alone, and refuses to start on a `modbee.yaml` that has Kafka mappings rather
than quietly ignore them.

**Why the Arduino library does not have it.** The command lives in `cli/` and
the server in `server/`, outside `src/`, which is the only folder the Arduino
IDE compiles. Nothing of either reaches a board, and neither costs a byte of
flash. A board can still be a broker - that is `MqttBroker`, in the library
([broker.md](broker.md)).

| Command | What it does |
| --- | --- |
| `modbee-mqtt pub` | Publish, and print the broker's answer for each message |
| `modbee-mqtt sub` | Subscribe, print what the broker granted, then print what arrives |
| `modbee-mqtt req` | MQTT 5 request/response: publish with a response topic and correlation data, wait for the matching reply |
| `modbee-mqtt bench` | Load generator, Linux only: `conn`, `pub`, `sub` |
| `modbee-mqtt serve` | The broker, Linux and Windows: one `MqttBroker` per core behind one port, with the Kafka pipe. `--data <folder>` keeps everything in that folder - logins, settings, retained messages, sessions - with no `modbee.yaml` |
| `modbee-mqtt service` | `serve` as a service - systemd on Linux, a Windows service on Windows: `install` (and start), `start`, `stop`, `status`, `uninstall`. With nothing else, it keeps everything in one folder (`serve --data`) ([releases.md](releases.md#installing-it)) |
| `modbee-mqtt admin` | The admin login, set on the server itself - added, or given a new password; it works at once, on a running server too |
| `modbee-mqtt check` | Validate a `modbee.yaml` and report every problem with its line |
| `modbee-mqtt user` | Device logins on a broker: `list`, `show`, `add`, `set`, `reset`, `disable`, `enable`, `approve`, `permissions`, `password`, `admin`, `remove` - live, no restart. `add` and `set` take `--fingerprint`, binding a login to one client certificate ([logins.md](logins.md#logging-in-with-a-certificate)) |
| `modbee-mqtt key` | Provisioning keys, with which devices register themselves: `create`, `list`, `remove` |
| `modbee-mqtt provision` | Register a device with a key from a computer, and print the login it was given |
| `modbee-mqtt config` | A broker's settings, kept in its login database: `get`, `set`, `apply`, `check`, `reset` - applied at its next restart, which `--restart` asks for, except the TLS certificate, key and client CA, which apply at once (`broker.tls.cert-pem=@file`; [settings.md](settings.md)) |
| `modbee-mqtt backup` | A copy of a broker's login database, and the settings kept in it |
| `modbee-mqtt restore` | Put a backup of the login database back, with the broker stopped |
| `modbee-mqtt --version` | The version, the TLS engine this build was compiled with - `modbee-mqtt 0.1.0 (TLS: wolfSSL)` - and what else is inside: the Kafka pipe with its librdkafka's features, and the Application API with its gRPC, or `not built` |

**Exit codes, for scripts:**

| Code | Meaning |
| --- | --- |
| 0 | Success. This includes a publish the broker accepted with `0x10` "no matching subscribers". |
| 1 | A message or subscription was refused, or a reply never came |
| 2 | Bad usage |
| 3 | Could not connect, or the connection was lost |

---

## Connecting

Every client command takes the same options.

| Option | |
| --- | --- |
| `-h, --host` / `-p, --port` | `localhost` and 1883 by default, or 8883 with TLS |
| `-i, --id` | Client Identifier; `modbee-<command>-<pid>` by default |
| `-u, --user` / `-P, --password` | Credentials. Without them, `MODBEE_AUTHENTICATION_USERNAME` and `_PASSWORD` from the environment are used - so inside a modbee container the command logs in to its own server by itself |
| `-V, --mqtt-version` | `5` (default) or `311` |
| `-k, --keepalive` | Seconds, 60 by default |
| `--session-expiry`, `-c, --no-clean` | Resume a session instead of starting clean |
| `--tls`, `--cafile`, `--pin`, `--cert`, `--key`, `--insecure` | TLS: verify against a CA, or pin the broker's key fingerprint, with optional mutual TLS |
| `-j, --json` | JSON lines instead of text |

## Publishing

```bash
modbee-mqtt pub -t plant/tank-01/level -m 4.25 -q 1
#1 plant/tank-01/level: delivered to the broker

modbee-mqtt pub -t nowhere/at/all -m x -q 1
#1 nowhere/at/all: accepted, but no matching subscribers
```

- **QoS 1 and 2 publishes end with the broker's own answer.** That includes
  the refusals MQTT 5 can express: not authorised, quota exceeded, payload
  format invalid. At QoS 0 there is no answer, so the tool reports the message
  as written.
- **Payload sources:** `-m` (text), `-f` (a file, sent as bytes), `-l` (one
  message per line of standard input) and `-n` (a zero-length payload, which
  clears a retained message).
- **`--count N --interval ms` repeats a message**, and every repeat is sent.
  It uses `publishSeries()`, because the ordinary `publish()` replaces a
  queued message to the same topic, which is right for a setpoint and wrong
  for "send this ten times".
- **MQTT 5 properties:**
  - `--content-type`, `--response-topic`, `--correlation`
  - `--expiry` (seconds)
  - `--utf8` (Payload Format Indicator)
  - `--user-property key=value`, repeatable

## Subscribing

```bash
modbee-mqtt sub -t 'plant/#' -q 1 -v
subscribed plant/#: granted
plant/tank-01/level 4.25
```

- `-t` repeats for several filters.
- `-C N` exits after N messages, and `-W s` after N seconds.
- MQTT 5 subscription options: `--no-local`, `--retain-as-published`,
  `--retain-handling 0|1|2`.
- **`--sparkplug`** prints Sparkplug B messages decoded, one JSON line each -
  their kind, group, node and device, and every metric typed, as the
  Application API gives them: `modbee-mqtt sub -t 'spBv1.0/#' --sparkplug`.
- A Will and MQTT 5's connect limits, on `pub`, `sub` and `req`: `--will-topic`,
  `--will-message`, `--will-qos`, `--will-retain`, `--will-delay`;
  `--receive-maximum`, `--topic-alias-maximum`, `--maximum-packet-size`.
- **Binary payloads, such as Sparkplug B protobuf, print as base64** rather
  than wrecking the terminal.
- **With `--json`, each message is one line.** Text payloads appear as
  `payload` and binary ones as `payload_base64`. Properties are included, and
  user properties are an array of pairs, because MQTT 5 allows a key more than
  once:

```json
{"topic":"demo/b","qos":1,"retain":false,"payload":"second","properties":{"content_type":"text/plain","user_properties":[["site","nz"],["site","au"]]}}
```

## Request and response

```bash
modbee-mqtt req -t svc/echo -m ping
echo:ping
```

1. Subscribes to a response topic (`modbee/reply/<id>` by default, or `-e`).
2. Waits for the SUBACK.
3. Publishes the request with that topic and a unique correlation value.
4. Prints the first reply carrying the same correlation value. A reply to
   somebody else's request is ignored.

It needs MQTT 5, because 3.1.1 has neither property.

## With Kafka in the middle: `--kafka`

`pub`, `sub` and `req` can talk to **Kafka** instead of a broker, writing and
reading records in the envelope the server's Kafka pipe and its connections
over Kafka use ([kafka.md](kafka.md#the-envelope-one-mqtt-message-one-kafka-record)) -
for testing the far side of a link:

```bash
modbee-mqtt sub --kafka kafka-1:9092 --kafka-topic plant-up -t 'plant/#' -v
modbee-mqtt pub --kafka kafka-1:9092 --kafka-topic plant-down -t plant/cmd -m start -q 1
plant/cmd: 1 record(s) written to Kafka topic plant-down
modbee-mqtt req --kafka kafka-1:9092 --kafka-topic svc-in --kafka-reply-topic svc-out -t svc/time -m now
```

- **`pub`** writes one record per message, its value the payload, its key the
  MQTT topic (`--kafka-key none` or a fixed text instead), its headers the
  topic, QoS, retain and every property given (`--content-type`,
  `--correlation`, `--user-property`, ...). It ends when Kafka has answered
  every record.
- **`sub`** prints each record as the MQTT message it carries - the same
  lines, `--json` and `--sparkplug` as from a broker; `-t` narrows by the MQTT
  topic (default `#`); `--kafka-topic` repeats. It reads every partition from
  its end, with no consumer group, so nothing waits for one to form;
  `--kafka-from-start` from the start, `--kafka-group <name>` as a member of a
  group.
- **`req`** reads the reply topic (`--kafka-reply-topic`, default the request's)
  before it writes the request, then prints the record carrying its
  correlation value.
- **TLS and login**, as a Kafka cluster takes them: `--kafka-tls`,
  `--kafka-ca <file>`, `--kafka-cert <file>` with `--kafka-key-file <file>`,
  `--kafka-insecure` (no host name check), `--kafka-sasl
  plain|scram-sha256|scram-sha512` with `--kafka-user` and `--kafka-password` or
  `--kafka-password-file`; `--kafka-set name=value` passes any librdkafka
  setting.
- A build without librdkafka refuses `--kafka` and says so.

## Serving

```bash
modbee-mqtt serve -C modbee.yaml                              # everything from the file
modbee-mqtt serve -c 200000                                   # plain MQTT on 1883, one shard per core
modbee-mqtt serve --tls-port 8883 --cert broker.pem --key broker.key
modbee-mqtt serve --no-plain --tls-port 8883 --cert broker.pem --key broker.key --client-ca devices-ca.pem
modbee-mqtt serve --tls-port 8883 --cert broker.pem --key broker.key --client-ca devices-ca.pem --client-certificates optional
# An application's own broker, from flags alone - then changed through the API while it runs:
modbee-mqtt serve --data /var/lib/app/broker --api unix:/run/app/broker.sock --api-token-file /run/app/token \
    --authority --authority-timeout-ms 1500 --events --max-sessions 200000 --queue-bytes-total 268435456
```

- **Everything from flags, no file.** `--api unix:<path>` and/or
  `--api tcp://host:port` serve the [Application API](app-api.md) (repeat the
  flag for both), `--api-token-file` names the token every call must carry,
  `--authority` has your application decide logins (`--authority-timeout-ms`,
  2000 by default), `--events` turns client events on, `--sparkplug-aware`
  keeps every Sparkplug birth on `$sparkplug/certificates/...`
  ([app-api.md](app-api.md#sparkplug-b)), `--queue`, `--queue-bytes` and
  `--queue-bytes-total` cap what sessions hold, `--max-connect-rate` how many
  new connections it takes a second (1000; `0` no limit - a reconnect storm
  past it waits in the kernel's backlog, a slower connect rather than a
  refusal). **`--set path=value`** sets
  any other setting by its place in `modbee.yaml` - `--set
  broker.max-keepalive=60`, `--set authentication.max-sessions-per-login=4` -
  checked as the file is, and refused with the reason if it is not a setting
  or not a value it takes. What changes live after that is changed through the
  API, or these commands ([below](#the-application-api-from-the-command-line)).

- **One broker per core, on one port.** Each shard is an `MqttBroker` on its
  own thread with its own listener (`SO_REUSEPORT`), and a session lives on the
  shard that owns its Client Identifier. A connection that lands elsewhere is
  handed over at its CONNECT. Clients cannot tell. `server/ShardedBroker.h`
  has the design.
- **`-c` and `--max-sessions` are for the whole server.** Each shard is sized
  for its share, plus a quarter, because the hash spreads evenly on average.
  An application can raise `-c` while the server runs (`Admin.PutListeners`,
  [app-api.md](app-api.md#administration-and-rest)) as far as `--max-sessions` - every
  connection has a session - so set that higher to leave room; the room
  costs nothing until it is used.
- **`--username` and `--password`** require that login of every MQTT client.
  For a login per device, changed while the broker runs, use
  `authentication: database:` in the file ([logins.md](logins.md)).
- **`--max-retained`** (10,000 by default) is how many retained topics the
  server keeps; every shard holds all of them.
- **`--retained-file /var/lib/modbee/retained.db`** keeps them in a file as
  well, so they survive a restart; `--retained-store kafka` keeps them in
  Kafka (with a `kafka:` section in the file). Memory only is the default.
  [docker.md](docker.md#where-messages-are-kept) compares the three.
- **`--sessions-directory /var/lib/modbee/sessions`** keeps persistent sessions
  and the messages queued for them on disk as well, so they survive a restart,
  or the server being killed ([docker.md](docker.md#sessions)).
- **TLS** listens beside plain TCP on `--tls-port`, or alone with
  `--no-plain`. On OpenSSL the shards share one TLS context, so a session
  ticket from one is honoured by all of them; on wolfSSL each shard has its
  own ([security.md](security.md#the-broker-images-wolfssl)). `--client-ca` makes it mutual TLS: every client
  must present a certificate that CA signed - or, with
  `--client-certificates optional`, may, and logs in with a password if it
  does not. The start-up line names the engine:
  `tls 8883 (wolfSSL, client certificates required)`. With
  `authentication: certificate-login: true` the certificate is the login
  ([logins.md](logins.md#logging-in-with-a-certificate)), and the
  certificate, key and client CA can be changed while it runs
  ([settings.md](settings.md#the-tls-material-applied-while-it-runs)).
- **Shared subscriptions work across shards.** Each message to
  `$share/<name>/<filter>` reaches one member of the group in the whole
  server. The shard it was published on chooses which shard delivers it,
  rotating over every member, and that shard picks one of its own members.
- **`-C modbee.yaml`** runs a configuration file; flags given as well
  override it. The file's format, and the Kafka pipe it configures, are in
  [kafka.md](kafka.md). `modbee-mqtt check -C modbee.yaml` validates a file
  without starting anything, and exits 0 only if it would load.
- **`--metrics-port 9464`** (or a `metrics:` section) serves Prometheus
  `/metrics`, `/healthz` (the process answers) and `/readyz` (it is ready
  for clients - [kafka.md](kafka.md#metrics)).
- **Settings kept in the database.** With `authentication: database:`, what
  `modbee-mqtt config` has stored goes over the file, and under the environment
  and these flags ([settings.md](settings.md#which-value-wins)).

**What a connection costs**, measured on Linux with idle connections:

| | Per connection in the broker |
| --- | --- |
| Plain TCP | about 1.1 KB, plus the kernel's socket memory |
| TLS, OpenSSL 3.5 | about 29 KB (TLS 1.2: 22 KB) |
| TLS, wolfSSL 5.9 | about 15 KB |

The TLS figures already include the engine releasing its idle record
buffers; without that, OpenSSL is 63 KB. At a million devices TLS is most of
the memory, so size the host for it.

## The Application API, from the command line

Every call of the [Application API](app-api.md) is a command: the same
names, the same fields, the same answers and the same errors. A command's
flags are the fields of the call's request, read from the contract compiled
into the program, so a command can neither lack a field its call has nor have
one it lacks; what it prints is the call's answer, as JSON with the
contract's field names.

```sh
export MODBEE_API=unix:/run/modbee-mqtt/api.sock      # what serve --api was given
export MODBEE_API_TOKEN_FILE=/etc/modbee-mqtt/api-token
modbee-mqtt sessions list --client-id-prefix pump-
modbee-mqtt listeners set --port 1884 --max-packet 131072 --queue 256 --queue-bytes-total 268435456 --connect-rate 2000
modbee-mqtt tls set --certificate-pem @server.crt --key-pem @server.key
modbee-mqtt connections put --id cloud --servers '{"host":"broker.example.com","port":8883}' --tls.enabled --client-id gw-1
modbee-mqtt messages publish --message.topic plant/cmd --message.payload start --confirm delivered
modbee-mqtt messages consume --group console --filters 'plant/#' --count 10
```

| Flag | Becomes |
| --- | --- |
| `--client-id-prefix pump` | `client_id_prefix: "pump"` - a field, kebab-case for snake_case |
| `--message.topic plant/x` | `message { topic: "plant/x" }` - a dot reaches inside a message |
| `--filters a --filters b` | `filters: ["a", "b"]` - a repeated field, once for each |
| `--set broker.port=1884` | `set { "broker.port": "1884" }` - a map, `key=value` |
| `--servers '{"host":"a"}'` | `servers: [{host: "a"}]` - a message, as its JSON |
| `--confirm delivered` | `confirm: CONFIRM_DELIVERED` - an enum by its name, or the end of it |
| `--connected-only` | `connected_only: true` - a bool, alone (or `=false`) |
| `--key-pem @server.key` | the file's contents (`@@` for a value that starts with `@`) |
| `--json '{...}'` or `--json @request.json` | the whole request, before the flags, which win |

- **Where:** `--api unix:<path>` or `--api tcp://host:port` (`MODBEE_API`),
  `--token-file` (`MODBEE_API_TOKEN_FILE`) or `--token` (`MODBEE_API_TOKEN`),
  `--api-ca` for an API on TLS. `--timeout` seconds (30; a stream waits for
  ever, or `--count N`).
- **`--help`** on a command lists its fields; `modbee-mqtt sessions` lists
  the `sessions` commands.
- **Errors** are the call's status, by name - `modbee-mqtt listeners set:
  FAILED_PRECONDITION: at most 400 connections without a restart ...` - and
  the exit code is `1` for a refusal, `3` when the server cannot be reached,
  `2` for a command that is not right.
- `messages consume` acknowledges what it prints; `connections watch` prints
  each change of state, one JSON line each.
- `logins` and `keys` are the same calls as `user` and `key`, over the API
  rather than MQTT; `settings` is `config`'s.

<!-- One row per call; test/cli_api_tests.cpp fails when this table, cli/ApiCommands.h and app.proto disagree. -->
| Command | Call | REST |
| --- | --- | --- |
| `modbee-mqtt hello` | `System.Hello` | - |
| `modbee-mqtt authority revoke` | `Authority.Revoke` | - |
| `modbee-mqtt authority regrant` | `Authority.Regrant` | - |
| `modbee-mqtt authority forget` | `Authority.Forget` | - |
| `modbee-mqtt sparkplug decode` | `Sparkplug.Decode` | - |
| `modbee-mqtt sparkplug encode` | `Sparkplug.Encode` | - |
| `modbee-mqtt sparkplug-hosts put` | `Sparkplug.PutHost` | - |
| `modbee-mqtt sparkplug-hosts get` | `Sparkplug.GetHost` | - |
| `modbee-mqtt sparkplug-hosts list` | `Sparkplug.ListHosts` | - |
| `modbee-mqtt sparkplug-hosts delete` | `Sparkplug.DeleteHost` | - |
| `modbee-mqtt sparkplug nodes` | `Sparkplug.ListNodes` | - |
| `modbee-mqtt sparkplug rebirth` | `Sparkplug.Rebirth` | - |
| `modbee-mqtt sparkplug write` | `Sparkplug.Write` | - |
| `modbee-mqtt sparkplug-nodes put` | `Sparkplug.PutEdgeNode` | - |
| `modbee-mqtt sparkplug-nodes get` | `Sparkplug.GetEdgeNode` | - |
| `modbee-mqtt sparkplug-nodes list` | `Sparkplug.ListEdgeNodes` | - |
| `modbee-mqtt sparkplug-nodes delete` | `Sparkplug.DeleteEdgeNode` | - |
| `modbee-mqtt sparkplug-nodes set` | `Sparkplug.SetValues` | - |
| `modbee-mqtt sparkplug-nodes device` | `Sparkplug.SetDeviceState` | - |
| `modbee-mqtt sparkplug-nodes rebirth` | `Sparkplug.RebirthEdgeNode` | - |
| `modbee-mqtt sparkplug-aware get` | `Sparkplug.GetAware` | - |
| `modbee-mqtt sparkplug-aware set` | `Sparkplug.PutAware` | - |
| `modbee-mqtt messages consume` | `Messages.Consume` | - |
| `modbee-mqtt messages publish` | `Messages.Publish` | `POST /v1/publish` |
| `modbee-mqtt groups list` | `Messages.ListGroups` | `GET /v1/groups` |
| `modbee-mqtt groups delete` | `Messages.DeleteGroup` | `DELETE /v1/groups/{group}?connection=` |
| `modbee-mqtt logins backup` | `Admin.Backup` | `POST /v1/backups` |
| `modbee-mqtt events get` | `Admin.GetEvents` | `GET /v1/events` |
| `modbee-mqtt events set` | `Admin.PutEvents` | `PUT /v1/events` |
| `modbee-mqtt kafka-clusters put` | `Kafka.PutCluster` | `PUT /v1/kafka-clusters/{id}` |
| `modbee-mqtt kafka-clusters get` | `Kafka.GetCluster` | `GET /v1/kafka-clusters/{id}` |
| `modbee-mqtt kafka-clusters list` | `Kafka.ListClusters` | `GET /v1/kafka-clusters` |
| `modbee-mqtt kafka-clusters delete` | `Kafka.DeleteCluster` | `DELETE /v1/kafka-clusters/{id}` |
| `modbee-mqtt kafka-mappings put` | `Kafka.PutMapping` | `PUT /v1/kafka-mappings/{id}` |
| `modbee-mqtt kafka-mappings get` | `Kafka.GetMapping` | `GET /v1/kafka-mappings/{id}` |
| `modbee-mqtt kafka-mappings list` | `Kafka.ListMappings` | `GET /v1/kafka-mappings` |
| `modbee-mqtt kafka-mappings delete` | `Kafka.DeleteMapping` | `DELETE /v1/kafka-mappings/{id}` |
| `modbee-mqtt kafka status` | `Kafka.Status` | `GET /v1/kafka-status` |
| `modbee-mqtt connections put` | `Connections.Put` | `PUT /v1/connections/{id}` |
| `modbee-mqtt connections get` | `Connections.Get` | `GET /v1/connections/{id}` |
| `modbee-mqtt connections list` | `Connections.List` | `GET /v1/connections` |
| `modbee-mqtt connections delete` | `Connections.Delete` | `DELETE /v1/connections/{id}` |
| `modbee-mqtt connections watch` | `Connections.Watch` | - |
| `modbee-mqtt connections reconnect` | `Connections.Reconnect` | `POST /v1/connections/{id}:reconnect` |
| `modbee-mqtt logins list` | `Admin.ListLogins` | `GET /v1/logins` |
| `modbee-mqtt logins get` | `Admin.GetLogin` | `GET /v1/logins/{name}` |
| `modbee-mqtt logins put` | `Admin.PutLogin` | `PUT /v1/logins/{name}` |
| `modbee-mqtt logins delete` | `Admin.DeleteLogin` | `DELETE /v1/logins/{name}` |
| `modbee-mqtt logins reset` | `Admin.ResetLogin` | `POST /v1/logins/{name}:reset` |
| `modbee-mqtt keys list` | `Admin.ListKeys` | `GET /v1/keys` |
| `modbee-mqtt keys put` | `Admin.PutKey` | `PUT /v1/keys/{name}` |
| `modbee-mqtt keys delete` | `Admin.DeleteKey` | `DELETE /v1/keys/{name}` |
| `modbee-mqtt sessions list` | `Admin.ListSessions` | `GET /v1/sessions` |
| `modbee-mqtt sessions kick` | `Admin.Kick` | `POST /v1/sessions/{clientId}:kick` |
| `modbee-mqtt retained list` | `Admin.ListRetained` | `GET /v1/retained` |
| `modbee-mqtt retained delete` | `Admin.DeleteRetained` | `DELETE /v1/retained?filter=` |
| `modbee-mqtt listeners get` | `Admin.GetListeners` | `GET /v1/listeners` |
| `modbee-mqtt listeners set` | `Admin.PutListeners` | `PUT /v1/listeners` |
| `modbee-mqtt tls set` | `Admin.PutTls` | `PUT /v1/listeners/tls` |
| `modbee-mqtt stats` | `Admin.Stats` | `GET /v1/stats` |
| `modbee-mqtt settings get` | `Admin.GetSettings` | `GET /v1/settings` |
| `modbee-mqtt settings check` | `Admin.CheckSettings` | `POST /v1/settings:check` |
| `modbee-mqtt settings set` | `Admin.SetSettings` | `PATCH /v1/settings` |
| `modbee-mqtt settings apply` | `Admin.ApplySettings` | `PUT /v1/settings` |
| `modbee-mqtt settings reset` | `Admin.ResetSettings` | `POST /v1/settings:reset` |
| - | `System.Attach` | an application instance's presence, held open for as long as it runs |
| - | `Authority.Serve` | the application answering CONNECTs, for as long as it runs |
| - | `Messages.PublishStream` | an outbox draining; `messages publish` sends one at a time |

---

## Benchmarking

```bash
modbee-mqtt bench conn -c 50000 -T 8 -R 2000 -d 30 --bind 127.0.0.2-127.0.0.5 --rss-pid $(pgrep -x my-broker)
modbee-mqtt bench sub  -c 10 -t bench -q 1 -d 60
modbee-mqtt bench pub  -c 1000 -r 10 -s 100 -q 1 -d 30
```

- **`conn`** opens N connections at `-R` per second and holds them for `-d`
  seconds. A few probe clients (`--probes`) publish at QoS 1 throughout and
  time the broker's answer, which is how loaded a broker *feels*.
  `--rss-pid` reports the broker's resident memory per connection.
- **`pub` and `sub`:** payloads carry the publisher id, a sequence number and
  the send time.
  - The subscriber reports p50, p99 and p999 latency, and counts loss and
    duplicates exactly, per subscriber and publisher.
  - Start subscribers first and stop publishers first, because loss is judged
    within the range of sequences each subscriber saw.
- **Every client is a real `MqttClient`.** Each thread runs a slice of them
  over one epoll set, and visits idle ones at least every `--sweep-ms` so
  keepalive runs.
- **`--bind` spreads connections over several source addresses.** Linux's
  default ephemeral port range gives about 28,000 connections per source
  address towards one broker port. On one machine, `127.0.0.2-127.0.0.12`
  needs no setup, because all of 127/8 is local.
- **Failures are reported by reason** (connection lost, refused, timed out),
  never as a bare count.
- **`--lean`** (`conn` only) holds each connection as a raw MQTT 3.1.1
  CONNECT and PINGREQ of a few bytes, instead of a full client. That is how one
  machine opens a million connections. The probes stay full clients.
- **`--tls`** dials TLS and does not verify the broker, unless `--pin` gives
  its key fingerprint. It cannot be combined with `--lean` or `--bind`.

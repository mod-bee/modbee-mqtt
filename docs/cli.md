# The `modbee` command-line tool

One binary that publishes, subscribes, benchmarks and runs the broker, built
on this library's own client. It reports what the **broker said** about every
operation, not just that something was sent.

## Getting it

| Where you are | The `modbee` command |
| --- | --- |
| **The Docker image** | **Included.** Nothing to install - see below |
| **This repository, built on Linux or macOS** | Built by default, as `build/cli/modbee` |
| **The Arduino library** | **Not included.** It is a computer program, not part of what a board runs |
| **Your own CMake project** that adds this library | Not built - your project gets the library alone. `-DMQTT_BUILD_CLI=ON` builds it |

**From the Docker image**, with nothing installed:

```bash
docker run --rm ghcr.io/mod-bee/modbee-mqtt pub -h broker.example.com -t test -m hi -q 1
docker run --rm -it ghcr.io/mod-bee/modbee-mqtt sub -h broker.example.com -t 'test/#' -v
```

Given a command, the image runs `modbee <command>`; given none, it runs the
broker. Inside a running compose stack, `docker compose exec modbee modbee ...`
reaches that stack's broker on `localhost`. To reach a broker on your own
machine from `docker run`, add `--network host`.

**From source:**

```bash
cmake -S . -B build && cmake --build build -j
./build/cli/modbee --help
sudo cmake --install build          # optional: puts modbee in /usr/local/bin
```

On Linux every command is built. On macOS, `pub`, `sub` and `req` are, and
`bench`, `serve` and `check` are not, because the broker tier is Linux only.
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
| `modbee pub` | Publish, and print the broker's answer for each message |
| `modbee sub` | Subscribe, print what the broker granted, then print what arrives |
| `modbee req` | MQTT 5 request/response: publish with a response topic and correlation data, wait for the matching reply |
| `modbee bench` | Load generator, Linux only: `conn`, `pub`, `sub` |
| `modbee serve` | The broker, Linux only: one `MqttBroker` per core behind one port, with the Kafka pipe |
| `modbee check` | Validate a `modbee.yaml` and report every problem with its line |
| `modbee user` | Device logins on a broker: `list`, `show`, `add`, `reset`, `disable`, `enable`, `approve`, `permissions`, `password`, `admin`, `remove` - live, no restart ([logins.md](logins.md)) |
| `modbee key` | Provisioning keys, with which devices register themselves: `create`, `list`, `remove` |
| `modbee provision` | Register a device with a key from a computer, and print the login it was given |
| `modbee backup` | A copy of a broker's login database |
| `modbee restore` | Put a backup of the login database back, with the broker stopped |

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
modbee pub -t plant/tank-01/level -m 4.25 -q 1
#1 plant/tank-01/level: delivered to the broker

modbee pub -t nowhere/at/all -m x -q 1
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
modbee sub -t 'plant/#' -q 1 -v
subscribed plant/#: granted
plant/tank-01/level 4.25
```

- `-t` repeats for several filters.
- `-C N` exits after N messages, and `-W s` after N seconds.
- MQTT 5 subscription options: `--no-local`, `--retain-as-published`,
  `--retain-handling 0|1|2`.
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
modbee req -t svc/echo -m ping
echo:ping
```

1. Subscribes to a response topic (`modbee/reply/<id>` by default, or `-e`).
2. Waits for the SUBACK.
3. Publishes the request with that topic and a unique correlation value.
4. Prints the first reply carrying the same correlation value. A reply to
   somebody else's request is ignored.

It needs MQTT 5, because 3.1.1 has neither property.

## Serving

```bash
modbee serve -C modbee.yaml                              # everything from the file
modbee serve -c 200000                                   # plain MQTT on 1883, one shard per core
modbee serve --tls-port 8883 --cert broker.pem --key broker.key
modbee serve --no-plain --tls-port 8883 --cert broker.pem --key broker.key --client-ca devices-ca.pem
```

- **One broker per core, on one port.** Each shard is an `MqttBroker` on its
  own thread with its own listener (`SO_REUSEPORT`), and a session lives on the
  shard that owns its Client Identifier. A connection that lands elsewhere is
  handed over at its CONNECT. Clients cannot tell. `server/ShardedBroker.h`
  has the design.
- **`-c` and `--max-sessions` are for the whole server.** Each shard is sized
  for its share, plus a quarter, because the hash spreads evenly on average.
- **`--username` and `--password`** require that login of every MQTT client.
  For a login per device, changed while the broker runs, use
  `authentication: database:` in the file ([logins.md](logins.md)).
- **`--max-retained`** (10,000 by default) is how many retained topics the
  server keeps; every shard holds all of them.
- **`--retained-file /var/lib/modbee/retained.db`** keeps them in a file as
  well, so they survive a restart; `--retained-store kafka` keeps them in
  Kafka (with a `kafka:` section in the file). Memory only is the default.
  [docker.md](docker.md#where-messages-are-kept) compares the three.
- **TLS** listens beside plain TCP on `--tls-port`, or alone with
  `--no-plain`. The shards share one TLS context, so a session ticket from one
  is honoured by all of them. `--client-ca` makes it mutual TLS: every client
  must present a certificate that CA signed.
- **Shared subscriptions work across shards.** Each message to
  `$share/<name>/<filter>` reaches one member of the group in the whole
  server. The shard it was published on chooses which shard delivers it,
  rotating over every member, and that shard picks one of its own members.
- **`-C modbee.yaml`** runs a configuration file; flags given as well
  override it. The file's format, and the Kafka pipe it configures, are in
  [kafka.md](kafka.md). `modbee check -C modbee.yaml` validates a file
  without starting anything, and exits 0 only if it would load.
- **`--metrics-port 9464`** (or a `metrics:` section) serves Prometheus
  `/metrics` and `/healthz`.

**What a connection costs**, measured on Linux with idle connections:

| | Per connection in the broker |
| --- | --- |
| Plain TCP | about 1.1 KB, plus the kernel's socket memory |
| TLS, OpenSSL 3.5 | about 29 KB (TLS 1.2: 22 KB) |
| TLS, wolfSSL 5.9 | about 15 KB |

The TLS figures already include the engine releasing its idle record
buffers; without that, OpenSSL is 63 KB. At a million devices TLS is most of
the memory, so size the host for it.

## Benchmarking

```bash
modbee bench conn -c 50000 -T 8 -R 2000 -d 30 --bind 127.0.0.2-127.0.0.5 --rss-pid $(pgrep -x my-broker)
modbee bench sub  -c 10 -t bench -q 1 -d 60
modbee bench pub  -c 1000 -r 10 -s 100 -q 1 -d 30
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

# Running modbee in Docker

The image is `ghcr.io/mod-bee/modbee-mqtt`: the broker (`modbee serve`), one
broker per CPU core behind one port, with the [Kafka pipe](kafka.md) built in,
and the [`modbee` command](#the-modbee-command-is-in-the-image) for publishing,
subscribing and testing. It runs on 64-bit Intel/AMD and ARM (a Raspberry Pi 4
or 5, an ARM cloud server).

**Which one do you want?**

| You want | Use | Takes |
| --- | --- | --- |
| Just an MQTT broker | [one command](#just-the-broker) | a minute |
| To try the Kafka pipe on this computer | [`docker/compose.yml`](#try-it-with-kafka) | five minutes |
| A broker on the internet, with a real certificate (Kafka optional, your own) | [`docker/stack/`](#on-the-internet-with-a-certificate) | ten minutes and a domain name |
| The same, **with Kafka included**, all in one file | [`docker/all-in-one/`](#everything-in-one-file-broker-certificate-and-kafka) | ten minutes and a domain name |
| Either of those behind a Nginx Proxy Manager you already run | [the NPM route](#already-running-nginx-proxy-manager) | ten minutes |

In none of them do you copy a certificate file, write a path to one, or renew
one. The two that put the broker on the internet also require every device to
**log in**, each with a login of its own that you can add, reset and remove
while it runs ([logins.md](logins.md)).

**Until the image is published**, build it from this checkout: the compose
files below each have a `build:` beside their `image:`, so
`docker compose up -d --build` builds it (a few minutes, the first time) and
then starts everything. That needs the compose file where it is, inside the
repository. Once the image is published, `--build` can be left off, and the
files can be copied anywhere. `MODBEE_IMAGE` in `.env` names another image -
your own registry's, say - for the stack to run instead. For the `docker run`
commands, build it once under the published name, from the repository root:
`docker build -t ghcr.io/mod-bee/modbee-mqtt -f docker/Dockerfile .`

---

## Just the broker

```bash
docker run -d --name modbee -p 1883:1883 --restart unless-stopped ghcr.io/mod-bee/modbee-mqtt
```

Plain MQTT on port 1883, every client allowed, nothing written to disk. To
require a login, add two settings - every device then connects with them:

```bash
docker run -d --name modbee -p 1883:1883 --restart unless-stopped \
  -e MODBEE_AUTHENTICATION_USERNAME=devices -e MODBEE_AUTHENTICATION_PASSWORD=a-long-password \
  ghcr.io/mod-bee/modbee-mqtt
```

Any MQTT client works. The `modbee` command is already inside the container
(and logs in by itself):

```bash
docker exec modbee modbee pub -t test/hello -m hi -q 1
#1 test/hello: accepted, but no matching subscribers
```

---

## Try it with Kafka

```bash
git clone https://github.com/mod-bee/modbee-mqtt && cd modbee-mqtt
docker compose -f docker/compose.yml up -d --build    # --build: until the image is published
docker compose -f docker/compose.yml exec modbee modbee pub -t plant/tank-01/level -m 4.25 -q 1
```

That starts modbee and a single Kafka node. The message lands in the Kafka
topic `mqtt-plant`, and its PUBACK came back only once Kafka had it. The
mappings are in `docker/modbee.yaml`; change them and
`docker compose -f docker/compose.yml restart modbee`.

If something else already uses port 1883:
`MODBEE_MQTT_PORT=11883 docker compose -f docker/compose.yml up -d`.

This Kafka is for trying things out: one node, no replication, nothing kept
when it is removed, and reachable only from inside the compose project. For
Kafka you can connect to and keep, use [`docker/all-in-one/`](#everything-in-one-file-broker-certificate-and-kafka);
for a cluster you already run, see [Your own Kafka](#your-own-kafka).

---

## On the internet, with a certificate

`docker/stack/` is three files: `compose.yml`, `.env.example` and
`modbee.yaml`. It runs **Traefik** in front of modbee. Traefik fetches a free
Let's Encrypt certificate for your domain, renews it before it expires, and
does the TLS. There is no Kafka in it: the pipe is off until you point
`modbee.yaml` at [your own Kafka](#your-own-kafka). For Kafka included, use
[`docker/all-in-one/`](#everything-in-one-file-broker-certificate-and-kafka)
instead - everything below applies to it too.

**Before you start:** a domain name (say `mqtt.example.com`) whose DNS points
at the server, and ports **80** and **8883** open to the internet. Port 80 is
how Let's Encrypt checks the domain is yours; 8883 is where devices connect.

```bash
cd docker/stack                 # or copy these three files anywhere
cp .env.example .env
nano .env                       # set DOMAIN, EMAIL and MQTT_PASSWORD - the only three settings
docker compose up -d --build    # --build: until the image is published
```

A minute later the certificate is in place. Check from any computer with
Docker:

```bash
docker run --rm ghcr.io/mod-bee/modbee-mqtt pub --tls -h mqtt.example.com \
  -u admin -P 'your MQTT_PASSWORD' -t test/hello -m hi -q 1
```

`MQTT_PASSWORD` is the **admin** login - for you, your own apps and the
`modbee` command. **Each device gets a login of its own**, added while the
broker runs and never needing a restart:

```bash
docker compose exec modbee modbee user add tank-01 --publish 'plant/${username}/#'   # prints the device's secret
docker compose exec modbee modbee key create site-north --site north --publish 'plant/${username}/#'   # or: devices register themselves
```

[logins.md](logins.md) explains both, and what each device may do. These
setups are strict about it (`default-topics: none` in `modbee.yaml`): a device
uses only the topics its `--publish` / `--subscribe` rules name, and its Will
must be inside them too; a login meant to use every topic is given `'#'` on
purpose. Devices connect to **`mqtts://mqtt.example.com:8883`** with their own
login; anything without one is refused (`not authorized`). Computers, phones and
most MQTT tools trust Let's Encrypt already. An ESP32 using `WiFiClientSecure`
needs Let's Encrypt's root certificate given to it once in the sketch
(`secure.setCACert(...)` with *ISRG Root X1*), as for any public broker.

Good to know:

- **The logins are kept in the `modbee-data` volume**, with a backup made
  every day - [logins.md](logins.md#backups) says how to keep a copy
  elsewhere and how to restore one. A login store made by an earlier version
  of this broker is refused at start, saying so
  ([logins.md](logins.md#a-login-store-from-an-earlier-version)).
- **Practise first** if you like: uncomment `ACME_SERVER` in `.env` to use
  Let's Encrypt's staging service, which has no rate limits but whose
  certificates are not trusted. Comment it out again and
  `docker compose up -d` for the real one.
- **Plain MQTT on the LAN as well:** uncomment the `ports` line under `modbee`
  in `compose.yml`. It asks for the same logins - without TLS, so passwords
  cross the LAN readable; keep that port off the internet.
- **The broker sees Traefik's address**, not the device's, for each
  connection. Traefik's own log has the devices' addresses.
- The stack's name is `modbee-stack` (`modbee-all-in-one` for the one with
  Kafka), so the two can run on one server side by side:
  `docker compose logs -f modbee`, `docker compose ps`.

---

## Everything in one file: broker, certificate and Kafka

`docker/all-in-one/` is `docker/stack/` with a Kafka server added: Traefik for
the certificate, modbee, and one Kafka node with its data kept in a volume.
The same three files, and the same three settings:

```bash
cd docker/all-in-one            # or copy these three files anywhere
cp .env.example .env
nano .env                       # set DOMAIN, EMAIL and MQTT_PASSWORD
docker compose up -d --build    # --build: until the image is published
```

Devices connect to **`mqtts://mqtt.example.com:8883`**, as above, and as it
ships:

- **every MQTT message goes to the Kafka topic `mqtt-messages`.** The record's
  key is the publisher (its MQTT client id), so each device's messages are
  read in the order it sent them; its value is the payload exactly as sent;
  and its headers carry the topic (`mqtt-topic`), QoS and MQTT 5 properties.
  The publisher's PUBACK comes back only once Kafka has the record.
- **a record written to the Kafka topic `mqtt-commands` is published to MQTT**,
  on the topic named by its key.

Both are two short blocks in `modbee.yaml`; [kafka.md](kafka.md) has every
option, and `docker compose restart modbee` applies a change. The two topics
are created when the stack starts, by the `kafka-topics` service in
`compose.yml`; if you add a mapping that reads another topic, add its name
there too, so no record written before modbee joins it is missed.

Kafka needs about 1 GB of memory on top of modbee, so allow 2 GB or more for
the whole stack. It is a single node: right for one site or one server, and
when you outgrow it, the same `modbee.yaml` points at a real cluster
([Your own Kafka](#your-own-kafka)).

### Connecting to Kafka

Building an app that reads your MQTT data from Kafka, or sends commands to
your devices through it? [kafka-apps.md](kafka-apps.md) walks through it from
the start, with a working Node.js app and the same in Python. This section is
the reference for where to connect.

Kafka tells every client which address to use for the rest of its
conversation, so the address depends on where the client is. The stack has one
for each:

| Your program runs | Connect to | Needs |
| --- | --- | --- |
| In this compose file | `kafka:9092` | nothing |
| In another compose project on this server | `kafka:9092` | [join the `modbee` network](#from-another-compose-project) |
| On this server, outside Docker | `localhost:9094` | nothing |
| On another machine on your network, or over a VPN | `<this server's address>:9094` | [two lines in `.env`](#from-another-machine-on-your-network) |
| **Anywhere** - another site, the cloud | **`mqtt.example.com:9093`**, with TLS | [a login: two lines in `.env`](#from-anywhere-with-a-password) |

The first four need no password, which is why port 9094 is open to this server
only until you say otherwise: **never open 9094 to the internet** - use 9093,
which has a login and TLS, for that.

#### See the messages arrive

Kafka's own tools are in its container. Publish something over MQTT, and watch
it land:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic mqtt-messages --from-beginning \
  --property print.key=true --property print.headers=true
```

#### Send a command from Kafka to MQTT

Type a key, `|`, and a value; each line becomes an MQTT message on the topic
in the key:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic mqtt-commands \
  --reader-property parse.key=true --reader-property key.separator='|'
plant/pump-7/cmd|start
```

A device subscribed to `plant/pump-7/cmd` receives `start`.

#### From this server

Any Kafka client library or tool, with `localhost:9094` as its bootstrap
server, e.g. [kcat](https://github.com/edenhill/kcat):

```bash
kcat -b localhost:9094 -t mqtt-messages -C -f '%k  %s  %h\n'    # publisher, payload, headers (topic among them)
```

#### From another compose project

The stack's network is named `modbee` (`MODBEE_NETWORK` in `.env` renames
it - then use the same name below). Another compose project joins it with
three lines, and then reaches Kafka as `kafka:9092` and the broker as
`modbee:1883` (plain MQTT - TLS is only needed from outside):

```yaml
services:
  my-app:
    image: my-app
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      MQTT_HOST: modbee

networks:
  default:
    name: modbee
    external: true
```

Start the stack first, so the network exists.

#### From another machine on your network

For a LAN, or machines that reach the server over a VPN. In `.env`, give the
server's address as the other machines see it, and open
the port to them:

```bash
KAFKA_BIND=0.0.0.0
KAFKA_EXTERNAL_HOST=192.168.1.20
```

then `docker compose up -d`. They connect to `192.168.1.20:9094`. Keep that
port behind your firewall: anyone who can reach it can read and write every
topic.

#### From anywhere, with a password

For apps that are not on your network - another site, a cloud service. In
`.env`:

```bash
KAFKA_PASSWORD=a-long-password      # no double quotes in it
# KAFKA_USERNAME=app                # the default user name
```

then `docker compose up -d`, and open port **9093** to them. Kafka is then
served at `mqtt.example.com:9093`, encrypted with the same Let's Encrypt
certificate as MQTT and asking for that login. With `KAFKA_PASSWORD` blank -
as it ships - port 9093 lets nobody in.

Any Kafka client connects with four settings beside the address:

| Setting | Value |
| --- | --- |
| bootstrap servers | `mqtt.example.com:9093` |
| security protocol | `SASL_SSL` |
| SASL mechanism | `PLAIN` |
| user name / password | `app` / your `KAFKA_PASSWORD` |

In Node.js ([kafka-apps.md](kafka-apps.md) has the whole app):

```js
const kafka = new Kafka({
  kafkaJS: {
    brokers: ["mqtt.example.com:9093"],
    ssl: true,
    sasl: { mechanism: "plain", username: "app", password: process.env.KAFKA_PASSWORD },
  },
});
```

In Python:

```python
consumer = Consumer({
    "bootstrap.servers": "mqtt.example.com:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "app",
    "sasl.password": os.environ["KAFKA_PASSWORD"],
    "group.id": "my-app",
})
```

SASL/PLAIN over TLS is the same scheme managed Kafka services use: the
password only ever travels encrypted. Apps on the server itself keep using
`kafka:9092` or `localhost:9094`, with no login.

---

## Already running Nginx Proxy Manager

If [Nginx Proxy Manager](https://nginxproxymanager.com) (version 2.12.3 or
later) already owns ports 80 and 443 on the server, let it do the certificate
instead of Traefik. Everything is done in its web page; nothing is copied.

**`docker/npm/` is the simple way:** modbee and Kafka without Traefik, and
two NPM streams (8883 for MQTT, 9093 for Kafka) with one certificate. Its
README has the steps. The steps below instead put modbee on NPM's network.

**1. Run modbee on NPM's network.** In `docker/stack/compose.yml` (or
`docker/all-in-one/compose.yml`, for Kafka too), delete the `traefik` service
and the `configs:` section, and make the network NPM's - its name is in
`docker network ls`, often `npm_default`:

```yaml
networks:
  default:
    name: npm_default
    external: true
```

(In `all-in-one`, that replaces the `networks:` section already there.) Then
`docker compose up -d`. modbee is now reachable from NPM as `modbee`, port
`1883`.

**2. Let NPM publish port 8883.** In NPM's own compose file, add `- "8883:8883"`
under its `ports:`, and `docker compose up -d` it again.

**3. In NPM's web page:**

1. **SSL Certificates → Add SSL Certificate → Let's Encrypt.** Domain:
   `mqtt.example.com`. Save. (This needs port 80 reaching NPM, which it
   already does.)
2. **Hosts → Streams → Add Stream.** Incoming port `8883`. Forward host
   `modbee`, forward port `1883`. Tick **TCP**.
3. On the stream's **SSL** tab, choose the certificate from step 1. Save.

Devices connect to `mqtts://mqtt.example.com:8883`, exactly as with Traefik,
and NPM renews the certificate.

**Kafka from anywhere, with a password** (`all-in-one` only, optional): add
`- "9093:9093"` to NPM's ports too, and a second stream - incoming port
`9093`, forward host `kafka`, forward port `9095`, TCP, the same certificate
on its SSL tab. `KAFKA_PASSWORD` in `.env` is the login, as with Traefik, and
`DOMAIN` in `.env` must still be set: it is the name Kafka tells apps to use.

---

## The `modbee` command is in the image

The image holds the broker and the `modbee` command-line tool
([cli.md](cli.md)): `pub`, `sub`, `req`, `bench` and `check`. Nothing needs
installing to use it.

```bash
# Inside a running stack: it reaches the broker on localhost:1883
docker compose exec modbee modbee sub -t 'plant/#' -v
docker compose exec modbee modbee pub -t plant/tank-01/level -m 4.25 -q 1

# Anywhere, against any broker
docker run --rm ghcr.io/mod-bee/modbee-mqtt pub --tls -h mqtt.example.com -t test -m hi -q 1
docker run --rm -it ghcr.io/mod-bee/modbee-mqtt sub --tls -h mqtt.example.com -t '#' -v
```

Given a command, the image runs `modbee <command>`; given none, or only
options such as `--metrics-port 9464`, it runs the broker. To reach a broker
on the same machine from `docker run`, add `--network host`.

**Logging in:** inside a stack's container, `modbee` uses the server's own
admin login, so `docker compose exec modbee modbee pub ...` needs no `-u` or
`-P`. From anywhere else, give them: `-u admin -P 'your MQTT_PASSWORD'`.

---

## Configuration

modbee reads `/etc/modbee/modbee.yaml`. Every key is in [kafka.md](kafka.md);
`modbee check` finds mistakes before they matter:

```bash
docker compose run --rm modbee check -C /etc/modbee/modbee.yaml
```

Any value can come from the environment instead of the file, named after its
place in it - which suits a compose file:

```yaml
    environment:
      MODBEE_BROKER_MAX_CONNECTIONS: "500000"
      MODBEE_KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092
```

Passwords never need to be in either: `password-file:` in the YAML, or
`MODBEE_..._PASSWORD_FILE` in the environment, names a Docker secret.

**Changing settings over MQTT.** `docker/stack/` and `docker/all-in-one/`
keep logins in a database, and the settings can be kept there too - changed
from `modbee config`, or from your own platform, with no shell on the server
and no edit to the file:

```bash
docker compose exec modbee modbee config set broker.max-session-expiry=86400 --restart
```

A stored setting goes over the file and under the environment, and applies
when modbee restarts; `--restart` restarts it once the change is stored, and
the compose files' `restart: unless-stopped` brings it back. Logins, by
contrast, change live. [settings.md](settings.md) has the whole of it.

---

## Your own Kafka

Use `docker/stack/` and uncomment the `kafka:` section of its `modbee.yaml`,
with `bootstrap-servers` set to your cluster. (In `all-in-one` or
`docker/compose.yml`, delete the `kafka` service and modbee's `depends_on`
first.) For a managed cluster with a password:

```yaml
kafka:
  clusters:
    - id: main
      bootstrap-servers: broker-1.example.net:9093
      tls: {}
      authentication:
        mechanism: scram-sha512
        username: modbee
        password-file: /run/secrets/kafka-password
```

with the password as a [Docker secret](https://docs.docker.com/compose/how-tos/use-secrets/)
named `kafka-password`.

---

## Running it well

- **Memory:** a few hundred MB idle, plus roughly 1-4 kB per connected device
  (measured: a million connections in 3.6 GB). TLS done by Traefik or NPM
  costs the broker nothing extra. The compose files cap modbee at 2 GB; raise
  `mem_limit` for a fleet of more than a few hundred thousand.
- **Open files:** one per connection. The compose files set `nofile` to a
  million; with `docker run`, add `--ulimit nofile=1048576:1048576`.
- **Health:** the image checks its own MQTT ports, so `docker ps` shows
  `healthy` once the plain port or the TLS port accepts a connection - either
  will do, so a broker with plain MQTT turned off is healthy too.
  `--metrics-port 9464` (or a `metrics:` section) adds Prometheus metrics at
  `/metrics`, and two checks for an orchestrator or a load balancer:
  `/healthz` answers while the process does (liveness), and `/readyz` answers
  `200 ready` only while the broker is running and Kafka can be reached -
  `503 not ready: Kafka cannot be reached` otherwise (readiness;
  [kafka.md](kafka.md#metrics)).
- **Stopping:** `docker stop` is graceful. Kafka offsets are committed,
  messages in flight to Kafka are given up to 5 seconds, and every device is
  told "server shutting down" so it reconnects at once to the next one.
- **Upgrading:** `docker compose pull && docker compose up -d`. Pin a release
  with `MODBEE_VERSION=1.0.0` in `.env` rather than following `latest`. Built
  from this checkout instead (no published image yet): `git pull`, then
  `docker compose up -d --build`.

## Where messages are kept

| | Kept in | Survives a restart or upgrade of modbee |
| --- | --- | --- |
| **Retained messages** | memory, and where `retained: store` says | **Yes**, with `store: kafka` or `store: file` |
| **Sessions**, and messages queued for a device that is away | memory | No |
| **Everything sent to Kafka** | Kafka, on disk (the `kafka-data` volume in `all-in-one`) | Yes |

### Retained messages

One setting in `modbee.yaml` decides where they are kept as well as memory:

```yaml
retained:
  store: kafka      # memory, file or kafka
```

| `store` | Where | Used by |
| --- | --- | --- |
| `memory` | Nowhere else: a restart starts empty | the default; `docker/stack/` |
| `file` | `/var/lib/modbee/retained.db`, saved two seconds after a change and when modbee stops | anywhere - mount a volume at `/var/lib/modbee` |
| `kafka` | A compacted Kafka topic, `modbee-retained`, written as each change happens | `docker/all-in-one/` and `docker/compose.yml` |

When modbee starts, it puts the kept messages back before it accepts a single
client, so the first subscriber after a restart sees exactly what was retained
before it - with any Message Expiry Interval counted down through the time
modbee was stopped.

- **`kafka`** creates the topic itself, compacted, the first time. Kafka then
  keeps the newest message for each MQTT topic for good, and a cleared one
  goes. If Kafka cannot be reached when modbee starts, modbee does not start
  (and Docker restarts it), rather than serve an empty store as if it were the
  real one.
- **`file`** needs somewhere to write that outlives the container.
  `docker/stack/` already mounts the volume `modbee-data` there, so switching
  is one word in its `modbee.yaml`. With `docker run`:

  ```bash
  docker run -d --name modbee -p 1883:1883 -v modbee-data:/var/lib/modbee \
    -e MODBEE_RETAINED_STORE=file ghcr.io/mod-bee/modbee-mqtt
  ```

**How many:** `broker: max-retained` (10,000 topics by default). A retained
publish to a new topic beyond that is still delivered, but not kept, and a
QoS 1 or 2 publisher is told `0x97`. Each CPU core keeps a copy, so allow
about 216 bytes a slot per core, plus the messages themselves.

**A new subscription is sent every matching retained message**, however many
there are: they go as fast as the subscriber takes them, a queue's worth at a
time.

### Sessions

While modbee runs, a device that drops off keeps its session - its
subscriptions, and the QoS 1 and 2 messages queued for it - for as long as it
asked, up to `broker: max-session-expiry`: **an hour** by default, whatever an
MQTT 5 client asks for, so a device that reboots or loses its link for a while
is sent what it missed. `4294967295` keeps sessions until modbee restarts. (A
3.1.1 client has no interval to ask for: with Clean Session 0 its session is
kept until the restart.) `broker: queue` (64 by default) is how many messages
are held for one device.

A restart ends every session, so devices subscribe again. This library's
client does that by itself; with another client, subscribe in its on-connect
handler, as its documentation advises. Messages that were queued for a device
while it was away are lost.

### Kafka

Every record the pipe wrote is on disk, in order, for the topic's retention
time - **seven days** by default - whether modbee, Kafka or the whole server
restarts. `docker compose down` keeps it; `docker compose down -v` deletes the
volume and the data with it.

## Building the image yourself

```bash
docker build -t modbee -f docker/Dockerfile .      # from the repository root
```

The compose files do the same with `docker compose up -d --build`, tagging the
result with the name in their `image:`.

It builds the Kafka client library (librdkafka) from its release, wolfSSL
from a pinned release, and the broker from this source, and runs as an
unprivileged user.

**MQTT's TLS is wolfSSL** - the engine a board runs, so a broker and a
controller fail the same way ([security.md](security.md)). OpenSSL is in the
image too, but only for librdkafka's TLS to Kafka. To build the broker on
OpenSSL instead:

```bash
docker build --build-arg TLS_ENGINE=openssl -t modbee -f docker/Dockerfile .
```

Which engine an image has, three ways: `docker run --rm <image> --version`
prints `modbee 0.1.0 (TLS: wolfSSL)`; `modbee serve`'s start-up line says
`tls 8883 (wolfSSL, ...)`; and the image is labelled,
`docker inspect -f '{{index .Config.Labels "io.modbee.tls-engine"}}' <image>`.
TLS 1.2 and 1.3 only. The published
image is built the same way by `.github/workflows/docker.yml`, which starts it
and publishes a message through it before anything is pushed.

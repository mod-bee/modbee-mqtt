# Your own app, through Kafka

Your devices speak MQTT. Your app - a dashboard, a rules engine, a database
writer, written in Node.js, Python, Java or anything else - does not have to.
It reads and writes **Kafka**, and modbee carries everything between the two:

```
 devices ──MQTT──► modbee ──► Kafka topic "mqtt-messages" ──► your app
 devices ◄──MQTT── modbee ◄── Kafka topic "mqtt-commands" ◄── your app
```

This page is for building that app on [`docker/all-in-one`](docker.md#everything-in-one-file-broker-certificate-and-kafka),
which sets all of this up as it ships. It assumes no Kafka experience.

**Why go through Kafka rather than subscribe to MQTT directly?** Kafka keeps
every message (seven days by default), so an app that was down catches up when
it starts again, a new app can read last week's data, and any number of apps
can each read everything without affecting each other or the devices. For one
small script, subscribing to MQTT is simpler, and that works too.

---

## Kafka in one minute

Three words are all you need:

- **Topic** - a named list of records that Kafka keeps on disk, in order. Here
  there are two: `mqtt-messages` (from your devices) and `mqtt-commands` (to
  them). Reading a record does not remove it.
- **Record** - one message: a **key**, a **value**, and optional **headers**
  (name/value pairs). modbee turns each MQTT message into one record.
- **Consumer group** - the name your app reads under (`groupId`). Kafka
  remembers, per group, how far it has read, so a restarted app carries on
  where it stopped. Two copies of an app with the **same** group share the
  work; apps with **different** groups each get every record.

---

## What arrives from your devices

Every MQTT message becomes one record in **`mqtt-messages`**:

| Part of the record | What it holds | Example |
| --- | --- | --- |
| **key** | the MQTT topic | `plant/tank-01/level` |
| **value** | the payload, byte for byte as the device sent it | `4.25` |
| header `mqtt-qos` | the QoS it was published at | `1` |
| header `mqtt-retain` | whether it was a retained message | `false` |
| header `mqtt-clientid` | which device published it (none if an app sent it through Kafka) | `tank-01` |
| headers `mqtt-content-type`, `mqtt-response-topic`, `mqtt-correlation-data`, `mqtt-message-expiry-interval`, `mqtt-payload-format-indicator` | MQTT 5 properties, when the device set them | `application/json` |
| any other header | the device's MQTT 5 user properties, by name | `site` = `nz` |

The **value is bytes**: text for a reading like `4.25`, JSON if your devices
send JSON (parse it in your app), and binary for Sparkplug B (decode it with a
Sparkplug library, such as `sparkplug-payload` for Node.js or Eclipse Tahu for
Python).

**Order:** messages on one MQTT topic arrive in the order they were published,
because they share a key. Messages on different topics may interleave.

## Sending to your devices

Write a record to **`mqtt-commands`**:

| Part of the record | What to put in it |
| --- | --- |
| **key** | the MQTT topic to publish on, e.g. `plant/pump-2/cmd` |
| **value** | the payload, e.g. `stop` |

modbee publishes it to MQTT at QoS 1, not retained, and every device
subscribed to that topic receives it. Both rules - the key as the topic, QoS
1 - are the `kafka-to-mqtt` mapping in `docker/all-in-one/modbee.yaml`, and
[kafka.md](kafka.md#kafka--mqtt) shows how to change them (a fixed topic, a
different QoS, retained, properties).

You will also see your own command come back on `mqtt-messages` - everything
published on MQTT goes there, including what apps send. It has no
`mqtt-clientid` header, which is how you tell it apart. It never loops: modbee
does not write a message back to the Kafka topic it came from.

## Where your app connects

| Your app runs | Kafka address (`bootstrap servers`) |
| --- | --- |
| In a container on the stack's network, `modbee` | `kafka:9092` |
| On the same computer as the stack, outside Docker | `localhost:9094` |
| On another machine on your network, or over a VPN | `<the server's address>:9094`, after [two lines in `.env`](docker.md#from-another-machine-on-your-network) |
| **Anywhere** - another site, the cloud | `mqtt.example.com:9093`, with TLS and a login: set `KAFKA_PASSWORD` in `.env` |

The first three need no password, so they stay on the server, or on your own
network. Over the internet, use port 9093: set a password in `.env` - nothing
to build or install - and connect like this:

```js
const kafka = new Kafka({
  kafkaJS: {
    brokers: ["mqtt.example.com:9093"],
    ssl: true,
    sasl: { mechanism: "plain", username: "app", password: process.env.KAFKA_PASSWORD },
  },
});
```

[docker.md](docker.md#from-anywhere-with-a-password) has the details, and the
same settings for Python and any other client.

---

## A complete example in Node.js

[`examples/kafka/nodejs`](../examples/kafka/nodejs) is a small app that reads
every MQTT message and runs one rule: when the tank level on
`plant/tank-01/level` goes above 8 it sends `stop` to `plant/pump-2/cmd`, and
below 2 it sends `start`. It uses
[`@confluentinc/kafka-javascript`](https://www.npmjs.com/package/@confluentinc/kafka-javascript),
Confluent's official Node.js client, through its KafkaJS-style API.

### Run it

With `docker/all-in-one` running, from the example's folder:

```bash
cd examples/kafka/nodejs
docker compose up -d --build       # joins the stack's network; Kafka is kafka:9092
docker compose logs -f
```

Or, with [Node.js](https://nodejs.org) installed on the same computer:

```bash
npm install
node tank-rule.js                  # uses localhost:9094
```

### Try it

Act as two devices, with the `modbee` command that is inside the stack. In one
terminal, be the pump, listening for its commands:

```bash
cd docker/all-in-one
docker compose exec modbee modbee sub -t plant/pump-2/cmd -v
```

In a second terminal, be the tank, publishing its level:

```bash
cd docker/all-in-one
docker compose exec modbee modbee pub -t plant/tank-01/level -m 8.6 -q 1
docker compose exec modbee modbee pub -t plant/tank-01/level -m 1.5 -q 1
```

The pump's terminal prints `plant/pump-2/cmd stop`, then
`plant/pump-2/cmd start`, and the app's log (`docker compose logs -f` in the
example's folder) shows each step:

```
connected to Kafka at kafka:9092 - waiting for MQTT messages
<- MQTT plant/tank-01/level: 8.6  (QoS 1, from modbee-pub-178)
-> MQTT plant/pump-2/cmd: stop
<- MQTT plant/pump-2/cmd: stop  (QoS 1, from a Kafka app)
<- MQTT plant/tank-01/level: 1.5  (QoS 1, from modbee-pub-199)
-> MQTT plant/pump-2/cmd: start
<- MQTT plant/pump-2/cmd: start  (QoS 1, from a Kafka app)
```

### The parts that matter

Connecting, and reading under a group:

```js
const { Kafka, logLevel } = require("@confluentinc/kafka-javascript").KafkaJS;

const kafka = new Kafka({ kafkaJS: { brokers: ["localhost:9094"], logLevel: logLevel.WARN } });
const consumer = kafka.consumer({ kafkaJS: { groupId: "tank-rule", fromBeginning: true } });
const producer = kafka.producer();
```

`fromBeginning: true` means the first time this group runs, it starts with
everything Kafka still holds (up to seven days); after that it carries on from
where it stopped. Use `false` to start with new messages only.

Reading an MQTT message:

```js
await consumer.subscribe({ topics: ["mqtt-messages"] });
await consumer.run({
  eachMessage: async ({ message }) => {
    const mqttTopic = message.key.toString();     // plant/tank-01/level
    const payload = message.value.toString();     // 4.25
    const device = message.headers["mqtt-clientid"]?.toString();
  },
});
```

Sending one:

```js
await producer.send({
  topic: "mqtt-commands",
  messages: [{ key: "plant/pump-2/cmd", value: "stop" }],
});
```

---

## The same in Python

With [`confluent-kafka`](https://pypi.org/project/confluent-kafka/)
(`pip install confluent-kafka`):

```python
from confluent_kafka import Consumer, Producer

consumer = Consumer({
    "bootstrap.servers": "localhost:9094",
    "group.id": "my-python-app",
    "auto.offset.reset": "earliest",   # a new group starts with what is already there
})
producer = Producer({"bootstrap.servers": "localhost:9094"})
consumer.subscribe(["mqtt-messages"])

while True:
    record = consumer.poll(1.0)
    if record is None or record.error():
        continue
    mqtt_topic = record.key().decode()     # the MQTT topic
    payload = record.value()               # the MQTT payload, as bytes
    headers = {name: value.decode() for name, value in (record.headers() or [])}
    print(mqtt_topic, payload.decode(errors="replace"), headers.get("mqtt-qos"))

    if mqtt_topic == "plant/tank-01/level" and float(payload) > 8:
        producer.produce("mqtt-commands", key="plant/pump-2/cmd", value="stop")
        producer.flush()
```

**Any other language** works the same way with any Kafka client: connect to
the address above, read `mqtt-messages` under a group id of your choosing, and
write records keyed by MQTT topic to `mqtt-commands`.

---

## Things that catch people out

- **The two addresses are not interchangeable.** From your computer, outside
  Docker, it is `localhost:9094` - port 9092 is not open outside Docker, so
  `localhost:9092` is refused. From a container, it is `kafka:9092` -
  `localhost` inside a container is the container itself. Kafka tells each
  client which address to use from then on, and each of the two is only right
  for one side.
- **An app in another compose file cannot find `kafka`** until it joins the
  `modbee` network - the three lines at the bottom of
  [the example's compose.yml](../examples/kafka/nodejs/compose.yml). Start the
  stack first, so the network exists.
- **A record may arrive twice** after your app or Kafka restarts: Kafka
  guarantees each record at least once, not exactly once. Make what your app
  does safe to repeat - setting a value is; adding to a total is not.
- **A new Kafka topic for commands must exist before anything is written to
  it**, or the first record is missed. `all-in-one` creates `mqtt-commands`;
  if you add another, add it to the `kafka-topics` service in `compose.yml`
  ([kafka.md](kafka.md#kafka--mqtt)).
- **Which MQTT topics reach Kafka, and which Kafka topics reach MQTT, is
  configured in `modbee.yaml`.** As shipped, every MQTT message goes to
  `mqtt-messages`. To send only `plant/#`, or to split topics across several
  Kafka topics, edit the mappings - [kafka.md](kafka.md) has every option -
  and run `docker compose restart modbee`.
- **Many Node.js examples online use the `kafkajs` package.** It works, but it
  is no longer actively developed; Confluent's client used here runs the same
  style of code.

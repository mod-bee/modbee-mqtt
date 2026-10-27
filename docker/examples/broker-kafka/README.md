# The broker and Kafka on your own network

modbee and one Kafka node, for a LAN, a VPN or a private cloud network. Every
MQTT message lands in Kafka, Kafka apps (a SCADA, a database writer) send
commands back to devices, and Kafka knows which devices are online.

**Not for the open internet** - no TLS, and Kafka has no password here. On the
internet use [`../traefik-kafka`](../traefik-kafka) or [`../npm-kafka`](../npm-kafka).

## Start it

You need a Linux server with Docker (4 GB of memory is plenty), and these four files.

```bash
cp .env.example .env        # set MQTT_PASSWORD (a long one)
docker compose up -d
docker compose ps           # kafka-topics "exited (0)", modbee and kafka "running"
```

| Who | Connects to |
| --- | --- |
| Devices | `mqtt://<this server>:1883` |
| Apps in Docker, on the network `modbee` | MQTT `modbee:1883`, Kafka `kafka:9092` |
| Apps on this server | Kafka `localhost:9094` |
| Apps on other machines of your network | set `KAFKA_BIND` and `KAFKA_EXTERNAL_HOST` in `.env`, then Kafka `<this server>:9094` |

## What is in Kafka

| Topic | What |
| --- | --- |
| `mqtt-messages` | Every MQTT message. Value: the payload. Key: the publisher's client id. Headers `mqtt-topic`, `mqtt-qos`, `mqtt-retain`, `mqtt-clientid`, `mqtt-username`, ... |
| `modbee-events` | Clients connecting, disconnecting, subscribing ([docs/events.md](../../../docs/events.md)) |
| `mqtt-commands` | **You write here**, and modbee publishes it to MQTT: with the header `mqtt-topic` (optionally `mqtt-qos`, `mqtt-retain`) exactly so; without it, on the topic in the record's key |
| `modbee-retained` | modbee's own copy of the retained messages - leave it alone |

Building an app on it: [docs/kafka-apps.md](../../../docs/kafka-apps.md).

## Add a device

```bash
docker compose exec modbee modbee-mqtt user add tank-01 \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

It prints the device's password, once. Many devices of one kind can register
themselves with one key instead (`modbee-mqtt key create`):
[docs/logins.md](../../../docs/logins.md).

## Check it works

```bash
docker compose exec modbee modbee-mqtt pub -t plant/test -m hi -q 1    # "accepted": Kafka has it
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic mqtt-messages --from-beginning --max-messages 1
```

## Keep it running

| | |
| --- | --- |
| Upgrade | `docker compose pull && docker compose up -d` - logins, retained messages and Kafka's data stay (volumes) |
| Back up the logins | `docker compose exec modbee modbee-mqtt backup` - a copy in the volume's `backups` folder |
| Change a setting | edit `modbee.yaml`, then `docker compose restart modbee` |
| One Kafka node | no replication: a lost disk loses Kafka's data. For more, run your own Kafka cluster and point `bootstrap-servers` at it |

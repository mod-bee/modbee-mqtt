# The broker and Kafka on the internet, with Traefik

modbee, one Kafka node and Traefik on one server: MQTT over TLS on port 8883,
Kafka over TLS with a password on port 9093, both on a free Let's Encrypt
certificate fetched and renewed by itself. Devices anywhere publish; your SCADA
or other Kafka apps, anywhere, read every message, send commands back, and know
which devices are online.

Already run Nginx Proxy Manager on the server? Use [`../npm-kafka`](../npm-kafka).

## Start it

You need a Linux server with Docker (4 GB of memory is plenty), a DNS name
pointing at it, and ports 80, 8883 and 9093 open to the internet.

```bash
cp .env.example .env        # set DOMAIN, EMAIL, MQTT_PASSWORD and KAFKA_PASSWORD
docker compose up -d
docker compose ps           # kafka-topics "exited (0)"; the others "running"
docker compose logs traefik # the certificate arrives within a minute
```

| Who | Connects to |
| --- | --- |
| Devices | `mqtts://<DOMAIN>:8883` |
| Kafka apps anywhere | `<DOMAIN>:9093`, SASL_SSL, mechanism PLAIN, user `app` (`KAFKA_USERNAME`) and `KAFKA_PASSWORD` |
| Apps in Docker on this server, network `modbee` | MQTT `modbee:1883`, Kafka `kafka:9092` |
| Apps on this server | Kafka `localhost:9094` (no password) |

First time? Set `ACME_SERVER` in `.env` to Let's Encrypt's staging server
while you try it out, and remove it when it works.

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
docker compose exec modbee modbee user add tank-01 \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

It prints the device's password, once. Many devices of one kind can register
themselves with one key instead (`modbee key create`):
[docs/logins.md](../../../docs/logins.md).

## Good to know

- **Traefik ends TLS**, and passes plain MQTT to modbee. So devices cannot sign
  in with a client certificate here, and must not pin the certificate's key:
  every renewal (about every 60 days) brings a new one. They check the
  certificate against the public roots instead.
- **Port 80** must stay open: Let's Encrypt checks it at every renewal.
- **Kafka's port 9094 has no password.** It is published on this server only
  (`KAFKA_BIND=127.0.0.1`). Never open it to the internet; use 9093.

## Keep it running

| | |
| --- | --- |
| Upgrade | `docker compose pull && docker compose up -d` - logins, retained messages, Kafka's data and the certificate stay (volumes) |
| Back up the logins | `docker compose exec modbee modbee backup` - a copy in the volume's `backups` folder |
| Change a setting | edit `modbee.yaml`, then `docker compose restart modbee` |
| One Kafka node | no replication: a lost disk loses Kafka's data. For more, run your own Kafka cluster and point `bootstrap-servers` at it |

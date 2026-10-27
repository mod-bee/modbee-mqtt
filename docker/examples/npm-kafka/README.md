# The broker and Kafka behind Nginx Proxy Manager

For a server that already runs Nginx Proxy Manager (NPM). NPM does the TLS with
a Let's Encrypt certificate it already looks after: MQTT for devices on port
8883, Kafka with a password on port 9093 for your SCADA or other Kafka apps,
anywhere. Sized for a small server (1 CPU, 2 GB).

No NPM? Use [`../traefik-kafka`](../traefik-kafka).

## Start it

You need Docker and NPM on the server, a DNS name pointing at it with a Let's
Encrypt certificate in NPM, and ports 8883 and 9093 open to the internet.

```bash
cp .env.example .env        # set DOMAIN, MQTT_PASSWORD and KAFKA_PASSWORD
docker compose up -d
docker compose ps           # kafka-topics "exited (0)"; modbee and kafka "running"
```

modbee is now at `10.80.80.10:1883` and Kafka's password-protected listener at
`10.80.80.11:9095`, on the Docker network `modbee`. Kafka's listener without a
password is on a second network, `modbee-internal`, that only modbee shares -
connect nothing else to it.

**If NPM runs in Docker**, let it reach that network (once):

```bash
docker network connect modbee <npm container>    # e.g. nginx-proxy-manager-app-1
```

**In NPM**, add two Streams (Hosts → Streams → Add Stream), each with TCP
Forwarding on and your domain's certificate on the SSL tab:

| Incoming Port | Forward Host | Forward Port | For |
| --- | --- | --- | --- |
| `8883` | `10.80.80.10` | `1883` | MQTT - devices |
| `9093` | `10.80.80.11` | `9095` | Kafka - your SCADA |

NPM's own container must publish both ports (`ports:` in its compose file).

| Who | Connects to |
| --- | --- |
| Devices | `mqtts://<DOMAIN>:8883` |
| Kafka apps anywhere | `<DOMAIN>:9093`, SASL_SSL, mechanism PLAIN, user `app` (`KAFKA_USERNAME`) and `KAFKA_PASSWORD` |
| The admin login, from anywhere | `mqtts://<DOMAIN>:8883` as `admin` (`MQTT_USERNAME`) and `MQTT_PASSWORD` |

## What is in Kafka

| Topic | What |
| --- | --- |
| `mqtt-messages` | Every MQTT message. Value: the payload. Key: the publisher's client id. Headers `mqtt-topic`, `mqtt-qos`, `mqtt-retain`, `mqtt-clientid`, `mqtt-username`, ... |
| `modbee-events` | Clients connecting, disconnecting, subscribing ([docs/events.md](../../../docs/events.md)) - how a SCADA knows which devices are online |
| `mqtt-commands` | **You write here**, and modbee publishes it to MQTT: with the header `mqtt-topic` (optionally `mqtt-qos`, `mqtt-retain`) exactly so; without it, on the topic in the record's key |
| `modbee-retained` | modbee's own copy of the retained messages - leave it alone |

The Kafka login (`KAFKA_USERNAME`) may read `mqtt-messages` and
`modbee-events`, write `mqtt-commands`, and use consumer groups - nothing
else: it cannot delete a topic, nor write `modbee-retained`. A record on
`mqtt-commands` can publish to any topic devices use, but never to a `$`
topic: those are the broker's own (`$modbee/admin/...`, `$modbee/events/...`).
Give a new topic its ACL in `kafka-topics` (compose.yml) when your apps need it.

## Add a device

```bash
docker compose exec modbee modbee-mqtt user add tank-01 \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

It prints the device's password, once. Many devices of one kind can register
themselves with one key instead (`modbee-mqtt key create`):
[docs/logins.md](../../../docs/logins.md).

## Good to know

- **NPM ends TLS**, and passes plain MQTT to modbee. So devices cannot sign in
  with a client certificate here, and must not pin the certificate's key: every
  renewal brings a new one. They check the certificate against the public roots.
- **Metrics** (port 9464) stay inside Docker: they are not forwarded.
- **Limit connections per address in NPM.** modbee sees every device arrive
  from NPM's address, so only NPM can tell one source from another. NPM
  includes custom files in its stream block; for example, in NPM's data
  folder, `nginx/custom/stream.conf`:

  ```nginx
  limit_conn_zone $binary_remote_addr zone=per_address:10m;
  ```

  and `nginx/custom/server_stream_tcp.conf` (every TCP stream):

  ```nginx
  limit_conn per_address 50;
  ```

  then restart NPM. Choose the number from how many devices sit behind one
  address - a site router, a cellular carrier's NAT.

## Keep it running

| | |
| --- | --- |
| Upgrade | `docker compose pull && docker compose up -d` - logins, retained messages and Kafka's data stay (volumes) |
| Back up the logins | `docker compose exec modbee modbee-mqtt backup` - a copy in the volume's `backups` folder |
| Change a setting | edit `modbee.yaml`, then `docker compose restart modbee` |
| One Kafka node | no replication: a lost disk loses Kafka's data |

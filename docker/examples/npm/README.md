# The broker behind Nginx Proxy Manager

For a server that already runs Nginx Proxy Manager (NPM) for its websites. NPM
does the TLS with a Let's Encrypt certificate it already looks after, and
forwards MQTT to modbee. Devices anywhere connect to `mqtts://<your domain>:8883`.

No NPM? Use [`../traefik`](../traefik). Want the messages in Kafka too? Use
[`../npm-kafka`](../npm-kafka).

## Start it

You need Docker and NPM on the server, a DNS name pointing at it with a Let's
Encrypt certificate in NPM, and port 8883 open to the internet.

```bash
cp .env.example .env        # set MQTT_PASSWORD (a long one)
docker compose up -d
```

modbee is now at `10.80.80.10:1883`, on the Docker network `modbee`.

**If NPM runs in Docker**, let it reach that network (once):

```bash
docker network connect modbee <npm container>    # e.g. nginx-proxy-manager-app-1
```

**In NPM**, add a Stream (Hosts → Streams → Add Stream):

| Field | Value |
| --- | --- |
| Incoming Port | `8883` |
| Forward Host | `10.80.80.10` |
| Forward Port | `1883` |
| TCP Forwarding | on |
| SSL tab | your domain's certificate |

NPM's own container must publish port 8883 (`ports: - "8883:8883"` in its
compose file) - add it and restart NPM if it is not there.

## Add a device

```bash
docker compose exec modbee modbee user add tank-01 \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

It prints the device's password, once. Many devices of one kind can register
themselves with one key instead (`modbee key create`):
[docs/logins.md](../../../docs/logins.md).

## Check it works

From any computer with Docker:

```bash
docker run --rm ghcr.io/mod-bee/modbee-mqtt pub --tls -h <your domain> \
  -u admin -P '<MQTT_PASSWORD>' -t plant/test -m hi -q 1
```

## Good to know

- **NPM ends TLS**, and passes plain MQTT to modbee. So devices cannot sign in
  with a client certificate here, and must not pin the certificate's key: every
  renewal brings a new one. They check the certificate against the public roots.

## Keep it running

| | |
| --- | --- |
| Upgrade | `docker compose pull && docker compose up -d` - logins and retained messages stay (volume `modbee-data`) |
| Back up the logins | `docker compose exec modbee modbee backup` - a copy in the volume's `backups` folder |
| Change a setting | edit `modbee.yaml`, then `docker compose restart modbee` |

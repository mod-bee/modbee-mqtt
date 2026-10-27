# The broker on the internet, with Traefik

modbee behind Traefik: MQTT over TLS on port 8883 with a free Let's Encrypt
certificate, fetched and renewed by itself. For devices anywhere - on cellular,
at customers' sites - talking to your server.

Already run Nginx Proxy Manager on the server? Use [`../npm`](../npm) instead.
Want the messages in Kafka too? Use [`../traefik-kafka`](../traefik-kafka).

## Start it

You need a Linux server with Docker, a DNS name pointing at it, and ports 80
and 8883 open to the internet.

```bash
cp .env.example .env        # set DOMAIN, EMAIL and MQTT_PASSWORD
docker compose up -d
docker compose logs traefik # the certificate arrives within a minute
```

Devices connect to `mqtts://<DOMAIN>:8883` and check the certificate against
the usual public roots (Let's Encrypt's ISRG Root X1).

First time? Set `ACME_SERVER` in `.env` to Let's Encrypt's staging server
while you try it out - its limits are generous - and remove it when it works.

## Add a device

```bash
docker compose exec modbee modbee-mqtt user add tank-01 \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

It prints the device's password, once. Many devices of one kind can register
themselves with one key instead (`modbee-mqtt key create`):
[docs/logins.md](../../../docs/logins.md).

## Check it works

From any computer with Docker:

```bash
docker run --rm ghcr.io/mod-bee/modbee-mqtt pub --tls -h <DOMAIN> \
  -u admin -P '<MQTT_PASSWORD>' -t plant/test -m hi -q 1
```

## Good to know

- **Traefik ends TLS**, and passes plain MQTT to modbee. So devices cannot sign
  in with a client certificate here, and must not pin the certificate's key:
  every renewal (about every 60 days) brings a new one. They check the
  certificate against the public roots instead.
- **Port 80** must stay open: Let's Encrypt checks it at every renewal.

## Keep it running

| | |
| --- | --- |
| Upgrade | `docker compose pull && docker compose up -d` - logins, retained messages and the certificate stay (volumes) |
| Back up the logins | `docker compose exec modbee modbee-mqtt backup` - a copy in the volume's `backups` folder |
| Change a setting | edit `modbee.yaml`, then `docker compose restart modbee` |
| Your own app in Docker | join the network `modbee` and connect to `modbee:1883` (plain, inside the server) |

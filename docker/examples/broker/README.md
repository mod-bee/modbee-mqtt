# The broker on your own network

modbee alone, plain MQTT on port 1883, for devices on a LAN, a VPN or a private
cloud network. Every device has its own login and may use only the topics you
give it.

**Not for the open internet** - there is no TLS here. On the internet use
[`../traefik`](../traefik) (it gets a free certificate) or [`../npm`](../npm)
(you already run Nginx Proxy Manager). Want the messages in Kafka too? Use
[`../broker-kafka`](../broker-kafka).

## Start it

You need a Linux server with Docker, and these four files.

```bash
cp .env.example .env        # set MQTT_PASSWORD (a long one)
docker compose up -d
docker compose logs modbee  # "modbee-mqtt serve: ... on 0.0.0.0 port 1883"
```

Devices connect to `mqtt://<this server>:1883`.

## Add a device

```bash
docker compose exec modbee modbee-mqtt user add tank-01 \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

It prints the device's password, once. `tank-01` may then publish under
`plant/tank-01/...` and receive `plant/tank-01/cmd`, nothing else. Rules and
everything else about logins: [docs/logins.md](../../../docs/logins.md).

Many devices of one kind? Let them register themselves with one key instead:

```bash
docker compose exec modbee modbee-mqtt key create site-north --site north \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

Each device signs in once with that key and its own name, and is given its own
login ([docs/logins.md](../../../docs/logins.md#devices-that-register-themselves)).

## Check it works

```bash
docker compose exec modbee modbee-mqtt sub -t 'plant/#' -v      # in one terminal
docker compose exec modbee modbee-mqtt pub -t plant/test -m hi  # in another
```

Inside the container `modbee-mqtt` uses the admin login by itself. From another
computer add `-h <server> -u admin -P '<MQTT_PASSWORD>'`.

## Keep it running

| | |
| --- | --- |
| Upgrade | `docker compose pull && docker compose up -d` - logins and retained messages stay (volume `modbee-data`) |
| Back up the logins | `docker compose exec modbee modbee-mqtt backup` - a copy in the volume's `backups` folder |
| Change a setting | edit `modbee.yaml`, then `docker compose restart modbee` |
| Your own app in Docker | join the network `modbee` and connect to `modbee:1883` |

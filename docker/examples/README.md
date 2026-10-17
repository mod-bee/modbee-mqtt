# Running modbee in production

Pick the folder that matches your server, copy its four files (`compose.yml`,
`modbee.yaml`, `.env.example`, `README.md`) anywhere, and follow its README.
Each one runs the published image and needs nothing else from this repository.

| Your server | Without Kafka | With Kafka |
| --- | --- | --- |
| **Your own network** - a LAN, a VPN or a private cloud network; plain MQTT | [`broker`](broker) | [`broker-kafka`](broker-kafka) |
| **On the internet, nothing else running** - Traefik gets a free certificate | [`traefik`](traefik) | [`traefik-kafka`](traefik-kafka) |
| **On the internet, Nginx Proxy Manager already running** - it does the TLS | [`npm`](npm) | [`npm-kafka`](npm-kafka) |

**With Kafka** means every MQTT message lands in Kafka, Kafka apps (a SCADA, a
historian) send commands back to devices, and Kafka hears which devices are
online. The ModBee platform needs this.

## The same in all six

- **Every device has its own login**, and may use only the topics you give it
  - nothing is open by default. The admin login is in `.env`.
- **Logins and retained messages survive** restarts and upgrades (volumes).
- **Upgrade** with `docker compose pull && docker compose up -d`.
- **Every setting** is in `modbee.yaml`, explained line by line;
  `docker compose exec modbee modbee check` validates it.

## Download one folder with curl

```bash
mkdir modbee && cd modbee
for f in compose.yml modbee.yaml .env.example README.md; do
  curl -fsSL -o "$f" "https://raw.githubusercontent.com/mod-bee/modbee-mqtt/main/docker/examples/npm-kafka/$f"
done
```

Change `npm-kafka` to the folder you picked. Running it again downloads the
newest files over the old ones; your `.env` is never touched.

Just trying modbee on your own computer? [`../compose.yml`](../compose.yml) starts
modbee and Kafka with no logins, for a first look - not for production.

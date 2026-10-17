# modbee + Kafka with Nginx Proxy Manager

```bash
cp .env.example .env      # set your domain and the two passwords (user names optional)
docker compose up -d
```

| Container | Address |
| --- | --- |
| modbee (MQTT) | 10.80.80.10:1883 |
| kafka | 10.80.80.11:9095 (with the Kafka password) |

In NPM, with a Let's Encrypt certificate for your domain:

- Stream **8883** → `10.80.80.10` port `1883`, SSL: the certificate. MQTT for the sites.
- Stream **9093** → `10.80.80.11` port `9095`, SSL: the certificate. Kafka for your SCADA (user `KAFKA_USERNAME`, default `app`).

NPM must be able to reach the `modbee` network. If NPM runs in Docker:
`docker network connect modbee <npm container>` once.

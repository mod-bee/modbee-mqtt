# modbee behind Nginx Proxy Manager

## On the server

```bash
cp .env.example .env      # set MQTT_PASSWORD and KAFKA_PASSWORD
docker compose up -d
```

Keep ports 1883 and 9095 closed in the server's firewall. They are not
encrypted; NPM forwards to them.

## In NPM

1. **SSL Certificates → Add → Let's Encrypt**: `broker.example.com`.
2. **Streams → Add Stream**: incoming `8883` → the server, port `1883`, TCP.
   SSL tab: the certificate. This is MQTT for the sites.
3. **Streams → Add Stream**: incoming `9093` → the server, port `9095`, TCP.
   SSL tab: the same certificate. This is Kafka for your SCADA.

Open 8883 and 9093 in the firewall.

## Who connects where

| Who | Connects to | Login |
| --- | --- | --- |
| Sites (PLCs) | `broker.example.com:8883`, MQTT with TLS | their own login |
| SCADA on another machine | `broker.example.com:9093`, Kafka with TLS (SASL_SSL, PLAIN) | `app` / KAFKA_PASSWORD |
| SCADA on this server, in Docker | `kafka:9092` on the `modbee` network | none |

One domain and one certificate for both.

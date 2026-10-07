// Subscribing from a host.
//
//     modbee_subscribe <broker> [port] [filter]
//     modbee_subscribe localhost 1883 'plant/+/pressure'

#include <ModBeeMqtt.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>

static volatile sig_atomic_t g_running = 1;
static void stop(int) { g_running = 0; }

static void onMessage(void*, const MqttMessage& message) {
    // The spans point into the receive buffer and are valid ONLY for this
    // call. Printing is fine; keeping one is not, and anything that must
    // outlive the handler copies what it needs here.
    std::printf("%.*s  (%u bytes, QoS %u%s)\n",
                (int)message.topic.length, (const char*)message.topic.data,
                message.payload.length, message.qos,
                message.retain ? ", retained" : "");

    // A retained message is a STORED value, not a live one. An alarm panel
    // that cannot tell the difference shows every historical fault as new the
    // moment it reconnects.
    if (message.payload.length && message.payload.length < 200) {
        std::printf("   %.*s\n", (int)message.payload.length,
                    (const char*)message.payload.data);
    }

    // 5.0 only. Null on a 3.1.1 connection, where there are no properties.
    if (message.properties && message.properties->has(MQ_PROP_CONTENT_TYPE)) {
        std::printf("   content type: %.*s\n",
                    (int)message.properties->contentType.length,
                    (const char*)message.properties->contentType.data);
    }
}

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);

    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <broker> [port] [filter]\n", argv[0]);
        return 2;
    }
    const char* host = argv[1];
    const uint16_t port = (uint16_t)(argc > 2 ? atoi(argv[2]) : MQ_PORT_TCP);
    const char* filter = argc > 3 ? argv[3] : "plant/#";

    MqttTcpTransport transport;
    transport.configure(host, port);

    MqttConnectionConfig connection;
    connection.host = host;
    connection.port = port;
    connection.clientId = "modbee-subscribe";
    connection.version = MqttVersion::V5;
    connection.fallbackEnabled = true;
    connection.keepAlive = 30;

    MqttClient mqtt;
    mqtt.begin(transport, connection);
    mqtt.onMessage(&onMessage, nullptr);

    // Subscribing before the connection exists is RECORDED, not refused. It is
    // a standing intent the library owns, and it is re-established on every
    // reconnect where the peer reports no session.
    const MqttResult queued = mqtt.subscribe(filter, MQ_QOS_1);
    if (queued != MqttResult::Queued) {
        std::fprintf(stderr, "cannot subscribe to %s: %s\n",
                     filter, mq_resultName(queued));
        return 2;
    }

    mqtt.connect();
    std::printf("watching %s on %s:%u\n", filter, host, port);

    while (g_running) {
        mqtt.poll();
        mq_yield();
    }

    std::printf("\nreceived %u messages\n", mqtt.status().received);
    mqtt.disconnect();
    for (int i = 0; i < 100; i++) mqtt.poll();
    mqtt.end();
    return 0;
}

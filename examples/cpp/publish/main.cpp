// Publishing from a host.
//
//     modbee_publish <broker> [port] [topic] [qos]
//     modbee_publish localhost 1883 plant/line3/pressure 1
//
// Nothing here blocks, including connect(). The loop below is what a scan
// cycle looks like: poll, do your own work, poll again.

#include <ModBeeMqtt.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static volatile sig_atomic_t g_running = 1;
static void stop(int) { g_running = 0; }

static void onPublished(void*, MqttResult result, uint16_t packetId) {
    // Every accepted publish reaches exactly one conclusion. A caller holding
    // a slot per publish gets every one of them back - including the ones that
    // were superseded, which is what stops a lost setpoint looking exactly
    // like a delivered one.
    std::printf("  publish %u: %s\n", packetId, mq_resultName(result));
}

static void onState(void*, MqttState state, MqttResult reason) {
    std::printf("  state: %s%s%s\n", mq_stateName(state),
                reason == MqttResult::Ok ? "" : " - ",
                reason == MqttResult::Ok ? "" : mq_resultName(reason));
}

int main(int argc, char** argv) {
    // Line buffered on purpose. Output to a pipe - a service manager, a
    // container log, a redirect - is block buffered by default, so a program
    // killed before its buffer fills appears to have said nothing at all. On a
    // device diagnosed from its log that is the difference between a fault you
    // can see and one you cannot.
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);

    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <broker> [port] [topic] [qos]\n", argv[0]);
        return 2;
    }
    const char* host = argv[1];
    const uint16_t port = (uint16_t)(argc > 2 ? atoi(argv[2]) : MQ_PORT_TCP);
    const char* topic = argc > 3 ? argv[3] : "plant/line3/pressure";
    const uint8_t qos = (uint8_t)(argc > 4 ? atoi(argv[4]) : 1);

    MqttTcpTransport transport;
    transport.configure(host, port);

    MqttConnectionConfig connection;
    connection.host = host;
    connection.port = port;
    connection.clientId = "modbee-publish";
    connection.version = MqttVersion::V5;
    connection.fallbackEnabled = true;          // talks to an old broker too
    connection.keepAlive = 30;

    MqttClient mqtt;
    mqtt.begin(transport, connection);
    mqtt.onStateChange(&onState, nullptr);

    if (mqtt.connect() != MqttResult::Queued) {
        std::fprintf(stderr, "cannot start connecting: %s\n",
                     transport.lastError().c_str());
        return 1;
    }

    mq_time_t lastPublish = 0;
    double value = 0.0;

    while (g_running) {
        mqtt.poll();

        if (mqtt.isConnected() && mq_elapsed(lastPublish, mq_millis()) >= 1000) {
            lastPublish = mq_millis();
            value += 0.1;

            char payload[32];
            std::snprintf(payload, sizeof payload, "%.1f", value);

            const MqttResult queued =
                mqtt.publish(topic, payload, qos, false, &onPublished, nullptr);
            if (queued != MqttResult::Queued) {
                std::printf("  refused: %s\n", mq_resultName(queued));
            }
        }
        mq_yield();
    }

    std::printf("stopping\n");
    mqtt.disconnect();
    for (int i = 0; i < 100; i++) mqtt.poll();
    mqtt.end();
    return 0;
}

// A broker, in about fifty lines.
//
//     modbee_broker [port]
//     modbee_broker 1883
//
// Point mosquitto_sub at it and it works:
//
//     mosquitto_sub -h localhost -t 'plant/#' -q 1 -v
//     mosquitto_pub -h localhost -t plant/line3/pressure -m 3.2 -q 1
//
// WHAT THIS IS FOR. A plant panel that must keep working when the site link is
// down: the broker runs in the panel, every device on the line talks to it,
// and a bridge (see ../bridge) carries the few topics head office needs
// upstream. When the site link drops, the line keeps running.
//
#include <ModBeeMqtt.h>   // the broker is always compiled - no flag

#include <csignal>
#include <cstdio>
#include <cstdlib>

static volatile sig_atomic_t g_running = 1;
static void stop(int) { g_running = 0; }

// Told about every connection, so the log says who is on the line.
static void onConnection(void*, const MqttSpan& clientId, bool connected,
                         uint8_t reasonCode) {
    std::printf("  %-12s %.*s", connected ? "connected" : "disconnected",
                (int)clientId.length, (const char*)clientId.data);
    if (!connected && reasonCode != 0) {
        std::printf("  (%s)", mq_reasonName(reasonCode, MQ_PKT_DISCONNECT));
    }
    std::printf("\n");
}

// Every message that passes through, after routing.
static void onMessage(void*, const MqttSpan& clientId, const MqttSpan& topic,
                      const MqttSpan& payload, uint8_t qos, bool retain) {
    std::printf("  %.*s -> %.*s  (%u bytes, QoS %u%s)\n",
                (int)clientId.length, (const char*)clientId.data,
                (int)topic.length, (const char*)topic.data,
                (unsigned)payload.length, (unsigned)qos,
                retain ? ", retained" : "");
}

// WHO MAY DO WHAT.
//
// This one refuses anything under "safety/" to anybody but the safety
// controller - the kind of rule that belongs in a broker rather than in a
// hopeful comment on an operator panel.
static bool authorise(void*, const MqttBrokerRequest& request, uint8_t& reasonCode) {
    const bool isSafety = request.topic.length >= 7 &&
                          memcmp(request.topic.data, "safety/", 7) == 0;
    if (!isSafety) return true;
    if (mq_spanEquals(request.clientId, "safety-plc")) return true;

    reasonCode = MQ_REASON_NOT_AUTHORIZED;
    return false;
}

int main(int argc, char** argv) {
    // Line buffered on purpose: output to a pipe - a service manager, a
    // container log - is block buffered by default, so a process killed before
    // its buffer fills appears to have said nothing at all.
    std::setvbuf(stdout, nullptr, _IOLBF, BUFSIZ);   // a size: Windows aborts on 0
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);

    const uint16_t port = (uint16_t)(argc > 1 ? atoi(argv[1]) : MQ_PORT_TCP);

    MqttBrokerConfig config;
    config.maxConnections = 32;
    config.receiveBufferSize = 65536;      // ONE WHOLE PACKET
    config.transmitBufferSize = 65536;
    config.maximumPacketSize = 65536;      // stated in CONNACK, and enforced
    config.receiveMaximum = 20;
    config.serverKeepAliveMaximum = 120;   // a CEILING: a shorter ask is kept

    MqttTcpListener listener;
    listener.configure(nullptr, port, config.maxConnections);
    if (!listener.begin()) {
        std::fprintf(stderr, "cannot listen: %s\n", listener.lastError().c_str());
        return 1;
    }

    MqttBroker broker;
    broker.begin(config);
    broker.onConnection(onConnection, nullptr);
    broker.onMessage(onMessage, nullptr);
    broker.onAuthorise(authorise, nullptr);
    broker.addListener(listener);

    std::printf("listening on %u  (ctrl-c to stop)\n", (unsigned)listener.boundPort());

    while (g_running) {
        broker.poll();     // the whole contract; it never waits for anything
        mq_yield();
    }

    // Every client is told 0x8B Server shutting down rather than being left to
    // discover it by keepalive timeout one interval later.
    std::printf("\nstopping\n");
    broker.end();

    const MqttBrokerStatus& status = broker.status();
    // %llu for the three message counters: they are 64-bit because a 32-bit
    // running total of delivered messages wraps in hours at real rates.
    std::printf("accepted=%u  published=%llu  delivered=%llu  wills=%u  errors=%u\n",
                status.accepted,
                (unsigned long long)status.published,
                (unsigned long long)status.delivered,
                status.willsPublished, status.protocolErrors);
    return 0;
}

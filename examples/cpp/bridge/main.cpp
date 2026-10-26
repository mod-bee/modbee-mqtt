// A bridge between two brokers.
//
//     modbee_bridge <local-broker> <remote-broker> [remote-prefix]
//     modbee_bridge localhost site.example.com site7/
//
// Readings go up under "data/#" and arrive on the remote as
// "site7/data/...". Commands come down under "cmd/#" and arrive locally
// unchanged.
//
// THE THING TO UNDERSTAND BEFORE YOU CHANGE THE RULES. A bridge publishes what
// it receives, so if what it publishes comes back it publishes it again - and
// the two brokers spend the afternoon passing one message between them as fast
// as the link allows. That is not theoretical; it is the normal outcome of a
// bridge built without thinking about it.
//
// At 5.0 the No Local subscription option prevents it exactly. At 3.1.1 there
// is no such option, so the two directions need filters that cannot match each
// other's output - which is why this example uses "data/#" up and "cmd/#"
// down rather than one rule both ways. begin() checks, and REFUSES a
// configuration it cannot make safe.
//

#include <ModBeeMqtt.h>   // the bridge is always compiled - no flag

#include <csignal>
#include <cstdio>
#include <cstdlib>

static volatile sig_atomic_t g_running = 1;
static void stop(int) { g_running = 0; }

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, BUFSIZ);   // a size: Windows aborts on 0
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);

    if (argc < 3) {
        std::fprintf(stderr,
                     "usage: %s <local-broker> <remote-broker> [remote-prefix]\n",
                     argv[0]);
        return 2;
    }
    const char* localHost = argv[1];
    const char* remoteHost = argv[2];
    const char* prefix = argc > 3 ? argv[3] : "site/";

    MqttTcpTransport localLink, remoteLink;
    localLink.configure(localHost, MQ_PORT_TCP);
    remoteLink.configure(remoteHost, MQ_PORT_TCP);

    MqttBridgeConfig config;
    config.name = "panel-bridge";

    // THE TWO ENDS NEED DIFFERENT CLIENT IDENTIFIERS, and begin() enforces it.
    // One identifier on both ends would take over its own session the moment
    // the two brokers turned out to be the same box.
    config.local.clientId  = "panel-bridge-local";
    config.remote.clientId = "panel-bridge-remote";
    config.local.version = config.remote.version = MqttVersion::V5;
    config.local.keepAlive = config.remote.keepAlive = 30;
    config.behaviour.autoReconnect = true;
    config.behaviour.receiveBufferSize = 16384;
    config.behaviour.transmitBufferSize = 16384;

    MqttBridgeTopic up;
    up.filter = "data/#";
    up.direction = MqttBridgeDirection::Out;
    up.qos = MQ_QOS_1;
    up.remotePrefix = prefix;
    config.topics.push_back(up);

    MqttBridgeTopic down;
    down.filter = "cmd/#";
    down.direction = MqttBridgeDirection::In;
    down.qos = MQ_QOS_1;
    config.topics.push_back(down);

    MqttBridge bridge;
    const MqttResult began = bridge.begin(localLink, remoteLink, config);
    if (!mq_accepted(began)) {
        // The refusal says which two filters would have looped, in words you
        // can act on. That refusal is the feature.
        std::fprintf(stderr, "%s: %s\n", mq_resultName(began),
                     bridge.lastError().c_str());
        return 1;
    }

    bridge.start();
    std::printf("bridging %s -> %s\n", localHost, remoteHost);
    std::printf("  up:   data/#  ->  %sdata/...\n", prefix);
    std::printf("  down: cmd/#   <-  cmd/...\n");

    MqttBridgeState lastState = MqttBridgeState::Idle;
    while (g_running) {
        bridge.poll();

        if (bridge.state() != lastState) {
            lastState = bridge.state();
            // DEGRADED IS NOT DOWN, and saying so matters: one end down means
            // the panel still works and only what was crossing is lost. An
            // operator told "bridge down" goes looking for the wrong fault.
            std::printf("  %s  (local %s, remote %s)\n",
                        mq_bridgeStateName(lastState),
                        bridge.status().localUp ? "up" : "down",
                        bridge.status().remoteUp ? "up" : "down");
        }
        mq_yield();
    }

    bridge.stop();
    std::printf("\nout=%u  in=%u  refused out=%u  refused in=%u\n",
                bridge.status().forwardedOut, bridge.status().forwardedIn,
                bridge.status().refusedOut, bridge.status().refusedIn);
    return 0;
}

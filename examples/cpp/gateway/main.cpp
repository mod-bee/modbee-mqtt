// A SMALL GATEWAY, IN A CONTAINER
//
// The job: a Linux box - an industrial PC, a Raspberry Pi, an OpenPLC runtime -
// that watches a plant's MQTT traffic, and turns a pump off when a tank is
// full. The same class the Arduino examples use, because the only thing that
// differs between a board and a container is where the bytes go.
//
// Build:  cmake -S . -B build && cmake --build build
// Run:    ./build/examples/cpp/modbee_gateway [broker-host]

#include <ModBeeMqtt.h>

#include <chrono>
#include <cstdio>
#include <thread>

namespace {

// On a host there is no network channel to hand in: the sockets are already
// there. Everything else is identical to a sketch.
MqttDevice mqtt;

double tankLevel = 0.0;
bool tankLevelKnown = false;

const char* TOPIC_LEVEL   = "plant/tank-01/level";
const char* TOPIC_COMMAND = "plant/pump-2/command";

void onLevel(const char* topic, const MqttPayload& payload) {
    // -1 rather than 0 as the fallback, and then checked. A payload that is
    // not a number reading as zero would be an empty tank, which is a
    // perfectly plausible value and exactly why it is the wrong default.
    const double reading = payload.toNumber(-1.0);
    if (reading < 0.0) {
        std::printf("ignored a level that is not a number: \"%s\" on %s\n",
                    payload.text(), topic);
        return;
    }
    tankLevel = reading;
    tankLevelKnown = true;
}

void onConnected() {
    std::printf("connected: MQTT %s, keep alive %u s\n",
                mq_versionName(mqtt.activeVersion()),
                (unsigned)mqtt.activeKeepAlive());
}

void onDisconnected() {
    std::printf("link lost: %s\n", mqtt.lastErrorText());
}

} // namespace

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";

    mqtt.statusTopic("plant/gateway-1/status");
    mqtt.rememberSession(3600);
    mqtt.onConnected(&onConnected);
    mqtt.onDisconnected(&onDisconnected);
    mqtt.onMessage(TOPIC_LEVEL, &onLevel, MQ_QOS_1);

    if (!mqtt.connect(host, "gateway-1")) {
        std::printf("nowhere to connect to\n");
        return 1;
    }

    bool pumpCommanded = true;

    while (true) {
        mqtt.poll();

        // THE RULE THAT MAKES THIS SAFE: act only on a reading that is
        // actually current. MQTT will serve a retained level from a device
        // that died last Tuesday and say nothing about it, so a control
        // decision needs the age as well as the number.
        //
        // Thirty seconds, because that is what the tank promised. A different
        // device would deserve a different number - there is no constant here
        // that is right for both.
        if (mqtt.isFresh(TOPIC_LEVEL, 30000) && tankLevelKnown) {
            const bool wantPump = tankLevel < 8.0;      // stop at 8 m, start below
            if (wantPump != pumpCommanded) {
                // The command is recorded as issued ONLY if it was accepted.
                // Recording it first would leave this gateway believing it had
                // commanded a pump it never told - and, because the state then
                // matches, it would never try again.
                if (mqtt.publish(TOPIC_COMMAND, wantPump ? "start" : "stop",
                                 MQ_QOS_1, true)) {
                    pumpCommanded = wantPump;
                    std::printf("pump %s at %.2f m\n",
                                wantPump ? "start" : "stop", tankLevel);
                } else {
                    std::printf("pump command REFUSED (%s) - will retry\n",
                                mqtt.lastErrorText());
                }
            }
        } else if (tankLevelKnown) {
            // Stale. Say so, once, rather than acting on it or pretending
            // nothing is wrong.
            static bool complained = false;
            if (!complained) {
                complained = true;
                std::printf("tank level is stale (%u ms old) - not acting on it\n",
                            (unsigned)mqtt.sinceMessage(TOPIC_LEVEL));
            }
        }

        // poll() never waits, so something has to. On an OpenPLC runtime this
        // is the scan cycle; here it is a plain sleep.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

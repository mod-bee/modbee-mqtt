// A SPARKPLUG B EDGE NODE ON A HOST
//
// The job: a Linux box - a gateway, a Raspberry Pi, an OpenPLC runtime - that
// publishes a tank's level and a pump's state as Sparkplug B, so a SCADA
// system finds both with their types and units, and is told the moment the
// box drops off. The same class the Arduino sketch uses; on a host there is
// no network channel to supply, and the clock is already UTC.
//
// Build:  cmake -S . -B build && cmake --build build
// Run:    ./build/examples/cpp/modbee_sparkplug_node [broker-host[:port]]
// Watch:  ./build/examples/cpp/modbee_sparkplug_host [broker-host[:port]]

#include <ModBeeMqtt.h>

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

namespace {

MqttSparkplugNode node;

double setpoint = 2.0;

void onCommand(const MqttSparkplugCommand& command) {
    if (command.isRebirth) {
        std::printf("asked to re-announce, and did\n");
        return;
    }
    if (std::strcmp(command.name, "Tank/Setpoint") == 0) {
        setpoint = command.value.asNumber();
        std::printf("setpoint is now %.2f m\n", setpoint);
    }
}

} // namespace

int main(int argc, char** argv) {
    // "broker.example.com", or "broker.example.com:1884" for another port.
    std::string broker = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = 0;   // 0: 1883, or 8883 with TLS
    const size_t colon = broker.rfind(':');
    if (colon != std::string::npos) {
        port = (uint16_t)std::atoi(broker.c_str() + colon + 1);
        broker.resize(colon);
    }

    node.group("Plant");
    node.node("Gateway-1");
    node.server(broker.c_str(), port);
    // node.useTls();   // 8883, verified against the system's certificates

    // Every metric, before connecting: the birth lists them all.
    node.declare("Tank/Level", MqttSparkplugDataType::Double, "m");
    node.declare("Pump/Running", MqttSparkplugDataType::Boolean);
    node.declare("Pump/Starts", MqttSparkplugDataType::UInt32);
    node.declareWritable("Tank/Setpoint", MqttSparkplugDataType::Double);
    node.onCommand(&onCommand);

    if (!node.connect()) {
        std::printf("nowhere to connect to\n");
        return 1;
    }

    bool running = false;
    uint32_t starts = 0;
    bool announced = false;
    auto lastSent = std::chrono::steady_clock::now();

    for (double t = 0;; t += 0.01) {
        node.poll();

        if (node.isBorn() != announced) {
            announced = node.isBorn();
            std::printf(announced ? "online: Plant/Gateway-1 is in SCADA\n"
                                  : "offline: %s\n", node.lastErrorText());
        }

        // A tank that fills and drains, and a pump that keeps it near the
        // setpoint.
        const double level = 2.5 + 2.0 * std::sin(t / 20.0);
        const bool wantRunning = level < setpoint;
        if (wantRunning && !running) starts++;
        running = wantRunning;

        node.set("Tank/Level", mq_spDouble(level));
        node.set("Pump/Running", mq_spBool(running));
        node.set("Pump/Starts", mq_spUInt32(starts));
        node.set("Tank/Setpoint", mq_spDouble(setpoint));

        // Once a second, one message with only what changed.
        const auto now = std::chrono::steady_clock::now();
        if (now - lastSent >= std::chrono::seconds(1)) {
            lastSent = now;
            node.publishChanged();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

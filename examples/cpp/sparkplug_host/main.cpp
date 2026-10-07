// A SPARKPLUG B HOST APPLICATION
//
// The other end: what a SCADA system does with Sparkplug, in one file. It
// announces itself (so edge nodes configured to wait for it start talking),
// watches every group, keeps each node's metric list from its birth, and
// prints what arrives BY NAME - a node sends names only in its birth and
// numbers (aliases) after that, and the host puts the names back.
//
// Build:  cmake -S . -B build && cmake --build build
// Run:    ./build/examples/cpp/modbee_sparkplug_host [broker-host[:port]]
//         ./build/examples/cpp/modbee_sparkplug_host [broker-host[:port]] Plant Gateway-1 Tank/Setpoint 3.5
//         (the second form writes a value to a node, then keeps watching)

#include <ModBeeMqtt.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

namespace {

MqttSparkplugHost host;

void printValue(const MqttSparkplugValue& value) {
    if (value.isNull) {
        std::printf("null");
    } else if (value.type == MqttSparkplugDataType::Boolean) {
        std::printf("%s", value.as.boolean ? "true" : "false");
    } else if (value.isNumeric()) {
        std::printf("%g", value.asNumber());
    } else if (value.isText()) {
        std::printf("\"%.*s\"", (int)value.as.bytes.length, (const char*)value.as.bytes.data);
    } else {
        std::printf("(%s)", mq_sparkplugDataTypeName(value.type));
    }
}

// A write must carry the metric's own datatype - a node refuses a Double
// written to a Float, rather than guess. The birth said what each metric is,
// and the host kept it, so the type comes from there.
bool valueOfType(MqttSparkplugDataType type, double number, MqttSparkplugValue& into) {
    switch (type) {
        case MqttSparkplugDataType::Float:   into = mq_spFloat((float)number); return true;
        case MqttSparkplugDataType::Double:  into = mq_spDouble(number); return true;
        case MqttSparkplugDataType::Boolean: into = mq_spBool(number != 0); return true;
        case MqttSparkplugDataType::Int8:    into = mq_spInt8((int8_t)number); return true;
        case MqttSparkplugDataType::Int16:   into = mq_spInt16((int16_t)number); return true;
        case MqttSparkplugDataType::Int32:   into = mq_spInt32((int32_t)number); return true;
        case MqttSparkplugDataType::Int64:   into = mq_spInt64((int64_t)number); return true;
        case MqttSparkplugDataType::UInt8:   into = mq_spUInt8((uint8_t)number); return true;
        case MqttSparkplugDataType::UInt16:  into = mq_spUInt16((uint16_t)number); return true;
        case MqttSparkplugDataType::UInt32:  into = mq_spUInt32((uint32_t)number); return true;
        case MqttSparkplugDataType::UInt64:  into = mq_spUInt64((uint64_t)number); return true;
        default: return false;
    }
}

void onMessage(const MqttSparkplugInbound& message) {
    std::printf("%-6s %s/%s%s%s", mq_sparkplugTypeName(message.type), message.group,
                message.node, message.device ? "/" : "", message.device ? message.device : "");
    const MqttSparkplugPayload& payload = *message.payload;
    for (size_t i = 0; i < payload.metricCount; i++) {
        const MqttSparkplugMetric& metric = payload.metrics[i];
        std::printf("  %s=", metric.name ? metric.name : "?");
        printValue(metric.value);
    }
    std::printf("\n");
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

    // An optional write: group, node, metric, value.
    const bool writing = argc > 5;

    // A host id is also its MQTT client id, so two hosts sharing one would
    // keep taking each other's connection. The one-off writer gets its own.
    host.hostId(writing ? "modbee-example-writer" : "modbee-example-host");
    host.server(broker.c_str(), port);
    host.watch();              // every group: spBv1.0/#
    if (!writing) {
        host.actAsPrimary();   // ask a node to re-announce when a message is missed
    }
    host.onMessage(&onMessage);

    if (!host.connect()) {
        std::printf("nowhere to connect to\n");
        return 1;
    }

    bool written = false;
    bool askedRebirth = false;

    while (true) {
        host.poll();

        // The metric list is in the node's birth, and a host that starts
        // after the node has not seen it. So it asks for it again - a Node
        // Control/Rebirth command, which any host may send - once.
        if (writing && !askedRebirth && host.isBorn()) {
            askedRebirth = host.requestRebirth(argv[2], argv[3]);
        }

        const MqttSparkplugTrackedNode* tracked =
            writing && !written && host.isBorn() ? host.findNode(argv[2], argv[3]) : nullptr;
        const MqttSparkplugTrackedMetric* metric = tracked ? tracked->find(argv[4]) : nullptr;
        if (metric) {
            MqttSparkplugValue value;
            if (!valueOfType(metric->type, std::atof(argv[5]), value)) {
                std::printf("%s is a %s - this example writes numbers and booleans\n",
                            argv[4], mq_sparkplugDataTypeName(metric->type));
                return 1;
            }
            written = host.command(argv[2], argv[3], argv[4], value);
            std::printf("write %s = %s to %s/%s: %s\n", argv[4], argv[5], argv[2], argv[3],
                        written ? "sent" : "refused");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

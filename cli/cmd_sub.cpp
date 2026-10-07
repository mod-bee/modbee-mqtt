// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee sub - subscribe and print what arrives.
//
// What a subscription was GRANTED is printed too, because a broker may grant
// a lower QoS than was asked for, or refuse a filter outright, and a tool that
// only printed messages would leave somebody waiting for ones that can never
// come.

#include "Common.h"

#include <cstdio>

namespace {

struct SubState {
    bool json = false;
    bool verbose = false;
    long long received = 0;
    long long limit = 0;
    size_t answered = 0;
    size_t refused = 0;
};

SubState* g_state = nullptr;

struct FilterOutcome {
    std::string filter;
};

void onSubscribed(void* context, MqttResult result, uint16_t) {
    FilterOutcome* outcome = static_cast<FilterOutcome*>(context);
    if (g_state == nullptr || outcome == nullptr) return;
    g_state->answered++;
    const bool ok = result == MqttResult::Ok;
    if (!ok) g_state->refused++;
    if (g_state->json) {
        std::printf("{\"event\":\"subscribe\",\"filter\":%s,\"result\":\"%s\",\"ok\":%s}\n",
                    jsonString(outcome->filter).c_str(), mq_resultName(result),
                    ok ? "true" : "false");
    } else {
        std::fprintf(stderr, "subscribed %s: %s\n", outcome->filter.c_str(),
                     ok ? "granted" : mq_resultName(result));
    }
}

void onMessage(void*, const MqttMessage& message) {
    if (g_state == nullptr) return;
    g_state->received++;
    if (g_state->json) {
        std::printf("%s\n", messageJson(message).c_str());
        return;
    }
    if (g_state->verbose) {
        std::printf("%.*s ", (int)message.topic.length, (const char*)message.topic.data);
    }
    if (isPrintableText(message.payload.data, message.payload.length)) {
        std::printf("%.*s\n", (int)message.payload.length, (const char*)message.payload.data);
    } else {
        // Binary - Sparkplug protobuf, for one - would wreck a terminal.
        std::printf("<%u bytes binary, base64 %s>\n", (unsigned)message.payload.length,
                    base64(message.payload.data, message.payload.length).c_str());
    }
}

const std::vector<ArgSpec>& subSpecs() {
    static const std::vector<ArgSpec> specs = withConnectionSpecs({
        {"topic", 't', true, "filter to subscribe to (repeatable, required)"},
        {"qos", 'q', true, "maximum QoS to ask for: 0, 1 or 2 (default 0)"},
        {"verbose", 'v', false, "print the topic before each payload"},
        {"count", 'C', true, "exit after this many messages"},
        {"wait", 'W', true, "exit after this many seconds"},
        {"no-local", 0, false, "MQTT 5: do not receive my own publishes"},
        {"retain-as-published", 0, false, "MQTT 5: keep the retain flag as published"},
        {"retain-handling", 0, true, "MQTT 5: 0 send, 1 send if new, 2 never"},
    });
    return specs;
}

} // namespace

int cmdSub(int argc, char** argv) {
    const std::vector<ArgSpec>& specs = subSpecs();
    Args args;
    if (!args.parse(argc, argv, 2, specs)) {
        std::fprintf(stderr, "modbee sub: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp("modbee sub -t <filter> [-t <filter>...] [options]", specs);
        return 0;
    }
    const std::vector<std::string> filters = args.all("topic");
    if (filters.empty()) {
        std::fprintf(stderr, "modbee sub: at least one --topic is required\n");
        return 2;
    }

    long long qos = 0, count = 0, wait = 0, retainHandling = 0;
    if (!args.getInt("qos", 0, 0, 2, qos) || !args.getInt("count", 0, 0, 1LL << 40, count) ||
        !args.getInt("wait", 0, 0, 86400 * 365, wait) ||
        !args.getInt("retain-handling", 0, 0, 2, retainHandling)) {
        std::fprintf(stderr, "modbee sub: %s\n", args.error().c_str());
        return 2;
    }

    cliInstallSignalHandlers();
    CliClient cli;
    std::string error;
    if (!cli.configure(args, "modbee-sub", error)) {
        std::fprintf(stderr, "modbee sub: %s\n", error.c_str());
        return 2;
    }

    SubState state;
    state.json = cli.json;
    state.verbose = args.has("verbose");
    state.limit = count;
    g_state = &state;
    cli.client.onMessage(&onMessage, nullptr);

    // Subscriptions are recorded before connecting; the library establishes
    // them when the session comes up.
    std::vector<FilterOutcome> outcomes(filters.size());
    uint8_t options = (uint8_t)qos;
    if (args.has("no-local")) options |= MQ_SUBOPT_NO_LOCAL;
    if (args.has("retain-as-published")) options |= MQ_SUBOPT_RETAIN_AS_PUB;
    options |= (uint8_t)(retainHandling << MQ_SUBOPT_RETAIN_HANDLING_SHIFT);
    for (size_t index = 0; index < filters.size(); index++) {
        outcomes[index].filter = filters[index];
        const MqttResult queued = cli.client.subscribeWithOptions(
            filters[index].c_str(), options, &onSubscribed, &outcomes[index]);
        if (queued != MqttResult::Queued) {
            std::fprintf(stderr, "modbee sub: cannot subscribe to %s: %s\n",
                         filters[index].c_str(), mq_resultName(queued));
            return 2;
        }
    }

    if (!cli.connectAndWait(10000)) return 3;

    const mq_time_t start = mq_millis();
    bool lost = false;
    while (!cliStopRequested()) {
        cli.client.poll();
        if (state.limit > 0 && state.received >= state.limit) break;
        if (wait > 0 && mq_millis() - start >= (mq_time_t)wait * 1000) break;
        if (state.answered == filters.size() && state.refused == filters.size()) {
            std::fprintf(stderr, "modbee sub: every subscription was refused\n");
            break;
        }
        if (!cli.client.isConnected() && cli.client.state() == MqttState::Idle) {
            std::fprintf(stderr, "modbee sub: connection lost: %s\n",
                         mq_resultName(cli.client.status().lastError));
            lost = true;
            break;
        }
        cliIdle(cli.transport.get(), 50);
    }

    cli.shutdown();
    g_state = nullptr;
    if (lost) return 3;
    if (state.refused > 0) return 1;
    return 0;
}

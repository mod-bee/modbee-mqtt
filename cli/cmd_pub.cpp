// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee-mqtt pub - publish, and say what happened to every message.
//
// mosquitto_pub reports that it sent something. This reports what the broker
// SAID: a QoS 1 or 2 publish ends with the broker's answer, including the
// refusals the protocol can express - not authorised, quota exceeded, no
// matching subscribers - because this library exists so that nothing is
// silently dropped, and its own tool should not be the exception.

#include "Common.h"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <iterator>

namespace {

struct Outcome {
    int index = 0;
    bool done = false;
    MqttResult result = MqttResult::Queued;
};

struct Tally {
    std::vector<Outcome> outcomes;
    size_t concluded = 0;
    size_t failed = 0;
    bool json = false;
    std::string topic;
};

Tally* g_tally = nullptr;

// 0x10 No matching subscribers is an ACCEPTED publish that nobody was
// listening for: real information - check the spelling - but not a failure,
// so it does not fail the command either.
bool accepted(MqttResult result) {
    return result == MqttResult::Ok || result == MqttResult::NoMatchingSubscribers;
}

void report(const Tally& tally, const Outcome& outcome) {
    const bool ok = accepted(outcome.result);
    if (tally.json) {
        std::printf("{\"event\":\"publish\",\"n\":%d,\"topic\":%s,\"result\":\"%s\",\"ok\":%s}\n",
                    outcome.index, jsonString(tally.topic).c_str(),
                    mq_resultName(outcome.result), ok ? "true" : "false");
    } else {
        const char* what = outcome.result == MqttResult::Ok ? "delivered to the broker"
                         : outcome.result == MqttResult::NoMatchingSubscribers
                             ? "accepted, but no matching subscribers"
                             : mq_resultName(outcome.result);
        std::printf("#%d %s: %s\n", outcome.index, tally.topic.c_str(), what);
    }
}

void onComplete(void* context, MqttResult result, uint16_t) {
    Outcome* outcome = static_cast<Outcome*>(context);
    if (outcome == nullptr || g_tally == nullptr || outcome->done) return;
    outcome->done = true;
    outcome->result = result;
    g_tally->concluded++;
    if (!accepted(result)) g_tally->failed++;
    report(*g_tally, *outcome);
}

const std::vector<ArgSpec>& pubSpecs() {
    static const std::vector<ArgSpec> specs = withConnectionSpecs({
        {"topic", 't', true, "topic to publish to (required)"},
        {"message", 'm', true, "payload text"},
        {"file", 'f', true, "payload read from a file, sent as bytes"},
        {"stdin-line", 'l', false, "publish each line of standard input"},
        {"null", 'n', false, "a zero-length payload (clears a retained message)"},
        {"qos", 'q', true, "0, 1 or 2 (default 0)"},
        {"retain", 'r', false, "set the retain flag"},
        {"count", 0, true, "publish the message this many times (default 1)"},
        {"interval", 0, true, "milliseconds between repeats (default 0)"},
        {"content-type", 0, true, "MQTT 5 Content Type"},
        {"response-topic", 0, true, "MQTT 5 Response Topic"},
        {"correlation", 0, true, "MQTT 5 Correlation Data, as text"},
        {"expiry", 0, true, "MQTT 5 Message Expiry Interval, seconds"},
        {"utf8", 0, false, "MQTT 5 Payload Format Indicator: UTF-8 text"},
        {"user-property", 0, true, "MQTT 5 user property key=value (repeatable)"},
        {"wait", 'W', true, "seconds to wait for the broker (default 10)"},
    });
    return specs;
}

bool readFile(const std::string& path, std::vector<uint8_t>& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    out.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    return true;
}

} // namespace

int cmdPub(int argc, char** argv) {
    const std::vector<ArgSpec>& specs = pubSpecs();
    Args args;
    if (!args.parse(argc, argv, 2, specs)) {
        std::fprintf(stderr, "modbee-mqtt pub: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp("modbee-mqtt pub -t <topic> (-m <text> | -f <file> | -l | -n) [options]", specs);
        return 0;
    }
    if (!args.has("topic")) {
        std::fprintf(stderr, "modbee-mqtt pub: --topic is required\n");
        return 2;
    }
    const int sources = (int)args.has("message") + (int)args.has("file") +
                        (int)args.has("stdin-line") + (int)args.has("null");
    if (sources != 1) {
        std::fprintf(stderr, "modbee-mqtt pub: give exactly one of --message, --file, --stdin-line, --null\n");
        return 2;
    }

    long long qos = 0, count = 1, interval = 0, wait = 10, expiry = 0;
    if (!args.getInt("qos", 0, 0, 2, qos) || !args.getInt("count", 1, 1, 100000000, count) ||
        !args.getInt("interval", 0, 0, 86400000, interval) ||
        !args.getInt("wait", 10, 1, 86400, wait) ||
        !args.getInt("expiry", 0, 0, 0xFFFFFFFFll, expiry)) {
        std::fprintf(stderr, "modbee-mqtt pub: %s\n", args.error().c_str());
        return 2;
    }

    MqttOwnedProperties properties;
    if (args.has("content-type")) properties.setContentType(args.get("content-type").c_str());
    if (args.has("response-topic")) properties.setResponseTopic(args.get("response-topic").c_str());
    if (args.has("correlation")) properties.setCorrelationData(args.get("correlation").c_str());
    if (args.has("expiry")) properties.setMessageExpiry((uint32_t)expiry);
    if (args.has("utf8")) properties.setPayloadFormat(true);
    for (const std::string& pair : args.all("user-property")) {
        MqttOwnedProperties::UserProperty property;
        if (!splitUserProperty(pair, property.key, property.value)) {
            std::fprintf(stderr, "modbee-mqtt pub: --user-property wants key=value, not '%s'\n", pair.c_str());
            return 2;
        }
        properties.user.push_back(property);
    }

    // The payloads, decided before connecting so a bad file is reported
    // without a connection opened for nothing.
    std::vector<std::vector<uint8_t>> payloads;
    if (args.has("message")) {
        const std::string text = args.get("message");
        payloads.push_back(std::vector<uint8_t>(text.begin(), text.end()));
    } else if (args.has("file")) {
        std::vector<uint8_t> bytes;
        if (!readFile(args.get("file"), bytes)) {
            std::fprintf(stderr, "modbee-mqtt pub: cannot read %s\n", args.get("file").c_str());
            return 2;
        }
        payloads.push_back(bytes);
    } else if (args.has("null")) {
        payloads.push_back(std::vector<uint8_t>());
    }

    cliInstallSignalHandlers();
    CliClient cli;
    std::string error;
    if (!cli.configure(args, "modbee-pub", error)) {
        std::fprintf(stderr, "modbee-mqtt pub: %s\n", error.c_str());
        return 2;
    }
    if (!cli.connectAndWait((mq_time_t)wait * 1000)) return 3;

    Tally tally;
    tally.json = cli.json;
    tally.topic = args.get("topic");
    g_tally = &tally;

    // Reserved up front: completions hold a pointer into this vector, so it
    // must never reallocate once a publish is queued.
    const bool fromStdin = args.has("stdin-line");
    tally.outcomes.reserve(fromStdin ? 1 : (size_t)count);

    const std::string topic = tally.topic;
    const bool retain = args.has("retain");
    const bool series = count > 1 || fromStdin;
    int index = 0;

    // One publish. publishSeries for a repeat or a stream: the ordinary
    // publish() SUPERSEDES a queued message to the same topic, which is right
    // for a setpoint and wrong for "send this ten times".
    auto send = [&](const std::vector<uint8_t>& payload) -> bool {
        if (fromStdin) {
            if (tally.outcomes.size() == tally.outcomes.capacity()) {
                // Wait for everything outstanding, then recycle the storage.
                cli.pollUntil([&] { return tally.concluded == tally.outcomes.size(); },
                              (mq_time_t)wait * 1000);
                tally.outcomes.clear();
                tally.concluded = 0;
                tally.outcomes.reserve(256);
            }
        }
        tally.outcomes.push_back(Outcome());
        Outcome* outcome = &tally.outcomes.back();
        outcome->index = ++index;

        for (;;) {
            const MqttResult queued = series
                ? cli.client.publishSeries(topic.c_str(), payload.data(), payload.size(),
                                           (uint8_t)qos, retain, properties, &onComplete, outcome)
                : cli.client.publish(topic.c_str(), payload.data(), payload.size(),
                                     (uint8_t)qos, retain, properties, &onComplete, outcome);
            if (queued == MqttResult::Queued) return true;
            if (queued == MqttResult::QueueFull) {
                // The queue is bounded and says so. Let it drain and try again.
                cli.client.poll();
                cliIdle(cli.transport.get(), 5);
                if (cliStopRequested() || !cli.client.isConnected()) return false;
                continue;
            }
            outcome->done = true;
            outcome->result = queued;
            tally.concluded++;
            tally.failed++;
            report(tally, *outcome);
            return queued != MqttResult::NotConnected;
        }
    };

    if (fromStdin) {
        std::string line;
        while (!cliStopRequested() && std::getline(std::cin, line)) {
            if (!send(std::vector<uint8_t>(line.begin(), line.end()))) break;
            cli.client.poll();
        }
    } else {
        for (long long n = 0; n < count && !cliStopRequested(); n++) {
            if (!send(payloads[0])) break;
            cli.client.poll();
            if (interval > 0 && n + 1 < count) {
                const mq_time_t until = mq_millis() + (mq_time_t)interval;
                while (mq_isFuture(mq_millis(), until) && !cliStopRequested()) {
                    cli.client.poll();
                    cliIdle(cli.transport.get(), 5);
                }
            }
        }
    }

    // Every message reaches a conclusion before the tool reports its own.
    cli.pollUntil([&] { return tally.concluded == tally.outcomes.size(); },
                  (mq_time_t)wait * 1000);
    const size_t unanswered = tally.outcomes.size() - tally.concluded;
    if (unanswered > 0) {
        std::fprintf(stderr, "modbee-mqtt pub: %zu message(s) had no answer within %llds\n",
                     unanswered, wait);
    }

    cli.shutdown();
    g_tally = nullptr;
    return (tally.failed == 0 && unanswered == 0) ? 0 : 1;
}

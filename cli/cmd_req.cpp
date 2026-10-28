// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee-mqtt req - MQTT 5 request/response from the command line.
//
// Subscribes to a response topic, publishes the request carrying that topic
// and a correlation value [V5:4.10], and prints the first reply whose
// Correlation Data matches. A reply to somebody else's request on a shared
// response topic is ignored rather than printed, because matching on
// correlation is the whole point of the mechanism.

#include "Common.h"
#include "KafkaCli.h"

#include <cstdio>
#include <cstring>

namespace {

struct ReqState {
    std::string correlation;
    bool answered = false;
    bool json = false;
    bool subscribed = false;
    MqttResult subscribeResult = MqttResult::Queued;
};

ReqState* g_req = nullptr;

void onSubscribed(void*, MqttResult result, uint16_t) {
    if (g_req == nullptr) return;
    g_req->subscribed = true;
    g_req->subscribeResult = result;
}

void onMessage(void*, const MqttMessage& message) {
    if (g_req == nullptr || g_req->answered) return;
    const MqttProperties* p = message.properties;
    if (p == nullptr || !p->has(MQ_PROP_CORRELATION_DATA)) return;
    if (p->correlationData.length != g_req->correlation.size() ||
        std::memcmp(p->correlationData.data, g_req->correlation.data(),
                    g_req->correlation.size()) != 0) {
        return;
    }
    g_req->answered = true;
    if (g_req->json) {
        std::printf("%s\n", messageJson(message).c_str());
    } else if (isPrintableText(message.payload.data, message.payload.length)) {
        std::printf("%.*s\n", (int)message.payload.length, (const char*)message.payload.data);
    } else {
        std::printf("<%u bytes binary, base64 %s>\n", (unsigned)message.payload.length,
                    base64(message.payload.data, message.payload.length).c_str());
    }
}

#if defined(MODBEE_HAVE_KAFKA)
// The request as a record in the envelope, its Response Topic and Correlation
// Data in it; the reply, a record on the reply topic carrying the same
// correlation - read from before the request went, so it cannot be missed.
int reqKafka(const Args& args, long long qos, long long wait) {
    const std::string kafkaTopic = args.get("kafka-topic");
    if (kafkaTopic.empty()) {
        std::fprintf(stderr, "modbee-mqtt req: --kafka needs --kafka-topic (the Kafka topic to write)\n");
        return 2;
    }
    const std::string replyTopic = args.get("kafka-reply-topic", kafkaTopic.c_str());
    CliKafka kafka;
    std::string error;
    if (!kafka.configure(args, error) || !kafka.openConsumer({replyTopic}, error) || !kafka.openProducer(error)) {
        std::fprintf(stderr, "modbee-mqtt req: %s\n", error.c_str());
        return 3;
    }
    ReqState state;
    state.json = args.has("json");
    state.correlation = "modbee-req-" + std::to_string(cliProcessId()) + "-" + std::to_string((unsigned long)mq_millis());
    g_req = &state;
    const std::string responseTopic = args.get("response-topic", ("modbee/reply/" + state.correlation).c_str());
    MqttOwnedProperties properties;
    properties.setResponseTopic(responseTopic.c_str());
    properties.setCorrelationData((const uint8_t*)state.correlation.data(), state.correlation.size());
    const std::string payload = args.get("message");
    size_t failed = 0;
    if (!kafka.produce(kafkaTopic, args.get("topic"), (const uint8_t*)payload.data(), payload.size(), (uint8_t)qos,
                       false, properties, error) ||
        !kafka.flush((int)wait * 1000, failed) || failed > 0) {
        std::fprintf(stderr, "modbee-mqtt req: the request was not written: %s\n",
                     error.empty() ? "Kafka refused it" : error.c_str());
        g_req = nullptr;
        return 1;
    }
    const mq_time_t start = mq_millis();
    while (!state.answered && !cliStopRequested() && mq_millis() - start < (mq_time_t)wait * 1000) {
        MqttMessage message;
        if (!kafka.next(100, message)) continue;
        if (message.topic.length == responseTopic.size() &&
            std::memcmp(message.topic.data, responseTopic.data(), responseTopic.size()) == 0) {
            onMessage(nullptr, message);
        }
    }
    if (!state.answered) std::fprintf(stderr, "modbee-mqtt req: no reply within %llds\n", wait);
    g_req = nullptr;
    return state.answered ? 0 : 1;
}
#endif

const std::vector<ArgSpec>& reqSpecs() {
    static const std::vector<ArgSpec> specs = withKafkaSpecs(withConnectionSpecs({
        {"topic", 't', true, "request topic (required)"},
        {"message", 'm', true, "request payload text (default empty)"},
        {"response-topic", 'e', true, "where the reply should go (default modbee/reply/<id>)"},
        {"qos", 'q', true, "QoS for request and reply: 0, 1 or 2 (default 1)"},
        {"wait", 'W', true, "seconds to wait for the reply (default 10)"},
    }));
    return specs;
}

} // namespace

int cmdReq(int argc, char** argv) {
    const std::vector<ArgSpec>& specs = reqSpecs();
    Args args;
    if (!args.parse(argc, argv, 2, specs)) {
        std::fprintf(stderr, "modbee-mqtt req: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp("modbee-mqtt req -t <topic> [-m <text>] [options]", specs);
        return 0;
    }
    if (!args.has("topic")) {
        std::fprintf(stderr, "modbee-mqtt req: --topic is required\n");
        return 2;
    }
    long long qos = 1, wait = 10;
    if (!args.getInt("qos", 1, 0, 2, qos) || !args.getInt("wait", 10, 1, 86400, wait)) {
        std::fprintf(stderr, "modbee-mqtt req: %s\n", args.error().c_str());
        return 2;
    }

    cliInstallSignalHandlers();
    bool refused = false;
    if (kafkaAsked(args, "req", refused)) {
#if defined(MODBEE_HAVE_KAFKA)
        return reqKafka(args, qos, wait);
#endif
    }
    if (refused) return 2;
    CliClient cli;
    std::string error;
    if (!cli.configure(args, "modbee-req", error)) {
        std::fprintf(stderr, "modbee-mqtt req: %s\n", error.c_str());
        return 2;
    }
    if (cli.connection.version != MqttVersion::V5) {
        // 3.1.1 has no Response Topic or Correlation Data property at all.
        std::fprintf(stderr, "modbee-mqtt req: request/response needs MQTT 5\n");
        return 2;
    }

    ReqState state;
    state.json = cli.json;
    state.correlation = cli.connection.clientId + "-" + std::to_string((unsigned long)mq_millis());
    g_req = &state;
    cli.client.onMessage(&onMessage, nullptr);

    const std::string responseTopic = args.get("response-topic",
        ("modbee/reply/" + cli.connection.clientId).c_str());
    cli.client.subscribe(responseTopic.c_str(), (uint8_t)qos, &onSubscribed, nullptr);

    if (!cli.connectAndWait((mq_time_t)wait * 1000)) return 3;

    // The reply cannot be caught before the subscription is in place, so the
    // request waits for the SUBACK rather than racing it.
    cli.pollUntil([&] { return state.subscribed; }, (mq_time_t)wait * 1000);
    if (!state.subscribed || state.subscribeResult != MqttResult::Ok) {
        std::fprintf(stderr, "modbee-mqtt req: could not subscribe to %s: %s\n",
                     responseTopic.c_str(),
                     state.subscribed ? mq_resultName(state.subscribeResult) : "no answer");
        cli.shutdown();
        return 3;
    }

    MqttOwnedProperties properties;
    properties.setResponseTopic(responseTopic.c_str());
    properties.setCorrelationData((const uint8_t*)state.correlation.data(), state.correlation.size());
    const std::string payload = args.get("message");
    const MqttResult queued = cli.client.publish(args.get("topic").c_str(),
                                                 (const uint8_t*)payload.data(), payload.size(),
                                                 (uint8_t)qos, false, properties);
    if (queued != MqttResult::Queued) {
        std::fprintf(stderr, "modbee-mqtt req: request refused: %s\n", mq_resultName(queued));
        cli.shutdown();
        return 1;
    }

    const bool answered = cli.pollUntil([&] { return state.answered; }, (mq_time_t)wait * 1000);
    if (!answered) std::fprintf(stderr, "modbee-mqtt req: no reply within %llds\n", wait);
    cli.shutdown();
    g_req = nullptr;
    return answered ? 0 : 1;
}

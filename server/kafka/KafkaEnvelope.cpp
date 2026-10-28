// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "KafkaEnvelope.h"
#include "KafkaCommon.h"

#include <cstdlib>
#include <cstring>

namespace {

bool wanted(const MqttToKafkaConfig* selection, const char* placeholder) {
    if (selection == nullptr || selection->allHeaders) return true;
    for (const KafkaHeaderSpec& h : selection->headers) {
        if (!h.fixed && h.name == placeholder) return true;
    }
    return false;
}

} // namespace

size_t kafkaWriteEnvelope(rd_kafka_headers_t* headers, const MqttToKafkaConfig* selection,
                          const MqttSpan& topic, uint8_t qos, bool retain, const MqttSpan& clientId,
                          const MqttSpan& userName, const MqttProperties* p) {
    size_t bytes = 0;
    const auto add = [&](const char* name, const void* value, size_t size) {
        rd_kafka_header_add(headers, name, -1, value, (ssize_t)size);
        bytes += std::strlen(name) + size;
    };
    const auto addText = [&](const char* name, const std::string& value) { add(name, value.data(), value.size()); };
    if (selection == nullptr || selection->allHeaders) addText(kEnvelopeVersionHeader, kEnvelopeVersion);
    if (wanted(selection, "mqtt-topic")) add("mqtt-topic", topic.data, topic.length);
    if (wanted(selection, "mqtt-qos")) addText("mqtt-qos", std::to_string(qos));
    if (wanted(selection, "mqtt-retain")) addText("mqtt-retain", retain ? "true" : "false");
    if (wanted(selection, "mqtt-clientid") && clientId.length > 0) add("mqtt-clientid", clientId.data, clientId.length);
    // Who published it - the login the broker checked, so a reader knows whose message this is
    // without trusting anything in the payload or the topic. A Will carries its session's.
    if (wanted(selection, "mqtt-username") && userName.length > 0) add("mqtt-username", userName.data, userName.length);
    if (p != nullptr) {
        if (wanted(selection, "mqtt-content-type") && p->has(MQ_PROP_CONTENT_TYPE)) {
            add("mqtt-content-type", p->contentType.data, p->contentType.length);
        }
        if (wanted(selection, "mqtt-correlation-data") && p->has(MQ_PROP_CORRELATION_DATA)) {
            add("mqtt-correlation-data", p->correlationData.data, p->correlationData.length);
        }
        if (wanted(selection, "mqtt-payload-format-indicator") && p->has(MQ_PROP_PAYLOAD_FORMAT_INDICATOR)) {
            addText("mqtt-payload-format-indicator", std::to_string(p->payloadFormatIndicator));
        }
        if (wanted(selection, "mqtt-response-topic") && p->has(MQ_PROP_RESPONSE_TOPIC)) {
            add("mqtt-response-topic", p->responseTopic.data, p->responseTopic.length);
        }
        if (wanted(selection, "mqtt-message-expiry-interval") && p->has(MQ_PROP_MESSAGE_EXPIRY_INTERVAL)) {
            addText("mqtt-message-expiry-interval", std::to_string(p->messageExpiryInterval));
        }
        if (wanted(selection, "mqtt-user-properties")) {
            for (uint8_t u = 0; u < p->userCount; u++) {
                std::string name((const char*)p->user[u].key.data, p->user[u].key.length);
                if (name.compare(0, 5, "mqtt-") == 0) name = kUserPropertyEscape + name;
                add(name.c_str(), p->user[u].value.data, p->user[u].value.length);
            }
        }
    }
    if (selection != nullptr) {
        for (const KafkaHeaderSpec& h : selection->headers) {
            if (h.fixed) addText(h.name.c_str(), h.value);
        }
    }
    return bytes;
}

bool kafkaReadEnvelope(rd_kafka_headers_t* headers, KafkaEnvelope& out) {
    const char* name;
    const void* value;
    size_t size;
    for (size_t i = 0; rd_kafka_header_get_all(headers, i, &name, &value, &size) == RD_KAFKA_RESP_ERR_NO_ERROR;
         i++) {
        const std::string v = value ? std::string((const char*)value, size) : std::string();
        if (std::strcmp(name, kEnvelopeVersionHeader) == 0) out.version = v;
        else if (std::strcmp(name, "mqtt-topic") == 0) { out.hasTopic = true; out.topic = v; }
        else if (std::strcmp(name, "mqtt-qos") == 0) { out.hasQos = true; out.qos = (uint8_t)(v == "2" ? 2 : v == "1" ? 1 : 0); }
        else if (std::strcmp(name, "mqtt-retain") == 0) { out.hasRetain = true; out.retain = v == "true"; }
        else if (std::strcmp(name, "mqtt-clientid") == 0) out.clientId = v;
        else if (std::strcmp(name, "mqtt-username") == 0) out.userName = v;
        else if (std::strcmp(name, kEnvelopeLinkHeader) == 0) out.link = v;
        else if (std::strcmp(name, "mqtt-content-type") == 0) { out.hasContentType = true; out.contentType = v; }
        else if (std::strcmp(name, "mqtt-response-topic") == 0) { out.hasResponseTopic = true; out.responseTopic = v; }
        else if (std::strcmp(name, "mqtt-correlation-data") == 0) { out.hasCorrelation = true; out.correlation = v; }
        else if (std::strcmp(name, "mqtt-payload-format-indicator") == 0) { out.hasFormat = true; out.format = v == "1" ? 1 : 0; }
        else if (std::strcmp(name, "mqtt-message-expiry-interval") == 0) {
            out.hasExpiry = true;
            out.expiry = (uint32_t)std::strtoul(v.c_str(), nullptr, 10);
        }
        else if (std::strncmp(name, kUserPropertyEscape, sizeof kUserPropertyEscape - 1) == 0) {
            out.users.push_back(std::make_pair(std::string(name + sizeof kUserPropertyEscape - 1), v));
        }
        else if (std::strncmp(name, "mqtt-", 5) == 0) { /* a later version's: not this reader's to guess at */ }
        else out.users.push_back(std::make_pair(std::string(name), v));
    }
    return out.version.empty() || out.version == kEnvelopeVersion;
}

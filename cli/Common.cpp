// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "Common.h"

#include <cstdlib>

#include <csignal>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <thread>

#include "transport/MqttSocketSupport.h"   // poll, or WSAPoll

#if defined(_WIN32)
#include <process.h>
#define MQ_CLI_POLL WSAPoll
typedef WSAPOLLFD MqCliPollFd;
#else
#include <unistd.h>
#define MQ_CLI_POLL ::poll
typedef struct pollfd MqCliPollFd;
#endif

namespace {

volatile sig_atomic_t g_stop = 0;
void onSignal(int) { g_stop = 1; }

} // namespace

// NOT mq_yield(). On a host that is std::this_thread::yield(), which is right
// for the library - poll() must never wait - and wrong for a command-line tool
// sitting on a subscription for an hour, where it would spin a core at 100%.
// The tool is the daemon here, and a daemon may sleep on its socket.
void cliIdle(MqttTransport* transport, int ms) {
    const int fd = transport ? transport->waitHandle() : -1;
    if (fd < 0 || (transport && transport->hasBufferedInput())) {
        if (fd < 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return;
    }
    MqCliPollFd p;
    std::memset(&p, 0, sizeof p);
    p.fd = fd;
    p.events = POLLIN;
    MQ_CLI_POLL(&p, 1, ms);
}

bool cliStopRequested() { return g_stop != 0; }

void cliInstallSignalHandlers() {
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
#if defined(SIGPIPE)
    std::signal(SIGPIPE, SIG_IGN);
#endif
}

std::vector<ArgSpec> connectionSpecs() {
    return {
        {"host", 'h', true, "broker host (default localhost)"},
        {"port", 'p', true, "broker port (default 1883, or 8883 with --tls)"},
        {"id", 'i', true, "client identifier (default modbee-<command>-<pid>)"},
        {"user", 'u', true, "user name"},
        {"password", 'P', true, "password"},
        {"mqtt-version", 'V', true, "5 (default) or 311"},
        {"keepalive", 'k', true, "keep alive in seconds (default 60)"},
        {"session-expiry", 0, true, "MQTT 5 Session Expiry Interval, seconds"},
        {"no-clean", 'c', false, "Clean Start 0: resume a session that exists"},
        {"tls", 0, false, "connect with TLS"},
        {"cafile", 0, true, "CA certificate file to verify the broker against"},
        {"pin", 0, true, "pin the broker's key fingerprint instead of a CA"},
        {"cert", 0, true, "client certificate file, for mutual TLS"},
        {"key", 0, true, "client private key file, for mutual TLS"},
        {"insecure", 0, false, "TLS without verifying the broker (testing only)"},
        {"json", 'j', false, "print JSON lines instead of text"},
        {"help", 0, false, "show this help"},
    };
}

std::vector<ArgSpec> withConnectionSpecs(std::vector<ArgSpec> extra) {
    std::vector<ArgSpec> all = connectionSpecs();
    all.insert(all.end(), extra.begin(), extra.end());
    return all;
}

bool CliClient::configure(Args& args, const char* defaultClientIdPrefix, std::string& error) {
    json = args.has("json");
    const bool tls = args.has("tls") || args.has("cafile") || args.has("pin") ||
                     args.has("insecure");

    long long port = 0, keepAlive = 0, expiry = 0;
    if (!args.getInt("port", tls ? MQ_PORT_TLS : MQ_PORT_TCP, 1, 65535, port) ||
        !args.getInt("keepalive", 60, 0, 65535, keepAlive) ||
        !args.getInt("session-expiry", 0, 0, 0xFFFFFFFFll, expiry)) {
        error = args.error();
        return false;
    }

    const std::string host = args.get("host", "localhost");
    connection.host = host;
    connection.port = (uint16_t)port;
    connection.keepAlive = (uint16_t)keepAlive;
    connection.cleanStart = !args.has("no-clean");
    if (args.has("session-expiry")) {
        connection.hasSessionExpiry = true;
        connection.sessionExpiryInterval = (uint32_t)expiry;
    }

    const std::string version = args.get("mqtt-version", "5");
    if (version == "5" || version == "5.0") {
        connection.version = MqttVersion::V5;
    } else if (version == "311" || version == "3.1.1" || version == "4") {
        connection.version = MqttVersion::V311;
    } else {
        error = "--mqtt-version must be 5 or 311, not '" + version + "'";
        return false;
    }

    connection.clientId = args.has("id")
        ? args.get("id")
        : std::string(defaultClientIdPrefix) + "-" + std::to_string((long)
#if defined(_WIN32)
                                                                                _getpid()
#else
                                                                                getpid()
#endif
                                                                                );

    // -u and -P, or else the server's own login from the environment - so
    // `docker compose exec modbee modbee pub ...` logs in to the server it
    // runs beside without the password being typed (or shown in `ps`).
    const char* envUser = std::getenv("MODBEE_AUTHENTICATION_USERNAME");
    const char* envPassword = std::getenv("MODBEE_AUTHENTICATION_PASSWORD");
    if (args.has("user") || (envUser != nullptr && envUser[0] != '\0')) {
        connection.hasUserName = true;
        connection.userName = args.has("user") ? args.get("user") : envUser;
    }
    if (args.has("password") ||
        (!args.has("user") && envPassword != nullptr && envPassword[0] != '\0')) {
        const std::string password = args.has("password") ? args.get("password") : envPassword;
        connection.hasPassword = true;
        connection.password.assign(password.begin(), password.end());
    }

    // The CLI reports every outcome itself, so a lost connection ends the
    // command rather than being retried behind the user's back.
    behaviour.autoReconnect = false;

    if (tls) {
#if MQTT_TLS_SOCKETS
        MqttTlsTransport* secure = new MqttTlsTransport();
        secure->configure(host.c_str(), (uint16_t)port);
        if (args.has("cafile")) secure->setCaFile(args.get("cafile").c_str());
        if (args.has("pin")) secure->setPinnedKeyFingerprint(args.get("pin").c_str());
        if (args.has("cert") || args.has("key")) {
            if (!args.has("cert") || !args.has("key")) {
                delete secure;
                error = "mutual TLS needs both --cert and --key";
                return false;
            }
            secure->setClientCertificate(args.get("cert").c_str(), args.get("key").c_str());
        }
        if (args.has("insecure")) secure->setInsecureNoCertificateValidation();
        transport.reset(secure);
#else
        error = "this build has no TLS engine (neither wolfSSL nor OpenSSL was found)";
        return false;
#endif
    } else {
        MqttTcpTransport* plain = new MqttTcpTransport();
        plain->configure(host.c_str(), (uint16_t)port);
        transport.reset(plain);
    }

    client.begin(*transport, connection, behaviour);
    return true;
}

bool CliClient::connectAndWait(mq_time_t timeoutMs) {
    const MqttResult started = client.connect();
    if (started != MqttResult::Queued && started != MqttResult::Ok) {
        std::fprintf(stderr, "cannot connect: %s\n", mq_resultName(started));
        return false;
    }
    const mq_time_t start = mq_millis();
    while (!client.isConnected()) {
        if (client.state() == MqttState::Idle) break;   // concluded, unconnected
        if (mq_millis() - start >= timeoutMs || cliStopRequested()) break;
        client.poll();
        cliIdle(transport.get(), 20);
    }
    if (client.isConnected()) return true;

    const MqttStatus& status = client.status();
    std::fprintf(stderr, "could not connect to %s:%u: %s", connection.host.c_str(),
                 connection.port, mq_resultName(status.lastError));
    if (status.connectReasonCode != 0) {
        std::fprintf(stderr, " (CONNACK 0x%02X %s)", status.connectReasonCode,
                     connection.version == MqttVersion::V5
                         ? mq_reasonName(status.connectReasonCode, MQ_PKT_CONNACK)
                         : mq_returnCodeName(status.connectReasonCode));
    }
    std::fprintf(stderr, "\n");
    return false;
}

void CliClient::shutdown() {
    if (client.isConnected()) {
        client.disconnect();
        pollUntil([&] { return !client.isConnected(); }, 1000);
    }
    client.end();
}

// --- Output ---

std::string jsonString(const uint8_t* data, size_t length) {
    std::string out = "\"";
    for (size_t index = 0; index < length; index++) {
        const unsigned char c = data[index];
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char escaped[8];
                    std::snprintf(escaped, sizeof escaped, "\\u%04x", c);
                    out += escaped;
                } else {
                    out += (char)c;   // UTF-8 passes through as is
                }
        }
    }
    out += "\"";
    return out;
}

bool isPrintableText(const uint8_t* data, size_t length) {
    size_t index = 0;
    while (index < length) {
        const uint8_t c = data[index];
        size_t extra = 0;
        if (c < 0x80) {
            if (c < 0x20 && c != '\t' && c != '\n' && c != '\r') return false;
            if (c == 0x7F) return false;
        } else if ((c & 0xE0) == 0xC0 && c >= 0xC2) {
            extra = 1;
        } else if ((c & 0xF0) == 0xE0) {
            extra = 2;
        } else if ((c & 0xF8) == 0xF0 && c <= 0xF4) {
            extra = 3;
        } else {
            return false;
        }
        if (extra > 0 && index + extra >= length) return false;   // truncated
        for (size_t k = 1; k <= extra; k++) {
            if ((data[index + k] & 0xC0) != 0x80) return false;
        }
        index += 1 + extra;
    }
    return true;
}

std::string base64(const uint8_t* data, size_t length) {
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((length + 2) / 3 * 4);
    for (size_t index = 0; index < length; index += 3) {
        const uint32_t a = data[index];
        const uint32_t b = index + 1 < length ? data[index + 1] : 0;
        const uint32_t c = index + 2 < length ? data[index + 2] : 0;
        const uint32_t triple = (a << 16) | (b << 8) | c;
        out += alphabet[(triple >> 18) & 0x3F];
        out += alphabet[(triple >> 12) & 0x3F];
        out += index + 1 < length ? alphabet[(triple >> 6) & 0x3F] : '=';
        out += index + 2 < length ? alphabet[triple & 0x3F] : '=';
    }
    return out;
}

std::string messageJson(const MqttMessage& message) {
    std::string out = "{\"topic\":";
    out += jsonString(message.topic.data, message.topic.length);
    out += ",\"qos\":" + std::to_string(message.qos);
    out += message.retain ? ",\"retain\":true" : ",\"retain\":false";
    if (message.dup) out += ",\"dup\":true";

    // Text when it is text; base64 when it is not, and said so, so a consumer
    // never has to guess which it is looking at. Sparkplug payloads are
    // protobuf and will always be the second.
    if (isPrintableText(message.payload.data, message.payload.length)) {
        out += ",\"payload\":" + jsonString(message.payload.data, message.payload.length);
    } else {
        out += ",\"payload_base64\":\"" + base64(message.payload.data, message.payload.length) + "\"";
    }

    const MqttProperties* p = message.properties;
    if (p != nullptr) {
        std::string props;
        if (p->has(MQ_PROP_PAYLOAD_FORMAT_INDICATOR)) {
            props += ",\"payload_format\":" + std::to_string(p->payloadFormatIndicator);
        }
        if (p->has(MQ_PROP_MESSAGE_EXPIRY_INTERVAL)) {
            props += ",\"message_expiry\":" + std::to_string(p->messageExpiryInterval);
        }
        if (p->has(MQ_PROP_CONTENT_TYPE)) {
            props += ",\"content_type\":" + jsonString(p->contentType.data, p->contentType.length);
        }
        if (p->has(MQ_PROP_RESPONSE_TOPIC)) {
            props += ",\"response_topic\":" + jsonString(p->responseTopic.data, p->responseTopic.length);
        }
        if (p->has(MQ_PROP_CORRELATION_DATA)) {
            props += ",\"correlation_data_base64\":\"" +
                     base64(p->correlationData.data, p->correlationData.length) + "\"";
        }
        if (p->subscriptionIdCount > 0) {
            props += ",\"subscription_ids\":[";
            for (uint8_t k = 0; k < p->subscriptionIdCount; k++) {
                if (k) props += ",";
                props += std::to_string(p->subscriptionIds[k]);
            }
            props += "]";
        }
        if (p->userCount > 0) {
            // An array of pairs, not an object: MQTT 5 allows the same key
            // more than once and keeps the order, and an object would lose
            // both.
            props += ",\"user_properties\":[";
            for (uint8_t k = 0; k < p->userCount; k++) {
                if (k) props += ",";
                props += "[" + jsonString(p->user[k].key.data, p->user[k].key.length) + "," +
                         jsonString(p->user[k].value.data, p->user[k].value.length) + "]";
            }
            props += "]";
        }
        if (!props.empty()) out += ",\"properties\":{" + props.substr(1) + "}";
    }
    out += "}";
    return out;
}

bool splitUserProperty(const std::string& text, std::string& key, std::string& value) {
    const size_t equals = text.find('=');
    if (equals == std::string::npos) return false;
    key = text.substr(0, equals);
    value = text.substr(equals + 1);
    return true;
}

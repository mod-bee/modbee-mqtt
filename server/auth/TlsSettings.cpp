// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "TlsSettings.h"
#include "Json.h"

#include "transport/MqttCertificate.h"

#include <fstream>
#include <sstream>

namespace {

bool readText(const std::string& path, std::string& out) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return false;
    std::ostringstream text;
    text << file.rdbuf();
    out = text.str();
    return true;
}

#if MQTT_TLS_AVAILABLE
// Where a piece of material came from, for a message that sends somebody to
// the right place: the setting, and the file when it was one. Only a build
// with a TLS engine checks the material.
std::string source(const char* setting, const std::string& file) {
    return file.empty() ? std::string("broker.tls.") + setting + "-pem"
                        : std::string("broker.tls.") + setting + " (" + file + ")";
}
#endif

} // namespace

bool tlsFromSettings(const BrokerSection& b, ShardedTlsConfig& out, std::string& error) {
    out = ShardedTlsConfig();
    out.enabled = b.tls;
    out.port = b.tlsPort;
    out.clientCertificateOptional = b.clientCertificates == "optional";
    if (!b.tls) return true;

    // The certificate and its key travel together: both from the text, or
    // both from the files - never a stored certificate with the file's key.
    const bool text = !b.certificatePem.empty() && !b.keyPem.empty();
    const std::string certificateFile = text ? std::string() : b.certificateFile;
    const std::string keyFile = text ? std::string() : b.keyFile;
    if (text) {
        out.certificatePem = b.certificatePem;
        out.keyPem = b.keyPem;
    } else {
        if (!readText(b.certificateFile, out.certificatePem)) {
            error = "cannot read the certificate " + b.certificateFile + " (broker.tls.cert)";
            return false;
        }
        if (!readText(b.keyFile, out.keyPem)) {
            error = "cannot read the private key " + b.keyFile + " (broker.tls.key)";
            return false;
        }
    }
    if (!b.clientCaPem.empty()) {
        out.clientCaPem = b.clientCaPem;
    } else if (!b.clientCaFile.empty() && !readText(b.clientCaFile, out.clientCaPem)) {
        error = "cannot read the client CA " + b.clientCaFile + " (broker.tls.client-ca)";
        return false;
    }

#if MQTT_TLS_AVAILABLE
    std::string why;
    if (!mqttCertificateMatchesKey(out.certificatePem.c_str(), out.keyPem.c_str(), why)) {
        error = source("cert", certificateFile) + ": " + why;
        return false;
    }
    if (!out.clientCaPem.empty() && mqttCountCertificates(out.clientCaPem.c_str()) == 0) {
        error = source("client-ca", b.clientCaPem.empty() ? b.clientCaFile : std::string()) +
                ": the client CA would not parse";
        return false;
    }
#endif
    return true;
}

bool isLiveTlsSetting(const std::string& path) {
    return path == "broker.tls.cert-pem" || path == "broker.tls.key-pem" ||
           path == "broker.tls.client-ca-pem" || path == "broker.tls.client-certificates";
}

bool sameTlsMaterial(const ShardedTlsConfig& a, const ShardedTlsConfig& b) {
    return a.certificatePem == b.certificatePem && a.keyPem == b.keyPem &&
           a.certificateFile == b.certificateFile && a.keyFile == b.keyFile &&
           a.clientCaPem == b.clientCaPem &&
           (a.clientCaPem.empty() || a.clientCertificateOptional == b.clientCertificateOptional);
}

std::string describeTls(const ShardedTlsConfig& served, bool certificateLogin) {
    json::Object o;
    o.add("serving", served.enabled)
        .add("port", (long long)(served.enabled ? served.port : 0))
        .add("engine", MQTT_TLS_ENGINE_NAME);

    std::string certificate = "null", clientCa = "null";
#if MQTT_TLS_AVAILABLE
    if (served.enabled) {
        MqttCertificateFacts facts;
        std::string why;
        if (mqttReadCertificate(served.certificatePem.c_str(), facts, why)) {
            json::Object c;
            c.add("fingerprint", facts.fingerprint)
                .add("subject", facts.subject)
                .add("not_after", facts.notAfter)
                .raw("names", json::array(facts.names));
            certificate = c.str();
        }
        if (!served.clientCaPem.empty() &&
            mqttReadCertificate(served.clientCaPem.c_str(), facts, why)) {
            json::Object c;
            c.add("fingerprint", facts.fingerprint)
                .add("subject", facts.subject)
                .add("certificates", (long long)mqttCountCertificates(served.clientCaPem.c_str()));
            clientCa = c.str();
        }
    }
#endif
    o.raw("certificate", certificate)
        .add("key", served.enabled && !served.keyPem.empty() ? "set" : "not set")
        .raw("client_ca", clientCa)
        .add("client_certificates", !served.enabled || served.clientCaPem.empty()
                                        ? "none"
                                        : served.clientCertificateOptional ? "optional" : "required")
        .add("certificate_login", certificateLogin);
    return o.str();
}

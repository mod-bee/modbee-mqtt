// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ServerConfig.h"

#include "MqttTopic.h"
#include "transport/MqttCertificate.h"

#include <yaml.h>

#include <ctime>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

const char* const kMqttHeaderPlaceholders[] = {
    "mqtt-topic",
    "mqtt-qos",
    "mqtt-retain",
    "mqtt-clientid",
    "mqtt-username",
    "mqtt-content-type",
    "mqtt-correlation-data",
    "mqtt-payload-format-indicator",
    "mqtt-response-topic",
    "mqtt-message-expiry-interval",
    "mqtt-user-properties",
    nullptr,
};

namespace {

const char* processEnvironment(void*, const char* name) { return std::getenv(name); }

// "kafka.clusters[0].bootstrap-servers" -> "MODBEE_KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS"
std::string environmentName(const std::string& path) {
    std::string name = "MODBEE_";
    for (char c : path) {
        if (c == ']') continue;
        if (c == '.' || c == '[' || c == '-') {
            name.push_back('_');
        } else if (c >= 'a' && c <= 'z') {
            name.push_back((char)(c - 'a' + 'A'));
        } else {
            name.push_back(c);
        }
    }
    return name;
}

size_t editDistance(const std::string& a, const std::string& b) {
    std::vector<size_t> row(b.size() + 1);
    for (size_t j = 0; j <= b.size(); j++) row[j] = j;
    for (size_t i = 1; i <= a.size(); i++) {
        size_t previous = row[0];
        row[0] = i;
        for (size_t j = 1; j <= b.size(); j++) {
            const size_t keep = row[j];
            const size_t substitute = previous + (a[i - 1] == b[j - 1] ? 0 : 1);
            row[j] = std::min(std::min(row[j] + 1, row[j - 1] + 1), substitute);
            previous = keep;
        }
    }
    return row[b.size()];
}

bool parseInteger(const std::string& text, long long& out) {
    if (text.empty()) return false;
    errno = 0;
    char* end = nullptr;
    const long long value = std::strtoll(text.c_str(), &end, 10);
    if (errno == ERANGE || end == nullptr || *end != '\0') return false;
    out = value;
    return true;
}

bool parseFlag(const std::string& text, bool& out) {
    if (text == "true" || text == "yes" || text == "on") { out = true; return true; }
    if (text == "false" || text == "no" || text == "off") { out = false; return true; }
    return false;
}

// Kafka's own rule: 1 to 249 of [A-Za-z0-9._-], and not "." or "..".
bool kafkaTopicIsValid(const std::string& topic) {
    if (topic.empty() || topic.size() > 249 || topic == "." || topic == "..") return false;
    for (char c : topic) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (!ok) return false;
    }
    return true;
}

MqttSpan spanOf(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

// Checks the ${...} placeholders in a template against the allowed names, and
// returns the template with every placeholder removed - the literal text,
// which the caller can check on its own.
bool checkTemplate(const std::string& text, const char* const* allowed, bool allowLevel,
                   std::string& literal, std::string& error) {
    literal.clear();
    size_t at = 0;
    while (at < text.size()) {
        const size_t open = text.find("${", at);
        if (open == std::string::npos) {
            literal.append(text, at, std::string::npos);
            break;
        }
        literal.append(text, at, open - at);
        const size_t close = text.find('}', open + 2);
        if (close == std::string::npos) {
            error = "an unclosed ${ in \"" + text + "\"";
            return false;
        }
        const std::string name = text.substr(open + 2, close - open - 2);
        bool known = false;
        for (const char* const* candidate = allowed; *candidate != nullptr; candidate++) {
            if (name == *candidate) { known = true; break; }
        }
        static const char kLevel[] = "MQTT_TOPIC_LEVEL_";
        if (!known && allowLevel && name.compare(0, sizeof kLevel - 1, kLevel) == 0) {
            long long level = 0;
            known = parseInteger(name.substr(sizeof kLevel - 1), level) && level >= 0 && level < 1000;
        }
        if (!known) {
            std::string names;
            for (const char* const* candidate = allowed; *candidate != nullptr; candidate++) {
                if (!names.empty()) names += ", ";
                names += std::string("${") + *candidate + "}";
            }
            if (allowLevel) names += ", ${MQTT_TOPIC_LEVEL_n}";
            error = "unknown placeholder ${" + name + "}; the placeholders are " + names;
            return false;
        }
        at = close + 1;
    }
    return true;
}

class Loader;

// One mapping in the file - or one that is not there, in which case every
// lookup finds nothing but the environment can still answer.
class Section {
public:
    Section(Loader& loader, yaml_node_t* node, const std::string& path)
        : _loader(loader), _node(node), _path(path) {}

    bool present() const { return _node != nullptr; }
    yaml_node_t* node() const { return _node; }
    const std::string& path() const { return _path; }
    std::string pathOf(const char* key) const { return _path.empty() ? key : _path + "." + key; }

    // The node under `key`, or null. Marks the key as known.
    yaml_node_t* find(const char* key);

    bool text(const char* key, std::string& out, bool required = false);
    bool integer(const char* key, long long& out, long long minimum, long long maximum);
    bool flag(const char* key, bool& out);
    bool choice(const char* key, std::string& out, const char* const* allowed);
    bool list(const char* key, std::vector<std::string>& out);
    bool secret(const char* key, std::string& out);
    Section child(const char* key);
    size_t count(const char* key);
    Section item(const char* key, size_t index);

    // Every key that was not asked for is reported, with the nearest one
    // that was as a suggestion.
    void finish();

private:
    Loader& _loader;
    yaml_node_t* _node;
    std::string _path;
    std::vector<std::string> _asked;
    std::string _fromEnvironment;   // the variable the last value came from, if any
    std::string origin(const char* key) const {
        (void)key;
        return _fromEnvironment.empty() ? std::string() : " (from " + _fromEnvironment + ")";
    }
    bool scalarOf(yaml_node_t* value, const std::string& path, std::string& out);
};

class Loader {
public:
    Loader(const ConfigSource& source, std::vector<ConfigProblem>& problems)
        : _source(source), _problems(problems) {}

    yaml_document_t document;
    bool errors = false;

    void problem(const yaml_node_t* at, const std::string& path, const std::string& message,
                 bool warning = false) {
        ConfigProblem p;
        if (at != nullptr) {
            p.line = at->start_mark.line + 1;
            p.column = at->start_mark.column + 1;
        }
        p.path = path;
        p.message = message;
        p.warning = warning;
        _problems.push_back(p);
        if (!warning) errors = true;
    }

    const char* environment(const std::string& path) {
        const std::string name = environmentName(path);
        ConfigEnvFn fn = _source.env != nullptr ? _source.env : &processEnvironment;
        return fn(_source.envContext, name.c_str());
    }
    const char* environmentFile(const std::string& path) {
        const std::string name = environmentName(path) + "_FILE";
        ConfigEnvFn fn = _source.env != nullptr ? _source.env : &processEnvironment;
        return fn(_source.envContext, name.c_str());
    }

    yaml_node_t* node(int index) { return yaml_document_get_node(&document, index); }

    static std::string scalar(const yaml_node_t* node) {
        return std::string((const char*)node->data.scalar.value, node->data.scalar.length);
    }

private:
    const ConfigSource& _source;
    std::vector<ConfigProblem>& _problems;
};

yaml_node_t* Section::find(const char* key) {
    _asked.push_back(key);
    if (_node == nullptr || _node->type != YAML_MAPPING_NODE) return nullptr;
    yaml_node_t* found = nullptr;
    for (yaml_node_pair_t* pair = _node->data.mapping.pairs.start;
         pair < _node->data.mapping.pairs.top; pair++) {
        yaml_node_t* k = _loader.node(pair->key);
        if (k == nullptr || k->type != YAML_SCALAR_NODE) continue;
        if (Loader::scalar(k) != key) continue;
        if (found != nullptr) {
            _loader.problem(k, pathOf(key), std::string("\"") + key + "\" appears twice");
            continue;
        }
        found = _loader.node(pair->value);
    }
    return found;
}

bool Section::scalarOf(yaml_node_t* value, const std::string& path, std::string& out) {
    if (value->type != YAML_SCALAR_NODE) {
        _loader.problem(value, path, "expected a single value, not a list or a mapping");
        return false;
    }
    out = Loader::scalar(value);
    return true;
}

bool Section::text(const char* key, std::string& out, bool required) {
    const std::string path = pathOf(key);
    yaml_node_t* value = find(key);
    _fromEnvironment.clear();
    if (const char* fromEnvironment = _loader.environment(path)) {
        _fromEnvironment = environmentName(path);
        out = fromEnvironment;
        return true;
    }
    if (value == nullptr) {
        if (required) _loader.problem(_node, path, std::string("\"") + key + "\" is required");
        return false;
    }
    if (!scalarOf(value, path, out)) return false;
    if (required && out.empty()) {
        _loader.problem(value, path, std::string("\"") + key + "\" must not be empty");
        return false;
    }
    return true;
}

bool Section::integer(const char* key, long long& out, long long minimum, long long maximum) {
    std::string value;
    if (!text(key, value)) return false;
    long long parsed = 0;
    if (!parseInteger(value, parsed) || parsed < minimum || parsed > maximum) {
        _loader.problem(find(key), pathOf(key),
                        "expected a whole number from " + std::to_string(minimum) + " to " +
                        std::to_string(maximum) + ", not \"" + value + "\"" + origin(key));
        return false;
    }
    out = parsed;
    return true;
}

bool Section::flag(const char* key, bool& out) {
    std::string value;
    if (!text(key, value)) return false;
    if (!parseFlag(value, out)) {
        _loader.problem(find(key), pathOf(key),
                        "expected true or false, not \"" + value + "\"" + origin(key));
        return false;
    }
    return true;
}

bool Section::choice(const char* key, std::string& out, const char* const* allowed) {
    std::string value;
    if (!text(key, value)) return false;
    std::string names;
    for (const char* const* candidate = allowed; *candidate != nullptr; candidate++) {
        if (value == *candidate) { out = value; return true; }
        if (!names.empty()) names += ", ";
        names += *candidate;
    }
    _loader.problem(find(key), pathOf(key),
                    "expected one of " + names + ", not \"" + value + "\"" + origin(key));
    return false;
}

bool Section::list(const char* key, std::vector<std::string>& out) {
    const std::string path = pathOf(key);
    yaml_node_t* value = find(key);
    if (const char* fromEnvironment = _loader.environment(path)) {
        out.clear();
        std::string item;
        std::istringstream split(fromEnvironment);
        while (std::getline(split, item, ',')) {
            if (!item.empty()) out.push_back(item);
        }
        return true;
    }
    if (value == nullptr) return false;
    out.clear();
    if (value->type == YAML_SCALAR_NODE) {
        out.push_back(Loader::scalar(value));
        return true;
    }
    if (value->type != YAML_SEQUENCE_NODE) {
        _loader.problem(value, path, "expected a value or a list of values");
        return false;
    }
    bool ok = true;
    for (yaml_node_item_t* at = value->data.sequence.items.start;
         at < value->data.sequence.items.top; at++) {
        yaml_node_t* element = _loader.node(*at);
        std::string text;
        if (element != nullptr && scalarOf(element, path, text)) {
            out.push_back(text);
        } else {
            ok = false;
        }
    }
    return ok;
}

bool Section::secret(const char* key, std::string& out) {
    const std::string path = pathOf(key);
    const std::string fileKey = std::string(key) + "-file";
    std::string fileName;
    // Precedence: the environment over the file, and a direct value over a
    // file of it within each.
    if (const char* direct = _loader.environment(path)) {
        find(key);
        find(fileKey.c_str());
        out = direct;
        return true;
    }
    if (const char* named = _loader.environmentFile(path)) {
        fileName = named;
    }
    if (fileName.empty()) {
        if (text(key, out)) {
            find(fileKey.c_str());
            return true;
        }
        if (!text(fileKey.c_str(), fileName)) return false;
    } else {
        find(key);
        find(fileKey.c_str());
    }
    std::ifstream file(fileName, std::ios::binary);
    if (!file) {
        _loader.problem(find(fileKey.c_str()), path, "cannot read the secret file " + fileName);
        return false;
    }
    std::ostringstream contents;
    contents << file.rdbuf();
    out = contents.str();
    // One trailing newline is how every editor and `echo` writes a file; it is
    // never part of a password.
    while (!out.empty() && (out.back() == '\n' || out.back() == '\r')) out.pop_back();
    return true;
}

Section Section::child(const char* key) {
    yaml_node_t* value = find(key);
    if (value != nullptr && value->type == YAML_SCALAR_NODE && value->data.scalar.length == 0) {
        value = nullptr;     // "key:" with nothing after it
    }
    if (value != nullptr && value->type != YAML_MAPPING_NODE) {
        _loader.problem(value, pathOf(key), "expected a mapping of keys and values");
        value = nullptr;
    }
    return Section(_loader, value, pathOf(key));
}

size_t Section::count(const char* key) {
    yaml_node_t* value = find(key);
    if (value == nullptr) return 0;
    if (value->type == YAML_SCALAR_NODE && value->data.scalar.length == 0) return 0;
    if (value->type != YAML_SEQUENCE_NODE) {
        _loader.problem(value, pathOf(key), "expected a list (each entry starting with \"- \")");
        return 0;
    }
    return (size_t)(value->data.sequence.items.top - value->data.sequence.items.start);
}

Section Section::item(const char* key, size_t index) {
    yaml_node_t* list = find(key);
    const std::string path = pathOf(key) + "[" + std::to_string(index) + "]";
    yaml_node_t* element = _loader.node(list->data.sequence.items.start[index]);
    if (element != nullptr && element->type != YAML_MAPPING_NODE) {
        _loader.problem(element, path, "expected a mapping of keys and values");
        element = nullptr;
    }
    return Section(_loader, element, path);
}

void Section::finish() {
    if (_node == nullptr || _node->type != YAML_MAPPING_NODE) return;
    for (yaml_node_pair_t* pair = _node->data.mapping.pairs.start;
         pair < _node->data.mapping.pairs.top; pair++) {
        yaml_node_t* k = _loader.node(pair->key);
        if (k == nullptr || k->type != YAML_SCALAR_NODE) {
            _loader.problem(k, _path, "a key must be a plain name");
            continue;
        }
        const std::string name = Loader::scalar(k);
        bool known = false;
        for (const std::string& asked : _asked) {
            if (asked == name) { known = true; break; }
        }
        if (known) continue;
        std::string best;
        size_t bestDistance = 4;
        for (const std::string& asked : _asked) {
            const size_t distance = editDistance(name, asked);
            if (distance < bestDistance) { bestDistance = distance; best = asked; }
        }
        _loader.problem(k, pathOf(name.c_str()),
                        "unknown key \"" + name + "\"" +
                        (best.empty() ? std::string() : " - did you mean \"" + best + "\"?"));
    }
}

// --- The schema --------------------------------------------------------------

// The PEM text is checked here, where every other value is - so config.check
// and config.set refuse a certificate that will not load, or a key that is
// not its key, while the old one is still being served, rather than the
// server discovering it at the next start.
void checkTlsText(Loader& loader, Section& tls, const BrokerSection& out) {
    if (out.certificatePem.empty() != out.keyPem.empty()) {
        const char* missing = out.certificatePem.empty() ? "cert-pem" : "key-pem";
        loader.problem(nullptr, tls.pathOf(missing),
                       "cert-pem and key-pem go together: a certificate cannot be served "
                       "without its key, and a key is nothing without its certificate");
    }
#if MQTT_TLS_AVAILABLE
    if (!out.certificatePem.empty() && !out.keyPem.empty()) {
        std::string why;
        if (!mqttCertificateMatchesKey(out.certificatePem.c_str(), out.keyPem.c_str(), why)) {
            loader.problem(nullptr, tls.pathOf("cert-pem"), why);
        } else {
            MqttCertificateFacts facts;
            if (mqttReadCertificate(out.certificatePem.c_str(), facts, why) &&
                facts.notAfterSeconds < (int64_t)std::time(nullptr)) {
                loader.problem(nullptr, tls.pathOf("cert-pem"),
                               "the certificate expired at " + facts.notAfter +
                               ": clients that check will refuse it", true);
            }
        }
    }
    if (!out.clientCaPem.empty() && mqttCountCertificates(out.clientCaPem.c_str()) == 0) {
        loader.problem(nullptr, tls.pathOf("client-ca-pem"),
                       "the client CA would not parse: it must be one or more PEM certificates");
    }
#else
    if (!out.certificatePem.empty() || !out.clientCaPem.empty()) {
        loader.problem(nullptr, tls.pathOf("cert-pem"),
                       "this build has no TLS engine, so it cannot serve a certificate");
    }
#endif
}

void readBroker(Loader& loader, Section broker, BrokerSection& out) {
    long long n = 0;
    broker.text("address", out.address);
    broker.flag("plain", out.plain);
    if (broker.integer("port", n, 0, 65535)) out.port = (uint16_t)n;
    // TLS. Read whether or not the section is there, as events: is: a
    // certificate and its key given as PEM text - stored with config.set, or
    // in the environment - turn the listener on by themselves, so a server
    // whose file never mentioned TLS can be given it over MQTT.
    Section tls = broker.child("tls");
    if (tls.integer("port", n, 0, 65535)) out.tlsPort = (uint16_t)n;
    tls.text("cert-pem", out.certificatePem);
    tls.text("key-pem", out.keyPem);
    tls.text("client-ca-pem", out.clientCaPem);
    static const char* const kClientCertificates[] = {"required", "optional", nullptr};
    tls.choice("client-certificates", out.clientCertificates, kClientCertificates);
    const bool pemPair = !out.certificatePem.empty() && !out.keyPem.empty();
    if (tls.present()) {
        out.tls = true;
        // A path is required only when there is no text to serve instead.
        tls.text("cert", out.certificateFile, !pemPair);
        tls.text("key", out.keyFile, !pemPair);
        tls.text("client-ca", out.clientCaFile);
        tls.finish();
    } else if (pemPair) {
        out.tls = true;
    }
    checkTlsText(loader, tls, out);
    if (broker.integer("shards", n, 0, 1024)) out.shards = (size_t)n;
    broker.integer("max-connections", out.maxConnections, 1, 100000000);
    broker.integer("max-sessions", out.maxSessions, 0, 100000000);
    if (broker.integer("max-packet", n, 64, 268435455)) out.maxPacket = (uint32_t)n;
    if (broker.integer("queue", n, 1, 1000000)) out.queue = (size_t)n;
    if (broker.integer("subscriptions", n, 1, 100000)) out.subscriptions = (size_t)n;
    broker.integer("max-session-expiry", out.maxSessionExpiry, 0, 4294967295LL);
    if (broker.integer("max-retained", n, 0, 10000000)) out.maxRetained = (size_t)n;
    broker.flag("pin-threads", out.pinThreads);
    broker.integer("status-every", out.statusEvery, 0, 86400);
    broker.finish();
    if (!out.plain && !out.tls) {
        loader.problem(broker.node(), broker.path(),
                       "no listener: plain is false and there is no tls section");
    }
}

void readCluster(Loader& loader, Section c, KafkaClusterConfig& out) {
    out.line = c.node() ? c.node()->start_mark.line + 1 : 0;
    c.text("id", out.id, true);
    std::vector<std::string> servers;
    if (c.list("bootstrap-servers", servers) && !servers.empty()) {
        for (const std::string& server : servers) {
            if (!out.bootstrapServers.empty()) out.bootstrapServers += ",";
            out.bootstrapServers += server;
        }
    } else {
        loader.problem(c.node(), c.pathOf("bootstrap-servers"), "\"bootstrap-servers\" is required");
    }
    Section tls = c.child("tls");
    if (tls.present()) {
        out.tls = true;
        tls.text("ca", out.caFile);
        tls.text("cert", out.certificateFile);
        tls.text("key", out.keyFile);
        tls.flag("verify-hostname", out.verifyHostname);
        tls.finish();
        if (out.certificateFile.empty() != out.keyFile.empty()) {
            loader.problem(tls.node(), tls.path(), "a client certificate needs both cert and key");
        }
    }
    Section auth = c.child("authentication");
    if (auth.present()) {
        static const char* const kMechanisms[] = {"none", "plain", "scram-sha256", "scram-sha512",
                                                  "oauthbearer", nullptr};
        auth.choice("mechanism", out.mechanism, kMechanisms);
        auth.text("username", out.username);
        auth.secret("password", out.password);
        auth.finish();
        if ((out.mechanism == "plain" || out.mechanism.compare(0, 5, "scram") == 0) &&
            (out.username.empty() || out.password.empty())) {
            loader.problem(auth.node(), auth.path(),
                           out.mechanism + " needs a username and a password (or password-file)");
        }
    }
    if (yaml_node_t* properties = c.find("properties")) {
        if (properties->type != YAML_MAPPING_NODE) {
            loader.problem(properties, c.pathOf("properties"), "expected a mapping of librdkafka settings");
        } else {
            for (yaml_node_pair_t* pair = properties->data.mapping.pairs.start;
                 pair < properties->data.mapping.pairs.top; pair++) {
                yaml_node_t* k = loader.node(pair->key);
                yaml_node_t* v = loader.node(pair->value);
                if (k->type != YAML_SCALAR_NODE || v->type != YAML_SCALAR_NODE) {
                    loader.problem(k, c.pathOf("properties"), "each setting is a name and a single value");
                    continue;
                }
                out.properties.push_back(std::make_pair(Loader::scalar(k), Loader::scalar(v)));
            }
        }
    }
    c.finish();
}

void readHeaders(Loader& loader, Section& m, MqttToKafkaConfig& out) {
    yaml_node_t* headers = m.find("headers");
    if (const char* fromEnvironment = loader.environment(m.pathOf("headers"))) {
        const std::string value = fromEnvironment;
        if (value == "all") out.allHeaders = true;
        else if (value != "none") loader.problem(nullptr, m.pathOf("headers"), "the environment may set headers to all or none");
        return;
    }
    if (headers == nullptr) return;
    if (headers->type == YAML_SCALAR_NODE) {
        const std::string value = Loader::scalar(headers);
        if (value == "all") out.allHeaders = true;
        else if (value != "none" && !value.empty()) {
            loader.problem(headers, m.pathOf("headers"), "expected all, none, or a list, not \"" + value + "\"");
        }
        return;
    }
    if (headers->type != YAML_SEQUENCE_NODE) {
        loader.problem(headers, m.pathOf("headers"), "expected all, none, or a list");
        return;
    }
    for (yaml_node_item_t* at = headers->data.sequence.items.start;
         at < headers->data.sequence.items.top; at++) {
        yaml_node_t* element = loader.node(*at);
        KafkaHeaderSpec spec;
        if (element->type == YAML_SCALAR_NODE) {
            spec.name = Loader::scalar(element);
            bool known = false;
            for (const char* const* p = kMqttHeaderPlaceholders; *p != nullptr; p++) {
                if (spec.name == *p) { known = true; break; }
            }
            if (!known) {
                std::string best;
                size_t bestDistance = 5;
                for (const char* const* p = kMqttHeaderPlaceholders; *p != nullptr; p++) {
                    const size_t d = editDistance(spec.name, *p);
                    if (d < bestDistance) { bestDistance = d; best = *p; }
                }
                loader.problem(element, m.pathOf("headers"),
                               "unknown placeholder \"" + spec.name + "\"" +
                               (best.empty() ? std::string(" - a fixed header is written name: value")
                                             : " - did you mean \"" + best + "\"?"));
                continue;
            }
        } else if (element->type == YAML_MAPPING_NODE &&
                   element->data.mapping.pairs.top - element->data.mapping.pairs.start == 1) {
            yaml_node_t* k = loader.node(element->data.mapping.pairs.start->key);
            yaml_node_t* v = loader.node(element->data.mapping.pairs.start->value);
            if (k->type != YAML_SCALAR_NODE || v->type != YAML_SCALAR_NODE) {
                loader.problem(element, m.pathOf("headers"), "a fixed header is written name: value");
                continue;
            }
            spec.name = Loader::scalar(k);
            spec.value = Loader::scalar(v);
            spec.fixed = true;
            if (spec.name.compare(0, 5, "mqtt-") == 0) {
                loader.problem(k, m.pathOf("headers"),
                               "header names starting mqtt- are reserved for the MQTT message itself");
                continue;
            }
        } else {
            loader.problem(element, m.pathOf("headers"),
                           "each header is a placeholder (mqtt-topic) or name: value");
            continue;
        }
        out.headers.push_back(spec);
    }
}

void readToKafka(Loader& loader, Section m, const ServerConfig& config, MqttToKafkaConfig& out) {
    out.line = m.node() ? m.node()->start_mark.line + 1 : 0;
    m.text("id", out.id, true);
    if (!m.text("cluster", out.cluster) && config.clusters.size() == 1) out.cluster = config.clusters[0].id;
    if (!m.list("mqtt-topic-filters", out.filters) || out.filters.empty()) {
        loader.problem(m.node(), m.pathOf("mqtt-topic-filters"), "\"mqtt-topic-filters\" is required");
    }
    for (const std::string& filter : out.filters) {
        if (!mq_topicFilterIsValid(spanOf(filter))) {
            loader.problem(m.find("mqtt-topic-filters"), m.pathOf("mqtt-topic-filters"),
                           "\"" + filter + "\" is not a valid MQTT topic filter");
        } else if (mq_isSharedFilter(spanOf(filter))) {
            loader.problem(m.find("mqtt-topic-filters"), m.pathOf("mqtt-topic-filters"),
                           "\"" + filter + "\" is a shared subscription; a mapping takes every "
                           "matching message, so use the filter inside it");
        }
    }
    if (m.text("kafka-topic", out.kafkaTopic, true) && !kafkaTopicIsValid(out.kafkaTopic)) {
        loader.problem(m.find("kafka-topic"), m.pathOf("kafka-topic"),
                       "\"" + out.kafkaTopic + "\" is not a valid Kafka topic name "
                       "(1 to 249 of letters, digits, '.', '_' and '-')");
    }
    std::string acks;
    static const char* const kAcks[] = {"zero", "one", "all", nullptr};
    if (m.choice("kafka-acks", acks, kAcks)) out.acks = acks == "zero" ? 0 : acks == "one" ? 1 : -1;
    long long n = 0;
    if (m.integer("max-record-bytes", n, 1, 1LL << 30)) out.maxRecordBytes = (size_t)n;

    std::string key;
    if (m.text("key", key)) {
        if (key == "topic") out.key = KafkaKeyMode::Topic;
        else if (key == "clientid") out.key = KafkaKeyMode::ClientId;
        else if (key == "none") out.key = KafkaKeyMode::None;
        else if (key.find("${") != std::string::npos) {
            static const char* const kKeyPlaceholders[] = {"MQTT_TOPIC", "MQTT_CLIENTID", nullptr};
            std::string literal, error;
            if (checkTemplate(key, kKeyPlaceholders, true, literal, error)) {
                out.key = KafkaKeyMode::Template;
                out.keyTemplate = key;
            } else {
                loader.problem(m.find("key"), m.pathOf("key"), error);
            }
        } else {
            loader.problem(m.find("key"), m.pathOf("key"),
                           "expected topic, clientid, none, or a template such as "
                           "\"${MQTT_TOPIC_LEVEL_1}/${MQTT_TOPIC_LEVEL_2}\", not \"" + key + "\"");
        }
    }
    readHeaders(loader, m, out);
    std::string strategy;
    static const char* const kStrategies[] = {"key-based", "uniform", nullptr};
    if (m.choice("partition-strategy", strategy, kStrategies)) out.uniform = strategy == "uniform";
    std::string acknowledge;
    static const char* const kAcknowledge[] = {"kafka", "routed", nullptr};
    if (m.choice("acknowledge", acknowledge, kAcknowledge)) out.acknowledgeOnKafka = acknowledge == "kafka";
    m.finish();
}

void readFromKafka(Loader& loader, Section m, const ServerConfig& config, KafkaToMqttConfig& out) {
    out.line = m.node() ? m.node()->start_mark.line + 1 : 0;
    m.text("id", out.id, true);
    if (!m.text("cluster", out.cluster) && config.clusters.size() == 1) out.cluster = config.clusters[0].id;
    const bool listed = m.list("kafka-topics", out.kafkaTopics) && !out.kafkaTopics.empty();
    const bool pattern = m.text("kafka-topic-pattern", out.kafkaTopicPattern) && !out.kafkaTopicPattern.empty();
    if (listed == pattern) {
        loader.problem(m.node(), m.path(), "give exactly one of kafka-topics or kafka-topic-pattern");
    }
    for (const std::string& topic : out.kafkaTopics) {
        if (!kafkaTopicIsValid(topic)) {
            loader.problem(m.find("kafka-topics"), m.pathOf("kafka-topics"),
                           "\"" + topic + "\" is not a valid Kafka topic name");
        }
    }
    if (!m.list("mqtt-topic", out.mqttTopics) || out.mqttTopics.empty()) {
        out.mqttTopics.assign(1, "${KAFKA_TOPIC}");
    }
    static const char* const kTopicPlaceholders[] = {"KAFKA_TOPIC", "KAFKA_KEY", nullptr};
    for (const std::string& topic : out.mqttTopics) {
        std::string literal, error;
        if (!checkTemplate(topic, kTopicPlaceholders, false, literal, error)) {
            loader.problem(m.find("mqtt-topic"), m.pathOf("mqtt-topic"), error);
        } else if (literal.find_first_of("+#") != std::string::npos) {
            loader.problem(m.find("mqtt-topic"), m.pathOf("mqtt-topic"),
                           "\"" + topic + "\" contains a wildcard; a message is published to a topic, not a filter");
        } else if (topic.find("${") == std::string::npos && !mq_topicNameIsValid(spanOf(topic))) {
            loader.problem(m.find("mqtt-topic"), m.pathOf("mqtt-topic"),
                           "\"" + topic + "\" is not a valid MQTT topic name");
        }
    }
    m.flag("from-headers", out.fromHeaders);
    long long n = 0;
    if (m.integer("qos", n, 0, 2)) out.qos = (uint8_t)n;
    m.flag("retain", out.retain);
    if (!m.text("consumer-group", out.group) || out.group.empty()) out.group = "modbee-" + out.id + "-group";

    Section properties = m.child("properties");
    if (properties.present()) {
        properties.text("content-type", out.contentType);
        if (properties.text("response-topic", out.responseTopic) &&
            !mq_topicNameIsValid(spanOf(out.responseTopic))) {
            loader.problem(properties.find("response-topic"), properties.pathOf("response-topic"),
                           "\"" + out.responseTopic + "\" is not a valid MQTT topic name");
        }
        if (properties.integer("payload-format-indicator", n, 0, 1)) {
            out.hasPayloadFormat = true;
            out.payloadFormat = (uint8_t)n;
        }
        if (properties.integer("message-expiry-interval", n, 0, 4294967295LL)) {
            out.hasMessageExpiry = true;
            out.messageExpiry = (uint32_t)n;
        }
        if (yaml_node_t* user = properties.find("user-properties")) {
            bool ok = user->type == YAML_SEQUENCE_NODE;
            if (ok) {
                for (yaml_node_item_t* at = user->data.sequence.items.start;
                     at < user->data.sequence.items.top; at++) {
                    yaml_node_t* element = loader.node(*at);
                    if (element->type != YAML_MAPPING_NODE ||
                        element->data.mapping.pairs.top - element->data.mapping.pairs.start != 1) {
                        ok = false;
                        break;
                    }
                    yaml_node_t* k = loader.node(element->data.mapping.pairs.start->key);
                    yaml_node_t* v = loader.node(element->data.mapping.pairs.start->value);
                    if (k->type != YAML_SCALAR_NODE || v->type != YAML_SCALAR_NODE) { ok = false; break; }
                    out.userProperties.push_back(std::make_pair(Loader::scalar(k), Loader::scalar(v)));
                }
            }
            if (!ok) {
                loader.problem(user, properties.pathOf("user-properties"),
                               "expected a list of name: value pairs (a name may repeat)");
            }
        }
        properties.finish();
    }
    m.finish();
}

void crossCheck(Loader& loader, ServerConfig& config) {
    if (!config.authentication.database.empty() && !config.authentication.hasAdmin) {
        loader.problem(nullptr, "authentication.database",
                       "a login database needs an admin to manage it: set authentication.username "
                       "and password (MQTT_USERNAME / MQTT_PASSWORD in .env)");
    }
    const BrokerSection& b = config.broker;
    const bool clientCa = !b.clientCaFile.empty() || !b.clientCaPem.empty();
    if (config.authentication.certificateLogin && config.authentication.database.empty()) {
        loader.problem(nullptr, "authentication.certificate-login",
                       "certificate-login takes the login from a certificate and looks it up in "
                       "the login database - it needs authentication.database");
    }
    if (config.authentication.certificateLogin && (!b.tls || !clientCa)) {
        loader.problem(nullptr, "authentication.certificate-login",
                       "no client certificate can be verified without TLS and a client CA "
                       "(broker.tls.client-ca or client-ca-pem), so nobody can log in this way",
                       true);
    }
    if (b.clientCertificates == "optional" && !clientCa) {
        loader.problem(nullptr, "broker.tls.client-certificates",
                       "optional client certificates need a client CA to verify them against "
                       "(broker.tls.client-ca or client-ca-pem); without one none is asked for",
                       true);
    }
    RetainedSection& retained = config.retained;
    if (retained.store == RetainedStore::File && retained.file.empty()) {
        loader.problem(nullptr, "retained.file", "store: file needs a file to keep them in");
    }
    if (retained.store == RetainedStore::Kafka) {
        if (config.clusters.empty()) {
            loader.problem(nullptr, "retained.store",
                           "store: kafka needs a Kafka cluster - add kafka.clusters, or use store: file");
        } else if (retained.cluster.empty()) {
            if (config.clusters.size() == 1) retained.cluster = config.clusters[0].id;
            else loader.problem(nullptr, "retained.cluster",
                                "\"cluster\" is required when there is more than one");
        } else if (config.cluster(retained.cluster) == nullptr) {
            loader.problem(nullptr, "retained.cluster",
                           "no cluster has the id \"" + retained.cluster + "\"");
        }
        if (retained.topic.empty()) {
            loader.problem(nullptr, "retained.topic", "store: kafka needs a topic name");
        }
    }

    std::vector<std::string> ids;
    for (const KafkaClusterConfig& c : config.clusters) {
        for (const std::string& seen : ids) {
            if (seen == c.id) {
                loader.problem(nullptr, "kafka.clusters", "cluster id \"" + c.id + "\" is used twice");
            }
        }
        ids.push_back(c.id);
    }
    std::vector<std::pair<std::string, size_t>> mappings;
    const auto mappingId = [&](const std::string& id, size_t line, const std::string& path) {
        for (const auto& seen : mappings) {
            if (seen.first == id) {
                loader.problem(nullptr, path, "mapping id \"" + id + "\" (line " + std::to_string(line) +
                               ") is already used at line " + std::to_string(seen.second) +
                               "; ids name consumer groups and metrics, so they must be unique");
            }
        }
        mappings.push_back(std::make_pair(id, line));
    };
    for (size_t i = 0; i < config.toKafka.size(); i++) {
        const MqttToKafkaConfig& m = config.toKafka[i];
        const std::string path = "kafka.mqtt-to-kafka[" + std::to_string(i) + "]";
        mappingId(m.id, m.line, path);
        if (m.cluster.empty()) {
            loader.problem(nullptr, path + ".cluster", "\"cluster\" is required when there is not exactly one cluster");
        } else if (config.cluster(m.cluster) == nullptr) {
            loader.problem(nullptr, path + ".cluster", "no cluster has the id \"" + m.cluster + "\"");
        }
    }
    for (size_t i = 0; i < config.fromKafka.size(); i++) {
        const KafkaToMqttConfig& m = config.fromKafka[i];
        const std::string path = "kafka.kafka-to-mqtt[" + std::to_string(i) + "]";
        mappingId(m.id, m.line, path);
        if (m.cluster.empty()) {
            loader.problem(nullptr, path + ".cluster", "\"cluster\" is required when there is not exactly one cluster");
        } else if (config.cluster(m.cluster) == nullptr) {
            loader.problem(nullptr, path + ".cluster", "no cluster has the id \"" + m.cluster + "\"");
        }
        // A LOOP. A message consumed from Kafka topic T is published to MQTT,
        // where an mqtt-to-kafka mapping writing to T on the same cluster may
        // match it. The pipe never writes a message back to the topic it came
        // from, so this cannot spin; it is said because it is rarely intended.
        for (const MqttToKafkaConfig& to : config.toKafka) {
            if (to.cluster != m.cluster) continue;
            bool sameTopic = false;
            for (const std::string& topic : m.kafkaTopics) sameTopic = sameTopic || topic == to.kafkaTopic;
            if (!sameTopic) continue;
            for (const std::string& mqttTopic : m.mqttTopics) {
                bool matches = mqttTopic.find("${") != std::string::npos;    // cannot tell; say so
                for (const std::string& filter : to.filters) {
                    matches = matches || mq_topicMatches(spanOf(filter), spanOf(mqttTopic));
                }
                if (matches) {
                    loader.problem(nullptr, path,
                                   "messages from Kafka topic \"" + to.kafkaTopic +
                                   "\" may match mqtt-to-kafka mapping \"" + to.id +
                                   "\", which writes to the same topic. They are never written back "
                                   "(no loop), but check this is what you meant",
                                   true);
                    break;
                }
            }
        }
    }
}

} // namespace

bool loadServerConfig(const std::string& yamlText, const ConfigSource& source,
                      ServerConfig& out, std::vector<ConfigProblem>& problems) {
    out = ServerConfig();
    Loader loader(source, problems);

    yaml_parser_t parser;
    if (!yaml_parser_initialize(&parser)) {
        loader.problem(nullptr, "", "the YAML parser could not be created");
        return false;
    }
    yaml_parser_set_input_string(&parser, (const unsigned char*)yamlText.data(), yamlText.size());
    if (!yaml_parser_load(&parser, &loader.document)) {
        ConfigProblem p;
        p.line = parser.problem_mark.line + 1;
        p.column = parser.problem_mark.column + 1;
        p.message = std::string("not valid YAML: ") + (parser.problem ? parser.problem : "unknown error");
        if (parser.context) p.message += std::string(" ") + parser.context;
        problems.push_back(p);
        yaml_parser_delete(&parser);
        return false;
    }

    yaml_node_t* root = yaml_document_get_root_node(&loader.document);
    if (root != nullptr && root->type != YAML_MAPPING_NODE &&
        !(root->type == YAML_SCALAR_NODE && root->data.scalar.length == 0)) {
        loader.problem(root, "", "the file must be a mapping, starting with broker: or kafka:");
        root = nullptr;
    } else if (root != nullptr && root->type != YAML_MAPPING_NODE) {
        root = nullptr;
    }

    // One document only: a second "---" is almost always a paste accident.
    yaml_document_t second;
    if (yaml_parser_load(&parser, &second)) {
        if (yaml_document_get_root_node(&second) != nullptr) {
            yaml_node_t* extra = yaml_document_get_root_node(&second);
            ConfigProblem p;
            p.line = extra->start_mark.line + 1;
            p.message = "a second YAML document; modbee.yaml is one document";
            problems.push_back(p);
            loader.errors = true;
        }
        yaml_document_delete(&second);
    }

    Section top(loader, root, "");
    readBroker(loader, top.child("broker"), out.broker);
    Section metrics = top.child("metrics");
    if (metrics.present()) {
        out.metrics.enabled = true;
        metrics.flag("enabled", out.metrics.enabled);
        metrics.text("address", out.metrics.address);
        long long port = 0;
        if (metrics.integer("port", port, 0, 65535)) out.metrics.port = (uint16_t)port;
        metrics.finish();
    }
    // Read whether or not the section is there, so MODBEE_EVENTS_ENABLED alone
    // turns it on.
    Section events = top.child("events");
    if (events.present()) out.events.enabled = true;
    events.flag("enabled", out.events.enabled);
    if (events.present()) events.finish();
    // Who may connect. One login for every device -
    //   authentication: {username: devices, password: ...}
    // - or a list, one per device. Read whether or not the section is there,
    // so MODBEE_AUTHENTICATION_USERNAME and _PASSWORD alone turn it on, which
    // is how the Docker stacks set it from .env.
    Section auth = top.child("authentication");
    {
        MqttAccount one;
        const bool hasName = auth.text("username", one.username);
        const bool hasPassword = auth.secret("password", one.password);
        if (hasName || hasPassword) {
            if (one.username.empty()) {
                loader.problem(auth.node(), "authentication.username", "a password needs a user name");
            } else if (one.password.empty()) {
                loader.problem(auth.node(), "authentication.password",
                               "user \"" + one.username + "\" needs a password");
            } else {
                out.authentication.accounts.push_back(one);
                out.authentication.hasAdmin = true;
            }
        }
        auth.text("database", out.authentication.database);
        auth.flag("certificate-login", out.authentication.certificateLogin);
        static const char* const kDefaultTopics[] = {"all", "none", nullptr};
        auth.choice("default-topics", out.authentication.defaultTopics, kDefaultTopics);
        const size_t users = auth.count("users");
        for (size_t i = 0; i < users; i++) {
            Section user = auth.item("users", i);
            MqttAccount account;
            user.text("username", account.username, true);
            user.secret("password", account.password);
            if (!account.username.empty() && account.password.empty()) {
                loader.problem(user.node(), "authentication.users[" + std::to_string(i) + "].password",
                               "user \"" + account.username + "\" needs a password");
            }
            user.finish();
            if (!account.username.empty() && !account.password.empty()) {
                for (const MqttAccount& seen : out.authentication.accounts) {
                    if (seen.username == account.username) {
                        loader.problem(user.node(), "authentication.users[" + std::to_string(i) + "]",
                                       "user \"" + account.username + "\" is listed twice");
                    }
                }
                out.authentication.accounts.push_back(account);
            }
        }
        auth.finish();
    }

    // Read whether or not the section is there, so MODBEE_RETAINED_STORE=file
    // alone turns it on.
    Section retained = top.child("retained");
    static const char* const kStores[] = {"memory", "file", "kafka", nullptr};
    std::string store;
    if (retained.choice("store", store, kStores)) {
        out.retained.store = store == "file"  ? RetainedStore::File
                           : store == "kafka" ? RetainedStore::Kafka
                                              : RetainedStore::Memory;
    }
    retained.text("file", out.retained.file);
    retained.text("topic", out.retained.topic);
    retained.text("cluster", out.retained.cluster);
    retained.finish();

    Section kafka = top.child("kafka");
    const size_t clusters = kafka.count("clusters");
    for (size_t i = 0; i < clusters; i++) {
        KafkaClusterConfig c;
        readCluster(loader, kafka.item("clusters", i), c);
        out.clusters.push_back(c);
    }
    const size_t toKafka = kafka.count("mqtt-to-kafka");
    for (size_t i = 0; i < toKafka; i++) {
        MqttToKafkaConfig m;
        readToKafka(loader, kafka.item("mqtt-to-kafka", i), out, m);
        out.toKafka.push_back(m);
    }
    const size_t fromKafka = kafka.count("kafka-to-mqtt");
    for (size_t i = 0; i < fromKafka; i++) {
        KafkaToMqttConfig m;
        readFromKafka(loader, kafka.item("kafka-to-mqtt", i), out, m);
        out.fromKafka.push_back(m);
    }
    kafka.finish();
    if ((toKafka > 0 || fromKafka > 0) && clusters == 0) {
        loader.problem(kafka.node(), "kafka.clusters", "mappings need at least one cluster");
    }
    top.finish();
    crossCheck(loader, out);

    yaml_document_delete(&loader.document);
    yaml_parser_delete(&parser);

    // In file order: the order someone reading the file meets them.
    std::stable_sort(problems.begin(), problems.end(),
                     [](const ConfigProblem& a, const ConfigProblem& b) {
                         const size_t la = a.line == 0 ? (size_t)-1 : a.line;
                         const size_t lb = b.line == 0 ? (size_t)-1 : b.line;
                         return la < lb;
                     });
    return !loader.errors;
}

bool loadServerConfigFile(const std::string& path, ServerConfig& out,
                          std::vector<ConfigProblem>& problems) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        ConfigProblem p;
        p.message = "cannot read " + path;
        problems.push_back(p);
        return false;
    }
    std::ostringstream text;
    text << file.rdbuf();
    ConfigSource source;
    source.name = path;
    return loadServerConfig(text.str(), source, out, problems);
}

std::string configEnvironmentName(const std::string& path) { return environmentName(path); }

namespace {

struct EffectiveEnvironment {
    std::vector<std::pair<std::string, std::string>> overrides;   // MODBEE_ name -> value
    std::vector<std::string> asked;                               // every name the loader asked for
    std::vector<std::string> pinned;
};

const char* effectiveEnvironment(void* context, const char* name) {
    EffectiveEnvironment& env = *static_cast<EffectiveEnvironment*>(context);
    env.asked.push_back(name);
    if (const char* value = std::getenv(name)) {
        env.pinned.push_back(name);
        return value;
    }
    for (const auto& o : env.overrides) if (o.first == name) return o.second.c_str();
    return nullptr;
}

} // namespace

bool loadEffectiveConfig(const std::string& fileText, const std::string& fileName,
                         const StoredSettings& stored, ServerConfig& out,
                         std::vector<ConfigProblem>& problems, EffectiveConfigInfo* info) {
    EffectiveEnvironment env;
    for (const auto& o : stored.overrides) env.overrides.emplace_back(environmentName(o.first), o.second);
    ConfigSource source;
    source.name = stored.hasDocument ? std::string("the stored settings") : fileName;
    source.env = &effectiveEnvironment;
    source.envContext = &env;
    bool ok = loadServerConfig(stored.hasDocument ? stored.document : fileText, source, out, problems);
    // An override the loader never asked about names no setting: refused, as a
    // misspelt key in the file is, because a setting silently not set is the
    // worse failure.
    for (const auto& o : stored.overrides) {
        const std::string name = environmentName(o.first);
        bool asked = false;
        for (const std::string& a : env.asked) if (a == name) { asked = true; break; }
        if (!asked) {
            ConfigProblem p;
            p.path = o.first;
            p.message = "no setting is called \"" + o.first + "\"";
            problems.push_back(p);
            ok = false;
        }
    }
    if (info) {
        info->pinned.clear();
        for (const std::string& p : env.pinned) {
            if (std::find(info->pinned.begin(), info->pinned.end(), p) == info->pinned.end()) info->pinned.push_back(p);
        }
    }
    return ok;
}

std::string describe(const ConfigProblem& problem, const std::string& fileName) {
    std::string text = fileName;
    if (problem.line != 0) {
        text += ":" + std::to_string(problem.line);
        if (problem.column != 0) text += ":" + std::to_string(problem.column);
    }
    text += problem.warning ? ": warning: " : ": ";
    if (!problem.path.empty()) text += problem.path + ": ";
    text += problem.message;
    return text;
}

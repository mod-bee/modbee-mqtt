// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// =============================================================================
// modbee-mqtt <noun> <verb> - THE APPLICATION API, FROM THE COMMAND LINE
// =============================================================================
//
// One command for each call (ApiCommands.h), and nothing of its own: a
// command's flags are the fields of the call's request, found in the contract
// compiled into this program, and what it prints is the call's answer.
//
//   --client-id-prefix pump      client_id_prefix: "pump"     (kebab for snake)
//   --message.topic plant/x      message { topic: "plant/x" } (a dot goes inside)
//   --filters a --filters b      filters: ["a", "b"]          (repeated: repeat it)
//   --set broker.port=1884       set { "broker.port": "1884" } (a map: key=value)
//   --servers '{"host":"a"}'     servers: [{host: "a"}]       (a message: its JSON)
//   --confirm delivered          confirm: CONFIRM_DELIVERED   (an enum: its name, or its end)
//   --certificate-pem @cert.pem  the file's contents          (@@ for a leading @)
//   --json '{...}' / @file       the whole request as JSON first; flags after it win
//
// Where the server is: --api unix:<path> or tcp://host:port (MODBEE_API), its
// token --token-file (MODBEE_API_TOKEN_FILE) or --token (MODBEE_API_TOKEN), and
// --api-ca for a TLS listener. Errors are the call's own status, by name.

#include "ApiCommands.h"

#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#if defined(MODBEE_HAVE_APP_API)
#include "modbee/app/v1/app.grpc.pb.h"

#include <google/protobuf/util/json_util.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/client_unary_call.h>
#endif

namespace {

bool isNoun(const std::string& noun) {
    for (size_t i = 0; i < kApiCommandCount; i++) {
        if (noun == kApiCommands[i].noun) return true;
    }
    return false;
}

#if defined(MODBEE_HAVE_APP_API)
const ApiCommand* findCommand(const std::string& noun, const std::string& verb) {
    for (size_t i = 0; i < kApiCommandCount; i++) {
        if (noun == kApiCommands[i].noun && verb == kApiCommands[i].verb) return &kApiCommands[i];
    }
    return nullptr;
}

void nounHelp(const std::string& noun) {
    std::printf("usage: modbee-mqtt %s%s [fields] [--api ADDRESS] [--token-file FILE]\n\n", noun.c_str(),
                findCommand(noun, "") ? "" : " <verb>");
    for (size_t i = 0; i < kApiCommandCount; i++) {
        const ApiCommand& c = kApiCommands[i];
        if (noun != c.noun) continue;
        std::printf("  modbee-mqtt %s%s%-12s %s\n", c.noun, c.verb[0] ? " " : "", c.verb, c.call);
    }
    std::printf("\n`modbee-mqtt %s%s --help` lists a command's fields.\n", noun.c_str(),
                findCommand(noun, "") ? "" : " <verb>");
}
#endif

} // namespace

bool cliIsApiNoun(const char* noun) { return noun != nullptr && isNoun(noun); }

#if !defined(MODBEE_HAVE_APP_API)

int cmdApi(int argc, char** argv) {
    (void)argc;
    std::fprintf(stderr, "modbee-mqtt %s: this build has no Application API (gRPC was not found when it was built)\n",
                 argv[1]);
    return 2;
}

#else

namespace {

namespace gpb = google::protobuf;
namespace v1 = modbee::app::v1;

std::string snake(const std::string& kebab) {
    std::string out = kebab;
    for (char& c : out) {
        if (c == '-') c = '_';
    }
    return out;
}

std::string kebab(const std::string& snakeName) {
    std::string out = snakeName;
    for (char& c : out) {
        if (c == '_') c = '-';
    }
    return out;
}

// "@file" is the file's contents; "@@x" is "@x".
bool valueOf(const std::string& given, std::string& out, std::string& error) {
    if (given.size() < 2 || given[0] != '@') {
        out = given;
        return true;
    }
    if (given[1] == '@') {
        out = given.substr(1);
        return true;
    }
    std::ifstream file(given.substr(1), std::ios::binary);
    if (!file) {
        error = "cannot read " + given.substr(1);
        return false;
    }
    std::ostringstream text;
    text << file.rdbuf();
    out = text.str();
    return true;
}

const char* typeName(const gpb::FieldDescriptor* f) {
    if (f->is_map()) return "key=value";
    switch (f->cpp_type()) {
    case gpb::FieldDescriptor::CPPTYPE_STRING: return f->type() == gpb::FieldDescriptor::TYPE_BYTES ? "bytes" : "text";
    case gpb::FieldDescriptor::CPPTYPE_BOOL: return "";
    case gpb::FieldDescriptor::CPPTYPE_ENUM: return "name";
    case gpb::FieldDescriptor::CPPTYPE_MESSAGE: return "JSON";
    case gpb::FieldDescriptor::CPPTYPE_DOUBLE:
    case gpb::FieldDescriptor::CPPTYPE_FLOAT: return "number";
    default: return "integer";
    }
}

// Every flag a request takes: a field, or a field inside a singular message.
void fieldHelp(const gpb::Descriptor* d, const std::string& prefix, int depth) {
    for (int i = 0; i < d->field_count(); i++) {
        const gpb::FieldDescriptor* f = d->field(i);
        const std::string name = prefix + kebab(f->name());
        const bool inside = f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_MESSAGE && !f->is_repeated() && depth < 3;
        std::string line = "  --" + name;
        if (typeName(f)[0]) line += std::string(" ") + typeName(f);
        if (f->is_repeated()) line += f->is_map() ? "   (repeat for more)" : "   (repeat for each)";
        if (f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_ENUM) {
            line += "   ";
            const gpb::EnumDescriptor* e = f->enum_type();
            for (int v = 0; v < e->value_count(); v++) line += (v ? " | " : "") + e->value(v)->name();
        }
        if (inside) line += "   (or --" + name + ".<field>)";
        std::printf("%s\n", line.c_str());
        if (inside) fieldHelp(f->message_type(), name + ".", depth + 1);
    }
}

// The field a flag names, through singular messages; null, with `error`, for none.
const gpb::FieldDescriptor* resolve(const gpb::Descriptor* d, const std::string& path, std::string& error) {
    const size_t dot = path.find('.');
    const std::string head = snake(path.substr(0, dot));
    const gpb::FieldDescriptor* f = d->FindFieldByName(head);
    if (f == nullptr) {
        error = "no field \"" + kebab(head) + "\" in " + d->name();
        return nullptr;
    }
    if (dot == std::string::npos) return f;
    if (f->cpp_type() != gpb::FieldDescriptor::CPPTYPE_MESSAGE || f->is_repeated()) {
        error = "--" + path.substr(0, dot) + " has no fields of its own to name" +
                (f->is_repeated() && f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_MESSAGE
                     ? ": give each as JSON, --" + path.substr(0, dot) + " '{...}'"
                     : "");
        return nullptr;
    }
    return resolve(f->message_type(), path.substr(dot + 1), error);
}

bool enumValue(const gpb::EnumDescriptor* e, const std::string& given, int& out) {
    std::string upper = snake(given);
    for (char& c : upper) c = (char)std::toupper((unsigned char)c);
    for (int i = 0; i < e->value_count(); i++) {
        const std::string& name = e->value(i)->name();
        const bool ends = name.size() > upper.size() && name.compare(name.size() - upper.size(), upper.size(), upper) == 0 &&
                          name[name.size() - upper.size() - 1] == '_';
        if (name == upper || ends) {
            out = e->value(i)->number();
            return true;
        }
    }
    char* end = nullptr;
    const long n = std::strtol(given.c_str(), &end, 10);
    if (!given.empty() && *end == '\0' && e->FindValueByNumber((int)n) != nullptr) {
        out = (int)n;
        return true;
    }
    return false;
}

bool setScalar(gpb::Message& m, const gpb::FieldDescriptor* f, const std::string& given, std::string& error) {
    const gpb::Reflection* r = m.GetReflection();
    const bool many = f->is_repeated();
    std::string text;
    char* end = nullptr;
    switch (f->cpp_type()) {
    case gpb::FieldDescriptor::CPPTYPE_STRING:
        if (!valueOf(given, text, error)) return false;
        many ? r->AddString(&m, f, text) : r->SetString(&m, f, text);
        return true;
    case gpb::FieldDescriptor::CPPTYPE_BOOL: {
        const bool yes = given == "true" || given == "1" || given == "yes";
        if (!yes && given != "false" && given != "0" && given != "no") break;
        many ? r->AddBool(&m, f, yes) : r->SetBool(&m, f, yes);
        return true;
    }
    case gpb::FieldDescriptor::CPPTYPE_ENUM: {
        int value = 0;
        if (!enumValue(f->enum_type(), given, value)) break;
        many ? r->AddEnumValue(&m, f, value) : r->SetEnumValue(&m, f, value);
        return true;
    }
    case gpb::FieldDescriptor::CPPTYPE_INT32:
    case gpb::FieldDescriptor::CPPTYPE_INT64: {
        const long long n = std::strtoll(given.c_str(), &end, 10);
        if (given.empty() || *end != '\0') break;
        if (f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_INT32) {
            many ? r->AddInt32(&m, f, (int32_t)n) : r->SetInt32(&m, f, (int32_t)n);
        } else {
            many ? r->AddInt64(&m, f, (int64_t)n) : r->SetInt64(&m, f, (int64_t)n);
        }
        return true;
    }
    case gpb::FieldDescriptor::CPPTYPE_UINT32:
    case gpb::FieldDescriptor::CPPTYPE_UINT64: {
        const unsigned long long n = std::strtoull(given.c_str(), &end, 10);
        if (given.empty() || given[0] == '-' || *end != '\0') break;
        if (f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_UINT32) {
            many ? r->AddUInt32(&m, f, (uint32_t)n) : r->SetUInt32(&m, f, (uint32_t)n);
        } else {
            many ? r->AddUInt64(&m, f, (uint64_t)n) : r->SetUInt64(&m, f, (uint64_t)n);
        }
        return true;
    }
    case gpb::FieldDescriptor::CPPTYPE_DOUBLE:
    case gpb::FieldDescriptor::CPPTYPE_FLOAT: {
        const double n = std::strtod(given.c_str(), &end);
        if (given.empty() || *end != '\0') break;
        if (f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_DOUBLE) {
            many ? r->AddDouble(&m, f, n) : r->SetDouble(&m, f, n);
        } else {
            many ? r->AddFloat(&m, f, (float)n) : r->SetFloat(&m, f, (float)n);
        }
        return true;
    }
    default:
        break;
    }
    if (error.empty()) {
        error = "--" + kebab(f->name()) + " \"" + given + "\" is not " +
                (f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_ENUM ? "one of its names" : "a value it takes");
    }
    return false;
}

bool fromJson(const std::string& text, gpb::Message& m, std::string& error) {
    gpb::util::JsonParseOptions options;
    const auto status = gpb::util::JsonStringToMessage(text, &m, options);
    if (status.ok()) return true;
    error = std::string(status.message());
    return false;
}

// `value` into the field `path` names; null `value` for a bool flag given alone.
bool setField(gpb::Message& m, const std::string& path, const std::string* value, std::string& error) {
    const size_t dot = path.find('.');
    const gpb::FieldDescriptor* f = m.GetDescriptor()->FindFieldByName(snake(path.substr(0, dot)));
    if (f == nullptr) return resolve(m.GetDescriptor(), path, error) != nullptr;   // says why
    const gpb::Reflection* r = m.GetReflection();
    if (dot != std::string::npos) {
        if (resolve(m.GetDescriptor(), path, error) == nullptr) return false;
        return setField(*r->MutableMessage(&m, f), path.substr(dot + 1), value, error);
    }
    if (value == nullptr) {
        if (f->cpp_type() != gpb::FieldDescriptor::CPPTYPE_BOOL) {
            error = "--" + path + " needs a value";
            return false;
        }
        f->is_repeated() ? r->AddBool(&m, f, true) : r->SetBool(&m, f, true);
        return true;
    }
    if (f->is_map()) {
        const size_t equals = value->find('=');
        if (equals == std::string::npos) {
            error = "--" + path + " is key=value";
            return false;
        }
        gpb::Message* entry = r->AddMessage(&m, f);
        const gpb::Descriptor* e = f->message_type();
        return setScalar(*entry, e->FindFieldByNumber(1), value->substr(0, equals), error) &&
               setScalar(*entry, e->FindFieldByNumber(2), value->substr(equals + 1), error);
    }
    if (f->cpp_type() == gpb::FieldDescriptor::CPPTYPE_MESSAGE) {
        std::string text;
        if (!valueOf(*value, text, error)) return false;
        gpb::Message* into = f->is_repeated() ? r->AddMessage(&m, f) : r->MutableMessage(&m, f);
        if (fromJson(text, *into, error)) return true;
        error = "--" + path + ": " + error;
        return false;
    }
    return setScalar(m, f, *value, error);
}

std::string jsonText(const gpb::Message& m, bool pretty) {
    gpb::util::JsonPrintOptions options;
    options.add_whitespace = pretty;
    options.preserve_proto_field_names = true;     // the names the flags use
#if (defined(PROTOBUF_VERSION) && PROTOBUF_VERSION >= 5026000) || \
    (defined(GOOGLE_PROTOBUF_VERSION) && GOOGLE_PROTOBUF_VERSION >= 5026000)
    options.always_print_fields_with_no_presence = true;
#else
    options.always_print_primitive_fields = true;
#endif
    std::string out;
    gpb::util::MessageToJsonString(m, &out, options);
    if (!out.empty() && out.back() == '\n') out.pop_back();
    return out;
}

const char* codeName(grpc::StatusCode code) {
    switch (code) {
    case grpc::StatusCode::OK: return "OK";
    case grpc::StatusCode::CANCELLED: return "CANCELLED";
    case grpc::StatusCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
    case grpc::StatusCode::DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
    case grpc::StatusCode::NOT_FOUND: return "NOT_FOUND";
    case grpc::StatusCode::ALREADY_EXISTS: return "ALREADY_EXISTS";
    case grpc::StatusCode::PERMISSION_DENIED: return "PERMISSION_DENIED";
    case grpc::StatusCode::RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED";
    case grpc::StatusCode::FAILED_PRECONDITION: return "FAILED_PRECONDITION";
    case grpc::StatusCode::ABORTED: return "ABORTED";
    case grpc::StatusCode::OUT_OF_RANGE: return "OUT_OF_RANGE";
    case grpc::StatusCode::UNIMPLEMENTED: return "UNIMPLEMENTED";
    case grpc::StatusCode::INTERNAL: return "INTERNAL";
    case grpc::StatusCode::UNAVAILABLE: return "UNAVAILABLE";
    case grpc::StatusCode::DATA_LOSS: return "DATA_LOSS";
    case grpc::StatusCode::UNAUTHENTICATED: return "UNAUTHENTICATED";
    default: return "UNKNOWN";
    }
}

// The exit code, as the other commands have them: 1 refused, 3 could not reach it.
int failed(const std::string& who, const grpc::Status& status) {
    std::fprintf(stderr, "modbee-mqtt %s: %s: %s\n", who.c_str(), codeName(status.error_code()),
                 status.error_message().c_str());
    return status.error_code() == grpc::StatusCode::UNAVAILABLE ||
                   status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED
               ? 3
               : 1;
}

std::string environment(const char* name) {
    const char* value = std::getenv(name);
    return value ? value : "";
}

// The call's own options, apart from the request's fields.
struct Options {
    std::string api, token, tokenFile, ca, json;
    long timeoutSeconds = 30;
    long count = 0;             // a stream: stop after this many (0: never)
    bool help = false;
};

} // namespace

int cmdApi(int argc, char** argv) {
    const std::string noun = argv[1];
    int at = 2;
    std::string verb;
    if (findCommand(noun, "") == nullptr) {
        if (argc < 3 || argv[2][0] == '-') {
            nounHelp(noun);
            return argc >= 3 && (std::strcmp(argv[2], "--help") == 0 || std::strcmp(argv[2], "-h") == 0) ? 0 : 2;
        }
        verb = argv[2];
        at = 3;
    }
    const ApiCommand* command = findCommand(noun, verb);
    const std::string who = noun + (verb.empty() ? "" : " " + verb);
    if (command == nullptr) {
        std::fprintf(stderr, "modbee-mqtt %s: no such command\n\n", who.c_str());
        nounHelp(noun);
        return 2;
    }
    const std::string call = command->call;
    const std::string serviceName = call.substr(0, call.find('.'));
    const std::string methodName = call.substr(call.find('.') + 1);
    const gpb::FileDescriptor* file = v1::HelloRequest::descriptor()->file();
    const gpb::ServiceDescriptor* service = file->FindServiceByName(serviceName);
    const gpb::MethodDescriptor* method = service ? service->FindMethodByName(methodName) : nullptr;
    if (method == nullptr) {
        std::fprintf(stderr, "modbee-mqtt %s: the contract has no %s\n", who.c_str(), call.c_str());
        return 2;
    }
    // Consume's stream opens with a ConsumeOpen: that is what a person fills in.
    const bool consume = call == "Messages.Consume";
    const gpb::Descriptor* requestType = consume ? v1::ConsumeOpen::descriptor() : method->input_type();
    std::unique_ptr<gpb::Message> request(gpb::MessageFactory::generated_factory()->GetPrototype(requestType)->New());

    Options o;
    o.api = environment("MODBEE_API");
    o.token = environment("MODBEE_API_TOKEN");
    o.tokenFile = environment("MODBEE_API_TOKEN_FILE");
    std::vector<std::pair<std::string, std::string>> fields;   // after --json, in order
    std::vector<std::string> bare;                              // bool flags given alone
    for (int i = at; i < argc; i++) {
        std::string token = argv[i];
        if (token.compare(0, 2, "--") != 0) {
            std::fprintf(stderr, "modbee-mqtt %s: \"%s\" is not a --field (see --help)\n", who.c_str(), token.c_str());
            return 2;
        }
        token = token.substr(2);
        std::string value;
        bool hasValue = false;
        const size_t equals = token.find('=');
        if (equals != std::string::npos) {
            value = token.substr(equals + 1);
            token = token.substr(0, equals);
            hasValue = true;
        }
        if (token == "help") {
            o.help = true;
            continue;
        }
        const bool own = token == "api" || token == "token" || token == "token-file" || token == "api-ca" ||
                         token == "json" || token == "timeout" || token == "count";
        std::string error;
        const gpb::FieldDescriptor* field = own ? nullptr : resolve(requestType, token, error);
        if (!own && field == nullptr) {
            std::fprintf(stderr, "modbee-mqtt %s: --%s: %s (see --help)\n", who.c_str(), token.c_str(), error.c_str());
            return 2;
        }
        const bool flagAlone = field != nullptr && field->cpp_type() == gpb::FieldDescriptor::CPPTYPE_BOOL;
        if (!hasValue && !flagAlone) {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "modbee-mqtt %s: --%s needs a value\n", who.c_str(), token.c_str());
                return 2;
            }
            value = argv[++i];
            hasValue = true;
        }
        if (token == "api") o.api = value;
        else if (token == "token") o.token = value;
        else if (token == "token-file") o.tokenFile = value;
        else if (token == "api-ca") o.ca = value;
        else if (token == "json") o.json = value;
        else if (token == "timeout") o.timeoutSeconds = std::strtol(value.c_str(), nullptr, 10);
        else if (token == "count") o.count = std::strtol(value.c_str(), nullptr, 10);
        else if (hasValue) fields.emplace_back(token, value);
        else bare.push_back(token);
    }
    if (o.help) {
        std::printf("usage: modbee-mqtt %s [fields] [--api ADDRESS] [--token-file FILE]\n\n", who.c_str());
        std::printf("%s%s, %s -> %s%s%s\n\n", call.c_str(), consume ? " (opened with ConsumeOpen)" : "",
                    requestType->name().c_str(), method->output_type()->name().c_str(),
                    command->rest[0] ? ", REST " : "", command->rest);
        if (requestType->field_count() == 0) std::printf("  (no fields)\n");
        fieldHelp(requestType, "", 0);
        std::printf("\n  --json BODY | @FILE   the request as JSON, before the flags\n"
                    "  --api ADDRESS         unix:<path> or tcp://host:port (MODBEE_API)\n"
                    "  --token-file FILE     the API token (MODBEE_API_TOKEN_FILE; --token, MODBEE_API_TOKEN)\n"
                    "  --api-ca FILE         a TLS listener's CA\n"
                    "  --timeout SECONDS     how long to wait (default 30; a stream: for ever)\n"
                    "%s", method->server_streaming() ? "  --count N             stop after N\n" : "");
        return 0;
    }

    std::string error;
    if (!o.json.empty()) {
        std::string text;
        if (!valueOf(o.json, text, error) || !fromJson(text, *request, error)) {
            std::fprintf(stderr, "modbee-mqtt %s: --json: %s\n", who.c_str(), error.c_str());
            return 2;
        }
    }
    for (const auto& f : fields) {
        if (!setField(*request, f.first, &f.second, error)) {
            std::fprintf(stderr, "modbee-mqtt %s: %s\n", who.c_str(), error.c_str());
            return 2;
        }
    }
    for (const std::string& f : bare) {
        if (!setField(*request, f, nullptr, error)) {
            std::fprintf(stderr, "modbee-mqtt %s: %s\n", who.c_str(), error.c_str());
            return 2;
        }
    }

    if (o.api.empty()) {
        std::fprintf(stderr, "modbee-mqtt %s: where is the server's Application API? --api unix:<path> or "
                             "tcp://host:port (or MODBEE_API) - what `serve --api` was given\n", who.c_str());
        return 2;
    }
    if (!o.tokenFile.empty()) {
        std::string text;
        if (!valueOf("@" + o.tokenFile, text, error)) {
            std::fprintf(stderr, "modbee-mqtt %s: --token-file: %s\n", who.c_str(), error.c_str());
            return 2;
        }
        while (!text.empty() && (text.back() == '\n' || text.back() == '\r' || text.back() == ' ')) text.pop_back();
        o.token = text;
    }
    const std::string target = o.api.compare(0, 6, "tcp://") == 0 ? o.api.substr(6) : o.api;
    std::shared_ptr<grpc::ChannelCredentials> credentials = grpc::InsecureChannelCredentials();
    if (!o.ca.empty()) {
        grpc::SslCredentialsOptions ssl;
        if (!valueOf("@" + o.ca, ssl.pem_root_certs, error)) {
            std::fprintf(stderr, "modbee-mqtt %s: --api-ca: %s\n", who.c_str(), error.c_str());
            return 2;
        }
        credentials = grpc::SslCredentials(ssl);
    }
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(target, credentials);
    grpc::ClientContext context;
    if (!o.token.empty()) context.AddMetadata("authorization", "Bearer " + o.token);
    const bool stream = method->server_streaming() || method->client_streaming();
    if (!stream || o.timeoutSeconds != 30) {
        if (o.timeoutSeconds > 0) {
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(o.timeoutSeconds));
        }
    }

    if (call == "Connections.Watch") {
        auto stub = v1::Connections::NewStub(channel);
        std::unique_ptr<grpc::ClientReader<v1::ConnectionInfo>> reader(
            stub->Watch(&context, *static_cast<v1::WatchConnectionsRequest*>(request.get())));
        v1::ConnectionInfo info;
        long seen = 0;
        while (reader->Read(&info)) {
            std::printf("%s\n", jsonText(info, false).c_str());
            std::fflush(stdout);
            if (o.count > 0 && ++seen >= o.count) {
                context.TryCancel();
                return 0;
            }
        }
        const grpc::Status status = reader->Finish();
        return status.ok() ? 0 : failed(who, status);
    }
    if (consume) {
        auto stub = v1::Messages::NewStub(channel);
        std::unique_ptr<grpc::ClientReaderWriter<v1::ConsumeUp, v1::ConsumeDown>> s(stub->Consume(&context));
        v1::ConsumeUp up;
        up.mutable_open()->CopyFrom(*request);
        s->Write(up);
        v1::ConsumeDown down;
        long seen = 0;
        while (s->Read(&down)) {
            std::printf("%s\n", jsonText(down, false).c_str());
            std::fflush(stdout);
            // Printed is handled: acknowledged, so it is never sent again.
            const uint64_t id = down.has_delivery() ? down.delivery().id() : down.has_event() ? down.event().id() : 0;
            if (id != 0) {
                v1::ConsumeUp ack;
                ack.mutable_ack()->add_ids(id);
                s->Write(ack);
                if (o.count > 0 && ++seen >= o.count) {
                    s->WritesDone();
                    context.TryCancel();
                    return 0;
                }
            }
        }
        const grpc::Status status = s->Finish();
        return status.ok() ? 0 : failed(who, status);
    }

    std::unique_ptr<gpb::Message> reply(
        gpb::MessageFactory::generated_factory()->GetPrototype(method->output_type())->New());
    const std::string path = "/" + service->full_name() + "/" + method->name();
    const grpc::internal::RpcMethod rpc(path.c_str(), grpc::internal::RpcMethod::NORMAL_RPC);
    const grpc::Status status = grpc::internal::BlockingUnaryCall<gpb::Message, gpb::Message>(
        channel.get(), rpc, &context, *request, reply.get());
    if (!status.ok()) return failed(who, status);
    std::printf("%s\n", jsonText(*reply, true).c_str());
    return 0;
}

#endif

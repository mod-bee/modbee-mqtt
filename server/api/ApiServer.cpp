// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ApiServer.h"
#include "ServerConfig.h"   // apiAddressIsLoopback
#include "Authority.h"
#include "AppHub.h"
#include "AppConnections.h"
#include "AppAdmin.h"
#include "AuthGate.h"
#include "Json.h"
#include "ShardedBroker.h"
#if MQTT_TLS_SOCKETS
#include "transport/MqttCertificate.h"
#endif

#include "modbee/app/v1/app.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/stubs/common.h>   // GOOGLE_PROTOBUF_VERSION

#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <map>
#include <deque>
#include <fstream>
#include <mutex>
#include <sstream>

#if !defined(_WIN32)
#include <sys/stat.h>   // chmod: the local socket's permissions
#endif

namespace v1 = modbee::app::v1;

namespace {

const char* const kApiVersion = "1.0";

// --- Who may call ------------------------------------------------------------

bool allowed(grpc::CallbackServerContext* context, const std::string& token) {
    if (token.empty()) return true;
    const auto& metadata = context->client_metadata();
    const auto found = metadata.find("authorization");
    if (found == metadata.end()) return false;
    const std::string presented(found->second.data(), found->second.size());
    return presented == "Bearer " + token;
}

grpc::Status refusedToken() {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                        "this call needs \"authorization: Bearer <token>\" (api.token)");
}

grpc::Status noAuthority() {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "logins are decided by this server's login database "
                        "(authentication: database), not an authority");
}

// --- Between the wire and Authority.h -------------------------------------------

AuthorityRefusal refusalOf(v1::Refusal refusal) {
    switch (refusal) {
    case v1::REFUSAL_BAD_CREDENTIALS: return AuthorityRefusal::BadCredentials;
    case v1::REFUSAL_BANNED: return AuthorityRefusal::Banned;
    case v1::REFUSAL_QUOTA: return AuthorityRefusal::Quota;
    case v1::REFUSAL_UNAVAILABLE: return AuthorityRefusal::Unavailable;
    default: break;
    }
    return AuthorityRefusal::NotAuthorized;
}

std::shared_ptr<const AuthorityScope> scopeOf(const v1::Scope& wire) {
    std::shared_ptr<AuthorityScope> scope(new AuthorityScope());
    scope->publish.assign(wire.publish().begin(), wire.publish().end());
    scope->subscribe.assign(wire.subscribe().begin(), wire.subscribe().end());
    scope->subscribeWide.assign(wire.subscribe_wide().begin(), wire.subscribe_wide().end());
    scope->deny.assign(wire.deny().begin(), wire.deny().end());
    scope->hasMaxSessions = wire.has_max_sessions();
    scope->maxSessions = wire.max_sessions();
    scope->hasMaxRetained = wire.has_max_retained();
    scope->maxRetained = wire.max_retained();
    scope->tag = wire.tag().substr(0, 255);
    scope->settle();
    return scope;
}

AuthorityDecision decisionOf(const v1::Decision& wire) {
    AuthorityDecision decision;
    decision.id = wire.id();
    decision.allow = wire.allow();
    decision.refusal = refusalOf(wire.refusal());
    decision.ttlSeconds = wire.ttl_seconds();
    if (decision.allow) decision.scope = scopeOf(wire.scope());
    decision.hasLogin = wire.has_login();
    decision.login = wire.login();
    decision.replaceSession = wire.replace_session();
    return decision;
}

void questionTo(const AuthorityQuestion& question, v1::ConnectQuestion& wire) {
    wire.set_id(question.id);
    wire.set_client_id(question.clientId);
    if (question.hasUsername) wire.set_username(question.username);
    if (question.hasPassword) wire.set_password(question.password);
    wire.set_address(question.address);
    wire.set_listener(v1::LISTENER_UNSPECIFIED);
    wire.set_protocol_version(question.protocol);
    if (!question.certificateName.empty()) {
        wire.mutable_certificate()->set_common_name(question.certificateName);
        wire.mutable_certificate()->set_fingerprint(question.certificateFingerprint);
    }
}

// --- Messages: properties, deliveries, events -----------------------------------

void propertiesTo(const MqttOwnedProperties& p, v1::Properties& wire) {
    if (p.hasPayloadFormat) wire.set_payload_is_utf8(p.payloadFormat == 1);
    if (p.hasContentType) wire.set_content_type(p.contentType);
    if (p.hasResponseTopic) wire.set_response_topic(p.responseTopic);
    if (p.hasCorrelationData) {
        wire.set_correlation_data(std::string(p.correlationData.begin(), p.correlationData.end()));
    }
    if (p.hasExpiry) wire.set_message_expiry_seconds(p.expiryInterval);
    for (const auto& user : p.user) {
        v1::UserProperty* one = wire.add_user_properties();
        one->set_name(user.key);
        one->set_value(user.value);
    }
}

MqttOwnedProperties propertiesOf(const v1::Properties& wire) {
    MqttOwnedProperties p;
    if (wire.has_payload_is_utf8()) p.setPayloadFormat(wire.payload_is_utf8());
    if (!wire.content_type().empty()) p.setContentType(wire.content_type().c_str());
    if (!wire.response_topic().empty()) p.setResponseTopic(wire.response_topic().c_str());
    if (!wire.correlation_data().empty()) {
        p.setCorrelationData((const uint8_t*)wire.correlation_data().data(), wire.correlation_data().size());
    }
    if (wire.has_message_expiry_seconds()) p.setMessageExpiry(wire.message_expiry_seconds());
    for (const v1::UserProperty& user : wire.user_properties()) {
        p.addUserProperty(user.name().c_str(), user.value().c_str());
    }
    return p;
}

void deliveryTo(uint64_t id, const AppMessage& m, bool replay, bool redelivered, v1::ConsumeDown& down) {
    if (m.isEvent) {
        v1::ClientEvent* event = down.mutable_event();
        event->set_id(id);
        switch (m.event.kind) {
        case AppClientEvent::Connected: event->set_kind(v1::CLIENT_EVENT_CONNECTED); break;
        case AppClientEvent::Disconnected: event->set_kind(v1::CLIENT_EVENT_DISCONNECTED); break;
        case AppClientEvent::Subscribed: event->set_kind(v1::CLIENT_EVENT_SUBSCRIBED); break;
        case AppClientEvent::Unsubscribed: event->set_kind(v1::CLIENT_EVENT_UNSUBSCRIBED); break;
        case AppClientEvent::ServerStarted: event->set_kind(v1::CLIENT_EVENT_SERVER_STARTED); break;
        case AppClientEvent::PublishRefused: event->set_kind(v1::CLIENT_EVENT_PUBLISH_REFUSED); break;
        case AppClientEvent::SubscribeRefused: event->set_kind(v1::CLIENT_EVENT_SUBSCRIBE_REFUSED); break;
        case AppClientEvent::ConnectRefused: event->set_kind(v1::CLIENT_EVENT_CONNECT_REFUSED); break;
        case AppClientEvent::HandshakeFailed: event->set_kind(v1::CLIENT_EVENT_HANDSHAKE_FAILED); break;
        default: event->set_kind(v1::CLIENT_EVENT_UNSPECIFIED); break;
        }
        event->set_client_id(m.event.clientId);
        event->set_login(m.event.login);
        event->set_time_ms(m.event.timeMs);
        event->set_resumed(m.event.resumed);
        event->set_reason_code(m.event.reasonCode);
        event->set_filter(m.event.filter);
        event->set_qos(m.event.qos);
        event->set_address(m.event.address);
        event->set_detail(m.event.detail);
        return;
    }
    v1::Delivery* d = down.mutable_delivery();
    d->set_id(id);
    d->set_topic(m.topic);
    d->set_payload(m.payload);
    d->set_qos(m.qos);
    d->set_retain(m.retain);
    propertiesTo(m.properties, *d->mutable_properties());
    v1::Publisher* publisher = d->mutable_publisher();
    publisher->set_client_id(m.clientId);
    publisher->set_login(m.login);
    publisher->set_tag(m.tag);
    publisher->set_origin(m.origin);
    d->set_broker_time_ms(m.brokerTimeMs);
    d->set_replay(replay);
    d->set_redelivered(redelivered);
    d->set_connection(m.connection);
}

v1::PublishOutcome outcomeOf(AppPublishOutcome outcome) {
    switch (outcome) {
    case AppPublishOutcome::Accepted: return v1::PUBLISH_ACCEPTED;
    case AppPublishOutcome::Delivered: return v1::PUBLISH_DELIVERED;
    case AppPublishOutcome::NoSubscribers: return v1::PUBLISH_NO_SUBSCRIBERS;
    case AppPublishOutcome::Partial: return v1::PUBLISH_PARTIAL;
    case AppPublishOutcome::Refused: return v1::PUBLISH_REFUSED;
    case AppPublishOutcome::NotConnected: return v1::PUBLISH_NOT_CONNECTED;
    }
    return v1::PUBLISH_OUTCOME_UNSPECIFIED;
}

void resultTo(const AppPublishResult& result, uint64_t requestId, v1::PublishReply& reply) {
    reply.set_request_id(requestId);
    reply.set_outcome(outcomeOf(result.outcome));
    reply.set_reason_code(result.reasonCode);
    reply.set_matched(result.matched);
    reply.set_acknowledged(result.acknowledged);
    reply.set_pending(result.pending);
}

// CONFIRM_DELIVERED waits as long as the call may: its gRPC deadline, or ten
// seconds for one with none.
uint32_t deadlineMsOf(grpc::CallbackServerContext* context) {
    const auto deadline = context->deadline();
    const auto now = std::chrono::system_clock::now();
    if (deadline <= now || deadline - now > std::chrono::hours(24)) return 10000;
    return (uint32_t)std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
}

AppPublish publishOf(const v1::PublishRequest& request) {
    AppPublish message;
    message.confirmDelivered = request.confirm() == v1::CONFIRM_DELIVERED;
    message.topic = request.message().topic();
    message.payload = request.message().payload();
    message.qos = (uint8_t)request.message().qos();
    message.retain = request.message().retain();
    message.properties = propertiesOf(request.message().properties());
    message.connection = request.connection();
    message.origin = request.origin();
    return message;
}

// --- Connections -------------------------------------------------------------------

AppPublish messageOf(const v1::Message& wire) {
    AppPublish m;
    m.topic = wire.topic();
    m.payload = wire.payload();
    m.qos = (uint8_t)wire.qos();
    m.retain = wire.retain();
    m.properties = propertiesOf(wire.properties());
    return m;
}

// A ConsumeOpen's filters, each with its subscription options byte.
void subscriptionsOf(const v1::ConsumeOpen& wire, AppConsumeOptions& options) {
    options.filters.assign(wire.filters().begin(), wire.filters().end());
    options.subscribeOptions.assign(options.filters.size(), (uint8_t)MQ_QOS_1);
    for (const v1::Subscription& one : wire.subscriptions()) {
        // Out of range is kept out of range, for AppHub::open to refuse.
        const uint32_t qos = one.has_qos() ? one.qos() : 1u;
        const uint32_t handling = (uint32_t)one.retain_handling();
        uint8_t byte = (uint8_t)(qos > 3 ? 3 : qos);
        if (one.no_local()) byte |= MQ_SUBOPT_NO_LOCAL;
        if (one.retain_as_published()) byte |= MQ_SUBOPT_RETAIN_AS_PUB;
        byte |= (uint8_t)((handling > 3 ? 3 : handling) << MQ_SUBOPT_RETAIN_HANDLING_SHIFT);
        auto at = std::find(options.filters.begin(), options.filters.end(), one.filter());
        if (at == options.filters.end()) {
            options.filters.push_back(one.filter());
            options.subscribeOptions.push_back(byte);
        } else {
            options.subscribeOptions[(size_t)(at - options.filters.begin())] = byte;
        }
    }
}

void groupTo(const AppGroupInfo& info, v1::GroupInfo& group) {
    group.set_group(info.group);
    group.set_connection(info.connection);
    for (size_t i = 0; i < info.filters.size(); i++) {
        group.add_filters(info.filters[i]);
        const uint8_t byte = i < info.subscribeOptions.size() ? info.subscribeOptions[i] : (uint8_t)MQ_QOS_1;
        v1::Subscription* one = group.add_subscriptions();
        one->set_filter(info.filters[i]);
        one->set_qos(byte & MQ_SUBOPT_QOS_MASK);
        one->set_no_local((byte & MQ_SUBOPT_NO_LOCAL) != 0);
        one->set_retain_as_published((byte & MQ_SUBOPT_RETAIN_AS_PUB) != 0);
        one->set_retain_handling(
            (v1::RetainHandling)((byte & MQ_SUBOPT_RETAIN_HANDLING_MASK) >> MQ_SUBOPT_RETAIN_HANDLING_SHIFT));
    }
    group.set_members(info.members);
    group.set_queued(info.queued);
    group.set_in_flight(info.inFlight);
    group.set_refused(info.refused);
}

AppConnectionSpec specOf(const v1::ConnectionSpec& wire) {
    AppConnectionSpec s;
    s.id = wire.id();
    for (const v1::Endpoint& e : wire.servers()) s.servers.push_back(AppEndpoint{e.host(), (uint16_t)e.port()});
    s.clientId = wire.client_id();
    s.hasUsername = wire.has_username();
    s.username = wire.username();
    s.hasPassword = wire.has_password();
    s.password = wire.password();
    s.tls = wire.tls().enabled();
    s.caPem = wire.tls().ca_pem();
    s.certificatePem = wire.tls().certificate_pem();
    s.keyPem = wire.tls().key_pem();
    s.protocol = wire.protocol_version() == 4 ? 4 : 5;
    s.keepaliveSeconds = wire.keepalive_seconds() ? wire.keepalive_seconds() : 30;
    s.cleanStart = wire.clean_start();
    s.sessionExpirySeconds = wire.session_expiry_seconds();
    if (wire.has_will() && !wire.will().topic().empty()) {
        s.hasWill = true;
        s.will = messageOf(wire.will());
    }
    s.reconnectWhenAsked = wire.reconnect() == v1::RECONNECT_WHEN_ASKED;
    s.primaryCheckSeconds = wire.primary_check_seconds() ? wire.primary_check_seconds() : 60;
    return s;
}

void infoTo(const AppConnectionInfo& info, v1::ConnectionInfo& wire) {
    v1::ConnectionSpec* s = wire.mutable_spec();
    s->set_id(info.spec.id);
    for (const AppEndpoint& e : info.spec.servers) {
        v1::Endpoint* endpoint = s->add_servers();
        endpoint->set_host(e.host);
        endpoint->set_port(e.port);
    }
    s->set_client_id(info.spec.clientId);
    if (info.spec.hasUsername) s->set_username(info.spec.username);
    s->mutable_tls()->set_enabled(info.spec.tls);
    s->mutable_tls()->set_ca_pem(info.spec.caPem);
    s->mutable_tls()->set_certificate_pem(info.spec.certificatePem);
    s->set_protocol_version(info.spec.protocol);
    s->set_keepalive_seconds(info.spec.keepaliveSeconds);
    s->set_clean_start(info.spec.cleanStart);
    s->set_session_expiry_seconds(info.spec.sessionExpirySeconds);
    if (info.spec.hasWill) {
        v1::Message* will = s->mutable_will();
        will->set_topic(info.spec.will.topic);
        will->set_payload(info.spec.will.payload);
        will->set_qos(info.spec.will.qos);
        will->set_retain(info.spec.will.retain);
    }
    s->set_reconnect(info.spec.reconnectWhenAsked ? v1::RECONNECT_WHEN_ASKED : v1::RECONNECT_AUTOMATIC);
    s->set_primary_check_seconds(info.spec.primaryCheckSeconds);
    switch (info.state) {
    case AppConnectionState::Connecting: wire.set_state(v1::CONNECTION_CONNECTING); break;
    case AppConnectionState::Connected: wire.set_state(v1::CONNECTION_CONNECTED); break;
    case AppConnectionState::Waiting: wire.set_state(v1::CONNECTION_WAITING); break;
    case AppConnectionState::BackingOff: wire.set_state(v1::CONNECTION_BACKING_OFF); break;
    case AppConnectionState::Refused: wire.set_state(v1::CONNECTION_REFUSED); break;
    }
    wire.set_server_index(info.serverIndex);
    wire.set_session_present(info.sessionPresent);
    wire.set_last_error(info.lastError);
    wire.set_since_ms(info.sinceMs);
    wire.set_messages_in(info.messagesIn);
    wire.set_messages_out(info.messagesOut);
}


// =============================================================================
// ADMIN: one function per call, on proto types - the gRPC service and the REST
// surface both call these, so the two cannot say different things
// =============================================================================

typedef std::function<void(const grpc::Status&)> Done;

std::string dataJson(const google::protobuf::Map<std::string, std::string>& data) {
    json::Object o;
    for (const auto& pair : data) o.add(pair.first, pair.second);
    return o.str();
}

void dataFrom(const json::Value& value, google::protobuf::Map<std::string, std::string>& into) {
    for (const auto& field : value.fields) into[field.first] = field.second.text;
}

void loginFrom(const json::Value& u, v1::Login& login) {
    login.set_name(u.str("username"));
    login.set_enabled(u.flag("enabled"));
    login.set_pending(u.flag("pending"));
    login.set_admin(u.flag("admin"));
    std::vector<std::string> publish, subscribe;
    u.strings("publish", publish);
    u.strings("subscribe", subscribe);
    for (const std::string& f : publish) login.add_publish(f);
    for (const std::string& f : subscribe) login.add_subscribe(f);
    login.set_fingerprint(u.str("fingerprint"));
    login.set_site(u.str("site"));
    const std::string by = u.str("provisioned_by");
    login.set_made_by(by.empty() ? "hand" : by);
    dataFrom(u["data"], *login.mutable_data());
}

void keyFrom(const json::Value& k, v1::SiteKey& key) {
    key.set_name(k.str("name"));
    key.set_site(k.str("site"));
    key.set_manual_approval(k.str("approval") == "manual");
    const long long left = k.integer("uses_left", -1);
    key.set_uses(left < 0 ? 0 : (uint32_t)left);
    key.set_expires_ms(k.integer("expires") * 1000);
    key.set_device(k.str("device"));
    std::vector<std::string> publish, subscribe;
    k.strings("publish", publish);
    k.strings("subscribe", subscribe);
    for (const std::string& f : publish) key.add_publish(f);
    for (const std::string& f : subscribe) key.add_subscribe(f);
    dataFrom(k["data"], *key.mutable_data());
}

class AdminCore {
public:
    AdminCore(const ApiServer::Services& services)
        : _s(services),
          _startedMs(std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch()).count()) {}

    // --- Logins and site keys: the login database's own requests ---

    // A request to the login database; its answer, or false with `status`.
    bool ask(const std::string& request, json::Value& reply, grpc::Status& status) {
        if (_s.logins == nullptr) {
            status = grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                  "logins are not kept by this server (authentication: database)");
            return false;
        }
        if (!json::parse(_s.logins->admin(request), reply) || !reply.flag("ok")) {
            const std::string error = reply.str("error", "the login database did not answer");
            const grpc::StatusCode code =
                error.find("no user") != std::string::npos || error.find("no key") != std::string::npos
                    ? grpc::StatusCode::NOT_FOUND
                : error.find("exists") != std::string::npos ? grpc::StatusCode::ALREADY_EXISTS
                                                            : grpc::StatusCode::INVALID_ARGUMENT;
            status = grpc::Status(code, error);
            return false;
        }
        return true;
    }

    void listLogins(const v1::ListRequest& request, v1::ListLoginsReply* reply, Done done) {
        json::Object q;
        q.add("command", "user.list").add("prefix", request.prefix()).add("after", request.page_token());
        q.add("limit", (long long)(request.page_size() ? std::min(request.page_size(), 1000u) : 100u));
        json::Value answer;
        grpc::Status status;
        if (!ask(q.str(), answer, status)) return done(status);
        for (const json::Value& u : answer["users"].items) loginFrom(u, *reply->add_logins());
        reply->set_total((uint64_t)answer.integer("total"));
        reply->set_next_page_token(answer.str("next"));
        done(grpc::Status::OK);
    }

    void getLogin(const std::string& name, v1::Login* reply, Done done) {
        json::Object q;
        q.add("command", "user.get").add("username", name);
        json::Value answer;
        grpc::Status status;
        if (!ask(q.str(), answer, status)) return done(status);
        loginFrom(answer["user"], *reply);
        done(grpc::Status::OK);
    }

    void putLogin(const v1::PutLoginRequest& request, v1::PutLoginReply* reply, Done done) {
        const v1::Login& login = request.login();
        if (login.name().empty()) return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "the login needs a name"));
        json::Value answer;
        grpc::Status status;
        json::Object exists;
        exists.add("command", "user.get").add("username", login.name());
        const bool known = ask(exists.str(), answer, status);
        if (!known && status.error_code() != grpc::StatusCode::NOT_FOUND) return done(status);
        std::vector<std::string> publish(login.publish().begin(), login.publish().end());
        std::vector<std::string> subscribe(login.subscribe().begin(), login.subscribe().end());
        json::Object q;
        q.add("command", known ? "user.set" : "user.add").add("username", login.name());
        q.raw("publish", json::array(publish)).raw("subscribe", json::array(subscribe));
        q.add("admin", login.admin());
        if (known) q.add("enabled", login.enabled());
        if (!login.fingerprint().empty() || known) q.add("fingerprint", login.fingerprint());
        q.raw("data", dataJson(login.data()));
        if (request.has_password()) q.add("password", request.password());
        if (!ask(q.str(), answer, status)) return done(status);
        reply->set_password(answer.str("password"));
        if (request.approve()) {
            json::Object approve;
            approve.add("command", "user.approve").add("username", login.name());
            if (!ask(approve.str(), answer, status)) return done(status);
        }
        getLogin(login.name(), reply->mutable_login(), done);
    }

    void deleteLogin(const std::string& name, v1::DeleteReply* reply, Done done) {
        json::Object q;
        q.add("command", "user.remove").add("username", name);
        json::Value answer;
        grpc::Status status;
        if (!ask(q.str(), answer, status)) return done(status);
        reply->set_removed(1);
        done(grpc::Status::OK);
    }

    void resetLogin(const std::string& name, v1::PutLoginReply* reply, Done done) {
        json::Object q;
        q.add("command", "user.reset").add("username", name);
        json::Value answer;
        grpc::Status status;
        if (!ask(q.str(), answer, status)) return done(status);
        reply->set_password(answer.str("password"));
        getLogin(name, reply->mutable_login(), done);
    }

    void listKeys(v1::ListKeysReply* reply, Done done) {
        json::Value answer;
        grpc::Status status;
        if (!ask("{\"command\":\"key.list\"}", answer, status)) return done(status);
        for (const json::Value& k : answer["keys"].items) keyFrom(k, *reply->add_keys());
        done(grpc::Status::OK);
    }

    void putKey(const v1::SiteKey& key, v1::PutKeyReply* reply, Done done) {
        if (key.name().empty()) return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "the key needs a name"));
        json::Value answer;
        grpc::Status status;
        if (!ask("{\"command\":\"key.list\"}", answer, status)) return done(status);
        bool known = false;
        for (const json::Value& k : answer["keys"].items) known = known || k.str("name") == key.name();
        json::Object q;
        if (known) {
            // A key's rules are set when it is made; what it hands its devices may change.
            q.add("command", "key.set").add("name", key.name()).raw("data", dataJson(key.data()));
        } else {
            std::vector<std::string> publish(key.publish().begin(), key.publish().end());
            std::vector<std::string> subscribe(key.subscribe().begin(), key.subscribe().end());
            q.add("command", "key.create").add("name", key.name()).add("site", key.site());
            q.add("approval", key.manual_approval() ? "manual" : "auto");
            if (key.uses() != 0) q.add("uses", (long long)key.uses());
            if (key.expires_ms() != 0) q.add("expires", (long long)(key.expires_ms() / 1000));
            if (!key.device().empty()) q.add("device", key.device());
            q.raw("publish", json::array(publish)).raw("subscribe", json::array(subscribe));
            q.raw("data", dataJson(key.data()));
        }
        if (!ask(q.str(), answer, status)) return done(status);
        reply->set_secret(answer.str("key"));
        v1::ListKeysReply keys;
        listKeys(&keys, [&](const grpc::Status&) {});
        for (const v1::SiteKey& k : keys.keys()) {
            if (k.name() == key.name()) *reply->mutable_key() = k;
        }
        done(grpc::Status::OK);
    }

    void deleteKey(const std::string& name, v1::DeleteReply* reply, Done done) {
        json::Object q;
        q.add("command", "key.remove").add("name", name);
        json::Value answer;
        grpc::Status status;
        if (!ask(q.str(), answer, status)) return done(status);
        reply->set_removed(1);
        done(grpc::Status::OK);
    }

    // --- Sessions, on every shard ---

    void listSessions(const v1::ListSessionsRequest& request, v1::ListSessionsReply* reply, Done done) {
        if (_s.admin == nullptr) return done(unavailable());
        struct Gathered {
            std::mutex lock;
            std::vector<MqttBrokerSessionSummary> all;
        };
        std::shared_ptr<Gathered> gathered(new Gathered());
        const v1::ListSessionsRequest filter = request;
        _s.admin->everyShard(
            [gathered, filter](uint32_t, MqttBroker& broker) {
                std::vector<MqttBrokerSessionSummary> mine;
                broker.describeSessions([](void* context, const MqttBrokerSessionSummary& s) {
                    static_cast<std::vector<MqttBrokerSessionSummary>*>(context)->push_back(s);
                }, &mine, filter.client_id_prefix(), filter.login());
                std::lock_guard<std::mutex> guard(gathered->lock);
                for (auto& s : mine) gathered->all.push_back(std::move(s));
            },
            [gathered, filter, reply, done]() {
                std::vector<MqttBrokerSessionSummary>& all = gathered->all;
                std::sort(all.begin(), all.end(), [](const MqttBrokerSessionSummary& a, const MqttBrokerSessionSummary& b) {
                    return a.clientId < b.clientId;
                });
                const size_t limit = filter.page_size() ? std::min<size_t>(filter.page_size(), 1000) : 100;
                const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch()).count();
                for (const MqttBrokerSessionSummary& s : all) {
                    if (!filter.page_token().empty() && s.clientId <= filter.page_token()) continue;
                    if (s.clientId.compare(0, filter.client_id_prefix().size(), filter.client_id_prefix()) != 0) continue;
                    if (!filter.login().empty() && s.login != filter.login()) continue;
                    if (filter.connected_only() && !s.connected) continue;
                    if ((size_t)reply->sessions_size() == limit) {
                        reply->set_next_page_token(reply->sessions(reply->sessions_size() - 1).client_id());
                        break;
                    }
                    v1::Session* out = reply->add_sessions();
                    out->set_client_id(s.clientId);
                    out->set_login(s.login);
                    out->set_connected(s.connected);
                    out->set_address(s.address);
                    out->set_protocol_version(s.protocol);
                    if (s.connected) out->set_connected_since_ms(now - s.connectedForMs);
                    out->set_expiry_seconds(s.expiryInterval);
                    for (const std::string& f : s.subscriptions) out->add_subscriptions(f);
                    out->set_queued(s.queued);
                    out->set_in_flight(s.inFlight);
                    out->set_messages_in(s.messagesIn);
                    out->set_messages_out(s.messagesOut);
                    out->set_bytes_in(s.bytesIn);
                    out->set_bytes_out(s.bytesOut);
                    out->set_last_activity_ms((int64_t)s.lastActivityMs);
                    out->set_connects(s.connects);
                }
                done(grpc::Status::OK);
            });
    }

    void kick(const v1::KickRequest& request, v1::KickReply* reply, Done done) {
        if (_s.admin == nullptr) return done(unavailable());
        if (request.client_id().empty() && request.login().empty()) {
            return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "kick a client_id or a login"));
        }
        std::shared_ptr<std::atomic<uint32_t>> closed(new std::atomic<uint32_t>(0));
        const v1::KickRequest asked = request;
        _s.admin->everyShard(
            [closed, asked](uint32_t, MqttBroker& broker) {
                if (!asked.login().empty()) {
                    MqttSpan login;
                    login.data = (const uint8_t*)asked.login().data();
                    login.length = (uint32_t)asked.login().size();
                    *closed += (uint32_t)broker.disconnectUser(login, MQ_REASON_ADMINISTRATIVE_ACTION);
                } else {
                    MqttSpan id;
                    id.data = (const uint8_t*)asked.client_id().data();
                    id.length = (uint32_t)asked.client_id().size();
                    *closed += (uint32_t)broker.disconnectClient(id, MQ_REASON_ADMINISTRATIVE_ACTION,
                                                                 asked.discard_session());
                }
            },
            [closed, reply, done]() {
                reply->set_closed(closed->load());
                done(grpc::Status::OK);
            });
    }

    // --- Retained messages: every shard holds them all, so one is asked ---

    void listRetained(const v1::ListRetainedRequest& request, v1::ListRetainedReply* reply, Done done) {
        if (_s.admin == nullptr) return done(unavailable());
        const v1::ListRetainedRequest asked = request;
        _s.admin->oneShard(0, [asked, reply, done](MqttBroker& broker) {
            struct Visit {
                std::vector<const MqttRetainedMessage*> found;
            } visit;
            const std::string filter = asked.filter().empty() ? "#" : asked.filter();
            MqttSpan span;
            span.data = (const uint8_t*)filter.data();
            span.length = (uint32_t)filter.size();
            broker.retained().visitMatching(span, mq_millis(), [](void* context, const MqttRetainedMessage& m, uint32_t, bool) {
                static_cast<Visit*>(context)->found.push_back(&m);
            }, &visit);
            std::sort(visit.found.begin(), visit.found.end(),
                      [](const MqttRetainedMessage* a, const MqttRetainedMessage* b) { return a->topic < b->topic; });
            const size_t limit = asked.page_size() ? std::min<size_t>(asked.page_size(), 1000) : 100;
            const mq_time_t now = mq_millis();
            const int64_t utc = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count();
            for (const MqttRetainedMessage* m : visit.found) {
                if (!asked.page_token().empty() && m->topic <= asked.page_token()) continue;
                if ((size_t)reply->messages_size() == limit) {
                    reply->set_next_page_token(reply->messages(reply->messages_size() - 1).message().topic());
                    break;
                }
                v1::RetainedMessage* out = reply->add_messages();
                out->mutable_message()->set_topic(m->topic);
                if (asked.payloads()) out->mutable_message()->set_payload(std::string(m->payload.begin(), m->payload.end()));
                out->mutable_message()->set_qos(m->qos);
                out->mutable_message()->set_retain(true);
                propertiesTo(m->properties, *out->mutable_message()->mutable_properties());
                out->set_size((uint32_t)m->payload.size());
                out->set_stored_ms(utc - (int64_t)mq_elapsed(m->storedAt, now));
            }
            done(grpc::Status::OK);
        });
    }

    // Cleared the way any MQTT client clears one - an empty retained message -
    // so every shard, and wherever retained messages are kept, forgets it.
    void deleteRetained(const v1::DeleteRetainedRequest& request, v1::DeleteReply* reply, Done done) {
        if (_s.admin == nullptr || _s.messages == nullptr) return done(unavailable());
        if (request.filter().empty()) return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "a topic or filter is required"));
        AppHub* hub = _s.messages;
        const std::string filter = request.filter();
        _s.admin->oneShard(0, [hub, filter, reply, done](MqttBroker& broker) {
            std::vector<std::string> topics;
            MqttSpan span;
            span.data = (const uint8_t*)filter.data();
            span.length = (uint32_t)filter.size();
            broker.retained().visitMatching(span, mq_millis(), [](void* context, const MqttRetainedMessage& m, uint32_t, bool) {
                static_cast<std::vector<std::string>*>(context)->push_back(m.topic);
            }, &topics);
            for (const std::string& topic : topics) {
                AppPublish clear;
                clear.topic = topic;
                clear.retain = true;
                clear.origin = "admin";
                hub->publish(clear, [](const AppPublishResult&) {});
            }
            reply->set_removed(topics.size());
            done(grpc::Status::OK);
        });
    }

    // --- Listeners, TLS, status ---

    void listeners(v1::Listeners* reply, Done done) {
        if (_s.server == nullptr) return done(unavailable());
        reply->set_max_connections(_s.server->maxConnections());
        reply->set_max_connections_ceiling(_s.server->maxConnectionsCeiling());
        reply->set_max_packet(_s.server->maxPacket());
        if (_s.server->port() != 0) {
            v1::ListenerInfo* plain = reply->add_listeners();
            plain->set_kind(v1::LISTENER_PLAIN);
            plain->set_address(_s.server->address());
            plain->set_port(_s.server->port());
        }
        const ShardedTlsConfig tls = _s.server->servedTls();
        if (tls.enabled) {
            v1::ListenerInfo* secure = reply->add_listeners();
            secure->set_kind(v1::LISTENER_TLS);
            secure->set_address(tls.address);
            secure->set_port(_s.server->tlsPort());
#if MQTT_TLS_SOCKETS
            std::string pem = tls.certificatePem;
            if (pem.empty() && !tls.certificateFile.empty()) {
                std::ifstream in(tls.certificateFile, std::ios::binary);
                std::ostringstream text;
                text << in.rdbuf();
                pem = text.str();
            }
            MqttCertificateFacts facts;
            std::string error;
            if (!pem.empty() && mqttReadCertificate(pem.c_str(), facts, error)) {
                secure->set_certificate_subject(facts.subject);
                secure->set_certificate_expires_ms(facts.notAfterSeconds * 1000);
            }
#endif
        }
        done(grpc::Status::OK);
    }

    void putTls(const v1::ServerTls& request, v1::Listeners* reply, Done done) {
        if (_s.server == nullptr) return done(unavailable());
        ShardedTlsConfig tls = _s.server->servedTls();
        tls.certificateFile.clear();
        tls.keyFile.clear();
        tls.certificatePem = request.certificate_pem();
        tls.keyPem = request.key_pem();
        tls.clientCaPem = request.client_ca_pem();
        tls.clientCertificateOptional = request.client_certificate_optional();
        std::string error;
        if (!_s.server->replaceTls(tls, error)) return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error));
        listeners(reply, done);
    }

    void putListeners(const v1::ListenersChange& request, v1::Listeners* reply, Done done) {
        if (_s.server == nullptr) return done(unavailable());
        ShardedBroker::ListenerChange change;
        if (request.has_port()) {
            if (request.port() > 65535) return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "port: 0 to 65535"));
            change.setPlain = true;
            change.plain = request.port() != 0;
            change.port = (uint16_t)request.port();
        }
        if (request.has_tls_port()) {
            if (request.tls_port() > 65535) {
                return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "tls_port: 0 to 65535"));
            }
            change.setTls = true;
            change.tls.enabled = request.tls_port() != 0;
            change.tls.port = (uint16_t)request.tls_port();
            if (request.has_tls()) {
                change.tls.certificatePem = request.tls().certificate_pem();
                change.tls.keyPem = request.tls().key_pem();
                change.tls.clientCaPem = request.tls().client_ca_pem();
                change.tls.clientCertificateOptional = request.tls().client_certificate_optional();
            }
        } else if (request.has_tls()) {
            return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "tls comes with tls_port (or use PutTls)"));
        }
        if (request.has_max_connections()) {
            if (request.max_connections() == 0) {
                return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "max_connections: at least 1"));
            }
            change.maxConnections = (size_t)request.max_connections();
        }
        if (request.has_max_packet()) {
            if (request.max_packet() < 64 || request.max_packet() > 268435455u) {
                return done(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "max_packet: 64 to 268435455 bytes"));
            }
            change.maxPacket = request.max_packet();
        }
        std::string error;
        if (!_s.server->changeListeners(change, error)) {
            const bool taken = error.find("in use") != std::string::npos ||
                               error.find("without a restart") != std::string::npos;
            return done(grpc::Status(taken ? grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::INVALID_ARGUMENT,
                                     error));
        }
        listeners(reply, done);
    }

    void stats(v1::StatsReply* reply, Done done) {
        if (_s.server == nullptr) return done(unavailable());
        const ShardedBrokerStatus status = _s.server->status();
        reply->set_connections(status.total.connections);
        reply->set_sessions(status.total.sessions);
        reply->set_retained(status.total.retained / (status.shards ? status.shards : 1));
        reply->set_messages_in(status.total.published);
        reply->set_messages_out(status.total.delivered);
        reply->set_dropped(status.total.dropped);
        reply->set_refused_connects(status.total.refused);
        reply->set_shards((uint32_t)status.shards);
        reply->set_started_ms(_startedMs);
        if (_s.authority) {
            const AuthorityStatus a = _s.authority->status();
            reply->set_authority_asked(a.asked);
            reply->set_authority_cached(a.cached);
            reply->set_authority_stale(a.stale);
        }
        done(grpc::Status::OK);
    }

private:
    static grpc::Status unavailable() {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "the server is not running");
    }
    const ApiServer::Services& _s;
    const int64_t _startedMs;     // the API starts with the server, once its shards run
};

// =============================================================================
// Authority.Serve: one application instance answering
// =============================================================================
//
// Writes go one at a time (gRPC's rule): a question asked while one is on its
// way waits in `queue`. A deque, so the one being written does not move.

class AuthorityReactor : public grpc::ServerBidiReactor<v1::AuthorityUp, v1::AuthorityDown>,
                         public AuthorityStream {
public:
    AuthorityReactor(Authority& authority, bool allowedIn) : _authority(authority) {
        if (!allowedIn) {
            _finished = true;
            Finish(refusedToken());
            return;
        }
        StartRead(&_up);
    }

    void ask(const AuthorityQuestion& question) override {
        v1::AuthorityDown* next = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            if (_ending) return;
            _queue.emplace_back();
            questionTo(question, *_queue.back().mutable_connect());
            if (_writing) return;
            _writing = true;
            next = &_queue.front();
        }
        StartWrite(next);
    }

    void OnReadDone(bool ok) override {
        if (!ok) {
            // The instance has gone, or closed its side: what it was asked is
            // asked of another, and the stream ends once its last write has.
            _authority.detach(this);
            end();
            return;
        }
        if (_up.has_open()) {
            _authority.attach(this);
        } else if (_up.has_decision()) {
            _authority.decided(this, decisionOf(_up.decision()));
        }
        StartRead(&_up);
    }

    void OnWriteDone(bool ok) override {
        // A write that failed means the instance has gone: what it was asked
        // goes to another, and what was still queued for it with it.
        if (!ok) _authority.detach(this);
        v1::AuthorityDown* next = nullptr;
        bool finish = false;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _queue.pop_front();
            if (!ok) _ending = true;
            if (!_ending && !_queue.empty()) {
                next = &_queue.front();
            } else {
                _writing = false;
                if (_ending && !_finished) {
                    _finished = true;
                    finish = true;
                }
            }
        }
        if (next) StartWrite(next);
        if (finish) Finish(grpc::Status::OK);
    }

    void OnDone() override {
        _authority.detach(this);
        delete this;
    }

private:
    void end() {
        bool finish = false;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _ending = true;
            if (!_writing && !_finished) {
                _finished = true;
                finish = true;
            }
        }
        if (finish) Finish(grpc::Status::OK);
    }

    Authority& _authority;
    v1::AuthorityUp _up;
    std::mutex _lock;
    std::deque<v1::AuthorityDown> _queue;
    bool _writing = false;
    bool _ending = false;
    bool _finished = false;
};

// =============================================================================
// Messages.Consume: one member of a consumer group
// =============================================================================
//
// The first message up is the open. Deliveries come down as the hub deals
// them, in-flight bounded by max_in_flight; acks and retries go up. While the
// open is answered, deliveries wait behind the `opened` reply: it is first.

class ConsumeReactor : public grpc::ServerBidiReactor<v1::ConsumeUp, v1::ConsumeDown>, public AppConsumer {
public:
    ConsumeReactor(AppHub& hub, bool allowedIn) : _hub(hub) {
        if (!allowedIn) {
            _finished = true;
            Finish(refusedToken());
            return;
        }
        StartRead(&_up);
    }

    void deliver(uint64_t id, const AppMessage& message, bool replay, bool redelivered) override {
        v1::ConsumeDown* next = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            if (_ending) return;
            _queue.emplace_back();
            deliveryTo(id, message, replay, redelivered, _queue.back());
            if (_writing || _holding) return;
            _writing = true;
            next = &_queue.front();
        }
        StartWrite(next);
    }

    void OnReadDone(bool ok) override {
        if (!ok) {
            end(grpc::Status::OK);
            return;
        }
        if (_member == nullptr) {
            if (!_up.has_open()) {
                end(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "the first message on a Consume stream is open"));
                return;
            }
            if (!open(_up.open())) return;
        } else if (_up.has_ack()) {
            _hub.ack(_member, std::vector<uint64_t>(_up.ack().ids().begin(), _up.ack().ids().end()));
        } else if (_up.has_retry()) {
            _hub.retry(_member, std::vector<uint64_t>(_up.retry().ids().begin(), _up.retry().ids().end()),
                       _up.retry().after_ms());
        }
        StartRead(&_up);
    }

    void OnWriteDone(bool ok) override {
        v1::ConsumeDown* next = nullptr;
        bool finish = false;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _queue.pop_front();
            if (!ok) _ending = true;
            if (!_ending && !_queue.empty()) {
                next = &_queue.front();
            } else {
                _writing = false;
                if (_ending && !_finished) {
                    _finished = true;
                    finish = true;
                }
            }
        }
        if (!ok) leave();
        if (next) StartWrite(next);
        if (finish) Finish(_status);
    }

    void OnDone() override {
        leave();
        delete this;
    }

private:
    bool open(const v1::ConsumeOpen& wire) {
        AppConsumeOptions options;
        options.group = wire.group();
        subscriptionsOf(wire, options);
        options.connection = wire.connection();
        options.maxInFlight = wire.max_in_flight() ? wire.max_in_flight() : 100;
        options.pick = wire.pick() == v1::PICK_ROUND_ROBIN ? AppPick::RoundRobin : AppPick::Publisher;
        options.events = wire.events();
        options.expirySeconds = wire.expiry_seconds() ? wire.expiry_seconds() : 86400;
        options.onFull = wire.on_full() == v1::ON_FULL_DROP ? AppOnFull::Drop : AppOnFull::Refuse;
        options.excludeOrigins.assign(wire.exclude_origins().begin(), wire.exclude_origins().end());
        v1::ConsumeDown* opened = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _holding = true;
            _queue.emplace_back();
            opened = &_queue.back();
        }
        std::string error;
        uint32_t members = 0;
        uint64_t queued = 0;
        AppHub::Member* member = _hub.open(options, this, error, &members, &queued);
        v1::ConsumeDown* next = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _member = member;
            _holding = false;
            if (member != nullptr) {
                opened->mutable_opened()->set_members(members);
                opened->mutable_opened()->set_queued(queued);
                _writing = true;
                next = &_queue.front();
            }
        }
        if (member == nullptr) {
            end(grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, error));
            return false;
        }
        StartWrite(next);
        return true;
    }

    // The member leaves the group - what it held goes to the others - and the
    // stream ends once its last write has.
    void end(const grpc::Status& status) {
        leave();
        bool finish = false;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _ending = true;
            _status = status;
            if (!_writing && !_finished) {
                _finished = true;
                finish = true;
            }
        }
        if (finish) Finish(status);
    }

    void leave() {
        AppHub::Member* member = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            member = _member;
            _member = nullptr;
            _ending = true;
        }
        if (member) _hub.close(member);
    }

    AppHub& _hub;
    AppHub::Member* _member = nullptr;
    v1::ConsumeUp _up;
    std::mutex _lock;
    std::deque<v1::ConsumeDown> _queue;
    bool _writing = false;
    bool _holding = false;
    bool _ending = false;
    bool _finished = false;
    grpc::Status _status = grpc::Status::OK;
};

// Messages.PublishStream: each request answered as it is routed (in the order
// they are routed, which need not be the order they were sent).
class PublishStreamReactor : public grpc::ServerBidiReactor<v1::PublishRequest, v1::PublishReply> {
public:
    PublishStreamReactor(AppHub& hub, bool allowedIn, uint32_t deadlineMs) : _hub(hub), _deadlineMs(deadlineMs) {
        if (!allowedIn) {
            _finished = true;
            Finish(refusedToken());
            return;
        }
        StartRead(&_request);
    }

    void OnReadDone(bool ok) override {
        if (!ok) {
            std::lock_guard<std::mutex> guard(_lock);
            _readsDone = true;
            finishIfDone();
            return;
        }
        {
            std::lock_guard<std::mutex> guard(_lock);
            _outstanding++;
        }
        const uint64_t requestId = _request.request_id();
        AppPublish message = publishOf(_request);
        message.deadlineMs = _deadlineMs;
        _hub.publish(message, [this, requestId](const AppPublishResult& result) {
            v1::PublishReply reply;
            resultTo(result, requestId, reply);
            reply_(reply);
        });
        StartRead(&_request);
    }

    void OnWriteDone(bool) override {
        v1::PublishReply* next = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _queue.pop_front();
            if (!_queue.empty()) {
                next = &_queue.front();
            } else {
                _writing = false;
                finishIfDone();
            }
        }
        if (next) StartWrite(next);
    }

    void OnDone() override { delete this; }

private:
    void reply_(const v1::PublishReply& reply) {
        v1::PublishReply* next = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _outstanding--;
            _queue.push_back(reply);
            if (_writing || _finished) return;
            _writing = true;
            next = &_queue.front();
        }
        StartWrite(next);
    }

    // With the lock held.
    void finishIfDone() {
        if (_readsDone && _outstanding == 0 && !_writing && !_finished) {
            _finished = true;
            Finish(grpc::Status::OK);
        }
    }

    AppHub& _hub;
    uint32_t _deadlineMs;
    v1::PublishRequest _request;
    std::mutex _lock;
    std::deque<v1::PublishReply> _queue;
    size_t _outstanding = 0;
    bool _writing = false;
    bool _readsDone = false;
    bool _finished = false;
};

// =============================================================================
// System.Attach: an application instance's presence, and its Wills
// =============================================================================
//
// Open with the Wills; Close first for a clean end. A stream that ends any
// other way - the process killed, the socket gone - has its Wills published,
// in order, as a device's would be.

class AttachReactor : public grpc::ServerBidiReactor<v1::AttachUp, v1::AttachDown> {
public:
    AttachReactor(AppHub* hub, bool allowedIn) : _hub(hub) {
        if (!allowedIn) {
            _done = true;
            Finish(refusedToken());
            return;
        }
        StartRead(&_up);
    }

    void OnReadDone(bool ok) override {
        if (!ok) {
            // Gone without a Close: its Wills are owed.
            if (!_closed && _hub) {
                for (const AppPublish& will : _wills) _hub->publish(will, [](const AppPublishResult&) {});
            }
            finish(grpc::Status::OK);
            return;
        }
        if (_up.has_open() && !_opened) {
            _opened = true;
            for (const v1::Message& will : _up.open().wills()) {
                AppPublish one;
                one.topic = will.topic();
                one.payload = will.payload();
                one.qos = (uint8_t)will.qos();
                one.retain = will.retain();
                one.properties = propertiesOf(will.properties());
                one.origin = _up.open().instance();
                _wills.push_back(one);
            }
            _reply.set_opened(true);
            _writing = true;
            StartWrite(&_reply);
        } else if (_up.has_close()) {
            _closed = true;
        }
        StartRead(&_up);
    }

    void OnWriteDone(bool) override {
        std::lock_guard<std::mutex> guard(_lock);
        _writing = false;
        if (_ending && !_done) {
            _done = true;
            Finish(_status);
        }
    }

    void OnDone() override { delete this; }

private:
    void finish(const grpc::Status& status) {
        std::lock_guard<std::mutex> guard(_lock);
        _ending = true;
        _status = status;
        if (!_writing && !_done) {
            _done = true;
            Finish(status);
        }
    }

    AppHub* _hub;
    v1::AttachUp _up;
    v1::AttachDown _reply;
    std::vector<AppPublish> _wills;
    std::mutex _lock;
    bool _opened = false, _closed = false, _writing = false, _ending = false, _done = false;
    grpc::Status _status = grpc::Status::OK;
};

// Connections.Watch: every change of state, as it happens. A server stream:
// it ends when the caller cancels it.
class WatchReactor : public grpc::ServerWriteReactor<v1::ConnectionInfo> {
public:
    WatchReactor(AppConnections& connections, const std::vector<std::string>& ids, bool allowedIn)
        : _connections(connections) {
        if (!allowedIn) {
            _finished = true;
            Finish(refusedToken());
            return;
        }
        _watcher = _connections.watch(ids, [this](const AppConnectionInfo& info) {
            v1::ConnectionInfo* next = nullptr;
            {
                std::lock_guard<std::mutex> guard(_lock);
                if (_ending) return;
                _queue.emplace_back();
                infoTo(info, _queue.back());
                if (_writing) return;
                _writing = true;
                next = &_queue.front();
            }
            StartWrite(next);
        });
    }

    void OnWriteDone(bool ok) override {
        v1::ConnectionInfo* next = nullptr;
        {
            std::lock_guard<std::mutex> guard(_lock);
            _queue.pop_front();
            if (ok && !_ending && !_queue.empty()) {
                next = &_queue.front();
            } else {
                _writing = false;
            }
        }
        if (next) StartWrite(next);
    }

    void OnCancel() override {
        _connections.unwatch(_watcher);
        std::lock_guard<std::mutex> guard(_lock);
        _ending = true;
        if (!_finished) {
            _finished = true;
            Finish(grpc::Status::OK);
        }
    }

    void OnDone() override {
        _connections.unwatch(_watcher);
        delete this;
    }

private:
    AppConnections& _connections;
    uint64_t _watcher = 0;
    std::mutex _lock;
    std::deque<v1::ConnectionInfo> _queue;
    bool _writing = false, _ending = false, _finished = false;
};

// =============================================================================
// THE SERVICES
// =============================================================================

class SystemService final : public v1::System::CallbackService {
public:
    SystemService(const ApiServerConfig& config, bool authority, AppHub* messages, bool connections, bool admin)
        : _config(config), _authority(authority), _hub(messages), _messages(messages != nullptr),
          _connections(connections), _admin(admin) {}

    grpc::ServerBidiReactor<v1::AttachUp, v1::AttachDown>* Attach(grpc::CallbackServerContext* context) override {
        return new AttachReactor(_hub, allowed(context, _config.token));
    }

    grpc::ServerUnaryReactor* Hello(grpc::CallbackServerContext* context, const v1::HelloRequest*,
                                    v1::HelloReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        reply->set_api_version(kApiVersion);
        reply->set_server_version(_config.serverVersion);
        reply->set_server_id(_config.serverId);
        if (_authority) reply->add_features("authority");
        if (_messages) reply->add_features("messages");
        if (_connections) reply->add_features("connections");
        if (_connections) reply->add_features("connections.subscriptions");
        if (_admin) reply->add_features("admin");
        if (_admin) reply->add_features("admin.listeners");
        if (_messages) reply->add_features("events.handshake");
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

private:
    const ApiServerConfig& _config;
    bool _authority;
    AppHub* _hub;
    bool _messages;
    bool _connections;
    bool _admin;
};


class AdminService final : public v1::Admin::CallbackService {
public:
    AdminService(const ApiServerConfig& config, AdminCore& core) : _config(config), _core(core) {}

#define MODBEE_ADMIN_CALL(Name, Request, Reply, call)                                                     \
    grpc::ServerUnaryReactor* Name(grpc::CallbackServerContext* context, const Request* request,          \
                                   Reply* reply) override {                                               \
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();                                    \
        if (!allowed(context, _config.token)) {                                                           \
            reactor->Finish(refusedToken());                                                              \
            return reactor;                                                                               \
        }                                                                                                 \
        (void)request;                                                                                    \
        call;                                                                                             \
        return reactor;                                                                                   \
    }
#define MODBEE_FINISH [reactor](const grpc::Status& status) { reactor->Finish(status); }

    MODBEE_ADMIN_CALL(ListLogins, v1::ListRequest, v1::ListLoginsReply, _core.listLogins(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(GetLogin, v1::NameRef, v1::Login, _core.getLogin(request->name(), reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(PutLogin, v1::PutLoginRequest, v1::PutLoginReply, _core.putLogin(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(DeleteLogin, v1::NameRef, v1::DeleteReply, _core.deleteLogin(request->name(), reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(ResetLogin, v1::NameRef, v1::PutLoginReply, _core.resetLogin(request->name(), reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(ListKeys, v1::ListRequest, v1::ListKeysReply, _core.listKeys(reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(PutKey, v1::SiteKey, v1::PutKeyReply, _core.putKey(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(DeleteKey, v1::NameRef, v1::DeleteReply, _core.deleteKey(request->name(), reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(ListSessions, v1::ListSessionsRequest, v1::ListSessionsReply, _core.listSessions(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(Kick, v1::KickRequest, v1::KickReply, _core.kick(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(ListRetained, v1::ListRetainedRequest, v1::ListRetainedReply, _core.listRetained(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(DeleteRetained, v1::DeleteRetainedRequest, v1::DeleteReply, _core.deleteRetained(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(GetListeners, v1::Empty, v1::Listeners, _core.listeners(reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(PutTls, v1::ServerTls, v1::Listeners, _core.putTls(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(PutListeners, v1::ListenersChange, v1::Listeners,
                      _core.putListeners(*request, reply, MODBEE_FINISH))
    MODBEE_ADMIN_CALL(Stats, v1::Empty, v1::StatsReply, _core.stats(reply, MODBEE_FINISH))

#undef MODBEE_FINISH
#undef MODBEE_ADMIN_CALL

private:
    const ApiServerConfig& _config;
    AdminCore& _core;
};

class ConnectionsService final : public v1::Connections::CallbackService {
public:
    ConnectionsService(const ApiServerConfig& config, AppConnections& connections)
        : _config(config), _connections(connections) {}

    grpc::ServerUnaryReactor* Put(grpc::CallbackServerContext* context, const v1::ConnectionSpec* spec,
                                  v1::ConnectionInfo* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        AppConnectionInfo info;
        std::string error;
        if (!_connections.put(specOf(*spec), &info, error)) {
            reactor->Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error));
            return reactor;
        }
        infoTo(info, *reply);
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor* Get(grpc::CallbackServerContext* context, const v1::ConnectionRef* ref,
                                  v1::ConnectionInfo* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        AppConnectionInfo info;
        if (!_connections.get(ref->id(), info)) {
            reactor->Finish(grpc::Status(grpc::StatusCode::NOT_FOUND, "no connection \"" + ref->id() + "\""));
            return reactor;
        }
        infoTo(info, *reply);
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor* List(grpc::CallbackServerContext* context, const v1::ListConnectionsRequest*,
                                   v1::ListConnectionsReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        for (const AppConnectionInfo& info : _connections.list()) infoTo(info, *reply->add_connections());
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor* Delete(grpc::CallbackServerContext* context, const v1::ConnectionRef* ref,
                                     v1::DeleteConnectionReply*) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        if (!_connections.remove(ref->id())) {
            reactor->Finish(grpc::Status(grpc::StatusCode::NOT_FOUND, "no connection \"" + ref->id() + "\""));
            return reactor;
        }
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerWriteReactor<v1::ConnectionInfo>* Watch(grpc::CallbackServerContext* context,
                                                        const v1::WatchConnectionsRequest* request) override {
        return new WatchReactor(_connections, std::vector<std::string>(request->ids().begin(), request->ids().end()),
                                allowed(context, _config.token));
    }

    grpc::ServerUnaryReactor* Reconnect(grpc::CallbackServerContext* context, const v1::ReconnectRequest* request,
                                        v1::ConnectionInfo* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        AppPublish will;
        const bool hasWill = request->has_will() && !request->will().topic().empty();
        if (hasWill) will = messageOf(request->will());
        AppConnectionInfo info;
        std::string error;
        if (!_connections.reconnect(request->id(), hasWill ? &will : nullptr, &info, error)) {
            reactor->Finish(grpc::Status(grpc::StatusCode::NOT_FOUND, error));
            return reactor;
        }
        infoTo(info, *reply);
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

private:
    const ApiServerConfig& _config;
    AppConnections& _connections;
};

class MessagesService final : public v1::Messages::CallbackService {
public:
    MessagesService(const ApiServerConfig& config, AppHub& hub) : _config(config), _hub(hub) {}

    grpc::ServerBidiReactor<v1::ConsumeUp, v1::ConsumeDown>* Consume(grpc::CallbackServerContext* context) override {
        return new ConsumeReactor(_hub, allowed(context, _config.token));
    }

    grpc::ServerUnaryReactor* Publish(grpc::CallbackServerContext* context, const v1::PublishRequest* request,
                                      v1::PublishReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        const uint64_t requestId = request->request_id();
        AppPublish message = publishOf(*request);
        message.deadlineMs = deadlineMsOf(context);
        _hub.publish(message, [reactor, reply, requestId](const AppPublishResult& result) {
            resultTo(result, requestId, *reply);
            reactor->Finish(grpc::Status::OK);
        });
        return reactor;
    }

    grpc::ServerBidiReactor<v1::PublishRequest, v1::PublishReply>* PublishStream(
        grpc::CallbackServerContext* context) override {
        return new PublishStreamReactor(_hub, allowed(context, _config.token), deadlineMsOf(context));
    }

    grpc::ServerUnaryReactor* ListGroups(grpc::CallbackServerContext* context, const v1::ListGroupsRequest*,
                                         v1::ListGroupsReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        for (const AppGroupInfo& info : _hub.groups()) groupTo(info, *reply->add_groups());
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor* DeleteGroup(grpc::CallbackServerContext* context, const v1::DeleteGroupRequest* request,
                                          v1::DeleteGroupReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return reactor;
        }
        uint64_t discarded = 0;
        std::string why;
        if (!_hub.deleteGroup(request->group(), request->connection(), &discarded, &why)) {
            reactor->Finish(grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, why));
            return reactor;
        }
        reply->set_discarded(discarded);
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

private:
    const ApiServerConfig& _config;
    AppHub& _hub;
};

class AuthorityService final : public v1::Authority::CallbackService {
public:
    AuthorityService(const ApiServerConfig& config, Authority* authority)
        : _config(config), _authority(authority) {}

    grpc::ServerBidiReactor<v1::AuthorityUp, v1::AuthorityDown>* Serve(
        grpc::CallbackServerContext* context) override {
        if (_authority == nullptr) {
            // A reactor that only says why.
            struct Refusal : grpc::ServerBidiReactor<v1::AuthorityUp, v1::AuthorityDown> {
                Refusal() { Finish(noAuthority()); }
                void OnDone() override { delete this; }
            };
            return new Refusal();
        }
        return new AuthorityReactor(*_authority, allowed(context, _config.token));
    }

    grpc::ServerUnaryReactor* Revoke(grpc::CallbackServerContext* context, const v1::RevokeRequest* request,
                                     v1::RevokeReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!check(context, reactor)) return reactor;
        if (request->login().empty()) {
            reactor->Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "login is required"));
            return reactor;
        }
        _authority->revoke(request->login(), refusalOf(request->reason()),
                           [reactor, reply](const AuthorityChange& change) {
                               reply->set_closed((uint32_t)change.closed);
                               reactor->Finish(grpc::Status::OK);
                           });
        return reactor;
    }

    grpc::ServerUnaryReactor* Regrant(grpc::CallbackServerContext* context, const v1::RegrantRequest* request,
                                      v1::RegrantReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!check(context, reactor)) return reactor;
        if (request->login().empty()) {
            reactor->Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "login is required"));
            return reactor;
        }
        _authority->regrant(request->login(), request->has_client_id() ? request->client_id() : std::string(),
                            scopeOf(request->scope()), request->ttl_seconds(),
                            [reactor, reply](const AuthorityChange& change) {
                                reply->set_sessions((uint32_t)change.sessions);
                                reply->set_subscriptions_removed((uint32_t)change.removed);
                                reactor->Finish(grpc::Status::OK);
                            });
        return reactor;
    }

    grpc::ServerUnaryReactor* Forget(grpc::CallbackServerContext* context, const v1::ForgetRequest* request,
                                     v1::ForgetReply* reply) override {
        grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
        if (!check(context, reactor)) return reactor;
        _authority->forget(request->has_login() ? request->login() : std::string(),
                           [reactor, reply](const AuthorityChange& change) {
                               reply->set_forgotten(change.forgotten);
                               reactor->Finish(grpc::Status::OK);
                           });
        return reactor;
    }

private:
    bool check(grpc::CallbackServerContext* context, grpc::ServerUnaryReactor* reactor) {
        if (!allowed(context, _config.token)) {
            reactor->Finish(refusedToken());
            return false;
        }
        if (_authority == nullptr) {
            reactor->Finish(noAuthority());
            return false;
        }
        return true;
    }

    const ApiServerConfig& _config;
    Authority* _authority;
};

} // namespace

// =============================================================================
// THE SERVER
// =============================================================================

struct ApiServer::Impl {
    ApiServerConfig config;
    ApiServer::Services services;
    std::unique_ptr<AdminCore> core;
    std::unique_ptr<AdminService> admin;
    std::unique_ptr<SystemService> system;
    std::unique_ptr<AuthorityService> authority;
    std::unique_ptr<MessagesService> messages;
    std::unique_ptr<ConnectionsService> connections;
    std::unique_ptr<grpc::Server> server;
    std::vector<std::string> listening;
    std::vector<std::string> sockets;      // Unix socket files, removed at stop
};

ApiServer::ApiServer() : _impl(new Impl()) {}

ApiServer::~ApiServer() { stop(); }

bool ApiServer::start(const ApiServerConfig& config, const Services& services, std::string& error) {
    Impl& self = *_impl;
    self.config = config;
    self.services = services;
    Authority* authority = services.authority;
    AppHub* messages = services.messages;
    AppConnections* connections = services.connections;
    self.core.reset(new AdminCore(self.services));
    self.admin.reset(new AdminService(self.config, *self.core));
    self.system.reset(new SystemService(self.config, authority != nullptr, messages, connections != nullptr,
                                         services.admin != nullptr));
    self.authority.reset(new AuthorityService(self.config, authority));
    if (messages) self.messages.reset(new MessagesService(self.config, *messages));
    if (connections) self.connections.reset(new ConnectionsService(self.config, *connections));

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    std::vector<std::unique_ptr<int>> ports;
    for (const std::string& address : config.addresses) {
        if (address.compare(0, 5, "unix:") == 0) {
            const std::string path = address.substr(5);
            if (path.size() > 107) {
                error = "api.grpc: \"" + address + "\": a Unix socket's path is at most 107 bytes";
                return false;
            }
            std::remove(path.c_str());    // one a server before this one left
            builder.AddListeningPort(address, grpc::InsecureServerCredentials());
            self.sockets.push_back(path);
            self.listening.push_back(address);
            ports.emplace_back();
        } else if (address.compare(0, 6, "tcp://") == 0 && (apiAddressIsLoopback(address) || config.plainTcp) &&
                   config.tlsCertPem.empty()) {
            // This machine only, the token the lock: how an application
            // beside the server reaches it on Windows (and may on Linux).
            ports.emplace_back(new int(0));
            builder.AddListeningPort(address.substr(6), grpc::InsecureServerCredentials(), ports.back().get());
            self.listening.push_back(address);
        } else if (address.compare(0, 6, "tcp://") == 0) {
            grpc::SslServerCredentialsOptions tls(
                config.tlsClientCaPem.empty() ? GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE
                                              : GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
            tls.pem_root_certs = config.tlsClientCaPem;
            tls.pem_key_cert_pairs.push_back({config.tlsKeyPem, config.tlsCertPem});
            ports.emplace_back(new int(0));
            builder.AddListeningPort(address.substr(6), grpc::SslServerCredentials(tls), ports.back().get());
            self.listening.push_back(address);
        } else {
            error = "api.grpc: \"" + address + "\" is neither unix:<path> nor tcp://<host>:<port>";
            return false;
        }
    }
    builder.RegisterService(self.system.get());
    builder.RegisterService(self.authority.get());
    if (self.messages) builder.RegisterService(self.messages.get());
    if (self.connections) builder.RegisterService(self.connections.get());
    builder.RegisterService(self.admin.get());
    self.server = builder.BuildAndStart();
    if (!self.server) {
        error = "the Application API could not listen on every address (in use, or no permission?)";
        return false;
    }
    for (size_t index = 0; index < ports.size(); index++) {
        if (!ports[index]) continue;
        const std::string& address = self.listening[index];
        const size_t colon = address.rfind(':');
        if (*ports[index] == 0) {
            error = "api.grpc: could not listen on " + address;
            stop();
            return false;
        }
        self.listening[index] = address.substr(0, colon + 1) + std::to_string(*ports[index]);
    }
    // Only its own user and group: the file is the lock on the local socket.
#if !defined(_WIN32)
    for (const std::string& path : self.sockets) ::chmod(path.c_str(), 0660);
#endif
    return true;
}

void ApiServer::stop() {
    Impl& self = *_impl;
    if (self.server) {
        self.server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        self.server->Wait();
        self.server.reset();
    }
    for (const std::string& path : self.sockets) std::remove(path.c_str());
    self.sockets.clear();
}

const std::vector<std::string>& ApiServer::listening() const { return _impl->listening; }

std::string ApiServer::libraryVersion() { return grpc::Version(); }

// =============================================================================
// THE REST SURFACE: the same calls, as JSON over HTTP
// =============================================================================

namespace {

std::string percentDecoded(const std::string& in) {
    std::string out;
    for (size_t i = 0; i < in.size(); i++) {
        if (in[i] == '%' && i + 2 < in.size() && std::isxdigit((unsigned char)in[i + 1]) &&
            std::isxdigit((unsigned char)in[i + 2])) {
            out.push_back((char)std::stoi(in.substr(i + 1, 2), nullptr, 16));
            i += 2;
        } else if (in[i] == '+') {
            out.push_back(' ');
        } else {
            out.push_back(in[i]);
        }
    }
    return out;
}

std::map<std::string, std::string> queryOf(const std::string& query) {
    std::map<std::string, std::string> out;
    size_t at = 0;
    while (at < query.size()) {
        size_t end = query.find('&', at);
        if (end == std::string::npos) end = query.size();
        const std::string pair = query.substr(at, end - at);
        const size_t equals = pair.find('=');
        if (equals == std::string::npos) out[percentDecoded(pair)] = "true";
        else out[percentDecoded(pair.substr(0, equals))] = percentDecoded(pair.substr(equals + 1));
        at = end + 1;
    }
    return out;
}

// As grpc-gateway maps them.
int httpOf(grpc::StatusCode code) {
    switch (code) {
    case grpc::StatusCode::OK: return 200;
    case grpc::StatusCode::INVALID_ARGUMENT:
    case grpc::StatusCode::FAILED_PRECONDITION:
    case grpc::StatusCode::OUT_OF_RANGE: return 400;
    case grpc::StatusCode::UNAUTHENTICATED: return 401;
    case grpc::StatusCode::PERMISSION_DENIED: return 403;
    case grpc::StatusCode::NOT_FOUND: return 404;
    case grpc::StatusCode::ALREADY_EXISTS:
    case grpc::StatusCode::ABORTED: return 409;
    case grpc::StatusCode::RESOURCE_EXHAUSTED: return 429;
    case grpc::StatusCode::UNIMPLEMENTED: return 501;
    case grpc::StatusCode::UNAVAILABLE: return 503;
    case grpc::StatusCode::DEADLINE_EXCEEDED: return 504;
    default: return 500;
    }
}

std::string errorBody(int code, const std::string& message) {
    json::Object o;
    o.add("code", (long long)code).add("message", message);
    return o.str();
}

std::string jsonOf(const google::protobuf::Message& message) {
    google::protobuf::util::JsonPrintOptions options;
    // Renamed in protobuf 26 (C++ 5.26): the same switch, every field shown.
#if (defined(PROTOBUF_VERSION) && PROTOBUF_VERSION >= 5026000) || \
    (defined(GOOGLE_PROTOBUF_VERSION) && GOOGLE_PROTOBUF_VERSION >= 5026000)
    options.always_print_fields_with_no_presence = true;
#else
    options.always_print_primitive_fields = true;
#endif
    std::string out;
    google::protobuf::util::MessageToJsonString(message, &out, options);
    return out;
}

bool parsed(const std::string& body, google::protobuf::Message& message, std::string& error) {
    if (body.empty()) return true;
    google::protobuf::util::JsonParseOptions options;
    options.ignore_unknown_fields = false;
    const auto status = google::protobuf::util::JsonStringToMessage(body, &message, options);
    if (status.ok()) return true;
    error = std::string(status.message());
    return false;
}

// One call, waited for. The request and the answer are held by whoever lets go
// last - this thread, or a call that answers after the wait gave up on it.
struct RestCall {
    std::mutex lock;
    std::condition_variable answered;
    bool finished = false;
    grpc::Status status;
    std::shared_ptr<google::protobuf::Message> request, reply;
};

grpc::Status waited(std::shared_ptr<google::protobuf::Message> request,
                    std::shared_ptr<google::protobuf::Message> reply, std::function<void(Done)> call) {
    std::shared_ptr<RestCall> state(new RestCall());
    state->request = request;
    state->reply = reply;
    call([state](const grpc::Status& status) {
        std::lock_guard<std::mutex> guard(state->lock);
        state->status = status;
        state->finished = true;
        state->answered.notify_all();
    });
    std::unique_lock<std::mutex> guard(state->lock);
    if (!state->answered.wait_for(guard, std::chrono::seconds(10), [&] { return state->finished; })) {
        return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "no answer in time");
    }
    return state->status;
}

template <typename T>
std::shared_ptr<T> made() { return std::make_shared<T>(); }

} // namespace

int ApiServer::rest(const std::string& method, const std::string& target, const std::string& authorization,
                    const std::string& body, std::string& response) {
    Impl& self = *_impl;
    if (!self.core) {
        response = errorBody(503, "the Application API is not running");
        return 503;
    }
    if (!self.config.token.empty() && authorization != "Bearer " + self.config.token) {
        response = errorBody(401, "this call needs \"Authorization: Bearer <token>\" (api.token)");
        return 401;
    }
    const size_t mark = target.find('?');
    const std::string path = target.substr(0, mark);
    std::map<std::string, std::string> query = queryOf(mark == std::string::npos ? "" : target.substr(mark + 1));
    auto number = [&](const char* key) { return (uint32_t)std::strtoul(query[key].c_str(), nullptr, 10); };
    if (path.compare(0, 4, "/v1/") != 0) {
        response = errorBody(404, "the REST surface is under /v1/");
        return 404;
    }
    // "<collection>/<name>[:verb]", the name percent-decoded (a login may hold '/').
    const std::string rest = path.substr(4);
    const size_t slash = rest.find('/');
    const std::string collection = rest.substr(0, slash);
    std::string name = slash == std::string::npos ? "" : rest.substr(slash + 1);
    std::string verb;
    const size_t colon = name.rfind(':');
    if (colon != std::string::npos) {
        verb = name.substr(colon + 1);
        name = name.substr(0, colon);
    }
    name = percentDecoded(name);
    AdminCore* core = self.core.get();
    std::string error;
    grpc::Status status = grpc::Status(grpc::StatusCode::NOT_FOUND, "no such call: " + method + " " + path);
    std::shared_ptr<google::protobuf::Message> reply;
    // An answer made here, at once.
    auto answer = [&](std::shared_ptr<google::protobuf::Message> out, const grpc::Status& result) {
        reply = out;
        status = result;
    };
    // An answer the core gives, perhaps on another thread.
    auto run = [&](std::shared_ptr<google::protobuf::Message> request, std::shared_ptr<google::protobuf::Message> out,
                   std::function<void(Done)> call) {
        reply = out;
        status = waited(request, out, call);
    };
    // A body parsed into `request`, or false with the status said.
    auto read = [&](google::protobuf::Message& request) {
        if (parsed(body, request, error)) return true;
        status = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error);
        return false;
    };

    if (collection == "logins") {
        if (method == "GET" && name.empty()) {
            auto r = made<v1::ListRequest>();
            r->set_prefix(query["prefix"]);
            r->set_page_size(number("pageSize"));
            r->set_page_token(query["pageToken"]);
            auto out = made<v1::ListLoginsReply>();
            run(r, out, [=](Done done) { core->listLogins(*r, out.get(), done); });
        } else if (method == "GET") {
            auto out = made<v1::Login>();
            run(nullptr, out, [=](Done done) { core->getLogin(name, out.get(), done); });
        } else if (method == "PUT") {
            auto r = made<v1::PutLoginRequest>();
            if (read(*r)) {
                r->mutable_login()->set_name(name);
                auto out = made<v1::PutLoginReply>();
                run(r, out, [=](Done done) { core->putLogin(*r, out.get(), done); });
            }
        } else if (method == "DELETE") {
            auto out = made<v1::DeleteReply>();
            run(nullptr, out, [=](Done done) { core->deleteLogin(name, out.get(), done); });
        } else if (method == "POST" && verb == "reset") {
            auto out = made<v1::PutLoginReply>();
            run(nullptr, out, [=](Done done) { core->resetLogin(name, out.get(), done); });
        } else if (method == "POST" && verb == "kick") {
            auto r = made<v1::KickRequest>();
            if (read(*r)) {
                r->set_login(name);
                auto out = made<v1::KickReply>();
                run(r, out, [=](Done done) { core->kick(*r, out.get(), done); });
            }
        }
    } else if (collection == "keys") {
        if (method == "GET") {
            auto out = made<v1::ListKeysReply>();
            run(nullptr, out, [=](Done done) { core->listKeys(out.get(), done); });
        } else if (method == "PUT") {
            auto r = made<v1::SiteKey>();
            if (read(*r)) {
                r->set_name(name);
                auto out = made<v1::PutKeyReply>();
                run(r, out, [=](Done done) { core->putKey(*r, out.get(), done); });
            }
        } else if (method == "DELETE") {
            auto out = made<v1::DeleteReply>();
            run(nullptr, out, [=](Done done) { core->deleteKey(name, out.get(), done); });
        }
    } else if (collection == "sessions") {
        if (method == "GET") {
            auto r = made<v1::ListSessionsRequest>();
            r->set_client_id_prefix(query["clientIdPrefix"]);
            r->set_login(query["login"]);
            r->set_connected_only(query["connectedOnly"] == "true");
            r->set_page_size(number("pageSize"));
            r->set_page_token(query["pageToken"]);
            auto out = made<v1::ListSessionsReply>();
            run(r, out, [=](Done done) { core->listSessions(*r, out.get(), done); });
        } else if (method == "POST" && verb == "kick") {
            auto r = made<v1::KickRequest>();
            if (read(*r)) {
                r->set_client_id(name);
                auto out = made<v1::KickReply>();
                run(r, out, [=](Done done) { core->kick(*r, out.get(), done); });
            }
        }
    } else if (collection == "retained") {
        if (method == "GET") {
            auto r = made<v1::ListRetainedRequest>();
            r->set_filter(query["filter"]);
            r->set_payloads(query["payloads"] == "true");
            r->set_page_size(number("pageSize"));
            r->set_page_token(query["pageToken"]);
            auto out = made<v1::ListRetainedReply>();
            run(r, out, [=](Done done) { core->listRetained(*r, out.get(), done); });
        } else if (method == "DELETE") {
            auto r = made<v1::DeleteRetainedRequest>();
            r->set_filter(query["filter"]);
            auto out = made<v1::DeleteReply>();
            run(r, out, [=](Done done) { core->deleteRetained(*r, out.get(), done); });
        }
    } else if (collection == "listeners") {
        if (method == "GET" && name.empty()) {
            auto out = made<v1::Listeners>();
            run(nullptr, out, [=](Done done) { core->listeners(out.get(), done); });
        } else if (method == "PUT" && name == "tls") {
            auto r = made<v1::ServerTls>();
            if (read(*r)) {
                auto out = made<v1::Listeners>();
                run(r, out, [=](Done done) { core->putTls(*r, out.get(), done); });
            }
        } else if ((method == "PUT" || method == "PATCH") && name.empty()) {
            auto r = made<v1::ListenersChange>();
            if (read(*r)) {
                auto out = made<v1::Listeners>();
                run(r, out, [=](Done done) { core->putListeners(*r, out.get(), done); });
            }
        }
    } else if (collection == "stats" && method == "GET") {
        auto out = made<v1::StatsReply>();
        run(nullptr, out, [=](Done done) { core->stats(out.get(), done); });
    } else if (collection == "health" && method == "GET") {
        response = "{\"status\":\"SERVING\"}";
        return 200;
    } else if (collection == "connections" && self.services.connections) {
        AppConnections& connections = *self.services.connections;
        const grpc::Status missing(grpc::StatusCode::NOT_FOUND, "no connection \"" + name + "\"");
        if (method == "GET" && name.empty()) {
            auto out = made<v1::ListConnectionsReply>();
            for (const AppConnectionInfo& info : connections.list()) infoTo(info, *out->add_connections());
            answer(out, grpc::Status::OK);
        } else if (method == "GET") {
            AppConnectionInfo info;
            auto out = made<v1::ConnectionInfo>();
            const bool found = connections.get(name, info);
            if (found) infoTo(info, *out);
            answer(out, found ? grpc::Status::OK : missing);
        } else if (method == "PUT") {
            v1::ConnectionSpec r;
            if (read(r)) {
                r.set_id(name);
                AppConnectionInfo info;
                auto out = made<v1::ConnectionInfo>();
                const bool put = connections.put(specOf(r), &info, error);
                if (put) infoTo(info, *out);
                answer(out, put ? grpc::Status::OK : grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error));
            }
        } else if (method == "DELETE") {
            answer(made<v1::DeleteConnectionReply>(), connections.remove(name) ? grpc::Status::OK : missing);
        }
    } else if (collection == "groups" && self.services.messages) {
        AppHub& hub = *self.services.messages;
        if (method == "GET") {
            auto out = made<v1::ListGroupsReply>();
            for (const AppGroupInfo& info : hub.groups()) groupTo(info, *out->add_groups());
            answer(out, grpc::Status::OK);
        } else if (method == "DELETE") {
            uint64_t discarded = 0;
            auto out = made<v1::DeleteGroupReply>();
            const bool deleted = hub.deleteGroup(name, query["connection"], &discarded, &error);
            out->set_discarded(discarded);
            answer(out, deleted ? grpc::Status::OK : grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, error));
        }
    } else if (collection == "publish" && method == "POST" && self.services.messages) {
        auto r = made<v1::PublishRequest>();
        if (read(*r)) {
            auto out = made<v1::PublishReply>();
            AppHub* hub = self.services.messages;
            run(r, out, [=](Done done) {
                hub->publish(publishOf(*r), [r, out, done](const AppPublishResult& result) {
                    resultTo(result, r->request_id(), *out);
                    done(grpc::Status::OK);
                });
            });
        }
    }

    const int code = httpOf(status.error_code());
    response = status.ok() && reply ? jsonOf(*reply) : errorBody(code, status.error_message());
    return code;
}

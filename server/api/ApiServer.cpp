// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ApiServer.h"
#include "Authority.h"
#include "AppHub.h"

#include "modbee/app/v1/app.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <chrono>
#include <deque>
#include <mutex>

#include <sys/stat.h>
#include <unistd.h>

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
                        "this call needs \"authorization: Bearer <token>\" (api.token-file)");
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
        options.filters.assign(wire.filters().begin(), wire.filters().end());
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
// THE SERVICES
// =============================================================================

class SystemService final : public v1::System::CallbackService {
public:
    SystemService(const ApiServerConfig& config, bool authority, bool messages)
        : _config(config), _authority(authority), _messages(messages) {}

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
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

private:
    const ApiServerConfig& _config;
    bool _authority;
    bool _messages;
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
        for (const AppGroupInfo& info : _hub.groups()) {
            v1::GroupInfo* group = reply->add_groups();
            group->set_group(info.group);
            group->set_connection(info.connection);
            for (const std::string& filter : info.filters) group->add_filters(filter);
            group->set_members(info.members);
            group->set_queued(info.queued);
            group->set_in_flight(info.inFlight);
            group->set_refused(info.refused);
        }
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
    std::unique_ptr<SystemService> system;
    std::unique_ptr<AuthorityService> authority;
    std::unique_ptr<MessagesService> messages;
    std::unique_ptr<grpc::Server> server;
    std::vector<std::string> listening;
    std::vector<std::string> sockets;      // Unix socket files, removed at stop
};

ApiServer::ApiServer() : _impl(new Impl()) {}

ApiServer::~ApiServer() { stop(); }

bool ApiServer::start(const ApiServerConfig& config, Authority* authority, AppHub* messages,
                      std::string& error) {
    Impl& self = *_impl;
    self.config = config;
    self.system.reset(new SystemService(self.config, authority != nullptr, messages != nullptr));
    self.authority.reset(new AuthorityService(self.config, authority));
    if (messages) self.messages.reset(new MessagesService(self.config, *messages));

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
            ::unlink(path.c_str());       // one a server before this one left
            builder.AddListeningPort(address, grpc::InsecureServerCredentials());
            self.sockets.push_back(path);
            self.listening.push_back(address);
            ports.emplace_back();
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
    for (const std::string& path : self.sockets) ::chmod(path.c_str(), 0660);
    return true;
}

void ApiServer::stop() {
    Impl& self = *_impl;
    if (self.server) {
        self.server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        self.server->Wait();
        self.server.reset();
    }
    for (const std::string& path : self.sockets) ::unlink(path.c_str());
    self.sockets.clear();
}

const std::vector<std::string>& ApiServer::listening() const { return _impl->listening; }

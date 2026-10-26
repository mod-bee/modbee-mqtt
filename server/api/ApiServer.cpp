// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ApiServer.h"
#include "Authority.h"

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
// THE SERVICES
// =============================================================================

class SystemService final : public v1::System::CallbackService {
public:
    SystemService(const ApiServerConfig& config, bool authority) : _config(config), _authority(authority) {}

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
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

private:
    const ApiServerConfig& _config;
    bool _authority;
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
    std::unique_ptr<grpc::Server> server;
    std::vector<std::string> listening;
    std::vector<std::string> sockets;      // Unix socket files, removed at stop
};

ApiServer::ApiServer() : _impl(new Impl()) {}

ApiServer::~ApiServer() { stop(); }

bool ApiServer::start(const ApiServerConfig& config, Authority* authority, std::string& error) {
    Impl& self = *_impl;
    self.config = config;
    self.system.reset(new SystemService(self.config, authority != nullptr));
    self.authority.reset(new AuthorityService(self.config, authority));

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

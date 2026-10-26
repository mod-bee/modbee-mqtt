// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MetricsEndpoint.h"

#include "transport/MqttSocketSupport.h"   // POSIX sockets, or Winsock

#if defined(_WIN32)
    #define MQ_POLL WSAPoll
    typedef WSAPOLLFD MqPollFd;
    #define MQ_SEND_FLAGS 0
    #define SOCK_CLOEXEC 0
#else
    #define MQ_POLL ::poll
    typedef struct pollfd MqPollFd;
    #define MQ_SEND_FLAGS MSG_NOSIGNAL
#endif

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>

bool MetricsEndpoint::start(const std::string& address, uint16_t port, RenderFn render,
                            void* context, std::string& error) {
    _render = render;
    _context = context;
    mq_socket::ensureWinsock();
    _listen = (int)::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (_listen < 0) { error = "metrics: no socket"; return false; }
    const int one = 1;
    setsockopt(_listen, SOL_SOCKET, SO_REUSEADDR, (const char*)&one, sizeof one);
    sockaddr_in at;
    std::memset(&at, 0, sizeof at);
    at.sin_family = AF_INET;
    at.sin_port = htons(port);
    if (inet_pton(AF_INET, address.empty() ? "0.0.0.0" : address.c_str(), &at.sin_addr) != 1) {
        error = "metrics: " + address + " is not an IPv4 address";
        MQ_CLOSE(_listen); _listen = -1;
        return false;
    }
    if (::bind(_listen, (sockaddr*)&at, sizeof at) != 0 || ::listen(_listen, 16) != 0) {
        error = "metrics: port " + std::to_string(port) + " is already in use";
        MQ_CLOSE(_listen); _listen = -1;
        return false;
    }
    socklen_t length = sizeof at;
    getsockname(_listen, (sockaddr*)&at, &length);
    _port = ntohs(at.sin_port);
    _wake.open();
    _stopping = false;
    _thread = std::thread([this] { serve(); });
    return true;
}

void MetricsEndpoint::stop() {
    if (!_thread.joinable()) return;
    _stopping = true;
    _wake.signal();
    _thread.join();
    MQ_CLOSE(_listen); _listen = -1;
    _wake.close();
}

namespace {

// A REST body is a login, a key, a connection or a message: well under this.
const size_t kMaxBody = 1024 * 1024;

std::string lowered(std::string text) {
    for (char& c : text) c = (char)std::tolower((unsigned char)c);
    return text;
}

struct Request {
    std::string method, target, authorization, body;
    bool tooLarge = false;
};

// The request line, the two headers wanted and the body, or false.
bool readRequest(int client, Request& request) {
    std::string data;
    char chunk[4096];
    size_t headEnd = std::string::npos;
    while (headEnd == std::string::npos) {
        const int got = (int)::recv(client, chunk, (int)sizeof chunk, 0);
        if (got <= 0) return false;
        data.append(chunk, (size_t)got);
        headEnd = data.find("\r\n\r\n");
        if (headEnd == std::string::npos && data.size() > 16384) return false;
    }
    const size_t lineEnd = data.find("\r\n");
    const std::string line = data.substr(0, lineEnd);
    const size_t space = line.find(' ');
    const size_t space2 = line.find(' ', space + 1);
    if (space == std::string::npos || space2 == std::string::npos) return false;
    request.method = line.substr(0, space);
    request.target = line.substr(space + 1, space2 - space - 1);
    size_t length = 0;
    size_t at = lineEnd + 2;
    while (at < headEnd) {
        size_t end = data.find("\r\n", at);
        if (end == std::string::npos || end > headEnd) end = headEnd;
        const std::string header = data.substr(at, end - at);
        const size_t colon = header.find(':');
        if (colon != std::string::npos) {
            const std::string name = lowered(header.substr(0, colon));
            size_t from = colon + 1;
            while (from < header.size() && header[from] == ' ') from++;
            const std::string value = header.substr(from);
            if (name == "authorization") request.authorization = value;
            else if (name == "content-length") length = (size_t)std::strtoull(value.c_str(), nullptr, 10);
        }
        at = end + 2;
    }
    if (length > kMaxBody) {
        request.tooLarge = true;
        return true;
    }
    request.body = data.substr(headEnd + 4);
    while (request.body.size() < length) {
        const int got = (int)::recv(client, chunk, (int)sizeof chunk, 0);
        if (got <= 0) return false;
        request.body.append(chunk, (size_t)got);
    }
    request.body.resize(length);
    return true;
}

} // namespace

void MetricsEndpoint::serve() {
    while (!_stopping.load()) {
        MqPollFd fds[2];
        std::memset(fds, 0, sizeof fds);
        fds[0].fd = _listen;
        fds[0].events = POLLIN;
        fds[1].fd = _wake.handle();
        fds[1].events = POLLIN;
        if (MQ_POLL(fds, 2, 1000) <= 0 || !(fds[0].revents & POLLIN)) continue;
        const int client = (int)::accept(_listen, nullptr, nullptr);
        if (client < 0) continue;
        // A client that dribbles its request gets two seconds a read, then
        // the door.
#if defined(_WIN32)
        const DWORD timeout = 2000;
#else
        timeval timeout = {2, 0};
#endif
        setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout);
        setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof timeout);
        Request request;
        const bool got = readRequest(client, request);
        const std::string& target = request.target;
        auto is = [&](const char* method, const char* path) {
            const size_t n = std::strlen(path);
            return request.method == method && target.compare(0, n, path) == 0 &&
                   (target.size() == n || target[n] == '?');
        };
        std::string body, status = "200 OK", type = "text/plain; version=0.0.4; charset=utf-8";
        if (got) {
            if (_rest != nullptr && target.compare(0, 4, "/v1/") == 0) {
                type = "application/json";
                int code = 413;
                if (request.tooLarge) body = "{\"code\":413,\"message\":\"the body is over 1 MB\"}";
                else code = _rest(_restContext, request.method, target, request.authorization, request.body, body);
                const char* reason = code == 200 ? "OK" : code == 400 ? "Bad Request" : code == 401 ? "Unauthorized"
                                   : code == 403 ? "Forbidden" : code == 404 ? "Not Found" : code == 409 ? "Conflict"
                                   : code == 413 ? "Payload Too Large" : code == 429 ? "Too Many Requests"
                                   : code == 501 ? "Not Implemented" : code == 503 ? "Service Unavailable"
                                   : code == 504 ? "Gateway Timeout" : "Error";
                status = std::to_string(code) + " " + reason;
            } else if (is("GET", "/metrics")) {
                _render(_context, body);
            } else if (is("GET", "/") || is("GET", "/healthz")) {
                // LIVENESS: the process answers. Restart it only if this fails.
                body = "ok\n";
                type = "text/plain";
            } else if (is("GET", "/readyz")) {
                // READINESS: give it clients. Not ready while Kafka, which
                // holds publishers' acknowledgements, cannot be reached.
                std::string why;
                const bool ready = _ready == nullptr || _ready(_readyContext, why);
                if (!ready) status = "503 Service Unavailable";
                body = ready ? "ready\n" : "not ready: " + why + "\n";
                type = "text/plain";
            } else {
                status = "404 Not Found";
                body = "only /metrics, /healthz and /readyz here\n";
                type = "text/plain";
            }
        }
        const std::string head = "HTTP/1.1 " + status + "\r\nContent-Type: " + type +
                                 "\r\nContent-Length: " + std::to_string(body.size()) +
                                 "\r\nConnection: close\r\n\r\n";
        const std::string response = head + body;
        size_t sent = 0;
        while (sent < response.size()) {
            const int n = (int)::send(client, response.data() + sent, (int)(response.size() - sent), MQ_SEND_FLAGS);
            if (n <= 0) break;
            sent += (size_t)n;
        }
        MQ_CLOSE(client);
    }
}

namespace {

void metric(std::string& out, const char* name, const char* type, const char* help,
            unsigned long long value) {
    out += std::string("# HELP ") + name + " " + help + "\n# TYPE " + name + " " + type + "\n";
    out += std::string(name) + " " + std::to_string(value) + "\n";
}

} // namespace

void renderBrokerMetrics(const ShardedBrokerStatus& s, std::string& out) {
    const MqttBrokerStatus& t = s.total;
    metric(out, "modbee_connections", "gauge", "Client connections open now.", t.connections);
    metric(out, "modbee_sessions", "gauge", "Sessions held now, connected or not.", t.sessions);
    metric(out, "modbee_retained_messages", "gauge", "Retained messages stored.", t.retained);
    metric(out, "modbee_connections_accepted_total", "counter", "Connections accepted at the socket.", t.accepted);
    metric(out, "modbee_connects_total", "counter", "CONNECTs answered with success.", t.connected);
    metric(out, "modbee_connects_refused_total", "counter", "CONNECTs answered with a failure.", t.refused);
    metric(out, "modbee_disconnects_total", "counter", "Connections ended.", t.disconnected);
    metric(out, "modbee_takeovers_total", "counter", "Sessions taken over by a new connection.", t.takeovers);
    metric(out, "modbee_messages_published_total", "counter", "PUBLISHes accepted from clients.", t.published);
    metric(out, "modbee_messages_delivered_total", "counter", "PUBLISHes sent to clients.", t.delivered);
    metric(out, "modbee_messages_dropped_total", "counter", "Deliveries a full session queue could not take.", t.dropped);
    metric(out, "modbee_protocol_errors_total", "counter", "Connections closed for a protocol error.", t.protocolErrors);
    metric(out, "modbee_keepalive_timeouts_total", "counter", "Connections closed for silence.", t.keepAliveTimeouts);
    metric(out, "modbee_wills_published_total", "counter", "Will messages published.", t.willsPublished);
    metric(out, "modbee_sessions_expired_total", "counter", "Sessions ended by expiry.", t.sessionsExpired);
    metric(out, "modbee_shards", "gauge", "Brokers, one per thread.", s.shards);
    metric(out, "modbee_handoffs_total", "counter", "Connections handed to the shard owning their session.", t.handedOff);
    metric(out, "modbee_mailbox_full_total", "counter", "Messages a full inter-shard ring refused.", s.mailboxFull);
    out += "# HELP modbee_shard_connections Client connections open now, per shard.\n"
           "# TYPE modbee_shard_connections gauge\n";
    for (size_t i = 0; i < s.connectionsPerShard.size(); i++) {
        out += "modbee_shard_connections{shard=\"" + std::to_string(i) + "\"} " +
               std::to_string(s.connectionsPerShard[i]) + "\n";
    }
}

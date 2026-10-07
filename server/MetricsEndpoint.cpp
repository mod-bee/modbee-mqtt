// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "MetricsEndpoint.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>

bool MetricsEndpoint::start(const std::string& address, uint16_t port, RenderFn render,
                            void* context, std::string& error) {
    _render = render;
    _context = context;
    _listen = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (_listen < 0) { error = "metrics: no socket"; return false; }
    const int one = 1;
    setsockopt(_listen, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in at;
    std::memset(&at, 0, sizeof at);
    at.sin_family = AF_INET;
    at.sin_port = htons(port);
    if (inet_pton(AF_INET, address.empty() ? "0.0.0.0" : address.c_str(), &at.sin_addr) != 1) {
        error = "metrics: " + address + " is not an IPv4 address";
        ::close(_listen); _listen = -1;
        return false;
    }
    if (::bind(_listen, (sockaddr*)&at, sizeof at) != 0 || ::listen(_listen, 16) != 0) {
        error = "metrics: port " + std::to_string(port) + " is already in use";
        ::close(_listen); _listen = -1;
        return false;
    }
    socklen_t length = sizeof at;
    getsockname(_listen, (sockaddr*)&at, &length);
    _port = ntohs(at.sin_port);
    _wake = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    _stopping = false;
    _thread = std::thread([this] { serve(); });
    return true;
}

void MetricsEndpoint::stop() {
    if (!_thread.joinable()) return;
    _stopping = true;
    const uint64_t one = 1;
    const ssize_t ignored = ::write(_wake, &one, sizeof one);
    (void)ignored;
    _thread.join();
    ::close(_listen); _listen = -1;
    ::close(_wake); _wake = -1;
}

void MetricsEndpoint::serve() {
    while (!_stopping.load()) {
        pollfd fds[2] = {{_listen, POLLIN, 0}, {_wake, POLLIN, 0}};
        if (::poll(fds, 2, 1000) <= 0 || !(fds[0].revents & POLLIN)) continue;
        const int client = ::accept4(_listen, nullptr, nullptr, SOCK_CLOEXEC);
        if (client < 0) continue;
        // One read is enough for a GET; a client that dribbles its request
        // gets two seconds, then the door.
        timeval timeout = {2, 0};
        setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
        setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout);
        char request[1024];
        const ssize_t got = ::recv(client, request, sizeof request - 1, 0);
        std::string body, status = "200 OK", type = "text/plain; version=0.0.4; charset=utf-8";
        if (got > 0) {
            request[got] = '\0';
            if (std::strncmp(request, "GET /metrics", 12) == 0) {
                _render(_context, body);
            } else if (std::strncmp(request, "GET / ", 6) == 0 ||
                       std::strncmp(request, "GET /healthz", 12) == 0) {
                body = "ok\n";
                type = "text/plain";
            } else {
                status = "404 Not Found";
                body = "only /metrics and /healthz here\n";
                type = "text/plain";
            }
        }
        const std::string head = "HTTP/1.1 " + status + "\r\nContent-Type: " + type +
                                 "\r\nContent-Length: " + std::to_string(body.size()) +
                                 "\r\nConnection: close\r\n\r\n";
        const std::string response = head + body;
        size_t sent = 0;
        while (sent < response.size()) {
            const ssize_t n = ::send(client, response.data() + sent, response.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) break;
            sent += (size_t)n;
        }
        ::close(client);
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

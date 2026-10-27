// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee-mqtt bench - a load generator built on the library's own client.
//
//   bench conn   open N connections and hold them; measure the broker's
//                responsiveness under that load with a few probe clients
//   bench pub    N publishers at a rate; payloads carry id, sequence, time
//   bench sub    N subscribers; latency from the payload's send time, and
//                loss and duplication from its sequence numbers
//
// Every client is a real MqttClient over a real socket. Each thread owns a
// slice of them and one epoll set; a client is polled when its socket is ready
// and, on a rolling sweep, at least every --sweep-ms anyway, so keepalive and
// timers run for clients that have nothing to read.
//
// Linux only: it is the epoll, and the per-source-address port arithmetic
// (--bind) is the Linux kernel's.

#include "../Args.h"
#include "../Common.h"

#include <ModBeeMqtt.h>

#include <cstdio>

#if defined(__linux__)

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace {

// --- Time ---

uint64_t nowNs() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

// The send time IN THE PAYLOAD is wall-clock time, so whatever reads the
// message - a bench subscriber, or a Kafka consumer comparing it with the
// broker's append timestamp - can measure against the same clock. Pacing
// still uses nowNs(), which never jumps.
uint64_t wallNs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

// --- The payload ---
//
// [magic 4][publisher 4][sequence 8][sent ns 8] then padding to --size. The
// sent time is CLOCK_MONOTONIC, so latency is only meaningful when publisher
// and subscriber run on the same machine - which is how this rig is used.
const uint32_t kMagic = 0x4D42424Eu;   // "MBBN"
const size_t kHeader = 24;

void putU32(uint8_t* p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i)); }
void putU64(uint8_t* p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i)); }
uint32_t getU32(const uint8_t* p) { uint32_t v = 0; for (int i = 0; i < 4; i++) v |= (uint32_t)p[i] << (8 * i); return v; }
uint64_t getU64(const uint8_t* p) { uint64_t v = 0; for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i); return v; }

// --- Options ---

struct Options {
    std::string mode;
    std::string host = "127.0.0.1";
    uint16_t port = 1883;
    long long clients = 1000;
    long long threads = 4;
    long long connectRate = 5000;        // new connections per second, all threads
    long long duration = 30;             // seconds to hold or run, after connecting
    long long keepAlive = 60;
    long long sweepMs = 1000;
    long long probes = 4;
    long long probeIntervalMs = 100;
    long long rate = 1;                  // messages per second per publisher
    long long size = 64;
    long long qos = 0;
    long long rssPid = 0;
    std::string topic = "bench";
    std::string idPrefix = "bench";
    std::vector<std::string> bindAddresses;
    MqttVersion version = MqttVersion::V5;
    bool json = false;
    bool lean = false;
    bool tls = false;                    // dial TLS; the broker is not verified unless pinned
    std::string pin;                     // the broker's key fingerprint, with tls
};

// "127.0.0.2-127.0.0.12" or "10.0.0.5,10.0.0.6" -> a list of addresses.
bool parseBind(const std::string& text, std::vector<std::string>& out, std::string& error) {
    size_t start = 0;
    while (start <= text.size()) {
        const size_t comma = text.find(',', start);
        const std::string item = text.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        const size_t dash = item.find('-');
        if (dash == std::string::npos) {
            if (!item.empty()) out.push_back(item);
        } else {
            struct in_addr first, last;
            const std::string a = item.substr(0, dash), b = item.substr(dash + 1);
            if (inet_pton(AF_INET, a.c_str(), &first) != 1 || inet_pton(AF_INET, b.c_str(), &last) != 1) {
                error = "--bind range must be two IPv4 addresses: '" + item + "'";
                return false;
            }
            const uint32_t lo = ntohl(first.s_addr), hi = ntohl(last.s_addr);
            if (hi < lo || hi - lo > 4096) {
                error = "--bind range is backwards or larger than 4096 addresses: '" + item + "'";
                return false;
            }
            for (uint32_t address = lo; address <= hi; address++) {
                struct in_addr one;
                one.s_addr = htonl(address);
                char buffer[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &one, buffer, sizeof buffer);
                out.push_back(buffer);
            }
        }
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return true;
}

long readRssKb(long long pid) {
    if (pid <= 0) return -1;
    char path[64];
    std::snprintf(path, sizeof path, "/proc/%lld/status", pid);
    FILE* file = std::fopen(path, "r");
    if (file == nullptr) return -1;
    char line[256];
    long kb = -1;
    while (std::fgets(line, sizeof line, file)) {
        if (std::strncmp(line, "VmRSS:", 6) == 0) { kb = std::strtol(line + 6, nullptr, 10); break; }
    }
    std::fclose(file);
    return kb;
}

// --- Shared results ---

struct Shared {
    std::atomic<bool> stop{false};
    std::atomic<long long> connected{0};
    std::atomic<long long> connectFailures{0};
    std::atomic<long long> dropped{0};          // connections lost after coming up
    std::atomic<long long> sent{0};
    std::atomic<long long> acked{0};
    std::atomic<long long> refused{0};          // a publish concluded with anything but Ok
    std::atomic<long long> queueFull{0};        // the client's own bounded queue said no
    std::atomic<long long> unanswered{0};       // still awaiting an acknowledgement when the run ended
    std::atomic<long long> received{0};
    // Why connections failed or dropped, by MqttResult. A bench that only
    // says "28,618 failed" sends nobody anywhere.
    std::atomic<long long> reasons[256];
    Shared() { for (std::atomic<long long>& r : reasons) r = 0; }

    std::mutex lock;
    std::vector<uint32_t> latencyUs;            // merged from threads at the end
    std::vector<uint32_t> probeUs;
    // Per publisher: lowest and highest sequence seen, and how many distinct.
    struct Seen { uint64_t lo = UINT64_MAX; uint64_t hi = 0; std::vector<uint64_t> bits; uint64_t unique = 0; uint64_t dups = 0; };
    std::vector<Seen> seen;
};

// --- One client ---

enum class Role { Idle, Probe, Publisher, Subscriber };

struct BenchClient;

struct Worker {
    int index = 0;
    int epoll = -1;
    const Options* options = nullptr;
    Shared* shared = nullptr;
    std::vector<std::unique_ptr<BenchClient>> clients;
    std::vector<uint32_t> latencyUs;
    std::vector<uint32_t> probeUs;
    // Clients still connecting: polled every pass, because the TCP handshake
    // completing wakes no EPOLLIN, and the CONNECT goes out on the next poll.
    std::vector<BenchClient*> pending;
};

struct BenchClient {
    MqttTcpTransport transport;
#if MQTT_TLS_SOCKETS
    std::unique_ptr<MqttTlsTransport> tls;   // only with --tls
#endif
    MqttTransport& wire() {
#if MQTT_TLS_SOCKETS
        if (tls) return *tls;
#endif
        return transport;
    }
    MqttClient client;
    Worker* worker = nullptr;
    Role role = Role::Idle;
    uint32_t id = 0;
    int registeredFd = -1;
    bool everConnected = false;
    bool finished = false;                       // dropped or failed; no longer polled
    uint64_t sequence = 0;
    uint64_t nextSendNs = 0;
    uint64_t probeSentNs = 0;
    bool probeOutstanding = false;
    uint32_t outstanding = 0;                    // publishes awaiting their acknowledgement
    bool leaving = false;                        // past the end of the run: outcomes not counted
    std::vector<uint8_t> payload;
    std::string topic;
    // Subscriber-side: per publisher, what this subscriber has seen. Each
    // subscriber keeps its own, because each one is owed every message - a
    // shared table would count the second subscriber's copy as a duplicate.
    std::vector<Shared::Seen> seen;
};

void onPublishDone(void* context, MqttResult result, uint16_t) {
    BenchClient* bench = static_cast<BenchClient*>(context);
    if (bench == nullptr) return;
    Shared* shared = bench->worker->shared;
    if (bench->role == Role::Probe) {
        // The probe measures the broker's answer time, whatever the answer:
        // with nobody subscribed a 5.0 broker says 0x10, and that is still a
        // round trip through its whole publish path.
        bench->worker->probeUs.push_back((uint32_t)((nowNs() - bench->probeSentNs) / 1000));
        bench->probeOutstanding = false;
        return;
    }
    if (bench->outstanding > 0) bench->outstanding--;
    // Concluded by the disconnect at the end of the run, not by the broker:
    // already counted as unanswered, and not a refusal.
    if (bench->leaving) return;
    if (result == MqttResult::Ok) shared->acked++;
    else shared->refused++;
}

void onBenchMessage(void* context, const MqttMessage& message) {
    BenchClient* bench = static_cast<BenchClient*>(context);
    if (bench == nullptr || message.payload.length < kHeader) return;
    const uint8_t* p = message.payload.data;
    if (getU32(p) != kMagic) return;
    const uint32_t publisher = getU32(p + 4);
    const uint64_t sequence = getU64(p + 8);
    const uint64_t sent = getU64(p + 16);
    Worker* worker = bench->worker;
    worker->shared->received++;
    const uint64_t now = wallNs();
    if (now > sent) worker->latencyUs.push_back((uint32_t)std::min<uint64_t>((now - sent) / 1000, UINT32_MAX));

    if (bench->seen.size() <= publisher) bench->seen.resize(publisher + 1);
    Shared::Seen& seen = bench->seen[publisher];
    // Sequences are dense from 0 per publisher, so a bitset indexed by
    // sequence counts distinct arrivals and catches duplicates exactly.
    const size_t word = (size_t)(sequence / 64);
    if (seen.bits.size() <= word) seen.bits.resize(word + 1, 0);
    const uint64_t bit = 1ull << (sequence % 64);
    if (seen.bits[word] & bit) {
        seen.dups++;
    } else {
        seen.bits[word] |= bit;
        seen.unique++;
    }
    if (sequence < seen.lo) seen.lo = sequence;
    if (sequence > seen.hi) seen.hi = sequence;
}

void registerFd(Worker& worker, BenchClient& bench) {
    const int fd = bench.wire().waitHandle();
    if (fd < 0 || fd == bench.registeredFd) return;
    struct epoll_event event;
    std::memset(&event, 0, sizeof event);
    event.events = EPOLLIN;
    event.data.ptr = &bench;
    if (epoll_ctl(worker.epoll, EPOLL_CTL_ADD, fd, &event) == 0 || errno == EEXIST) {
        bench.registeredFd = fd;
    }
}

// Polls one client and does whatever its role wants done now.
void service(Worker& worker, BenchClient& bench, uint64_t now) {
    if (bench.finished) return;
    bench.client.poll();
    registerFd(worker, bench);

    Shared& shared = *worker.shared;
    const bool connected = bench.client.isConnected();
    if (connected && !bench.everConnected) {
        bench.everConnected = true;
        shared.connected++;
        if (bench.role == Role::Subscriber) {
            bench.client.subscribe(bench.topic.c_str(), (uint8_t)worker.options->qos);
        }
    }
    if (!connected && bench.client.state() == MqttState::Idle) {
        // Concluded and not coming back: autoReconnect is off, so a bench run
        // reports its losses instead of hiding them behind reconnects.
        bench.finished = true;
        if (bench.everConnected) { shared.dropped++; shared.connected--; }
        else shared.connectFailures++;
        shared.reasons[(uint8_t)bench.client.status().lastError]++;
        return;
    }
    if (!connected) return;

    if (bench.role == Role::Probe && !bench.probeOutstanding && now >= bench.nextSendNs) {
        bench.probeSentNs = now;
        bench.probeOutstanding = true;
        bench.nextSendNs = now + (uint64_t)worker.options->probeIntervalMs * 1000000ull;
        static const uint8_t ping[1] = {0};
        if (bench.client.publish(bench.topic.c_str(), ping, 1, MQ_QOS_1, false,
                                 &onPublishDone, &bench) != MqttResult::Queued) {
            bench.probeOutstanding = false;
        }
    }
    if (bench.role == Role::Publisher && now >= bench.nextSendNs) {
        const uint64_t interval = 1000000000ull / (uint64_t)std::max<long long>(1, worker.options->rate);
        // Catch up at most a few messages if the loop fell behind, rather than
        // bursting a backlog that would measure the generator, not the broker.
        int burst = 0;
        while (now >= bench.nextSendNs && burst < 4) {
            uint8_t* p = bench.payload.data();
            putU32(p, kMagic);
            putU32(p + 4, bench.id);
            putU64(p + 8, bench.sequence);
            putU64(p + 16, wallNs());
            const MqttResult queued = bench.client.publishSeries(
                bench.topic.c_str(), p, bench.payload.size(), (uint8_t)worker.options->qos,
                false, &onPublishDone, &bench);
            if (queued == MqttResult::Queued) {
                bench.sequence++;
                shared.sent++;
                if (worker.options->qos > 0) bench.outstanding++;
            } else if (queued == MqttResult::QueueFull) {
                shared.queueFull++;
                break;
            } else {
                shared.refused++;
            }
            bench.nextSendNs += interval;
            burst++;
        }
        if (now > bench.nextSendNs + interval * 8) bench.nextSendNs = now + interval;
        bench.client.poll();
    }
}

void runWorker(Worker& worker, long long firstId, long long count, long long probes) {
    const Options& options = *worker.options;
    Shared& shared = *worker.shared;
    worker.epoll = epoll_create1(EPOLL_CLOEXEC);
    if (worker.epoll < 0) { std::perror("epoll_create1"); return; }

    const size_t payloadSize = std::max<long long>((long long)kHeader, options.size);
    const long long perThreadRate = std::max<long long>(1, options.connectRate / options.threads);
    const uint64_t start = nowNs();
    long long opened = 0;
    const long long total = count + probes;
    size_t sweepCursor = 0;
    std::vector<struct epoll_event> events(1024);

    worker.clients.reserve((size_t)total);
    while (!shared.stop.load(std::memory_order_relaxed)) {
        const uint64_t now = nowNs();

        // Open new connections at the configured rate.
        const long long due = std::min<long long>(total,
            (long long)((double)(now - start) / 1e9 * (double)perThreadRate) + 1);
        while (opened < due) {
            std::unique_ptr<BenchClient> bench(new BenchClient());
            bench->worker = &worker;
            const long long id = firstId + opened;
            bench->id = (uint32_t)id;
            const bool isProbe = opened >= count;
            bench->role = isProbe ? Role::Probe
                        : options.mode == "pub" ? Role::Publisher
                        : options.mode == "sub" ? Role::Subscriber : Role::Idle;
            bench->transport.configure(options.host.c_str(), options.port);
#if MQTT_TLS_SOCKETS
            if (options.tls) {
                bench->tls.reset(new MqttTlsTransport());
                bench->tls->configure(options.host.c_str(), options.port);
                if (!options.pin.empty()) {
                    bench->tls->setPinnedKeyFingerprint(options.pin.c_str());
                } else {
                    bench->tls->setInsecureNoCertificateValidation();
                }
            }
#endif
            if (!options.bindAddresses.empty()) {
                bench->transport.setLocalAddress(
                    options.bindAddresses[(size_t)id % options.bindAddresses.size()].c_str());
            }
            MqttConnectionConfig connection;
            connection.host = options.host;
            connection.port = options.port;
            connection.clientId = options.idPrefix + "-" + std::to_string(id);
            connection.version = options.version;
            connection.keepAlive = (uint16_t)options.keepAlive;
            MqttClientConfig behaviour;
            behaviour.autoReconnect = false;
            const size_t buffer = std::max<size_t>(256, payloadSize + 128);
            behaviour.receiveBufferSize = buffer;
            behaviour.transmitBufferSize = buffer;
            bench->client.begin(bench->wire(), connection, behaviour);
            if (bench->role == Role::Subscriber) {
                bench->client.onMessage(&onBenchMessage, bench.get());
                bench->topic = options.topic + "/#";
            } else if (bench->role == Role::Probe) {
                bench->topic = options.topic + "/probe/" + std::to_string(id);
            } else {
                bench->topic = options.topic + "/" + std::to_string(id);
            }
            bench->payload.assign(payloadSize, 0);
            bench->nextSendNs = now;
            bench->client.connect();
            BenchClient* raw = bench.get();
            worker.clients.push_back(std::move(bench));
            worker.pending.push_back(raw);
            service(worker, *raw, now);
            opened++;
        }

        // The ones still connecting, every pass.
        size_t kept = 0;
        for (size_t k = 0; k < worker.pending.size(); k++) {
            BenchClient* bench = worker.pending[k];
            service(worker, *bench, now);
            if (!bench->everConnected && !bench->finished) worker.pending[kept++] = bench;
        }
        worker.pending.resize(kept);

        // Whatever the kernel says is ready.
        const int ready = epoll_wait(worker.epoll, events.data(), (int)events.size(), 1);
        for (int k = 0; k < ready; k++) {
            service(worker, *static_cast<BenchClient*>(events[k].data.ptr), now);
        }

        // And a slice of everyone, so keepalive and connect completion run for
        // clients with nothing to read. The slice is sized so the whole set is
        // visited about once per --sweep-ms; publishers and probes are visited
        // every pass because their work is driven by time, not by input.
        const size_t population = worker.clients.size();
        if (population > 0) {
            const size_t slice = std::max<size_t>(1, population / std::max<long long>(1, options.sweepMs));
            for (size_t k = 0; k < slice && k < population; k++) {
                service(worker, *worker.clients[sweepCursor], now);
                sweepCursor = (sweepCursor + 1) % population;
            }
        }
        if (options.mode == "pub" || probes > 0) {
            for (std::unique_ptr<BenchClient>& bench : worker.clients) {
                if (bench->role == Role::Publisher || bench->role == Role::Probe) {
                    service(worker, *bench, now);
                }
            }
        }
    }

    // THE LAST ACKNOWLEDGEMENTS. Publishing has stopped; what is still in
    // flight is given up to five seconds to be answered, so a broker that
    // holds its PUBACKs (for Kafka, say) is not charged with refusals for
    // the ones the run's end cut off. Whatever is still open is reported as
    // unanswered - separately, because it is neither success nor refusal.
    const uint64_t drainUntil = nowNs() + 5000000000ull;
    for (;;) {
        uint64_t open = 0;
        for (std::unique_ptr<BenchClient>& bench : worker.clients) {
            if (bench->role != Role::Publisher || !bench->client.isConnected()) continue;
            bench->client.poll();
            open += bench->outstanding;
        }
        if (open == 0 || nowNs() >= drainUntil) break;
        struct epoll_event drained[64];
        epoll_wait(worker.epoll, drained, 64, 1);
    }
    for (std::unique_ptr<BenchClient>& bench : worker.clients) {
        shared.unanswered += bench->outstanding;
        bench->leaving = true;
    }

    // Leave cleanly, so the broker sees DISCONNECTs rather than resets - a
    // reset would publish every Will and count as a fault on its side.
    for (std::unique_ptr<BenchClient>& bench : worker.clients) {
        if (bench->client.isConnected()) bench->client.disconnect();
    }
    for (int pass = 0; pass < 50; pass++) {
        for (std::unique_ptr<BenchClient>& bench : worker.clients) bench->client.poll();
    }
    for (std::unique_ptr<BenchClient>& bench : worker.clients) bench->client.end();
    close(worker.epoll);
}

// Nearest-rank: the smallest value with at least p of the samples at or below it.
// =============================================================================
// LEAN CONNECTIONS - for counts the real client cannot reach on one machine
// =============================================================================
//
// MqttClient is about 17 kB with its transport, and emqtt-bench about 16 kB a
// connection: either one at a million connections is more memory than the
// broker being measured. So `bench conn --lean` holds each connection as a
// socket and a few bytes of state, and speaks the minimum of real MQTT 3.1.1
// to the broker: CONNECT, wait for CONNACK, then PINGREQ on schedule and read
// what comes back. To the broker it is an ordinary client - which is the
// point; the probes measuring how the broker FEELS are still full MqttClients.

struct LeanConn {
    int fd = -1;
    uint8_t state = 0;       // 0 connecting, 1 awaiting CONNACK, 2 connected, 3 finished
    uint8_t have = 0;        // CONNACK bytes collected so far
    mq_time_t nextPing = 0;
};

size_t encodeConnect311(uint8_t* out, const std::string& id, uint16_t keepAlive) {
    const size_t remaining = 10 + 2 + id.size();
    size_t at = 0;
    out[at++] = 0x10;
    size_t r = remaining;
    do {
        uint8_t b = (uint8_t)(r % 128);
        r /= 128;
        if (r) b |= 0x80;
        out[at++] = b;
    } while (r);
    const uint8_t header[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02};   // level 4, clean session
    std::memcpy(out + at, header, sizeof header);
    at += sizeof header;
    out[at++] = (uint8_t)(keepAlive >> 8);
    out[at++] = (uint8_t)(keepAlive & 0xFF);
    out[at++] = (uint8_t)(id.size() >> 8);
    out[at++] = (uint8_t)(id.size() & 0xFF);
    std::memcpy(out + at, id.data(), id.size());
    return at + id.size();
}

void runLeanWorker(Worker& worker, long long firstId, long long count) {
    const Options& options = *worker.options;
    Shared& shared = *worker.shared;
    const int epoll = epoll_create1(EPOLL_CLOEXEC);
    if (epoll < 0) { std::perror("epoll_create1"); return; }

    struct sockaddr_in broker;
    std::memset(&broker, 0, sizeof broker);
    broker.sin_family = AF_INET;
    broker.sin_port = htons(options.port);
    inet_pton(AF_INET, options.host.c_str(), &broker.sin_addr);

    std::vector<LeanConn> conns((size_t)count);
    const long long perThreadRate = std::max<long long>(1, options.connectRate / options.threads);
    const uint64_t start = nowNs();
    long long opened = 0;
    size_t pingCursor = 0;
    const mq_time_t pingEvery = (mq_time_t)std::max<long long>(1, options.keepAlive) * 750;  // 3/4 of keep alive
    std::vector<struct epoll_event> events(4096);
    uint8_t scratch[512];

    auto finish = [&](LeanConn& c, bool wasConnected, MqttResult reason) {
        if (c.state == 3) return;
        if (c.fd >= 0) ::close(c.fd);
        c.fd = -1;
        c.state = 3;
        if (wasConnected) { shared.dropped++; shared.connected--; }
        else shared.connectFailures++;
        shared.reasons[(uint8_t)reason]++;
    };

    while (!shared.stop.load(std::memory_order_relaxed)) {
        const uint64_t now = nowNs();
        const mq_time_t nowMs = mq_millis();

        const long long due = std::min<long long>(count,
            (long long)((double)(now - start) / 1e9 * (double)perThreadRate) + 1);
        while (opened < due) {
            LeanConn& c = conns[(size_t)opened];
            const long long id = firstId + opened;
            opened++;
            c.fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
            if (c.fd < 0) { c.state = 3; shared.connectFailures++; shared.reasons[(uint8_t)MqttResult::ConnectionRefused]++; continue; }
            if (!options.bindAddresses.empty()) {
                const int one = 1;
#if defined(IP_BIND_ADDRESS_NO_PORT)
                setsockopt(c.fd, IPPROTO_IP, IP_BIND_ADDRESS_NO_PORT, &one, sizeof one);
#endif
                struct sockaddr_in local;
                std::memset(&local, 0, sizeof local);
                local.sin_family = AF_INET;
                inet_pton(AF_INET, options.bindAddresses[(size_t)id % options.bindAddresses.size()].c_str(),
                          &local.sin_addr);
                if (::bind(c.fd, (struct sockaddr*)&local, sizeof local) != 0) {
                    finish(c, false, MqttResult::ConnectionRefused);
                    continue;
                }
            }
            if (::connect(c.fd, (struct sockaddr*)&broker, sizeof broker) != 0 && errno != EINPROGRESS) {
                finish(c, false, MqttResult::ConnectionRefused);
                continue;
            }
            struct epoll_event event;
            std::memset(&event, 0, sizeof event);
            event.events = EPOLLOUT | EPOLLIN;
            event.data.u64 = (uint64_t)(opened - 1);
            epoll_ctl(epoll, EPOLL_CTL_ADD, c.fd, &event);
        }

        const int ready = epoll_wait(epoll, events.data(), (int)events.size(), 5);
        for (int k = 0; k < ready; k++) {
            LeanConn& c = conns[(size_t)events[k].data.u64];
            if (c.state == 3) continue;
            if (c.state == 0 && (events[k].events & (EPOLLOUT | EPOLLERR | EPOLLHUP))) {
                int error = 0;
                socklen_t length = sizeof error;
                getsockopt(c.fd, SOL_SOCKET, SO_ERROR, &error, &length);
                if (error != 0) { finish(c, false, MqttResult::ConnectionRefused); continue; }
                const std::string id = options.idPrefix + "-" + std::to_string(firstId + (long long)events[k].data.u64);
                const size_t n = encodeConnect311(scratch, id, (uint16_t)options.keepAlive);
                if (::send(c.fd, scratch, n, MSG_NOSIGNAL) != (ssize_t)n) { finish(c, false, MqttResult::ConnectionRefused); continue; }
                c.state = 1;
                struct epoll_event event;
                std::memset(&event, 0, sizeof event);
                event.events = EPOLLIN;
                event.data.u64 = events[k].data.u64;
                epoll_ctl(epoll, EPOLL_CTL_MOD, c.fd, &event);
                continue;
            }
            if (events[k].events & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
                const ssize_t got = ::recv(c.fd, scratch, sizeof scratch, 0);
                if (got == 0 || (got < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                    finish(c, c.state == 2, MqttResult::ConnectionLost);
                    continue;
                }
                if (got > 0 && c.state == 1) {
                    // CONNACK is 0x20 0x02 <session present> <return code>.
                    // Read until all four have come; a refusal is a failure.
                    for (ssize_t i = 0; i < got && c.have < 4; i++, c.have++) {
                        if (c.have == 0 && scratch[i] != 0x20) { finish(c, false, MqttResult::ProtocolError); break; }
                        if (c.have == 3 && scratch[i] != 0x00) { finish(c, false, MqttResult::ConnectionRefused); break; }
                    }
                    if (c.state == 1 && c.have >= 4) {
                        c.state = 2;
                        c.nextPing = nowMs + pingEvery;
                        shared.connected++;
                    }
                }
                // Once connected, what arrives is PINGRESP; read and dropped.
            }
        }

        // PINGREQ for whoever is due, a slice at a time.
        const size_t slice = std::max<size_t>(1, conns.size() / 200);
        for (size_t step = 0; step < slice && !conns.empty(); step++) {
            LeanConn& c = conns[pingCursor];
            pingCursor = (pingCursor + 1) % conns.size();
            if (c.state != 2 || mq_isFuture(nowMs, c.nextPing)) continue;
            static const uint8_t ping[2] = {0xC0, 0x00};
            if (::send(c.fd, ping, 2, MSG_NOSIGNAL) != 2 && errno != EAGAIN) {
                finish(c, true, MqttResult::ConnectionLost);
                continue;
            }
            c.nextPing = nowMs + pingEvery;
        }
    }

    // A DISCONNECT each, so the broker counts a clean close rather than a
    // pulled cable.
    static const uint8_t disconnect[2] = {0xE0, 0x00};
    for (LeanConn& c : conns) {
        if (c.fd < 0) continue;
        if (c.state == 2) (void)::send(c.fd, disconnect, 2, MSG_NOSIGNAL);
        ::close(c.fd);
    }
    ::close(epoll);
}

uint32_t percentile(std::vector<uint32_t>& values, double p) {
    if (values.empty()) return 0;
    size_t rank = (size_t)std::ceil(p * (double)values.size());
    if (rank == 0) rank = 1;
    const size_t index = std::min(values.size() - 1, rank - 1);
    std::nth_element(values.begin(), values.begin() + (long)index, values.end());
    return values[index];
}

const std::vector<ArgSpec>& benchSpecs() {
    static const std::vector<ArgSpec> specs = {
        {"host", 'h', true, "broker address (default 127.0.0.1)"},
        {"port", 'p', true, "broker port (default 1883)"},
        {"clients", 'c', true, "connections to open (default 1000)"},
        {"threads", 'T', true, "generator threads (default 4)"},
        {"connect-rate", 'R', true, "new connections per second, total (default 5000)"},
        {"duration", 'd', true, "seconds to run once everything is connected (default 30)"},
        {"keepalive", 'k', true, "keep alive, seconds (default 60)"},
        {"sweep-ms", 0, true, "visit every idle client at least this often (default 1000)"},
        {"probes", 0, true, "conn: QoS 1 probe clients measuring broker answer time (default 4)"},
        {"probe-interval", 0, true, "conn: ms between probe publishes (default 100)"},
        {"rate", 'r', true, "pub: messages per second per publisher (default 1)"},
        {"size", 's', true, "pub: payload bytes, at least 24 (default 64)"},
        {"qos", 'q', true, "pub/sub: 0, 1 or 2 (default 0)"},
        {"topic", 't', true, "topic prefix (default bench)"},
        {"id-prefix", 0, true, "client identifier prefix (default bench)"},
        {"bind", 'b', true, "source addresses: 127.0.0.2-127.0.0.12 or a,b,c"},
        {"rss-pid", 0, true, "broker process id, to report its resident memory"},
        {"mqtt-version", 'V', true, "5 (default) or 311"},
        {"json", 'j', false, "print the final report as JSON"},
        {"lean", 0, false, "conn: raw MQTT 3.1.1 connections of a few bytes each, for very large counts"},
        {"tls", 0, false, "dial TLS, without verifying the broker unless --pin is given"},
        {"pin", 0, true, "with --tls: the broker's key fingerprint (SHA-256, hex)"},
        {"help", 0, false, "show this help"},
    };
    return specs;
}

} // namespace

int cmdBench(int argc, char** argv) {
    const std::vector<ArgSpec>& specs = benchSpecs();
    if (argc < 3 || std::strcmp(argv[2], "--help") == 0) {
        Args::printHelp("modbee-mqtt bench (conn | pub | sub) [options]", specs);
        return argc < 3 ? 2 : 0;
    }
    Options options;
    options.mode = argv[2];
    if (options.mode != "conn" && options.mode != "pub" && options.mode != "sub") {
        std::fprintf(stderr, "modbee-mqtt bench: mode must be conn, pub or sub\n");
        return 2;
    }
    Args args;
    if (!args.parse(argc, argv, 3, specs)) {
        std::fprintf(stderr, "modbee-mqtt bench: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp("modbee-mqtt bench (conn | pub | sub) [options]", specs);
        return 0;
    }
    long long port = 1883;
    if (!args.getInt("port", 1883, 1, 65535, port) ||
        !args.getInt("clients", 1000, 1, 10000000, options.clients) ||
        !args.getInt("threads", 4, 1, 256, options.threads) ||
        !args.getInt("connect-rate", 5000, 1, 10000000, options.connectRate) ||
        !args.getInt("duration", 30, 1, 86400 * 7, options.duration) ||
        !args.getInt("keepalive", 60, 0, 65535, options.keepAlive) ||
        !args.getInt("sweep-ms", 1000, 1, 600000, options.sweepMs) ||
        !args.getInt("probes", options.mode == "conn" ? 4 : 0, 0, 1000, options.probes) ||
        !args.getInt("probe-interval", 100, 1, 600000, options.probeIntervalMs) ||
        !args.getInt("rate", 1, 1, 1000000, options.rate) ||
        !args.getInt("size", 64, 24, 256 * 1024 * 1024, options.size) ||
        !args.getInt("qos", 0, 0, 2, options.qos) ||
        !args.getInt("rss-pid", 0, 0, 1LL << 31, options.rssPid)) {
        std::fprintf(stderr, "modbee-mqtt bench: %s\n", args.error().c_str());
        return 2;
    }
    options.port = (uint16_t)port;
    options.host = args.get("host", "127.0.0.1");
    options.topic = args.get("topic", "bench");
    options.idPrefix = args.get("id-prefix", ("bench-" + options.mode).c_str());
    options.json = args.has("json");
    options.lean = args.has("lean");
    options.tls = args.has("tls") || args.has("pin");
    options.pin = args.get("pin", "");
#if !MQTT_TLS_SOCKETS
    if (options.tls) {
        std::fprintf(stderr, "modbee-mqtt bench: this build has no TLS engine\n");
        return 2;
    }
#endif
    if (options.tls && (options.lean || args.has("bind"))) {
        std::fprintf(stderr, "modbee-mqtt bench: --tls cannot be combined with --lean or --bind\n");
        return 2;
    }
    const std::string version = args.get("mqtt-version", "5");
    options.version = (version == "311" || version == "3.1.1") ? MqttVersion::V311 : MqttVersion::V5;
    if (args.has("bind")) {
        std::string error;
        if (!parseBind(args.get("bind"), options.bindAddresses, error)) {
            std::fprintf(stderr, "modbee-mqtt bench: %s\n", error.c_str());
            return 2;
        }
    }
    if (options.threads > options.clients) options.threads = options.clients;

    cliInstallSignalHandlers();
    Shared shared;
    const bool leanProbes = options.lean && options.mode == "conn" && options.probes > 0;
    std::vector<Worker> workers((size_t)options.threads + (leanProbes ? 1 : 0));
    std::vector<std::thread> threads;
    const long long perThread = options.clients / options.threads;
    const long long remainder = options.clients % options.threads;
    long long nextId = 0;
    for (long long t = 0; t < options.threads; t++) {
        Worker& worker = workers[(size_t)t];
        worker.index = (int)t;
        worker.options = &options;
        worker.shared = &shared;
        const long long count = perThread + (t < remainder ? 1 : 0);
        const long long probes = options.lean ? 0
            : options.probes / options.threads + (t < options.probes % options.threads ? 1 : 0);
        const long long firstId = nextId;
        nextId += count + probes;
        if (options.lean && options.mode == "conn") {
            // The connections lean, and the probes - which measure how the
            // broker FEELS - as full clients on a worker of their own below.
            threads.emplace_back([&worker, firstId, count] { runLeanWorker(worker, firstId, count); });
        } else {
            threads.emplace_back([&worker, firstId, count, probes] { runWorker(worker, firstId, count, probes); });
        }
    }
    if (leanProbes) {
        Worker& worker = workers.back();
        worker.index = (int)options.threads;
        worker.options = &options;
        worker.shared = &shared;
        const long long firstId = nextId;
        const long long probes = options.probes;
        threads.emplace_back([&worker, firstId, probes] { runWorker(worker, firstId, 0, probes); });
    }

    const long long target = options.clients + options.probes;
    const uint64_t start = nowNs();
    uint64_t allConnectedAt = 0;
    long long lastConnected = 0, lastSent = 0, lastReceived = 0;
    while (!cliStopRequested()) {
        usleep(1000000);
        const uint64_t now = nowNs();
        const long long connected = shared.connected.load();
        const long long failed = shared.connectFailures.load() + shared.dropped.load();
        const long long sent = shared.sent.load(), received = shared.received.load();
        const long rss = readRssKb(options.rssPid);
        if (!options.json) {
            std::printf("t=%5.0fs  connected %lld (+%lld/s)  failed %lld  sent %lld/s  received %lld/s",
                        (double)(now - start) / 1e9, connected, connected - lastConnected, failed,
                        sent - lastSent, received - lastReceived);
            if (rss >= 0) {
                std::printf("  broker RSS %ld MiB", rss / 1024);
                if (connected > 0) std::printf(" (%.0f B/conn)", (double)rss * 1024.0 / (double)connected);
            }
            std::printf("\n");
        }
        lastConnected = connected; lastSent = sent; lastReceived = received;
        if (allConnectedAt == 0 && connected + failed >= target) allConnectedAt = now;
        if (allConnectedAt != 0 && now - allConnectedAt >= (uint64_t)options.duration * 1000000000ull) break;
    }
    const long rssAtEnd = readRssKb(options.rssPid);
    const long long connectedAtEnd = shared.connected.load();
    shared.stop = true;
    for (std::thread& thread : threads) thread.join();

    // Merge.
    // Loss is counted per (subscriber, publisher) pair, over the range of
    // sequences that subscriber saw from that publisher: a gap inside the
    // range is a lost message. Messages before a subscriber's first or after
    // its last are outside what it can judge, which is why a run should start
    // subscribers before publishers and stop publishers first.
    std::vector<uint32_t> latency, probe;
    uint64_t expected = 0, unique = 0, dups = 0;
    for (Worker& worker : workers) {
        latency.insert(latency.end(), worker.latencyUs.begin(), worker.latencyUs.end());
        probe.insert(probe.end(), worker.probeUs.begin(), worker.probeUs.end());
        for (std::unique_ptr<BenchClient>& bench : worker.clients) {
            for (const Shared::Seen& s : bench->seen) {
                if (s.unique == 0) continue;
                expected += s.hi - s.lo + 1;
                unique += s.unique;
                dups += s.dups;
            }
        }
    }

    const double elapsed = (double)(nowNs() - start) / 1e9;
    const uint32_t p50 = percentile(latency, 0.50), p99 = percentile(latency, 0.99),
                   p999 = percentile(latency, 0.999);
    const uint32_t q50 = percentile(probe, 0.50), q99 = percentile(probe, 0.99),
                   q999 = percentile(probe, 0.999);

    if (options.json) {
        std::printf("{\"mode\":\"%s\",\"clients\":%lld,\"threads\":%lld,\"elapsed_s\":%.1f,"
                    "\"connected\":%lld,\"connect_failures\":%lld,\"dropped\":%lld,"
                    "\"sent\":%lld,\"acked\":%lld,\"refused\":%lld,\"queue_full\":%lld,\"unanswered\":%lld,"
                    "\"received\":%lld,\"latency_us\":{\"p50\":%u,\"p99\":%u,\"p999\":%u,\"samples\":%zu},"
                    "\"probe_us\":{\"p50\":%u,\"p99\":%u,\"p999\":%u,\"samples\":%zu},"
                    "\"loss\":{\"expected\":%llu,\"unique\":%llu,\"duplicates\":%llu},"
                    "\"broker_rss_kib\":%ld,\"broker_bytes_per_connection\":%.0f}\n",
                    options.mode.c_str(), options.clients, options.threads, elapsed,
                    connectedAtEnd, shared.connectFailures.load(), shared.dropped.load(),
                    shared.sent.load(), shared.acked.load(), shared.refused.load(),
                    shared.queueFull.load(), shared.unanswered.load(), shared.received.load(),
                    p50, p99, p999, latency.size(),
                    q50, q99, q999, probe.size(),
                    (unsigned long long)expected, (unsigned long long)unique, (unsigned long long)dups,
                    rssAtEnd, connectedAtEnd > 0 && rssAtEnd > 0 ? (double)rssAtEnd * 1024.0 / (double)connectedAtEnd : 0.0);
    } else {
        std::printf("\n%s: %lld clients on %lld threads, %.1fs\n", options.mode.c_str(),
                    options.clients, options.threads, elapsed);
        std::printf("  connected at end   %lld   (failed to connect %lld, dropped %lld)\n",
                    connectedAtEnd, shared.connectFailures.load(), shared.dropped.load());
        if (rssAtEnd >= 0 && connectedAtEnd > 0) {
            std::printf("  broker RSS         %ld MiB  = %.0f bytes per connection\n",
                        rssAtEnd / 1024, (double)rssAtEnd * 1024.0 / (double)connectedAtEnd);
        }
        for (int code = 0; code < 256; code++) {
            const long long count = shared.reasons[code].load();
            if (count > 0) {
                std::printf("  failure reason     %-26s %lld\n", mq_resultName((MqttResult)code), count);
            }
        }
        if (!probe.empty()) {
            std::printf("  probe answer time  p50 %u us  p99 %u us  p999 %u us  (%zu samples)\n",
                        q50, q99, q999, probe.size());
        }
        if (options.mode == "pub") {
            std::printf("  published          %lld  (acked %lld, refused %lld, own queue full %lld, "
                        "unanswered at the end %lld)\n",
                        shared.sent.load(), shared.acked.load(), shared.refused.load(),
                        shared.queueFull.load(), shared.unanswered.load());
        }
        if (options.mode == "sub") {
            std::printf("  received           %lld  (%llu distinct of %llu expected, %llu duplicates)\n",
                        shared.received.load(), (unsigned long long)unique,
                        (unsigned long long)expected, (unsigned long long)dups);
            std::printf("  latency            p50 %u us  p99 %u us  p999 %u us\n", p50, p99, p999);
        }
    }
    return 0;
}

#else  // not Linux

int cmdBench(int, char**) {
    std::fprintf(stderr, "modbee-mqtt bench needs Linux (epoll, and Linux's port allocation)\n");
    return 2;
}

#endif

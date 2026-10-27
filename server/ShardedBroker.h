// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// One MqttBroker per core, on one port, behaving as one broker.
//
// THE SHAPE (internal/CLOUD-PLAN.md, section 5). N shards, each a thread with
// its own MqttBroker, its own listener on the shared port (SO_REUSEPORT, so
// the kernel spreads arrivals) and its own epoll set. The brokers never share
// memory; they talk through one bounded ring for each ordered pair of shards,
// woken by an eventfd. Nothing inside MqttBroker learns that threads exist.
//
// WHERE A SESSION LIVES. On the shard that OWNS its Client Identifier:
// hash(identifier) mod N, or for an identifier this server assigned, the
// shard named in it. A connection that arrives elsewhere is handed to the
// owner at its CONNECT, with the bytes already read, so a session never moves
// and takeover, Wills and in-flight QoS stay single-threaded.
//
// HOW A MESSAGE TRAVELS. The shard it was published on routes it to its own
// sessions, then sends one shared copy to every other shard, each of which
// routes it to its sessions - retained messages are stored on every shard as
// they pass. At QoS 1 and 2 the PUBACK waits until every shard has reported,
// so "no matching subscribers" and "quota exceeded" are true of the whole
// server. A ring that is full counts as that shard refusing the message.
//
// TLS. Every shard can run a TLS listener beside the plain one, on a port of
// its own shared the same way. The shards share one TLS context, so a session
// ticket issued by one shard is honoured by all of them. A TLS connection is
// handed to its owning shard after its handshake and CONNECT, exactly like a
// plain one: the transport, with the decrypted bytes already read, moves.
//
// THE CERTIFICATE CAN BE CHANGED WHILE IT RUNS (replaceTls). The new context
// is built on the caller's thread - so a certificate that will not load is
// refused while the old one is still served - and handed to each shard,
// which swaps it in on its own thread between two passes of its loop. Every
// connection already up keeps the context it shook hands with; only new
// handshakes see the new certificate.
//
// SO CAN THE LISTENERS AND THE LIMITS (changeListeners): a port moved, a
// listener turned on or off, max connections, max packet. New sockets are
// opened on the caller's thread - a port that is taken is refused with
// nothing changed - and each shard swaps them in on its own thread. A
// listener whose port moved keeps its connections (they hold their own
// sockets); one turned off closes them, 0x98 Administrative action. The
// address stays what the server started with.
//
// SHARED SUBSCRIPTIONS. "$share/<name>/<filter>" means one member of the
// group receives each message - one in the whole server, not one per shard.
// Every shard reports its members of every group to a registry; the shard a
// message is published on chooses, once, which shard delivers each matching
// group's copy (rotating over all members, so a shard with three members gets
// three turns in four against a shard with one), and that choice travels with
// the forwarded message. The chosen shard then picks among its own members.
//
// Linux only: SO_REUSEPORT, epoll, eventfd.

#include <ModBeeMqtt.h>

#include "broker/MqttBroker.h"
#include "transport/MqttPollSet.h"
#include "transport/MqttTcpListener.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Something that runs INSIDE every shard alongside its broker - the Kafka pipe
// is one. Every call is made on the shard's own thread with that shard's
// broker, so an extension keeps per-shard state and needs no locks for it.
// Virtual calls only; no RTTI.
class ShardExtension {
public:
    virtual ~ShardExtension() {}

    // Before any thread starts, on the caller's thread. False with `error`
    // refuses to start the server.
    virtual bool begin(size_t shards, std::string& error) = 0;

    // Each shard's broker, once it is begun and before its thread starts or
    // any client is accepted, on the caller's thread. Where a store kept
    // elsewhere is put back (MqttBroker::restoreRetained).
    virtual void prepare(uint32_t shard, MqttBroker& broker) { (void)shard; (void)broker; }

    // A descriptor that becomes readable when the extension has work for this
    // shard - it is added to the shard's epoll set, so a shard asleep in its
    // wait wakes for it. -1 for none.
    virtual int waitHandle(uint32_t shard) = 0;

    // Every descriptor the shard should wait on for this extension: the one
    // above, or - a chain of extensions - each member's.
    virtual void waitHandles(uint32_t shard, std::vector<int>& out) {
        const int handle = waitHandle(shard);
        if (handle >= 0) out.push_back(handle);
    }

    // Every message that ORIGINATED on this shard (a client's PUBLISH, a
    // Will, publishMessage()), after local routing and cross-shard
    // forwarding. `deferral` as for MqttBrokerEgressFn: return how many
    // parties will report through broker.concludeDeferred(deferral, ...).
    virtual uint32_t offer(uint32_t shard, MqttBroker& broker,
                           const MqttBrokerRouted& message, uint64_t deferral) = 0;

    // Once per pass of the shard's loop. Never blocks.
    virtual void service(uint32_t shard, MqttBroker& broker) = 0;

    // Whether the shard must not sleep: something to do that no descriptor
    // will announce.
    virtual bool hasPendingWork(uint32_t shard) = 0;

    // Shutting down, while the broker still has its clients: finish what can
    // be finished, within a bound.
    virtual void stop(uint32_t shard, MqttBroker& broker) = 0;

    // A QoS 1 or 2 message another shard forwarded has just been routed here,
    // and that shard's PUBACK waits for this one's report. Return a hold the
    // report must wait for - what the routing changed is not on disk yet,
    // say - or 0 to report now.
    virtual uint64_t forwardedHold(uint32_t shard) { (void)shard; return 0; }

    // Whether a hold is released. Asked every pass until it is; a shard that
    // is stopping reports regardless.
    virtual bool forwardedSettled(uint32_t shard, uint64_t hold) {
        (void)shard;
        (void)hold;
        return true;
    }
};

// A TLS listener on every shard. Needs a build with a TLS engine (OpenSSL);
// without one, start() fails and says so.
struct ShardedTlsConfig {
    bool enabled = false;
    std::string address;                    // empty: the plain listener's address
    uint16_t port = 8883;                   // 0: an ephemeral port, shared by every shard
    std::string certificateFile, keyFile;   // PEM files...
    std::string certificatePem, keyPem;     // ...or the PEM text itself
    std::string clientCaPem;                // set: mutual TLS, clients must present a certificate it signed
    // With clientCaPem: a certificate is asked for and verified when offered,
    // but a client that offers none is let through (to log in with a
    // password). False: one is required.
    bool clientCertificateOptional = false;
};

struct ShardedBrokerConfig {
    MqttBrokerConfig broker;           // per shard; maxConnections and maxSessions are per shard too
    // The whole server's maxConnections and maxQueuedBytesTotal, as they were
    // asked for, for saying so; 0: the broker's, times the shards.
    size_t maxConnectionsTotal = 0;
    size_t maxQueuedBytesTotal = 0;
    std::string address = "0.0.0.0";
    bool plain = true;                 // the plain-TCP listener; false for TLS only
    uint16_t port = 1883;              // 0: an ephemeral port, shared by every shard
    ShardedTlsConfig tls;
    size_t shards = 0;                 // 0: one per core
    // Messages waiting in one shard's inbox, from all the other shards
    // together - each sender's ring gets an equal share, a power of two, at
    // least 1024. A budget per shard, not per pair: rings per pair of this
    // size would cost N^2 (24 shards: 2.6 GB before a single client).
    size_t mailboxCapacity = 65536;
    bool pinThreads = false;           // one shard per core, pinned
    ShardExtension* extension = nullptr;   // not owned; must outlive stop()
};

struct ShardedBrokerStatus {
    MqttBrokerStatus total;            // summed across shards
    size_t shards = 0;
    uint64_t mailboxFull = 0;          // messages a full ring made a shard refuse
    uint64_t skipped = 0;              // sends avoided: nobody on that shard could want it
    std::vector<size_t> connectionsPerShard;
};

class ShardedBroker {
public:
    ShardedBroker();
    ~ShardedBroker();

    ShardedBroker(const ShardedBroker&) = delete;
    ShardedBroker& operator=(const ShardedBroker&) = delete;

    // Binds every shard's listener and starts the threads. False, with
    // lastError(), if any listener could not be opened.
    bool start(const ShardedBrokerConfig& config);

    // Every client is told 0x8B Server shutting down, every transport is
    // handed back to the shard whose listener it came from, and the threads
    // end. Safe to call twice.
    void stop();

    uint16_t port() const { return _port; }        // 0 if there is no plain listener
    uint16_t tlsPort() const { return _tlsPort; }  // 0 if there is no TLS listener
    const std::string& address() const { return _address; }
    size_t shardCount() const { return _shards.size(); }
    const std::string& lastError() const { return _lastError; }

    // A snapshot, refreshed by each shard about ten times a second.
    ShardedBrokerStatus status() const;

    // --- The TLS material, while running ---

    // What the TLS listeners serve now: the configuration they were started
    // with, as replaceTls() has since changed it. `enabled` is false when
    // there is no TLS listener.
    ShardedTlsConfig servedTls() const;

    // Serve `tls`'s certificate, key, client CA and client-certificate mode
    // to every connection accepted from now on, on every shard. Its address
    // and port are ignored: they are what the sockets are bound to. False,
    // with `error`, and nothing changed, if there is no TLS listener to
    // change or the material will not load. Callable from any thread,
    // including a shard's own.
    bool replaceTls(const ShardedTlsConfig& tls, std::string& error);

    // --- The listeners and the limits, while running ---
    struct ListenerChange {
        bool setPlain = false;          // the plain listener: on `port`, or off
        bool plain = false;
        uint16_t port = 0;
        // The TLS listener: tls.enabled on tls.port, or off. Turning it on
        // needs its material in `tls`, or what it served before; material
        // given while it is on replaces it (replaceTls).
        bool setTls = false;
        ShardedTlsConfig tls;
        size_t maxConnections = 0;      // the whole server's; 0: unchanged
        uint32_t maxPacket = 0;         // bytes; 0: unchanged
        // What a session may hold: messages, bytes, and bytes in every
        // session together (the whole server's). Bytes 0: no cap.
        bool setQueue = false, setQueueBytes = false, setQueueBytesTotal = false;
        size_t queue = 0, queueBytes = 0, queueBytesTotal = 0;
    };
    // All of it, or - false, with `error` - none of it: a port that is taken,
    // material that will not load, more connections than there is room for.
    // Callable from any thread but a shard's own.
    bool changeListeners(const ListenerChange& change, std::string& error);

    size_t maxConnections() const { return _maxConnections; }   // the whole server's
    // How far maxConnections can be raised without a restart: the room every
    // shard kept at the start (MqttBrokerConfig::connectionCapacity).
    size_t maxConnectionsCeiling() const;
    uint32_t maxPacket() const { return _maxPacket; }
    size_t maxQueued() const { return _maxQueued; }                   // messages, per session
    size_t maxQueuedBytes() const { return _maxQueuedBytes; }         // per session; 0: no cap
    size_t maxQueuedBytesTotal() const { return _maxQueuedBytesTotal; }   // the whole server's; 0: no cap

    // Each shard's share of `total` connections (or sessions): a quarter more
    // than an even share, since the hash spreads them evenly on average, not
    // exactly. One shard has nothing to spread: `total` itself.
    static size_t perShard(size_t total, size_t shards);

    // Which shard owns this Client Identifier. Public for tests and tooling.
    static uint32_t ownerOf(const uint8_t* identifier, size_t length, size_t shards);

    struct Shard;
    struct ShareRegistry;

private:
    friend struct Shard;
    std::unique_ptr<ShareRegistry> _shares;
    std::vector<std::unique_ptr<Shard>> _shards;
    std::string _address;
    std::atomic<uint16_t> _port{0};
    std::atomic<uint16_t> _tlsPort{0};
    std::atomic<size_t> _maxConnections{0};
    std::atomic<uint32_t> _maxPacket{0};
    std::atomic<size_t> _maxQueued{0}, _maxQueuedBytes{0}, _maxQueuedBytesTotal{0};
    bool _everyShardListens = true;
    std::mutex _changeLock;                // one change of TLS or listeners at a time
    std::string _lastError;
    std::atomic<bool> _stopping{false};
    std::atomic<size_t> _ended{0};         // shards whose broker has ended
    std::atomic<int64_t> _inFlight{0};     // envelopes posted and not yet handled
    mutable std::mutex _tlsLock;           // guards _tlsServed
    ShardedTlsConfig _tlsServed;
    ShardExtension* _extension = nullptr;
    bool _running = false;

    void run(Shard& shard);
    bool makeTlsContexts(const ShardedTlsConfig& tls, std::vector<void*>& contexts, std::string& error);
    void deliverTlsContexts(const std::vector<void*>& contexts);
    bool openSockets(uint16_t port, const std::string& address, std::vector<int>& sockets, uint16_t& bound,
                     std::string& error);
};

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ShardedBroker.h"

#include "SpscRing.h"

#include "transport/MqttTlsListener.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <deque>
#include <map>
#include <unordered_map>

#include "WakeSignal.h"

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace {

// The token the wake-up signal is registered under in a shard's readiness
// set. The broker ignores tokens that name no connection slot, and this one
// cannot: no slot index is 0xFFFFFFFE.
const uint64_t kWakeToken = 0xFFFFFFFFFFFFFFFEull;
// And the extension's (ShardExtension::waitHandle), for the same reason.
const uint64_t kExtensionToken = 0xFFFFFFFFFFFFFFFDull;

// The connection slots a shard's broker keeps (MqttBroker::begin): its
// maxConnections, or the room asked for beyond it, never past its sessions.
size_t roomOf(const MqttBrokerConfig& config) {
    const size_t sessions = std::max(config.maxSessions, config.maxConnections);
    return std::min(std::max(config.maxConnections, config.connectionCapacity), sessions);
}

// Counters per shard for "might anybody here want this topic": one slot per
// routing-key hash, 256 kB a shard. Collisions only ever make a shard look
// interested when it is not, which costs a wasted send - never a lost one.
const size_t kInterestSlots = 1u << 16;

// Envelopes handled per pass, so a flood from one shard cannot starve this
// shard's own connections; the rest wait a pass, which is microseconds.
const size_t kDrainBudget = 4096;

// One copy of a message, shared by every shard it is sent to and freed by the
// last one to finish with it.
struct SharedMessage {
    std::atomic<uint32_t> refs{0};
    std::string topic;
    std::vector<uint8_t> payload;
    uint8_t qos = MQ_QOS_0;
    bool retain = false;
    MqttOwnedProperties properties;
    uint64_t confirm = 0;          // followed to its subscribers (MqttBrokerConfirm.h)
};

void unref(SharedMessage* message) {
    if (message != nullptr && message->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete message;
    }
}

MqttSpan spanOf(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

MqttSpan spanOf(const std::vector<uint8_t>& bytes) {
    MqttSpan span;
    span.data = bytes.empty() ? nullptr : bytes.data();
    span.length = (uint32_t)bytes.size();
    return span;
}

// One shared group - "$share/<name>/<filter>" - and how many of its members
// each shard holds. The counts are written by the shard that owns them and
// read by every shard choosing where a message goes.
struct ShareGroupEntry {
    std::string key;                                // name, NUL, filter
    std::string filter;
    std::unique_ptr<std::atomic<int32_t>[]> members;
    std::atomic<uint32_t> cursor{0};                // the rotation, server-wide
};

// The groups that exist, as an immutable list. Replaced - never edited - when
// a group appears or empties, which is rare; member counts change in place.
struct ShareSnapshot {
    std::vector<std::shared_ptr<ShareGroupEntry>> groups;
    std::unordered_map<std::string, ShareGroupEntry*> byKey;
};

std::string shareKey(const uint8_t* name, size_t nameLength,
                     const uint8_t* filter, size_t filterLength) {
    std::string key((const char*)name, nameLength);
    key.push_back('\0');
    key.append((const char*)filter, filterLength);
    return key;
}

} // namespace

struct ShardedBroker::ShareRegistry {
    std::mutex lock;
    std::map<std::string, std::shared_ptr<ShareGroupEntry>> byKey;   // under lock
    std::shared_ptr<const ShareSnapshot> snapshot;                    // under lock
    std::atomic<uint64_t> version{0};
    size_t shards = 0;

    void republish() {   // with the lock held
        std::shared_ptr<ShareSnapshot> fresh(new ShareSnapshot());
        for (auto& entry : byKey) {
            fresh->groups.push_back(entry.second);
            fresh->byKey[entry.first] = entry.second.get();
        }
        snapshot = fresh;
        version.fetch_add(1, std::memory_order_release);
    }

    std::shared_ptr<const ShareSnapshot> current() {
        std::lock_guard<std::mutex> guard(lock);
        return snapshot;
    }

    void member(uint32_t shard, const std::string& key, const std::string& filter, int delta) {
        std::lock_guard<std::mutex> guard(lock);
        auto found = byKey.find(key);
        if (found == byKey.end()) {
            if (delta <= 0) return;
            std::shared_ptr<ShareGroupEntry> entry(new ShareGroupEntry());
            entry->key = key;
            entry->filter = filter;
            entry->members.reset(new std::atomic<int32_t>[shards]);
            for (size_t index = 0; index < shards; index++) entry->members[index].store(0);
            found = byKey.insert(std::make_pair(key, entry)).first;
            entry->members[shard].fetch_add(delta, std::memory_order_release);
            republish();
            return;
        }
        // Release: the SUBACK goes out after this, so a message published by
        // somebody who saw that SUBACK is decided with this member counted.
        found->second->members[shard].fetch_add(delta, std::memory_order_release);
        int64_t total = 0;
        for (size_t index = 0; index < shards; index++) {
            total += found->second->members[index].load(std::memory_order_relaxed);
        }
        if (total <= 0) {
            byKey.erase(found);
            republish();
        }
    }
};

// What one shard says to another.
struct Envelope {
    enum Kind : uint8_t {
        Forward,     // route this message to your sessions; conclude if deferral != 0
        Conclude,    // your forwarded message's outcome on my shard
        HandOff,     // a connection whose session you own, with its bytes so far
        Release,     // you lent me this transport; it is finished
    };
    Kind kind = Forward;
    uint32_t from = 0;
    uint64_t deferral = 0;
    size_t matched = 0;
    size_t dropped = 0;
    SharedMessage* message = nullptr;
    MqttTransport* transport = nullptr;
    MqttListener* listener = nullptr;
    std::vector<uint8_t>* bytes = nullptr;
    // Forward: the shared groups (name, NUL, filter) the receiver delivers
    // this message to. Owned by the envelope; null for none.
    std::vector<std::string>* groups = nullptr;
};

// The "listener" an adopted connection points back to. A connection handed
// from shard A to shard B still belongs to A's listener pool, and only A's
// thread may touch that pool - so when B's broker finishes with it and calls
// release(), this posts the transport home rather than releasing it here.
class RemoteRelease : public MqttListener {
public:
    RemoteRelease(ShardedBroker::Shard* holder, uint32_t origin, MqttListener* listener)
        : _holder(holder), _origin(origin), _listener(listener) {}
    bool begin() override { return true; }
    void end() override {}
    MqttTransport* accept() override { return nullptr; }
    void release(MqttTransport* transport) override;
    bool isListening() const override { return false; }
    MqttListener* listener() const { return _listener; }

private:
    ShardedBroker::Shard* _holder;
    uint32_t _origin;
    MqttListener* _listener;
};

struct ShardedBroker::Shard {
    ShardedBroker* server = nullptr;
    uint32_t index = 0;
    MqttBrokerConfig config;
    char prefix[24] = {0};
    MqttBroker broker;
    MqttTcpListener listener;
#if MQTT_TLS_SOCKETS
    MqttTlsListener tlsListener;
#endif
    MqttPollSet readiness;
    WakeSignal wake;
    std::thread thread;

    // A TLS context waiting to be swapped in (ShardedBroker::replaceTls), and
    // whether there is one - so the loop's check is one atomic load.
    std::mutex tlsSwapLock;
    void* tlsPending = nullptr;
    std::atomic<bool> tlsSwap{false};
    void takeTlsContext() {
        if (!tlsSwap.load(std::memory_order_acquire)) return;
        void* context = nullptr;
        {
            std::lock_guard<std::mutex> guard(tlsSwapLock);
            context = tlsPending;
            tlsPending = nullptr;
            tlsSwap.store(false, std::memory_order_release);
        }
#if MQTT_TLS_SOCKETS
        if (context != nullptr) tlsListener.useContext(context);
#else
        (void)context;   // a build without a TLS engine has no listener to give it to
#endif
    }

    // Changes to its listeners and limits (ShardedBroker::changeListeners),
    // made on its own thread between two passes of its loop, in order. A
    // socket is -1 for none: the listener is off, or this shard does not
    // listen.
    struct Change {
        bool plain = false, plainOff = false;
        int plainSocket = -1;
        uint16_t plainPort = 0;
        bool tls = false, tlsOff = false;
        int tlsSocket = -1;
        uint16_t tlsPort = 0;
        size_t maxConnections = 0;
        uint32_t maxPacket = 0;
        bool setQueue = false;              // all three, each shard's share of the total
        size_t queue = 0, queueBytes = 0, queueBytesTotal = 0;
    };
    std::mutex changeLock;
    std::deque<Change> changes;
    std::atomic<bool> changed{false};
    void takeChanges();
    // Every listener a connection on this shard may have been lent by: its
    // own, and each other shard's, through the proxy it is released to.
    std::vector<const MqttListener*> lenders(bool tls) const;

    // inbox[from]: the ring shard `from` writes and this shard reads.
    std::vector<std::unique_ptr<SpscRing<Envelope>>> inbox;
    // proxies[origin]: where this shard's adopted connections are released,
    // one for each listener on shard `origin` - a transport goes back to the
    // pool it came from.
    std::vector<std::vector<std::unique_ptr<RemoteRelease>>> proxies;
    RemoteRelease* proxyFor(uint32_t origin, MqttListener* listener) const {
        for (const std::unique_ptr<RemoteRelease>& proxy : proxies[origin]) {
            if (proxy->listener() == listener) return proxy.get();
        }
        return nullptr;
    }
    void endListeners() {
        listener.end();
#if MQTT_TLS_SOCKETS
        tlsListener.end();
#endif
    }
    std::vector<uint8_t> wakePending;
    // Envelopes this shard could not post because the ring was full and that
    // must not be lost - conclusions, hand-offs and releases. Retried every
    // pass. A forwarded MESSAGE is never parked here: a full ring is reported
    // as that shard refusing it.
    std::deque<std::pair<uint32_t, Envelope>> overflow;
    // Reports to a publishing shard that wait for this shard's extension -
    // a session store committing what the message queued - in the order they
    // were made, with the hold each waits for.
    struct Held {
        uint32_t to;
        Envelope reply;
        uint64_t hold;
    };
    // Not work of the shard's own: the extension's wait handle wakes the
    // shard when a hold is released.
    std::deque<Held> held;
    void releaseHeld(bool all);

    std::atomic<uint64_t> mailboxFull{0};
    std::atomic<uint64_t> skipped{0};       // messages not sent because nobody there could want them

    // INTEREST: per routing key, how many subscriptions this shard holds
    // under it. Written only by this shard (as its sessions subscribe and
    // unsubscribe) and read by every other shard deciding where to send.
    std::unique_ptr<std::atomic<int32_t>[]> interest;
    static void onRouteKey(void* context, uint32_t key, int delta) {
        Shard& shard = *static_cast<Shard*>(context);
        // Release: the SUBACK goes out after this, so a publisher who acts on
        // that SUBACK reaches a shard that already shows the interest.
        shard.interest[key & (kInterestSlots - 1)].fetch_add(delta, std::memory_order_release);
    }
    bool mightWant(const std::vector<uint32_t>& keys) const {
        for (uint32_t key : keys) {
            if (interest[key & (kInterestSlots - 1)].load(std::memory_order_acquire) > 0) return true;
        }
        return false;
    }
    std::vector<uint32_t> topicKeys;        // scratch for onEgress

    // SHARED GROUPS. `decisions` holds, for the message being published
    // here, which shard delivers each group: made when route() first asks
    // the gate, completed in onEgress for groups with no member here, and
    // cleared there. While a forwarded message is routed, `forwarded` is the
    // list of groups it was sent here for, and the gate answers from it.
    struct ShareDecision {
        const ShareGroupEntry* group;
        uint32_t shard;
    };
    std::vector<ShareDecision> decisions;
    std::shared_ptr<const ShareSnapshot> shares;
    uint64_t sharesVersion = UINT64_MAX;
    bool forwarding = false;
    const std::vector<std::string>* forwarded = nullptr;
    std::vector<int32_t> memberScratch;

    void refreshShares() {
        const uint64_t version = server->_shares->version.load(std::memory_order_acquire);
        if (version == sharesVersion) return;
        shares = server->_shares->current();
        sharesVersion = version;
    }

    // Which shard delivers this group's copy: the next member in a rotation
    // over every member on every shard. kNoShard if it has none left.
    static const uint32_t kNoShard = 0xFFFFFFFFu;
    uint32_t choose(ShareGroupEntry& group) {
        const size_t shards = server->_shards.size();
        memberScratch.resize(shards);
        uint64_t total = 0;
        for (size_t at = 0; at < shards; at++) {
            const int32_t count = group.members[at].load(std::memory_order_acquire);
            memberScratch[at] = count > 0 ? count : 0;
            total += (uint64_t)memberScratch[at];
        }
        if (total == 0) return kNoShard;
        uint64_t turn = group.cursor.fetch_add(1, std::memory_order_relaxed) % total;
        for (size_t at = 0; at < shards; at++) {
            if (turn < (uint64_t)memberScratch[at]) return (uint32_t)at;
            turn -= (uint64_t)memberScratch[at];
        }
        return kNoShard;
    }

    const ShareDecision* decided(const ShareGroupEntry* group) const {
        for (const ShareDecision& decision : decisions) {
            if (decision.group == group) return &decision;
        }
        return nullptr;
    }

    static void onShareMember(void* context, const std::string& name,
                              const std::string& filter, int delta);
    static bool onShareGate(void* context, const MqttSpan& name, const MqttSpan& filter);
    mutable std::mutex statusLock;
    MqttBrokerStatus snapshot;
    size_t connections = 0;
    mq_time_t snapshotAt = 0;

    // --- Posting ---

    bool post(uint32_t to, const Envelope& envelope) {
        Shard& target = *server->_shards[to];
        // COUNTED BEFORE IT IS VISIBLE. Counting after the push left a window
        // in which the envelope sat in a ring while the count read zero - and
        // a shard shutting down that saw zero would leave, with a transport
        // in its inbox that nobody would ever release.
        server->_inFlight.fetch_add(1, std::memory_order_acq_rel);
        if (!target.inbox[index]->push(envelope)) {
            server->_inFlight.fetch_sub(1, std::memory_order_acq_rel);
            return false;
        }
        wakePending[to] = 1;
        return true;
    }

    // For envelopes that must arrive: parked and retried if the ring is full.
    void postReliably(uint32_t to, const Envelope& envelope) {
        if (!overflow.empty() || !post(to, envelope)) {
            overflow.push_back(std::make_pair(to, envelope));
        }
    }

    void retryOverflow() {
        while (!overflow.empty()) {
            if (!post(overflow.front().first, overflow.front().second)) return;
            overflow.pop_front();
        }
    }

    // One eventfd write per destination per pass, however many envelopes
    // went to it - a write per message would be a system call per message
    // per shard.
    void flushWakes() {
        for (size_t to = 0; to < wakePending.size(); to++) {
            if (!wakePending[to]) continue;
            wakePending[to] = 0;
            server->_shards[to]->wake.signal();
        }
    }

    bool inboxEmpty() const {
        for (size_t from = 0; from < inbox.size(); from++) {
            if (from != index && !inbox[from]->empty()) return false;
        }
        return true;
    }

    // --- The broker's hooks ---

    static uint32_t onEgress(void* context, const MqttBrokerRouted& routed, uint64_t deferral);
    uint32_t forwardToShards(const MqttBrokerRouted& routed, uint64_t deferral);
    static int onPlace(void* context, const MqttSpan& clientId);
    static void onHandOff(void* context, int target, MqttTransport& transport,
                          MqttListener* origin, const uint8_t* bytes, size_t length);

    void drain(bool stopping);
    void refreshSnapshot(mq_time_t now);
};

void RemoteRelease::release(MqttTransport* transport) {
    Envelope envelope;
    envelope.kind = Envelope::Release;
    envelope.from = _holder->index;
    envelope.transport = transport;
    envelope.listener = _listener;
    _holder->postReliably(_origin, envelope);
}

// =============================================================================
// PLACEMENT
// =============================================================================

uint32_t ShardedBroker::ownerOf(const uint8_t* identifier, size_t length, size_t shards) {
    if (shards <= 1) return 0;
    // An identifier THIS server assigned names its shard: "auto-s<k>-...".
    static const char kAssigned[] = "auto-s";
    const size_t prefix = sizeof kAssigned - 1;
    if (length > prefix && std::memcmp(identifier, kAssigned, prefix) == 0) {
        uint32_t shard = 0;
        size_t at = prefix;
        while (at < length && identifier[at] >= '0' && identifier[at] <= '9') {
            shard = shard * 10 + (uint32_t)(identifier[at] - '0');
            at++;
        }
        if (at > prefix && at < length && identifier[at] == '-' && shard < shards) return shard;
    }
    uint32_t hash = 2166136261u;      // FNV-1a
    for (size_t at = 0; at < length; at++) {
        hash ^= identifier[at];
        hash *= 16777619u;
    }
    return hash % (uint32_t)shards;
}

int ShardedBroker::Shard::onPlace(void* context, const MqttSpan& clientId) {
    Shard& shard = *static_cast<Shard*>(context);
    // An empty identifier is assigned here, with this shard's prefix, so it is
    // this shard's by construction.
    if (clientId.length == 0) return -1;
    const uint32_t owner = ownerOf(clientId.data, clientId.length, shard.server->_shards.size());
    return owner == shard.index ? -1 : (int)owner;
}

void ShardedBroker::Shard::onHandOff(void* context, int target, MqttTransport& transport,
                                     MqttListener* origin, const uint8_t* bytes,
                                     size_t length) {
    Shard& shard = *static_cast<Shard*>(context);
    Envelope envelope;
    envelope.kind = Envelope::HandOff;
    envelope.from = shard.index;
    envelope.transport = &transport;
    envelope.listener = origin;
    envelope.bytes = new std::vector<uint8_t>(bytes, bytes + length);
    shard.postReliably((uint32_t)target, envelope);
}

// =============================================================================
// SHARED GROUPS ACROSS SHARDS
// =============================================================================

void ShardedBroker::Shard::onShareMember(void* context, const std::string& name,
                                         const std::string& filter, int delta) {
    Shard& shard = *static_cast<Shard*>(context);
    const std::string key = shareKey((const uint8_t*)name.data(), name.size(),
                                     (const uint8_t*)filter.data(), filter.size());
    shard.server->_shares->member(shard.index, key, filter, delta);
}

bool ShardedBroker::Shard::onShareGate(void* context, const MqttSpan& name,
                                       const MqttSpan& filter) {
    Shard& shard = *static_cast<Shard*>(context);
    const std::string key = shareKey(name.data, name.length, filter.data, filter.length);

    // A forwarded message: delivered here only if the publishing shard said so.
    if (shard.forwarding) {
        if (shard.forwarded == nullptr) return false;
        for (const std::string& group : *shard.forwarded) {
            if (group == key) return true;
        }
        return false;
    }

    // Published here: decide now, for the whole server, and remember it so
    // onEgress sends the same answer everywhere else.
    shard.refreshShares();
    const ShareGroupEntry* group = nullptr;
    if (shard.shares) {
        auto found = shard.shares->byKey.find(key);
        if (found != shard.shares->byKey.end()) group = found->second;
    }
    // Not registered: only possible in the instant a group is forming. The
    // member is here, so this shard delivers - and nobody else can, because
    // no other shard can decide a group it has not seen.
    if (group == nullptr) return true;
    if (const ShareDecision* decision = shard.decided(group)) {
        return decision->shard == shard.index;
    }
    uint32_t chosen = shard.choose(*const_cast<ShareGroupEntry*>(group));
    if (chosen == kNoShard) chosen = shard.index;
    shard.decisions.push_back(ShareDecision{group, chosen});
    return chosen == shard.index;
}

// =============================================================================
// ROUTING ACROSS SHARDS
// =============================================================================

// Every message that originated on this shard: to the other shards that might
// want it, then to the extension. Both kinds of party conclude the same
// deferral, and the acknowledgement waits for all of them.
uint32_t ShardedBroker::Shard::onEgress(void* context, const MqttBrokerRouted& routed,
                                        uint64_t deferral) {
    Shard& shard = *static_cast<Shard*>(context);
    uint32_t parties = shard.forwardToShards(routed, deferral);
    if (ShardExtension* extension = shard.server->_extension) {
        parties += extension->offer(shard.index, shard.broker, routed, deferral);
    }
    return parties;
}

uint32_t ShardedBroker::Shard::forwardToShards(const MqttBrokerRouted& routed, uint64_t deferral) {
    Shard& shard = *this;
    const size_t shards = shard.server->_shards.size();
    if (shards <= 1) return 0;

    // WHO MIGHT WANT IT. A shard none of whose subscriptions is filed under
    // any of this topic's keys cannot have a matching one (the keys are the
    // routing index's own, which only ever over-approximate), so it is not
    // sent the message and is not waited for. A RETAINED message goes to
    // every shard regardless: a subscriber that has not arrived yet will
    // want it from whichever shard it lands on.
    shard.topicKeys.clear();
    MqttRouteIndex::forEachTopicKey(routed.topic, [](void* context, uint32_t key) {
        static_cast<std::vector<uint32_t>*>(context)->push_back(key);
    }, &shard.topicKeys);

    // SHARED GROUPS: every group on the server whose filter matches, decided
    // now if route() did not already (it asks only about groups with a
    // member here), and each shard told which groups it delivers.
    std::vector<std::vector<std::string>*> groupsFor(shards, nullptr);
    shard.refreshShares();
    if (shard.shares && !shard.shares->groups.empty()) {
        for (const std::shared_ptr<ShareGroupEntry>& group : shard.shares->groups) {
            const ShareDecision* decision = shard.decided(group.get());
            uint32_t chosen;
            if (decision != nullptr) {
                chosen = decision->shard;
            } else {
                MqttSpan filter;
                filter.data = (const uint8_t*)group->filter.data();
                filter.length = (uint32_t)group->filter.size();
                if (!mq_topicMatches(filter, routed.topic)) continue;
                chosen = shard.choose(*group);
            }
            if (chosen == kNoShard || chosen == shard.index) continue;
            if (groupsFor[chosen] == nullptr) groupsFor[chosen] = new std::vector<std::string>();
            groupsFor[chosen]->push_back(group->key);
        }
    }
    shard.decisions.clear();

    std::vector<uint32_t> targets;
    targets.reserve(shards);
    for (uint32_t to = 0; to < shards; to++) {
        if (to == shard.index) continue;
        // A followed message goes everywhere: each shard says how many of its
        // subscribers there are, even none, so its publisher knows when it
        // has heard from them all.
        if (routed.retain || routed.confirm != 0 || groupsFor[to] != nullptr ||
            shard.server->_shards[to]->mightWant(shard.topicKeys)) {
            targets.push_back(to);
        } else {
            shard.skipped.fetch_add(1, std::memory_order_relaxed);
        }
    }
    if (targets.empty()) return 0;

    // ONE copy for every shard it goes to, built here, freed by the last reader.
    SharedMessage* message = new SharedMessage();
    message->topic.assign((const char*)routed.topic.data, routed.topic.length);
    if (routed.payload.length > 0) {
        message->payload.assign(routed.payload.data, routed.payload.data + routed.payload.length);
    }
    message->qos = routed.qos;
    message->retain = routed.retain;
    message->confirm = routed.confirm;
    if (routed.properties != nullptr) message->properties.copyForForwarding(*routed.properties);
    message->refs.store((uint32_t)targets.size(), std::memory_order_relaxed);

    uint32_t parties = 0;
    for (uint32_t to : targets) {
        Envelope envelope;
        envelope.kind = Envelope::Forward;
        envelope.from = shard.index;
        envelope.deferral = deferral;
        envelope.message = message;
        envelope.groups = groupsFor[to];
        if (shard.post(to, envelope)) {
            parties++;
            continue;
        }
        delete groupsFor[to];
        // THE RING IS FULL, so that shard does not get this message. Said so,
        // not hidden: counted, and - for a PUBLISH that is waiting on its
        // PUBACK - reported as a refusal by that shard, so a publisher whose
        // message reached nobody is told 0x97, not success. (Reported before
        // the broker knows how many parties there are; it waits for both.)
        shard.mailboxFull.fetch_add(1, std::memory_order_relaxed);
        unref(message);
        if (deferral != 0) {
            shard.broker.concludeDeferred(deferral, 1, 1);
            parties++;
        }
    }
    return deferral != 0 ? parties : 0;
}

void ShardedBroker::Shard::drain(bool stopping) {
    wake.drain();
    retryOverflow();

    size_t handled = 0;
    for (size_t from = 0; from < inbox.size() && handled < kDrainBudget; from++) {
        if (from == index) continue;
        Envelope envelope;
        while (handled < kDrainBudget && inbox[from]->pop(envelope)) {
            handled++;
            switch (envelope.kind) {
            case Envelope::Forward: {
                SharedMessage* message = envelope.message;
                if (!stopping) {
                    MqttProperties properties;
                    message->properties.describeTo(properties);
                    MqttBrokerRouted routed;
                    routed.topic = spanOf(message->topic);
                    routed.payload = spanOf(message->payload);
                    routed.qos = message->qos;
                    routed.retain = message->retain;
                    routed.properties = &properties;
                    routed.confirm = message->confirm;
                    size_t dropped = 0;
                    forwarding = true;
                    forwarded = envelope.groups;
                    const size_t matched = broker.publishForwarded(routed, &dropped);
                    forwarding = false;
                    forwarded = nullptr;
                    if (envelope.deferral != 0) {
                        Envelope reply;
                        reply.kind = Envelope::Conclude;
                        reply.from = index;
                        reply.deferral = envelope.deferral;
                        reply.matched = matched;
                        reply.dropped = dropped;
                        // Reported once what it changed here is kept, so the
                        // publisher's PUBACK is a promise every shard made.
                        const uint64_t hold =
                            server->_extension ? server->_extension->forwardedHold(index) : 0;
                        if (hold == 0 && held.empty()) {
                            postReliably(envelope.from, reply);
                        } else {
                            // In order: a report behind a held one waits too.
                            held.push_back(Held{envelope.from, reply, hold});
                        }
                    }
                }
                delete envelope.groups;
                unref(message);
                break;
            }
            case Envelope::Conclude:
                if (!stopping) {
                    broker.concludeDeferred(envelope.deferral, envelope.matched,
                                            envelope.dropped);
                }
                break;
            case Envelope::HandOff: {
                bool adopted = false;
                RemoteRelease* proxy = proxyFor(envelope.from, envelope.listener);
                if (!stopping && proxy != nullptr) {
                    const std::vector<uint8_t>& bytes = *envelope.bytes;
                    adopted = broker.adopt(*envelope.transport, proxy,
                                           bytes.empty() ? nullptr : bytes.data(),
                                           bytes.size());
                }
                delete envelope.bytes;
                if (!adopted) {
                    // Nowhere to put it (a full shard, or shutting down): it goes
                    // home and is closed there, by the only thread that may.
                    Envelope home;
                    home.kind = Envelope::Release;
                    home.from = index;
                    home.transport = envelope.transport;
                    home.listener = envelope.listener;
                    postReliably(envelope.from, home);
                }
                break;
            }
            case Envelope::Release:
                if (envelope.listener != nullptr) envelope.listener->release(envelope.transport);
                break;
            }
            server->_inFlight.fetch_sub(1, std::memory_order_acq_rel);
        }
    }
}

void ShardedBroker::Shard::releaseHeld(bool all) {
    while (!held.empty()) {
        const Held& front = held.front();
        if (!all && front.hold != 0 && server->_extension &&
            !server->_extension->forwardedSettled(index, front.hold)) {
            break;
        }
        postReliably(front.to, front.reply);
        held.pop_front();
    }
}

void ShardedBroker::Shard::refreshSnapshot(mq_time_t now) {
    if (snapshotAt != 0 && mq_elapsed(snapshotAt, now) < 100) return;
    snapshotAt = now;
    std::lock_guard<std::mutex> guard(statusLock);
    snapshot = broker.status();
    connections = snapshot.connections;
}

// =============================================================================
// THE SHARD THREAD
// =============================================================================

void ShardedBroker::run(Shard& shard) {
#if defined(__linux__)
    if (_shards.size() > 0 && shard.server != nullptr) {
        // Named, so a profiler or top -H can tell the shards apart.
        char name[16];
        std::snprintf(name, sizeof name, "mqtt-shard-%u", shard.index);
        pthread_setname_np(pthread_self(), name);
    }
#endif

    while (!_stopping.load(std::memory_order_acquire)) {
        // Before accepting anything this pass, so a connection that arrives
        // after replaceTls() returned is served the new certificate.
        shard.takeTlsContext();
        shard.takeChanges();
        shard.broker.poll();
        shard.drain(false);
        if (_extension) _extension->service(shard.index, shard.broker);
        shard.releaseHeld(false);
        shard.flushWakes();
        shard.refreshSnapshot(mq_millis());
        // The daemon may sleep - poll() never does - but not while there is
        // work no socket will announce: the broker's own, or an inbox with
        // something in it (the eventfd also wakes the wait, for envelopes
        // that arrive during it).
        const bool busy = shard.broker.hasPendingWork() || !shard.inboxEmpty() ||
                          !shard.overflow.empty() ||
                          (_extension && _extension->hasPendingWork(shard.index));
        shard.readiness.wait(busy ? 0 : 20);
    }

    // SHUTTING DOWN, in an order that loses nothing. Stop accepting; end the
    // broker, which tells every client 0x8B and releases every transport -
    // adopted ones by posting them home. Then keep handing envelopes on until
    // every shard has ended and nothing is in flight anywhere, so every
    // transport is released by the shard whose listener lent it.
    // The extension first, while clients are still connected: a held PUBACK
    // that its party can still conclude goes out rather than being lost.
    if (_extension) _extension->stop(shard.index, shard.broker);
    shard.releaseHeld(true);
    shard.endListeners();
    shard.broker.end();
    shard.flushWakes();
    _ended.fetch_add(1, std::memory_order_acq_rel);
    for (;;) {
        shard.drain(true);
        shard.releaseHeld(true);
        shard.flushWakes();
        if (_ended.load(std::memory_order_acquire) == _shards.size() &&
            _inFlight.load(std::memory_order_acquire) == 0 && shard.overflow.empty()) {
            break;
        }
        shard.readiness.wait(5);
    }
    {
        std::lock_guard<std::mutex> guard(shard.statusLock);
        shard.snapshot = shard.broker.status();
        shard.connections = 0;
    }
}

// One bucket every shard draws from: `perSecond` tokens a second, holding a
// second's worth, so a storm starts at once and then goes at the rate.
struct ShardedBroker::ConnectRate : MqttAcceptLimit {
    std::mutex lock;
    std::atomic<uint32_t> perSecond{0};
    double tokens = 0;
    std::chrono::steady_clock::time_point at = std::chrono::steady_clock::now();

    void set(uint32_t rate) {
        std::lock_guard<std::mutex> guard(lock);
        perSecond = rate;
        tokens = rate;
        at = std::chrono::steady_clock::now();
    }
    bool take(mq_time_t& retryInMs) override {
        if (perSecond.load(std::memory_order_relaxed) == 0) return true;
        std::lock_guard<std::mutex> guard(lock);
        const double rate = perSecond.load(std::memory_order_relaxed);
        if (rate == 0) return true;
        const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        tokens = std::min(rate, tokens + std::chrono::duration<double>(now - at).count() * rate);
        at = now;
        if (tokens >= 1) {
            tokens -= 1;
            return true;
        }
        retryInMs = (mq_time_t)std::ceil((1 - tokens) * 1000.0 / rate);
        return false;
    }
    void giveBack() override {
        std::lock_guard<std::mutex> guard(lock);
        const double rate = perSecond.load(std::memory_order_relaxed);
        if (rate != 0) tokens = std::min(rate, tokens + 1);
    }
};

ShardedBroker::ShardedBroker() : _connectRate(new ConnectRate()) {}

uint32_t ShardedBroker::connectRate() const { return _connectRate->perSecond.load(); }

ShardedBroker::~ShardedBroker() { stop(); }

bool ShardedBroker::start(const ShardedBrokerConfig& config) {
    if (_running) return false;
    _connectRate->set(config.connectRate);
    size_t count = config.shards;
    if (count == 0) count = std::thread::hardware_concurrency();
    if (count == 0) count = 1;

    _shards.clear();
    _extension = config.extension;
    if (_extension != nullptr) {
        std::string why;
        if (!_extension->begin(count, why)) {
            _lastError = why;
            return false;
        }
    }
    _shares.reset(new ShareRegistry());
    _shares->shards = count;
    _stopping = false;
    _ended = 0;
    _inFlight = 0;
    for (size_t index = 0; index < count; index++) {
        std::unique_ptr<Shard> shard(new Shard());
        shard->server = this;
        shard->index = (uint32_t)index;
        _shards.push_back(std::move(shard));
    }
    // Each sender's share of an inbox, rounded DOWN to a power of two (the
    // ring would round up, and up is where the memory went).
    size_t perRing = 1024;
    if (count > 1) {
        const size_t share = config.mailboxCapacity / (count - 1);
        while (perRing * 2 <= share) perRing *= 2;
    }
    for (std::unique_ptr<Shard>& shard : _shards) {
        for (size_t from = 0; from < count; from++) {
            shard->inbox.push_back(std::unique_ptr<SpscRing<Envelope>>(
                new SpscRing<Envelope>(from == shard->index ? 2 : perRing)));
        }
        shard->wakePending.assign(count, 0);
        shard->interest.reset(new std::atomic<int32_t>[kInterestSlots]);
        for (size_t slot = 0; slot < kInterestSlots; slot++) shard->interest[slot].store(0);
    }
    for (std::unique_ptr<Shard>& shard : _shards) {
        shard->proxies.resize(count);
        for (size_t origin = 0; origin < count; origin++) {
            shard->proxies[origin].push_back(std::unique_ptr<RemoteRelease>(new RemoteRelease(
                shard.get(), (uint32_t)origin, &_shards[origin]->listener)));
#if MQTT_TLS_SOCKETS
            shard->proxies[origin].push_back(std::unique_ptr<RemoteRelease>(new RemoteRelease(
                shard.get(), (uint32_t)origin, &_shards[origin]->tlsListener)));
#endif
        }
    }

    auto abandon = [this](const std::string& why) {
        _lastError = why;
        for (std::unique_ptr<Shard>& other : _shards) {
            other->endListeners();
            // Each shard's wake-up descriptor, too: a server that failed to
            // start and was asked again leaked one per shard per attempt.
            other->wake.close();
        }
        _shards.clear();
        return false;
    };
    if (!config.plain && !config.tls.enabled) return abandon("no listener: neither plain nor TLS");
#if !MQTT_TLS_SOCKETS
    if (config.tls.enabled) {
        return abandon("TLS was asked for, but this build has no TLS engine (engine: " +
                       std::string(MQTT_TLS_ENGINE_NAME) + ")");
    }
#endif

    // Listeners first, on the calling thread, so a port that cannot be had
    // is reported here rather than discovered by a thread. The first binds
    // (an ephemeral port, if 0 was asked for); the rest join it.
    //
    // WHERE THE KERNEL CANNOT SHARE A PORT between listeners (SO_REUSEPORT
    // balancing is Linux's; Windows has nothing like it) shard 0 alone
    // listens, and the hand-off every shard already does at CONNECT - to the
    // shard that owns the client identifier - spreads the connections.
#if defined(__linux__)
    const bool everyShardListens = true;
#else
    const bool everyShardListens = false;
#endif
    _everyShardListens = everyShardListens;
    uint16_t port = config.port;
    uint16_t tlsPort = config.tls.port;
    for (std::unique_ptr<Shard>& shard : _shards) {
        Shard& s = *shard;
        s.config = config.broker;
        std::snprintf(s.prefix, sizeof s.prefix, "auto-s%u-", s.index);
        s.config.assignedIdPrefix = s.prefix;
        // The pools grow when max connections is raised (changeListeners).
        const size_t pool = s.config.maxConnections;

        const bool listens = everyShardListens || s.index == 0;
        if (config.plain && listens) {
            s.listener.configure(config.address.c_str(), port, pool);
            s.listener.setReusePort(count > 1);
            if (!s.listener.begin()) {
                return abandon("shard " + std::to_string(s.index) + ": " + s.listener.lastError());
            }
            if (port == 0) port = s.listener.boundPort();
        }
#if MQTT_TLS_SOCKETS
        if (config.tls.enabled && listens) {
            const ShardedTlsConfig& tls = config.tls;
            const std::string& address = tls.address.empty() ? config.address : tls.address;
            s.tlsListener.configure(address.c_str(), tlsPort, pool);
            s.tlsListener.setReusePort(count > 1);
            if (s.index == 0) {
                if (!tls.certificateFile.empty()) {
                    s.tlsListener.useCertificateFile(tls.certificateFile.c_str(), tls.keyFile.c_str());
                } else {
                    s.tlsListener.useCertificate(tls.certificatePem.c_str(), tls.keyPem.c_str());
                }
                if (!tls.clientCaPem.empty()) {
                    if (tls.clientCertificateOptional) {
                        s.tlsListener.requestClientCertificate(tls.clientCaPem.c_str());
                    } else {
                        s.tlsListener.requireClientCertificate(tls.clientCaPem.c_str());
                    }
                }
            } else {
                s.tlsListener.shareContextWith(_shards[0]->tlsListener);
            }
            if (!s.tlsListener.begin()) {
                return abandon("shard " + std::to_string(s.index) + " (TLS): " +
                               s.tlsListener.lastError());
            }
            if (tlsPort == 0) tlsPort = s.tlsListener.boundPort();
        }
#endif

        s.broker.begin(s.config);
        s.broker.setAcceptLimit(_connectRate.get());
        if (config.plain && listens) s.broker.addListener(s.listener);
#if MQTT_TLS_SOCKETS
        if (config.tls.enabled && listens) s.broker.addListener(s.tlsListener);
#endif
        if (!s.readiness.beginPersistent()) return abandon("could not create an epoll set");
        s.broker.useReadiness(s.readiness);
        if (!s.wake.open()) return abandon("could not create a wake-up descriptor");
        s.readiness.add(s.wake.handle(), kWakeToken, false);
        if (_extension != nullptr) {
            std::vector<int> handles;
            _extension->waitHandles(s.index, handles);
            for (int handle : handles) s.readiness.add(handle, kExtensionToken, false);
            s.broker.onEgress(&Shard::onEgress, &s);   // one shard or many
        }
        if (count > 1) {
            s.broker.onEgress(&Shard::onEgress, &s);
            s.broker.onPlace(&Shard::onPlace, &Shard::onHandOff, &s);
            s.broker.onRouteKey(&Shard::onRouteKey, &s);
            s.broker.onShareMember(&Shard::onShareMember, &s);
            s.broker.onShareGate(&Shard::onShareGate, &s);
        }
        // AFTER the hooks: what an extension puts back - a session store's
        // subscriptions - must reach the other shards' view of who wants
        // what, or a message published elsewhere would never be sent here.
        if (_extension != nullptr) _extension->prepare(s.index, s.broker);
    }
    _address = config.address;
    _port = config.plain ? port : 0;
    _tlsPort = config.tls.enabled ? tlsPort : 0;
    _maxConnections = config.maxConnectionsTotal ? config.maxConnectionsTotal
                                                 : _shards[0]->broker.config().maxConnections * count;
    _maxPacket = _shards[0]->broker.config().maximumPacketSize;
    _maxQueued = _shards[0]->broker.config().maxQueuedPerSession;
    _maxQueuedBytes = _shards[0]->broker.config().maxQueuedBytesPerSession;
    _maxQueuedBytesTotal = config.maxQueuedBytesTotal ? config.maxQueuedBytesTotal
                                                      : _shards[0]->broker.config().maxQueuedBytesTotal * count;
    {
        std::lock_guard<std::mutex> guard(_tlsLock);
        _tlsServed = config.tls;
        _tlsServed.port = _tlsPort;
    }

    for (std::unique_ptr<Shard>& shard : _shards) {
        Shard* s = shard.get();
        s->thread = std::thread([this, s] { run(*s); });
#if defined(__linux__)
        if (config.pinThreads) {
            cpu_set_t set;
            CPU_ZERO(&set);
            CPU_SET(s->index % std::thread::hardware_concurrency(), &set);
            pthread_setaffinity_np(s->thread.native_handle(), sizeof set, &set);
        }
#endif
    }
    _running = true;
    return true;
}

void ShardedBroker::stop() {
    if (!_running) return;
    _stopping.store(true, std::memory_order_release);
    for (std::unique_ptr<Shard>& shard : _shards) shard->wake.signal();
    for (std::unique_ptr<Shard>& shard : _shards) {
        if (shard->thread.joinable()) shard->thread.join();
    }
    for (std::unique_ptr<Shard>& shard : _shards) shard->wake.close();
    // Sockets a change brought that no pass took.
    for (std::unique_ptr<Shard>& shard : _shards) {
        std::lock_guard<std::mutex> guard(shard->changeLock);
        for (const Shard::Change& change : shard->changes) {
            if (change.plainSocket >= 0) MQ_CLOSE(change.plainSocket);
            if (change.tlsSocket >= 0) MQ_CLOSE(change.tlsSocket);
        }
        shard->changes.clear();
    }
#if MQTT_TLS_SOCKETS
    // A context replaceTls() delivered after the last pass took one.
    for (std::unique_ptr<Shard>& shard : _shards) {
        std::lock_guard<std::mutex> guard(shard->tlsSwapLock);
        MqttTlsListener::dropContext(shard->tlsPending);
        shard->tlsPending = nullptr;
    }
#endif
    _running = false;
}

ShardedTlsConfig ShardedBroker::servedTls() const {
    std::lock_guard<std::mutex> guard(_tlsLock);
    return _tlsServed;
}

bool ShardedBroker::makeTlsContexts(const ShardedTlsConfig& tls, std::vector<void*>& contexts,
                                    std::string& error) {
#if !MQTT_TLS_SOCKETS
    (void)tls;
    (void)contexts;
    error = "this build has no TLS engine";
    return false;
#else
    // Built here, on the caller's thread, from a listener that never opens a
    // socket: it exists to turn settings into contexts. One context per
    // shard on an engine that cannot share one between threads, one shared
    // by every shard on an engine that can - the same arrangement start()
    // makes.
    MqttTlsListener maker;
    if (!tls.certificateFile.empty()) {
        maker.useCertificateFile(tls.certificateFile.c_str(), tls.keyFile.c_str());
    } else {
        maker.useCertificate(tls.certificatePem.c_str(), tls.keyPem.c_str());
    }
    if (!tls.clientCaPem.empty()) {
        if (tls.clientCertificateOptional) maker.requestClientCertificate(tls.clientCaPem.c_str());
        else maker.requireClientCertificate(tls.clientCaPem.c_str());
    }
    for (size_t i = 0; i < _shards.size(); i++) {
        void* context = i == 0 ? nullptr : MqttTlsListener::shareContext(contexts[0]);
        if (context == nullptr) context = maker.makeContext();
        if (context == nullptr) {
            error = maker.lastError();
            for (void* made : contexts) MqttTlsListener::dropContext(made);
            contexts.clear();
            return false;
        }
        contexts.push_back(context);
    }
    return true;
#endif
}

void ShardedBroker::deliverTlsContexts(const std::vector<void*>& contexts) {
#if MQTT_TLS_SOCKETS
    // Each shard woken so an idle one swaps now rather than at its next
    // timeout. A context still waiting from an earlier call was never served
    // and is dropped.
    for (size_t i = 0; i < _shards.size() && i < contexts.size(); i++) {
        Shard& shard = *_shards[i];
        {
            std::lock_guard<std::mutex> guard(shard.tlsSwapLock);
            MqttTlsListener::dropContext(shard.tlsPending);
            shard.tlsPending = contexts[i];
            shard.tlsSwap.store(true, std::memory_order_release);
        }
        shard.wake.signal();
    }
#else
    (void)contexts;
#endif
}

bool ShardedBroker::replaceTls(const ShardedTlsConfig& tls, std::string& error) {
    std::lock_guard<std::mutex> changing(_changeLock);
    if (!_running || _tlsPort == 0) {
        error = "there is no TLS listener running to change - turn it on first (changeListeners)";
        return false;
    }
    std::vector<void*> contexts;
    if (!makeTlsContexts(tls, contexts, error)) return false;
    deliverTlsContexts(contexts);
    std::lock_guard<std::mutex> guard(_tlsLock);
    const std::string address = _tlsServed.address;
    const uint16_t port = _tlsServed.port;
    _tlsServed = tls;
    _tlsServed.enabled = true;
    _tlsServed.address = address;
    _tlsServed.port = port;
    return true;
}

// --- Listeners and limits, while running -------------------------------------

size_t ShardedBroker::perShard(size_t total, size_t shards) {
    if (shards <= 1) return total;
    const size_t even = (total + shards - 1) / shards;
    return even + even / 4 + 16;
}

size_t ShardedBroker::maxConnectionsCeiling() const {
    if (_shards.empty()) return 0;
    const size_t room = roomOf(_shards[0]->config);
    if (_shards.size() == 1) return room;
    // The largest even share whose perShard() fits the room: e + e/4 <= R
    // holds for no e above (4R + 3) / 5.
    if (room <= 16) return 0;
    size_t even = ((room - 16) * 4 + 3) / 5 + 1;
    while (even > 0 && even + even / 4 + 16 > room) even--;
    return even * _shards.size();
}

bool ShardedBroker::openSockets(uint16_t port, const std::string& address, std::vector<int>& sockets,
                                uint16_t& bound, std::string& error) {
    sockets.assign(_shards.size(), -1);
    bound = port;
    for (size_t i = 0; i < _shards.size(); i++) {
        if (!_everyShardListens && i != 0) continue;
        const size_t pool = roomOf(_shards[i]->config);
        sockets[i] = mq_socket::openListeningSocket(address, bound, _shards.size() > 1, (int)pool + 1, bound, error);
        if (sockets[i] < 0) {
            for (int opened : sockets) {
                if (opened >= 0) MQ_CLOSE(opened);
            }
            return false;
        }
    }
    return true;
}

bool ShardedBroker::changeListeners(const ListenerChange& change, std::string& error) {
    std::lock_guard<std::mutex> changing(_changeLock);
    if (!_running) {
        error = "the server is not running";
        return false;
    }
    const ShardedTlsConfig served = servedTls();
    const bool plainOn = _port != 0, tlsOn = _tlsPort != 0;
    const bool plainAfter = change.setPlain ? change.plain : plainOn;
    const bool tlsAfter = change.setTls ? change.tls.enabled : tlsOn;
    if (!plainAfter && !tlsAfter) {
        error = "no listener left: neither plain nor TLS";
        return false;
    }
    size_t perShardLimit = 0;
    if (change.maxConnections != 0) {
        perShardLimit = perShard(change.maxConnections, _shards.size());
        if (perShardLimit > roomOf(_shards[0]->config)) {
            error = "at most " + std::to_string(maxConnectionsCeiling()) +
                    " connections without a restart: the room kept for sessions at the start (max-sessions)";
            return false;
        }
    }
    if (change.maxPacket != 0 && (change.maxPacket < 64 || change.maxPacket > 268435455u)) {
        error = "max packet is 64 to 268435455 bytes";
        return false;
    }
    if (change.setQueue && (change.queue < 1 || change.queue > 1000000)) {
        error = "messages queued per session is 1 to 1000000";
        return false;
    }

    // The TLS material, when TLS is turned on or given new material.
    const bool tlsMaterial = change.setTls && change.tls.enabled &&
                             (!change.tls.certificatePem.empty() || !change.tls.certificateFile.empty());
    ShardedTlsConfig tls = served;
    if (tlsMaterial) {
        const std::string address = served.address;
        tls = change.tls;
        tls.address = address;
    }
    std::vector<void*> contexts;
    if (change.setTls && change.tls.enabled && (tlsMaterial || !tlsOn)) {
        if (!tlsMaterial && tls.certificatePem.empty() && tls.certificateFile.empty()) {
            error = "TLS needs a certificate and its key";
            return false;
        }
        if (!makeTlsContexts(tls, contexts, error)) return false;
    }

    // The sockets, last: what can still fail before anything changes.
    std::vector<int> plainSockets(_shards.size(), -1), tlsSockets(_shards.size(), -1);
    uint16_t plainPort = _port, tlsPort = _tlsPort;
    const bool plainMoves = change.setPlain && change.plain && (!plainOn || change.port != _port || change.port == 0);
    const bool tlsMoves = change.setTls && change.tls.enabled && (!tlsOn || change.tls.port != _tlsPort ||
                                                                  change.tls.port == 0);
    auto dropContexts = [&] {
#if MQTT_TLS_SOCKETS
        for (void* made : contexts) MqttTlsListener::dropContext(made);
#endif
    };
    if (plainMoves && !openSockets(change.port, _address, plainSockets, plainPort, error)) {
        dropContexts();
        return false;
    }
    if (tlsMoves) {
        const std::string& address = served.address.empty() ? _address : served.address;
        if (!openSockets(change.tls.port, address, tlsSockets, tlsPort, error)) {
            for (int opened : plainSockets) {
                if (opened >= 0) MQ_CLOSE(opened);
            }
            dropContexts();
            return false;
        }
    }

    // Nothing can fail from here.
    if (!contexts.empty()) deliverTlsContexts(contexts);
    for (size_t i = 0; i < _shards.size(); i++) {
        Shard& shard = *_shards[i];
        Shard::Change c;
        c.plain = plainMoves || (change.setPlain && !change.plain && plainOn);
        c.plainOff = change.setPlain && !change.plain && plainOn;
        c.plainSocket = plainSockets[i];
        c.plainPort = plainPort;
        c.tls = tlsMoves || (change.setTls && !change.tls.enabled && tlsOn);
        c.tlsOff = change.setTls && !change.tls.enabled && tlsOn;
        c.tlsSocket = tlsSockets[i];
        c.tlsPort = tlsPort;
        c.maxConnections = perShardLimit;
        c.maxPacket = change.maxPacket;
        c.setQueue = change.setQueue || change.setQueueBytes || change.setQueueBytesTotal;
        c.queue = change.setQueue ? change.queue : _maxQueued.load();
        c.queueBytes = change.setQueueBytes ? change.queueBytes : _maxQueuedBytes.load();
        const size_t total = change.setQueueBytesTotal ? change.queueBytesTotal : _maxQueuedBytesTotal.load();
        c.queueBytesTotal = total == 0 ? 0 : (total + _shards.size() - 1) / _shards.size();
        {
            std::lock_guard<std::mutex> guard(shard.changeLock);
            shard.changes.push_back(c);
            shard.changed.store(true, std::memory_order_release);
        }
        shard.wake.signal();
    }
    if (change.setPlain) _port = change.plain ? plainPort : 0;
    if (change.setTls) {
        _tlsPort = change.tls.enabled ? tlsPort : 0;
        std::lock_guard<std::mutex> guard(_tlsLock);
        const std::string address = _tlsServed.address;
        _tlsServed = tls;
        _tlsServed.enabled = change.tls.enabled;
        _tlsServed.address = address;
        _tlsServed.port = _tlsPort;
    }
    if (change.maxConnections != 0) _maxConnections = change.maxConnections;
    if (change.maxPacket != 0) _maxPacket = change.maxPacket;
    if (change.setQueue) _maxQueued = change.queue;
    if (change.setQueueBytes) _maxQueuedBytes = change.queueBytes;
    if (change.setQueueBytesTotal) _maxQueuedBytesTotal = change.queueBytesTotal;
    if (change.setConnectRate) _connectRate->set(change.connectRate);
    return true;
}

void ShardedBroker::Shard::takeChanges() {
    if (!changed.load(std::memory_order_acquire)) return;
    std::deque<Change> now;
    {
        std::lock_guard<std::mutex> guard(changeLock);
        now.swap(changes);
        changed.store(false, std::memory_order_release);
    }
    for (const Change& c : now) {
        if (c.maxPacket != 0) broker.setMaximumPacketSize(c.maxPacket);
        if (c.setQueue) broker.setQueueLimits(c.queue, c.queueBytes, c.queueBytesTotal);
        if (c.maxConnections != 0 && broker.setMaxConnections(c.maxConnections)) {
            listener.grow(c.maxConnections);
#if MQTT_TLS_SOCKETS
            tlsListener.grow(c.maxConnections);
#endif
        }
        const size_t pool = broker.config().maxConnections;
        if (c.plain) {
            broker.removeListener(listener);
            listener.useSocket(c.plainSocket, c.plainPort, pool);
            if (c.plainSocket >= 0) broker.addListener(listener);
            if (c.plainOff) broker.disconnectFrom(lenders(false), 0x98);
        }
#if MQTT_TLS_SOCKETS
        if (c.tls) {
            takeTlsContext();       // turned on: its context came first
            broker.removeListener(tlsListener);
            tlsListener.useSocket(c.tlsSocket, c.tlsPort, pool);
            if (c.tlsSocket >= 0) broker.addListener(tlsListener);
            if (c.tlsOff) broker.disconnectFrom(lenders(true), 0x98);
        }
#endif
    }
}

std::vector<const MqttListener*> ShardedBroker::Shard::lenders(bool tls) const {
    std::vector<const MqttListener*> out;
    for (size_t origin = 0; origin < server->_shards.size(); origin++) {
        const Shard& other = *server->_shards[origin];
#if MQTT_TLS_SOCKETS
        const MqttListener* listening = tls ? (const MqttListener*)&other.tlsListener : &other.listener;
#else
        if (tls) break;
        const MqttListener* listening = &other.listener;
#endif
        if (origin == index) out.push_back(listening);
        else if (const RemoteRelease* proxy = proxyFor((uint32_t)origin, const_cast<MqttListener*>(listening))) {
            out.push_back(proxy);
        }
    }
    return out;
}

ShardedBrokerStatus ShardedBroker::status() const {
    ShardedBrokerStatus out;
    out.shards = _shards.size();
    for (const std::unique_ptr<Shard>& shard : _shards) {
        std::lock_guard<std::mutex> guard(shard->statusLock);
        const MqttBrokerStatus& s = shard->snapshot;
        MqttBrokerStatus& t = out.total;
        t.accepted += s.accepted;
        t.connected += s.connected;
        t.refused += s.refused;
        t.disconnected += s.disconnected;
        t.takeovers += s.takeovers;
        t.published += s.published;
        t.delivered += s.delivered;
        t.dropped += s.dropped;
        t.protocolErrors += s.protocolErrors;
        t.keepAliveTimeouts += s.keepAliveTimeouts;
        t.willsPublished += s.willsPublished;
        t.sessionsExpired += s.sessionsExpired;
        t.retainedDelivered += s.retainedDelivered;
        t.handedOff += s.handedOff;
        t.adopted += s.adopted;
        t.connections += s.connections;
        t.sessions += s.sessions;
        t.queuedMessages += s.queuedMessages;
        t.queuedBytes += s.queuedBytes;
        t.retained = s.retained;     // replicated: every shard holds the same set
        out.mailboxFull += shard->mailboxFull.load(std::memory_order_relaxed);
        out.skipped += shard->skipped.load(std::memory_order_relaxed);
        out.connectionsPerShard.push_back(shard->connections);
    }
    return out;
}

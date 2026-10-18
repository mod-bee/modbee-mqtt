// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ShardedBroker.h"

#include "SpscRing.h"

#include "transport/MqttTlsListener.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <deque>
#include <map>
#include <unordered_map>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif

namespace {

#if defined(__linux__)
// The token the wake-up eventfd is registered under in a shard's readiness
// set. The broker ignores tokens that name no connection slot, and this one
// cannot: no slot index is 0xFFFFFFFE. (Only Linux runs shards.)
const uint64_t kWakeToken = 0xFFFFFFFFFFFFFFFEull;
// And the extension's (ShardExtension::waitHandle), for the same reason.
const uint64_t kExtensionToken = 0xFFFFFFFFFFFFFFFDull;
#endif

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
    int wakeFd = -1;
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
#if defined(__linux__)
        for (size_t to = 0; to < wakePending.size(); to++) {
            if (!wakePending[to]) continue;
            wakePending[to] = 0;
            const uint64_t one = 1;
            const ssize_t ignored = ::write(server->_shards[to]->wakeFd, &one, sizeof one);
            (void)ignored;
        }
#endif
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
        if (routed.retain || groupsFor[to] != nullptr ||
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
#if defined(__linux__)
    uint64_t counter = 0;
    const ssize_t ignored = ::read(wakeFd, &counter, sizeof counter);   // non-blocking
    (void)ignored;
#endif
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
                        postReliably(envelope.from, reply);
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
        shard.broker.poll();
        shard.drain(false);
        if (_extension) _extension->service(shard.index, shard.broker);
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
    shard.endListeners();
    shard.broker.end();
    shard.flushWakes();
    _ended.fetch_add(1, std::memory_order_acq_rel);
    for (;;) {
        shard.drain(true);
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

ShardedBroker::ShardedBroker() {}

ShardedBroker::~ShardedBroker() { stop(); }

bool ShardedBroker::start(const ShardedBrokerConfig& config) {
#if !defined(__linux__)
    _lastError = "the sharded broker needs Linux (SO_REUSEPORT, epoll, eventfd)";
    (void)config;
    return false;
#else
    if (_running) return false;
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
            if (other->wakeFd >= 0) ::close(other->wakeFd);
            other->wakeFd = -1;
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
    uint16_t port = config.port;
    uint16_t tlsPort = config.tls.port;
    for (std::unique_ptr<Shard>& shard : _shards) {
        Shard& s = *shard;
        s.config = config.broker;
        std::snprintf(s.prefix, sizeof s.prefix, "auto-s%u-", s.index);
        s.config.assignedIdPrefix = s.prefix;

        if (config.plain) {
            s.listener.configure(config.address.c_str(), port, s.config.maxConnections);
            s.listener.setReusePort(count > 1);
            if (!s.listener.begin()) {
                return abandon("shard " + std::to_string(s.index) + ": " + s.listener.lastError());
            }
            if (port == 0) port = s.listener.boundPort();
        }
#if MQTT_TLS_SOCKETS
        if (config.tls.enabled) {
            const ShardedTlsConfig& tls = config.tls;
            const std::string& address = tls.address.empty() ? config.address : tls.address;
            s.tlsListener.configure(address.c_str(), tlsPort, s.config.maxConnections);
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
        if (_extension != nullptr) _extension->prepare(s.index, s.broker);
        if (config.plain) s.broker.addListener(s.listener);
#if MQTT_TLS_SOCKETS
        if (config.tls.enabled) s.broker.addListener(s.tlsListener);
#endif
        if (!s.readiness.beginPersistent()) return abandon("could not create an epoll set");
        s.broker.useReadiness(s.readiness);
        s.wakeFd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (s.wakeFd < 0) return abandon("could not create a wake-up descriptor");
        s.readiness.add(s.wakeFd, kWakeToken, false);
        if (_extension != nullptr) {
            const int handle = _extension->waitHandle(s.index);
            if (handle >= 0) s.readiness.add(handle, kExtensionToken, false);
            s.broker.onEgress(&Shard::onEgress, &s);   // one shard or many
        }
        if (count > 1) {
            s.broker.onEgress(&Shard::onEgress, &s);
            s.broker.onPlace(&Shard::onPlace, &Shard::onHandOff, &s);
            s.broker.onRouteKey(&Shard::onRouteKey, &s);
            s.broker.onShareMember(&Shard::onShareMember, &s);
            s.broker.onShareGate(&Shard::onShareGate, &s);
        }
    }
    _port = config.plain ? port : 0;
    _tlsPort = config.tls.enabled ? tlsPort : 0;
    {
        std::lock_guard<std::mutex> guard(_tlsLock);
        _tlsServed = config.tls;
        _tlsServed.port = _tlsPort;
    }

    for (std::unique_ptr<Shard>& shard : _shards) {
        Shard* s = shard.get();
        s->thread = std::thread([this, s] { run(*s); });
        if (config.pinThreads) {
            cpu_set_t set;
            CPU_ZERO(&set);
            CPU_SET(s->index % std::thread::hardware_concurrency(), &set);
            pthread_setaffinity_np(s->thread.native_handle(), sizeof set, &set);
        }
    }
    _running = true;
    return true;
#endif
}

void ShardedBroker::stop() {
    if (!_running) return;
    _stopping.store(true, std::memory_order_release);
#if defined(__linux__)
    for (std::unique_ptr<Shard>& shard : _shards) {
        const uint64_t one = 1;
        const ssize_t ignored = ::write(shard->wakeFd, &one, sizeof one);
        (void)ignored;
    }
#endif
    for (std::unique_ptr<Shard>& shard : _shards) {
        if (shard->thread.joinable()) shard->thread.join();
    }
#if defined(__linux__)
    for (std::unique_ptr<Shard>& shard : _shards) {
        if (shard->wakeFd >= 0) ::close(shard->wakeFd);
        shard->wakeFd = -1;
    }
#endif
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

bool ShardedBroker::replaceTls(const ShardedTlsConfig& tls, std::string& error) {
#if !MQTT_TLS_SOCKETS
    (void)tls;
    error = "this build has no TLS engine";
    return false;
#else
    if (!_running || _tlsPort == 0) {
        error = "there is no TLS listener running to change - TLS starts with the server";
        return false;
    }
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
    std::vector<void*> contexts;
    for (size_t i = 0; i < _shards.size(); i++) {
        void* context = i == 0 ? nullptr : MqttTlsListener::shareContext(contexts[0]);
        if (context == nullptr) context = maker.makeContext();
        if (context == nullptr) {
            error = maker.lastError();
            for (void* made : contexts) MqttTlsListener::dropContext(made);
            return false;
        }
        contexts.push_back(context);
    }
    // Delivered, and each shard woken so an idle one swaps now rather than at
    // its next timeout. A context still waiting from an earlier call was
    // never served and is dropped.
    for (size_t i = 0; i < _shards.size(); i++) {
        Shard& shard = *_shards[i];
        {
            std::lock_guard<std::mutex> guard(shard.tlsSwapLock);
            MqttTlsListener::dropContext(shard.tlsPending);
            shard.tlsPending = contexts[i];
            shard.tlsSwap.store(true, std::memory_order_release);
        }
        const uint64_t one = 1;
        const ssize_t ignored = ::write(shard.wakeFd, &one, sizeof one);
        (void)ignored;
    }
    std::lock_guard<std::mutex> guard(_tlsLock);
    const std::string address = _tlsServed.address;
    const uint16_t port = _tlsServed.port;
    _tlsServed = tls;
    _tlsServed.enabled = true;
    _tlsServed.address = address;
    _tlsServed.port = port;
    return true;
#endif
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
        t.retained = s.retained;     // replicated: every shard holds the same set
        out.mailboxFull += shard->mailboxFull.load(std::memory_order_relaxed);
        out.skipped += shard->skipped.load(std::memory_order_relaxed);
        out.connectionsPerShard.push_back(shard->connections);
    }
    return out;
}

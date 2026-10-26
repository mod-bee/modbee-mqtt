// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AppHub.h"

#include "Authority.h"
#include "Json.h"
#include "MqttTopic.h"

#include <algorithm>
#include <cstring>

#include <sys/eventfd.h>
#include <unistd.h>

namespace {

typedef std::chrono::steady_clock Clock;

std::string text(const MqttSpan& span) {
    return span.length ? std::string((const char*)span.data, span.length) : std::string();
}

MqttSpan spanOf(const std::string& s) {
    MqttSpan span;
    span.data = (const uint8_t*)s.data();
    span.length = (uint32_t)s.size();
    return span;
}

int64_t utcNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// 64-bit FNV-1a: stable across runs and builds, which std::hash is not.
uint64_t fnv(const std::string& a, const std::string& b) {
    uint64_t h = 1469598103934665603ull;
    for (unsigned char c : a) h = (h ^ c) * 1099511628211ull;
    h = (h ^ 0xFF) * 1099511628211ull;
    for (unsigned char c : b) h = (h ^ c) * 1099511628211ull;
    return h;
}

const char kEvents[] = "$modbee/events/";

// $modbee/events/<kind>: the JSON BrokerEvents publishes (docs/events.md).
AppClientEvent eventOf(const std::string& topic, const std::string& payload) {
    AppClientEvent event;
    const std::string kind = topic.substr(sizeof kEvents - 1);
    event.kind = kind == "connected"      ? AppClientEvent::Connected
               : kind == "disconnected"   ? AppClientEvent::Disconnected
               : kind == "subscribed"     ? AppClientEvent::Subscribed
               : kind == "unsubscribed"   ? AppClientEvent::Unsubscribed
               : kind == "started"        ? AppClientEvent::ServerStarted
                                          : AppClientEvent::Unknown;
    json::Value value;
    if (!json::parse(payload, value)) return event;
    event.clientId = value.str("clientid");
    event.login = value.str("username");
    event.filter = value.str("filter");
    event.address = value.str("address");
    event.timeMs = value.integer("time");
    event.resumed = value.flag("resumed");
    event.reasonCode = (uint32_t)value.integer("reason");
    event.qos = (uint32_t)value.integer("qos");
    return event;
}

} // namespace

// =============================================================================
// GROUPS AND MEMBERS
// =============================================================================

struct AppHub::Entry {
    std::shared_ptr<const AppMessage> message;
    bool replay = false;          // it waited while no member was connected
    bool redelivered = false;     // sent before, to a member that did not acknowledge it
};

struct AppHub::Member {
    Group* group = nullptr;
    AppConsumer* consumer = nullptr;
    std::string stableId;                          // what the publisher hash is taken with
    uint32_t maxInFlight = 100;
    uint64_t nextId = 1;
    std::deque<std::shared_ptr<Entry>> pending;    // assigned to it, not yet sent
    std::map<uint64_t, std::shared_ptr<Entry>> inFlight;   // sent, not acknowledged
};

struct AppHub::Group {
    std::string name, connection;
    bool durable = true;                           // named: it outlives its members
    AppConsumeOptions options;
    std::vector<Member*> members;
    std::deque<std::shared_ptr<Entry>> waiting;    // no member to give it to
    std::vector<std::pair<Clock::time_point, std::shared_ptr<Entry>>> delayed;   // Retry
    size_t queued = 0;                             // everything it holds: waiting, pending, in flight, delayed
    uint64_t refused = 0;
    size_t nextRound = 0;
    Clock::time_point emptySince = Clock::now();
};

struct AppHub::Shard {
    uint32_t index = 0;
    MqttBroker* broker = nullptr;
    int eventFd = -1;
    std::mutex lock;
    std::deque<std::pair<AppPublish, std::function<void(const AppPublishResult&)>>> publishes;
    // While this hub's own publish is routed: its offer() knows it by this.
    char marker = 0;
    const std::string* publishingOrigin = nullptr;

    void wake() {
        const uint64_t one = 1;
        const ssize_t ignored = ::write(eventFd, &one, sizeof one);
        (void)ignored;
    }
};

AppHub::AppHub(const AppHubConfig& config) : _config(config) {}

AppHub::~AppHub() {
    {
        std::lock_guard<std::mutex> guard(_lock);
        _quit = true;
    }
    _tick.notify_all();
    if (_timer.joinable()) _timer.join();
    for (auto& group : _groups) {
        for (Member* member : group.second->members) delete member;
    }
    for (std::unique_ptr<Shard>& shard : _shards) {
        if (shard->eventFd >= 0) ::close(shard->eventFd);
    }
}

bool AppHub::begin(size_t shards, std::string& error) {
    _shards.clear();
    for (size_t index = 0; index < shards; index++) {
        std::unique_ptr<Shard> shard(new Shard());
        shard->index = (uint32_t)index;
        shard->eventFd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (shard->eventFd < 0) {
            error = "messages: could not create a wake-up descriptor";
            return false;
        }
        _shards.push_back(std::move(shard));
    }
    _timer = std::thread([this] { timers(); });
    return true;
}

void AppHub::prepare(uint32_t shard, MqttBroker& broker) {
    if (shard < _shards.size()) _shards[shard]->broker = &broker;
}

int AppHub::waitHandle(uint32_t shard) {
    return shard < _shards.size() ? _shards[shard]->eventFd : -1;
}

void AppHub::stop(uint32_t, MqttBroker&) {}

// --- Taking a message ------------------------------------------------------

uint32_t AppHub::offer(uint32_t index, MqttBroker& broker, const MqttBrokerRouted& message,
                       uint64_t deferral) {
    if (index >= _shards.size()) return 0;
    Shard& shard = *_shards[index];
    const std::string topic = text(message.topic);
    const bool isEvent = topic.compare(0, sizeof kEvents - 1, kEvents) == 0;
    const std::string origin =
        message.origin != nullptr && message.origin == &shard.marker && shard.publishingOrigin
            ? *shard.publishingOrigin : std::string();

    bool refused = false;
    bool taken = false;
    {
        std::lock_guard<std::mutex> guard(_lock);
        if (_groups.empty()) return 0;
        std::shared_ptr<AppMessage> built;
        for (auto& named : _groups) {
            Group& group = *named.second;
            if (!group.connection.empty()) continue;      // a managed connection's, not the broker's
            bool matches = false;
            if (isEvent) {
                matches = group.options.events;
            } else {
                for (const std::string& filter : group.options.filters) {
                    if (mq_topicMatches(spanOf(filter), message.topic)) {
                        matches = true;
                        break;
                    }
                }
            }
            if (!matches) continue;
            if (!origin.empty() && std::find(group.options.excludeOrigins.begin(),
                                             group.options.excludeOrigins.end(),
                                             origin) != group.options.excludeOrigins.end()) {
                continue;
            }
            if (!built) {
                built.reset(new AppMessage());
                built->topic = topic;
                built->payload = text(message.payload);
                built->qos = message.qos;
                built->retain = message.retain;
                if (message.properties) built->properties.copyForForwarding(*message.properties);
                built->clientId = text(message.clientId);
                built->login = text(message.userName);
                built->tag = authorityTagOf(message.context);
                built->origin = origin;
                built->brokerTimeMs = utcNowMs();
                if (isEvent) {
                    built->isEvent = true;
                    built->event = eventOf(topic, built->payload);
                }
            }
            if (take(group, built)) {
                taken = true;
            } else if (group.options.onFull == AppOnFull::Refuse) {
                refused = true;
            }
        }
    }
    if (deferral == 0 || message.qos == MQ_QOS_0) return 0;
    // A group that had to take it could not: the publisher is told, and keeps it.
    if (refused) {
        broker.concludeDeferred(deferral, 0, 0, MQ_REASON_QUOTA_EXCEEDED);
        return 1;
    }
    // A group took it, so it has somewhere to go: not 0x10 No matching
    // subscribers, as with a Kafka mapping that took it.
    if (taken) {
        broker.concludeDeferred(deferral, 1, 0);
        return 1;
    }
    return 0;
}

bool AppHub::take(Group& group, const std::shared_ptr<const AppMessage>& message) {
    if (group.queued >= _config.maxQueued) {
        group.refused++;
        (group.options.onFull == AppOnFull::Refuse ? _refused : _dropped)++;
        return false;
    }
    std::shared_ptr<Entry> entry(new Entry());
    entry->message = message;
    group.queued++;
    _taken++;
    assign(group, entry);
    return true;
}

// --- Dealing it to a member ------------------------------------------------

void AppHub::assign(Group& group, std::shared_ptr<Entry> entry) {
    if (group.members.empty()) {
        entry->replay = true;
        group.waiting.push_back(std::move(entry));
        return;
    }
    Member* chosen = nullptr;
    if (group.options.pick == AppPick::RoundRobin) {
        chosen = group.members[group.nextRound++ % group.members.size()];
    } else {
        // Rendezvous: the member whose hash with this publisher is highest.
        // Stable while the members are, and a member's going moves only what
        // was its own.
        const AppMessage& message = *entry->message;
        const std::string& publisher = message.isEvent ? message.event.clientId : message.clientId;
        uint64_t best = 0;
        for (Member* member : group.members) {
            const uint64_t score = fnv(publisher, member->stableId);
            if (chosen == nullptr || score > best) {
                best = score;
                chosen = member;
            }
        }
    }
    chosen->pending.push_back(std::move(entry));
    pump(group, *chosen);
}

void AppHub::pump(Group&, Member& member) {
    while (member.inFlight.size() < member.maxInFlight && !member.pending.empty()) {
        std::shared_ptr<Entry> entry = std::move(member.pending.front());
        member.pending.pop_front();
        const uint64_t id = member.nextId++;
        member.inFlight[id] = entry;
        _delivered++;
        if (entry->redelivered) _redelivered++;
        member.consumer->deliver(id, *entry->message, entry->replay, entry->redelivered);
    }
}

void AppHub::leave(Group& group, Member& member) {
    group.members.erase(std::remove(group.members.begin(), group.members.end(), &member),
                        group.members.end());
    if (group.members.empty()) group.emptySince = Clock::now();
    // What it was sent and did not acknowledge, oldest first, then what was
    // still to be sent: dealt again to the others - or kept for the next.
    std::vector<std::shared_ptr<Entry>> held;
    for (auto& sent : member.inFlight) {
        sent.second->redelivered = true;
        held.push_back(sent.second);
    }
    for (auto& unsent : member.pending) held.push_back(unsent);
    member.inFlight.clear();
    member.pending.clear();
    for (std::shared_ptr<Entry>& entry : held) assign(group, std::move(entry));
}

// --- The members' side -----------------------------------------------------

AppHub::Member* AppHub::open(const AppConsumeOptions& options, AppConsumer* consumer, std::string& error,
                             uint32_t* members, uint64_t* queued) {
    if (options.filters.empty() && !options.events) {
        error = "a Consume stream needs at least one filter (or events)";
        return nullptr;
    }
    for (const std::string& filter : options.filters) {
        if (!mq_topicFilterIsValid(spanOf(filter)) || mq_isSharedFilter(spanOf(filter))) {
            error = "\"" + filter + "\" is not a topic filter";
            return nullptr;
        }
    }
    if (options.events && !_config.eventsEnabled) {
        error = "client events are off on this server (events: enabled)";
        return nullptr;
    }
    std::lock_guard<std::mutex> guard(_lock);
    std::string key = options.connection;
    key.push_back('\0');
    const bool durable = !options.group.empty();
    key += durable ? options.group : "#stream-" + std::to_string(_nextPrivate++);
    auto found = _groups.find(key);
    if (found == _groups.end()) {
        std::unique_ptr<Group> group(new Group());
        group->name = options.group;
        group->connection = options.connection;
        group->durable = durable;
        group->options = options;
        found = _groups.emplace(key, std::move(group)).first;
    } else {
        std::vector<std::string> mine = options.filters, theirs = found->second->options.filters;
        std::sort(mine.begin(), mine.end());
        std::sort(theirs.begin(), theirs.end());
        if (mine != theirs || options.events != found->second->options.events) {
            error = "the group \"" + options.group + "\" exists with other filters";
            return nullptr;
        }
    }
    Group& group = *found->second;
    Member* member = new Member();
    member->group = &group;
    member->consumer = consumer;
    member->maxInFlight = options.maxInFlight ? options.maxInFlight : 100;
    member->stableId = std::to_string((uintptr_t)member) + "#" + std::to_string(_nextPrivate++);
    group.members.push_back(member);
    if (members) *members = (uint32_t)group.members.size();
    if (queued) *queued = group.queued;
    // What waited for a member is dealt now.
    std::deque<std::shared_ptr<Entry>> waiting;
    waiting.swap(group.waiting);
    for (std::shared_ptr<Entry>& entry : waiting) assign(group, std::move(entry));
    return member;
}

void AppHub::ack(Member* member, const std::vector<uint64_t>& ids) {
    std::lock_guard<std::mutex> guard(_lock);
    Group& group = *member->group;
    for (uint64_t id : ids) {
        if (member->inFlight.erase(id) == 0) continue;
        group.queued--;
        _acknowledged++;
    }
    pump(group, *member);
}

void AppHub::retry(Member* member, const std::vector<uint64_t>& ids, uint32_t afterMs) {
    std::lock_guard<std::mutex> guard(_lock);
    Group& group = *member->group;
    for (uint64_t id : ids) {
        auto found = member->inFlight.find(id);
        if (found == member->inFlight.end()) continue;
        std::shared_ptr<Entry> entry = found->second;
        member->inFlight.erase(found);
        entry->redelivered = true;
        if (afterMs == 0) {
            assign(group, entry);
        } else {
            group.delayed.emplace_back(Clock::now() + std::chrono::milliseconds(afterMs), entry);
        }
    }
    pump(group, *member);
    _tick.notify_all();
}

void AppHub::close(Member* member) {
    std::lock_guard<std::mutex> guard(_lock);
    Group& group = *member->group;
    leave(group, *member);
    delete member;
    if (!group.durable && group.members.empty()) {
        for (auto at = _groups.begin(); at != _groups.end(); ++at) {
            if (at->second.get() == &group) {
                _groups.erase(at);
                break;
            }
        }
    }
}

void AppHub::timers() {
    std::unique_lock<std::mutex> guard(_lock);
    while (!_quit) {
        _tick.wait_for(guard, std::chrono::milliseconds(50));
        if (_quit) break;
        const Clock::time_point now = Clock::now();
        for (auto at = _groups.begin(); at != _groups.end();) {
            Group& group = *at->second;
            // Retries that are due.
            std::vector<std::shared_ptr<Entry>> due;
            for (auto d = group.delayed.begin(); d != group.delayed.end();) {
                if (d->first <= now) {
                    due.push_back(d->second);
                    d = group.delayed.erase(d);
                } else {
                    ++d;
                }
            }
            for (std::shared_ptr<Entry>& entry : due) assign(group, std::move(entry));
            // A durable group nobody has opened for its expiry is gone.
            if (group.durable && group.members.empty() &&
                now - group.emptySince > std::chrono::seconds(group.options.expirySeconds)) {
                at = _groups.erase(at);
                continue;
            }
            ++at;
        }
    }
}

// --- Out: the application publishes ------------------------------------------

void AppHub::publish(const AppPublish& message, std::function<void(const AppPublishResult&)> done) {
    AppPublishResult result;
    if (!mq_topicNameIsValid(spanOf(message.topic)) || message.qos > MQ_QOS_2) {
        result.outcome = AppPublishOutcome::Refused;
        result.reasonCode = MQ_REASON_TOPIC_NAME_INVALID;
        done(result);
        return;
    }
    if (!message.connection.empty() || _shards.empty()) {
        result.outcome = AppPublishOutcome::NotConnected;
        done(result);
        return;
    }
    Shard& shard = *_shards[_nextShard++ % _shards.size()];
    {
        std::lock_guard<std::mutex> guard(shard.lock);
        shard.publishes.emplace_back(message, std::move(done));
    }
    shard.wake();
}

void AppHub::service(uint32_t index, MqttBroker& broker) {
    if (index >= _shards.size()) return;
    Shard& shard = *_shards[index];
    uint64_t counter = 0;
    const ssize_t ignored = ::read(shard.eventFd, &counter, sizeof counter);
    (void)ignored;
    std::deque<std::pair<AppPublish, std::function<void(const AppPublishResult&)>>> publishes;
    {
        std::lock_guard<std::mutex> guard(shard.lock);
        publishes.swap(shard.publishes);
    }
    for (auto& one : publishes) {
        const AppPublish& message = one.first;
        MqttProperties properties;
        message.properties.describeTo(properties);
        MqttBrokerRouted routed;
        routed.topic = spanOf(message.topic);
        routed.payload = spanOf(message.payload);
        routed.qos = message.qos;
        routed.retain = message.retain;
        routed.properties = &properties;
        routed.origin = &shard.marker;
        shard.publishingOrigin = &message.origin;
        const bool taken = broker.publishMessage(routed);
        shard.publishingOrigin = nullptr;
        _published++;
        AppPublishResult result;
        result.outcome = taken ? AppPublishOutcome::Accepted : AppPublishOutcome::Refused;
        if (!taken) result.reasonCode = MQ_REASON_QUOTA_EXCEEDED;
        one.second(result);
    }
}

// --- Looking at it -----------------------------------------------------------

std::vector<AppGroupInfo> AppHub::groups() const {
    std::lock_guard<std::mutex> guard(_lock);
    std::vector<AppGroupInfo> out;
    for (const auto& named : _groups) {
        const Group& group = *named.second;
        if (!group.durable) continue;
        AppGroupInfo info;
        info.group = group.name;
        info.connection = group.connection;
        info.filters = group.options.filters;
        info.members = (uint32_t)group.members.size();
        info.queued = group.queued;
        for (const Member* member : group.members) info.inFlight += member->inFlight.size();
        info.refused = group.refused;
        out.push_back(info);
    }
    return out;
}

bool AppHub::deleteGroup(const std::string& name, const std::string& connection, uint64_t* discarded,
                         std::string* why) {
    std::lock_guard<std::mutex> guard(_lock);
    std::string key = connection;
    key.push_back('\0');
    key += name;
    auto found = _groups.find(key);
    if (found == _groups.end()) {
        if (why) *why = "no group \"" + name + "\"";
        return false;
    }
    if (!found->second->members.empty()) {
        if (why) *why = "the group \"" + name + "\" has members connected";
        return false;
    }
    if (discarded) *discarded = found->second->queued;
    _groups.erase(found);
    return true;
}

AppHubStatus AppHub::status() const {
    AppHubStatus out;
    {
        std::lock_guard<std::mutex> guard(_lock);
        out.groups = _groups.size();
        for (const auto& named : _groups) out.members += named.second->members.size();
    }
    out.taken = _taken.load();
    out.delivered = _delivered.load();
    out.acknowledged = _acknowledged.load();
    out.redelivered = _redelivered.load();
    out.refused = _refused.load();
    out.dropped = _dropped.load();
    out.published = _published.load();
    return out;
}

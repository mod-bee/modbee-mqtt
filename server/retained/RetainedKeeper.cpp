// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "RetainedKeeper.h"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <mutex>
#include <sstream>
#include <thread>

#include "ServerOs.h"

namespace {

std::string toText(const MqttSpan& span) {
    if (span.data == nullptr || span.length == 0) return std::string();
    return std::string((const char*)span.data, span.length);
}

MqttSpan spanOf(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

} // namespace

uint64_t retainedNowUtc() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// =============================================================================
// THE RECORD
// =============================================================================

RetainedRecord RetainedRecord::from(const MqttBrokerRouted& message, uint64_t nowUtc) {
    RetainedRecord record;
    record.topic = toText(message.topic);
    record.payload = toText(message.payload);
    record.qos = message.qos;
    const MqttProperties* p = message.properties;
    if (p == nullptr) return record;
    if (p->has(MQ_PROP_PAYLOAD_FORMAT_INDICATOR)) {
        record.hasFormat = true;
        record.format = p->payloadFormatIndicator;
    }
    if (p->has(MQ_PROP_CONTENT_TYPE)) record.contentType = toText(p->contentType);
    if (p->has(MQ_PROP_RESPONSE_TOPIC)) record.responseTopic = toText(p->responseTopic);
    if (p->has(MQ_PROP_CORRELATION_DATA)) {
        record.hasCorrelation = true;
        record.correlation = toText(p->correlationData);
    }
    if (p->has(MQ_PROP_MESSAGE_EXPIRY_INTERVAL)) {
        record.expiryInterval = p->messageExpiryInterval;
        record.expiresAt = nowUtc + (uint64_t)p->messageExpiryInterval * 1000u;
    }
    for (uint8_t u = 0; u < p->userCount; u++) {
        record.userProperties.push_back(std::make_pair(toText(p->user[u].key), toText(p->user[u].value)));
    }
    return record;
}

bool RetainedRecord::describeTo(MqttProperties& properties, uint64_t nowUtc) const {
    properties = MqttProperties();
    if (expiresAt != 0) {
        if (nowUtc >= expiresAt) return false;
        // Rounded up: a message with 1.5 s left is not expired at 1 s.
        properties.messageExpiryInterval = (uint32_t)((expiresAt - nowUtc + 999u) / 1000u);
        properties.markPresent(MQ_PROP_MESSAGE_EXPIRY_INTERVAL);
    }
    if (hasFormat) {
        properties.payloadFormatIndicator = format;
        properties.markPresent(MQ_PROP_PAYLOAD_FORMAT_INDICATOR);
    }
    if (!contentType.empty()) {
        properties.contentType = spanOf(contentType);
        properties.markPresent(MQ_PROP_CONTENT_TYPE);
    }
    if (!responseTopic.empty()) {
        properties.responseTopic = spanOf(responseTopic);
        properties.markPresent(MQ_PROP_RESPONSE_TOPIC);
    }
    if (hasCorrelation) {
        properties.correlationData = spanOf(correlation);
        properties.markPresent(MQ_PROP_CORRELATION_DATA);
    }
    for (const auto& user : userProperties) {
        properties.addUserProperty(spanOf(user.first), spanOf(user.second));
    }
    return true;
}

// =============================================================================
// THE SNAPSHOT FILE
// =============================================================================
//
// The whole set, rewritten a few seconds after a change and at shutdown:
// written to <file>.new, flushed to disk, then renamed over <file> - so the
// file on disk is always one complete snapshot, the previous or the new, and
// never half of each. A crash loses at most the last few seconds' changes.
//
// Layout, little-endian: "MBRT", version (1 byte), record count (4), then
// each record: topic, payload (4-byte length + bytes each), qos, flags
// (1: payload format, 2: correlation data), format, expires-at (8, UTC ms),
// content type, response topic, correlation data, user property count (2),
// then each user property's key and value.

namespace {

const char kMagic[4] = {'M', 'B', 'R', 'T'};
const uint8_t kVersion = 1;

// Written this long after the first unsaved change.
const auto kSaveAfter = std::chrono::seconds(2);

void put8(std::string& out, uint8_t v) { out.push_back((char)v); }
void put16(std::string& out, uint16_t v) { for (int i = 0; i < 2; i++) out.push_back((char)(v >> (8 * i))); }
void put32(std::string& out, uint32_t v) { for (int i = 0; i < 4; i++) out.push_back((char)(v >> (8 * i))); }
void put64(std::string& out, uint64_t v) { for (int i = 0; i < 8; i++) out.push_back((char)(v >> (8 * i))); }
void putText(std::string& out, const std::string& text) {
    put32(out, (uint32_t)text.size());
    out += text;
}

struct Reader {
    const std::string& data;
    size_t at = 0;
    bool ok = true;
    explicit Reader(const std::string& d) : data(d) {}
    bool need(size_t n) {
        if (!ok || data.size() - at < n) ok = false;
        return ok;
    }
    uint64_t number(int bytes) {
        if (!need((size_t)bytes)) return 0;
        uint64_t v = 0;
        for (int i = 0; i < bytes; i++) v |= (uint64_t)(uint8_t)data[at + i] << (8 * i);
        at += (size_t)bytes;
        return v;
    }
    std::string text() {
        const size_t length = (size_t)number(4);
        if (!need(length)) return std::string();
        std::string out = data.substr(at, length);
        at += length;
        return out;
    }
};

void encode(const RetainedRecord& r, std::string& out) {
    putText(out, r.topic);
    putText(out, r.payload);
    put8(out, r.qos);
    put8(out, (uint8_t)((r.hasFormat ? 1 : 0) | (r.hasCorrelation ? 2 : 0)));
    put8(out, r.format);
    put64(out, r.expiresAt);
    putText(out, r.contentType);
    putText(out, r.responseTopic);
    putText(out, r.correlation);
    put16(out, (uint16_t)r.userProperties.size());
    for (const auto& user : r.userProperties) {
        putText(out, user.first);
        putText(out, user.second);
    }
}

bool decode(Reader& in, RetainedRecord& r) {
    r.topic = in.text();
    r.payload = in.text();
    r.qos = (uint8_t)in.number(1);
    const uint8_t flags = (uint8_t)in.number(1);
    r.hasFormat = (flags & 1) != 0;
    r.hasCorrelation = (flags & 2) != 0;
    r.format = (uint8_t)in.number(1);
    r.expiresAt = in.number(8);
    r.contentType = in.text();
    r.responseTopic = in.text();
    r.correlation = in.text();
    const size_t users = (size_t)in.number(2);
    for (size_t u = 0; u < users && in.ok; u++) {
        std::string key = in.text();
        std::string value = in.text();
        r.userProperties.push_back(std::make_pair(std::move(key), std::move(value)));
    }
    return in.ok && !r.topic.empty() && !r.payload.empty() && r.qos <= 2;
}

class RetainedFile : public RetainedBackend {
public:
    explicit RetainedFile(const std::string& path) : _path(path) {}

    ~RetainedFile() override {
        {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_writer.joinable() && _dirty) save(lock);
            _quit = true;
        }
        _wake.notify_all();
        if (_writer.joinable()) _writer.join();
    }

    bool load(RetainedSet& into, std::string& error) override {
        // Writable first: a store that cannot be written is one that would
        // fail silently at the first change and lose everything at restart.
        std::string why;
        if (!server_os::canWrite(_path + ".new", why)) {
            error = "cannot write retained messages to " + _path + ": " + why +
                    " (in Docker, mount a volume at its folder - docs/docker.md)";
            return false;
        }

        std::ifstream file(_path, std::ios::binary);
        if (file) {
            std::ostringstream contents;
            contents << file.rdbuf();
            const std::string data = contents.str();
            if (!read(data, into)) {
                // Kept, not deleted: whatever is wrong with it, somebody may
                // want to look. The server starts with an empty store rather
                // than not at all.
                const std::string aside = _path + ".unreadable";
                server_os::replaceFile(_path, aside);
                std::fprintf(stderr, "modbee serve: %s is not a retained-message file this "
                                     "version can read; moved it to %s and starting empty\n",
                             _path.c_str(), aside.c_str());
                into.clear();
            }
        }
        _set = into;
        _writer = std::thread([this] { run(); });
        return true;
    }

    void stored(const RetainedRecord& record) override {
        std::lock_guard<std::mutex> lock(_mutex);
        _set[record.topic] = record;
        changed();
    }

    void cleared(const std::string& topic) override {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_set.erase(topic) != 0) changed();
    }

    void flush() override {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_dirty) save(lock);
    }

    std::string where() const override { return "file " + _path; }
    uint64_t failures() const override { return _failures.load(); }

private:
    static bool read(const std::string& data, RetainedSet& into) {
        if (data.size() < 9 || std::memcmp(data.data(), kMagic, 4) != 0) return false;
        Reader in(data);
        in.at = 4;
        if (in.number(1) != kVersion) return false;
        const size_t count = (size_t)in.number(4);
        for (size_t i = 0; i < count; i++) {
            RetainedRecord record;
            if (!decode(in, record)) return false;
            into[record.topic] = std::move(record);
        }
        return in.ok && in.at == data.size();
    }

    // With the mutex held.
    void changed() {
        if (!_dirty) {
            _dirty = true;
            _dirtySince = std::chrono::steady_clock::now();
            _wake.notify_all();
        }
    }

    void run() {
        std::unique_lock<std::mutex> lock(_mutex);
        while (!_quit) {
            if (!_dirty) {
                _wake.wait(lock);
                continue;
            }
            const auto due = _dirtySince + kSaveAfter;
            if (std::chrono::steady_clock::now() < due) {
                _wake.wait_until(lock, due);
                continue;
            }
            save(lock);
        }
    }

    // Encodes under the lock (a copy of the set would cost as much), writes
    // without it.
    void save(std::unique_lock<std::mutex>& lock) {
        std::string data(kMagic, 4);
        put8(data, kVersion);
        put32(data, (uint32_t)_set.size());
        for (const auto& entry : _set) encode(entry.second, data);
        _dirty = false;
        lock.unlock();

        std::string why;
        const bool ok = server_os::writeFileAtomically(_path, data, why);
        lock.lock();
        if (!ok) {
            if (_failures.fetch_add(1) == 0) {
                std::fprintf(stderr, "modbee serve: could not save retained messages to %s: %s "
                                     "(retrying; counted in the status line)\n",
                             _path.c_str(), why.c_str());
            }
            // Try again after the usual delay rather than spinning.
            if (!_dirty) {
                _dirty = true;
                _dirtySince = std::chrono::steady_clock::now();
            }
        }
    }

    const std::string _path;
    std::mutex _mutex;
    std::condition_variable _wake;
    std::thread _writer;
    RetainedSet _set;
    bool _dirty = false;
    bool _quit = false;
    std::chrono::steady_clock::time_point _dirtySince;
    std::atomic<uint64_t> _failures{0};
};

} // namespace

std::unique_ptr<RetainedBackend> makeRetainedFile(const std::string& path) {
    return std::unique_ptr<RetainedBackend>(new RetainedFile(path));
}

// =============================================================================
// THE KEEPER
// =============================================================================

RetainedKeeper::RetainedKeeper(std::unique_ptr<RetainedBackend> backend)
    : _backend(std::move(backend)) {}

bool RetainedKeeper::begin(size_t shards, std::string& error) {
    _shards = shards;
    _stopped = 0;
    _loaded.clear();
    return _backend->load(_loaded, error);
}

void RetainedKeeper::prepare(uint32_t shard, MqttBroker& broker) {
    const uint64_t now = retainedNowUtc();
    uint64_t expired = 0, refused = 0;
    for (const auto& entry : _loaded) {
        const RetainedRecord& record = entry.second;
        MqttProperties properties;
        if (!record.describeTo(properties, now)) {
            expired++;
            continue;
        }
        if (!broker.restoreRetained(spanOf(record.topic), spanOf(record.payload), record.qos,
                                    properties)) {
            refused++;
        }
    }
    // Every shard sees the same set, so one shard's count is the server's.
    if (shard == 0) {
        _expired = expired;
        _refused = refused;
    }
}

uint32_t RetainedKeeper::offer(uint32_t, MqttBroker&, const MqttBrokerRouted& message, uint64_t) {
    if (!message.retain) return 0;
    if (message.payload.length == 0) {
        _backend->cleared(toText(message.topic));
    } else {
        _backend->stored(RetainedRecord::from(message, retainedNowUtc()));
    }
    // Never holds the acknowledgement: memory has it, which is what MQTT
    // promises; this is the copy that outlives a restart.
    return 0;
}

void RetainedKeeper::stop(uint32_t, MqttBroker&) {
    // Once, when the last shard is stopping - every change is in by then.
    if (++_stopped == _shards) _backend->flush();
}

// =============================================================================
// THE CHAIN
// =============================================================================

ShardExtensionChain::~ShardExtensionChain() {}

bool ShardExtensionChain::begin(size_t shards, std::string& error) {
    _perShard.assign(shards, PerShard());
    for (ShardExtension* extension : _chain) {
        if (!extension->begin(shards, error)) return false;
    }
    return true;
}

void ShardExtensionChain::prepare(uint32_t shard, MqttBroker& broker) {
    for (ShardExtension* extension : _chain) extension->prepare(shard, broker);
}

// One only when exactly one member has one; a shard asks waitHandles().
int ShardExtensionChain::waitHandle(uint32_t shard) {
    std::vector<int> handles;
    waitHandles(shard, handles);
    return handles.size() == 1 ? handles[0] : -1;
}

void ShardExtensionChain::waitHandles(uint32_t shard, std::vector<int>& out) {
    for (ShardExtension* extension : _chain) extension->waitHandles(shard, out);
}

uint32_t ShardExtensionChain::offer(uint32_t shard, MqttBroker& broker,
                                    const MqttBrokerRouted& message, uint64_t deferral) {
    uint32_t parties = 0;
    for (ShardExtension* extension : _chain) {
        parties += extension->offer(shard, broker, message, deferral);
    }
    return parties;
}

void ShardExtensionChain::service(uint32_t shard, MqttBroker& broker) {
    for (ShardExtension* extension : _chain) extension->service(shard, broker);
}

bool ShardExtensionChain::hasPendingWork(uint32_t shard) {
    for (ShardExtension* extension : _chain) {
        if (extension->hasPendingWork(shard)) return true;
    }
    return false;
}

void ShardExtensionChain::stop(uint32_t shard, MqttBroker& broker) {
    for (ShardExtension* extension : _chain) extension->stop(shard, broker);
}

uint64_t ShardExtensionChain::forwardedHold(uint32_t shard) {
    std::vector<std::pair<size_t, uint64_t>> members;
    for (size_t index = 0; index < _chain.size(); index++) {
        const uint64_t hold = _chain[index]->forwardedHold(shard);
        if (hold != 0) members.emplace_back(index, hold);
    }
    if (members.empty() || shard >= _perShard.size()) return 0;
    PerShard& own = _perShard[shard];
    const uint64_t hold = own.nextHold++;
    own.holds.emplace_back(hold, std::move(members));
    return hold;
}

bool ShardExtensionChain::forwardedSettled(uint32_t shard, uint64_t hold) {
    if (shard >= _perShard.size()) return true;
    PerShard& own = _perShard[shard];
    for (size_t at = 0; at < own.holds.size(); at++) {
        if (own.holds[at].first != hold) continue;
        for (const std::pair<size_t, uint64_t>& member : own.holds[at].second) {
            if (!_chain[member.first]->forwardedSettled(shard, member.second)) return false;
        }
        own.holds.erase(own.holds.begin() + (long)at);
        return true;
    }
    return true;
}

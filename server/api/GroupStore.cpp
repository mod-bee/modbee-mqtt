// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "GroupStore.h"

#include "sqlite3.h"
#include "MqttProperties.h"
#include "MqttWire.h"

#include <chrono>
#include <cstring>
#include <map>

#include <sys/stat.h>

struct GroupStore::Change {
    enum Kind : uint8_t { PutGroup, DropGroup, PutMessage, DropMessage } kind = PutGroup;
    uint64_t id = 0;
    std::string name, connection;
    AppConsumeOptions options;
    int64_t emptySinceMs = 0;
    std::string message;          // encoded
};

namespace {

const char* const kTables =
    "CREATE TABLE IF NOT EXISTS groups(connection BLOB, name BLOB, filters BLOB, events INTEGER, "
    "  pick INTEGER, expiry INTEGER, on_full INTEGER, exclude BLOB, empty_since INTEGER, "
    "  PRIMARY KEY(connection, name)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, connection BLOB, name BLOB, message BLOB);"
    "CREATE INDEX IF NOT EXISTS messages_by_group ON messages(connection, name);";

bool exec(sqlite3* db, const char* sql) {
    return sqlite3_exec(db, sql, nullptr, nullptr, nullptr) == SQLITE_OK;
}

// --- A message as a blob: length-prefixed fields, in order ---

void put(std::string& out, const std::string& field) {
    const uint32_t size = (uint32_t)field.size();
    out.append((const char*)&size, sizeof size);
    out += field;
}

void put(std::string& out, uint64_t value) {
    out.append((const char*)&value, sizeof value);
}

struct Reader {
    const char* at;
    const char* end;
    bool ok = true;
    std::string text() {
        uint32_t size = 0;
        if (end - at < (long)sizeof size) return fail();
        std::memcpy(&size, at, sizeof size);
        at += sizeof size;
        if (end - at < (long)size) return fail();
        std::string out(at, size);
        at += size;
        return out;
    }
    uint64_t number() {
        uint64_t value = 0;
        if (end - at < (long)sizeof value) {
            ok = false;
            return 0;
        }
        std::memcpy(&value, at, sizeof value);
        at += sizeof value;
        return value;
    }
    std::string fail() {
        ok = false;
        return std::string();
    }
};

// MQTT 5 properties as the PUBLISH codec writes them.
std::string encodeProperties(const MqttOwnedProperties& owned) {
    MqttProperties properties;
    owned.describeTo(properties);
    const size_t size = mq_propertiesSize(properties, MQ_PKT_PUBLISH);
    std::string out(size, '\0');
    MqttWriter writer((uint8_t*)&out[0], size);
    mq_encodeProperties(writer, properties, MQ_PKT_PUBLISH);
    if (!writer.ok()) return std::string();
    out.resize(writer.written());
    return out;
}

bool decodeProperties(const std::string& data, MqttOwnedProperties& into) {
    into.clear();
    if (data.empty()) return true;
    MqttReader reader((const uint8_t*)data.data(), data.size());
    MqttProperties properties;
    if (mq_decodeProperties(reader, MQ_PKT_PUBLISH, properties) != MqttError::None) return false;
    into.copyForForwarding(properties);
    return true;
}

std::string encodeMessage(const AppMessage& m) {
    std::string out;
    put(out, m.topic);
    put(out, m.payload);
    put(out, (uint64_t)m.qos | (uint64_t)(m.retain ? 0x100 : 0) | (uint64_t)(m.isEvent ? 0x200 : 0));
    put(out, encodeProperties(m.properties));
    put(out, m.clientId);
    put(out, m.login);
    put(out, m.tag);
    put(out, m.origin);
    put(out, (uint64_t)m.brokerTimeMs);
    put(out, m.connection);
    if (m.isEvent) {
        const AppClientEvent& e = m.event;
        put(out, (uint64_t)e.kind | (uint64_t)(e.resumed ? 0x100 : 0));
        put(out, e.clientId);
        put(out, e.login);
        put(out, e.filter);
        put(out, e.address);
        put(out, (uint64_t)e.timeMs);
        put(out, (uint64_t)e.reasonCode | ((uint64_t)e.qos << 32));
    }
    return out;
}

bool decodeMessage(const void* data, int size, AppMessage& m) {
    Reader r{(const char*)data, (const char*)data + size};
    m.topic = r.text();
    m.payload = r.text();
    const uint64_t flags = r.number();
    m.qos = (uint8_t)(flags & 0xFF);
    m.retain = (flags & 0x100) != 0;
    m.isEvent = (flags & 0x200) != 0;
    if (!decodeProperties(r.text(), m.properties)) return false;
    m.clientId = r.text();
    m.login = r.text();
    m.tag = r.text();
    m.origin = r.text();
    m.brokerTimeMs = (int64_t)r.number();
    m.connection = r.text();
    if (m.isEvent) {
        AppClientEvent& e = m.event;
        const uint64_t kind = r.number();
        e.kind = (AppClientEvent::Kind)(kind & 0xFF);
        e.resumed = (kind & 0x100) != 0;
        e.clientId = r.text();
        e.login = r.text();
        e.filter = r.text();
        e.address = r.text();
        e.timeMs = (int64_t)r.number();
        const uint64_t codes = r.number();
        e.reasonCode = (uint32_t)(codes & 0xFFFFFFFFu);
        e.qos = (uint32_t)(codes >> 32);
    }
    return r.ok;
}

// A list of filters or origins: each '\0'-terminated.
std::string joined(const std::vector<std::string>& list) {
    std::string out;
    for (const std::string& one : list) {
        out += one;
        out.push_back('\0');
    }
    return out;
}

std::vector<std::string> split(const void* data, int size) {
    std::vector<std::string> out;
    const char* at = (const char*)data;
    const char* end = at + (size > 0 ? size : 0);
    while (at < end) {
        const char* zero = (const char*)std::memchr(at, '\0', (size_t)(end - at));
        if (zero == nullptr) zero = end;
        out.emplace_back(at, zero);
        at = zero + 1;
    }
    return out;
}

std::string column(sqlite3_stmt* statement, int index) {
    const void* data = sqlite3_column_blob(statement, index);
    const int size = sqlite3_column_bytes(statement, index);
    return data ? std::string((const char*)data, (size_t)size) : std::string();
}

void bindBlob(sqlite3_stmt* statement, int index, const std::string& value) {
    sqlite3_bind_blob(statement, index, value.data(), (int)value.size(), SQLITE_TRANSIENT);
}

} // namespace

GroupStore::GroupStore(const GroupStoreConfig& config) : _config(config) {}

GroupStore::~GroupStore() {
    close();
}

std::string GroupStore::path() const {
    return _config.directory + "/groups.db";
}

bool GroupStore::open(std::vector<StoredGroup>& groups, uint64_t& nextId, std::function<void()> committed,
                      std::string& error) {
    ::mkdir(_config.directory.c_str(), 0700);
    const std::string file = path();
    if (sqlite3_open_v2(file.c_str(), &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
        error = file + ": " + (_db ? sqlite3_errmsg(_db) : "cannot open");
        sqlite3_close(_db);
        _db = nullptr;
        return false;
    }
    sqlite3_busy_timeout(_db, 5000);
    if (!exec(_db, "PRAGMA journal_mode=WAL") ||
        !exec(_db, _config.syncFull ? "PRAGMA synchronous=FULL" : "PRAGMA synchronous=NORMAL") ||
        !exec(_db, kTables)) {
        error = file + ": " + sqlite3_errmsg(_db);
        sqlite3_close(_db);
        _db = nullptr;
        return false;
    }
    if (!load(groups, nextId, error)) {
        sqlite3_close(_db);
        _db = nullptr;
        return false;
    }
    _onCommit = std::move(committed);
    _quit = false;
    _writer = std::thread([this] { write(); });
    return true;
}

bool GroupStore::load(std::vector<StoredGroup>& groups, uint64_t& nextId, std::string& error) {
    std::map<std::pair<std::string, std::string>, size_t> index;
    sqlite3_stmt* statement = nullptr;
    if (sqlite3_prepare_v2(_db, "SELECT connection, name, filters, events, pick, expiry, on_full, exclude, "
                                "empty_since FROM groups", -1, &statement, nullptr) != SQLITE_OK) {
        error = path() + ": " + sqlite3_errmsg(_db);
        return false;
    }
    while (sqlite3_step(statement) == SQLITE_ROW) {
        StoredGroup group;
        group.connection = column(statement, 0);
        group.name = column(statement, 1);
        group.options.group = group.name;
        group.options.connection = group.connection;
        group.options.filters = split(sqlite3_column_blob(statement, 2), sqlite3_column_bytes(statement, 2));
        group.options.events = sqlite3_column_int(statement, 3) != 0;
        group.options.pick = (AppPick)sqlite3_column_int(statement, 4);
        group.options.expirySeconds = (uint32_t)sqlite3_column_int64(statement, 5);
        group.options.onFull = (AppOnFull)sqlite3_column_int(statement, 6);
        group.options.excludeOrigins = split(sqlite3_column_blob(statement, 7), sqlite3_column_bytes(statement, 7));
        group.emptySinceMs = sqlite3_column_int64(statement, 8);
        index[std::make_pair(group.connection, group.name)] = groups.size();
        groups.push_back(std::move(group));
    }
    sqlite3_finalize(statement);

    nextId = 1;
    if (sqlite3_prepare_v2(_db, "SELECT id, connection, name, message FROM messages ORDER BY id", -1, &statement,
                           nullptr) != SQLITE_OK) {
        error = path() + ": " + sqlite3_errmsg(_db);
        return false;
    }
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const uint64_t id = (uint64_t)sqlite3_column_int64(statement, 0);
        nextId = std::max(nextId, id + 1);
        auto found = index.find(std::make_pair(column(statement, 1), column(statement, 2)));
        if (found == index.end()) continue;   // its group went; the row goes at the next drop
        std::shared_ptr<AppMessage> message(new AppMessage());
        if (!decodeMessage(sqlite3_column_blob(statement, 3), sqlite3_column_bytes(statement, 3), *message)) continue;
        groups[found->second].messages.emplace_back(id, std::move(message));
    }
    sqlite3_finalize(statement);
    // Rows of groups that are no longer kept.
    exec(_db, "DELETE FROM messages WHERE NOT EXISTS (SELECT 1 FROM groups g "
              "WHERE g.connection = messages.connection AND g.name = messages.name)");
    return true;
}

void GroupStore::close() {
    {
        std::lock_guard<std::mutex> guard(_lock);
        _quit = true;
    }
    _wake.notify_all();
    if (_writer.joinable()) _writer.join();
    if (_db) {
        sqlite3_close(_db);
        _db = nullptr;
    }
}

uint64_t GroupStore::queue(Change change) {
    uint64_t number;
    {
        std::lock_guard<std::mutex> guard(_lock);
        _queue.push_back(std::move(change));
        number = ++_queued;
    }
    _wake.notify_one();
    return number;
}

uint64_t GroupStore::putGroup(const std::string& name, const std::string& connection,
                              const AppConsumeOptions& options, int64_t emptySinceMs) {
    Change change;
    change.kind = Change::PutGroup;
    change.name = name;
    change.connection = connection;
    change.options = options;
    change.emptySinceMs = emptySinceMs;
    return queue(std::move(change));
}

uint64_t GroupStore::dropGroup(const std::string& name, const std::string& connection) {
    Change change;
    change.kind = Change::DropGroup;
    change.name = name;
    change.connection = connection;
    return queue(std::move(change));
}

uint64_t GroupStore::putMessage(uint64_t id, const std::string& name, const std::string& connection,
                                const AppMessage& message) {
    Change change;
    change.kind = Change::PutMessage;
    change.id = id;
    change.name = name;
    change.connection = connection;
    change.message = encodeMessage(message);
    return queue(std::move(change));
}

uint64_t GroupStore::dropMessage(uint64_t id) {
    Change change;
    change.kind = Change::DropMessage;
    change.id = id;
    return queue(std::move(change));
}

bool GroupStore::flush(uint32_t ms) {
    std::unique_lock<std::mutex> guard(_lock);
    const uint64_t wanted = _queued;
    return _done.wait_for(guard, std::chrono::milliseconds(ms), [&] { return _committed.load() >= wanted; });
}

void GroupStore::write() {
    std::unique_lock<std::mutex> guard(_lock);
    while (true) {
        _wake.wait(guard, [&] { return _quit || !_queue.empty(); });
        if (_queue.empty() && _quit) break;
        std::vector<Change> batch;
        batch.swap(_queue);
        const uint64_t last = _queued;
        guard.unlock();
        if (!apply(batch)) _failures++;
        _commits++;
        _changes += batch.size();
        // Whether or not it reached the disk: a writer that cannot write
        // must not hold every publisher's PUBACK for ever.
        _committed.store(last);
        if (_onCommit) _onCommit();
        guard.lock();
        _done.notify_all();
    }
}

bool GroupStore::apply(const std::vector<Change>& batch) {
    if (!exec(_db, "BEGIN")) return false;
    sqlite3_stmt* putGroup = nullptr;
    sqlite3_stmt* dropGroup = nullptr;
    sqlite3_stmt* dropGroupMessages = nullptr;
    sqlite3_stmt* putMessage = nullptr;
    sqlite3_stmt* dropMessage = nullptr;
    bool ok =
        sqlite3_prepare_v2(_db, "INSERT OR REPLACE INTO groups VALUES(?,?,?,?,?,?,?,?,?)", -1, &putGroup,
                           nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(_db, "DELETE FROM groups WHERE connection=? AND name=?", -1, &dropGroup, nullptr) ==
            SQLITE_OK &&
        sqlite3_prepare_v2(_db, "DELETE FROM messages WHERE connection=? AND name=?", -1, &dropGroupMessages,
                           nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(_db, "INSERT OR REPLACE INTO messages VALUES(?,?,?,?)", -1, &putMessage, nullptr) ==
            SQLITE_OK &&
        sqlite3_prepare_v2(_db, "DELETE FROM messages WHERE id=?", -1, &dropMessage, nullptr) == SQLITE_OK;
    for (size_t i = 0; ok && i < batch.size(); i++) {
        const Change& c = batch[i];
        sqlite3_stmt* s = nullptr;
        switch (c.kind) {
        case Change::PutGroup:
            s = putGroup;
            bindBlob(s, 1, c.connection);
            bindBlob(s, 2, c.name);
            bindBlob(s, 3, joined(c.options.filters));
            sqlite3_bind_int(s, 4, c.options.events ? 1 : 0);
            sqlite3_bind_int(s, 5, (int)c.options.pick);
            sqlite3_bind_int64(s, 6, c.options.expirySeconds);
            sqlite3_bind_int(s, 7, (int)c.options.onFull);
            bindBlob(s, 8, joined(c.options.excludeOrigins));
            sqlite3_bind_int64(s, 9, c.emptySinceMs);
            break;
        case Change::DropGroup:
            bindBlob(dropGroupMessages, 1, c.connection);
            bindBlob(dropGroupMessages, 2, c.name);
            ok = sqlite3_step(dropGroupMessages) == SQLITE_DONE;
            sqlite3_reset(dropGroupMessages);
            s = dropGroup;
            bindBlob(s, 1, c.connection);
            bindBlob(s, 2, c.name);
            break;
        case Change::PutMessage:
            s = putMessage;
            sqlite3_bind_int64(s, 1, (sqlite3_int64)c.id);
            bindBlob(s, 2, c.connection);
            bindBlob(s, 3, c.name);
            bindBlob(s, 4, c.message);
            break;
        case Change::DropMessage:
            s = dropMessage;
            sqlite3_bind_int64(s, 1, (sqlite3_int64)c.id);
            break;
        }
        ok = ok && sqlite3_step(s) == SQLITE_DONE;
        sqlite3_reset(s);
    }
    sqlite3_finalize(putGroup);
    sqlite3_finalize(dropGroup);
    sqlite3_finalize(dropGroupMessages);
    sqlite3_finalize(putMessage);
    sqlite3_finalize(dropMessage);
    if (!ok) {
        exec(_db, "ROLLBACK");
        return false;
    }
    return exec(_db, "COMMIT");
}

GroupStoreStatus GroupStore::status() const {
    GroupStoreStatus out;
    out.commits = _commits.load();
    out.changes = _changes.load();
    out.failures = _failures.load();
    return out;
}

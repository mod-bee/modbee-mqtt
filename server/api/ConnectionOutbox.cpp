// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ConnectionOutbox.h"
#include "DiagnosticsLog.h"

#include <cstdio>
#include "GroupStore.h"
#include "ServerOs.h"

#include "sqlite3.h"

namespace {

bool exec(sqlite3* db, const char* sql) { return sqlite3_exec(db, sql, nullptr, nullptr, nullptr) == SQLITE_OK; }

// A statement, finalized however the function ends.
struct Statement {
    sqlite3_stmt* s = nullptr;
    Statement(sqlite3* db, const char* sql) { sqlite3_prepare_v2(db, sql, -1, &s, nullptr); }
    ~Statement() { sqlite3_finalize(s); }
    void text(int at, const std::string& value) { sqlite3_bind_blob(s, at, value.data(), (int)value.size(), SQLITE_TRANSIENT); }
    void number(int at, int64_t value) { sqlite3_bind_int64(s, at, value); }
};

std::string column(sqlite3_stmt* s, int at) {
    const void* data = sqlite3_column_blob(s, at);
    return data ? std::string((const char*)data, (size_t)sqlite3_column_bytes(s, at)) : std::string();
}

} // namespace

ConnectionOutbox::~ConnectionOutbox() { close(); }

bool ConnectionOutbox::open(const std::string& directory, bool syncFull, std::string& error) {
    close();
    std::string file = ":memory:";
    if (!directory.empty()) {
        server_os::makeDirectory(directory, 0700);
        file = directory + "/outbox.db";
    }
    if (sqlite3_open_v2(file.c_str(), &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
        error = file + ": " + (_db ? sqlite3_errmsg(_db) : "cannot open");
        close();
        return false;
    }
    sqlite3_busy_timeout(_db, 5000);
    if (!exec(_db, "PRAGMA journal_mode=WAL") ||
        !exec(_db, syncFull ? "PRAGMA synchronous=FULL" : "PRAGMA synchronous=NORMAL") ||
        !exec(_db, "CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY, connection BLOB, bytes INTEGER, "
                   "taken INTEGER, message BLOB);"
                   "CREATE INDEX IF NOT EXISTS outbox_by_connection ON outbox(connection, id);"
                   "CREATE TABLE IF NOT EXISTS replay(node BLOB, serial INTEGER, device BLOB, name BLOB, "
                   "type INTEGER, nullvalue INTEGER, bits INTEGER, at INTEGER, quality INTEGER, "
                   "PRIMARY KEY(node, serial)) WITHOUT ROWID;")) {
        error = file + ": " + sqlite3_errmsg(_db);
        close();
        return false;
    }
    return true;
}

void ConnectionOutbox::close() {
    if (_db) sqlite3_close(_db);
    _db = nullptr;
}

bool ConnectionOutbox::push(const std::string& connection, const AppMessage& message, uint64_t& id) {
    if (!_db) return false;
    Statement s(_db, "INSERT INTO outbox(connection, bytes, taken, message) VALUES(?,?,?,?)");
    s.text(1, connection);
    s.number(2, (int64_t)(message.topic.size() + message.payload.size()));
    s.number(3, message.brokerTimeMs);
    s.text(4, encodeAppMessage(message));
    if (sqlite3_step(s.s) != SQLITE_DONE) return false;
    id = (uint64_t)sqlite3_last_insert_rowid(_db);
    return true;
}

std::vector<ConnectionOutbox::Kept> ConnectionOutbox::next(const std::string& connection, uint64_t afterId,
                                                           size_t limit) {
    std::vector<Kept> out;
    if (!_db) return out;
    Statement s(_db, "SELECT id, bytes, message FROM outbox WHERE connection=? AND id>? ORDER BY id LIMIT ?");
    s.text(1, connection);
    s.number(2, (int64_t)afterId);
    s.number(3, (int64_t)limit);
    std::vector<uint64_t> unreadable;
    while (sqlite3_step(s.s) == SQLITE_ROW) {
        Kept k;
        k.id = (uint64_t)sqlite3_column_int64(s.s, 0);
        k.bytes = (size_t)sqlite3_column_int64(s.s, 1);
        const std::string blob = column(s.s, 2);
        if (decodeAppMessage(blob.data(), (int)blob.size(), k.message)) out.push_back(std::move(k));
        else unreadable.push_back(k.id);
    }
    // A row that will never read (a damaged file) is let go: kept, it held its
    // place at the head for ever, and enough of them stopped everything behind.
    for (uint64_t id : unreadable) {
        diagnostics::error("connections", "outbox.db: message %llu of %s could not be read - dropped\n",
                     (unsigned long long)id, connection.c_str());
        drop(id);
    }
    return out;
}

void ConnectionOutbox::drop(uint64_t id) {
    if (!_db) return;
    Statement s(_db, "DELETE FROM outbox WHERE id=?");
    s.number(1, (int64_t)id);
    sqlite3_step(s.s);
}

bool ConnectionOutbox::dropOldest(const std::string& connection, Kept* dropped) {
    std::vector<Kept> oldest = next(connection, 0, 1);
    if (oldest.empty()) return false;
    drop(oldest[0].id);
    if (dropped) *dropped = std::move(oldest[0]);
    return true;
}

uint64_t ConnectionOutbox::dropAll(const std::string& connection) {
    if (!_db) return 0;
    Statement s(_db, "DELETE FROM outbox WHERE connection=?");
    s.text(1, connection);
    sqlite3_step(s.s);
    return (uint64_t)sqlite3_changes(_db);
}

bool ConnectionOutbox::replayApply(const std::string& node, const std::vector<ReplayChange>& changes) {
    if (!_db) return false;
    if (changes.empty()) return true;
    if (!exec(_db, "BEGIN IMMEDIATE")) return false;
    bool ok = true;
    {
        Statement held(_db, "INSERT OR REPLACE INTO replay(node, serial, device, name, type, nullvalue, bits, at, quality) "
                            "VALUES(?,?,?,?,?,?,?,?,?)");
        Statement released(_db, "DELETE FROM replay WHERE node=? AND serial=?");
        Statement cleared(_db, "DELETE FROM replay WHERE node=?");
        for (const ReplayChange& c : changes) {
            Statement& s = c.kind == ReplayChange::Held ? held : c.kind == ReplayChange::Released ? released : cleared;
            sqlite3_reset(s.s);
            s.text(1, node);
            if (c.kind != ReplayChange::Cleared) s.number(2, (int64_t)c.value.serial);
            if (c.kind == ReplayChange::Held) {
                s.text(3, c.value.device);
                s.text(4, c.value.name);
                s.number(5, (int64_t)c.value.type);
                s.number(6, c.value.isNull ? 1 : 0);
                s.number(7, (int64_t)c.value.bits);
                s.number(8, (int64_t)c.value.timestamp);
                s.number(9, c.value.quality);
            }
            if (sqlite3_step(s.s) != SQLITE_DONE) {
                ok = false;
                break;
            }
        }
    }
    if (!ok || !exec(_db, "COMMIT")) {
        exec(_db, "ROLLBACK");
        return false;
    }
    return true;
}

std::vector<ConnectionOutbox::Replay> ConnectionOutbox::replayHeld(const std::string& node) {
    std::vector<Replay> out;
    if (!_db) return out;
    Statement s(_db, "SELECT serial, device, name, type, nullvalue, bits, at, quality FROM replay WHERE node=? "
                     "ORDER BY serial");
    s.text(1, node);
    while (sqlite3_step(s.s) == SQLITE_ROW) {
        Replay r;
        r.serial = (uint64_t)sqlite3_column_int64(s.s, 0);
        r.device = column(s.s, 1);
        r.name = column(s.s, 2);
        r.type = (uint32_t)sqlite3_column_int64(s.s, 3);
        r.isNull = sqlite3_column_int64(s.s, 4) != 0;
        r.bits = (uint64_t)sqlite3_column_int64(s.s, 5);
        r.timestamp = (uint64_t)sqlite3_column_int64(s.s, 6);
        r.quality = (int32_t)sqlite3_column_int64(s.s, 7);
        out.push_back(std::move(r));
    }
    return out;
}

void ConnectionOutbox::replayDropAll(const std::string& node) {
    if (!_db) return;
    Statement s(_db, "DELETE FROM replay WHERE node=?");
    s.text(1, node);
    sqlite3_step(s.s);
}

ConnectionOutbox::Totals ConnectionOutbox::totals(const std::string& connection) {
    Totals t;
    if (!_db) return t;
    Statement s(_db, "SELECT COUNT(*), COALESCE(SUM(bytes),0), COALESCE(MIN(taken),0) FROM outbox WHERE connection=?");
    s.text(1, connection);
    if (sqlite3_step(s.s) == SQLITE_ROW) {
        t.messages = (uint64_t)sqlite3_column_int64(s.s, 0);
        t.bytes = (uint64_t)sqlite3_column_int64(s.s, 1);
        t.oldestMs = sqlite3_column_int64(s.s, 2);
    }
    return t;
}

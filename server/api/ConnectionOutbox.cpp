// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "ConnectionOutbox.h"

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
                   "CREATE INDEX IF NOT EXISTS outbox_by_connection ON outbox(connection, id);")) {
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
        std::fprintf(stderr, "outbox.db: message %llu of %s could not be read - dropped\n",
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

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AuthStore.h"

#include <chrono>
#include <cstdio>

namespace {

// Filters are stored one per line: a topic filter cannot usefully contain a
// newline, and the admin path refuses one that does.
std::string joinLines(const std::vector<std::string>& items) {
    std::string out;
    for (size_t i = 0; i < items.size(); i++) {
        if (i) out.push_back('\n');
        out += items[i];
    }
    return out;
}

std::vector<std::string> splitLines(const unsigned char* text) {
    std::vector<std::string> out;
    if (text == nullptr || *text == 0) return out;
    std::string all((const char*)text);
    size_t from = 0;
    for (;;) {
        const size_t next = all.find('\n', from);
        out.push_back(all.substr(from, next - from));
        if (next == std::string::npos) return out;
        from = next + 1;
    }
}

std::string column(sqlite3_stmt* s, int i) {
    const unsigned char* text = sqlite3_column_text(s, i);
    return text ? std::string((const char*)text) : std::string();
}

int64_t now() {
    return (int64_t)std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

const char kUserColumns[] =
    "username, hash, generated, enabled, pending, admin, publish, subscribe, provisioned_by, created, updated";

void readUser(sqlite3_stmt* s, AuthUser& u) {
    u.username = column(s, 0);
    u.hash = column(s, 1);
    u.generated = sqlite3_column_int(s, 2) != 0;
    u.enabled = sqlite3_column_int(s, 3) != 0;
    u.pending = sqlite3_column_int(s, 4) != 0;
    u.admin = sqlite3_column_int(s, 5) != 0;
    u.publish = splitLines(sqlite3_column_text(s, 6));
    u.subscribe = splitLines(sqlite3_column_text(s, 7));
    u.provisionedBy = column(s, 8);
    u.created = sqlite3_column_int64(s, 9);
    u.updated = sqlite3_column_int64(s, 10);
}

void readKey(sqlite3_stmt* s, AuthKey& k) {
    k.name = column(s, 0);
    k.hash = column(s, 1);
    k.manualApproval = sqlite3_column_int(s, 2) != 0;
    k.publish = splitLines(sqlite3_column_text(s, 3));
    k.subscribe = splitLines(sqlite3_column_text(s, 4));
    k.usesLeft = sqlite3_column_int64(s, 5);
    k.expires = sqlite3_column_int64(s, 6);
    k.device = column(s, 7);
    k.created = sqlite3_column_int64(s, 8);
}

const char kKeyColumns[] = "name, hash, manual, publish, subscribe, uses_left, expires, device, created";

struct Statement {
    sqlite3_stmt* s = nullptr;
    Statement(sqlite3* db, const char* sql) { sqlite3_prepare_v2(db, sql, -1, &s, nullptr); }
    ~Statement() { sqlite3_finalize(s); }
    bool ok() const { return s != nullptr; }
    void text(int i, const std::string& v) { sqlite3_bind_text(s, i, v.data(), (int)v.size(), SQLITE_TRANSIENT); }
    void integer(int i, int64_t v) { sqlite3_bind_int64(s, i, v); }
    int step() { return sqlite3_step(s); }
};

} // namespace

AuthStore::~AuthStore() { close(); }

bool AuthStore::exec(const char* sql) {
    return sqlite3_exec(_writer, sql, nullptr, nullptr, nullptr) == SQLITE_OK;
}

std::string AuthStore::lastError() const {
    return _writer ? sqlite3_errmsg(_writer) : "the login store is not open";
}

bool AuthStore::open(const std::string& path, size_t readers, std::string& error) {
    close();
    _path = path;
    if (sqlite3_open_v2(path.c_str(), &_writer,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                        nullptr) != SQLITE_OK) {
        error = "cannot open the login store " + path + ": " +
                (_writer ? sqlite3_errmsg(_writer) : "out of memory") +
                " (in Docker, mount a volume at its folder - docs/docker.md)";
        close();
        return false;
    }
    sqlite3_busy_timeout(_writer, 5000);
    const bool schema =
        exec("PRAGMA journal_mode=WAL") && exec("PRAGMA synchronous=NORMAL") &&
        exec("CREATE TABLE IF NOT EXISTS users ("
             " username TEXT PRIMARY KEY, hash TEXT NOT NULL, generated INTEGER NOT NULL,"
             " enabled INTEGER NOT NULL, pending INTEGER NOT NULL, admin INTEGER NOT NULL,"
             " publish TEXT NOT NULL, subscribe TEXT NOT NULL, provisioned_by TEXT NOT NULL,"
             " created INTEGER NOT NULL, updated INTEGER NOT NULL) WITHOUT ROWID") &&
        exec("CREATE TABLE IF NOT EXISTS keys ("
             " name TEXT PRIMARY KEY, hash TEXT NOT NULL, manual INTEGER NOT NULL,"
             " publish TEXT NOT NULL, subscribe TEXT NOT NULL, uses_left INTEGER NOT NULL,"
             " expires INTEGER NOT NULL, device TEXT NOT NULL, created INTEGER NOT NULL)") &&
        exec("CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT)") &&
        exec("INSERT OR IGNORE INTO meta VALUES ('schema', '1')");
    if (!schema) {
        error = "cannot set up the login store " + path + ": " + sqlite3_errmsg(_writer);
        close();
        return false;
    }
    for (size_t i = 0; i < readers; i++) {
        Reader r;
        if (sqlite3_open_v2(path.c_str(), &r.db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                            nullptr) != SQLITE_OK) {
            error = "cannot open the login store " + path + " to read";
            if (r.db) sqlite3_close(r.db);
            close();
            return false;
        }
        sqlite3_busy_timeout(r.db, 5000);
        const std::string sql = std::string("SELECT ") + kUserColumns + " FROM users WHERE username = ?1";
        sqlite3_prepare_v2(r.db, sql.c_str(), -1, &r.getUser, nullptr);
        _readers.push_back(r);
    }
    return true;
}

void AuthStore::close() {
    for (Reader& r : _readers) {
        sqlite3_finalize(r.getUser);
        sqlite3_close(r.db);
    }
    _readers.clear();
    if (_writer) sqlite3_close(_writer);
    _writer = nullptr;
}

bool AuthStore::getUser(size_t r, const std::string& username, AuthUser& out) {
    if (r >= _readers.size() || _readers[r].getUser == nullptr) return false;
    sqlite3_stmt* s = _readers[r].getUser;
    sqlite3_reset(s);
    sqlite3_bind_text(s, 1, username.data(), (int)username.size(), SQLITE_TRANSIENT);
    const bool found = sqlite3_step(s) == SQLITE_ROW;
    if (found) readUser(s, out);
    sqlite3_reset(s);
    return found;
}

AuthResult AuthStore::putUser(const AuthUser& u, bool replace) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return AuthResult::Failed;
    const std::string sql = std::string(replace ? "INSERT OR REPLACE" : "INSERT") + " INTO users (" +
                            kUserColumns + ") VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)";
    Statement st(_writer, sql.c_str());
    if (!st.ok()) return AuthResult::Failed;
    const int64_t t = now();
    st.text(1, u.username);
    st.text(2, u.hash);
    st.integer(3, u.generated);
    st.integer(4, u.enabled);
    st.integer(5, u.pending);
    st.integer(6, u.admin);
    st.text(7, joinLines(u.publish));
    st.text(8, joinLines(u.subscribe));
    st.text(9, u.provisionedBy);
    st.integer(10, u.created ? u.created : t);
    st.integer(11, t);
    const int rc = st.step();
    if (rc == SQLITE_CONSTRAINT) return AuthResult::Exists;
    if (rc != SQLITE_DONE) return AuthResult::Failed;
    changed();
    return AuthResult::Ok;
}

AuthResult AuthStore::removeUser(const std::string& username) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return AuthResult::Failed;
    Statement st(_writer, "DELETE FROM users WHERE username = ?1");
    st.text(1, username);
    if (st.step() != SQLITE_DONE) return AuthResult::Failed;
    if (sqlite3_changes(_writer) == 0) return AuthResult::NotFound;
    changed();
    return AuthResult::Ok;
}

bool AuthStore::findUser(const std::string& username, AuthUser& out) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return false;
    const std::string sql = std::string("SELECT ") + kUserColumns + " FROM users WHERE username = ?1";
    Statement st(_writer, sql.c_str());
    st.text(1, username);
    if (st.step() != SQLITE_ROW) return false;
    readUser(st.s, out);
    return true;
}

bool AuthStore::listUsers(const std::string& prefix, const std::string& after, size_t limit,
                          std::vector<AuthUser>& out) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return false;
    // In name order, a page at a time: "after" is the last name of the page
    // before. substr() rather than LIKE, so a prefix containing % or _ means
    // what it says.
    const std::string sql = std::string("SELECT ") + kUserColumns +
                            " FROM users WHERE username > ?1 AND substr(username, 1, length(?2)) = ?2"
                            " ORDER BY username LIMIT ?3";
    Statement st(_writer, sql.c_str());
    st.text(1, after);
    st.text(2, prefix);
    st.integer(3, (int64_t)limit);
    out.clear();
    while (st.step() == SQLITE_ROW) {
        AuthUser u;
        readUser(st.s, u);
        out.push_back(u);
    }
    return true;
}

size_t AuthStore::userCount() {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return 0;
    Statement st(_writer, "SELECT count(*) FROM users");
    return st.step() == SQLITE_ROW ? (size_t)sqlite3_column_int64(st.s, 0) : 0;
}

AuthResult AuthStore::putKey(const AuthKey& k) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return AuthResult::Failed;
    const std::string sql = std::string("INSERT INTO keys (") + kKeyColumns +
                            ") VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)";
    Statement st(_writer, sql.c_str());
    st.text(1, k.name);
    st.text(2, k.hash);
    st.integer(3, k.manualApproval);
    st.text(4, joinLines(k.publish));
    st.text(5, joinLines(k.subscribe));
    st.integer(6, k.usesLeft);
    st.integer(7, k.expires);
    st.text(8, k.device);
    st.integer(9, k.created ? k.created : now());
    const int rc = st.step();
    if (rc == SQLITE_CONSTRAINT) return AuthResult::Exists;
    if (rc != SQLITE_DONE) return AuthResult::Failed;
    changed();
    return AuthResult::Ok;
}

AuthResult AuthStore::removeKey(const std::string& name) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return AuthResult::Failed;
    Statement st(_writer, "DELETE FROM keys WHERE name = ?1");
    st.text(1, name);
    if (st.step() != SQLITE_DONE) return AuthResult::Failed;
    if (sqlite3_changes(_writer) == 0) return AuthResult::NotFound;
    changed();
    return AuthResult::Ok;
}

bool AuthStore::findKey(const std::string& name, AuthKey& out) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return false;
    const std::string sql = std::string("SELECT ") + kKeyColumns + " FROM keys WHERE name = ?1";
    Statement st(_writer, sql.c_str());
    st.text(1, name);
    if (st.step() != SQLITE_ROW) return false;
    readKey(st.s, out);
    return true;
}

bool AuthStore::listKeys(std::vector<AuthKey>& out) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return false;
    const std::string sql = std::string("SELECT ") + kKeyColumns + " FROM keys ORDER BY name";
    Statement st(_writer, sql.c_str());
    out.clear();
    while (st.step() == SQLITE_ROW) {
        AuthKey k;
        readKey(st.s, k);
        out.push_back(k);
    }
    return true;
}

bool AuthStore::spendKey(const std::string& name) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) return false;
    // Unlimited (-1) is left alone; a counted key goes down by one, and only
    // while it has a use left - one statement, so two devices cannot both
    // spend the last use.
    Statement st(_writer, "UPDATE keys SET uses_left = uses_left - 1 WHERE name = ?1 AND uses_left > 0");
    st.text(1, name);
    if (st.step() != SQLITE_DONE) return false;
    if (sqlite3_changes(_writer) == 1) {
        changed();
        return true;
    }
    Statement unlimited(_writer, "SELECT uses_left FROM keys WHERE name = ?1");
    unlimited.text(1, name);
    return unlimited.step() == SQLITE_ROW && sqlite3_column_int64(unlimited.s, 0) < 0;
}

bool AuthStore::restore(const std::string& fromPath, const std::string& toPath, std::string& error) {
    sqlite3* from = nullptr;
    sqlite3* to = nullptr;
    if (sqlite3_open_v2(fromPath.c_str(), &from, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
        error = "cannot read " + fromPath + ": " + (from ? sqlite3_errmsg(from) : "out of memory");
        if (from) sqlite3_close(from);
        return false;
    }
    // Is it a login store at all?
    sqlite3_stmt* check = nullptr;
    const bool isStore = sqlite3_prepare_v2(from, "SELECT count(*) FROM users", -1, &check, nullptr) == SQLITE_OK &&
                         sqlite3_step(check) == SQLITE_ROW;
    sqlite3_finalize(check);
    if (!isStore) {
        error = fromPath + " is not a modbee login database";
        sqlite3_close(from);
        return false;
    }
    if (sqlite3_open_v2(toPath.c_str(), &to, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
        error = "cannot write " + toPath + ": " + (to ? sqlite3_errmsg(to) : "out of memory");
        if (to) sqlite3_close(to);
        sqlite3_close(from);
        return false;
    }
    sqlite3_busy_timeout(to, 2000);
    sqlite3_backup* b = sqlite3_backup_init(to, "main", from, "main");
    bool ok = b != nullptr;
    if (b) {
        ok = sqlite3_backup_step(b, -1) == SQLITE_DONE;
        sqlite3_backup_finish(b);
    }
    if (!ok) {
        error = std::string("restore failed: ") + sqlite3_errmsg(to) +
                (sqlite3_errcode(to) == SQLITE_BUSY ? " - stop the broker first" : "");
    }
    sqlite3_close(to);
    sqlite3_close(from);
    return ok;
}

bool AuthStore::backup(const std::string& toPath, std::string& error) {
    std::lock_guard<std::mutex> lock(_write);
    if (!_writer) {
        error = "the login store is not open";
        return false;
    }
    const std::string temporary = toPath + ".new";
    sqlite3* to = nullptr;
    if (sqlite3_open(temporary.c_str(), &to) != SQLITE_OK) {
        error = "cannot write " + temporary + ": " + (to ? sqlite3_errmsg(to) : "out of memory");
        if (to) sqlite3_close(to);
        return false;
    }
    sqlite3_backup* b = sqlite3_backup_init(to, "main", _writer, "main");
    bool ok = b != nullptr;
    if (b) {
        ok = sqlite3_backup_step(b, -1) == SQLITE_DONE;
        sqlite3_backup_finish(b);
    }
    if (!ok) error = std::string("backup failed: ") + sqlite3_errmsg(to);
    sqlite3_close(to);
    if (ok && std::rename(temporary.c_str(), toPath.c_str()) != 0) {
        error = "cannot rename the backup into place: " + toPath;
        ok = false;
    }
    if (!ok) std::remove(temporary.c_str());
    return ok;
}

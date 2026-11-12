// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "DiagnosticsLog.h"

#include <algorithm>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstring>

namespace {

// The log's shape, in its meta table. A file of another shape is set aside, not read.
const char kSchema[] = "1";

// The writer is woken early once this many wait; it writes them this many to a transaction, the limits checked
// after each.
const size_t kBatch = 2048;
const size_t kChunk = 256;

// What one entry may hold: a longer text is cut, so no entry is larger than a few kB.
const size_t kNameBytes = 256;
const size_t kReasonBytes = 1024;
const size_t kDetailBytes = 2048;

void cut(std::string& text, size_t most) {
    if (text.size() <= most) return;
    size_t at = most - 3;
    while (at > 0 && ((unsigned char)text[at] & 0xC0) == 0x80) at--;    // not inside a UTF-8 character
    text.resize(at);
    text += "...";
}

// Once the file's pages are this full, the oldest entries go until it is down to the second.
const double kTrimAt = 0.90;
const double kTrimTo = 0.75;

const char kColumns[] = "id, time, kind, client, login, address, code, reason, detail";

std::atomic<DiagnosticsLog*> g_installed{nullptr};

int64_t nowMs() {
    return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string column(sqlite3_stmt* s, int i) {
    const unsigned char* text = sqlite3_column_text(s, i);
    return text ? std::string((const char*)text) : std::string();
}

void bindText(sqlite3_stmt* s, int i, const std::string& v) {
    sqlite3_bind_text(s, i, v.data(), (int)v.size(), SQLITE_TRANSIENT);
}

bool exec(sqlite3* db, const char* sql) { return sqlite3_exec(db, sql, nullptr, nullptr, nullptr) == SQLITE_OK; }

// The entries a query asks for, from any open connection to a log.
bool runQuery(sqlite3* db, const DiagnosticsQuery& q, std::vector<DiagnosticsEntry>& out, std::string& error) {
    std::string sql = std::string("SELECT ") + kColumns + " FROM log WHERE 1=1";
    if (!q.clientId.empty()) sql += " AND client = ?";
    if (!q.login.empty()) sql += " AND login = ?";
    if (!q.kinds.empty()) {
        sql += " AND kind IN (";
        for (size_t i = 0; i < q.kinds.size(); i++) sql += i ? ",?" : "?";
        sql += ")";
    }
    if (q.sinceMs > 0) sql += " AND time >= ?";
    if (q.untilMs > 0) sql += " AND time < ?";
    if (q.beforeId > 0) sql += " AND id < ?";
    if (q.forward) sql += " AND id > ?";
    sql += q.forward ? " ORDER BY id ASC" : " ORDER BY id DESC";
    sql += " LIMIT ?";
    sqlite3_stmt* s = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &s, nullptr) != SQLITE_OK) {
        // A log nothing has been written to yet has no table: nothing in it, not a failure.
        const std::string why = sqlite3_errmsg(db);
        if (why.find("no such table") != std::string::npos) return true;
        error = "the diagnostics log cannot be read: " + why;
        return false;
    }
    int i = 1;
    if (!q.clientId.empty()) bindText(s, i++, q.clientId);
    if (!q.login.empty()) bindText(s, i++, q.login);
    for (const std::string& kind : q.kinds) bindText(s, i++, kind);
    if (q.sinceMs > 0) sqlite3_bind_int64(s, i++, q.sinceMs);
    if (q.untilMs > 0) sqlite3_bind_int64(s, i++, q.untilMs);
    if (q.beforeId > 0) sqlite3_bind_int64(s, i++, q.beforeId);
    if (q.forward) sqlite3_bind_int64(s, i++, q.afterId);
    sqlite3_bind_int64(s, i++, (int64_t)(q.limit == 0 ? 100 : q.limit));
    int rc;
    while ((rc = sqlite3_step(s)) == SQLITE_ROW) {
        DiagnosticsEntry e;
        e.id = sqlite3_column_int64(s, 0);
        e.timeMs = sqlite3_column_int64(s, 1);
        e.kind = column(s, 2);
        e.clientId = column(s, 3);
        e.login = column(s, 4);
        e.address = column(s, 5);
        e.reasonCode = sqlite3_column_type(s, 6) == SQLITE_NULL ? -1 : sqlite3_column_int(s, 6);
        e.reason = column(s, 7);
        e.detail = column(s, 8);
        out.push_back(std::move(e));
    }
    if (rc != SQLITE_DONE) error = "the diagnostics log cannot be read: " + std::string(sqlite3_errmsg(db));
    sqlite3_finalize(s);
    return rc == SQLITE_DONE;
}

bool fileExists(const std::string& path) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (f) std::fclose(f);
    return f != nullptr;
}

} // namespace

DiagnosticsLog::~DiagnosticsLog() { stop(); }

void DiagnosticsLog::install(DiagnosticsLog* log) { g_installed.store(log, std::memory_order_release); }

DiagnosticsLog* DiagnosticsLog::installed() { return g_installed.load(std::memory_order_acquire); }

bool DiagnosticsLog::start(const std::string& path, const DiagnosticsSettings& settings, std::string& error) {
    stop();
    _path = path;
    // Opened here once, so a file that cannot be made is said at the start rather than found out later; a log
    // started off is opened by its writer when it is turned on.
    if (settings.enabled && !openFile(error)) {
        closeFile();
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _settings = settings;
        _quit = false;
        _queue.clear();
        _started = true;
    }
    _thread = std::thread([this] { run(); });
    return true;
}

void DiagnosticsLog::stop() {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_started) return;
        _quit = true;
    }
    _wake.notify_all();
    if (_thread.joinable()) _thread.join();
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _started = false;
    }
    DiagnosticsLog* self = this;
    g_installed.compare_exchange_strong(self, nullptr);
    std::lock_guard<std::mutex> lock(_readMutex);
    if (_reader) sqlite3_close(_reader);
    _reader = nullptr;
}

void DiagnosticsLog::record(DiagnosticsEntry entry) {
    if (entry.timeMs == 0) entry.timeMs = nowMs();
    cut(entry.clientId, kNameBytes);
    cut(entry.login, kNameBytes);
    cut(entry.address, kNameBytes);
    cut(entry.reason, kReasonBytes);
    cut(entry.detail, kDetailBytes);
    bool wake = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_started || _quit || !_settings.enabled) return;
        if (_queue.size() >= _settings.queue) {
            _dropped++;
            return;
        }
        _queue.push_back(std::move(entry));
        _recorded++;
        wake = _queue.size() == kBatch;
    }
    if (wake) _wake.notify_one();
}

void DiagnosticsLog::configure(const DiagnosticsSettings& settings) {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _settings = settings;
        _flushWanted = true;      // the limits act now, not at the next pass
    }
    _wake.notify_all();
}

DiagnosticsSettings DiagnosticsLog::settings() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _settings;
}

bool DiagnosticsLog::flush(int ms) {
    std::unique_lock<std::mutex> lock(_mutex);
    if (!_started) return true;
    const uint64_t target = _recorded;
    _flushWanted = true;
    _wake.notify_all();
    return _written.wait_for(lock, std::chrono::milliseconds(ms), [&] { return _done >= target; });
}

DiagnosticsStatus DiagnosticsLog::status() const {
    std::lock_guard<std::mutex> lock(_mutex);
    DiagnosticsStatus s;
    s.enabled = _settings.enabled;
    s.open = _open.load();
    s.path = _path;
    s.written = _writtenCount;
    s.dropped = _dropped;
    s.failed = _failed;
    s.removed = _removed;
    s.lastError = _lastError;
    return s;
}

bool DiagnosticsLog::openFile(std::string& error) {
    if (_db) return true;
    if (sqlite3_open_v2(_path.c_str(), &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
                        nullptr) != SQLITE_OK) {
        error = "cannot open the diagnostics log " + _path + ": " + (_db ? sqlite3_errmsg(_db) : "out of memory");
        closeFile();
        return false;
    }
    sqlite3_busy_timeout(_db, 2000);
    // INCREMENTAL auto-vacuum, set before the first table: pages freed by deleting old entries are written over
    // by new ones, and handed back to the file system only when the file is over its size (a lowered max-size).
    const bool ok =
        exec(_db, "PRAGMA auto_vacuum=INCREMENTAL") && exec(_db, "PRAGMA journal_mode=WAL") &&
        exec(_db, "PRAGMA synchronous=NORMAL") && exec(_db, "PRAGMA journal_size_limit=4194304") &&
        exec(_db, "CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT)") &&
        exec(_db, "CREATE TABLE IF NOT EXISTS log ("
                  " id INTEGER PRIMARY KEY AUTOINCREMENT, time INTEGER NOT NULL, kind TEXT NOT NULL,"
                  " client TEXT NOT NULL, login TEXT NOT NULL, address TEXT NOT NULL, code INTEGER,"
                  " reason TEXT NOT NULL, detail TEXT NOT NULL)") &&
        exec(_db, "CREATE INDEX IF NOT EXISTS log_time ON log (time)") &&
        exec(_db, "CREATE INDEX IF NOT EXISTS log_client ON log (client)") &&
        exec(_db, "CREATE INDEX IF NOT EXISTS log_login ON log (login)") &&
        exec(_db, (std::string("INSERT OR IGNORE INTO meta VALUES ('schema', '") + kSchema + "')").c_str());
    if (!ok) {
        error = "cannot set up the diagnostics log " + _path + ": " + sqlite3_errmsg(_db);
        closeFile();
        return false;
    }
    std::string found;
    {
        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(_db, "SELECT value FROM meta WHERE name = 'schema'", -1, &s, nullptr) == SQLITE_OK &&
            sqlite3_step(s) == SQLITE_ROW) {
            found = column(s, 0);
        }
        sqlite3_finalize(s);
    }
    if (found != kSchema) {
        error = "the diagnostics log " + _path + " was made by another version (schema " + found +
                "); remove it and a new one is started";
        closeFile();
        return false;
    }
    if (sqlite3_prepare_v2(_db,
                           "INSERT INTO log (time, kind, client, login, address, code, reason, detail)"
                           " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                           -1, &_insert, nullptr) != SQLITE_OK) {
        error = "cannot set up the diagnostics log " + _path + ": " + sqlite3_errmsg(_db);
        closeFile();
        return false;
    }
    _pageSize = pragma("page_size");
    if (_pageSize <= 0) _pageSize = 4096;
    _open = true;
    return true;
}

void DiagnosticsLog::closeFile() {
    _open = false;
    if (_insert) sqlite3_finalize(_insert);
    _insert = nullptr;
    if (_db) sqlite3_close(_db);
    _db = nullptr;
}

int64_t DiagnosticsLog::pragma(const char* name) {
    int64_t value = -1;
    sqlite3_stmt* s = nullptr;
    const std::string sql = std::string("PRAGMA ") + name;
    if (sqlite3_prepare_v2(_db, sql.c_str(), -1, &s, nullptr) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW) {
        value = sqlite3_column_int64(s, 0);
    }
    sqlite3_finalize(s);
    return value;
}

bool DiagnosticsLog::writeBatch(std::vector<DiagnosticsEntry>& batch) {
    for (int attempt = 0; attempt < 4; attempt++) {
        if (!exec(_db, "BEGIN")) return false;
        int rc = SQLITE_DONE;
        for (const DiagnosticsEntry& e : batch) {
            sqlite3_reset(_insert);
            sqlite3_bind_int64(_insert, 1, e.timeMs);
            bindText(_insert, 2, e.kind);
            bindText(_insert, 3, e.clientId);
            bindText(_insert, 4, e.login);
            bindText(_insert, 5, e.address);
            if (e.reasonCode < 0) sqlite3_bind_null(_insert, 6);
            else sqlite3_bind_int(_insert, 6, e.reasonCode);
            bindText(_insert, 7, e.reason);
            bindText(_insert, 8, e.detail);
            rc = sqlite3_step(_insert);
            if (rc != SQLITE_DONE) break;
        }
        sqlite3_reset(_insert);
        if (rc == SQLITE_DONE && exec(_db, "COMMIT")) return true;
        const bool full = rc == SQLITE_FULL || sqlite3_errcode(_db) == SQLITE_FULL;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _lastError = sqlite3_errmsg(_db);
        }
        exec(_db, "ROLLBACK");
        // At max-size (max_page_count) with no room left: the oldest third goes - then two thirds, then all - and
        // the batch is tried again.
        if (!full) return false;
        makeRoom(attempt == 0 ? 1.0 / 3 : attempt == 1 ? 2.0 / 3 : 1.0);
    }
    return false;
}

void DiagnosticsLog::makeRoom(double fraction) {
    int64_t lo = 0, hi = 0;
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(_db, "SELECT min(id), max(id) FROM log", -1, &st, nullptr) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW) {
        lo = sqlite3_column_int64(st, 0);
        hi = sqlite3_column_int64(st, 1);
    }
    sqlite3_finalize(st);
    if (hi == 0 || hi < lo) return;
    const int64_t n = std::max<int64_t>(1, (int64_t)((hi - lo + 1) * fraction + 0.5));
    st = nullptr;
    if (sqlite3_prepare_v2(_db, "DELETE FROM log WHERE id < ?", -1, &st, nullptr) != SQLITE_OK) return;
    sqlite3_bind_int64(st, 1, lo + n);
    uint64_t removed = 0;
    if (sqlite3_step(st) == SQLITE_DONE) removed = (uint64_t)sqlite3_changes(_db);
    sqlite3_finalize(st);
    std::lock_guard<std::mutex> lock(_mutex);
    _removed += removed;
}

void DiagnosticsLog::trimNow() {
    std::unique_lock<std::mutex> lock(_mutex);
    if (!_started) return;
    _flushWanted = true;
    _wake.notify_all();
    // Two passes: the one under way may have begun before the request.
    const uint64_t target = _passes + 2;
    _written.wait_for(lock, std::chrono::seconds(5), [&] { return _passes >= target; });
}

void DiagnosticsLog::trim(bool ageToo) {
    DiagnosticsSettings s;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        s = _settings;
    }
    uint64_t removed = 0;
    const auto run = [&](const char* sql, int64_t value) {
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(_db, sql, -1, &st, nullptr) != SQLITE_OK) return;
        sqlite3_bind_int64(st, 1, value);
        if (sqlite3_step(st) == SQLITE_DONE) removed += (uint64_t)sqlite3_changes(_db);
        sqlite3_finalize(st);
    };
    const auto bounds = [&](int64_t& lo, int64_t& hi) {
        lo = hi = 0;
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(_db, "SELECT min(id), max(id) FROM log", -1, &st, nullptr) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) {
            lo = sqlite3_column_int64(st, 0);
            hi = sqlite3_column_int64(st, 1);
        }
        sqlite3_finalize(st);
    };
    // Age.
    if (ageToo && s.keepDays > 0) run("DELETE FROM log WHERE time < ?", nowMs() - s.keepDays * 86400000LL);
    // Rows: entries are deleted only from the oldest end, so the ids between the oldest and the newest are the count.
    int64_t lo = 0, hi = 0;
    bounds(lo, hi);
    if (s.maxRows > 0 && hi >= lo && hi != 0 && hi - lo + 1 > s.maxRows) run("DELETE FROM log WHERE id < ?", hi - s.maxRows + 1);
    // Size: the pages in use, not the file's length - freed pages are written over before the file grows.
    const int64_t maxPages = std::max<int64_t>(16, s.maxBytes / _pageSize);
    for (int pass = 0; pass < 4; pass++) {
        const int64_t used = pragma("page_count") - pragma("freelist_count");
        if (used <= (int64_t)(maxPages * kTrimAt)) break;
        bounds(lo, hi);
        if (hi == 0 || hi < lo) break;
        const double fraction = 1.0 - (maxPages * kTrimTo) / (double)used;
        const int64_t rows = hi - lo + 1;
        const int64_t n = std::max<int64_t>(1, (int64_t)(rows * fraction));
        run("DELETE FROM log WHERE id < ?", lo + n);
    }
    // A file over its size - max-size was lowered - gives the freed pages back.
    if (pragma("page_count") > maxPages) exec(_db, "PRAGMA incremental_vacuum");
    // The hard ceiling: an insert past it fails (SQLITE_FULL) and writeBatch makes room. Never below what the
    // file holds now, which SQLite would refuse anyway.
    const int64_t ceiling = std::max<int64_t>(maxPages, pragma("page_count"));
    exec(_db, ("PRAGMA max_page_count=" + std::to_string(ceiling)).c_str());
    // The write-ahead log beside it is copied back and cut short at an eighth of the size (4 MB at most), so the
    // two together stay near max-size.
    const int64_t walPages = std::max<int64_t>(16, std::min<int64_t>(1000, maxPages / 8));
    exec(_db, ("PRAGMA wal_autocheckpoint=" + std::to_string(walPages)).c_str());
    exec(_db, ("PRAGMA journal_size_limit=" + std::to_string(walPages * _pageSize)).c_str());
    if (removed > 0) {
        std::lock_guard<std::mutex> lock(_mutex);
        _removed += removed;
    }
}

void DiagnosticsLog::run() {
    std::unique_lock<std::mutex> lock(_mutex);
    auto lastAge = std::chrono::steady_clock::now() - std::chrono::hours(1);
    bool reportedOpen = false;
    bool trimmedOnce = false;
    for (;;) {
        _wake.wait_for(lock, std::chrono::milliseconds(250),
                       [&] { return _quit || _flushWanted || _queue.size() >= kBatch; });
        std::vector<DiagnosticsEntry> batch;
        batch.swap(_queue);
        const bool quit = _quit;
        const bool enabled = _settings.enabled;
        const bool limitsNow = _flushWanted;
        _flushWanted = false;
        lock.unlock();

        if (enabled && _db == nullptr) {
            std::string error;
            if (!openFile(error) && !reportedOpen) {
                reportedOpen = true;
                std::fprintf(stderr, "modbee-mqtt serve: %s\n", error.c_str());
                std::lock_guard<std::mutex> l(_mutex);
                _lastError = error;
            }
        }
        uint64_t written = 0, failed = 0;
        if (_db && !batch.empty() && !trimmedOnce) {
            trim(false);            // the ceiling set before the first write
            trimmedOnce = true;
        }
        for (size_t from = 0; from < batch.size() && _db; from += kChunk) {
            std::vector<DiagnosticsEntry> part(batch.begin() + (long)from,
                                               batch.begin() + (long)std::min(batch.size(), from + kChunk));
            if (writeBatch(part)) written += part.size();
            else failed += part.size();
            trim(false);
        }
        if (_db == nullptr) failed += batch.size();
        const auto now = std::chrono::steady_clock::now();
        if (_db && (limitsNow || now - lastAge >= std::chrono::minutes(1))) {
            lastAge = now;
            trim(true);
        }

        lock.lock();
        _writtenCount += written;
        _failed += failed;
        _done += batch.size();
        _passes++;
        _written.notify_all();
        if (quit && _queue.empty()) break;
    }
    lock.unlock();
    closeFile();
    lock.lock();
    _written.notify_all();
}

bool DiagnosticsLog::query(const DiagnosticsQuery& q, std::vector<DiagnosticsEntry>& out, std::string& error) {
    std::lock_guard<std::mutex> lock(_readMutex);
    if (_reader == nullptr) {
        if (_path.empty() || !fileExists(_path)) return true;     // nothing written yet
        if (sqlite3_open_v2(_path.c_str(), &_reader, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr) !=
            SQLITE_OK) {
            error = "cannot read the diagnostics log " + _path + ": " + (_reader ? sqlite3_errmsg(_reader) : "out of memory");
            if (_reader) sqlite3_close(_reader);
            _reader = nullptr;
            return false;
        }
        sqlite3_busy_timeout(_reader, 2000);
    }
    return runQuery(_reader, q, out, error);
}

bool DiagnosticsLog::queryFile(const std::string& path, const DiagnosticsQuery& q, std::vector<DiagnosticsEntry>& out,
                               std::string& error) {
    if (!fileExists(path)) {
        error = "there is no diagnostics log at " + path;
        return false;
    }
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
        error = "cannot read the diagnostics log " + path + ": " + (db ? sqlite3_errmsg(db) : "out of memory");
        if (db) sqlite3_close(db);
        return false;
    }
    sqlite3_busy_timeout(db, 2000);
    const bool ok = runQuery(db, q, out, error);
    sqlite3_close(db);
    return ok;
}

// --- The server's own warnings and errors ------------------------------------

namespace diagnostics {

namespace {

void say(const char* kind, const char* source, const char* format, va_list args) {
    va_list again;
    va_copy(again, args);
    char small[512];
    const int n = std::vsnprintf(small, sizeof small, format, args);
    std::string text;
    if (n >= (int)sizeof small) {
        std::vector<char> big((size_t)n + 1);
        std::vsnprintf(big.data(), big.size(), format, again);
        text.assign(big.data(), (size_t)n);
    } else if (n > 0) {
        text.assign(small, (size_t)n);
    }
    va_end(again);
    std::fputs(text.c_str(), stderr);
    DiagnosticsLog* log = DiagnosticsLog::installed();
    if (log == nullptr) return;
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) text.pop_back();
    static const char kPrefix[] = "modbee-mqtt serve: ";
    if (text.compare(0, sizeof kPrefix - 1, kPrefix) == 0) text.erase(0, sizeof kPrefix - 1);
    DiagnosticsEntry e;
    e.kind = kind;
    e.reason = text;
    e.detail = source;
    log->record(std::move(e));
}

} // namespace

void warning(const char* source, const char* format, ...) {
    va_list args;
    va_start(args, format);
    say("warning", source, format, args);
    va_end(args);
}

void error(const char* source, const char* format, ...) {
    va_list args;
    va_start(args, format);
    say("error", source, format, args);
    va_end(args);
}

void note(DiagnosticsEntry entry) {
    if (DiagnosticsLog* log = DiagnosticsLog::installed()) log->record(std::move(entry));
}

} // namespace diagnostics

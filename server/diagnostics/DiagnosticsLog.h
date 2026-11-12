// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// THE DIAGNOSTICS LOG (diagnostics: in modbee.yaml; docs/diagnostics.md)
// =============================================================================
//
// What happened to whom, kept on disk to be looked at afterwards: a client
// that connected, went, subscribed or was turned away - and why, in words -
// a device signing up and the answer it got, a login or key an admin changed,
// the server starting and stopping, and the server's own warnings and errors.
//
// ITS OWN SQLITE FILE (diagnostics.db in the data folder), apart from the
// login database: it is written all the time, is never backed up with the
// logins, and can be deleted without losing anything a broker needs.
//
// THE BROKER NEVER WAITS ON IT. record() takes a short lock to append to a
// bounded queue and returns; when the queue is full the entry is dropped and
// counted (status().dropped). One thread of its own writes what is queued,
// in one transaction per batch.
//
// BOUNDED ON DISK. Entries older than keep-days go, the oldest go first once
// there are more than max-rows, and once the file's pages are nearly all used
// (90% of max-size) the oldest quarter or so is deleted - SQLite then writes
// the new entries over the pages they freed, so the file stops growing at
// max-size and the newest entries are always the ones kept.

#include <sqlite3.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct DiagnosticsEntry {
    int64_t id = 0;              // given when written; increases with every entry, never reused
    int64_t timeMs = 0;          // UTC milliseconds; 0 when recorded: now
    std::string kind;            // "connected", "connect-refused", "signup", "warning", ... (docs/diagnostics.md)
    std::string clientId;
    std::string login;
    std::string address;         // where the connection came from, where there is one
    int reasonCode = -1;         // the MQTT reason code sent or received; -1: none
    std::string reason;          // why, in words
    std::string detail;          // what else there is: a topic, a filter, what an admin changed
};

struct DiagnosticsSettings {
    bool enabled = true;
    int64_t keepDays = 7;                    // 0: no age limit (the size and row limits still apply)
    int64_t maxBytes = 100LL * 1024 * 1024;  // the file's size, at most
    int64_t maxRows = 1000000;               // 0: no row limit
    size_t queue = 50000;                    // entries waiting to be written, at most
};

struct DiagnosticsQuery {
    std::string clientId;                // exactly this client id; "" any
    std::string login;                   // exactly this login; "" any
    std::vector<std::string> kinds;      // any of these; empty: every kind
    int64_t sinceMs = 0;                 // at or after; 0: no bound
    int64_t untilMs = 0;                 // before; 0: no bound
    int64_t beforeId = 0;                // paging back: older than this entry; 0: from the newest
    bool forward = false;                // following instead: the entries after afterId, oldest first
    int64_t afterId = 0;                 // with forward; 0: from the oldest
    size_t limit = 100;
};

struct DiagnosticsStatus {
    bool enabled = false;
    bool open = false;                   // the file is open for writing
    std::string path;
    uint64_t written = 0;
    uint64_t dropped = 0;                // the queue was full
    uint64_t failed = 0;                 // a batch the file would not take
    uint64_t removed = 0;                // deleted for age, rows or size
    std::string lastError;
};

class DiagnosticsLog {
public:
    DiagnosticsLog() = default;
    ~DiagnosticsLog();
    DiagnosticsLog(const DiagnosticsLog&) = delete;
    DiagnosticsLog& operator=(const DiagnosticsLog&) = delete;

    // Starts the writer for the file at `path`, created with its folder's
    // permissions if missing. False, with `error`, only when the file cannot
    // be opened at all - the server then runs without a log and says so.
    bool start(const std::string& path, const DiagnosticsSettings& settings, std::string& error);
    // Writes what is queued and stops. Safe to call twice.
    void stop();

    // Queues one entry. Never blocks on the file; a full queue drops it.
    void record(DiagnosticsEntry entry);

    // The limits, changed while it runs (config.set diagnostics.*). Turned
    // off, nothing more is recorded; the file and what it holds stay.
    void configure(const DiagnosticsSettings& settings);
    DiagnosticsSettings settings() const;

    // Waits until everything recorded so far is written (or `ms` passes).
    bool flush(int ms = 5000);

    // Entries, newest first - or, forward, the ones after afterId, oldest
    // first. From any thread.
    bool query(const DiagnosticsQuery& query, std::vector<DiagnosticsEntry>& out, std::string& error);
    // The same, from a file no server has open here: `modbee-mqtt log --data`.
    static bool queryFile(const std::string& path, const DiagnosticsQuery& query,
                          std::vector<DiagnosticsEntry>& out, std::string& error);

    DiagnosticsStatus status() const;
    const std::string& path() const { return _path; }

    // THE PROCESS'S LOG, for what is said from places that are handed no
    // pointer to it: the server's warnings and errors (below). serve
    // installs one; null when there is none.
    static void install(DiagnosticsLog* log);
    static DiagnosticsLog* installed();

    // Has the writer apply every limit now, and waits until it has: what it
    // does once a minute by itself. For tests, which cannot wait for the clock.
    void trimNow();

private:
    void run();
    bool openFile(std::string& error);
    void closeFile();
    bool writeBatch(std::vector<DiagnosticsEntry>& batch);
    void trim(bool ageToo);
    void makeRoom(double fraction);   // the oldest `fraction` of the entries go
    int64_t pragma(const char* name);

    std::string _path;
    mutable std::mutex _mutex;               // the queue, the settings and the counters below
    std::condition_variable _wake;           // the writer: work, or quit
    std::condition_variable _written;        // flush(): a batch went
    std::vector<DiagnosticsEntry> _queue;
    DiagnosticsSettings _settings;
    bool _quit = false;
    bool _started = false;
    uint64_t _recorded = 0;                  // entries taken into the queue
    uint64_t _done = 0;                      // of those, written, dropped by the file, or skipped while off
    uint64_t _writtenCount = 0, _dropped = 0, _failed = 0, _removed = 0;
    bool _flushWanted = false;
    uint64_t _passes = 0;                    // the writer's passes, for trimNow()
    std::atomic<bool> _open{false};          // _db is open, for status()
    std::string _lastError;
    std::thread _thread;

    sqlite3* _db = nullptr;                  // the writer's; only its thread touches it
    sqlite3_stmt* _insert = nullptr;
    int64_t _pageSize = 4096;

    std::mutex _readMutex;                   // query()'s own connection
    sqlite3* _reader = nullptr;
};

// =============================================================================
// THE SERVER'S OWN WARNINGS AND ERRORS
// =============================================================================
//
// Said on stderr exactly as before, and kept in the installed log as kind
// "warning" or "error", the text as the reason and `source` ("kafka",
// "retained", "sessions", "connections", "tls", "logins") as the detail.

namespace diagnostics {

#if defined(__GNUC__)
#define MODBEE_PRINTF(a, b) __attribute__((format(printf, a, b)))
#else
#define MODBEE_PRINTF(a, b)
#endif

void warning(const char* source, const char* format, ...) MODBEE_PRINTF(2, 3);
void error(const char* source, const char* format, ...) MODBEE_PRINTF(2, 3);

// One entry for the installed log; nothing when there is none.
void note(DiagnosticsEntry entry);

} // namespace diagnostics

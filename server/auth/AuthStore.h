// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// The device-login store: SQLite, one file (/var/lib/modbee/auth.db).
//
// WHY SQLITE. A login is looked up on every CONNECT, by user name. An indexed
// SQLite table answers that in microseconds whether it holds a thousand
// devices or ten million, from a page cache rather than RAM proportional to
// the fleet, starts instantly, and is one file to back up - with nothing else
// to run. (A Kafka topic would have to be read into memory, whole, by every
// server at every start.)
//
// THREADS. One read connection per shard, each used only by its shard's
// thread (WAL mode: readers never wait for the writer or each other), and one
// writer, behind a mutex, for the admin and provisioning paths - which are
// rare beside CONNECTs. Every write moves generation(), which is how shards
// know their cached permissions are stale.
//
// SECRETS ARE NEVER STORED, only their hashes (AuthHash.h). A lost secret is
// reset, not read back.

#include <sqlite3.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

struct AuthUser {
    std::string username;
    std::string hash;                    // AuthHash's stored form
    bool generated = true;               // a secret modbee made, or a password a person chose
    bool enabled = true;
    bool pending = false;                // provisioned through a key that needs approval
    bool admin = false;                  // may use $modbee/admin
    std::vector<std::string> publish;    // topic filters; empty = none (every topic: `#`)
    std::vector<std::string> subscribe;  // likewise; ${username} and ${clientid} expand
    std::string provisionedBy;           // the key's name, or "" if added by an admin
    int64_t created = 0;                 // UTC seconds
    int64_t updated = 0;
    // SHA-256 of the one client certificate this login may log in with
    // (authentication.certificate-login): 64 lowercase hex, or "" for any
    // certificate whose CN is this login's name.
    std::string fingerprint;
    // The site the login belongs to: the site of the key that provisioned it ("" for a login an admin
    // added). A key re-provisions only logins of its own site, so one customer's key cannot take over
    // another's device.
    std::string site;
};

struct AuthKey {
    std::string name;
    std::string hash;
    bool manualApproval = false;         // devices wait, pending, until approved
    std::vector<std::string> publish;    // what a device it creates may do
    std::vector<std::string> subscribe;
    int64_t usesLeft = -1;               // -1: unlimited
    int64_t expires = 0;                 // UTC seconds; 0: never
    std::string device;                  // "" or the one device name it may create
    int64_t created = 0;
    std::string site;                    // the site its devices belong to (required)
};

enum class AuthResult { Ok, NotFound, Exists, Invalid, Failed };

class AuthStore {
public:
    ~AuthStore();

    // Opens (creating if needed) the file, and `readers` read connections.
    bool open(const std::string& path, size_t readers, std::string& error);
    void close();
    const std::string& path() const { return _path; }

    // --- Reading, on reader `r`'s own thread only ---
    bool getUser(size_t r, const std::string& username, AuthUser& out);

    // --- Writing, from any thread ---
    AuthResult putUser(const AuthUser& user, bool replace);
    AuthResult removeUser(const std::string& username);
    bool findUser(const std::string& username, AuthUser& out);     // through the writer
    bool listUsers(const std::string& prefix, const std::string& after, size_t limit,
                   std::vector<AuthUser>& out);
    size_t userCount();

    AuthResult putKey(const AuthKey& key);
    AuthResult removeKey(const std::string& name);
    bool findKey(const std::string& name, AuthKey& out);
    bool listKeys(std::vector<AuthKey>& out);
    // One use of a key; false if it has none left.
    bool spendKey(const std::string& name);

    // --- The server's own settings (ServerConfig.h, "Settings kept in the
    // database"), beside the logins so one file backs up both ---
    bool getSetting(const std::string& name, std::string& out);
    AuthResult putSetting(const std::string& name, const std::string& value);
    AuthResult removeSetting(const std::string& name);
    bool listSettings(const std::string& prefix, std::vector<std::pair<std::string, std::string>>& out);

    // A consistent copy of the whole database, made while it is in use.
    bool backup(const std::string& toPath, std::string& error);

    // Replaces the database at `toPath` with the backup at `fromPath`,
    // through SQLite - so a write-ahead log left beside the old file is
    // dealt with, not replayed over the restored one. The broker must be
    // stopped.
    static bool restore(const std::string& fromPath, const std::string& toPath, std::string& error);

    uint64_t generation() const { return _generation.load(std::memory_order_acquire); }
    std::string lastError() const;

private:
    bool exec(const char* sql);
    void changed() { _generation.fetch_add(1, std::memory_order_acq_rel); }

    std::string _path;
    sqlite3* _writer = nullptr;
    std::mutex _write;
    struct Reader {
        sqlite3* db = nullptr;
        sqlite3_stmt* getUser = nullptr;
    };
    std::vector<Reader> _readers;
    std::atomic<uint64_t> _generation{1};
};

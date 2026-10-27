// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// STORE AND FORWARD: WHAT A MANAGED CONNECTION HOLDS FOR THE OTHER BROKER
// =============================================================================
//
// The way a bridge does it (EMQX's buffer, HiveMQ Edge, Mosquitto's bridge
// queue): a message published on a connection with store and forward is
// written here before the application is answered, and leaves only when the
// other broker has acknowledged it. A connection that is down, a server that
// restarts, a crash - none of them loses one, and they go in the order they
// came. Bounded in messages and bytes; full, the connection refuses or drops
// its oldest, as it was told.
//
// One SQLite file for every connection (outbox.db in the API's state
// directory), or memory where there is no directory. Used from the
// connections' own thread only.

#include "AppHub.h"

#include <cstdint>
#include <string>
#include <vector>

struct sqlite3;

class ConnectionOutbox {
public:
    ConnectionOutbox() = default;
    ~ConnectionOutbox();
    ConnectionOutbox(const ConnectionOutbox&) = delete;
    ConnectionOutbox& operator=(const ConnectionOutbox&) = delete;

    // `directory` "": in memory, nothing outlives the process. `syncFull`:
    // every write on the disk before it is answered (power loss loses none).
    bool open(const std::string& directory, bool syncFull, std::string& error);
    void close();

    struct Kept {
        uint64_t id = 0;
        AppMessage message;          // brokerTimeMs: when it was taken
        size_t bytes = 0;
    };
    struct Totals {
        uint64_t messages = 0, bytes = 0;
        int64_t oldestMs = 0;        // when the oldest was taken; 0: none
    };

    // Kept, with its id; false when it could not be written.
    bool push(const std::string& connection, const AppMessage& message, uint64_t& id);
    // The oldest `limit` after `afterId`, in order.
    std::vector<Kept> next(const std::string& connection, uint64_t afterId, size_t limit);
    void drop(uint64_t id);
    // The oldest, given up to make room; false when there is none.
    bool dropOldest(const std::string& connection, Kept* dropped);
    uint64_t dropAll(const std::string& connection);
    Totals totals(const std::string& connection);

private:
    sqlite3* _db = nullptr;
};

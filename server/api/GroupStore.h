// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// CONSUMER GROUPS THAT OUTLIVE A RESTART (Application API: Messages)
// =============================================================================
//
// A named group is durable: it keeps collecting while no member is connected.
// With `sessions: store: file` it is kept on disk too, beside the sessions -
// what it is (its filters and options) and every message it holds that no
// member has acknowledged - so a server restarted, upgraded or killed comes
// back with the group already collecting and its messages waiting.
//
// HOW. AppHub hands every change here as it makes it (a group made or gone, a
// message taken, a message acknowledged); a writer thread commits them to
// <directory>/groups.db in batches, one transaction each, and the hub's shards
// are woken after each so a QoS 1 or 2 publisher's PUBACK, held until its
// message is on disk, goes. WAL mode survives the process being killed;
// `sync: full` also the power going.
//
// WHAT IS NOT KEPT: which member held a message. After a restart every message
// waits for the group again, marked `replay` and `redelivered` - the server
// cannot know which had been sent.

#include "AppHub.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// A message as the stores keep it: its topic, payload, flags, MQTT 5
// properties, publisher and time (and a client event's fields). Shared by the
// groups and the connections' outboxes, so both read the same bytes.
std::string encodeAppMessage(const AppMessage& message);
bool decodeAppMessage(const void* data, int size, AppMessage& message);

struct sqlite3;

struct GroupStoreConfig {
    std::string directory;       // groups.db in it
    bool syncFull = false;       // fsync every commit (power-cut safe), not only WAL
};

// A group as it was kept, and its messages, oldest first.
struct StoredGroup {
    std::string name, connection;
    AppConsumeOptions options;
    int64_t emptySinceMs = 0;    // UTC: when its last member left
    std::vector<std::pair<uint64_t, std::shared_ptr<AppMessage>>> messages;   // by the id it was kept under
};

struct GroupStoreStatus {
    uint64_t commits = 0, changes = 0, failures = 0;
};

class GroupStore {
public:
    explicit GroupStore(const GroupStoreConfig& config);
    ~GroupStore();

    // Reads what was kept and starts the writer; `nextId` is the first id
    // free for a new message. `committed` is called on the writer's thread
    // after every commit.
    bool open(std::vector<StoredGroup>& groups, uint64_t& nextId, std::function<void()> committed,
              std::string& error);
    // Whatever is still to be written, then the writer stops.
    void close();

    // Each queues a change and returns its number; nothing here waits. From
    // any thread.
    uint64_t putGroup(const std::string& name, const std::string& connection, const AppConsumeOptions& options,
                      int64_t emptySinceMs);
    uint64_t dropGroup(const std::string& name, const std::string& connection);
    uint64_t putMessage(uint64_t id, const std::string& name, const std::string& connection,
                        const AppMessage& message);
    uint64_t dropMessage(uint64_t id);

    // Every change up to this number is on disk.
    uint64_t committed() const { return _committed.load(); }
    // Waits, at most `ms`, for every change queued so far.
    bool flush(uint32_t ms);

    GroupStoreStatus status() const;
    std::string path() const;

    struct Change;

private:
    GroupStoreConfig _config;
    sqlite3* _db = nullptr;
    std::function<void()> _onCommit;

    mutable std::mutex _lock;
    std::condition_variable _wake, _done;
    std::vector<Change> _queue;
    uint64_t _queued = 0;
    bool _quit = false;
    std::thread _writer;
    std::atomic<uint64_t> _committed{0};
    std::atomic<uint64_t> _commits{0}, _changes{0}, _failures{0};

    uint64_t queue(Change change);
    void write();
    bool apply(const std::vector<Change>& batch);
    bool load(std::vector<StoredGroup>& groups, uint64_t& nextId, std::string& error);
};

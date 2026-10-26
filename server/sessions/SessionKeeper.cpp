// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "SessionKeeper.h"
#include "ServerOs.h"

#include "sqlite3.h"
#include "MqttProperties.h"
#include "MqttWire.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>

#include "WakeSignal.h"
#include <sys/stat.h>

namespace {

const char* const kSchema = "1";

uint64_t utcNow() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// MQTT 5 properties as the PUBLISH codec writes them: exact, user properties
// in order, and nothing a store has to invent a format for.
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

bool decodeProperties(const void* data, int length, MqttOwnedProperties& into) {
    into.clear();
    if (data == nullptr || length <= 0) return true;
    MqttReader reader((const uint8_t*)data, (size_t)length);
    MqttProperties properties;
    if (mq_decodeProperties(reader, MQ_PKT_PUBLISH, properties) != MqttError::None) return false;
    into.copyForForwarding(properties);
    return true;
}

std::string encodeIds(const MqttBrokerDelivery& delivery) {
    std::string out;
    for (uint8_t index = 0; index < delivery.subscriptionIdCount && index < MQTT_MAX_SUBSCRIPTION_IDS; index++) {
        const uint32_t id = delivery.subscriptionIds[index];
        const char bytes[4] = {(char)(id >> 24), (char)(id >> 16), (char)(id >> 8), (char)id};
        out.append(bytes, 4);
    }
    return out;
}

void decodeIds(const void* data, int length, MqttBrokerDelivery& into) {
    into.subscriptionIdCount = 0;
    const uint8_t* bytes = (const uint8_t*)data;
    for (int at = 0; bytes && at + 4 <= length && into.subscriptionIdCount < MQTT_MAX_SUBSCRIPTION_IDS; at += 4) {
        into.subscriptionIds[into.subscriptionIdCount++] =
            ((uint32_t)bytes[at] << 24) | ((uint32_t)bytes[at + 1] << 16) |
            ((uint32_t)bytes[at + 2] << 8) | bytes[at + 3];
    }
}

std::string column(sqlite3_stmt* statement, int index) {
    const void* data = sqlite3_column_blob(statement, index);
    const int length = sqlite3_column_bytes(statement, index);
    return data ? std::string((const char*)data, (size_t)length) : std::string();
}

void bindText(sqlite3_stmt* statement, int index, const std::string& value) {
    sqlite3_bind_blob(statement, index, value.data(), (int)value.size(), SQLITE_TRANSIENT);
}

bool exec(sqlite3* db, const char* sql) {
    return sqlite3_exec(db, sql, nullptr, nullptr, nullptr) == SQLITE_OK;
}

std::string fileFor(const std::string& directory, size_t shards, size_t shard) {
    char name[64];
    std::snprintf(name, sizeof name, "/sessions-%zu-%zu.db", shards, shard);
    return directory + name;
}

void removeFile(const std::string& path) {
    std::remove(path.c_str());
    std::remove((path + "-wal").c_str());
    std::remove((path + "-shm").c_str());
}

// One file's statements: what a writer applies, and what a rewrite uses to
// put whole sessions into a new file.
struct Statements {
    sqlite3_stmt* putSession = nullptr;
    sqlite3_stmt* endSession[4] = {nullptr, nullptr, nullptr, nullptr};
    sqlite3_stmt* putSubscription = nullptr;
    sqlite3_stmt* dropSubscription = nullptr;
    sqlite3_stmt* putDelivery = nullptr;
    sqlite3_stmt* deliveryState = nullptr;
    sqlite3_stmt* dropDelivery = nullptr;
    sqlite3_stmt* holdInbound = nullptr;
    sqlite3_stmt* releaseInbound = nullptr;
    sqlite3_stmt* offline = nullptr;

    bool prepare(sqlite3* db) {
        struct { sqlite3_stmt** into; const char* sql; } all[] = {
            {&putSession, "INSERT OR REPLACE INTO sessions(client_id, owner, version, expiry, connected, "
                          "offline_utc, will_present, will_due, will_topic, will_payload, will_qos, "
                          "will_retain, will_delay, will_properties) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"},
            {&endSession[0], "DELETE FROM sessions WHERE client_id=?"},
            {&endSession[1], "DELETE FROM subscriptions WHERE client_id=?"},
            {&endSession[2], "DELETE FROM deliveries WHERE client_id=?"},
            {&endSession[3], "DELETE FROM inbound WHERE client_id=?"},
            {&putSubscription, "INSERT OR REPLACE INTO subscriptions(client_id, filter, options, "
                               "subscription_id) VALUES(?,?,?,?)"},
            {&dropSubscription, "DELETE FROM subscriptions WHERE client_id=? AND filter=?"},
            {&putDelivery, "INSERT OR REPLACE INTO deliveries(client_id, sequence, topic, payload, qos, "
                           "retain, properties, subscription_ids, packet_id, awaiting_pubcomp, queued_utc) "
                           "VALUES(?,?,?,?,?,?,?,?,?,?,?)"},
            {&deliveryState, "UPDATE deliveries SET packet_id=?, awaiting_pubcomp=? "
                             "WHERE client_id=? AND sequence=?"},
            {&dropDelivery, "DELETE FROM deliveries WHERE client_id=? AND sequence=?"},
            {&holdInbound, "INSERT OR IGNORE INTO inbound(client_id, packet_id) VALUES(?,?)"},
            {&releaseInbound, "DELETE FROM inbound WHERE client_id=? AND packet_id=?"},
            {&offline, "UPDATE sessions SET connected=0, offline_utc=?, will_due=will_present "
                       "WHERE client_id=?"},
        };
        for (auto& one : all) {
            if (sqlite3_prepare_v2(db, one.sql, -1, one.into, nullptr) != SQLITE_OK) return false;
        }
        return true;
    }

    void finalize() {
        sqlite3_stmt* each[] = {putSession, endSession[0], endSession[1], endSession[2], endSession[3],
                                putSubscription, dropSubscription, putDelivery, deliveryState,
                                dropDelivery, holdInbound, releaseInbound, offline};
        for (sqlite3_stmt* statement : each) sqlite3_finalize(statement);
        *this = Statements();
    }
};

bool step(sqlite3_stmt* statement) {
    const int result = sqlite3_step(statement);
    sqlite3_reset(statement);
    sqlite3_clear_bindings(statement);
    return result == SQLITE_DONE || result == SQLITE_ROW;
}

const char* const kTables =
    "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS sessions(client_id BLOB PRIMARY KEY, owner BLOB, version INTEGER, "
    "  expiry INTEGER, connected INTEGER, offline_utc INTEGER, will_present INTEGER, will_due INTEGER, "
    "  will_topic BLOB, will_payload BLOB, will_qos INTEGER, will_retain INTEGER, will_delay INTEGER, "
    "  will_properties BLOB) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS subscriptions(client_id BLOB, filter BLOB, options INTEGER, "
    "  subscription_id INTEGER, PRIMARY KEY(client_id, filter)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS deliveries(client_id BLOB, sequence INTEGER, topic BLOB, payload BLOB, "
    "  qos INTEGER, retain INTEGER, properties BLOB, subscription_ids BLOB, packet_id INTEGER, "
    "  awaiting_pubcomp INTEGER, queued_utc INTEGER, PRIMARY KEY(client_id, sequence)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS inbound(client_id BLOB, packet_id INTEGER, "
    "  PRIMARY KEY(client_id, packet_id)) WITHOUT ROWID;";

sqlite3* openFile(const std::string& path, bool syncFull, std::string& error) {
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
        error = path + ": " + (db ? sqlite3_errmsg(db) : "cannot open");
        sqlite3_close(db);
        return nullptr;
    }
    sqlite3_busy_timeout(db, 5000);
    if (!exec(db, "PRAGMA journal_mode=WAL") ||
        !exec(db, syncFull ? "PRAGMA synchronous=FULL" : "PRAGMA synchronous=NORMAL") ||
        !exec(db, kTables)) {
        error = path + ": " + sqlite3_errmsg(db);
        sqlite3_close(db);
        return nullptr;
    }
    return db;
}

std::string meta(sqlite3* db, const char* key) {
    sqlite3_stmt* statement = nullptr;
    std::string value;
    if (sqlite3_prepare_v2(db, "SELECT value FROM meta WHERE key=?", -1, &statement, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, key, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW) value = column(statement, 0);
    }
    sqlite3_finalize(statement);
    return value;
}

void setMeta(sqlite3* db, const char* key, const std::string& value) {
    sqlite3_stmt* statement = nullptr;
    if (sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO meta(key, value) VALUES(?,?)", -1, &statement,
                           nullptr) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, key, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, value.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(statement);
    }
    sqlite3_finalize(statement);
}

// A session as a file holds it, with what the loader needs besides the image.
struct Loaded {
    MqttBrokerSessionImage image;
    bool connected = false;
    uint64_t offlineUtc = 0;
};

bool readFile(sqlite3* db, std::map<std::string, Loaded>& into, std::string& error) {
    sqlite3_stmt* statement = nullptr;
    const char* sessions =
        "SELECT client_id, owner, version, expiry, connected, offline_utc, will_present, will_due, "
        "will_topic, will_payload, will_qos, will_retain, will_delay, will_properties FROM sessions";
    if (sqlite3_prepare_v2(db, sessions, -1, &statement, nullptr) != SQLITE_OK) {
        error = sqlite3_errmsg(db);
        return false;
    }
    while (sqlite3_step(statement) == SQLITE_ROW) {
        Loaded loaded;
        MqttBrokerSessionImage& image = loaded.image;
        image.clientId = column(statement, 0);
        image.owner = column(statement, 1);
        image.version = sqlite3_column_int(statement, 2) == (int)MqttVersion::V311 ? MqttVersion::V311
                                                                                   : MqttVersion::V5;
        image.expiryInterval = (uint32_t)sqlite3_column_int64(statement, 3);
        loaded.connected = sqlite3_column_int(statement, 4) != 0;
        loaded.offlineUtc = (uint64_t)sqlite3_column_int64(statement, 5);
        image.will.present = sqlite3_column_int(statement, 6) != 0;
        image.willDue = loaded.connected || sqlite3_column_int(statement, 7) != 0;
        if (image.will.present) {
            image.will.topic = column(statement, 8);
            const std::string payload = column(statement, 9);
            image.will.payload.assign(payload.begin(), payload.end());
            image.will.qos = (uint8_t)sqlite3_column_int(statement, 10);
            image.will.retain = sqlite3_column_int(statement, 11) != 0;
            image.will.delayInterval = (uint32_t)sqlite3_column_int64(statement, 12);
            decodeProperties(sqlite3_column_blob(statement, 13), sqlite3_column_bytes(statement, 13),
                             image.will.properties);
        }
        into[image.clientId] = std::move(loaded);
    }
    sqlite3_finalize(statement);

    if (sqlite3_prepare_v2(db, "SELECT client_id, filter, options, subscription_id FROM subscriptions",
                           -1, &statement, nullptr) == SQLITE_OK) {
        while (sqlite3_step(statement) == SQLITE_ROW) {
            auto found = into.find(column(statement, 0));
            if (found == into.end()) continue;
            MqttBrokerSubscription subscription;
            subscription.filter = column(statement, 1);
            subscription.options = (uint8_t)sqlite3_column_int(statement, 2);
            subscription.subscriptionId = (uint32_t)sqlite3_column_int64(statement, 3);
            found->second.image.subscriptions.push_back(subscription);
        }
    }
    sqlite3_finalize(statement);

    const uint64_t now = utcNow();
    if (sqlite3_prepare_v2(db,
                           "SELECT client_id, sequence, topic, payload, qos, retain, properties, "
                           "subscription_ids, packet_id, awaiting_pubcomp, queued_utc FROM deliveries "
                           "ORDER BY client_id, sequence",
                           -1, &statement, nullptr) == SQLITE_OK) {
        while (sqlite3_step(statement) == SQLITE_ROW) {
            auto found = into.find(column(statement, 0));
            if (found == into.end()) continue;
            MqttBrokerRestoredDelivery restored;
            MqttBrokerDelivery& message = restored.message;
            message.sequence = (uint64_t)sqlite3_column_int64(statement, 1);
            message.topic = column(statement, 2);
            const std::string payload = column(statement, 3);
            message.payload.assign(payload.begin(), payload.end());
            message.qos = (uint8_t)sqlite3_column_int(statement, 4);
            message.retain = sqlite3_column_int(statement, 5) != 0;
            decodeProperties(sqlite3_column_blob(statement, 6), sqlite3_column_bytes(statement, 6),
                             message.properties);
            decodeIds(sqlite3_column_blob(statement, 7), sqlite3_column_bytes(statement, 7), message);
            message.packetId = (uint16_t)sqlite3_column_int(statement, 8);
            message.awaitingPubcomp = sqlite3_column_int(statement, 9) != 0;
            const uint64_t queued = (uint64_t)sqlite3_column_int64(statement, 10);
            const uint64_t age = now > queued ? now - queued : 0;
            restored.ageMs = age < 0xFFFFFFFFull ? (uint32_t)age : 0xFFFFFFFFu;
            found->second.image.deliveries.push_back(restored);
        }
    }
    sqlite3_finalize(statement);

    if (sqlite3_prepare_v2(db, "SELECT client_id, packet_id FROM inbound", -1, &statement, nullptr) ==
        SQLITE_OK) {
        while (sqlite3_step(statement) == SQLITE_ROW) {
            auto found = into.find(column(statement, 0));
            if (found != into.end()) found->second.image.inbound.push_back((uint16_t)sqlite3_column_int(statement, 1));
        }
    }
    sqlite3_finalize(statement);
    return true;
}

// Writes one whole session into a new file (a rewrite for another shard count).
void writeImage(Statements& s, const MqttBrokerSessionImage& image, bool connected, uint64_t offlineUtc) {
    bindText(s.putSession, 1, image.clientId);
    bindText(s.putSession, 2, image.owner);
    sqlite3_bind_int(s.putSession, 3, (int)image.version);
    sqlite3_bind_int64(s.putSession, 4, image.expiryInterval);
    sqlite3_bind_int(s.putSession, 5, connected ? 1 : 0);
    sqlite3_bind_int64(s.putSession, 6, (sqlite3_int64)offlineUtc);
    sqlite3_bind_int(s.putSession, 7, image.will.present ? 1 : 0);
    sqlite3_bind_int(s.putSession, 8, image.willDue ? 1 : 0);
    bindText(s.putSession, 9, image.will.topic);
    sqlite3_bind_blob(s.putSession, 10, image.will.payload.data(), (int)image.will.payload.size(), SQLITE_TRANSIENT);
    sqlite3_bind_int(s.putSession, 11, image.will.qos);
    sqlite3_bind_int(s.putSession, 12, image.will.retain ? 1 : 0);
    sqlite3_bind_int64(s.putSession, 13, image.will.delayInterval);
    bindText(s.putSession, 14, image.will.present ? encodeProperties(image.will.properties) : std::string());
    step(s.putSession);
    for (const MqttBrokerSubscription& subscription : image.subscriptions) {
        bindText(s.putSubscription, 1, image.clientId);
        bindText(s.putSubscription, 2, subscription.filter);
        sqlite3_bind_int(s.putSubscription, 3, subscription.options);
        sqlite3_bind_int64(s.putSubscription, 4, subscription.subscriptionId);
        step(s.putSubscription);
    }
    const uint64_t now = utcNow();
    for (const MqttBrokerRestoredDelivery& restored : image.deliveries) {
        const MqttBrokerDelivery& message = restored.message;
        bindText(s.putDelivery, 1, image.clientId);
        sqlite3_bind_int64(s.putDelivery, 2, (sqlite3_int64)message.sequence);
        bindText(s.putDelivery, 3, message.topic);
        sqlite3_bind_blob(s.putDelivery, 4, message.payload.data(), (int)message.payload.size(), SQLITE_TRANSIENT);
        sqlite3_bind_int(s.putDelivery, 5, message.qos);
        sqlite3_bind_int(s.putDelivery, 6, message.retain ? 1 : 0);
        bindText(s.putDelivery, 7, encodeProperties(message.properties));
        bindText(s.putDelivery, 8, encodeIds(message));
        sqlite3_bind_int(s.putDelivery, 9, message.packetId);
        sqlite3_bind_int(s.putDelivery, 10, message.awaitingPubcomp ? 1 : 0);
        sqlite3_bind_int64(s.putDelivery, 11, (sqlite3_int64)(now - restored.ageMs));
        step(s.putDelivery);
    }
    for (uint16_t packetId : image.inbound) {
        bindText(s.holdInbound, 1, image.clientId);
        sqlite3_bind_int(s.holdInbound, 2, packetId);
        step(s.holdInbound);
    }
}

void endSession(Statements& s, const std::string& clientId) {
    for (sqlite3_stmt* statement : s.endSession) {
        bindText(statement, 1, clientId);
        step(statement);
    }
}

} // namespace

// =============================================================================
// ONE CHANGE, AND ONE SHARD'S JOURNAL AND WRITER
// =============================================================================

struct SessionKeeper::Change {
    enum Kind : uint8_t {
        PutSession, EndSession, Offline,
        PutSubscription, DropSubscription,
        PutDelivery, DeliveryState, DropDelivery,
        HoldInbound, ReleaseInbound,
    };
    Kind kind = PutSession;
    bool dead = false;                 // coalesced away within its batch
    std::string clientId;
    std::string text;                  // owner, filter, or topic
    std::string willTopic;
    std::vector<uint8_t> payload;      // a delivery's, or the Will's
    std::string properties;            // encoded: a delivery's, or the Will's
    std::string subscriptionIds;
    uint64_t sequence = 0;
    uint64_t utc = 0;                  // a session: when it went (0: connected); a delivery: when queued
    uint32_t number = 0;               // expiry interval / subscription identifier / packet identifier
    uint32_t willDelay = 0;
    uint8_t version = 0, qos = 0, options = 0, willQos = 0;
    bool retain = false, connected = false, willPresent = false, willDue = false, willRetain = false;
    bool awaitingPubcomp = false;
};

struct SessionKeeper::Shard : public MqttSessionJournal {
    SessionKeeper* keeper = nullptr;
    uint32_t index = 0;
    sqlite3* db = nullptr;
    Statements statements;

    // --- The shard's own thread ---
    struct Record {
        bool durable = false;          // a session with an expiry; else a Will-only record
        bool connected = false;
    };
    std::unordered_map<std::string, Record> records;  // client identifiers with a record
    std::vector<Change> open;                          // this pass's changes, not yet handed over
    std::unordered_map<std::string, size_t> queued;   // "<client>\0<sequence>" -> its PutDelivery in `open`
    uint64_t appended = 0;             // changes ever added to `open`
    uint64_t marked = 0;               // `appended` when a PUBACK last took a party
    uint64_t told = 0;                 // the last batch the broker was told is kept
    uint64_t nextBatch = 1;            // the batch `open` becomes
    std::deque<std::pair<uint64_t, uint64_t>> waiting;  // (batch, deferral)
    std::vector<MqttBrokerSessionImage> restore;       // loaded, for prepare()
    std::vector<MqttBrokerSessionImage> wills;         // owed, at the first pass

    // --- Between the shard and its writer ---
    std::mutex lock;
    std::condition_variable wake;
    std::deque<std::pair<uint64_t, std::vector<Change>>> handed;
    bool quit = false;
    std::atomic<uint64_t> done{0};     // the last batch committed (or given up on)
    WakeSignal waker;
    std::thread writer;
    std::atomic<uint64_t> commits{0}, changes{0}, failures{0}, keptCount{0};

    void add(Change&& change) {
        open.push_back(std::move(change));
        appended++;
    }

    static bool durable(const MqttBrokerSession& session) { return session.expiryInterval != 0; }

    // --- MqttSessionJournal ---

    void sessionChanged(const MqttBrokerSession& session) override {
        const std::string& id = session.clientId();
        const bool isDurable = durable(session);
        const bool willOnly = !isDurable && session.connected && session.will.present;
        auto found = records.find(id);
        if (!isDurable && !willOnly) {
            // A session that ends with its connection, and nothing owed:
            // nothing to keep - the common case for a clean client, and free.
            if (found != records.end()) {
                Change change;
                change.kind = Change::EndSession;
                change.clientId = id;
                add(std::move(change));
                records.erase(found);
                keptCount.store(records.size(), std::memory_order_relaxed);
            }
            return;
        }
        if (found != records.end() && found->second.durable && !isDurable) {
            // Resumed with an expiry of zero: what it held goes with it.
            Change change;
            change.kind = Change::EndSession;
            change.clientId = id;
            add(std::move(change));
        }
        Change change;
        change.kind = Change::PutSession;
        change.clientId = id;
        change.text = session.userName;
        change.version = (uint8_t)session.version;
        change.number = session.expiryInterval;
        change.connected = session.connected;
        if (!session.connected) {
            const uint64_t away = mq_elapsed(session.offlineSince, mq_millis());
            const uint64_t now = utcNow();
            change.utc = now > away ? now - away : now;
        }
        if (session.will.present) {
            change.willPresent = true;
            change.willDue = session.will.scheduled;
            change.willTopic = session.will.topic;
            change.payload = session.will.payload;
            change.willQos = session.will.qos;
            change.willRetain = session.will.retain;
            change.willDelay = session.will.delayInterval;
            change.properties = encodeProperties(session.will.properties);
        }
        add(std::move(change));
        Record& record = records[id];
        record.durable = isDurable;
        record.connected = session.connected;
        keptCount.store(records.size(), std::memory_order_relaxed);
    }

    void sessionEnded(const std::string& clientId) override {
        auto found = records.find(clientId);
        if (found == records.end()) return;
        records.erase(found);
        keptCount.store(records.size(), std::memory_order_relaxed);
        Change change;
        change.kind = Change::EndSession;
        change.clientId = clientId;
        add(std::move(change));
    }

    void subscriptionAdded(const MqttBrokerSession& session,
                           const MqttBrokerSubscription& subscription) override {
        if (!durable(session)) return;
        Change change;
        change.kind = Change::PutSubscription;
        change.clientId = session.clientId();
        change.text = subscription.filter;
        change.options = subscription.options;
        change.number = subscription.subscriptionId;
        add(std::move(change));
    }

    void subscriptionRemoved(const MqttBrokerSession& session, const std::string& filter) override {
        if (!durable(session)) return;
        Change change;
        change.kind = Change::DropSubscription;
        change.clientId = session.clientId();
        change.text = filter;
        add(std::move(change));
    }

    static std::string keyOf(const std::string& clientId, uint64_t sequence) {
        std::string key = clientId;
        key.push_back('\0');
        key.append((const char*)&sequence, sizeof sequence);
        return key;
    }

    uint64_t deliveryChanged(const MqttBrokerSession& session, const MqttBrokerDelivery& delivery) override {
        if (!durable(session)) return 0;
        const std::string key = keyOf(session.clientId(), delivery.sequence);
        auto pending = queued.find(key);
        if (pending != queued.end()) {
            // Queued in this same pass: the one row it will be written as
            // carries its exchange as it is now.
            Change& put = open[pending->second];
            put.number = delivery.packetId;
            put.awaitingPubcomp = delivery.awaitingPubcomp;
            return nextBatch;
        }
        Change change;
        change.clientId = session.clientId();
        change.sequence = delivery.sequence;
        change.number = delivery.packetId;
        change.awaitingPubcomp = delivery.awaitingPubcomp;
        if (delivery.packetId == MQ_PACKET_ID_NONE && !delivery.awaitingPubcomp) {
            change.kind = Change::PutDelivery;
            change.text = delivery.topic;
            change.payload = delivery.payload;
            change.qos = delivery.qos;
            change.retain = delivery.retain;
            change.properties = encodeProperties(delivery.properties);
            change.subscriptionIds = encodeIds(delivery);
            const uint64_t age = mq_elapsed(delivery.queuedAt, mq_millis());
            const uint64_t now = utcNow();
            change.utc = now > age ? now - age : now;
            queued[key] = open.size();
        } else {
            change.kind = Change::DeliveryState;
        }
        add(std::move(change));
        return nextBatch;
    }

    void deliveryRemoved(const MqttBrokerSession& session, uint64_t sequence) override {
        if (!durable(session)) return;
        const std::string key = keyOf(session.clientId(), sequence);
        auto pending = queued.find(key);
        if (pending != queued.end()) {
            // Queued and finished within one pass: neither is written.
            open[pending->second].dead = true;
            queued.erase(pending);
            return;
        }
        Change change;
        change.kind = Change::DropDelivery;
        change.clientId = session.clientId();
        change.sequence = sequence;
        add(std::move(change));
    }

    uint64_t kept() const override { return done.load(std::memory_order_acquire); }

    void inboundChanged(const MqttBrokerSession& session, uint16_t packetId, bool held) override {
        if (!durable(session)) return;
        Change change;
        change.kind = held ? Change::HoldInbound : Change::ReleaseInbound;
        change.clientId = session.clientId();
        change.number = packetId;
        add(std::move(change));
    }

    // --- Handing a pass's changes to the writer ---

    void seal() {
        // What is handed over now is no PUBLISH's to wait for afterwards: a
        // change after this one starts the next batch. Without this, the
        // first PUBLISH after a quiet pass waited on a batch nothing would
        // ever seal, until some unrelated change came along.
        marked = appended;
        if (open.empty()) return;
        std::vector<Change> batch;
        batch.swap(open);
        queued.clear();
        {
            std::lock_guard<std::mutex> guard(lock);
            handed.emplace_back(nextBatch, std::move(batch));
        }
        wake.notify_one();
        nextBatch++;
    }

    void conclude(MqttBroker& broker, bool all) {
        const uint64_t committed = done.load(std::memory_order_acquire);
        while (!waiting.empty() && (all || waiting.front().first <= committed)) {
            // Neutral (nothing matched, nothing dropped): the store is a party
            // only so the acknowledgement waits for it.
            broker.concludeDeferred(waiting.front().second, 0, 0);
            waiting.pop_front();
        }
    }

    // --- The writer thread ---

    void run() {
        for (;;) {
            std::pair<uint64_t, std::vector<Change>> batch;
            {
                std::unique_lock<std::mutex> guard(lock);
                wake.wait(guard, [this] { return quit || !handed.empty(); });
                if (handed.empty()) return;
                batch = std::move(handed.front());
                handed.pop_front();
            }
            apply(batch.second);
            done.store(batch.first, std::memory_order_release);
            waker.signal();
        }
    }

    void apply(const std::vector<Change>& batch) {
        Statements& s = statements;
        bool ok = exec(db, "BEGIN IMMEDIATE");
        uint64_t rows = 0;
        for (const Change& change : batch) {
            if (!ok) break;
            if (change.dead) continue;
            rows++;
            switch (change.kind) {
            case Change::PutSession:
                bindText(s.putSession, 1, change.clientId);
                bindText(s.putSession, 2, change.text);
                sqlite3_bind_int(s.putSession, 3, change.version);
                sqlite3_bind_int64(s.putSession, 4, change.number);
                sqlite3_bind_int(s.putSession, 5, change.connected ? 1 : 0);
                sqlite3_bind_int64(s.putSession, 6, (sqlite3_int64)change.utc);
                sqlite3_bind_int(s.putSession, 7, change.willPresent ? 1 : 0);
                sqlite3_bind_int(s.putSession, 8, change.willDue ? 1 : 0);
                bindText(s.putSession, 9, change.willTopic);
                sqlite3_bind_blob(s.putSession, 10, change.payload.data(), (int)change.payload.size(),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int(s.putSession, 11, change.willQos);
                sqlite3_bind_int(s.putSession, 12, change.willRetain ? 1 : 0);
                sqlite3_bind_int64(s.putSession, 13, change.willDelay);
                bindText(s.putSession, 14, change.properties);
                ok = step(s.putSession);
                break;
            case Change::EndSession:
                for (sqlite3_stmt* statement : s.endSession) {
                    bindText(statement, 1, change.clientId);
                    ok = step(statement) && ok;
                }
                break;
            case Change::Offline:
                sqlite3_bind_int64(s.offline, 1, (sqlite3_int64)change.utc);
                bindText(s.offline, 2, change.clientId);
                ok = step(s.offline);
                break;
            case Change::PutSubscription:
                bindText(s.putSubscription, 1, change.clientId);
                bindText(s.putSubscription, 2, change.text);
                sqlite3_bind_int(s.putSubscription, 3, change.options);
                sqlite3_bind_int64(s.putSubscription, 4, change.number);
                ok = step(s.putSubscription);
                break;
            case Change::DropSubscription:
                bindText(s.dropSubscription, 1, change.clientId);
                bindText(s.dropSubscription, 2, change.text);
                ok = step(s.dropSubscription);
                break;
            case Change::PutDelivery:
                bindText(s.putDelivery, 1, change.clientId);
                sqlite3_bind_int64(s.putDelivery, 2, (sqlite3_int64)change.sequence);
                bindText(s.putDelivery, 3, change.text);
                sqlite3_bind_blob(s.putDelivery, 4, change.payload.data(), (int)change.payload.size(),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int(s.putDelivery, 5, change.qos);
                sqlite3_bind_int(s.putDelivery, 6, change.retain ? 1 : 0);
                bindText(s.putDelivery, 7, change.properties);
                bindText(s.putDelivery, 8, change.subscriptionIds);
                sqlite3_bind_int(s.putDelivery, 9, (int)change.number);
                sqlite3_bind_int(s.putDelivery, 10, change.awaitingPubcomp ? 1 : 0);
                sqlite3_bind_int64(s.putDelivery, 11, (sqlite3_int64)change.utc);
                ok = step(s.putDelivery);
                break;
            case Change::DeliveryState:
                sqlite3_bind_int(s.deliveryState, 1, (int)change.number);
                sqlite3_bind_int(s.deliveryState, 2, change.awaitingPubcomp ? 1 : 0);
                bindText(s.deliveryState, 3, change.clientId);
                sqlite3_bind_int64(s.deliveryState, 4, (sqlite3_int64)change.sequence);
                ok = step(s.deliveryState);
                break;
            case Change::DropDelivery:
                bindText(s.dropDelivery, 1, change.clientId);
                sqlite3_bind_int64(s.dropDelivery, 2, (sqlite3_int64)change.sequence);
                ok = step(s.dropDelivery);
                break;
            case Change::HoldInbound:
                bindText(s.holdInbound, 1, change.clientId);
                sqlite3_bind_int(s.holdInbound, 2, (int)change.number);
                ok = step(s.holdInbound);
                break;
            case Change::ReleaseInbound:
                bindText(s.releaseInbound, 1, change.clientId);
                sqlite3_bind_int(s.releaseInbound, 2, (int)change.number);
                ok = step(s.releaseInbound);
                break;
            }
        }
        if (ok && exec(db, "COMMIT")) {
            commits.fetch_add(1, std::memory_order_relaxed);
            changes.fetch_add(rows, std::memory_order_relaxed);
            return;
        }
        // The disk is full, or the file went: what is in memory still serves,
        // and this is counted and said once, rather than the broker stopping.
        if (failures.fetch_add(1, std::memory_order_relaxed) == 0) {
            std::fprintf(stderr, "modbee serve: the session store could not write shard %u: %s\n",
                         index, sqlite3_errmsg(db));
        }
        exec(db, "ROLLBACK");
    }
};

// =============================================================================
// THE EXTENSION
// =============================================================================

SessionKeeper::SessionKeeper(const SessionKeeperConfig& config) : _config(config) {}

SessionKeeper::~SessionKeeper() {
    for (std::unique_ptr<Shard>& shard : _shards) {
        {
            std::lock_guard<std::mutex> guard(shard->lock);
            shard->quit = true;
        }
        shard->wake.notify_one();
        if (shard->writer.joinable()) shard->writer.join();
        shard->statements.finalize();
        if (shard->db) sqlite3_close(shard->db);
        shard->waker.close();
    }
}

bool SessionKeeper::load(size_t shards, std::vector<std::vector<MqttBrokerSessionImage>>& into,
                         std::string& error) {
    // Every file there, grouped by the shard count it was written for.
    std::map<size_t, std::map<size_t, std::string>> sets;
    for (const std::string& name : server_os::listDirectory(_config.directory)) {
        size_t count = 0, shard = 0;
        char tail[8] = {0};
        if (std::sscanf(name.c_str(), "sessions-%zu-%zu.d%2s", &count, &shard, tail) == 3 &&
            std::strcmp(tail, "b") == 0 && shard < count) {
            sets[count][shard] = _config.directory + "/" + name;
        }
    }

    // A set is used only if every file of it was finished: a rewrite cut short
    // leaves one that was not, and the set it came from is still whole.
    auto complete = [&](size_t count, time_t* newest) {
        auto found = sets.find(count);
        if (found == sets.end() || found->second.size() != count) return false;
        for (const auto& file : found->second) {
            std::string why;
            sqlite3* db = openFile(file.second, false, why);
            if (db == nullptr) return false;
            const bool whole = meta(db, "complete") == "1";
            const std::string schema = meta(db, "schema");
            sqlite3_close(db);
            if (!whole) return false;
            if (schema != kSchema) {
                error = file.second + " was made by another version of the session store (schema " +
                        schema + ", this one reads " + kSchema + "); remove the sessions-*.db files";
                return false;
            }
            struct stat info;
            if (newest && ::stat(file.second.c_str(), &info) == 0 && info.st_mtime > *newest) *newest = info.st_mtime;
        }
        return true;
    };

    size_t source = 0;
    if (complete(shards, nullptr)) {
        source = shards;
    } else {
        time_t best = 0;
        for (const auto& set : sets) {
            time_t newest = 0;
            if (set.first != shards && complete(set.first, &newest) && newest >= best) {
                best = newest;
                source = set.first;
            }
        }
    }
    if (!error.empty()) return false;

    std::map<std::string, Loaded> loaded;
    if (source != 0) {
        for (const auto& file : sets[source]) {
            sqlite3* db = openFile(file.second, false, error);
            if (db == nullptr || !readFile(db, loaded, error)) {
                if (db) sqlite3_close(db);
                return false;
            }
            sqlite3_close(db);
        }
    }

    // What expired while the server was down goes; the rest is dealt to the
    // shard that owns each identifier now.
    into.assign(shards, std::vector<MqttBrokerSessionImage>());
    const uint64_t now = utcNow();
    std::vector<std::vector<Loaded*>> perShard(shards);
    for (auto& entry : loaded) {
        Loaded& one = entry.second;
        MqttBrokerSessionImage& image = one.image;
        if (image.expiryInterval == 0) {
            // A Will-only record: kept if its Will is owed.
            if (!(image.will.present && image.willDue)) continue;
        } else if (!one.connected && image.expiryInterval != 0xFFFFFFFFu &&
                   one.offlineUtc + (uint64_t)image.expiryInterval * 1000u <= now) {
            _expired++;
            continue;
        }
        image.offlineMs = one.connected || now < one.offlineUtc
                              ? 0 : (uint32_t)std::min<uint64_t>(now - one.offlineUtc, 0xFFFFFFFFu);
        const uint32_t owner = ShardedBroker::ownerOf((const uint8_t*)image.clientId.data(),
                                                      image.clientId.size(), shards);
        perShard[owner].push_back(&one);
    }

    // The files this server writes: rebuilt from what survived, written whole
    // and marked complete before the old ones go, so a stop half way through
    // leaves one whole set or the other.
    for (size_t shard = 0; shard < shards; shard++) {
        const std::string path = fileFor(_config.directory, shards, shard);
        if (source != shards) removeFile(path);
        sqlite3* db = openFile(path, _config.syncFull, error);
        if (db == nullptr) return false;
        Statements statements;
        if (!statements.prepare(db)) {
            error = path + ": " + sqlite3_errmsg(db);
            sqlite3_close(db);
            return false;
        }
        exec(db, "BEGIN IMMEDIATE");
        if (source == shards) {
            // The same files: only what is gone has to go.
            std::map<std::string, bool> keep;
            for (Loaded* one : perShard[shard]) keep[one->image.clientId] = true;
            for (const auto& entry : loaded) {
                const uint32_t owner = ShardedBroker::ownerOf((const uint8_t*)entry.first.data(),
                                                              entry.first.size(), shards);
                if (owner == shard && !keep.count(entry.first)) endSession(statements, entry.first);
            }
        } else {
            for (Loaded* one : perShard[shard]) writeImage(statements, one->image, one->connected, one->offlineUtc);
        }
        // Whatever was connected is not now: offline from here, its Will owed.
        for (Loaded* one : perShard[shard]) {
            if (!one->connected) continue;
            sqlite3_bind_int64(statements.offline, 1, (sqlite3_int64)now);
            bindText(statements.offline, 2, one->image.clientId);
            step(statements.offline);
        }
        setMeta(db, "schema", kSchema);
        setMeta(db, "shards", std::to_string(shards));
        setMeta(db, "shard", std::to_string(shard));
        setMeta(db, "complete", "1");
        if (!exec(db, "COMMIT")) {
            error = path + ": " + sqlite3_errmsg(db);
            statements.finalize();
            sqlite3_close(db);
            return false;
        }
        statements.finalize();
        sqlite3_close(db);
        for (Loaded* one : perShard[shard]) into[shard].push_back(std::move(one->image));
    }
    for (const auto& set : sets) {
        if (set.first == shards) continue;
        for (const auto& file : set.second) removeFile(file.second);
    }
    return true;
}

bool SessionKeeper::begin(size_t shards, std::string& error) {
    if (_config.directory.empty()) {
        error = "sessions: store: file needs a directory";
        return false;
    }
    if (!server_os::makeDirectory(_config.directory, 0700)) {
        error = "sessions.directory " + _config.directory + ": " + server_os::lastError();
        return false;
    }
    std::vector<std::vector<MqttBrokerSessionImage>> images;
    if (!load(shards, images, error)) {
        if (error.empty()) error = "sessions: the files in " + _config.directory + " could not be read";
        return false;
    }
    _shards.clear();
    for (size_t index = 0; index < shards; index++) {
        std::unique_ptr<Shard> shard(new Shard());
        shard->keeper = this;
        shard->index = (uint32_t)index;
        shard->restore = std::move(images[index]);
        const std::string path = fileFor(_config.directory, shards, index);
        shard->db = openFile(path, _config.syncFull, error);
        if (shard->db == nullptr || !shard->statements.prepare(shard->db)) {
            if (error.empty()) error = path + ": " + sqlite3_errmsg(shard->db);
            return false;
        }
        if (!shard->waker.open()) {
            error = "sessions: could not create a wake-up descriptor";
            return false;
        }
        _shards.push_back(std::move(shard));
    }
    for (std::unique_ptr<Shard>& shard : _shards) {
        Shard* raw = shard.get();
        raw->writer = std::thread([raw] { raw->run(); });
    }
    return true;
}

void SessionKeeper::prepare(uint32_t shard, MqttBroker& broker) {
    if (shard >= _shards.size()) return;
    Shard& own = *_shards[shard];
    broker.onJournal(&own);
    for (MqttBrokerSessionImage& image : own.restore) {
        if (image.expiryInterval == 0) {
            // Ended as the server stopped; its Will goes out on the first pass,
            // once every shard has its sessions back to route it to.
            own.records[image.clientId] = Shard::Record();
            own.wills.push_back(std::move(image));
            _willsDue++;
            continue;
        }
        if (image.willDue && image.will.present) _willsDue++;
        if (broker.restoreSession(image)) {
            Shard::Record& record = own.records[image.clientId];
            record.durable = true;
            _restored++;
        } else {
            // No room: broker.max-sessions is lower than it was. Said, counted,
            // and dropped from the file so it is not tried again.
            _refused++;
            own.records[image.clientId] = Shard::Record();
            own.sessionEnded(image.clientId);
        }
    }
    own.restore.clear();
    own.keptCount.store(own.records.size(), std::memory_order_relaxed);
}

int SessionKeeper::waitHandle(uint32_t shard) {
    return shard < _shards.size() ? _shards[shard]->waker.handle() : -1;
}

uint32_t SessionKeeper::offer(uint32_t shard, MqttBroker&, const MqttBrokerRouted&, uint64_t deferral) {
    if (deferral == 0 || shard >= _shards.size()) return 0;
    Shard& own = *_shards[shard];
    // Nothing kept changed since the last PUBACK that waited: nothing to wait for.
    if (own.appended == own.marked) return 0;
    own.marked = own.appended;
    own.waiting.emplace_back(own.nextBatch, deferral);
    return 1;
}

uint64_t SessionKeeper::forwardedHold(uint32_t shard) {
    if (shard >= _shards.size()) return 0;
    Shard& own = *_shards[shard];
    if (own.appended == own.marked) return 0;
    own.marked = own.appended;
    return own.nextBatch;
}

bool SessionKeeper::forwardedSettled(uint32_t shard, uint64_t hold) {
    if (shard >= _shards.size()) return true;
    return _shards[shard]->done.load(std::memory_order_acquire) >= hold;
}

void SessionKeeper::service(uint32_t shard, MqttBroker& broker) {
    if (shard >= _shards.size()) return;
    Shard& own = *_shards[shard];
    if (!own.wills.empty()) {
        std::vector<MqttBrokerSessionImage> wills;
        wills.swap(own.wills);
        for (const MqttBrokerSessionImage& image : wills) broker.restoreWill(image);
    }
    own.waker.drain();
    own.seal();
    own.conclude(broker, false);
    const uint64_t committed = own.done.load(std::memory_order_acquire);
    if (committed != own.told) {
        own.told = committed;
        broker.journalKept();
    }
}

bool SessionKeeper::hasPendingWork(uint32_t shard) {
    if (shard >= _shards.size()) return false;
    const Shard& own = *_shards[shard];
    return !own.open.empty() || !own.wills.empty();
}

void SessionKeeper::stop(uint32_t shard, MqttBroker& broker) {
    if (shard >= _shards.size()) return;
    Shard& own = *_shards[shard];
    // The server is stopping, not the sessions ending: whoever is connected
    // goes offline now, and - as broker.end() is about to delete it - without
    // a Will owed. Written here because the broker's own record of it comes
    // after the writer has stopped.
    const uint64_t now = utcNow();
    for (const auto& entry : own.records) {
        if (!entry.second.connected) continue;
        Change change;
        change.kind = entry.second.durable ? Change::Offline : Change::EndSession;
        change.clientId = entry.first;
        change.utc = now;
        own.add(std::move(change));
    }
    broker.onJournal(nullptr);
    own.seal();
    const uint64_t last = own.nextBatch - 1;
    const mq_time_t started = mq_millis();
    while (own.done.load(std::memory_order_acquire) < last && mq_elapsed(started, mq_millis()) < 10000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    own.conclude(broker, true);
    {
        std::lock_guard<std::mutex> guard(own.lock);
        own.quit = true;
    }
    own.wake.notify_one();
    if (own.writer.joinable()) own.writer.join();
}

SessionKeeperStatus SessionKeeper::status() const {
    SessionKeeperStatus out;
    out.restored = _restored.load();
    out.expired = _expired.load();
    out.refused = _refused.load();
    out.willsDue = _willsDue.load();
    for (const std::unique_ptr<Shard>& shard : _shards) {
        out.kept += shard->keptCount.load(std::memory_order_relaxed);
        out.commits += shard->commits.load(std::memory_order_relaxed);
        out.changes += shard->changes.load(std::memory_order_relaxed);
        out.failures += shard->failures.load(std::memory_order_relaxed);
        std::lock_guard<std::mutex> guard(shard->lock);
        out.behind += shard->handed.size();
    }
    return out;
}

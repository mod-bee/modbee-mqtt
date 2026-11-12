// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee-mqtt log - the diagnostics log (docs/diagnostics.md).
//
// Two ways to it. Over MQTT, the log.list request on $modbee/admin/request,
// with an admin login - so it reads a broker beside it or across the world,
// as `modbee-mqtt user` does. Or the file itself (--data, --file), on the
// server's own machine - which works with the broker stopped, when it is
// needed most. With no login and no address given, on a machine where a
// server keeps its log in the usual folder, the file is read.

#include "Common.h"
#include "Json.h"
#if defined(MODBEE_HAVE_SERVER)
#include "DiagnosticsLog.h"
#endif

#include <algorithm>
#include <chrono>
#include <memory>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>
#include <vector>

namespace {

const char* kUsage =
    "modbee-mqtt log [--client ID] [--login NAME] [--kind K,...] [--since 2h|DATE] [--until ...] [--limit N] "
    "[--follow] [--json]";

const std::vector<ArgSpec>& logSpecs() {
    static const std::vector<ArgSpec> specs = withConnectionSpecs({
        {"client", 0, true, "only this client id"},
        {"login", 0, true, "only this login"},
        {"kind", 0, true, "only these kinds, comma-separated or repeated: connected, disconnected, connect-refused, "
                          "signup, admin, warning, ... (docs/diagnostics.md)"},
        {"since", 0, true, "from this long ago (90s, 30m, 2h, 7d) or this time (2026-11-12, \"2026-11-12 08:00\", "
                           "...Z for UTC)"},
        {"until", 0, true, "up to this long ago, or this time - as --since"},
        {"limit", 'n', true, "the newest this many (default 100)"},
        {"follow", 'f', false, "then keep printing new entries as they come, until Ctrl-C"},
        {"data", 0, true, "read the log in this data folder (serve --data) directly, not over MQTT"},
        {"file", 0, true, "read this diagnostics.db directly, not over MQTT"},
    });
    return specs;
}

struct Entry {
    long long id = 0;
    long long timeMs = 0;
    std::string kind, clientId, login, address, reason, detail;
    int reasonCode = -1;
};

long long nowMs() {
    return (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// "90s", "30m", "2h", "7d", "1w" ago; or a date, a date and time ("T" or a space), local time unless it ends in Z;
// or milliseconds since 1970. False when it is none of those.
bool parseWhen(const std::string& text, long long& outMs) {
    if (text.empty()) return false;
    const char unit = text.back();
    const std::string number = text.substr(0, text.size() - 1);
    if (std::strchr("smhdw", unit) != nullptr && !number.empty() &&
        number.find_first_not_of("0123456789") == std::string::npos) {
        const long long n = std::atoll(number.c_str());
        const long long seconds = unit == 's' ? 1 : unit == 'm' ? 60 : unit == 'h' ? 3600 : unit == 'd' ? 86400 : 604800;
        outMs = nowMs() - n * seconds * 1000;
        return true;
    }
    if (text.find_first_not_of("0123456789") == std::string::npos && text.size() >= 12) {
        outMs = std::atoll(text.c_str());
        return true;
    }
    std::string t = text;
    const bool utc = !t.empty() && (t.back() == 'Z' || t.back() == 'z');
    if (utc) t.pop_back();
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    char sep = 0;
    const int got = std::sscanf(t.c_str(), "%4d-%2d-%2d%c%2d:%2d:%2d", &year, &month, &day, &sep, &hour, &minute, &second);
    if (got != 3 && got < 6) return false;
    if (got >= 4 && sep != 'T' && sep != 't' && sep != ' ') return false;
    if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 60) return false;
    std::tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    tm.tm_isdst = -1;
    std::time_t when;
    if (utc) {
#if defined(_WIN32)
        when = _mkgmtime(&tm);
#else
        when = timegm(&tm);
#endif
    } else {
        when = std::mktime(&tm);
    }
    if (when == (std::time_t)-1) return false;
    outMs = (long long)when * 1000;
    return true;
}

std::string localTime(long long ms) {
    const std::time_t seconds = (std::time_t)(ms / 1000);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &seconds);
#else
    localtime_r(&seconds, &tm);
#endif
    char text[96];
    std::snprintf(text, sizeof text, "%04d-%02d-%02d %02d:%02d:%02d.%03d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec, (int)(ms % 1000));
    return text;
}

void print(const Entry& e, bool asJson) {
    if (asJson) {
        json::Object o;
        o.add("id", e.id).add("time", e.timeMs).add("kind", e.kind).add("clientid", e.clientId).add("login", e.login)
            .add("address", e.address);
        if (e.reasonCode >= 0) o.add("reason_code", (long long)e.reasonCode);
        o.add("reason", e.reason).add("detail", e.detail);
        std::printf("%s\n", o.str().c_str());
        return;
    }
    std::string line = localTime(e.timeMs) + "  " + e.kind;
    if (line.size() < 43) line.append(43 - line.size(), ' ');
    if (!e.clientId.empty()) line += " client " + e.clientId;
    if (!e.login.empty() && e.login != e.clientId) line += " login " + e.login;
    else if (!e.login.empty()) line += " (its login)";
    if (!e.address.empty()) line += " from " + e.address;
    if (!e.reason.empty()) {
        line += " - ";
        if (e.reasonCode > 0) line += "[" + std::to_string(e.reasonCode) + "] ";
        line += e.reason;
    }
    if (!e.detail.empty()) line += " (" + e.detail + ")";
    std::printf("%s\n", line.c_str());
}

struct Filters {
    std::string clientId, login;
    std::vector<std::string> kinds;
    long long sinceMs = 0, untilMs = 0;
};

// --- Over MQTT: log.list on $modbee/admin ------------------------------------

struct Asker {
    std::string id;
    std::string reply;
    bool answered = false;
    bool subscribed = false;
    MqttResult subscribeResult = MqttResult::Queued;
};
Asker* g_asker = nullptr;

void onSubscribed(void*, MqttResult result, uint16_t) {
    if (!g_asker) return;
    g_asker->subscribed = true;
    g_asker->subscribeResult = result;
}

void onMessage(void*, const MqttMessage& message) {
    if (!g_asker || g_asker->answered) return;
    const std::string body((const char*)message.payload.data, message.payload.length);
    json::Value v;
    if (!json::parse(body, v) || v.str("id") != g_asker->id) return;
    g_asker->reply = body;
    g_asker->answered = true;
}

// One connection, held while it asks as often as it needs - following the log asks every second.
class MqttLog {
public:
    explicit MqttLog(CliClient& cli) : _cli(cli) {}
    ~MqttLog() {
        if (_open) _cli.shutdown();
        g_asker = nullptr;
    }

    // 0 open, else the exit code.
    int open() {
        g_asker = &_asker;
        _cli.client.onMessage(&onMessage, nullptr);
        if (!_cli.connectAndWait(10000)) return 3;
        _open = true;
        const std::string replyTopic = "$modbee/admin/" + _cli.connection.clientId + "/reply";
        _cli.client.subscribe(replyTopic.c_str(), MQ_QOS_1, &onSubscribed, nullptr);
        _cli.pollUntil([&] { return _asker.subscribed; }, 10000);
        if (!_asker.subscribed || _asker.subscribeResult == MqttResult::NotAuthorized ||
            !mq_accepted(_asker.subscribeResult)) {
            std::fprintf(stderr, "modbee-mqtt: this login is not an admin on that broker "
                                 "(or the broker has no login database: read the file with --data)\n");
            return 1;
        }
        return 0;
    }

    // One page: from the newest back past `before` (0: the newest), or with `after` >= 0 the ones after it, oldest
    // first. 0 ok, 1 refused or no answer.
    int page(const Filters& f, long long before, long long after, size_t limit, std::vector<Entry>& out,
             long long& next) {
        json::Object request;
        _asker.id = std::to_string(cliProcessId()) + "-" + std::to_string(++_asked);
        _asker.answered = false;
        request.add("id", _asker.id).add("command", "log.list").add("limit", (long long)limit);
        if (!f.clientId.empty()) request.add("client", f.clientId);
        if (!f.login.empty()) request.add("login", f.login);
        if (!f.kinds.empty()) request.raw("kinds", json::array(f.kinds));
        if (f.sinceMs > 0) request.add("since", f.sinceMs);
        if (f.untilMs > 0) request.add("until", f.untilMs);
        if (before > 0) request.add("before", before);
        if (after >= 0) request.add("after", after);
        const std::string body = request.str();
        const MqttResult sent =
            _cli.client.publish("$modbee/admin/request", (const uint8_t*)body.data(), body.size(), MQ_QOS_1, false);
        if (!mq_accepted(sent)) {
            std::fprintf(stderr, "modbee-mqtt: the request could not be sent: %s\n", mq_resultName(sent));
            return 1;
        }
        _cli.pollUntil([&] { return _asker.answered; }, 15000);
        if (!_asker.answered) {
            if (!cliStopRequested()) std::fprintf(stderr, "modbee-mqtt: no answer - is `authentication: database:` set on that broker?\n");
            return 1;
        }
        json::Value reply;
        json::parse(_asker.reply, reply);
        if (!reply.flag("ok")) {
            std::fprintf(stderr, "modbee-mqtt: %s\n", reply.str("error", "refused").c_str());
            return 1;
        }
        for (const json::Value& item : reply["entries"].items) {
            Entry e;
            e.id = item.integer("id");
            e.timeMs = item.integer("time");
            e.kind = item.str("kind");
            e.clientId = item.str("clientid");
            e.login = item.str("login");
            e.address = item.str("address");
            e.reasonCode = item.has("reason_code") ? (int)item.integer("reason_code") : -1;
            e.reason = item.str("reason");
            e.detail = item.str("detail");
            out.push_back(e);
        }
        next = reply.integer("next", 0);
        return 0;
    }

    void idle(int ms) { _cli.pollUntil([] { return false; }, (mq_time_t)ms); }

private:
    CliClient& _cli;
    Asker _asker;
    bool _open = false;
    unsigned long _asked = 0;
};

// --- The file itself -----------------------------------------------------------

#if defined(MODBEE_HAVE_SERVER)
int filePage(const std::string& path, const Filters& f, long long before, long long after, size_t limit,
             std::vector<Entry>& out) {
    DiagnosticsQuery q;
    q.clientId = f.clientId;
    q.login = f.login;
    q.kinds = f.kinds;
    q.sinceMs = f.sinceMs;
    q.untilMs = f.untilMs;
    q.beforeId = before;
    q.forward = after >= 0;
    q.afterId = after >= 0 ? after : 0;
    q.limit = limit;
    std::vector<DiagnosticsEntry> entries;
    std::string error;
    if (!DiagnosticsLog::queryFile(path, q, entries, error)) {
        std::fprintf(stderr, "modbee-mqtt: %s\n", error.c_str());
        return 1;
    }
    for (const DiagnosticsEntry& d : entries) {
        Entry e;
        e.id = d.id;
        e.timeMs = d.timeMs;
        e.kind = d.kind;
        e.clientId = d.clientId;
        e.login = d.login;
        e.address = d.address;
        e.reasonCode = d.reasonCode;
        e.reason = d.reason;
        e.detail = d.detail;
        out.push_back(e);
    }
    return 0;
}

bool fileThere(const std::string& path) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (f) std::fclose(f);
    return f != nullptr;
}
#endif

} // namespace

int cmdApi(int argc, char** argv);

int cmdLog(int argc, char** argv) {
    // `modbee-mqtt log list ...` is the Application API's Admin.ListLog, as every other call is a command.
    if (argc >= 3 && std::strcmp(argv[2], "list") == 0) return cmdApi(argc, argv);
    Args args;
    if (!args.parse(argc, argv, 2, logSpecs())) {
        std::fprintf(stderr, "modbee-mqtt log: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp(kUsage, logSpecs());
        std::printf("\nWith --data or --file, the file is read on this machine (the broker may be stopped); with an\n"
                    "admin login (-u, -P, -h ...), it is asked over MQTT. Neither, on the server itself: the file in\n"
                    "the service's data folder.\n");
        return 0;
    }
    Filters f;
    f.clientId = args.get("client");
    f.login = args.get("login");
    for (const std::string& item : args.all("kind")) {
        size_t from = 0;
        while (from <= item.size()) {
            size_t comma = item.find(',', from);
            if (comma == std::string::npos) comma = item.size();
            std::string kind = item.substr(from, comma - from);
            kind.erase(0, kind.find_first_not_of(' '));
            while (!kind.empty() && kind.back() == ' ') kind.pop_back();
            if (!kind.empty()) f.kinds.push_back(kind);
            from = comma + 1;
        }
    }
    if (args.has("since") && !parseWhen(args.get("since"), f.sinceMs)) {
        std::fprintf(stderr, "modbee-mqtt log: --since is a time ago (90s, 30m, 2h, 7d) or a date (2026-11-12, "
                             "\"2026-11-12 08:00\"), not \"%s\"\n", args.get("since").c_str());
        return 2;
    }
    if (args.has("until") && !parseWhen(args.get("until"), f.untilMs)) {
        std::fprintf(stderr, "modbee-mqtt log: --until is a time ago or a date, as --since, not \"%s\"\n",
                     args.get("until").c_str());
        return 2;
    }
    long long limit = 100;
    if (!args.getInt("limit", 100, 1, 1000000, limit)) {
        std::fprintf(stderr, "modbee-mqtt log: %s\n", args.error().c_str());
        return 2;
    }
    const bool follow = args.has("follow");
    const bool asJson = args.has("json");

    // Where from: a file named, the usual folder on the server itself, or MQTT.
    std::string path;
    if (args.has("file")) path = args.get("file");
    else if (args.has("data")) path = cliInFolder(args.get("data"), "diagnostics.db");
    const char* envUser = std::getenv("MODBEE_AUTHENTICATION_USERNAME");
    const bool loginGiven = args.has("user") || args.has("host") || args.has("port") || args.has("tls") ||
                            (envUser != nullptr && envUser[0] != '\0');
#if defined(MODBEE_HAVE_SERVER)
    if (path.empty() && !loginGiven) {
        for (const std::string& candidate : {cliInFolder(cliDataDirectory(), "diagnostics.db"),
                                             std::string("/var/lib/modbee/diagnostics.db")}) {
            if (fileThere(candidate)) {
                path = candidate;
                break;
            }
        }
    }
#else
    (void)loginGiven;
    if (!path.empty()) {
        std::fprintf(stderr, "modbee-mqtt log: this build reads the log over MQTT only (no --data, --file)\n");
        return 2;
    }
#endif

    CliClient cli;
    std::unique_ptr<MqttLog> mqtt;
    if (path.empty()) {
        std::string error;
        cli.behaviour.transmitBufferSize = 16 * 1024;
        cli.behaviour.receiveBufferSize = 256 * 1024;   // a page of entries in one reply
        if (!cli.configure(args, "modbee-log", error)) {
            std::fprintf(stderr, "modbee-mqtt log: %s\n", error.c_str());
            return 2;
        }
        mqtt.reset(new MqttLog(cli));
        const int rc = mqtt->open();
        if (rc != 0) return rc;
    }
    const auto page = [&](long long before, long long after, size_t n, std::vector<Entry>& out, long long& next) {
        next = 0;
#if defined(MODBEE_HAVE_SERVER)
        if (!mqtt) {
            const int rc = filePage(path, f, before, after, n, out);
            if (rc == 0 && out.size() == n && !out.empty()) next = out.back().id;
            return rc;
        }
#endif
        return mqtt->page(f, before, after, n, out, next);
    };

    // The newest `limit`, a page at a time from the newest back, then printed oldest first.
    std::vector<Entry> newest;
    long long before = 0;
    while ((long long)newest.size() < limit) {
        std::vector<Entry> part;
        long long next = 0;
        const size_t want = (size_t)std::min<long long>(1000, limit - (long long)newest.size());
        const int rc = page(before, -1, want, part, next);
        if (rc != 0) return rc;
        newest.insert(newest.end(), part.begin(), part.end());
        if (next == 0 || part.empty()) break;
        before = next;
    }
    long long last = 0;
    for (auto it = newest.rbegin(); it != newest.rend(); ++it) {
        print(*it, asJson);
        last = std::max(last, it->id);
    }
    if (!follow) {
        if (newest.empty() && !asJson) std::fprintf(stderr, "(nothing in the diagnostics log matches)\n");
        return 0;
    }
    // FOLLOWING: whatever comes after the last one shown, once a second, until Ctrl-C. A follower that started on
    // an empty log takes everything from the start.
    cliInstallSignalHandlers();
    while (!cliStopRequested()) {
        if (mqtt) mqtt->idle(1000);
        else std::this_thread::sleep_for(std::chrono::seconds(1));
        if (cliStopRequested()) break;
        for (;;) {
            std::vector<Entry> part;
            long long next = 0;
            if (page(0, last, 1000, part, next) != 0) return cliStopRequested() ? 0 : 1;
            for (const Entry& e : part) {
                print(e, asJson);
                last = std::max(last, e.id);
            }
            if (part.size() < 1000) break;
        }
        std::fflush(stdout);
    }
    return 0;
}

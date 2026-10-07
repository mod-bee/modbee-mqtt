// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee user / modbee key / modbee backup - managing device logins.
//
// Every command is one request on $modbee/admin/request, answered on
// $modbee/admin/<clientid>/reply (server/auth/AuthGate.h), so it works the
// same against the broker beside it or one across the world, and the broker
// is the only thing that writes its database: every change is live. Inside a
// modbee container the server's own admin login is used without being typed
// (Common.cpp); from anywhere else, -u and -P.

#include "Common.h"
#include "Json.h"
#if defined(MODBEE_HAVE_SERVER)
#include "AuthStore.h"
#endif

#include <cstdio>
#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

struct AdminState {
    std::string id;
    std::string reply;
    bool answered = false;
    bool subscribed = false;
    MqttResult subscribeResult = MqttResult::Queued;
};
AdminState* g_admin = nullptr;

void onSubscribed(void*, MqttResult result, uint16_t) {
    if (!g_admin) return;
    g_admin->subscribed = true;
    g_admin->subscribeResult = result;
}

void onMessage(void*, const MqttMessage& message) {
    if (!g_admin || g_admin->answered) return;
    const std::string body((const char*)message.payload.data, message.payload.length);
    json::Value v;
    if (!json::parse(body, v) || v.str("id") != g_admin->id) return;   // someone else's
    g_admin->reply = body;
    g_admin->answered = true;
}

// Sends one request and waits for its answer. 0 ok, 1 refused, 3 no broker.
int ask(CliClient& cli, const std::string& clientId, json::Object request, json::Value& reply,
        std::string& replyText) {
    AdminState state;
    state.id = std::to_string((long)getpid()) + "-" + std::to_string((long)mq_millis());
    request.add("id", state.id);
    g_admin = &state;
    cli.client.onMessage(&onMessage, nullptr);
    if (!cli.connectAndWait(10000)) {
        g_admin = nullptr;
        return 3;
    }
    const std::string replyTopic = "$modbee/admin/" + clientId + "/reply";
    cli.client.subscribe(replyTopic.c_str(), MQ_QOS_1, &onSubscribed, nullptr);
    cli.pollUntil([&] { return state.subscribed; }, 10000);
    if (!state.subscribed || state.subscribeResult == MqttResult::NotAuthorized ||
        !mq_accepted(state.subscribeResult)) {
        std::fprintf(stderr, "modbee: this login is not an admin on that broker "
                             "(or the broker has no login database)\n");
        cli.shutdown();
        g_admin = nullptr;
        return 1;
    }
    const std::string body = request.str();
    cli.client.publish("$modbee/admin/request", (const uint8_t*)body.data(), body.size(), MQ_QOS_1, false);
    cli.pollUntil([&] { return state.answered; }, 15000);
    cli.shutdown();
    g_admin = nullptr;
    if (!state.answered) {
        std::fprintf(stderr, "modbee: no answer - is `authentication: database:` set on that broker?\n");
        return 1;
    }
    replyText = state.reply;
    json::parse(state.reply, reply);
    if (!reply.flag("ok")) {
        std::fprintf(stderr, "modbee: %s\n", reply.str("error", "refused").c_str());
        return 1;
    }
    return 0;
}

std::string joined(const json::Value& list) {
    std::string out;
    for (const json::Value& item : list.items) {
        if (!out.empty()) out += " ";
        out += item.text;
    }
    return out.empty() ? "(any)" : out;
}

const char* kUserUsage =
    "modbee user <list|show|add|remove|enable|disable|approve|reset|password|permissions|admin> [name] [options]";
const char* kKeyUsage = "modbee key <create|list|remove> [name] [options]";

const std::vector<ArgSpec>& adminSpecs() {
    static const std::vector<ArgSpec> specs = withConnectionSpecs({
        {"publish", 0, true, "a topic filter it may publish to; repeat for more (${username}, ${clientid})"},
        {"subscribe", 0, true, "a topic filter it may subscribe to; repeat for more"},
        {"secret", 0, true, "user add / password: a password you choose (default: generate one)"},
        {"admin", 0, false, "user add: an admin, who may manage logins"},
        {"no-admin", 0, false, "user admin: take admin away"},
        {"prefix", 0, true, "user list: only names starting with this"},
        {"approval", 0, true, "key create: auto (default) or manual - devices wait for approval"},
        {"uses", 0, true, "key create: how many devices it may create (default unlimited)"},
        {"days", 0, true, "key create: expires after this many days (default never)"},
        {"device", 0, true, "key create: the one device name it may create"},
    });
    return specs;
}

int prepare(int argc, char** argv, const char* usage, Args& args, CliClient& cli, std::string& clientId) {
    if (!args.parse(argc, argv, 3, adminSpecs())) {
        std::fprintf(stderr, "modbee: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help") || argc < 3) {
        Args::printHelp(usage, adminSpecs());
        return -1;
    }
    std::string error;
    if (!cli.configure(args, "modbee-admin", error)) {
        std::fprintf(stderr, "modbee: %s\n", error.c_str());
        return 2;
    }
    clientId = cli.connection.clientId;
    return 0;
}

void rules(Args& args, json::Object& request) {
    if (args.has("publish")) request.raw("publish", json::array(args.all("publish")));
    if (args.has("subscribe")) request.raw("subscribe", json::array(args.all("subscribe")));
}

} // namespace

int cmdUser(int argc, char** argv) {
    Args args;
    CliClient cli;
    std::string clientId;
    const int rc = prepare(argc, argv, kUserUsage, args, cli, clientId);
    if (rc != 0) return rc < 0 ? 0 : rc;
    const std::string action = argv[2];
    const std::vector<std::string>& names = args.positional();
    const std::string name = names.empty() ? "" : names[0];
    const bool needsName = action != "list";
    if (needsName && name.empty()) {
        std::fprintf(stderr, "usage: modbee user %s <name>\n", action.c_str());
        return 2;
    }

    json::Object request;
    json::Value reply;
    std::string text;
    if (action == "list") {
        // Every page, unless --json (which prints them as they come).
        std::string after;
        size_t shown = 0;
        long long total = 0;
        bool header = false;
        for (;;) {
            json::Object page;
            page.add("command", "user.list").add("prefix", args.get("prefix")).add("after", after).add("limit", 500LL);
            CliClient pageCli;
            std::string error;
            pageCli.configure(args, "modbee-admin", error);
            const int r = ask(pageCli, pageCli.connection.clientId, page, reply, text);
            if (r != 0) return r;
            total = reply.integer("total");
            if (cli.json) {
                std::printf("%s\n", text.c_str());
                break;
            }
            if (!header) {
                std::printf("%-24s %-9s %-9s %-14s %s\n", "USER", "STATUS", "PASSWORD", "PROVISIONED BY",
                            "MAY PUBLISH / SUBSCRIBE  ((any) = every topic except $)");
                header = true;
            }
            for (const json::Value& u : reply["users"].items) {
                shown++;
                const char* status = !u.flag("enabled") ? "disabled" : u.flag("pending") ? "pending"
                                     : u.flag("admin") ? "admin" : "ok";
                std::printf("%-24s %-9s %-9s %-14s %s / %s\n", u.str("username").c_str(), status,
                            u.str("password").c_str(),
                            u.str("provisioned_by").empty() ? "-" : u.str("provisioned_by").c_str(),
                            joined(u["publish"]).c_str(), joined(u["subscribe"]).c_str());
            }
            if (!reply.has("next") || cli.json) break;
            after = reply.str("next");
        }
        if (!cli.json) std::printf("%zu of %lld login(s)\n", shown, total);
        return 0;
    }
    if (action == "add") {
        request.add("command", "user.add").add("username", name);
        if (args.has("secret")) request.add("password", args.get("secret"));
        if (args.has("admin")) request.add("admin", true);
        rules(args, request);
    } else if (action == "show") {
        request.add("command", "user.get").add("username", name);
    } else if (action == "remove" || action == "enable" || action == "disable" || action == "approve" ||
               action == "reset") {
        request.add("command", "user." + action).add("username", name);
    } else if (action == "password") {
        if (!args.has("secret")) {
            std::fprintf(stderr, "usage: modbee user password <name> --secret <password>\n");
            return 2;
        }
        request.add("command", "user.password").add("username", name).add("password", args.get("secret"));
    } else if (action == "permissions") {
        request.add("command", "user.permissions").add("username", name);
        rules(args, request);
    } else if (action == "admin") {
        request.add("command", "user.admin").add("username", name).add("admin", !args.has("no-admin"));
    } else {
        std::fprintf(stderr, "modbee user: unknown action '%s'\n", action.c_str());
        Args::printHelp(kUserUsage, adminSpecs());
        return 2;
    }
    const int r = ask(cli, clientId, request, reply, text);
    if (r != 0) return r;
    if (cli.json) {
        std::printf("%s\n", text.c_str());
        return 0;
    }
    if (action == "show") {
        const json::Value& u = reply["user"];
        std::printf("%s\n  status:          %s\n  password:        %s (never shown again; `modbee user reset` issues a new one)\n"
                    "  may publish:     %s\n  may subscribe:   %s\n  provisioned by:  %s\n  admin:           %s\n",
                    u.str("username").c_str(),
                    !u.flag("enabled") ? "disabled" : u.flag("pending") ? "pending approval" : "enabled",
                    u.str("password").c_str(), joined(u["publish"]).c_str(), joined(u["subscribe"]).c_str(),
                    u.str("provisioned_by").empty() ? "-" : u.str("provisioned_by").c_str(),
                    u.flag("admin") ? "yes" : "no");
    } else if (reply.has("password")) {
        std::printf("%s %s\n  password: %s\n  (shown once - it cannot be read back; `modbee user reset %s` "
                    "issues a new one)\n",
                    action == "reset" ? "reset" : "added", name.c_str(), reply.str("password").c_str(),
                    name.c_str());
    } else {
        const char* done = action == "add" ? "added" : action == "remove" ? "removed"
                         : action == "enable" ? "enabled" : action == "disable" ? "disabled (and disconnected)"
                         : action == "approve" ? "approved" : action == "password" ? "new password set for"
                         : action == "permissions" ? "new permissions for (reconnecting)"
                         : args.has("no-admin") ? "no longer an admin:" : "now an admin:";
        std::printf("%s %s\n", done, name.c_str());
    }
    return 0;
}

int cmdKey(int argc, char** argv) {
    Args args;
    CliClient cli;
    std::string clientId;
    const int rc = prepare(argc, argv, kKeyUsage, args, cli, clientId);
    if (rc != 0) return rc < 0 ? 0 : rc;
    const std::string action = argv[2];
    const std::string name = args.positional().empty() ? "" : args.positional()[0];
    json::Object request;
    if (action == "create") {
        if (name.empty()) {
            std::fprintf(stderr, "usage: modbee key create <name> [--approval manual] [--publish F] ...\n");
            return 2;
        }
        request.add("command", "key.create").add("name", name).add("approval", args.get("approval", "auto"));
        rules(args, request);
        long long n = 0;
        if (args.has("uses")) {
            if (!args.getInt("uses", 0, 1, 1000000000, n)) { std::fprintf(stderr, "modbee: %s\n", args.error().c_str()); return 2; }
            request.add("uses", n);
        }
        if (args.has("days")) {
            if (!args.getInt("days", 0, 1, 36500, n)) { std::fprintf(stderr, "modbee: %s\n", args.error().c_str()); return 2; }
            request.add("days", n);
        }
        if (args.has("device")) request.add("device", args.get("device"));
    } else if (action == "list") {
        request.add("command", "key.list");
    } else if (action == "remove") {
        if (name.empty()) { std::fprintf(stderr, "usage: modbee key remove <name>\n"); return 2; }
        request.add("command", "key.remove").add("name", name);
    } else {
        std::fprintf(stderr, "modbee key: unknown action '%s'\n", action.c_str());
        Args::printHelp(kKeyUsage, adminSpecs());
        return 2;
    }
    json::Value reply;
    std::string text;
    const int r = ask(cli, clientId, request, reply, text);
    if (r != 0) return r;
    if (cli.json) {
        std::printf("%s\n", text.c_str());
    } else if (action == "create") {
        std::printf("key %s\n  %s\n  (shown once - flash it into the devices this key is for; they register\n"
                    "  themselves with it. `modbee key remove %s` stops it working.)\n",
                    name.c_str(), reply.str("key").c_str(), name.c_str());
    } else if (action == "list") {
        std::printf("%-20s %-9s %-10s %-12s %s\n", "KEY", "APPROVAL", "USES LEFT", "DEVICE", "DEVICES MAY PUBLISH / SUBSCRIBE");
        for (const json::Value& k : reply["keys"].items) {
            const long long uses = k.integer("uses_left");
            std::printf("%-20s %-9s %-10s %-12s %s / %s\n", k.str("name").c_str(), k.str("approval").c_str(),
                        uses < 0 ? "unlimited" : std::to_string(uses).c_str(),
                        k.str("device").empty() ? "any" : k.str("device").c_str(),
                        joined(k["publish"]).c_str(), joined(k["subscribe"]).c_str());
        }
    } else {
        std::printf("removed key %s\n", name.c_str());
    }
    return 0;
}

int cmdBackup(int argc, char** argv) {
    Args args;
    if (!args.parse(argc, argv, 2, withConnectionSpecs({}))) {
        std::fprintf(stderr, "modbee: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help")) {
        Args::printHelp("modbee backup   (a copy of the broker's login database, in its backups folder)",
                        withConnectionSpecs({}));
        return 0;
    }
    CliClient cli;
    std::string error;
    if (!cli.configure(args, "modbee-admin", error)) {
        std::fprintf(stderr, "modbee: %s\n", error.c_str());
        return 2;
    }
    json::Object request;
    request.add("command", "backup");
    json::Value reply;
    std::string text;
    const int r = ask(cli, cli.connection.clientId, request, reply, text);
    if (r != 0) return r;
    if (cli.json) std::printf("%s\n", text.c_str());
    else std::printf("backed up to %s (on the broker's machine)\n", reply.str("file").c_str());
    return 0;
}

// modbee provision <key> [--name N] - what a device does on first boot:
// register with a provisioning key and receive its own login. For a device
// that cannot provision itself (configure it with what this prints), and for
// trying the flow from a terminal.
int cmdProvision(int argc, char** argv) {
    // The key is the first argument, not an option: --key is already the
    // TLS client key, for a device that also uses mutual TLS.
    static const std::vector<ArgSpec> specs = withConnectionSpecs({
        {"name", 'n', true, "the device's name, which becomes its user name (default: --id)"},
    });
    Args args;
    if (!args.parse(argc, argv, 2, specs)) {
        std::fprintf(stderr, "modbee: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help") || args.positional().size() != 1) {
        Args::printHelp("modbee provision <provisioning key> --name <device>", specs);
        return args.has("help") ? 0 : 2;
    }
    CliClient cli;
    std::string error;
    if (!cli.configure(args, "modbee-device", error)) {
        std::fprintf(stderr, "modbee: %s\n", error.c_str());
        return 2;
    }
    // The device's name is its Client Identifier, and the key its password.
    const std::string name = args.has("name") ? args.get("name") : cli.connection.clientId;
    cli.connection.clientId = name;
    cli.connection.hasUserName = true;
    cli.connection.userName = "$provision";
    const std::string key = args.positional()[0];
    cli.connection.hasPassword = true;
    cli.connection.password.assign(key.begin(), key.end());
    cli.client.reconfigure(cli.connection);

    struct State { std::string reply; bool answered = false; bool subscribed = false; } state;
    cli.client.onMessage([](void* c, const MqttMessage& m) {
        State& s = *static_cast<State*>(c);
        s.reply.assign((const char*)m.payload.data, m.payload.length);
        s.answered = true;
    }, &state);
    if (!cli.connectAndWait(10000)) return 3;
    const std::string base = "$modbee/provision/" + name;
    cli.client.subscribe((base + "/reply").c_str(), MQ_QOS_1,
                         [](void* c, MqttResult, uint16_t) { static_cast<State*>(c)->subscribed = true; }, &state);
    cli.pollUntil([&] { return state.subscribed; }, 10000);
    cli.client.publish((base + "/request").c_str(), (const uint8_t*)"{}", 2, MQ_QOS_1, false);
    cli.pollUntil([&] { return state.answered; }, 15000);
    cli.shutdown();
    if (!state.answered) {
        std::fprintf(stderr, "modbee: no answer from the broker\n");
        return 1;
    }
    json::Value reply;
    json::parse(state.reply, reply);
    if (cli.json) {
        std::printf("%s\n", state.reply.c_str());
    } else if (reply.str("status") == "ok") {
        std::printf("provisioned %s\n  username: %s\n  password: %s\n  (shown once - store it in the device)\n",
                    name.c_str(), reply.str("username").c_str(), reply.str("password").c_str());
    } else if (reply.str("status") == "pending") {
        std::printf("%s is waiting for approval: `modbee user approve %s`, then run this again\n",
                    name.c_str(), name.c_str());
    } else {
        std::printf("refused: %s\n", reply.str("reason").c_str());
    }
    return reply.str("status") == "ok" ? 0 : 1;
}

// modbee restore <backup> [--database path] - puts a backup of the login
// database back, with the broker STOPPED (it is a file operation, not a
// request). In Docker:
//   docker compose stop modbee
//   docker compose run --rm --no-deps modbee restore /var/lib/modbee/backups/auth-....db
//   docker compose start modbee
int cmdRestore(int argc, char** argv) {
    static const std::vector<ArgSpec> specs = {
        {"database", 'd', true, "the login database to replace (default /var/lib/modbee/auth.db)"},
        {"help", 0, false, "show this help"},
    };
    Args args;
    if (!args.parse(argc, argv, 2, specs)) {
        std::fprintf(stderr, "modbee: %s\n", args.error().c_str());
        return 2;
    }
    if (args.has("help") || args.positional().size() != 1) {
        Args::printHelp("modbee restore <backup file>   (with the broker stopped)", specs);
        return args.has("help") ? 0 : 2;
    }
#if defined(MODBEE_HAVE_SERVER)
    const std::string from = args.positional()[0];
    const std::string to = args.get("database", "/var/lib/modbee/auth.db");
    std::string error;
    if (!AuthStore::restore(from, to, error)) {
        std::fprintf(stderr, "modbee restore: %s\n", error.c_str());
        return 1;
    }
    std::printf("restored %s from %s - start the broker\n", to.c_str(), from.c_str());
    return 0;
#else
    std::fprintf(stderr, "modbee restore: this build has no server tier (Linux only)\n");
    return 2;
#endif
}

// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// THE COMMAND LINE AND THE APPLICATION API STAY ONE TO ONE.
//
// Fails when a call in app.proto has no `modbee-mqtt` command and is not
// listed as not being one (cli/ApiCommands.h), when a command names a call the
// contract does not have, or when docs/cli.md's table says anything else. A
// command's fields cannot drift: cmd_api.cpp reads them from the contract.

#include "Check.h"
#include "../cli/ApiCommands.h"

#include "modbee/app/v1/app.pb.h"

#include <fstream>
#include <set>
#include <sstream>
#include <string>

using check::ok;
using check::section;

namespace {

std::string slurp(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

std::string commandWords(const ApiCommand& c) {
    return std::string("modbee-mqtt ") + c.noun + (c.verb[0] ? std::string(" ") + c.verb : std::string());
}

} // namespace

int main() {
    namespace pb = google::protobuf;
    const pb::FileDescriptor* file = modbee::app::v1::HelloRequest::descriptor()->file();
    std::set<std::string> calls;
    for (int s = 0; s < file->service_count(); s++) {
        const pb::ServiceDescriptor* service = file->service(s);
        for (int m = 0; m < service->method_count(); m++) calls.insert(service->name() + "." + service->method(m)->name());
    }

    section("every call in app.proto is a command, or says why it is not");
    std::set<std::string> covered;
    for (size_t i = 0; i < kApiCommandCount; i++) {
        ok(calls.count(kApiCommands[i].call) == 1,
           commandWords(kApiCommands[i]) + " names a call the contract has: " + kApiCommands[i].call);
        ok(covered.insert(kApiCommands[i].call).second, std::string(kApiCommands[i].call) + " has one command");
    }
    for (size_t i = 0; i < kApiNotCommandCount; i++) {
        ok(calls.count(kApiNotCommands[i].call) == 1, std::string(kApiNotCommands[i].call) + " is in the contract");
        ok(covered.insert(kApiNotCommands[i].call).second, std::string(kApiNotCommands[i].call) + " is listed once");
    }
    for (const std::string& call : calls) ok(covered.count(call) == 1, call + " has a command");

    section("docs/cli.md says the same");
    const std::string docs = slurp(std::string(MODBEE_SOURCE_DIR) + "/docs/cli.md");
    ok(!docs.empty(), "docs/cli.md is there");
    for (size_t i = 0; i < kApiCommandCount; i++) {
        const ApiCommand& c = kApiCommands[i];
        const std::string row = "| `" + commandWords(c) + "` | `" + c.call + "` | " +
                                (c.rest[0] ? "`" + std::string(c.rest) + "`" : std::string("-")) + " |";
        ok(docs.find(row) != std::string::npos, "its row: " + row);
    }
    for (size_t i = 0; i < kApiNotCommandCount; i++) {
        const std::string row = std::string("| - | `") + kApiNotCommands[i].call + "` | " + kApiNotCommands[i].why + " |";
        ok(docs.find(row) != std::string::npos, "its row: " + row);
    }
    // And no row the table does not have: the table, after its marker, row by row.
    const size_t marker = docs.find("test/cli_api_tests.cpp fails when this table");
    ok(marker != std::string::npos, "the table is marked");
    std::istringstream lines(marker == std::string::npos ? std::string() : docs.substr(marker));
    std::string line;
    std::getline(lines, line);                       // the marker
    size_t rows = 0;
    while (std::getline(lines, line) && line.compare(0, 1, "|") == 0) {
        if (line.compare(0, 2, "| ") != 0 || line.find("| Command |") == 0 || line.find("| ---") == 0) continue;
        rows++;
        bool known = false;
        for (size_t i = 0; i < kApiCommandCount && !known; i++) {
            const std::string head = "| `" + commandWords(kApiCommands[i]) + "` |";
            known = line.compare(0, head.size(), head) == 0;
        }
        for (size_t i = 0; i < kApiNotCommandCount && !known; i++) {
            known = line.find(std::string("| - | `") + kApiNotCommands[i].call + "`") == 0;
        }
        ok(known, "a row the table in cli/ApiCommands.h has: " + line.substr(0, 60));
    }
    ok(rows == kApiCommandCount + kApiNotCommandCount, "one row for each call, no more");
    return check::report("cli_api_tests");
}

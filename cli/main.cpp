// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// modbee - the command-line tool. One binary, subcommands.

#include <cstdio>
#include <cstring>

int cmdPub(int argc, char** argv);
int cmdSub(int argc, char** argv);
int cmdReq(int argc, char** argv);
int cmdBench(int argc, char** argv);
int cmdServe(int argc, char** argv);
int cmdCheck(int argc, char** argv);
int cmdUser(int argc, char** argv);
int cmdKey(int argc, char** argv);
int cmdBackup(int argc, char** argv);
int cmdProvision(int argc, char** argv);
int cmdRestore(int argc, char** argv);

namespace {

void usage() {
    std::printf(
        "modbee - MQTT 5.0 / 3.1.1 tools built on the ModBee MQTT library\n"
        "\n"
        "usage: modbee <command> [options]      modbee <command> --help\n"
        "\n"
        "  pub      publish, and report what the broker said about each message\n"
        "  sub      subscribe and print what arrives (--json for JSON lines)\n"
        "  req      MQTT 5 request/response: publish and wait for the reply\n"
        "  bench    load generator: conn, pub, sub\n"
        "  serve    the broker: one per core behind one port (Linux)\n"
        "  check    validate a modbee.yaml and report every problem with its line\n"
        "  user     device logins on a broker: list, add, reset, disable, approve, remove...\n"
        "  key      provisioning keys, with which devices register themselves\n"
        "  backup   a copy of a broker's login database\n"
        "  provision  register a device with a provisioning key, and print its login\n"
        "  restore  put a backup of the login database back (broker stopped)\n"
        "\n"
        "Exit codes: 0 success, 1 a message or subscription was refused,\n"
        "            2 bad usage, 3 could not connect or the connection was lost.\n");
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    if (argc < 2 || std::strcmp(argv[1], "--help") == 0 || std::strcmp(argv[1], "help") == 0) {
        usage();
        return argc < 2 ? 2 : 0;
    }
    const char* command = argv[1];
    if (std::strcmp(command, "pub") == 0) return cmdPub(argc, argv);
    if (std::strcmp(command, "sub") == 0) return cmdSub(argc, argv);
    if (std::strcmp(command, "req") == 0) return cmdReq(argc, argv);
    if (std::strcmp(command, "bench") == 0) return cmdBench(argc, argv);
    if (std::strcmp(command, "serve") == 0) return cmdServe(argc, argv);
    if (std::strcmp(command, "check") == 0) return cmdCheck(argc, argv);
    if (std::strcmp(command, "user") == 0) return cmdUser(argc, argv);
    if (std::strcmp(command, "key") == 0) return cmdKey(argc, argv);
    if (std::strcmp(command, "backup") == 0) return cmdBackup(argc, argv);
    if (std::strcmp(command, "provision") == 0) return cmdProvision(argc, argv);
    if (std::strcmp(command, "restore") == 0) return cmdRestore(argc, argv);

    std::fprintf(stderr, "modbee: unknown command '%s'\n\n", command);
    usage();
    return 2;
}

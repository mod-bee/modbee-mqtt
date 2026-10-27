// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

// `modbee service`: `modbee serve` as a service - Windows' Service Control
// Manager, or systemd on Linux - the same commands on both.
//
//   modbee service install [--name modbee] [-- <serve options>]
//   modbee service start | stop | status | uninstall [--name modbee]
//
// install also starts it. With no serve options it runs
// `modbee serve --data <folder>` - everything in one folder
// (cliDataDirectory), set up with `modbee admin` and `modbee config`.
//
// WINDOWS:
// install registers the service with the Service Control Manager - started
// at boot, restarted by Windows if it ever stops unexpectedly - to run
// `modbee service run`, which is serve with the arguments given after `--`.
// Windows asking it to stop (a stop, a shutdown) is the same as Ctrl+C: serve
// stops the way it always does, telling every client and keeping what it
// keeps. Its output goes to a log file (default
// %ProgramData%\ModBee\<name>.log); started, stopped and failed go to the
// Windows Event Log too.

#include "Common.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

int cmdServe(int argc, char** argv);

#if defined(_WIN32) && defined(MODBEE_HAVE_SERVER)

#include <windows.h>

namespace {

struct Options {
    std::string name = "modbee";
    std::string log;
    std::vector<std::string> serve;      // what follows `--`
};

const char* kUsage =
    "usage: modbee service <install|uninstall|start|stop|status> [--name <name>] [--log <file>]\n"
    "                      [-- <serve options, e.g. --config C:\\ModBee\\modbee.yaml>]\n";

bool parse(int argc, char** argv, int from, Options& o) {
    for (int i = from; i < argc; i++) {
        const std::string a = argv[i];
        if (a == "--") {
            for (i++; i < argc; i++) o.serve.push_back(argv[i]);
            break;
        }
        if ((a == "--name" || a == "--log") && i + 1 < argc) {
            (a == "--name" ? o.name : o.log) = argv[++i];
        } else {
            std::fprintf(stderr, "modbee service: unknown option %s\n%s", a.c_str(), kUsage);
            return false;
        }
    }
    return true;
}

std::string lastError() {
    char text[256] = {0};
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr, GetLastError(), 0, text,
                   sizeof text, nullptr);
    std::string out = text;
    while (!out.empty() && (out.back() == '\n' || out.back() == '\r')) out.pop_back();
    return out;
}

std::string quoted(const std::string& arg) {
    if (!arg.empty() && arg.find_first_of(" \t\"") == std::string::npos) return arg;
    std::string out = "\"";
    for (char c : arg) {
        if (c == '"') out += '\\';
        out += c;
    }
    return out + "\"";
}

std::string absolute(const std::string& path) {
    char full[MAX_PATH] = {0};
    return GetFullPathNameA(path.c_str(), MAX_PATH, full, nullptr) ? std::string(full) : path;
}

std::string defaultLog(const std::string& name) {
    char data[MAX_PATH] = {0};
    const DWORD n = GetEnvironmentVariableA("ProgramData", data, MAX_PATH);
    const std::string base = (n > 0 && n < MAX_PATH ? std::string(data) : std::string("C:\\ProgramData")) + "\\ModBee";
    CreateDirectoryA(base.c_str(), nullptr);
    return base + "\\" + name + ".log";
}

// The Service Control Manager, and the service in it.
struct Handles {
    SC_HANDLE manager = nullptr, service = nullptr;
    ~Handles() {
        if (service) CloseServiceHandle(service);
        if (manager) CloseServiceHandle(manager);
    }
};

bool openService(const Options& o, DWORD access, Handles& h) {
    h.manager = OpenSCManagerA(nullptr, nullptr, SC_MANAGER_CONNECT);
    if (!h.manager) {
        std::fprintf(stderr, "modbee service: the Service Control Manager: %s\n", lastError().c_str());
        return false;
    }
    h.service = OpenServiceA(h.manager, o.name.c_str(), access);
    if (!h.service) {
        std::fprintf(stderr, "modbee service: \"%s\": %s\n", o.name.c_str(), lastError().c_str());
        return false;
    }
    return true;
}

const char* stateName(DWORD state) {
    switch (state) {
    case SERVICE_STOPPED: return "stopped";
    case SERVICE_START_PENDING: return "starting";
    case SERVICE_STOP_PENDING: return "stopping";
    case SERVICE_RUNNING: return "running";
    default: return "changing";
    }
}

// Until the service is in `wanted`, at most `ms`.
bool waitFor(SC_HANDLE service, DWORD wanted, DWORD ms) {
    SERVICE_STATUS status;
    for (DWORD waited = 0; waited <= ms; waited += 250) {
        if (!QueryServiceStatus(service, &status)) return false;
        if (status.dwCurrentState == wanted) return true;
        Sleep(250);
    }
    return false;
}

int start(const Options& o);

int install(Options o) {
    char exe[MAX_PATH] = {0};
    GetModuleFileNameA(nullptr, exe, MAX_PATH);
    if (o.serve.empty()) o.serve = {"--data", cliDataDirectory(o.name)};
    std::string data;
    for (size_t i = 1; i < o.serve.size(); i++) {
        if (o.serve[i - 1] == "--data") data = o.serve[i] = absolute(o.serve[i]);
    }
    if (!data.empty() && !cliMakeDirectories(data)) {
        std::fprintf(stderr, "modbee service: cannot make the folder %s\n", data.c_str());
        return 3;
    }
    // The service starts in C:\Windows\System32: a relative path given here
    // is made absolute now, while it still means what was meant.
    std::string command = quoted(exe) + " service run --name " + quoted(o.name) +
                          " --log " + quoted(o.log.empty() ? defaultLog(o.name) : absolute(o.log)) + " --";
    for (size_t i = 0; i < o.serve.size(); i++) {
        const bool isPath = i > 0 && (o.serve[i - 1] == "--config" || o.serve[i - 1] == "-C");
        command += " " + quoted(isPath ? absolute(o.serve[i]) : o.serve[i]);
    }
    Handles h;
    h.manager = OpenSCManagerA(nullptr, nullptr, SC_MANAGER_CREATE_SERVICE);
    if (!h.manager) {
        std::fprintf(stderr, "modbee service: the Service Control Manager: %s (run as Administrator)\n",
                     lastError().c_str());
        return 3;
    }
    const std::string display = "ModBee MQTT (" + o.name + ")";
    h.service = CreateServiceA(h.manager, o.name.c_str(), display.c_str(), SERVICE_ALL_ACCESS,
                               SERVICE_WIN32_OWN_PROCESS, SERVICE_AUTO_START, SERVICE_ERROR_NORMAL,
                               command.c_str(), nullptr, nullptr, nullptr, nullptr, nullptr);
    if (!h.service) {
        std::fprintf(stderr, "modbee service: cannot install \"%s\": %s\n", o.name.c_str(), lastError().c_str());
        return 3;
    }
    SERVICE_DESCRIPTIONA description;
    std::string text = "The ModBee MQTT broker (modbee serve).";
    description.lpDescription = &text[0];
    ChangeServiceConfig2A(h.service, SERVICE_CONFIG_DESCRIPTION, &description);
    // Stopped by anything but a request: started again, after 5 s, then 30 s.
    SC_ACTION actions[3] = {{SC_ACTION_RESTART, 5000}, {SC_ACTION_RESTART, 30000}, {SC_ACTION_RESTART, 30000}};
    SERVICE_FAILURE_ACTIONSA failure;
    std::memset(&failure, 0, sizeof failure);
    failure.dwResetPeriod = 86400;
    failure.cActions = 3;
    failure.lpsaActions = actions;
    ChangeServiceConfig2A(h.service, SERVICE_CONFIG_FAILURE_ACTIONS, &failure);
    // ...and not only after a crash: an exit with an error code too, which is
    // how serve asks to be started again (a setting that needs a restart).
    SERVICE_FAILURE_ACTIONS_FLAG onErrors;
    onErrors.fFailureActionsOnNonCrashFailures = TRUE;
    ChangeServiceConfig2A(h.service, SERVICE_CONFIG_FAILURE_ACTIONS_FLAG, &onErrors);
    std::printf("modbee service: installed \"%s\" - it starts with Windows, and again if it stops\n  %s\n",
                o.name.c_str(), command.c_str());
    const int started = start(o);
    if (started != 0) return started;
    if (!data.empty()) {
        std::printf("  everything it keeps: %s\n", data.c_str());
        const std::string where = data == cliDataDirectory() ? "" : " --data \"" + data + "\"";
        std::printf("  the admin login, to set it or change its password: modbee admin%s\n", where.c_str());
    }
    return 0;
}

int start(const Options& o) {
    Handles h;
    if (!openService(o, SERVICE_START | SERVICE_QUERY_STATUS, h)) return 3;
    if (!StartServiceA(h.service, 0, nullptr) && GetLastError() != ERROR_SERVICE_ALREADY_RUNNING) {
        std::fprintf(stderr, "modbee service: cannot start \"%s\": %s\n", o.name.c_str(), lastError().c_str());
        return 3;
    }
    if (!waitFor(h.service, SERVICE_RUNNING, 30000)) {
        std::fprintf(stderr, "modbee service: \"%s\" did not reach running; see its log\n", o.name.c_str());
        return 3;
    }
    std::printf("modbee service: \"%s\" running\n", o.name.c_str());
    return 0;
}

int stop(const Options& o, bool quiet = false) {
    Handles h;
    if (!openService(o, SERVICE_STOP | SERVICE_QUERY_STATUS, h)) return 3;
    SERVICE_STATUS status;
    if (!ControlService(h.service, SERVICE_CONTROL_STOP, &status) && GetLastError() != ERROR_SERVICE_NOT_ACTIVE) {
        std::fprintf(stderr, "modbee service: cannot stop \"%s\": %s\n", o.name.c_str(), lastError().c_str());
        return 3;
    }
    if (!waitFor(h.service, SERVICE_STOPPED, 60000)) {
        std::fprintf(stderr, "modbee service: \"%s\" did not stop within a minute\n", o.name.c_str());
        return 3;
    }
    if (!quiet) std::printf("modbee service: \"%s\" stopped\n", o.name.c_str());
    return 0;
}

int status(const Options& o) {
    Handles h;
    if (!openService(o, SERVICE_QUERY_STATUS, h)) return 3;
    SERVICE_STATUS s;
    if (!QueryServiceStatus(h.service, &s)) {
        std::fprintf(stderr, "modbee service: %s\n", lastError().c_str());
        return 3;
    }
    std::printf("%s %s\n", o.name.c_str(), stateName(s.dwCurrentState));
    return s.dwCurrentState == SERVICE_RUNNING ? 0 : 1;
}

int uninstall(const Options& o) {
    {
        Handles h;
        if (!openService(o, SERVICE_QUERY_STATUS, h)) return 3;
        SERVICE_STATUS s;
        if (QueryServiceStatus(h.service, &s) && s.dwCurrentState != SERVICE_STOPPED && stop(o, true) != 0) {
            return 3;
        }
    }
    Handles h;
    if (!openService(o, DELETE, h)) return 3;
    if (!DeleteService(h.service)) {
        std::fprintf(stderr, "modbee service: cannot remove \"%s\": %s\n", o.name.c_str(), lastError().c_str());
        return 3;
    }
    std::printf("modbee service: removed \"%s\"\n", o.name.c_str());
    return 0;
}

// --- Run by the Service Control Manager ------------------------------------

Options g_run;
SERVICE_STATUS_HANDLE g_handle = nullptr;
SERVICE_STATUS g_state;
HANDLE g_events = nullptr;

void report(DWORD state, DWORD exitCode = NO_ERROR, DWORD waitHint = 0) {
    g_state.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    g_state.dwCurrentState = state;
    g_state.dwControlsAccepted = state == SERVICE_RUNNING ? SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN : 0;
    g_state.dwWin32ExitCode = exitCode == NO_ERROR ? NO_ERROR : ERROR_SERVICE_SPECIFIC_ERROR;
    g_state.dwServiceSpecificExitCode = exitCode;
    g_state.dwWaitHint = waitHint;
    if (state == SERVICE_RUNNING || state == SERVICE_STOPPED) g_state.dwCheckPoint = 0;
    else g_state.dwCheckPoint++;
    SetServiceStatus(g_handle, &g_state);
}

void event(WORD type, const std::string& text) {
    if (!g_events) return;
    const char* strings[1] = {text.c_str()};
    ReportEventA(g_events, type, 0, 0, nullptr, 1, 0, strings, nullptr);
}

DWORD WINAPI control(DWORD code, DWORD, LPVOID, LPVOID) {
    switch (code) {
    case SERVICE_CONTROL_STOP:
    case SERVICE_CONTROL_SHUTDOWN:
        // Serve stops the way Ctrl+C stops it: clients told, state kept.
        report(SERVICE_STOP_PENDING, NO_ERROR, 30000);
        cliRequestStop();
        return NO_ERROR;
    case SERVICE_CONTROL_INTERROGATE:
        return NO_ERROR;
    default:
        return ERROR_CALL_NOT_IMPLEMENTED;
    }
}

void WINAPI serviceMain(DWORD, LPSTR*) {
    g_handle = RegisterServiceCtrlHandlerExA(g_run.name.c_str(), control, nullptr);
    if (!g_handle) return;
    std::memset(&g_state, 0, sizeof g_state);
    report(SERVICE_START_PENDING, NO_ERROR, 10000);
    g_events = RegisterEventSourceA(nullptr, g_run.name.c_str());
    const std::string log = g_run.log.empty() ? defaultLog(g_run.name) : g_run.log;
    // No console: what serve says goes to the log, appended.
    if (std::freopen(log.c_str(), "a", stdout)) std::setvbuf(stdout, nullptr, _IOLBF, BUFSIZ);
    std::freopen(log.c_str(), "a", stderr);
    std::vector<std::string> args = {"modbee", "serve"};
    args.insert(args.end(), g_run.serve.begin(), g_run.serve.end());
    std::vector<char*> argv;
    for (std::string& a : args) argv.push_back(&a[0]);
    argv.push_back(nullptr);
    report(SERVICE_RUNNING);
    event(EVENTLOG_INFORMATION_TYPE, "modbee service \"" + g_run.name + "\" started; log: " + log);
    int rc = cmdServe((int)args.size(), argv.data());
    // A restart asked for: an error exit, which Windows answers by starting
    // the service again (the failure actions set at install).
    const bool restart = rc == 0 && cliRestartRequested();
    if (restart) rc = 75;
    std::fflush(stdout);
    std::fflush(stderr);
    if (restart) {
        event(EVENTLOG_INFORMATION_TYPE, "modbee service \"" + g_run.name + "\" restarting to apply a setting");
    } else if (rc == 0) {
        event(EVENTLOG_INFORMATION_TYPE, "modbee service \"" + g_run.name + "\" stopped");
    } else {
        event(EVENTLOG_ERROR_TYPE, "modbee service \"" + g_run.name + "\" stopped with exit code " +
                                       std::to_string(rc) + "; see " + log);
    }
    if (g_events) DeregisterEventSource(g_events);
    report(SERVICE_STOPPED, rc == 0 ? NO_ERROR : (DWORD)rc);
}

int run(const Options& o) {
    g_run = o;
    SERVICE_TABLE_ENTRYA table[] = {{&g_run.name[0], serviceMain}, {nullptr, nullptr}};
    if (!StartServiceCtrlDispatcherA(table)) {
        if (GetLastError() == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
            std::fprintf(stderr, "modbee service run: only the Service Control Manager starts this; "
                                 "to run in a console, use modbee serve\n");
        } else {
            std::fprintf(stderr, "modbee service run: %s\n", lastError().c_str());
        }
        return 2;
    }
    return 0;
}

} // namespace

int cmdService(int argc, char** argv) {
    if (argc < 3 || std::strcmp(argv[2], "--help") == 0) {
        std::fprintf(argc < 3 ? stderr : stdout, "%s", kUsage);
        return argc < 3 ? 2 : 0;
    }
    const std::string action = argv[2];
    Options o;
    if (!parse(argc, argv, 3, o)) return 2;
    if (action == "install") return install(o);
    if (action == "uninstall") return uninstall(o);
    if (action == "start") return start(o);
    if (action == "stop") return stop(o);
    if (action == "status") return status(o);
    if (action == "run") return run(o);
    std::fprintf(stderr, "modbee service: unknown action %s\n%s", action.c_str(), kUsage);
    return 2;
}

#elif defined(__linux__) && defined(MODBEE_HAVE_SERVER)

// LINUX: the same commands, through systemd. install writes the unit
// (/etc/systemd/system/<name>.service), makes the `modbee` system user and
// the data folder, and enables and starts it; its output is in the journal.

#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>

namespace {

struct Options {
    std::string name = "modbee";
    std::vector<std::string> serve;      // what follows `--`
};

const char* kUsage =
    "usage: modbee service <install|uninstall|start|stop|status> [--name <name>]\n"
    "                      [-- <serve options; default --data /var/lib/<name>>]\n";

bool parse(int argc, char** argv, int from, Options& o) {
    for (int i = from; i < argc; i++) {
        const std::string a = argv[i];
        if (a == "--") {
            for (i++; i < argc; i++) o.serve.push_back(argv[i]);
            break;
        }
        if (a == "--name" && i + 1 < argc) {
            o.name = argv[++i];
        } else {
            std::fprintf(stderr, "modbee service: unknown option %s\n%s", a.c_str(), kUsage);
            return false;
        }
    }
    return true;
}

std::string unitFile(const Options& o) { return "/etc/systemd/system/" + o.name + ".service"; }

// For the shell, in single quotes.
std::string shell(const std::string& arg) {
    std::string out = "'";
    for (char c : arg) out += c == '\'' ? std::string("'\\''") : std::string(1, c);
    return out + "'";
}

// For a unit's ExecStart, in double quotes.
std::string unitQuoted(const std::string& arg) {
    std::string out = "\"";
    for (char c : arg) {
        if (c == '"' || c == '\\') out += '\\';
        if (c == '%') out += '%';
        out += c;
    }
    return out + "\"";
}

bool sh(const std::string& command) { return std::system(command.c_str()) == 0; }

bool ready(const Options& o) {
    if (geteuid() != 0) {
        std::fprintf(stderr, "modbee service: run it as root (sudo modbee service ...)\n");
        return false;
    }
    struct stat info;
    if (::stat("/run/systemd/system", &info) != 0) {
        std::fprintf(stderr, "modbee service: this system does not run systemd - run `modbee serve --data %s` "
                             "under what it has instead\n", cliDataDirectory(o.name).c_str());
        return false;
    }
    return true;
}

bool installed(const Options& o) {
    struct stat info;
    if (::stat(unitFile(o).c_str(), &info) == 0) return true;
    std::fprintf(stderr, "modbee service: \"%s\" is not installed (modbee service install)\n", o.name.c_str());
    return false;
}

std::string absolute(const std::string& path) {
    if (path.empty() || path[0] == '/') return path;
    char here[4096];
    return getcwd(here, sizeof here) ? std::string(here) + "/" + path : path;
}

int install(Options o) {
    if (!ready(o)) return 3;
    char exe[4096] = {0};
    const ssize_t length = readlink("/proc/self/exe", exe, sizeof exe - 1);
    if (length <= 0) {
        std::fprintf(stderr, "modbee service: cannot tell where this modbee is\n");
        return 3;
    }
    if (o.serve.empty()) o.serve = {"--data", cliDataDirectory(o.name)};
    std::string data;
    for (size_t i = 0; i < o.serve.size(); i++) {
        const bool isPath = i > 0 && (o.serve[i - 1] == "--config" || o.serve[i - 1] == "-C" ||
                                      o.serve[i - 1] == "--data");
        if (isPath) o.serve[i] = absolute(o.serve[i]);
        if (i > 0 && o.serve[i - 1] == "--data") data = o.serve[i];
    }
    // The user it runs as: made once, shared by every instance.
    if (getpwnam("modbee") == nullptr &&
        !sh("useradd --system --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin "
            "--comment 'ModBee MQTT' modbee 2>/dev/null || adduser -S -D -H -h /nonexistent -s /sbin/nologin modbee")) {
        std::fprintf(stderr, "modbee service: cannot make the modbee user\n");
        return 3;
    }
    if (!data.empty()) {
        if (!cliMakeDirectories(data) || !sh("chown -R modbee:modbee " + shell(data) + " && chmod 0750 " + shell(data))) {
            std::fprintf(stderr, "modbee service: cannot prepare %s\n", data.c_str());
            return 3;
        }
    }
    std::string command = unitQuoted(std::string(exe, (size_t)length)) + " serve";
    std::string shown = std::string(exe, (size_t)length) + " serve";        // as a person would type it
    for (const std::string& a : o.serve) {
        command += " " + unitQuoted(a);
        shown += " " + (a.find_first_of(" \t'\"") == std::string::npos ? a : shell(a));
    }
    std::ofstream unit(unitFile(o).c_str(), std::ios::trunc);
    unit << "# Written by `modbee service install`; `modbee service uninstall` removes it.\n"
            "[Unit]\n"
            "Description=ModBee MQTT (" << o.name << ")\n"
            "Documentation=https://github.com/mod-bee/modbee-mqtt\n"
            "After=network-online.target\n"
            "Wants=network-online.target\n"
            "\n"
            "[Service]\n"
            "User=modbee\n"
            "Group=modbee\n"
            "ExecStart=" << command << "\n"
            // Always: a setting applied with --restart stops it on purpose,
            // and this is what starts it again.
            "Restart=always\n"
            "RestartSec=2\n"
            "TimeoutStopSec=30\n"
            "LimitNOFILE=1048576\n"
            "NoNewPrivileges=yes\n"
            "ProtectSystem=full\n"
            "ProtectHome=yes\n"
            "PrivateTmp=yes\n"
            "\n"
            "[Install]\n"
            "WantedBy=multi-user.target\n";
    unit.close();
    if (!unit) {
        std::fprintf(stderr, "modbee service: cannot write %s\n", unitFile(o).c_str());
        return 3;
    }
    if (!sh("systemctl daemon-reload && systemctl enable --now " + shell(o.name) + " >/dev/null 2>&1")) {
        std::fprintf(stderr, "modbee service: systemd would not start it - journalctl -u %s says why\n",
                     o.name.c_str());
        return 3;
    }
    std::printf("modbee service: \"%s\" installed and running - it starts at boot, and again if it stops\n",
                o.name.c_str());
    std::printf("  %s\n", shown.c_str());
    if (!data.empty()) std::printf("  everything it keeps: %s\n", data.c_str());
    std::printf("  its log: journalctl -u %s\n", o.name.c_str());
    if (!data.empty()) {
        const std::string where = data == cliDataDirectory() ? "" : " --data " + data;
        std::printf("  the admin login, to set it or change its password: sudo modbee admin%s\n", where.c_str());
    }
    return 0;
}

int uninstall(const Options& o) {
    if (!ready(o) || !installed(o)) return 3;
    sh("systemctl disable --now " + shell(o.name) + " >/dev/null 2>&1");
    std::remove(unitFile(o).c_str());
    sh("systemctl daemon-reload");
    std::printf("modbee service: \"%s\" stopped and removed - what it kept is still there\n", o.name.c_str());
    return 0;
}

int control(const Options& o, const char* verb, const char* done) {
    if (!ready(o) || !installed(o)) return 3;
    if (!sh(std::string("systemctl ") + verb + " " + shell(o.name))) {
        std::fprintf(stderr, "modbee service: cannot %s \"%s\" - journalctl -u %s says why\n", verb,
                     o.name.c_str(), o.name.c_str());
        return 3;
    }
    std::printf("modbee service: \"%s\" %s\n", o.name.c_str(), done);
    return 0;
}

int status(const Options& o) {
    struct stat info;
    if (::stat(unitFile(o).c_str(), &info) != 0) {
        std::printf("%s not installed\n", o.name.c_str());
        return 3;
    }
    const bool running = sh("systemctl is-active --quiet " + shell(o.name));
    std::printf("%s %s\n", o.name.c_str(), running ? "running" : "stopped");
    return running ? 0 : 1;
}

} // namespace

int cmdService(int argc, char** argv) {
    if (argc < 3 || std::strcmp(argv[2], "--help") == 0) {
        std::fprintf(argc < 3 ? stderr : stdout, "%s", kUsage);
        return argc < 3 ? 2 : 0;
    }
    const std::string action = argv[2];
    Options o;
    if (!parse(argc, argv, 3, o)) return 2;
    if (action == "install") return install(o);
    if (action == "uninstall") return uninstall(o);
    if (action == "start") return control(o, "start", "running");
    if (action == "stop") return control(o, "stop", "stopped");
    if (action == "status") return status(o);
    std::fprintf(stderr, "modbee service: unknown action %s\n%s", action.c_str(), kUsage);
    return 2;
}

#else

int cmdService(int, char**) {
    std::fprintf(stderr, "modbee service: Linux (systemd) and Windows only - elsewhere, run "
                         "`modbee serve --data <folder>` under what the system has.\n");
    return 2;
}

#endif

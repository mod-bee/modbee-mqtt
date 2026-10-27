// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// What pub, sub and req share: the connection options, building a client from
// them, waiting for it, and printing what came back.

#include "Args.h"

#include <ModBeeMqtt.h>

#include <memory>
#include <string>
#include <vector>

// The connection options every client command accepts.
std::vector<ArgSpec> connectionSpecs();

// Appends `extra` to the connection options.
std::vector<ArgSpec> withConnectionSpecs(std::vector<ArgSpec> extra);

// Sleeps until the transport has something to read, for at most `ms`.
// Not mq_yield(): on a host that spins, which is right for the library and
// wrong for a tool that may sit on a subscription for an hour.
void cliIdle(MqttTransport* transport, int ms);

bool cliStopRequested();

// A client, its transport and its configuration, built from the options.
struct CliClient {
    std::unique_ptr<MqttTransport> transport;
    MqttConnectionConfig connection;
    MqttClientConfig behaviour;
    MqttClient client;
    bool json = false;

    // Builds everything from the parsed options. False, with `error` set, when
    // an option is wrong or TLS was asked for and this build has no engine.
    bool configure(Args& args, const char* defaultClientIdPrefix, std::string& error);

    // Connects and polls until the session is up or the attempt has
    // concluded. False with the reason printed when it did not come up.
    bool connectAndWait(mq_time_t timeoutMs);

    // Polls until `done`, the timeout, or Ctrl-C; true if `done` held.
    template <typename Done>
    bool pollUntil(Done done, mq_time_t timeoutMs) {
        const mq_time_t start = mq_millis();
        while (!done()) {
            if (mq_millis() - start >= timeoutMs || cliStopRequested()) return false;
            client.poll();
            if (!done()) cliIdle(transport.get(), 20);
        }
        return true;
    }

    // DISCONNECT, then a bounded drain so it actually reaches the wire.
    void shutdown();
};

// Set by SIGINT/SIGTERM - or, on Windows, by the service manager asking
// the service to stop (cliRequestStop) - and polled by commands that run
// until they are stopped.
bool cliStopRequested();
void cliRequestStop();
// Stop, to be started again: serve applying a setting that needs a restart.
void cliRequestRestart();
bool cliRestartRequested();
void cliInstallSignalHandlers();

// This process's id, for a unique client id.
long cliProcessId();

// Where a server installed as the service `service` keeps everything
// (`modbee serve --data`): /var/lib/<service> on Linux; on Windows
// %ProgramData%\ModBee, or %ProgramData%\ModBee\<service> for another name.
std::string cliDataDirectory(const std::string& service = "modbee");

// The folder and every missing one above it.
bool cliMakeDirectories(const std::string& path);
// A name in a folder, with the system's own separator.
std::string cliInFolder(const std::string& folder, const std::string& name);

// Whether the input is a person at a terminal, not a pipe.
bool cliIsTerminal();
// One line from the input - typed without being shown, at a terminal.
bool cliReadSecret(const char* prompt, std::string& out);

// --- Output ---

// A JSON string literal, quotes included.
std::string jsonString(const uint8_t* data, size_t length);
inline std::string jsonString(const std::string& text) {
    return jsonString((const uint8_t*)text.data(), text.size());
}

// True when the bytes are valid UTF-8 with no control characters other than
// tab and newline - i.e. safe to show to a person as text.
bool isPrintableText(const uint8_t* data, size_t length);

std::string base64(const uint8_t* data, size_t length);

// A received message as one JSON object, without a trailing newline.
std::string messageJson(const MqttMessage& message);

// Parses "key=value" for --user-property. False when there is no '='.
bool splitUserProperty(const std::string& text, std::string& key, std::string& value);

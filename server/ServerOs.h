// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// The few things the server asks of the operating system beyond sockets, the
// same on Linux and Windows: files written so a crash leaves the old one or
// the new one and never half of either, directories only the server can read,
// the system's random numbers, the time in UTC.

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <string>
#include <vector>

namespace server_os {

// Writes `data` to `path` + ".new", flushes it to the disk, then puts it in
// place of `path` in one step. False, with `error`, when any of it fails;
// `path` is then as it was. `mode` is the file's permissions on POSIX (0600:
// the server's user alone).
bool writeFileAtomically(const std::string& path, const std::string& data, std::string& error,
                         int mode = 0644);

// Whether a file can be created at `path` (it is created and removed).
bool canWrite(const std::string& path, std::string& error);

// Replaces `to` with `from` in one step (rename, over an existing file too).
bool replaceFile(const std::string& from, const std::string& to);

// Makes `path` if it is not there, readable by the server's own user only
// where the system has such a thing (`mode` on POSIX). True if it exists
// afterwards.
bool makeDirectory(const std::string& path, int mode);

// The names in a directory (not "." or ".."); empty when it cannot be read.
std::vector<std::string> listDirectory(const std::string& path);

// Fills `out` from the system's cryptographic random source.
bool randomBytes(uint8_t* out, size_t length);

// `when` in UTC.
std::tm utcTime(std::time_t when);

// The last system error as text.
std::string lastError();

} // namespace server_os

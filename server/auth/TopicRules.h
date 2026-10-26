// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// Topic rules as both permission hooks use them: the login store's (AuthGate)
// and the application's (server/api/Authority).

#include <string>

// ${username} and ${clientid} in `pattern`, or false if either would put a
// wildcard or a level separator into a filter - the rule then grants nothing
// (CVE-2017-7650). A value is copied in once and never read again.
bool expand(const std::string& pattern, const std::string& username, const std::string& clientId,
            std::string& out);

// Whether every topic `requested` can match is one `allowed` matches too - so
// granting the subscription gives nothing the permission does not.
bool covers(const std::string& allowed, const std::string& requested);

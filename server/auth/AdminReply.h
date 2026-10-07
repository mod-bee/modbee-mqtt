// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// The shape of every answer on $modbee/admin/<clientid>/reply: the request's
// own "id" echoed, "ok", and on a refusal an "error" a person can read. One
// place, because the logins and the settings answer the same channel and an
// asker must not have to tell which half of the server wrote the reply.

#include "Json.h"

#include <string>

namespace admin_reply {

inline std::string reply(const json::Value& request, bool ok, json::Object body = json::Object()) {
    if (request.has("id")) {
        const json::Value& id = request["id"];
        if (id.type == json::Value::String) body.add("id", id.text);
        else if (id.type == json::Value::Number) body.add("id", (long long)id.number);
    }
    body.add("ok", ok);
    return body.str();
}

inline std::string failure(const json::Value& request, const std::string& error) {
    json::Object body;
    body.add("error", error);
    return reply(request, false, body);
}

} // namespace admin_reply

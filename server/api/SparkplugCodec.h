// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// SPARKPLUG B, EVERY PART OF IT, FOR THE APPLICATION API
// =============================================================================
//
// The wire payload (proto/modbee/sparkplug/v1/wire.proto) to the typed one the
// API gives an application (modbee.app.v1.SparkplugPayload), and back: every
// datatype, templates, datasets, property sets and their lists, metadata,
// arrays packed as section 6.4.17 packs them (little-endian; booleans a
// uint32 count and their bits, most significant first; strings NUL-ended), and
// compressed payloads (SPBV1.0_COMPRESSED: DEFLATE, with or without its zlib
// header, or GZIP). So the application carries no Sparkplug codec of its own.
//
// The library's codec (src/sparkplug) is a board's: scalars only, by design.
// This is the server's, and lives only where protobuf does.

#include "modbee/app/v1/app.pb.h"
#include "AppHub.h"

#include <string>

namespace sparkplug_codec {

// A spBv1.0/... topic (STATE included), or not one.
bool isSparkplugTopic(const std::string& topic);

// The topic and its payload, read. False - with `out.error` saying why, and
// what the topic said still filled in - when either cannot be. `names`: what a
// host knew of the aliases (data carries neither name nor, usually, datatype),
// filled in where the payload has only the alias.
bool decode(const std::string& topic, const std::string& payload, modbee::app::v1::SparkplugMessage& out,
            const std::vector<AppSparkplugName>* names = nullptr);

// A payload's bytes; false, with `error`, for a value its datatype cannot hold.
bool encode(const modbee::app::v1::SparkplugPayload& payload, bool compress, std::string& out, std::string& error);

} // namespace sparkplug_codec

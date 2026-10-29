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
// The library's codec (src/sparkplug) is a board's: it carries every datatype,
// a DataSet's or Template's as its encoded bytes, and reads them where they lie.
// This is the server's, typed through, and lives only where protobuf does.

#include "modbee/app/v1/app.pb.h"
#include "AppHub.h"

#include <string>

namespace sparkplug_codec {

// A spBv1.0/... topic (STATE included), or not one.
bool isSparkplugTopic(const std::string& topic);

// Text from a Sparkplug payload made fit for a proto3 string: an invalid UTF-8
// sequence becomes U+FFFD. A proto2 string is not checked, a proto3 one must be,
// or the whole reply cannot be sent.
std::string utf8(const std::string& in);

// The topic and its payload, read. False - with `out.error` saying why, and
// what the topic said still filled in - when either cannot be. `names`: what a
// host knew of the aliases (data carries neither name nor, usually, datatype),
// filled in where the payload has only the alias.
bool decode(const std::string& topic, const std::string& payload, modbee::app::v1::SparkplugMessage& out,
            const std::vector<AppSparkplugName>* names = nullptr);

// A payload's bytes; false, with `error`, for a value its datatype cannot hold.
bool encode(const modbee::app::v1::SparkplugPayload& payload, bool compress, std::string& out, std::string& error);

// One value as the library's codec carries it (src/sparkplug): an array's packed bytes_value, a DataSet's or a
// Template's encoded message. For an edge node the server runs. False, with `error`, for a value that is not of that
// datatype (an array metric given anything but array_value, say).
bool encodeValue(uint32_t datatype, const modbee::app::v1::SparkplugValue& value, std::string& out,
                 std::string& error);
// The reverse, for a node's values reported back.
bool decodeValue(uint32_t datatype, const std::string& bytes, modbee::app::v1::SparkplugValue& out, std::string& error);

// A Template - a definition or an instance - as its encoded Payload.Template, and back.
bool encodeTemplate(const modbee::app::v1::SparkplugTemplate& value, std::string& out, std::string& error);
bool decodeTemplate(const std::string& bytes, modbee::app::v1::SparkplugTemplate& out, std::string& error);

// A PropertySet as its encoded body (its keys, then its values) - what an edge node's births carry of a metric
// beside engUnit, Quality and the range - and back.
bool encodePropertySet(const modbee::app::v1::SparkplugPropertySet& set, std::string& out, std::string& error);
bool decodePropertySet(const std::string& bytes, modbee::app::v1::SparkplugPropertySet& out, std::string& error);

} // namespace sparkplug_codec

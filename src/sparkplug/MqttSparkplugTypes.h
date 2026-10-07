// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// =============================================================================
// SPARKPLUG B: THE NAMES AND NUMBERS THE SPECIFICATION FIXES
// =============================================================================
//
// Sparkplug B is a convention on top of plain MQTT: a topic namespace, protobuf
// payloads, sequence numbers, and birth and death certificates. No broker
// implements it and none needs to - a conformant MQTT server carries it without
// knowing what it is, which is what [tck-id-conformance-mqtt-qos0],
// [tck-id-conformance-mqtt-qos1], [tck-id-conformance-mqtt-will-messages] and
// [tck-id-conformance-mqtt-retained] amount to between them.
//
// EVERY CONSTANT HERE CITES THE STATEMENT THAT FIXES IT. The specification
// numbers all 295 of its requirements with a `tck-id`, and those identifiers
// are also the names of the assertions in the Eclipse conformance kit - so a
// citation here, a test name in test/, and a TCK result all say the same word.
// Where a value is fixed by the protobuf schema rather than by prose, the
// citation is the schema instead.
//
// Source of truth: Sparkplug Specification 3.0.0, 2022-11-16, and
// sparkplug_b.proto from eclipse-tahu/tahu. Both are kept, with hashes, in
// internal/sparkplug/ - grep internal/sparkplug/spec.txt for any tck-id below.
// 3.0.0 is current: the specification repository carries exactly two tags,
// v3.0.0 and v3.0.0-rc1.
//
// NOTHING IN THIS DIRECTORY KNOWS ABOUT OPENPLC, AND NOTHING BELOW IT KNOWS
// ABOUT SPARKPLUG. This is an L4 layer beside MqttDevice: it calls MqttClient's
// public API and the layers underneath do not know it exists.

#include "../MqttFeatures.h"

#include <stdint.h>
#include <stddef.h>

// --- The namespace ---
//
// [tck-id-topic-structure-namespace-a] For the Sparkplug B version of the
// payload definition, the UTF-8 string constant for the namespace element MUST
// be spBv1.0.
//
// There was an "A" version. It is deprecated and this library will not speak
// it: a second payload encoding for a protocol we are adding today would be
// dead code with a maintenance cost and no user.
#define MQ_SP_NAMESPACE "spBv1.0"

// [tck-id-payloads-state-will-message] Host Applications live under a fixed
// second-level element rather than a group id.
#define MQ_SP_STATE_ELEMENT "STATE"

// --- Message types ---
//
// [tck-id-topic-structure] namespace/group_id/message_type/edge_node_id/[device_id]
//
// The order here is the order the specification introduces them, and the
// device-level types are contiguous so `mq_sparkplugIsDeviceType()` is a range
// check rather than a switch somebody will forget to extend.
enum class MqttSparkplugType : uint8_t {
    // Node level. [tck-id-topic-structure-namespace-device-id-non-associated-message-types]
    // says device_id MUST NOT appear on any of these.
    NBirth = 0,
    NDeath,
    NData,
    NCmd,

    // Device level. [tck-id-topic-structure-namespace-device-id-associated-message-types]
    // says device_id MUST appear on all of these.
    DBirth,
    DDeath,
    DData,
    DCmd,

    // The Host Application's own birth and death, which is neither node nor
    // device level and carries JSON rather than protobuf.
    State,
};

#define MQ_SP_TYPE_COUNT 9

// The wire spelling: "NBIRTH", "DDATA", "STATE". Uppercase, and that is the
// specification's spelling rather than a convention - Sparkplug identifiers are
// case sensitive, [tck-id-case-sensitivity-sparkplug-ids].
const char* mq_sparkplugTypeName(MqttSparkplugType type);

// The reverse, for parsing a topic. Returns false rather than guessing.
bool mq_sparkplugTypeFromName(const char* name, size_t length,
                              MqttSparkplugType& out);

// True for DBIRTH, DDEATH, DDATA and DCMD - the four that carry a device_id.
inline bool mq_sparkplugIsDeviceType(MqttSparkplugType type) {
    return type >= MqttSparkplugType::DBirth && type <= MqttSparkplugType::DCmd;
}

// True for NBIRTH and DBIRTH. Births are where names, datatypes and aliases are
// declared, and where the rules differ most from everything else.
inline bool mq_sparkplugIsBirth(MqttSparkplugType type) {
    return type == MqttSparkplugType::NBirth || type == MqttSparkplugType::DBirth;
}

// True for NCMD and DCMD - inbound, from a Host Application to us.
inline bool mq_sparkplugIsCommand(MqttSparkplugType type) {
    return type == MqttSparkplugType::NCmd || type == MqttSparkplugType::DCmd;
}

// --- Datatypes ---
//
// The enum values are fixed by sparkplug_b.proto and repeated in spec section
// 6.4.16, and they are NOT contiguous by meaning: the scalars are 1-15, the
// structured types 16-21, and the arrays 22-34. The numbers are the wire
// contract, so they are written out rather than left to the compiler.
//
// [tck-id-payloads-metric-datatype-value-type] The datatype MUST be an
// unsigned 32-bit integer - hence uint32_t, not the enum, in the payload.
enum class MqttSparkplugDataType : uint32_t {
    Unknown = 0,

    // Scalars. Section 6.4.17 gives each one its protobuf field, and the
    // mapping is the part of this codec that is easy to get wrong:
    // see mq_sparkplugValueField() below.
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    UInt8 = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,
    Float = 9,
    Double = 10,
    Boolean = 11,
    String = 12,
    DateTime = 13,
    Text = 14,
    UUID = 15,

    // Structured. DataSet, Template, PropertySet and PropertySetList are the
    // nested, mutually recursive messages in the schema.
    //
    // THREE OF THESE FOUR ARE NOT SUPPORTED, DELIBERATELY, AND ARE REFUSED BY
    // NAME RATHER THAN DEFAULTED. They are most of the codec's difficulty and
    // none of them is needed to publish a plant. PropertySet IS supported,
    // with scalar values, because it is where engineering units and quality
    // codes live and a number with no unit is not much use to an integrator.
    //
    // A Template is refused at declaration time, not at encode time:
    // [tck-id-topics-nbirth-templates] requires every Template definition to be
    // published in the NBIRTH, so half-supporting them produces a node that is
    // wrong in a way the far end cannot detect.
    DataSet = 16,
    Bytes = 17,
    File = 18,
    Template = 19,
    PropertySet = 20,
    PropertySetList = 21,

    // Arrays. Not supported; see above.
    Int8Array = 22,
    Int16Array = 23,
    Int32Array = 24,
    Int64Array = 25,
    UInt8Array = 26,
    UInt16Array = 27,
    UInt32Array = 28,
    UInt64Array = 29,
    FloatArray = 30,
    DoubleArray = 31,
    BooleanArray = 32,
    StringArray = 33,
    DateTimeArray = 34,
};

// "Int8", "Double", "DateTime". The spelling in the schema, because a log line
// that says the same word as the .proto is one lookup shorter.
const char* mq_sparkplugDataTypeName(MqttSparkplugDataType type);

// Whether this build can carry a value of that datatype. Everything the codec
// refuses, it refuses through this - one predicate rather than a condition
// repeated in the encoder and the decoder and drifting between them.
bool mq_sparkplugDataTypeSupported(MqttSparkplugDataType type);

// --- Quality ---
//
// [tck-id-payloads-propertyset-quality-value-type] The type of the Property
// Value MUST be a value of 3 which represents a Signed 32-bit Integer.
// [tck-id-payloads-propertyset-quality-value-value] The value ... MUST be an
// int_value and be one of the valid quality codes of 0, 192, or 500.
//
// Three values and nothing else, which is why these are constants and the
// property is typed rather than an open int32.
#define MQ_SP_QUALITY_BAD    0
#define MQ_SP_QUALITY_GOOD   192
#define MQ_SP_QUALITY_STALE  500

// The reserved property name the quality code travels under.
#define MQ_SP_PROPERTY_QUALITY "Quality"

// --- Reserved metric names ---
//
// [tck-id-operational-behavior-data-commands-rebirth-datatype] The Node
// Control/Rebirth metric MUST be a Boolean; -rebirth-value says its value in an
// NBIRTH MUST be false. Every NBIRTH MUST include it, so a Host Application
// always has a way to ask for the birth sequence again.
#define MQ_SP_METRIC_NODE_REBIRTH   "Node Control/Rebirth"
#define MQ_SP_METRIC_DEVICE_REBIRTH "Device Control/Rebirth"

// [tck-id-topics-nbirth-bdseq-included] A bdSeq number as a metric MUST be
// included in the payload. The spelling is exact and it is NOT uppercase -
// [tck-id-case-sensitivity-metric-names] makes metric names case sensitive, so
// "BDSEQ" is a different metric and a Host Application will not find it.
#define MQ_SP_METRIC_BDSEQ "bdSeq"

// --- Reserved aliases ---
//
// [tck-id-payloads-alias-birth-requirement] requires EVERY metric in a birth to
// carry both a name and an alias, which includes the two the library adds
// itself. Their aliases are reserved constants at the top of the range rather
// than numbers from the application's counter, for two reasons: declaring a
// metric can never collide with them, and they do not shift when the metric set
// changes - so a host that cached them stays correct.
//
// [tck-id-payloads-alias-uniqueness] requires uniqueness across the node's
// entire metric set, and an application would have to declare four billion
// metrics to reach these.
#define MQ_SP_ALIAS_BDSEQ         0xFFFFFFFFu
#define MQ_SP_ALIAS_NODE_REBIRTH  0xFFFFFFFEu
#define MQ_SP_ALIAS_DEVICE_REBIRTH 0xFFFFFFFDu

// --- Limits ---
//
// Fixed at compile time and allocated once, because this has to run inside a
// PLC scan cycle on a 32-bit microcontroller. A board build that needs more
// says so with -D rather than discovering a heap it does not have.

// Metrics per payload. 32 matches MQTT_STRUCT_MEMBERS in the OpenPLC wrapper,
// which is what a SPARKPLUG_STRUCT block will bind, plus room for bdSeq and
// Node Control/Rebirth - which an NBIRTH must carry whatever else is in it.
// A host defaults to 256 (MqttFeatures.h), so a host application can read the
// births of real plant nodes.
#ifndef MQ_SP_MAX_METRICS
#define MQ_SP_MAX_METRICS 34
#endif

// Properties per metric. Four, and the number is a size decision rather than a
// generous one: a property slot is forty bytes, and a payload holds
// MQ_SP_MAX_METRICS metrics. At eight per metric the payload passes 13 kB,
// which on an ESP8266 with 80 kB of RAM is most of the heap for one message.
// Four covers engUnit, Quality, description and one more, which is what an
// NBIRTH actually carries.
#ifndef MQ_SP_MAX_PROPERTIES
#define MQ_SP_MAX_PROPERTIES 4
#endif

// Bytes of storage a decoded payload keeps for its metric names, property keys
// and uuid.
//
// THE DECODER HAS TO COPY THE NAMES AND ONLY THE NAMES. Everything else in a
// decoded payload is a span into the caller's buffer, which is the rule the
// rest of this library follows (MqttPacket does the same). Names cannot be:
// they arrive length-prefixed and not null-terminated, every caller wants a
// `const char*`, and terminating in place would mean writing into the received
// bytes. So the payload carries its own arena and the names point into it,
// which also means a decoded payload survives the buffer it came from.
//
// 34 metrics at an average 46 characters. A name longer than the arena has
// room for fails the decode by name rather than being truncated.
#ifndef MQ_SP_NAME_BYTES
#define MQ_SP_NAME_BYTES 1600
#endif

// [tck-id-payloads-sequence-num-incrementing] ... continually increasing by one
// in each message ... until a value of 255 is reached. At that point, the
// sequence number of the following message MUST be zero.
//
// So seq and bdSeq are both 0-255 on the wire even though the schema field is a
// uint64. The narrow type here is what makes the wrap free.
#define MQ_SP_SEQUENCE_MAX 255

// --- What went wrong ---
//
// Its own enum rather than MqttError, and the reason is honesty: MqttError's
// values carry MQTT's meanings in their comments - VbiTooLong says "a fifth
// continuation byte. A Variable Byte Integer is at most four", which is true of
// MQTT and false of protobuf, whose varints run to ten bytes. Reusing it would
// make a correct comment wrong.
enum class MqttSparkplugError : uint8_t {
    None = 0,

    // Ran out of bytes partway through a field. What a truncated payload
    // produces, and what the truncation sweep in the tests drives.
    Truncated,

    // A varint with an eleventh continuation byte. Ten is the most a 64-bit
    // value needs, and anything longer is either corruption or an attempt to
    // smuggle a different value past a parser that stops earlier.
    VarintTooLong,

    // A field's wire type is not the one the schema gives it - a length
    // delimited `seq`, a varint `metrics`. The bytes parsed and the structure
    // is wrong, which is a different fault from running out of bytes.
    WireTypeMismatch,

    // A length prefix longer than the bytes that follow it. Distinguished from
    // Truncated because it is the shape a hostile payload takes: a small
    // message claiming a large field.
    LengthOverrun,

    // The writer was asked for more room than it has. Never the peer's fault -
    // always ours, and always caught before a byte is written.
    Overflow,

    // A datatype this build does not carry: DataSet, Template, PropertySetList
    // or any array type. Named rather than defaulted, because a metric silently
    // decoded as zero is the fault that survives commissioning.
    DataTypeNotSupported,

    // More metrics or properties in one payload than MQ_SP_MAX_METRICS or
    // MQ_SP_MAX_PROPERTIES. Ours, not theirs: the payload was legal.
    TooManyMetrics,

    // A metric name that is not valid UTF-8, or a topic element containing one
    // of the reserved characters + / or #.
    // [tck-id-topic-structure-namespace-valid-group-id] and siblings.
    InvalidName,

    // A value whose type does not match the metric's declared datatype - an
    // Int32 metric handed a string. Caught at the API, before encoding.
    ValueTypeMismatch,
};

const char* mq_sparkplugErrorName(MqttSparkplugError error);

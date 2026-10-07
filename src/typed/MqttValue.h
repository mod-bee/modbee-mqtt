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
// A NUMBER ON THE WIRE
// =============================================================================
//
// MQTT carries bytes and says nothing about what they mean. At 5.0 there are
// two properties that can say - Payload Format Indicator [V5:3.3.2.3.2] and
// Content Type [V5:3.3.2.3.9] - and at 3.1.1 there is nothing at all. That is
// not a gap this library can fill by inventing a format; it is a gap that has
// to be closed by the two ends agreeing, and the only useful thing a library
// can do is make the agreement explicit and default it to whatever everything
// else already reads.
//
// SO WHAT DOES EVERYTHING ELSE ALREADY READ? This was checked rather than
// assumed, because the wrong answer here makes the library useless with the
// tools a plant actually runs:
//
//   - Node-RED publishes a number as its DECIMAL TEXT. Pass it a JavaScript
//     number and it converts to a string before publishing.
//   - Ignition's MQTT Engine, on a custom namespace, infers a tag's data type
//     from the JSON value it receives: 70 becomes an Integer, 70.25 a Double.
//   - Neither reads a big-endian IEEE-754 payload without being told to.
//
// So TEXT IS THE DEFAULT, at both versions. It interoperates, it is readable
// in a broker log, and it is what the rest of the plant is already doing.
// Binary is available, is smaller, and is opt-in per binding - because two
// ends that have agreed on it get a real benefit and nobody else gets a
// surprise.
//
// At 5.0 the choice is STATED on the wire as well as assumed:
//   Text   -> Payload Format Indicator 1, Content Type "text/plain"
//   Binary -> Payload Format Indicator 0, Content Type "application/octet-stream"
//
// Sources for the two claims above are in docs/typed-values.md, with the dates
// they were checked. If Node-RED or Ignition change, that file is where the
// evidence is and this default is what should be revisited.

#include "../MqttFeatures.h"


#include "../MqttWire.h"

// FIXED WIDTH ONLY.
//
// `int` is 16 bits on AVR and 32 on ESP32, so binding one would change the
// bytes on the wire depending on which board was compiled - a fault that only
// appears when the second kind of device joins the plant, months later. There
// is no entry here for a type whose width is not in its name.
enum class MqttValueType : uint8_t {
    Bool = 0,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Text,
};

#define MQ_VALUE_TYPE_COUNT 12

const char* mq_valueTypeName(MqttValueType type);

enum class MqttEncoding : uint8_t {
    // Decimal text. The default, because it is what Node-RED and Ignition
    // read without being configured. "21.5", "1", "-7".
    Text = 0,

    // Fixed width, big-endian - network byte order, the same order every
    // length and identifier in MQTT itself uses. Smaller, and only for two
    // ends that have agreed.
    Binary,
};

// One value, whatever its type.
struct MqttValue {
    MqttValueType type = MqttValueType::Float64;
    union {
        bool asBool;
        int64_t asSigned;
        uint64_t asUnsigned;
        double asReal;
    };
    MqttSpan asText;        // only when type is Text

    MqttValue() : type(MqttValueType::Float64), asReal(0.0) {}

    bool isNumeric() const { return type != MqttValueType::Text && type != MqttValueType::Bool; }

    // The value as a double, whatever it is stored as. For a deadband
    // comparison and for an application that just wants the number.
    double number() const;
};

// Bytes for a value. Returns the length written, or 0 if it would not fit.
//
// Never writes a partial value: a caller that measured wrongly gets nothing
// rather than half a number, because half a number reads as a whole one.
size_t mq_encodeValue(const MqttValue& value, MqttEncoding encoding,
                      uint8_t* out, size_t capacity);

// The other way. False when the bytes are not a value of that type - a text
// payload that is not a number, or a binary payload of the wrong length.
//
// Refused rather than salvaged. "21.5 degC" parsed as 21.5 would be a library
// quietly discarding the part it did not understand, and the part it did not
// understand is where the fault usually is.
bool mq_decodeValue(const uint8_t* data, size_t length, MqttValueType type,
                    MqttEncoding encoding, MqttValue& out);

// How many bytes a binary value of this type occupies. 0 for Text, which has
// no fixed width.
size_t mq_binaryWidth(MqttValueType type);

// What to put in Payload Format Indicator and Content Type at 5.0.
uint8_t mq_payloadFormatFor(MqttEncoding encoding);
const char* mq_contentTypeFor(MqttEncoding encoding);


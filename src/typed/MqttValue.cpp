// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttValue.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "../MqttNoExcept.h"

const char* mq_valueTypeName(MqttValueType type) {
    switch (type) {
        case MqttValueType::Bool:    return "bool";
        case MqttValueType::Int8:    return "int8";
        case MqttValueType::UInt8:   return "uint8";
        case MqttValueType::Int16:   return "int16";
        case MqttValueType::UInt16:  return "uint16";
        case MqttValueType::Int32:   return "int32";
        case MqttValueType::UInt32:  return "uint32";
        case MqttValueType::Int64:   return "int64";
        case MqttValueType::UInt64:  return "uint64";
        case MqttValueType::Float32: return "float32";
        case MqttValueType::Float64: return "float64";
        case MqttValueType::Text:    return "text";
    }
    return "?";
}

double MqttValue::number() const {
    switch (type) {
    case MqttValueType::Bool:    return asBool ? 1.0 : 0.0;
    case MqttValueType::Int8:
    case MqttValueType::Int16:
    case MqttValueType::Int32:
    case MqttValueType::Int64:   return (double)asSigned;
    case MqttValueType::UInt8:
    case MqttValueType::UInt16:
    case MqttValueType::UInt32:
    case MqttValueType::UInt64:  return (double)asUnsigned;
    case MqttValueType::Float32:
    case MqttValueType::Float64: return asReal;
    case MqttValueType::Text:    return 0.0;
    }
    return 0.0;
}

size_t mq_binaryWidth(MqttValueType type) {
    switch (type) {
    case MqttValueType::Bool:
    case MqttValueType::Int8:
    case MqttValueType::UInt8:   return 1;
    case MqttValueType::Int16:
    case MqttValueType::UInt16:  return 2;
    case MqttValueType::Int32:
    case MqttValueType::UInt32:
    case MqttValueType::Float32: return 4;
    case MqttValueType::Int64:
    case MqttValueType::UInt64:
    case MqttValueType::Float64: return 8;
    case MqttValueType::Text:    return 0;
    }
    return 0;
}

uint8_t mq_payloadFormatFor(MqttEncoding encoding) {
    // [V5:3.3.2.3.2] 0 means unspecified bytes; 1 means UTF-8 encoded
    // character data. Saying 1 for text is not decoration - a broker MAY
    // validate it, and ours does.
    return encoding == MqttEncoding::Text ? 1 : 0;
}

const char* mq_contentTypeFor(MqttEncoding encoding) {
    return encoding == MqttEncoding::Text ? "text/plain" : "application/octet-stream";
}

namespace {

void writeBigEndian(uint8_t* out, uint64_t bits, size_t width) {
    // Network byte order, the same order every length and identifier in MQTT
    // itself uses [V5:1.5.2]. Building it by shifting rather than by casting a
    // struct means the bytes are the same whatever the machine's own order is.
    for (size_t index = 0; index < width; index++) {
        out[index] = (uint8_t)((bits >> (8 * (width - 1 - index))) & 0xFF);
    }
}

uint64_t readBigEndian(const uint8_t* data, size_t width) {
    uint64_t bits = 0;
    for (size_t index = 0; index < width; index++) {
        bits = (bits << 8) | data[index];
    }
    return bits;
}

// IEEE-754 through a memcpy rather than a cast. Type-punning through a union
// or a pointer cast is undefined behaviour and the undefined behaviour
// sanitizer says so; memcpy is the one form every compiler turns into the same
// single instruction.
uint32_t floatBits(float value) {
    uint32_t bits = 0;
    memcpy(&bits, &value, sizeof bits);
    return bits;
}
float bitsToFloat(uint32_t bits) {
    float value = 0.0f;
    memcpy(&value, &bits, sizeof value);
    return value;
}
uint64_t doubleBits(double value) {
    uint64_t bits = 0;
    memcpy(&bits, &value, sizeof bits);
    return bits;
}
double bitsToDouble(uint64_t bits) {
    double value = 0.0;
    memcpy(&value, &bits, sizeof value);
    return value;
}

} // namespace

// =============================================================================
// ENCODE
// =============================================================================

size_t mq_encodeValue(const MqttValue& value, MqttEncoding encoding,
                      uint8_t* out, size_t capacity) {
    if (out == nullptr) return 0;

    if (value.type == MqttValueType::Text) {
        if (value.asText.length > capacity) return 0;
        if (value.asText.length > 0) {
            memcpy(out, value.asText.data, value.asText.length);
        }
        return value.asText.length;
    }

    if (encoding == MqttEncoding::Binary) {
        const size_t width = mq_binaryWidth(value.type);
        if (width == 0 || width > capacity) return 0;

        switch (value.type) {
        case MqttValueType::Bool:
            out[0] = value.asBool ? 1 : 0;
            return 1;
        case MqttValueType::Float32:
            writeBigEndian(out, floatBits((float)value.asReal), 4);
            return 4;
        case MqttValueType::Float64:
            writeBigEndian(out, doubleBits(value.asReal), 8);
            return 8;
        default:
            break;
        }

        const uint64_t bits =
            value.type == MqttValueType::Int8 || value.type == MqttValueType::Int16 ||
            value.type == MqttValueType::Int32 || value.type == MqttValueType::Int64
                ? (uint64_t)value.asSigned : value.asUnsigned;
        writeBigEndian(out, bits, width);
        return width;
    }

    // --- Text ---
    //
    // Formatted into a local buffer first, so a value that will not fit writes
    // nothing at all rather than a truncated number. "21.5" truncated to "21"
    // is not an error anybody would notice, and it is wrong by 0.5.
    char scratch[48];
    int written = 0;

    switch (value.type) {
    case MqttValueType::Bool:
        // "1" and "0" rather than "true" and "false". Both are read as a
        // boolean by Node-RED and by Ignition; only the digits are also read
        // as a number by everything else.
        written = snprintf(scratch, sizeof scratch, "%d", value.asBool ? 1 : 0);
        break;
    case MqttValueType::Int8:
    case MqttValueType::Int16:
    case MqttValueType::Int32:
    case MqttValueType::Int64:
        written = snprintf(scratch, sizeof scratch, "%lld",
                                (long long)value.asSigned);
        break;
    case MqttValueType::UInt8:
    case MqttValueType::UInt16:
    case MqttValueType::UInt32:
    case MqttValueType::UInt64:
        written = snprintf(scratch, sizeof scratch, "%llu",
                                (unsigned long long)value.asUnsigned);
        break;
    case MqttValueType::Float32:
        // %.9g round-trips every float exactly, and no shorter format does.
        // A reading printed with fewer digits and read back is a DIFFERENT
        // number, which turns a comparison against a setpoint into a coin
        // toss.
        written = snprintf(scratch, sizeof scratch, "%.9g", (double)(float)value.asReal);
        break;
    case MqttValueType::Float64:
        // %.17g does the same for a double.
        written = snprintf(scratch, sizeof scratch, "%.17g", value.asReal);
        break;
    case MqttValueType::Text:
        return 0;   // handled above
    }

    if (written <= 0 || (size_t)written > capacity) return 0;
    memcpy(out, scratch, (size_t)written);
    return (size_t)written;
}

// =============================================================================
// DECODE
// =============================================================================

bool mq_decodeValue(const uint8_t* data, size_t length, MqttValueType type,
                    MqttEncoding encoding, MqttValue& out) {
    out = MqttValue();
    out.type = type;

    if (type == MqttValueType::Text) {
        out.asText.data = data;
        out.asText.length = (uint32_t)length;
        return true;
    }
    if (data == nullptr) return false;

    if (encoding == MqttEncoding::Binary) {
        const size_t width = mq_binaryWidth(type);

        // EXACTLY the right width. A payload one byte long for an int32 is not
        // a small int32 - it is a different value from a sender that disagrees
        // with us, and reading it as 0x000000XX would be inventing data.
        if (width == 0 || length != width) return false;

        switch (type) {
        case MqttValueType::Bool:
            out.asBool = data[0] != 0;
            return true;
        case MqttValueType::Float32:
            out.asReal = (double)bitsToFloat((uint32_t)readBigEndian(data, 4));
            return true;
        case MqttValueType::Float64:
            out.asReal = bitsToDouble(readBigEndian(data, 8));
            return true;
        default:
            break;
        }

        const uint64_t bits = readBigEndian(data, width);
        switch (type) {
        case MqttValueType::Int8:   out.asSigned = (int8_t)bits; return true;
        case MqttValueType::Int16:  out.asSigned = (int16_t)bits; return true;
        case MqttValueType::Int32:  out.asSigned = (int32_t)bits; return true;
        case MqttValueType::Int64:  out.asSigned = (int64_t)bits; return true;
        default:                    out.asUnsigned = bits; return true;
        }
    }

    // --- Text ---
    //
    // Copied into a NUL-terminated buffer, because the payload is not
    // terminated and strtod would run off the end of it. A 200 kB payload is
    // not a number, so the limit is also the sanity check.
    if (length == 0 || length >= 47) return false;

    char scratch[48];
    memcpy(scratch, data, length);
    scratch[length] = '\0';

    char* end = nullptr;

    if (type == MqttValueType::Bool) {
        // "1"/"0" as published, and "true"/"false" because other tools publish
        // those and refusing them would make this layer useless against them.
        if (strcmp(scratch, "1") == 0 || strcmp(scratch, "true") == 0) {
            out.asBool = true;
            return true;
        }
        if (strcmp(scratch, "0") == 0 || strcmp(scratch, "false") == 0) {
            out.asBool = false;
            return true;
        }
        return false;
    }

    if (type == MqttValueType::Float32 || type == MqttValueType::Float64) {
        const double parsed = strtod(scratch, &end);
        // THE WHOLE PAYLOAD MUST BE THE NUMBER. "21.5 degC" parsed as 21.5
        // would be the library quietly discarding the part it did not
        // understand, and that part is where the disagreement is.
        if (end == scratch || *end != '\0') return false;
        if (!isfinite(parsed)) return false;
        out.asReal = parsed;
        return true;
    }

    const bool isSigned =
        type == MqttValueType::Int8 || type == MqttValueType::Int16 ||
        type == MqttValueType::Int32 || type == MqttValueType::Int64;

    if (isSigned) {
        const long long parsed = strtoll(scratch, &end, 10);
        if (end == scratch || *end != '\0') return false;
        out.asSigned = (int64_t)parsed;
    } else {
        // A leading '-' would be accepted by strtoull and wrapped round to an
        // enormous positive number, which is how a -1 becomes 18 quintillion
        // on somebody's dashboard.
        if (scratch[0] == '-') return false;
        const unsigned long long parsed = strtoull(scratch, &end, 10);
        if (end == scratch || *end != '\0') return false;
        out.asUnsigned = (uint64_t)parsed;
    }

    // And it must fit the type it claims to be. An int16 binding receiving
    // 70000 has a sender that disagrees about the type, and truncating to 4464
    // would hide that for ever.
    const size_t width = mq_binaryWidth(type);
    if (width < 8) {
        if (isSigned) {
            const int64_t limit = (int64_t)1 << (width * 8 - 1);
            if (out.asSigned >= limit || out.asSigned < -limit) return false;
        } else {
            const uint64_t limit = (uint64_t)1 << (width * 8);
            if (out.asUnsigned >= limit) return false;
        }
    }
    return true;
}


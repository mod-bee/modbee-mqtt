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
// THE CODEC INTERFACE
// =============================================================================
//
// One implementation per wire format. Two whole codecs rather than one with
// `if (version == 5)` inside every function.
//
// That is deliberate and it is the most important structural decision in the
// library. Branches scattered through shared functions are where version bugs
// live: they pass whichever version the test happened to exercise, and the
// other path is never run. Two codecs means every wire test runs twice, and a
// branch that was never written is a function that does not exist rather than
// a fall-through nobody noticed.
//
// Both encode AND decode every packet type, in both directions, from the
// start. A client never needs to decode a CONNECT - but a decoder that has
// only ever parsed one direction has half its branches untested, and a broker
// built on this later would be building on the untested half.
//
// THE STREAM NEVER RESYNCHRONISES.
//
// MQTT has no frame delimiter, no checksum and no start byte. One stray octet
// desynchronises the connection permanently, and every "packet" after it is
// garbage that may still parse. So a codec NEVER searches for a boundary after
// a failure: it reports, and the session tears the connection down - sending a
// DISCONNECT carrying the reason code where the version has one. That is the
// opposite of what a self-delimiting fieldbus protocol does, and it is the
// thing most likely to be got wrong by someone arriving from one.

#include "MqttPacket.h"

class MqttCodec {
public:
    virtual ~MqttCodec() = default;

    virtual MqttVersion version() const = 0;

    // Decodes one whole packet body - the bytes after the fixed header.
    //
    // The caller has already read the fixed header and waited for
    // header.remainingLength bytes, so `length` is exact. A body shorter than
    // the header promised is the reassembly's bug, not a peer's, and is
    // reported as Truncated either way.
    virtual MqttError decode(const MqttFixedHeader& header,
                             const uint8_t* body, size_t length,
                             MqttPacket& out) const = 0;

    // Bytes the whole packet will occupy, fixed header included. 0 when the
    // packet cannot be encoded at this version at all - a Topic Alias on
    // 3.1.1, an AUTH anywhere but 5.0.
    //
    // Always called before encode(). Nothing in this library ever writes a
    // packet it has not first measured.
    virtual size_t size(const MqttPacket& packet) const = 0;

    // Writes the packet. `written` receives the byte count on success.
    //
    // Fails without writing anything when the buffer is too small, so a
    // half-built packet is never mistaken for a whole one.
    virtual MqttError encode(uint8_t* out, size_t capacity,
                             const MqttPacket& packet, size_t& written) const = 0;
};

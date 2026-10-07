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
// ONE CONNECTION'S FRAMING
// =============================================================================
//
// Bytes in and out of one transport, turned into whole packets and back.
//
// This is the part of a connection that is the same at both ends. Reassembling
// a packet that arrived in four pieces, resuming a write that took 1,400 bytes
// of the 60,000 it was given, and refusing to resynchronise after a malformed
// one - none of that depends on whether this end initiated the connection or
// accepted it. It lived inside the client first and was lifted out when the
// broker needed exactly the same thing, which is the outcome R1 is for.
//
// THERE IS NO POLICY HERE. This layer never decides to disconnect, never
// chooses a reason code, never touches a clock. It reports what it found and
// the role above decides - a client fails the connection, a broker drops one
// of hundreds, and neither answer belongs down here.
//
// PARTIAL WRITES ARE ORDINARY. The transmit side holds a cursor and resumes
// from it. It never restarts a packet, because putting the middle of a packet
// on the wire twice desynchronises the stream for good, and MQTT has no
// delimiter to recover on.

#include "MqttFeatures.h"     // MQTT_LINK_GROWS
#include "MqttTransport.h"
#include "MqttCodec.h"

#include <vector>


// What the receive side found. Deliberately not an error enum: three of these
// are perfectly ordinary and only two are faults.
enum class MqttLinkStatus : uint8_t {
    Idle = 0,     // nothing buffered and nothing arrived
    Incomplete,   // bytes are here, but not a whole packet yet
    Packet,       // a whole packet is present - `header` describes it
    Malformed,    // the fixed header itself cannot be parsed and never will be
    TooLarge,     // the packet is bigger than this end can hold  [V5:3.2.2.3.6]
    Closed,       // the peer half-closed, or the transport went away
};

class MqttLink {
public:
    // The transport is the caller's and must outlive this object. Buffers are
    // allocated once, here, and never grow afterwards: a device with 320 kB of
    // RAM cannot discover at three in the morning that a 200 kB publish wants
    // a reallocation.
    // `initialCapacity` non-zero makes both buffers START that small and grow
    // towards the capacities above only when a packet needs the room, then
    // shrink back once it has gone. Zero - the default - gives each buffer its
    // full capacity now, as it always has. A broker holding a hundred thousand
    // mostly-idle connections wants the first; a client wants the second.
    // Host builds only (MQTT_LINK_GROWS): on a board it is ignored.
    void begin(MqttTransport& transport, size_t receiveCapacity,
               size_t transmitCapacity, size_t initialCapacity = 0);

    // Forgets the transport and empties the buffers. Does NOT close the
    // transport - the link does not own it and does not know whether the
    // caller means to reuse it.
    void end();

    // Empties both buffers but keeps the transport and its capacities.
    //
    // For a reconnect: half a packet left over from the connection that just
    // died would be read as the first half of the next connection's first
    // packet, and MQTT has no delimiter that would ever reveal the mistake.
    void reset();

    MqttTransport* transport() const { return _transport; }
    bool isOpen() const { return _transport != nullptr && _transport->isConnected(); }
    size_t transmitCapacity() const { return _transmit.size(); }

    // --- Receiving ---

    // Reads whatever the transport has ready, without blocking. Stops early
    // once a whole packet is buffered, so one packet is never delayed behind
    // the arrival of the next.
    void fill();

    // Whether a whole packet is buffered, and what it is.
    //
    // `Malformed` here means the Remaining Length is not a valid Variable Byte
    // Integer - five bytes have arrived and none of them terminated it. That
    // is unrecoverable rather than premature, which is why it is distinct from
    // `Incomplete`.
    MqttLinkStatus inspect(MqttFixedHeader& header) const;

    // Decodes the packet `inspect` reported and removes it from the buffer.
    //
    // The bytes are consumed WHETHER OR NOT the decode succeeds. A packet that
    // will not parse is not going to parse on the next attempt either, and
    // leaving it in the buffer turns one bad packet into an endless loop.
    //
    // SPANS IN `out` POINT INTO THIS LINK'S RECEIVE BUFFER, and the next call
    // to fill() or take() overwrites them. Anything that must outlive the
    // dispatch copies what it needs, which is the same contract MqttPacket.h
    // states one layer down.
    MqttError take(const MqttCodec& codec, const MqttFixedHeader& header,
                   MqttPacket& out);

    // Where the bytes of the packet just inspected begin, so a caller that
    // must forward a packet untouched - a bridge, a broker republishing - can
    // take them without re-encoding.
    const uint8_t* rawPacket() const { return _receive.data() + _receiveStart; }

    size_t buffered() const { return _receiveLength; }

    // What the buffers hold right now. With growth on (begin()'s initial
    // capacity) these move between that and the limits; for diagnostics and
    // tests.
    size_t receiveCapacity() const { return _receive.size(); }

    // Whether the peer has gone. Separate from isOpen(): a peer that has sent
    // FIN accepts nothing while our own socket is still perfectly writable.
    bool peerGone() const;

    // --- Sending ---

    // Encodes the packet and appends it to the outbound queue, then pushes
    // what the transport will take.
    //
    // False when the packet cannot be encoded at this version, will not fit
    // the scratch buffer, or will not fit the queue. The caller learns which
    // from `lastEncodeError()` - a packet refused because 3.1.1 cannot express
    // it is a different conversation from one refused for want of room.
    // `encoded` and `encodedLength`, when given, hand back the bytes that
    // were queued - so a caller that must keep a copy of what it just sent,
    // as the session store does for replay, gets it WITHOUT ENCODING TWICE.
    // On a 200 kB publish that second encode is 200 kB of work and 200 kB of
    // buffer for nothing. The bytes are valid until the next send().
    bool send(const MqttCodec& codec, const MqttPacket& packet,
              const uint8_t** encoded = nullptr, size_t* encodedLength = nullptr);

    // Appends already-encoded bytes. For a retransmission out of the session
    // store, where the bytes were encoded when the packet was first built and
    // MUST go out unchanged but for the DUP flag.
    bool queue(const uint8_t* data, size_t length);

    // Bytes that were read from this connection by somebody else - a broker
    // shard that accepted it and then handed it to the shard that owns its
    // session - placed in the receive buffer as if this link had read them.
    // Call once, straight after begin(). False if they do not fit the limit.
    bool prime(const uint8_t* data, size_t length);

    // Pushes queued bytes at the transport, resuming from wherever the last
    // write stopped. Never blocks and never restarts a packet.
    void serviceTransmit();

    size_t transmitPending() const { return _transmitLength - _transmitCursor; }
    bool transmitIdle() const { return _transmitCursor >= _transmitLength; }

    MqttError lastEncodeError() const { return _lastEncodeError; }

    // --- Counters, for diagnostics and for tests ---
    uint32_t packetsSent() const { return _packetsSent; }
    uint32_t packetsReceived() const { return _packetsReceived; }
    uint64_t bytesSent() const { return _bytesSent; }
    uint64_t bytesReceived() const { return _bytesReceived; }

private:
    MqttTransport* _transport = nullptr;

    // Growth, when begin() was given an initial capacity. The limits are the
    // capacities begin() was given; the buffers are never larger.
    size_t _receiveLimit = 0;
    size_t _transmitLimit = 0;
#if MQTT_LINK_GROWS
    size_t _initialCapacity = 0;      // 0: fixed at the limits
    bool growing() const { return _initialCapacity != 0; }
#else
    static constexpr bool growing() { return false; }
#endif
    bool _sending = false;            // inside send(), whose out-param points into _transmit
    void growTransmit(size_t needed);
    void shrinkTransmitIfIdle();
    void compactTransmit();

    std::vector<uint8_t> _receive;

    // A READ CURSOR, NOT A COMPACTED BUFFER.
    //
    // take() used to memmove the remaining bytes down over the packet it had
    // just decoded. That was two problems wearing one line:
    //
    //   THE SPANS. MqttPacket's topic, payload and properties point INTO these
    //   bytes, and the move overwrote exactly the region they pointed at. The
    //   contract above promises they survive until the NEXT fill() or take();
    //   in fact this take() clobbered them. It was invisible only because
    //   fill() stopped at one whole packet, so the move length was always
    //   zero - and the moment fill() became a bulk read, every packet that
    //   shared a recv() with its successor would have had its contents
    //   silently replaced by the next packet's bytes.
    //
    //   THE COST. A client that pipelines fills the buffer, and consuming each
    //   packet moved everything behind it - O(n^2) in the number of packets
    //   per read, worst exactly when the broker is busiest.
    //
    // So nothing moves. _receiveStart is where the unconsumed bytes begin and
    // _receiveLength is how many there are; the buffer is compacted only when
    // the tail genuinely has no room left, which is the one case where the
    // spans are being discarded anyway.
    size_t _receiveStart = 0;
    size_t _receiveLength = 0;

    // An APPEND QUEUE, not a single slot. The first version of this held one
    // packet and refused a second while the first was going out, which
    // silently dropped acknowledgements: a PUBREC arriving mid-PUBLISH could
    // not raise its PUBREL, and the QoS 2 exchange stalled while the peer
    // retried for ever. A live broker found it in the first minute.
    std::vector<uint8_t> _transmit;
    size_t _transmitLength = 0;
    size_t _transmitCursor = 0;

    // Where a packet is encoded before being appended whole, so a half-written

    MqttError _lastEncodeError = MqttError::None;

    uint32_t _packetsSent = 0;
    uint32_t _packetsReceived = 0;
    uint64_t _bytesSent = 0;
    uint64_t _bytesReceived = 0;
};

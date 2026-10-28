// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttLink.h"

#include <string.h>
#include "MqttNoExcept.h"

void MqttLink::begin(MqttTransport& transport, size_t receiveCapacity,
                     size_t transmitCapacity, size_t initialCapacity) {
    _transport = &transport;

    // A floor rather than a trusted figure. A receive buffer smaller than the
    // largest fixed header cannot hold a packet header, let alone a packet,
    // and a caller who passed 4 by accident should get a working link rather
    // than a mystery.
    if (receiveCapacity < 64) receiveCapacity = 64;
    if (transmitCapacity < 64) transmitCapacity = 64;

    _receiveLimit = receiveCapacity;
    _transmitLimit = transmitCapacity;
#if MQTT_LINK_GROWS
    _initialCapacity = 0;
    if (initialCapacity != 0) {
        if (initialCapacity < 64) initialCapacity = 64;
        if (initialCapacity < receiveCapacity || initialCapacity < transmitCapacity) {
            _initialCapacity = initialCapacity;
            if (receiveCapacity > initialCapacity) receiveCapacity = initialCapacity;
            if (transmitCapacity > initialCapacity) transmitCapacity = initialCapacity;
        }
    }
    // A slot recycled from a connection that grew its buffers gives the
    // memory back here, rather than carrying it to the next occupant.
    if (_initialCapacity != 0) {
        if (_receive.size() > receiveCapacity) MQ_HEAP_BYTES().swap(_receive);
        if (_transmit.size() > transmitCapacity) MQ_HEAP_BYTES().swap(_transmit);
    }
#else
    (void)initialCapacity;      // fixed buffers on a board; see MQTT_LINK_GROWS
#endif

    // resize(), not assign(). Both give the buffers their capacity; assign()
    // also writes a zero into every byte of them, and these are 8 kB each by
    // default - so accepting a connection zeroed 16 kB that is about to be
    // overwritten by a recv() or an encode anyway. With sixteen accepts per
    // poll, a connection storm was memsetting a quarter of a megabyte per
    // pass. Nothing reads a byte of either buffer before it is written:
    // _receiveLength and _transmitLength say how far the valid bytes go.
    //
    // resize() on a vector that is already the right size does nothing at all,
    // which is what makes recycling a connection slot free.
    if (_receive.size() != receiveCapacity) _receive.resize(receiveCapacity);
    if (_transmit.size() != transmitCapacity) _transmit.resize(transmitCapacity);

    _receiveStart = 0;
    _receiveLength = 0;
    _transmitLength = 0;
    _transmitCursor = 0;
    _backpressured = false;
    _lastEncodeError = MqttError::None;
}

void MqttLink::end() {
    _transport = nullptr;
    reset();
}

void MqttLink::reset() {
    _receiveStart = 0;
    _receiveLength = 0;
    _transmitLength = 0;
    _transmitCursor = 0;
    _backpressured = false;
    _lastEncodeError = MqttError::None;
}

bool MqttLink::peerGone() const {
    if (_transport == nullptr) return true;
    return _transport->peerClosed() || !_transport->isConnected();
}

// =============================================================================
// RECEIVE
// =============================================================================

void MqttLink::fill() {
    if (_transport == nullptr) return;

    // GROW OR SHRINK, HERE AND ONLY HERE. fill() runs before a packet is
    // taken, never while one is being handled, so no span from the last take()
    // can point into a buffer that moves - the same reason the compaction
    // below is safe.
#if MQTT_LINK_GROWS
    if (growing()) {
        if (_receiveLength == 0) {
            if (_receive.size() > _initialCapacity) {
                MQ_HEAP_BYTES(_initialCapacity).swap(_receive);
                _receiveStart = 0;
            }
        } else {
            MqttFixedHeader header;
            if (mq_decodeFixedHeader(_receive.data() + _receiveStart, _receiveLength,
                                     header) == MqttError::None &&
                header.packetSize() > _receive.size() &&
                header.packetSize() <= _receiveLimit) {
                // At the front, so the room left is all at the back.
                if (_receiveStart > 0) {
                    memmove(_receive.data(), _receive.data() + _receiveStart, _receiveLength);
                    _receiveStart = 0;
                }
                // GROWN BY WHAT ARRIVED, NOT BY WHAT WAS ANNOUNCED. Sized to
                // the header at once, four bytes - "10 FA FF 03", before any
                // CONNECT - held a whole maximum packet per connection, and a
                // few thousand sockets held the broker's memory. Doubled only
                // once the bytes have filled it, the buffer is never more than
                // twice what the peer has actually sent.
                if (_receiveLength >= _receive.size()) {
                    const size_t doubled = _receive.size() * 2;
                    _receive.resize(doubled < header.packetSize() ? doubled : header.packetSize());
                }
            }
        }
    }
#endif

    // A packet may arrive in any number of pieces split at any offset, so
    // bytes are accumulated and the header re-examined until a whole one is
    // present. Splitting mid-header is the single most common source of
    // real-world MQTT decoder bugs, which is why the test channel does it at
    // every offset.
    // ONE BULK TAKE, not a byte at a time.
    //
    // This was a loop that asked the transport for one byte, through a virtual
    // call, and then re-ran mq_decodeFixedHeader() over everything received so
    // far to see whether a packet had finished - so a 122-byte PUBLISH cost
    // 122 virtual calls and 122 header decodes. The transports underneath had
    // already read those bytes in a single recv(); the interface simply had no
    // way to hand them over in one piece. It does now.
    if (_receiveStart > 0 && _receiveStart + _receiveLength >= _receive.size()) {
        // The tail is full but the front is not. Reclaim it - and this is the
        // ONLY place the buffer moves, which is why the spans from the last
        // take() are safe everywhere else.
        if (_receiveLength > 0) {
            memmove(_receive.data(), _receive.data() + _receiveStart, _receiveLength);
        }
        _receiveStart = 0;
    }

    const size_t room = _receive.size() - (_receiveStart + _receiveLength);
    if (room == 0) return;

    const size_t got = _transport->read(_receive.data() + _receiveStart + _receiveLength,
                                        room);
    _receiveLength += got;
    _bytesReceived += got;
}

MqttLinkStatus MqttLink::inspect(MqttFixedHeader& header) const {
    if (_receiveLength == 0) {
        return peerGone() ? MqttLinkStatus::Closed : MqttLinkStatus::Idle;
    }

    const MqttError error =
        mq_decodeFixedHeader(_receive.data() + _receiveStart, _receiveLength, header);

    if (error == MqttError::Truncated) {
        // Five bytes and still no terminator means the Remaining Length is not
        // a valid Variable Byte Integer [V5:MQTT-1.5.5-1]. There is no
        // delimiter to resynchronise on, so this cannot become valid by
        // waiting - it is a fault, not a "not yet".
        return _receiveLength >= 5 ? MqttLinkStatus::Malformed
                                   : MqttLinkStatus::Incomplete;
    }
    if (error != MqttError::None) return MqttLinkStatus::Malformed;

    // Larger than this end can hold. Our own limit rather than the peer's
    // mistake, which is why the caller answers with 0x95 Packet Too Large
    // [V5:3.2.2.3.6] rather than blaming the bytes.
    if (header.packetSize() > _receiveLimit) return MqttLinkStatus::TooLarge;
    // Within the limit but larger than the buffer is now: fill() grows it on
    // the next call. Until then the packet is simply not all here yet.
    if (header.packetSize() > _receive.size()) return MqttLinkStatus::Incomplete;

    if (_receiveLength < header.packetSize()) return MqttLinkStatus::Incomplete;

    return MqttLinkStatus::Packet;
}

MqttError MqttLink::take(const MqttCodec& codec, const MqttFixedHeader& header,
                         MqttPacket& out) {
    const size_t consumed = header.packetSize();
    if (consumed > _receiveLength) return MqttError::Truncated;

    const MqttError error = codec.decode(header,
                                         _receive.data() + _receiveStart
                                             + header.headerLength,
                                         header.remainingLength, out);
    _packetsReceived++;

    // Consumed either way. A packet that will not parse will not parse next
    // time either, and leaving it in the buffer turns one bad packet into a
    // loop that never ends.
    //
    // The decode happens FIRST, because the spans in `out` point into these
    // bytes and the caller reads them before the next fill().
    _receiveStart += consumed;
    _receiveLength -= consumed;
    if (_receiveLength == 0) _receiveStart = 0;

    return error;
}

// =============================================================================
// TRANSMIT
// =============================================================================

void MqttLink::compactTransmit() {
    if (_transmitCursor == 0) return;
    // Drop what has already gone out so the space is reusable.
    const size_t remaining = _transmitLength - _transmitCursor;
    if (remaining > 0) {
        memmove(_transmit.data(), _transmit.data() + _transmitCursor, remaining);
    }
    _transmitLength = remaining;
    _transmitCursor = 0;
}

// Room for `needed` more bytes, if growth is on and the limit allows. Doubling
// rather than exactly fitting, so a run of similar packets grows once.
void MqttLink::growTransmit(size_t needed) {
#if MQTT_LINK_GROWS
    if (!growing()) return;
    compactTransmit();
    const size_t want = _transmitLength + needed;
    if (want <= _transmit.size() || want > _transmitLimit) return;
    size_t size = _transmit.size() * 2;
    if (size < want) size = want;
    if (size > _transmitLimit) size = _transmitLimit;
    _transmit.resize(size);
#else
    (void)needed;
#endif
}

bool MqttLink::prime(const uint8_t* data, size_t length) {
    if (length == 0) return true;
    if (data == nullptr || length > _receiveLimit) return false;
    if (_receive.size() < length) _receive.resize(length);
    memcpy(_receive.data(), data, length);
    _receiveStart = 0;
    _receiveLength = length;
    _bytesReceived += length;
    return true;
}

bool MqttLink::queue(const uint8_t* data, size_t length) {
    if (data == nullptr || length == 0) return false;

    compactTransmit();
    if (growing() && _transmitLength + length > _transmit.size()) growTransmit(length);
    if (_transmitLength + length > _transmit.size()) return false;

    memcpy(_transmit.data() + _transmitLength, data, length);
    _transmitLength += length;
    return true;
}

bool MqttLink::send(const MqttCodec& codec, const MqttPacket& packet,
                    const uint8_t** encoded, size_t* encodedLength) {
    if (encoded) *encoded = nullptr;
    if (encodedLength) *encodedLength = 0;
    if (_transport == nullptr) return false;

    _lastEncodeError = MqttError::None;

    // NOTHING IS EVER WRITTEN WITHOUT BEING MEASURED FIRST. size() returning
    // zero means this packet cannot be expressed at this version at all - a
    // Topic Alias on 3.1.1, an AUTH anywhere but 5.0 - which is a different
    // failure from not fitting.
    const size_t needed = codec.size(packet);
    if (needed == 0) {
        _lastEncodeError = MqttError::NotSupportedByProtocol;
        return false;
    }
    // ENCODED STRAIGHT INTO THE QUEUE'S TAIL, not into a scratch buffer and
    // then copied.
    //
    // There used to be a third buffer, _scratch, the same size as the
    // transmit queue - so every connection carried 8 kB it never needed, and
    // every packet was written once into scratch and memcpy'd a second time
    // into _transmit. A 200 kB PUBLISH was 200 kB encoded plus 200 kB copied,
    // on a device this file's own header describes as having 320 kB of RAM.
    //
    // The half-written-packet hazard that scratch existed for is handled by
    // where the commit happens: _transmitLength is only advanced once the
    // encode has SUCCEEDED, so a failure leaves the queue exactly as it was
    // and nothing partial is ever visible to serviceTransmit().
    if (growing() && _transmit.size() - _transmitLength < needed) growTransmit(needed);
    const size_t room = _transmit.size() - _transmitLength;
    if (room < needed) {
        _lastEncodeError = MqttError::Overflow;
        return false;
    }

    size_t written = 0;
    const MqttError error = codec.encode(_transmit.data() + _transmitLength,
                                         room, packet, written);
    if (error != MqttError::None) {
        _lastEncodeError = error;
        return false;    // nothing committed; the queue is untouched
    }

    // The out-params point INTO the transmit queue now rather than into
    // scratch. Both are equally transient - MqttLink.h has always said these
    // bytes are only valid until the next send() - and MqttSession::
    // storeOutbound() copies them immediately, which is the only caller.
    if (encoded) *encoded = _transmit.data() + _transmitLength;
    if (encodedLength) *encodedLength = written;

    _transmitLength += written;

    _packetsSent++;
    // Flushed now, but NOT shrunk now: the caller is still holding `encoded`,
    // which points into this buffer.
    _sending = true;
    serviceTransmit();
    _sending = false;
    return true;
}

void MqttLink::serviceTransmit() {
    if (_transport == nullptr || _transmitCursor >= _transmitLength) {
        // Nothing pending - which is how a buffer that send() grew AND drained
        // in one call finds out it can shrink: send() could not do it while
        // its caller still held a pointer into the bytes.
        if (growing()) shrinkTransmitIfIdle();
        return;
    }

    while (_transmitCursor < _transmitLength) {
        const size_t remaining = _transmitLength - _transmitCursor;
        const size_t sent = _transport->write(_transmit.data() + _transmitCursor,
                                              remaining);
        if (sent == 0) {
            // No room now. The rest goes on a later poll FROM WHERE IT
            // STOPPED - never from the beginning, which would put the middle
            // of a packet on the wire twice and desynchronise permanently.
            _backpressured = true;
            return;
        }
        // Room again after there was none: the far end took delivery.
        if (_backpressured) _bytesRelieved += sent;
        _transmitCursor += sent;
        _bytesSent += sent;
    }
    _backpressured = false;
    _transmitCursor = 0;
    _transmitLength = 0;

    if (growing()) shrinkTransmitIfIdle();
}

// A buffer grown for one large packet goes back to its starting size once it
// is empty - except inside send(), whose caller still holds a pointer into it.
void MqttLink::shrinkTransmitIfIdle() {
#if MQTT_LINK_GROWS
    if (!growing() || _sending || _transmitLength != 0) return;
    if (_transmit.size() > _initialCapacity) {
        MQ_HEAP_BYTES(_initialCapacity).swap(_transmit);
        _transmitCursor = 0;
    }
#endif
}

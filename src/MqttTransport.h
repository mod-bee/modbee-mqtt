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
// THE LINK
// =============================================================================
//
// One ordered, reliable byte stream. No platform types, and no knowledge of
// what the bytes mean or which end sent them.
//
// Four methods are required - begin, end, write, isConnected - plus a read
// pair, of which there are two shapes and you implement either. Everything
// else below is defaulted and can be ignored.
//
// This is the whole of what the protocol needs from the outside world. TCP,
// TLS, a loopback in a test, a Unix socket, a WebSocket later - the core reads
// and writes bytes and never learns which.
//
// PARTIAL WRITES ARE EXPECTED HERE, and that is a real difference from a
// frame-oriented fieldbus. A 200 kB PUBLISH will not fit one write() into a
// socket, so the transmit path holds a cursor into the outbound packet and
// resumes on the next poll(). It never blocks to finish one.

#include "MqttPlatform.h"
#include "MqttFeatures.h"   // MQTT_LEAN: openFailed() below

class MqttTransport {
public:
    virtual ~MqttTransport() = default;

    // Open the medium. False if it cannot be used.
    //
    // For a network transport this starts the connection; it does not wait for
    // it. isConnected() below is what says whether it arrived - nothing in
    // this library blocks, this call included.
    virtual bool begin() = 0;

    // Close and release. Must tolerate repeat calls.
    virtual void end() = 0;

    // --- Reading: implement EITHER pair ---
    //
    // THE LIBRARY ONLY EVER CALLS THE BULK PAIR. The byte-wise pair below is
    // the older shape of this interface, and it still works, unchanged, with
    // no edit to a transport that was written against it.
    //
    // That compatibility is not politeness, it is the point of the type. A
    // transport is how somebody attaches this library to a medium nobody here
    // anticipated - a proprietary radio, an RS-485 gateway, a WebSocket, a
    // test double. Breaking that interface to tidy it up would be charging the
    // people using the library for a change that benefits the library.
    //
    // WHICH TO IMPLEMENT. The bulk pair, if the medium can give you more than
    // one byte at a time - which a socket always can, because it has already
    // read them into a buffer of its own. Implementing the byte-wise pair
    // instead means every byte crosses a virtual call, and the default
    // read() below has to loop; that is exactly what the four socket
    // transports in this library used to do to themselves.

    // Bytes that can be taken without blocking.
    virtual size_t readable() {
        const int legacy = available();
        return legacy > 0 ? (size_t)legacy : 0;
    }

    // Takes up to `length` bytes into `into` and returns how many it took,
    // which may be fewer than asked for and may be zero. Never blocks.
    virtual size_t read(uint8_t* into, size_t length) {
        if (into == nullptr) return 0;
        size_t taken = 0;
        while (taken < length) {
            const int byte = read();
            if (byte < 0) break;
            into[taken++] = (uint8_t)byte;
        }
        return taken;
    }

    // --- The byte-wise pair ---
    //
    // A transport that implements the bulk pair above never needs these, and
    // one that implements these never needs the bulk pair.
    //
    // ONE C++ WRINKLE, since it will bite somebody otherwise: a class that
    // overrides `int read()` hides `read(uint8_t*, size_t)` for calls made
    // directly on that class. It does not affect this library, which calls
    // through MqttTransport*, and `using MqttTransport::read;` restores it for
    // anyone who wants both names on their own type.
    virtual int available() { return 0; }
    virtual int read() { return -1; }

    virtual size_t write(const uint8_t* buffer, size_t length) = 0;

    // Whether the medium is usable now.
    virtual bool isConnected() const = 0;

    // Something the host can wait on, or -1 where the platform has no such
    // idea. A BSD socket returns its descriptor; an Arduino Stream returns -1.
    //
    // THIS IS NOT THE SOCKET ESCAPING THE ABSTRACTION. Nothing may read or
    // write through it - it exists so a host can ask the KERNEL which
    // connections have something to say, instead of asking every connection in
    // turn and being told "nothing" by almost all of them. Measured on this
    // broker at 500 clients: half of every recv() returned EAGAIN, and 54% of
    // all CPU was spent in the kernel finding that out.
    //
    // A board keeps the default and keeps polling, which is correct there: it
    // has a handful of connections and no epoll to ask.
    virtual int waitHandle() const { return -1; }

    // True when this transport is already holding bytes that have not been
    // read yet.
    //
    // THE TRAP A READINESS SET WALKS INTO. The kernel reports a socket as not
    // readable once its buffer has been drained into userspace - but the
    // bytes are not gone, they are in THIS object's buffer, or inside an SSL
    // object as decrypted plaintext with nothing left on the wire. A broker
    // that serviced only what the kernel called ready would leave those bytes
    // sitting there, and a connection would stall with a whole packet already
    // in the building.
    virtual bool hasBufferedInput() const { return false; }

    // Whether the peer has half-closed - sent FIN, and will send no more.
    //
    // This earns its place because it is NOT the same question as
    // isConnected(). A peer that has gone away accepts nothing while our own
    // socket is still perfectly writable, so code that cannot tell the two
    // apart keeps writing into a pipe nobody is reading and only finds out
    // when keepalive eventually fails - one keepalive interval later, which on
    // a plant link is a minute of a control loop talking to nothing.
    //
    // Defaulted to false, because a medium that cannot tell should say so
    // rather than guess.
    virtual bool peerClosed() const { return false; }

    // Whether an attempt begin() accepted has since FAILED - the name did not
    // resolve, or every address refused - as opposed to still being under way.
    //
    // isConnected() cannot say: false means both "not yet" and "never", and a
    // client that cannot tell them apart waits out its whole connect timeout
    // for an attempt that ended long before. Defaulted to false, so a medium
    // that cannot tell keeps that behaviour rather than guessing.
    virtual bool connectFailed() const { return false; }

#if MQTT_LEAN
    // A board only: whether the attempt begin() accepted never OPENED - what
    // begin() returning false has always meant, reported a poll later because
    // the open ran on a task of its own (MqttConnectTask). The client answers
    // it exactly as it answers a false begin(): the reconnect backoff, and
    // nothing outstanding concluded, since nothing was ever sent. Defaulted to
    // false, so a transport that opens inside begin() never says it.
    virtual bool openFailed() const { return false; }
#endif

    // Where the far end is, in whatever words the medium has - "10.0.4.7:51544"
    // for TCP, empty for a loopback in a test.
    //
    // Diagnostics and policy. An authorisation hook deciding whether a device
    // may connect usually wants to know where it is connecting from, and a log
    // line that cannot say which client dropped is a log line nobody can act
    // on. Defaulted to empty, so a medium that has no answer gives none and no
    // existing implementation has to change.
    virtual const char* peerName() const { return ""; }

    // A certificate the far end presented AND the engine verified against the
    // authority it was told to trust: the subject's Common Name, and SHA-256
    // over the certificate's DER as 64 lowercase hexadecimal characters. Both
    // empty when there was no such certificate - a plain medium, a TLS peer
    // that offered none, or one nobody asked.
    //
    // For the same hook peerName() serves, and one step stronger: where a
    // connection came from is a claim about the network, while a verified
    // certificate is a claim the authority signed. A certificate that was
    // offered and NOT verified never reaches here - the handshake failed and
    // there is no connection to ask. Defaulted to empty for the same reason
    // peerName() is.
    virtual const char* peerCertificateName() const { return ""; }
    virtual const char* peerCertificateFingerprint() const { return ""; }
};

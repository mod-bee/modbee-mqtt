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
// WHAT HAPPENED TO AN OPERATION
// =============================================================================
//
// The contract, and it is the whole of what makes the queue worth using:
//
//   EVERY ACCEPTED OPERATION REACHES EXACTLY ONE CONCLUSION.
//
// Not "usually", and not "unless the connection dropped". An operation that is
// accepted and then silently never concluded does not look like a bug. It
// looks like a control loop that quietly stops publishing after a few hours,
// on a plant where nothing else changed - and that is far harder to find than
// a crash.
//
// Grouped by WHERE THE OUTCOME CAME FROM, because that is what decides what a
// caller does next. A refusal that never reached the wire is a configuration
// problem; one the peer sent is a permissions or capacity problem; a
// connection fault is neither.
//
// One flat enum rather than a Result<T>: the value an operation fetches is
// delivered into the caller's own memory, so there is nothing for a result
// type to carry and no allocation to make carrying it.

#include "MqttTypes.h"

enum class MqttResult : uint8_t {
    // --- Accepted ---
    Queued = 0,          // Accepted; the outcome arrives on a later poll()
    Ok,                  // Completed, and completed successfully

    // --- Refused here; nothing reached the wire ---
    NotInitialised,      // begin() has not been called
    InvalidArgument,
    InvalidTopic,        // failed V5:4.7 validation - see MqttTopic.h
    NotConnected,        // and no session to queue into either
    QueueFull,           // past queueCapacity; never a silent drop
    Duplicate,           // the identical operation is already queued
    Superseded,          // replaced by a later write to the same topic
    Cancelled,           // the application asked, or the client was ended

    // The connection cannot express what was asked for. Two results rather
    // than one, because they send somebody to different places: the first
    // means 3.1.1 has no way to say it, the second means the peer said no.
    //
    // Named "Peer" rather than after a role: this file sits below L3 and does
    // not know which end it is, and a broker built on it would report the same
    // thing about its own peer.
    NotSupportedByProtocol,
    NotSupportedByPeer,

    PacketTooLarge,      // over the peer's stated Maximum Packet Size
    QuotaExceeded,       // our send quota is exhausted  V5:4.9
    NoPacketIdentifier,  // every identifier is in flight

    // --- Concluded by the peer ---
    //
    // A 5.0 acknowledgement carries a reason code, so these are what it said.
    // At 3.1.1 an acknowledgement carries no reason at all, so only Ok and the
    // connection-level results below can ever be reported there - which is
    // itself worth knowing when an operator asks why a publish "just worked"
    // on the old broker.
    Refused,                // a failure reason with no more specific member
    NoMatchingSubscribers,  // 0x10 - delivered nowhere, which may be fine
    TopicNameInvalid,       // 0x90
    NotAuthorized,          // 0x87
    PayloadFormatInvalid,   // 0x99
    QuotaExceededByPeer,    // 0x97
    PacketIdentifierInUse,  // 0x91

    // --- The connection, not the operation ---
    ConnectionLost,      // dropped while this was outstanding
    SessionLost,         // dropped, and the session did not survive it
    ConnectionRefused,   // CONNACK said no
    ProtocolError,       // the peer said something the standard does not allow
    MalformedPacket,     // the peer's bytes did not parse
    KeepAliveTimeout,    // no PINGRESP inside the interval

    // SENT, AND THE PEER NEVER ACKNOWLEDGED IT.
    //
    // Not a retransmission and not a resend: the operation is GIVEN UP ON.
    // [V5:MQTT-4.4.0-1] forbids resending outside a session resume and this
    // library holds to that - see docs/ARCHITECTURE.md. It says nothing about
    // how long a client must wait before deciding an operation has failed, and
    // waiting for ever is not a choice a PLC can live with.
    //
    // WHAT THIS MEANS FOR THE MESSAGE: at QoS 1 it may well have been
    // delivered, and this reports failure anyway. That is what "at least once"
    // entitles an application to see, and it is strictly better than the
    // alternative, which is the connection quietly ceasing to publish. See
    // MqttClientConfig::acknowledgementTimeoutMs for the whole argument.
    AckTimeout,

    SessionTakenOver,    // 0x8E - another connection took our identifier
};

// Pinned so a binding that casts rather than translates turns a drift into a
// build error. A generated IEC 61131 wrapper does exactly that.
#define MQ_RESULT_COUNT 30

const char* mq_resultName(MqttResult result);

// The operation was taken on and will conclude later, or already has.
inline bool mq_accepted(MqttResult result) {
    return result == MqttResult::Queued || result == MqttResult::Ok;
}

inline bool mq_succeeded(MqttResult result) {
    return result == MqttResult::Ok;
}

// Whether trying the same thing again could plausibly work.
//
// NoMatchingSubscribers is deliberately absent: the publish succeeded and
// nobody was listening, so repeating it changes nothing. So is NotAuthorized -
// retrying a permissions failure is how a client gets itself banned.
inline bool mq_worthRetrying(MqttResult result) {
    switch (result) {
        case MqttResult::QueueFull:
        case MqttResult::QuotaExceeded:
        case MqttResult::QuotaExceededByPeer:
        case MqttResult::NoPacketIdentifier:
        case MqttResult::NotConnected:
        case MqttResult::ConnectionLost:
        case MqttResult::SessionLost:
            return true;
        default:
            return false;
    }
}

// Maps a 5.0 reason code from an acknowledgement onto a result.
//
// Takes the packet type because the same numeric value means different things
// on different packets - 0x00 is Success on a PUBACK and Granted QoS 0 on a
// SUBACK - and because Table 2-6 is only meaningful per packet.
MqttResult mq_reasonToResult(uint8_t reason, uint8_t packetType);

// Maps a 3.1.1 Connect Return Code onto a result. A DIFFERENT value space from
// the reason codes above; see MqttTypes.h.
MqttResult mq_returnCodeToResult(uint8_t code);

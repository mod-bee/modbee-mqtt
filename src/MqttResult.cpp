// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttResult.h"
#include "MqttNoExcept.h"

const char* mq_resultName(MqttResult result) {
    switch (result) {
        case MqttResult::Queued:                 return "queued";
        case MqttResult::Ok:                     return "ok";
        case MqttResult::NotInitialised:         return "not initialised";
        case MqttResult::InvalidArgument:        return "invalid argument";
        case MqttResult::InvalidTopic:           return "invalid topic";
        case MqttResult::NotConnected:           return "not connected";
        case MqttResult::QueueFull:              return "queue full";
        case MqttResult::Duplicate:              return "duplicate";
        case MqttResult::Superseded:             return "superseded";
        case MqttResult::Cancelled:              return "cancelled";
        case MqttResult::NotSupportedByProtocol: return "not supported by this protocol version";
        case MqttResult::NotSupportedByPeer:     return "not supported by the peer";
        case MqttResult::PacketTooLarge:         return "packet too large for the peer";
        case MqttResult::QuotaExceeded:          return "send quota exhausted";
        case MqttResult::NoPacketIdentifier:     return "no packet identifier free";
        case MqttResult::Refused:                return "refused";
        case MqttResult::NoMatchingSubscribers:  return "no matching subscribers";
        case MqttResult::TopicNameInvalid:       return "topic name invalid";
        case MqttResult::NotAuthorized:          return "not authorized";
        case MqttResult::PayloadFormatInvalid:   return "payload format invalid";
        case MqttResult::QuotaExceededByPeer:    return "quota exceeded at the peer";
        case MqttResult::PacketIdentifierInUse:  return "packet identifier in use";
        case MqttResult::ConnectionLost:         return "connection lost";
        case MqttResult::SessionLost:            return "session lost";
        case MqttResult::ConnectionRefused:      return "connection refused";
        case MqttResult::ProtocolError:          return "protocol error";
        case MqttResult::MalformedPacket:        return "malformed packet";
        case MqttResult::KeepAliveTimeout:       return "keep alive timeout";
        case MqttResult::AckTimeout:
        return "the peer never acknowledged it";
    case MqttResult::SessionTakenOver:       return "session taken over";
    }
    return "unknown result";
}

// The enum and the count must not drift. A wrapper that casts an integer
// straight into this enum reads a drift as a valid-looking wrong answer, so it
// is caught here instead.
static_assert((int)MqttResult::SessionTakenOver + 1 == MQ_RESULT_COUNT,
              "MQ_RESULT_COUNT does not match the enum");

MqttResult mq_reasonToResult(uint8_t reason, uint8_t packetType) {
    // V5:2.4 - anything below 0x80 is success. On a SUBACK that includes the
    // granted QoS levels, which are outcomes rather than failures even when
    // the QoS granted is lower than the one asked for.
    if (!mq_reasonIsFailure(reason)) {
        if (packetType == MQ_PKT_PUBACK || packetType == MQ_PKT_PUBREC) {
            // 0x10 is a success: the peer took the message and had nowhere to
            // put it. Worth telling the application, because a publish that
            // reaches nobody is usually a commissioning error - and worth
            // keeping distinct from a failure, because retrying changes nothing.
            if (reason == MQ_REASON_NO_MATCHING_SUBSCRIBERS) {
                return MqttResult::NoMatchingSubscribers;
            }
        }
        return MqttResult::Ok;
    }

    switch (reason) {
        case MQ_REASON_NOT_AUTHORIZED:           return MqttResult::NotAuthorized;

        // A WRONG PASSWORD IS A CREDENTIAL FAULT AT BOTH VERSIONS. Before this
        // 0x86 fell through to the default and read as a bare `Refused` - the
        // same word as a broker that is shutting down - while the identical
        // fault at 3.1.1 (return code 4, below) read as NotAuthorized. So the
        // commonest commissioning error looked different depending on which
        // protocol happened to be negotiated, and at 5.0 a PLC could not tell
        // its panel it had the wrong password.
        case MQ_REASON_BAD_USER_NAME_OR_PASSWORD: return MqttResult::NotAuthorized;

        case MQ_REASON_TOPIC_NAME_INVALID:       return MqttResult::TopicNameInvalid;
        case MQ_REASON_TOPIC_FILTER_INVALID:     return MqttResult::InvalidTopic;
        case MQ_REASON_PAYLOAD_FORMAT_INVALID:   return MqttResult::PayloadFormatInvalid;
        case MQ_REASON_QUOTA_EXCEEDED:           return MqttResult::QuotaExceededByPeer;
        case MQ_REASON_PACKET_IDENTIFIER_IN_USE: return MqttResult::PacketIdentifierInUse;
        case MQ_REASON_PACKET_TOO_LARGE:         return MqttResult::PacketTooLarge;
        case MQ_REASON_SESSION_TAKEN_OVER:       return MqttResult::SessionTakenOver;
        case MQ_REASON_MALFORMED_PACKET:         return MqttResult::MalformedPacket;
        case MQ_REASON_PROTOCOL_ERROR:           return MqttResult::ProtocolError;
        case MQ_REASON_KEEP_ALIVE_TIMEOUT:       return MqttResult::KeepAliveTimeout;
        case MQ_REASON_RETAIN_NOT_SUPPORTED:
        case MQ_REASON_QOS_NOT_SUPPORTED:
        case MQ_REASON_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED:
        case MQ_REASON_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED:
        case MQ_REASON_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED:
            return MqttResult::NotSupportedByPeer;
        default:
            return MqttResult::Refused;
    }
}

MqttResult mq_returnCodeToResult(uint8_t code) {
    // V311 Table 3.1. Deliberately separate from the function above: 0x01 is
    // "unacceptable protocol version" here and "Granted QoS 1" at 5.0, so one
    // shared mapping would be wrong half the time.
    switch (code) {
        case MQ_RETURN_ACCEPTED:              return MqttResult::Ok;
        case MQ_RETURN_NOT_AUTHORIZED:        return MqttResult::NotAuthorized;
        case MQ_RETURN_BAD_CREDENTIALS:       return MqttResult::NotAuthorized;
        case MQ_RETURN_UNACCEPTABLE_PROTOCOL:
        case MQ_RETURN_IDENTIFIER_REJECTED:
        case MQ_RETURN_SERVER_UNAVAILABLE:
        default:                              return MqttResult::ConnectionRefused;
    }
}

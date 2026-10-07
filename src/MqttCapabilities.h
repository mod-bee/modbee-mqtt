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
// WHAT THIS CONNECTION CAN DO
// =============================================================================
//
// Resolved once, when the acknowledgement to a connect arrives, and asked by
// everything above L2 from then on.
//
// THIS IS WHY VERSION HANDLING DOES NOT LEAK UPWARD. Nothing above this file
// asks "are we on 5.0" - it asks "may I use a topic alias", "what is the
// highest QoS I may publish at", "is retain available". A future broker layer
// or a Sparkplug layer therefore works at both protocol versions without
// knowing either.
//
// On 5.0 these come from the CONNACK properties. On 3.1.1 they are filled in
// with 3.1.1's fixed answers, because 3.1.1 has no way to say anything else -
// no aliases, no subscription identifiers, no shared subscriptions, QoS 2, and
// retain available.

#include "MqttProperties.h"

struct MqttCapabilities {
    MqttVersion version = MqttVersion::V5;

    // V5:3.2.2.3.4. Absent means 2; a peer that supports QoS 2 omits the
    // property rather than sending the value 2.
    uint8_t maximumQos = MQ_QOS_2;

    // V5:3.2.2.3.5.
    bool retainAvailable = true;

    // V5:3.2.2.3.6. Zero means the peer stated no limit.
    uint32_t maximumPacketSize = 0;

    // V5:3.2.2.3.8. How many aliases WE may use when sending. Zero - the
    // common default - means none at all, which is the case a branch tends to
    // forget.
    uint16_t topicAliasMaximum = 0;

    // V5:3.2.2.3.11 to 3.2.2.3.13.
    bool wildcardSubscriptionAvailable = true;
    bool subscriptionIdentifiersAvailable = true;
    bool sharedSubscriptionAvailable = true;

    // V5:3.2.2.3.14. When present this OVERRIDES the keep alive we asked for,
    // and the value we asked for is no longer what either end is measuring.
    bool keepAliveOverridden = false;
    uint16_t keepAlive = 0;

    // V5:3.1.2.11.3 - how many QoS>0 publishes may be outstanding towards the
    // peer. Not a capability in the same sense, but it arrives in the same
    // packet and is used the same way. See MqttFlow.
    uint16_t receiveMaximum = 65535;

    // V5:3.2.2.3.7. Set when we sent a zero-length identifier and the peer
    // assigned one. An application that logs it can tell two anonymous nodes
    // apart in a broker's own logs.
    MqttSpan assignedClientIdentifier;

    // Whether the session the peer holds was already there. Decides whether
    // subscriptions must be re-issued on a reconnect, and getting it backwards
    // either loses every subscription or duplicates them all.
    bool sessionPresent = false;

    // --- Resolution ---

    // 3.1.1's fixed answers. Called for every 3.1.1 connection, because the
    // alternative - leaving the 5.0 defaults in place - would let a caller ask
    // for a topic alias on a connection that has never heard of one.
    void applyV311Defaults();

    // 5.0's, from the CONNACK properties.
    void applyV5(const MqttProperties& properties);

    // --- Questions the layers above ask ---

    bool allowsQos(uint8_t qos) const { return qos <= maximumQos; }
    bool allowsTopicAliases() const { return topicAliasMaximum > 0; }
    bool allowsProperties() const { return version == MqttVersion::V5; }

    // Whether a packet of this size may be sent. V5:3.1.2.11.4 - the peer's
    // Maximum Packet Size is a promise we must keep, and exceeding it earns a
    // DISCONNECT with 0x95 rather than a polite refusal.
    bool allowsPacketSize(size_t bytes) const {
        return maximumPacketSize == 0 || bytes <= maximumPacketSize;
    }
};

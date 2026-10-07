// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttCapabilities.h"

void MqttCapabilities::applyV311Defaults() {
    version = MqttVersion::V311;

    // Everything 5.0 added, stated as absent rather than left at its 5.0
    // default. A caller asking for a topic alias on a 3.1.1 connection is
    // refused here rather than discovering it at the codec.
    maximumQos = MQ_QOS_2;              // 3.1.1 has no way to limit it
    retainAvailable = true;             // nor to withdraw retain
    maximumPacketSize = 0;              // nor to state a limit
    topicAliasMaximum = 0;              // aliases arrived with 5.0
    wildcardSubscriptionAvailable = true;
    subscriptionIdentifiersAvailable = false;   // 5.0 only
    sharedSubscriptionAvailable = false;        // 5.0 only

    keepAliveOverridden = false;        // no Server Keep Alive property
    keepAlive = 0;

    // No Receive Maximum either, so flow control is ours alone to bound. The
    // value here is the protocol's ceiling; the client's own configuration is
    // what actually limits it.
    receiveMaximum = 65535;

    assignedClientIdentifier = MqttSpan();
}

void MqttCapabilities::applyV5(const MqttProperties& properties) {
    version = MqttVersion::V5;

    // Each of these has a default defined by its own section, and
    // MqttProperties already carries that default in the field. So an absent
    // property and a present one are read the same way - the presence flag
    // matters only where absent and zero mean different things.
    maximumQos = properties.maximumQos;
    retainAvailable = properties.retainAvailable != 0;
    maximumPacketSize = properties.has(MQ_PROP_MAXIMUM_PACKET_SIZE)
                            ? properties.maximumPacketSize : 0;
    topicAliasMaximum = properties.topicAliasMaximum;
    wildcardSubscriptionAvailable = properties.wildcardSubscriptionAvailable != 0;
    subscriptionIdentifiersAvailable = properties.subscriptionIdentifiersAvailable != 0;
    sharedSubscriptionAvailable = properties.sharedSubscriptionAvailable != 0;

    // V5:3.2.2.3.14 - "If the Server sends a Server Keep Alive on the CONNACK
    // packet, the Client MUST use this value instead of the Keep Alive value
    // the Client sent on CONNECT."
    keepAliveOverridden = properties.has(MQ_PROP_SERVER_KEEP_ALIVE);
    keepAlive = properties.serverKeepAlive;

    receiveMaximum = properties.receiveMaximum;

    assignedClientIdentifier = properties.assignedClientIdentifier;
}

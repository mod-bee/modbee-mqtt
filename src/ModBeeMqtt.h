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
// ModBee MQTT
// =============================================================================
//
// MQTT 3.1.1 and 5.0, for Arduino and hosts.
//
// THE ONE RULE: NOTHING BLOCKS. No call in this library waits for the network,
// connect() included. One object, one poll() in your loop.
//
// START HERE:
//
//   MqttDevice          one object, sensible defaults, and the fiddly parts
//                       done for you. Most sketches need nothing else.
//
// The full form, which MqttDevice is built on and never hides:
//
//   MqttClient          the whole client: connect, publish, subscribe
//   MqttConnectionConfig  what you send, and the peer may revise
//   MqttClientConfig      what is yours alone
//
// Underneath, if you need it - and all of it is public:
//
//   MqttCapabilities    what THIS connection can actually do
//   MqttSession         what survives a reconnect
//   MqttOperations      the queue, and its one-conclusion contract
//   MqttTopic           name and filter validation, and matching
//   MqttCodecV5 / V311  the two wire formats
//   MqttWire            the five data representations
//
// Written from the OASIS standards. Every constant, length and reason code
// cites the statement that requires it, namespaced by version: V5:MQTT-x.y-n
// or V311:MQTT-x.y-n, because 119 statement identifiers appear in both
// documents meaning different things.

#include "MqttFeatures.h"
#include "MqttPlatform.h"
#include "MqttVersion.h"
#include "MqttTypes.h"
#include "MqttResult.h"
#include "MqttWire.h"
#include "MqttProperties.h"
#include "MqttOwnedProperties.h"
#include "MqttTopic.h"
#include "MqttPacket.h"
#include "MqttCodec.h"
#include "codec/MqttCodecV5.h"
#include "codec/MqttCodecV311.h"
#include "MqttCapabilities.h"
#include "MqttFlow.h"
#include "MqttSession.h"
#include "MqttAliases.h"
#include "MqttKeepAlive.h"
#include "MqttOperations.h"
#include "MqttTopicRegistry.h"
#include "MqttTransport.h"
#include "MqttConfig.h"
#include "MqttClient.h"
#include "MqttDevice.h"

// Transports and listeners. Each compiles to nothing on the platforms it does
// not serve, and the TLS ones to nothing at all where no engine is installed -
// wolfSSL or OpenSSL is DETECTED, never a dependency this library imposes on
// anyone who does not want it. See MqttFeatures.h for which is chosen and why.
#include "MqttListener.h"
#include "transport/MqttTcpTransport.h"
#include "transport/MqttTcpListener.h"
#include "transport/MqttTlsTransport.h"
#include "transport/MqttTlsListener.h"
#include "transport/MqttStreamListener.h"      // a board serving plain MQTT
#include "transport/MqttStreamTlsListener.h"   // a board serving TLS
#include "transport/MqttStreamTransport.h"

// An identity of the device's own: an EC key, a self-signed certificate and
// the fingerprint to pin it by. Unlike the two above it needs no sockets, so
// it is available on a board as well - which is the point, because a
// controller that can mint its own certificate needs nothing issued to it.
#include "transport/MqttIdentity.h"

// --- The other roles ---
//
// NOT OPTIONAL, AND NOT CONDITIONAL. This paragraph used to say they were
// "off unless asked for" and that they were "included conditionally here" -
// neither of which had been true since the MQTT_WITH_* flags were deleted.
// The blank lines below are where the #if blocks used to be. A comment that
// describes a build system the project no longer has is worse than none: it
// sends the next reader looking for a switch to turn something on.
//
// Everything is compiled, every time. `#include <ModBeeMqtt.h>` is the whole
// of the include story, and docs/footprint.md says what the library costs in
// bytes from `size`. A linker drops what a program does not call.

#include "broker/MqttBroker.h"
#include "broker/MqttBrokerAccounts.h"

#include "bridge/MqttBridge.h"

#include "typed/MqttValue.h"
#include "typed/MqttTypedBindings.h"
#include "typed/MqttPoller.h"

// Sparkplug B 3.0: an edge node and a host application, with the payload codec
// and topic namespace under them. Compiled always, like everything above, and
// linked only by a program that constructs one - docs/sparkplug.md.
#include "sparkplug/MqttSparkplugTypes.h"
#include "sparkplug/MqttSparkplugValue.h"
#include "sparkplug/MqttSparkplugPayload.h"
#include "sparkplug/MqttSparkplugTopic.h"
#include "sparkplug/MqttSparkplugNode.h"
#include "sparkplug/MqttSparkplugHost.h"

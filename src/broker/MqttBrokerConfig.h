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
// WHAT THIS BROKER WILL AND WILL NOT DO
// =============================================================================
//
// Every limit here becomes something the broker TELLS the client in CONNACK
// and then ENFORCES. That pairing is the whole point: a broker that advertises
// a Maximum Packet Size and then accepts a larger one has taught its clients
// a lie, and a broker that advertises nothing leaves every client guessing -
// which is how NanoMQ ends up with an undiscoverable 10 kB ceiling.
//
// The defaults are sized for a plant gateway, not for a cloud cluster: a few
// dozen devices, small messages, a modest amount of RAM. Everything is
// allocated once at begin() and never grows, so the memory a broker will use
// is knowable before it starts rather than discovered on a busy afternoon.

#include "../MqttFeatures.h"


#include "../MqttTypes.h"
#include "../MqttPlatform.h"

#include <string>
#include <vector>

struct MqttBrokerConfig {
    // --- Capacity ---

    // Connections served at once. The listener's pool must be at least this.
    size_t maxConnections = 32;

    // Room for more, on a host: how far setMaxConnections() can raise
    // maxConnections while the broker runs (never past maxSessions). The
    // address space is kept at begin() so the slots never move, but only
    // maxConnections of them are built; the rest cost nothing until the limit
    // is raised. 0: maxConnections, and no more. A board ignores it.
    size_t connectionCapacity = 0;

    // Sessions held, including those of clients currently disconnected whose
    // state has not expired. Larger than maxConnections on purpose: offline
    // sessions are the normal state of a plant at three in the morning.
    size_t maxSessions = 64;

    size_t maxSubscriptionsPerSession = 32;

    // Messages held for one client. When this fills, the oldest are NOT
    // dropped silently - the drop is counted, and at QoS > 0 the publisher is
    // told with 0x97 rather than being allowed to believe it was delivered.
    size_t maxQueuedPerSession = 64;

    // And in BYTES: each queued message is its own copy of topic and payload,
    // so the depth alone lets a few thousand sessions - offline ones fill as
    // well - hold gigabytes. A message that would take one session past
    // maxQueuedBytesPerSession, or all of them past maxQueuedBytesTotal, is
    // refused for that subscriber and counted, as a full queue is. 0 = no cap.
    size_t maxQueuedBytesPerSession = 0;
    size_t maxQueuedBytesTotal = 0;

    // ONE LOGIN, ONE SHARE. Sessions any one login may hold (a CONNECT that
    // would make one more is refused with 0x97 Quota exceeded), and retained
    // topics it may hold (a new one past it is refused with 0x97). 0 = no
    // limit. The authorise hook may set either per login at CONNECT
    // (MqttBrokerRequest::sessionLimit, ::retainedLimit) - an admin's tools
    // need more than a device. Counted per broker: on a server of several
    // shards, per shard.
    uint32_t maxSessionsPerLogin = 0;
    uint32_t maxRetainedPerLogin = 0;
    // Logins neither limit applies to - an admin's tools, a registration
    // login every new device shares. Names, compared exactly.
    std::vector<std::string> unlimitedLogins;

    size_t maxRetained = 256;

    size_t receiveBufferSize = 8192;
    size_t transmitBufferSize = 8192;

    // RESERVE EVERY SLOT AT begin(), or only when a session first needs it.
    //
    // True - the default, and what every board gets - keeps the rule this file
    // opens with: everything allocated once, at begin(), so the memory a
    // broker will use is known before it starts and nothing is allocated on a
    // busy afternoon.
    //
    // False is for a broker holding a hundred thousand connections, most of
    // which are devices that publish and never receive. With every slot
    // reserved up front, each connection cost about 43 kB whether it used any
    // of it or not. With this off:
    //   - a session reserves its subscription slots on its first SUBSCRIBE,
    //     and its delivery slots on its first queued message - all of them at
    //     once, so they never move afterwards;
    //   - a connection's receive and transmit buffers start small and grow to
    //     the sizes above only when a packet needs them, and shrink back when
    //     the large packet has gone.
    // Every LIMIT is unchanged and enforced exactly as before, so the worst
    // case is still knowable before start; what changes is that an idle
    // connection no longer pays for the worst case.
    bool reserveUpFront = true;

    // --- What CONNACK advertises, and what is then enforced ---

    // [V5:3.2.2.3.3] How many QoS > 0 PUBLISHes a client may have in flight
    // towards us. Enforced with 0x93 Receive Maximum Exceeded.
    uint16_t receiveMaximum = 32;

    // [V5:3.2.2.3.6] The largest packet we will accept. Enforced with 0x95.
    // Must not exceed receiveBufferSize, and is clamped to it at begin()
    // rather than trusted - advertising more than we can hold is the D-1
    // failure with the numbers the other way round.
    uint32_t maximumPacketSize = 8192;

    // [V5:3.2.2.3.4] Maximum QoS. A PUBLISH above it earns 0x9B, and SUBACK
    // never grants above it.
    uint8_t maximumQos = MQ_QOS_2;

    // [V5:3.2.2.3.5] Retain Available. When false, a retained PUBLISH earns
    // 0x9A Retain Not Supported.
    bool retainAvailable = true;

    // [V5:3.2.2.3.8] How many inbound topic aliases we will accept. Zero
    // refuses them all, which is legal and is what most brokers do.
    uint16_t topicAliasMaximum = 8;

    // Whether WE use aliases on the messages we send, bounded by the client's
    // own Topic Alias Maximum. Off by default: it saves bytes and it is one
    // more thing to get wrong in a client that rarely exercises it, so a plant
    // should turn it on deliberately.
    bool useOutboundTopicAliases = false;

    // [V5:3.2.2.3.11] Wildcard Subscription Available - 0xA2 when refused.
    bool wildcardSubscriptionAvailable = true;

    // [V5:3.2.2.3.12] Subscription Identifiers Available - 0xA1 when refused.
    bool subscriptionIdentifiersAvailable = true;

    // [V5:3.2.2.3.13] Shared Subscription Available - 0x9E when refused.
    // False unless the delivery code is compiled in, because advertising a
    // feature that is not built is worse than not having it.
    bool sharedSubscriptionAvailable = true;

    // [V5:3.2.2.3.14] Server Keep Alive, which is a CEILING rather than an
    // override.
    //
    // The standard says what it is for: "The Server uses this value to inform
    // the Client that it will disconnect the Client for inactivity sooner than
    // the Keep Alive specified by the Client." SOONER. So a client asking for
    // 5 seconds keeps 5 seconds, and a client asking for 120 - or for none at
    // all - is brought down to this.
    //
    // The first version of this replaced the client's value unconditionally,
    // which is legal and is also how a broker ends up holding a wedged
    // device's connection open for a minute when the device asked to be cut
    // off after seven seconds. Paho's suite found it, by requiring both
    // behaviours in two different tests.
    //
    // Zero means no ceiling.
    uint16_t serverKeepAliveMaximum = 0;

    // The keep alive applied when a client asks for none and we state none.
    // Zero means a connection may idle for ever, which is legal and is also
    // how a broker accumulates dead sockets nobody notices.
    uint16_t defaultKeepAlive = 0;

    // The longest Session Expiry Interval we will honour. A client asking for
    // more gets this, stated back in CONNACK [V5:3.2.2.3.2]. The default is
    // an hour: long enough for a device to reboot, short enough that a
    // decommissioned one does not hold a queue for ever.
    uint32_t maximumSessionExpiry = 3600;

    // How long a connection may take to send its CONNECT. [V5:3.1] allows the
    // Server to close a Network Connection on which no CONNECT has arrived
    // "within a reasonable amount of time". This is that time, and it is the
    // only defence against a port scanner holding every slot.
    mq_time_t connectTimeoutMs = 10000;

    // How long a connection is held open AFTER we have sent a DISCONNECT.
    //
    // NOT politeness. A DISCONNECT is the only way the client learns WHY it
    // was disconnected, and closing the socket the instant after writing one
    // throws it away: TCP resets a connection that is closed while the peer is
    // still writing, and a reset discards whatever was in flight. The client
    // then sees a connection reset, never learns it exceeded Receive Maximum,
    // and reports a broken broker.
    //
    // So the write side is shut down, the socket is kept, whatever the peer
    // still sends is read and discarded, and the socket closes when the peer
    // closes or this elapses. A quarter of a second is far longer than a round
    // trip on any plant network and short enough that a peer which never
    // closes cannot hold a slot.
    mq_time_t closeLingerMs = 250;

    // The prefix of the Client Identifiers this broker assigns [V5:3.1.3.1],
    // followed by eight hex digits. A server running one broker per core gives
    // each its own ("auto-s3-"), so an assigned identifier says which broker
    // holds its session. The string must outlive the broker.
    const char* assignedIdPrefix = "auto-";

    // --- Policy ---

    // Whether a client with no user name may connect. True is the right
    // default for a plant network that is already isolated, and the wrong one
    // for anything reachable from outside - which is why it is here rather
    // than assumed.
    bool allowAnonymous = true;

    // Whether a zero-length Client Identifier gets one assigned
    // [V5:3.1.3.1]. Turning it off refuses those connections with 0x85.
    bool assignClientIdentifiers = true;
};


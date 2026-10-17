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
// CONFIGURATION
// =============================================================================
//
// TWO STRUCTS, AND THE SPLIT IS NOT COSMETIC.
//
// One holds what is SENT in a connect and may be overridden by the answer: the
// keep alive the peer can replace, the session expiry it can shorten, the
// receive maximum it can undercut. Reading one of those fields back after
// connecting tells you what you ASKED FOR, not what is in force - what is in
// force is in MqttCapabilities.
//
// The other holds what is ours alone and nobody can override: how much we will
// queue, how we back off, how big our buffers are.
//
// A single struct would make those indistinguishable, and an application that
// read `keepAlive` back and believed it would ping at the wrong rate for as
// long as it ran.

#include "MqttVersion.h"
#include "MqttTypes.h"
#include "MqttOperations.h"
#include "MqttOwnedProperties.h"

#include <string.h>

#include <string>
#include <vector>

// The text fields on a board (MQTT_LEAN, MqttFixed.h): fixed, at least as long
// as the PLC blocks' pins (BROKER and topics STRING(128), USER and PASSWORD
// STRING(64); a Sparkplug client id is group:node, up to 97).
#if MQTT_LEAN
    #ifndef MQTT_CONFIG_HOST
    #define MQTT_CONFIG_HOST 128
    #endif
    #ifndef MQTT_CONFIG_CLIENT_ID
    #define MQTT_CONFIG_CLIENT_ID 128
    #endif
    #ifndef MQTT_CONFIG_USER
    #define MQTT_CONFIG_USER 64
    #endif
    #ifndef MQTT_CONFIG_PASSWORD
    #define MQTT_CONFIG_PASSWORD 64
    #endif
    #ifndef MQTT_CONFIG_WILL_TOPIC
    #define MQTT_CONFIG_WILL_TOPIC 128
    #endif
    #ifndef MQTT_CONFIG_WILL_PAYLOAD
    #define MQTT_CONFIG_WILL_PAYLOAD 128
    #endif
#endif

// A Will, as the application states it.
//
// The payload is BYTES, not text. A Sparkplug death certificate is protobuf,
// and a Will forced through a string type would be corrupted by the first byte
// that is not valid UTF-8.
struct MqttWillConfig {
    bool enabled = false;
    MQ_STRING(MQTT_CONFIG_WILL_TOPIC) topic;
    MQ_BYTES(MQTT_CONFIG_WILL_PAYLOAD) payload;
    uint8_t qos = MQ_QOS_0;
    bool retain = false;

    // 5.0 only. How long the peer waits after the connection ends before
    // publishing the Will. V5:3.1.3.2.2 - a reconnect inside this window means
    // the Will is never sent at all, which is what stops a flapping link
    // filling a plant with false offline alarms.
    //
    // Refused rather than dropped on a 3.1.1 connection: there, a Will is
    // published the moment the connection drops and there is no way to ask for
    // anything else. connect() returns NotSupportedByProtocol, and a fallback
    // that would land on 3.1.1 is refused for the same reason - silently
    // losing the delay would fill a plant with the false offline alarms the
    // delay existed to prevent, and nobody would know why.
    uint32_t delayInterval = 0;

    // 5.0 only, and refused at 3.1.1 exactly as the delay is. [V5:3.1.3.2]
    // permits a Will to carry Payload Format, Message Expiry, Content Type,
    // Response Topic, Correlation Data and User Properties - which is what
    // lets a death certificate say what it is rather than just that it
    // happened.
    MqttOwnedProperties properties;

    // Whether this Will needs 5.0 to be expressed as asked for.
    bool needsV5() const { return delayInterval != 0 || !properties.empty(); }

    void set(const char* willTopic, const uint8_t* bytes, size_t length,
             uint8_t willQos, bool willRetain) {
        enabled = true;
        topic = willTopic ? willTopic : "";
        payload.assign(bytes, bytes + length);
        qos = willQos;
        retain = willRetain;
    }

    void set(const char* willTopic, const char* text,
             uint8_t willQos, bool willRetain) {
        const size_t length = text ? strlen(text) : 0;
        set(willTopic, (const uint8_t*)text, length, willQos, willRetain);
    }
};

// --- What we send, and the peer may revise ---
struct MqttConnectionConfig {
    MQ_STRING(MQTT_CONFIG_HOST) host;
    uint16_t port = MQ_PORT_TCP;

    // V5:3.1.3.1 - may be empty, in which case a 5.0 peer assigns one and
    // returns it as the Assigned Client Identifier. A 3.1.1 peer may refuse.
    MQ_STRING(MQTT_CONFIG_CLIENT_ID) clientId;

    MQ_STRING(MQTT_CONFIG_USER) userName;
    MQ_BYTES(MQTT_CONFIG_PASSWORD) password;   // Binary Data at both versions
    bool hasUserName = false;
    bool hasPassword = false;

    // The version we ASK FOR. Explicit, because a plant engineer should be
    // able to read the config and know what is on the wire.
    MqttVersion version = MqttVersion::V5;

    // Opt-in, and off by default. When set, a rejection that specifically
    // means "this version is not supported" is retried at this version -
    // NEVER an authentication failure, a TLS failure or a network error.
    // Treating a network fault as a version problem hides the network fault.
    bool fallbackEnabled = false;
    MqttVersion fallbackVersion = MqttVersion::V311;

    // V5 calls it Clean Start and pairs it with Session Expiry Interval;
    // 3.1.1 calls it Clean Session and it alone decides whether state is kept.
    bool cleanStart = true;

    // Seconds. Zero disables the mechanism entirely at both versions.
    // A 5.0 peer may replace this - see MqttCapabilities::keepAlive.
    uint16_t keepAlive = 60;

    // 5.0 only. Seconds the peer keeps the session after the connection ends.
    // 0xFFFFFFFF means it does not expire.
    uint32_t sessionExpiryInterval = 0;
    bool hasSessionExpiry = false;

    // 5.0 only. How many QoS>0 publishes we will accept at once, and how many
    // aliases we will accept. Both are what WE offer; what we may use is in
    // MqttCapabilities, from the peer's answer.
    uint16_t receiveMaximum = 0;         // 0 leaves the default of 65535
    uint16_t topicAliasMaximum = 0;
    uint32_t maximumPacketSize = 0;      // 0 states no limit

    MqttWillConfig will;

    // A registration session: logging in with a provisioning key to ask for a
    // login of its own. It sends no Will - whatever a layer (Sparkplug's NDEATH,
    // a presence message) has put on the connection - because such a session may
    // publish nothing but its registration request, and a broker refuses a Will
    // it may not publish. The Will stays configured for the device's own login.
    bool registration = false;
};

// --- What is ours alone ---
struct MqttClientConfig {
    // Operations held before a publish is refused with QueueFull. Never a
    // silent drop. Any number: the queue is sized to it at begin(), and only
    // memory refuses it (MqttClient::queueOutOfMemory()).
    size_t queueCapacity = MQTT_QUEUE_CAPACITY;

    // The largest packet we will assemble on receive, and the buffer we encode
    // into. A packet larger than this cannot be received at all, so it is also
    // what should be offered as our Maximum Packet Size.
    size_t receiveBufferSize = 4096;
    size_t transmitBufferSize = 4096;

    // Reconnect backoff, in milliseconds. Doubling from the first to the
    // second, with deterministic jitter so two nodes restarting together do
    // not stay in step - a plant where forty PLCs lose power at once should
    // not present the broker with forty simultaneous connects, for ever.
    mq_time_t reconnectInitialMs = 1000;
    mq_time_t reconnectMaximumMs = 60000;

    // Whether to reconnect at all. Off means a lost connection is reported and
    // left; the application decides.
    bool autoReconnect = true;

    // WHETHER TO RETRY A CREDENTIAL THE BROKER HAS ALREADY REJECTED.
    //
    // Off, and this is the one reconnect decision that is not about the
    // network. A dropped link is worth retrying because the link may come
    // back; a password the broker said no to will not become correct by
    // itself, and sending it again every second is how a device gets itself
    // banned - some brokers ban by address, which takes the whole site with
    // it, and a cloud broker's rate limiter is not gentle about it.
    //
    // This library already refuses to retry a rejected credential at another
    // PROTOCOL VERSION, for the same reason and with a comment saying so. The
    // reconnect path did not, so with autoReconnect on - which is the default
    // - a wrong password meant a PLC hammering the broker for ever with the
    // same wrong password. Found by pointing the interoperability rig at a
    // broker that actually checks credentials, which it had never done.
    //
    // Turn it on where the credential is expected to become valid without the
    // application restarting: a broker whose access list is loaded after it
    // starts listening, or a token refreshed by something else in the same
    // program. Then it is a decision somebody made rather than a default
    // nobody noticed.
    //
    // Applies to 0x86 Bad User Name or Password and 0x87 Not Authorized, and
    // to 3.1.1's return codes 4 and 5. Every other refusal, and every network
    // fault, reconnects as before.
    bool retryRejectedCredentials = false;

    // ...BUT NEVER FOR GOOD. With retryRejectedCredentials off, a rejected
    // credential is tried again after THIS long, instead of at the network
    // backoff - and instead of never, which is what it used to be.
    //
    // Standing down for ever is the one reconnect decision a remote
    // installation cannot survive. The broker may have been restored from an
    // old password file, its authentication backend may have been down for a
    // minute, an account may have been disabled by mistake and re-enabled -
    // and every one of those becomes a site visit if the controller has given
    // up. Five minutes is twelve attempts an hour: nothing any broker's rate
    // limiter or ban list notices, and a site that is fixed at the broker end
    // comes back by itself within five minutes.
    //
    // ZERO STANDS THE CLIENT DOWN FOR GOOD, as before - a decision for an
    // application with an operator standing next to it, not a default.
    mq_time_t rejectedCredentialsRetryMs = 300000;

    // HOW LONG TO WAIT FOR A QoS>0 ACKNOWLEDGEMENT BEFORE GIVING UP ON IT.
    //
    // NOT A RETRANSMISSION TIMER. Nothing is ever resent:
    // [V5:MQTT-4.4.0-1] says resending is required in exactly one
    // circumstance - a session resumed with Clean Start 0 - and "Clients and
    // Servers MUST NOT resend messages at any other time". This library holds
    // to that, and docs/ARCHITECTURE.md argues it. What this does is CONCLUDE
    // the operation, which the standard does not speak to: the handler is
    // told AckTimeout, the queue slot is freed, and the in-flight quota it
    // held is released.
    //
    // WHY IT EXISTS. Without it, a broker that accepts a PUBLISH and never
    // acknowledges it - conforming behaviour, since MQTT sets no deadline on
    // a server - leaves the operation outstanding for ever. Each one holds a
    // unit of the peer's Receive Maximum, so after receiveMaximum of them the
    // client cannot publish at QoS>0 again. Silently: the link is up,
    // keepalive is answered, and nothing in any status field says why the
    // setpoints stopped arriving.
    //
    // Keepalive does not catch it. Keepalive proves a peer's socket and event
    // loop are alive; it proves nothing about whether acknowledgements are
    // flowing, and a broker can answer every PINGREQ while acking nothing.
    // That is what happened on a plant: PENDING stuck at 6 for five hours,
    // PUBLISHED still climbing because QoS 0 was unaffected.
    //
    // 60 SECONDS, and ON BY DEFAULT. An acknowledgement on a working link
    // arrives in milliseconds; a minute is long enough that a congested
    // cellular link is not cut off mid-transfer, and short enough that a PLC
    // recovers within one operator's attention span rather than never.
    //
    // ZERO RESTORES THE OLD BEHAVIOUR - wait for ever - for anyone who would
    // rather stall than see a delivered message reported as failed. That is a
    // real preference and it is a decision, not a default.
    //
    // THE PACKET IDENTIFIER IS NOT REUSED afterwards. It stays reserved for
    // the life of the connection, because the peer may still acknowledge the
    // original and a reused identifier would attribute that answer to a
    // different message.
    mq_time_t acknowledgementTimeoutMs = 60000;

    // How long to wait for the acknowledgement of a connect before giving up
    // and starting again. NOT a retransmission timer - see MqttOperations.h.
    // This one exists because a peer that accepts a TCP connection and then
    // says nothing is otherwise indistinguishable from a healthy one.
    mq_time_t connectTimeoutMs = 20000;
};

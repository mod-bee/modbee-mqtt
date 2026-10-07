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
// THE BRIDGE
// =============================================================================
//
// Two clients and a mapping. Nothing more, and deliberately nothing more: a
// bridge that was its own protocol implementation would be a third place for
// the same bugs to live.
//
// What it is for: a panel that must keep working when the site link is down.
// The local broker runs in the panel, every device on the line talks to it,
// and the bridge carries the handful of topics head office needs up to the
// site broker - reconnecting on its own, and losing nothing in between if the
// session is configured to survive.
//
// -----------------------------------------------------------------------------
// LOOP PREVENTION, WHICH IS THE WHOLE PROBLEM
// -----------------------------------------------------------------------------
//
// A bridge publishes what it receives. If what it publishes comes back, it
// publishes it again, and the two brokers spend the rest of the afternoon
// passing one message between them as fast as the link allows. This is not a
// theoretical failure; it is the normal outcome of a bridge built without
// thinking about it, and it takes a plant network down.
//
// There are exactly two honest answers, and which one is available depends on
// the protocol version:
//
//   5.0   No Local [V5:3.8.3.1]. "Application Messages MUST NOT be forwarded
//         to a connection with a ClientID equal to the ClientID of the
//         publishing connection." The bridge subscribes with No Local set, so
//         what the bridge published is never handed back to the bridge. This
//         is EXACT. Nothing is guessed and nothing is dropped.
//
//   3.1.1 There is no No Local, and there is no other mechanism in the
//         protocol. So the only way to be sure is that WHAT THE BRIDGE
//         PUBLISHES ON A SIDE IS NOT MATCHED BY WHAT THE BRIDGE SUBSCRIBES TO
//         ON THAT SIDE. The bridge works that out for itself with
//         mq_filtersOverlap and refuses the configuration if it cannot be sure.
//
//         A prefix alone does NOT fix this, which is worth stating plainly
//         because it is the obvious first answer and it is wrong: a
//         bidirectional rule mapping "panel/" to "site/" publishes into
//         "site/..." on the remote and subscribes to "site/..." on the remote,
//         so the message comes straight back. This was caught by the loop test
//         on its first run, having been written into the first version of this
//         file as if it worked.
//
//         What does work is two rules with filters that cannot match each
//         other's output - readings up under one filter, commands down under
//         another. That is how a bridge should be written anyway, because it
//         says out loud what is allowed to travel in each direction.
//
// Refusing is the point. The alternative - remembering what was just published
// and dropping anything that looks like it - is a heuristic, and a heuristic
// here discards real messages: two identical readings a second apart are
// completely ordinary in a plant, and a bridge that silently ate the second
// one would be a fault nobody could ever reproduce.

#include "../MqttFeatures.h"


#include "../MqttClient.h"

#include <string>
#include <vector>

enum class MqttBridgeDirection : uint8_t {
    // Local to remote. The usual case: a panel sending readings upstream.
    Out = 0,
    // Remote to local. Commands and setpoints coming down.
    In,
    Both,
};

const char* mq_bridgeDirectionName(MqttBridgeDirection direction);

// One rule.
//
// `filter` is the topic filter as it appears WITHOUT either prefix. The
// prefixes are what each side actually sees, so a rule with `filter` of
// "plant/#", a local prefix of "" and a remote prefix of "site7/" subscribes
// locally to "plant/#" and republishes "plant/a/temp" as "site7/plant/a/temp".
struct MqttBridgeTopic {
    std::string filter;
    MqttBridgeDirection direction = MqttBridgeDirection::Out;

    // The most this rule will carry. A message published at QoS 2 through a
    // rule of QoS 1 crosses at QoS 1 - never higher than the rule allows, and
    // never higher than it arrived.
    uint8_t qos = MQ_QOS_0;

    std::string localPrefix;
    std::string remotePrefix;

    // Whether a retained message stays retained on the far side. Off by
    // default: a retained message crossing a bridge outlives the link that
    // carried it, and a stale retained value on a head-office dashboard is
    // worse than no value at all.
    bool forwardRetain = false;
};

struct MqttBridgeConfig {
    // For diagnostics and for the client identifiers, which MUST be different
    // on the two sides: [V5:MQTT-3.1.4-3] means a bridge using one identifier
    // for both ends would take over its own session the moment both brokers
    // were the same box.
    std::string name = "bridge";

    MqttConnectionConfig local;
    MqttConnectionConfig remote;
    MqttClientConfig behaviour;

    std::vector<MqttBridgeTopic> topics;

    // Whether to set No Local on the bridge's own subscriptions at 5.0. On,
    // and there is no good reason to turn it off - it is here so that a test
    // can prove the protection is what stops the loop rather than luck.
    bool useNoLocal = true;
};

enum class MqttBridgeState : uint8_t {
    Idle = 0,
    Connecting,     // one or both ends are still coming up
    Up,             // both ends connected and subscribed
    Degraded,       // one end is down; the other still works
};

const char* mq_bridgeStateName(MqttBridgeState state);

struct MqttBridgeStatus {
    MqttBridgeState state = MqttBridgeState::Idle;

    // Running totals, never flags - the same rule as everywhere else, so two
    // readers cannot take each other's edges.
    uint32_t forwardedOut = 0;
    uint32_t forwardedIn = 0;
    uint32_t refusedOut = 0;      // the far side would not take it
    uint32_t refusedIn = 0;
    uint32_t localConnects = 0;
    uint32_t remoteConnects = 0;

    bool localUp = false;
    bool remoteUp = false;
};

class MqttBridge {
public:
    MqttBridge() = default;

    MqttBridge(const MqttBridge&) = delete;
    MqttBridge& operator=(const MqttBridge&) = delete;

    // Both transports are the caller's and must outlive this object.
    //
    // Returns the fault when the configuration cannot be made safe - which is
    // a real answer rather than a formality. See the loop-prevention note at
    // the top: a pair of rules that would carry the bridge's own output back
    // to it, on a version with no No Local, is refused with
    // MqttResult::InvalidArgument and an explanation naming the two filters.
    MqttResult begin(MqttTransport& localTransport, MqttTransport& remoteTransport,
                     const MqttBridgeConfig& config);

    // Connects both ends. Neither call waits.
    MqttResult start();

    // Disconnects both ends cleanly.
    void stop();

    // The whole contract. Polls both clients and moves whatever arrived.
    void poll();

    const MqttBridgeStatus& status() const { return _status; }
    MqttBridgeState state() const { return _status.state; }
    bool isUp() const { return _status.state == MqttBridgeState::Up; }

    // The two clients, public because a bridge that hid them would force an
    // application to choose between the bridge and everything else the client
    // can do.
    MqttClient& localClient() { return _local; }
    MqttClient& remoteClient() { return _remote; }
    const MqttClient& localClient() const { return _local; }
    const MqttClient& remoteClient() const { return _remote; }

    // Why begin() refused, in words an engineer can act on.
    const std::string& lastError() const { return _lastError; }

private:
    MqttBridgeConfig _config;
    // Scratch for the rewritten topic; see forward(). A member so that a
    // bridge does not allocate one per message it carries.
    std::string _outgoingTopic;
    MqttBridgeStatus _status;
    std::string _lastError;

    MqttClient _local;
    MqttClient _remote;

    bool _started = false;
    bool _localSubscribed = false;
    bool _remoteSubscribed = false;

    // Which rules apply in each direction, resolved once at begin() so the
    // message path does no string work it can avoid.
    struct Rule {
        std::string sourceFilter;   // what is subscribed to on the source side
        std::string sourcePrefix;   // stripped from the incoming topic
        std::string targetPrefix;   // prepended for the outgoing topic

        // The set of topics this rule can PUBLISH into, as a filter. Compared
        // against the other direction's sourceFilter to find a loop before it
        // happens rather than after.
        std::string targetSpace;

        uint8_t qos = MQ_QOS_0;
        bool forwardRetain = false;
    };
    std::vector<Rule> _outbound;    // local -> remote
    std::vector<Rule> _inbound;     // remote -> local

    static void onLocalMessage(void* context, const MqttMessage& message);
    static void onRemoteMessage(void* context, const MqttMessage& message);

    void forward(const MqttMessage& message, const std::vector<Rule>& rules,
                 MqttClient& target, uint32_t& forwarded, uint32_t& refused);

    void subscribeSide(MqttClient& client, const std::vector<Rule>& rules,
                       bool& done);
    void updateState();

    // Whether any rule in one direction publishes into a space the other
    // direction subscribes to.
    bool wouldLoop(std::string& explanation) const;
};


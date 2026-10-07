// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttBridge.h"


#include <string.h>

const char* mq_bridgeDirectionName(MqttBridgeDirection direction) {
    switch (direction) {
        case MqttBridgeDirection::Out:  return "out";
        case MqttBridgeDirection::In:   return "in";
        case MqttBridgeDirection::Both: return "both";
    }
    return "?";
}

const char* mq_bridgeStateName(MqttBridgeState state) {
    switch (state) {
        case MqttBridgeState::Idle:       return "idle";
        case MqttBridgeState::Connecting: return "connecting";
        case MqttBridgeState::Up:         return "up";
        case MqttBridgeState::Degraded:   return "degraded";
    }
    return "?";
}

namespace {

MqttSpan asSpan(const std::string& text) {
    MqttSpan span;
    span.data = (const uint8_t*)text.data();
    span.length = (uint32_t)text.size();
    return span;
}

} // namespace

bool MqttBridge::wouldLoop(std::string& explanation) const {
    // For every rule in one direction, does anything it publishes land inside
    // what the other direction subscribes to on that same side? If so, the
    // bridge would hear its own output and send it round again.
    for (const Rule& out : _outbound) {
        for (const Rule& in : _inbound) {
            if (mq_filtersOverlap(asSpan(out.targetSpace), asSpan(in.sourceFilter))) {
                explanation =
                    "what the bridge publishes on the remote side (\"" +
                    out.targetSpace + "\") is matched by what it subscribes to "
                    "there (\"" + in.sourceFilter + "\"), so every message "
                    "would come straight back and loop";
                return true;
            }
            if (mq_filtersOverlap(asSpan(in.targetSpace), asSpan(out.sourceFilter))) {
                explanation =
                    "what the bridge publishes on the local side (\"" +
                    in.targetSpace + "\") is matched by what it subscribes to "
                    "there (\"" + out.sourceFilter + "\"), so every message "
                    "would come straight back and loop";
                return true;
            }
        }
    }
    return false;
}

MqttResult MqttBridge::begin(MqttTransport& localTransport,
                             MqttTransport& remoteTransport,
                             const MqttBridgeConfig& config) {
    _config = config;
    _lastError.clear();
    _outbound.clear();
    _inbound.clear();
    _status = MqttBridgeStatus();
    _started = false;
    _localSubscribed = false;
    _remoteSubscribed = false;

    // [V5:MQTT-3.1.4-3] A bridge using one identifier on both ends would take
    // over its own session the moment the two brokers turned out to be the
    // same box - which happens in every test rig and in more plants than
    // anybody admits.
    if (_config.local.clientId.empty() || _config.remote.clientId.empty() ||
        _config.local.clientId == _config.remote.clientId) {
        _lastError = "the two ends of a bridge need different client identifiers; "
                     "one would take over the other's session";
        return MqttResult::InvalidArgument;
    }

    const bool noLocalAvailable =
        _config.local.version == MqttVersion::V5 &&
        _config.remote.version == MqttVersion::V5 &&
        _config.useNoLocal;

    for (const MqttBridgeTopic& topic : _config.topics) {
        if (topic.filter.empty()) {
            _lastError = "a bridge rule has an empty topic filter";
            return MqttResult::InvalidTopic;
        }

        const bool carriesOut = topic.direction != MqttBridgeDirection::In;
        const bool carriesIn = topic.direction != MqttBridgeDirection::Out;

        // FORWARDING RETAIN REQUIRES RETAIN AS PUBLISHED, and that is 5.0 only.
        //
        // [V5:MQTT-3.3.1-9] says a broker clears the RETAIN flag on an
        // ordinary delivery. So a bridge subscribing normally NEVER SEES that
        // a message was retained - it would forward every one of them
        // unretained and the option would silently do nothing. The subscription
        // has to ask for Retain As Published [V5:MQTT-3.3.1-13] to observe the
        // flag at all, and 3.1.1 has no such option.
        //
        // Found by the bridge's own retain test, which is the only way it
        // could have been: every other test publishes unretained.
        if (topic.forwardRetain &&
            (_config.local.version != MqttVersion::V5 ||
             _config.remote.version != MqttVersion::V5)) {
            _lastError =
                "the rule on \"" + topic.filter + "\" asks to forward retained "
                "messages, which needs the Retain As Published subscription "
                "option - and 3.1.1 does not have one. Without it the bridge "
                "cannot even tell that a message was retained, so the option "
                "would silently do nothing";
            return MqttResult::NotSupportedByProtocol;
        }

        if (carriesOut) {
            Rule rule;
            rule.sourceFilter = topic.localPrefix + topic.filter;
            rule.sourcePrefix = topic.localPrefix;
            rule.targetPrefix = topic.remotePrefix;
            rule.targetSpace = topic.remotePrefix + topic.filter;
            rule.qos = topic.qos;
            rule.forwardRetain = topic.forwardRetain;
            _outbound.push_back(rule);
        }
        if (carriesIn) {
            Rule rule;
            rule.sourceFilter = topic.remotePrefix + topic.filter;
            rule.sourcePrefix = topic.remotePrefix;
            rule.targetPrefix = topic.localPrefix;
            rule.targetSpace = topic.localPrefix + topic.filter;
            rule.qos = topic.qos;
            rule.forwardRetain = topic.forwardRetain;
            _inbound.push_back(rule);
        }
    }

    // THE REFUSAL THAT MATTERS.
    //
    // With No Local there is nothing to check: the protocol guarantees the
    // bridge never receives what the bridge published. Without it, the only
    // guarantee available is that the two topic spaces cannot meet - so that
    // is what is checked, across every pair of rules rather than within one.
    if (!noLocalAvailable) {
        std::string explanation;
        if (wouldLoop(explanation)) {
            _lastError = explanation +
                ". 3.1.1 has no No Local subscription option, so the two "
                "directions need filters that cannot match each other's "
                "output - for example readings up under \"data/#\" and "
                "commands down under \"cmd/#\"";
            return MqttResult::InvalidArgument;
        }
    }

    if (_outbound.empty() && _inbound.empty()) {
        _lastError = "a bridge with no topic rules would carry nothing";
        return MqttResult::InvalidArgument;
    }

    _local.begin(localTransport, _config.local, _config.behaviour);
    _remote.begin(remoteTransport, _config.remote, _config.behaviour);

    _local.onMessage(&MqttBridge::onLocalMessage, this);
    _remote.onMessage(&MqttBridge::onRemoteMessage, this);

    return MqttResult::Ok;
}

MqttResult MqttBridge::start() {
    if (_outbound.empty() && _inbound.empty()) return MqttResult::NotInitialised;

    _started = true;
    _status.state = MqttBridgeState::Connecting;

    const MqttResult localResult = _local.connect();
    const MqttResult remoteResult = _remote.connect();

    // Both are attempted even when the first refuses. A bridge whose remote
    // end is unreachable should still carry nothing rather than refuse to
    // start - the local side is what keeps the panel working.
    if (!mq_accepted(localResult)) return localResult;
    return mq_accepted(remoteResult) ? MqttResult::Queued : remoteResult;
}

void MqttBridge::stop() {
    _started = false;
    _local.disconnect();
    _remote.disconnect();
    _localSubscribed = false;
    _remoteSubscribed = false;
    _status.state = MqttBridgeState::Idle;
}

void MqttBridge::subscribeSide(MqttClient& client, const std::vector<Rule>& rules,
                               bool& done) {
    if (done || !client.isConnected()) return;

    for (const Rule& rule : rules) {
        uint8_t options = rule.qos;

        // Retain As Published, so the bridge can SEE that a message was
        // retained. Without it the broker clears the flag on the way to us
        // [V5:MQTT-3.3.1-9] and forwardRetain would quietly do nothing.
        if (rule.forwardRetain && client.activeVersion() == MqttVersion::V5) {
            options |= MQ_SUBOPT_RETAIN_AS_PUB;
        }

        // No Local. THIS IS THE LOOP PREVENTION at 5.0, and it is exact:
        // [V5:3.8.3.1] "Application Messages MUST NOT be forwarded to a
        // connection with a ClientID equal to the ClientID of the publishing
        // connection." What this bridge published is never handed back to it.
        //
        // At 3.1.1 the option does not exist and the codec refuses to encode
        // it, so it is not set - and begin() has already refused any rule that
        // would need it.
        if (_config.useNoLocal && client.activeVersion() == MqttVersion::V5) {
            options |= MQ_SUBOPT_NO_LOCAL;
        }

        client.subscribeWithOptions(rule.sourceFilter.c_str(), options);
    }
    done = true;
}

void MqttBridge::updateState() {
    _status.localUp = _local.isConnected();
    _status.remoteUp = _remote.isConnected();

    if (!_started) {
        _status.state = MqttBridgeState::Idle;
    } else if (_status.localUp && _status.remoteUp) {
        _status.state = MqttBridgeState::Up;
    } else if (_status.localUp || _status.remoteUp) {
        // One end down is NOT the same as the bridge being down, and saying so
        // matters: the panel still works, and what is lost is only what was
        // crossing. An operator told "bridge down" goes looking for the wrong
        // fault.
        _status.state = MqttBridgeState::Degraded;
    } else {
        _status.state = MqttBridgeState::Connecting;
    }
}

void MqttBridge::poll() {
    _local.poll();
    _remote.poll();

    // Subscriptions are (re)issued whenever a side comes up. The client keeps
    // its own standing subscriptions across a reconnect, so this only has to
    // run once per connection rather than once per poll.
    if (!_local.isConnected()) _localSubscribed = false;
    if (!_remote.isConnected()) _remoteSubscribed = false;

    const bool wasLocalUp = _status.localUp;
    const bool wasRemoteUp = _status.remoteUp;

    subscribeSide(_local, _outbound, _localSubscribed);
    subscribeSide(_remote, _inbound, _remoteSubscribed);

    updateState();

    if (_status.localUp && !wasLocalUp) _status.localConnects++;
    if (_status.remoteUp && !wasRemoteUp) _status.remoteConnects++;
}

// =============================================================================
// FORWARDING
// =============================================================================

void MqttBridge::onLocalMessage(void* context, const MqttMessage& message) {
    MqttBridge& bridge = *(MqttBridge*)context;
    bridge.forward(message, bridge._outbound, bridge._remote,
                   bridge._status.forwardedOut, bridge._status.refusedOut);
}

void MqttBridge::onRemoteMessage(void* context, const MqttMessage& message) {
    MqttBridge& bridge = *(MqttBridge*)context;
    bridge.forward(message, bridge._inbound, bridge._local,
                   bridge._status.forwardedIn, bridge._status.refusedIn);
}

void MqttBridge::forward(const MqttMessage& message, const std::vector<Rule>& rules,
                         MqttClient& target, uint32_t& forwarded,
                         uint32_t& refused) {
    // The spans point into the source client's receive buffer and are valid
    // only for this call. publish() copies both into the operation before it
    // returns, so forwarding from inside the handler is safe - and doing it
    // anywhere else would need a copy of its own.
    MqttSpan topic = message.topic;

    for (const Rule& rule : rules) {
        MqttSpan filter;
        filter.data = (const uint8_t*)rule.sourceFilter.data();
        filter.length = (uint32_t)rule.sourceFilter.size();

        if (!mq_topicMatches(filter, topic)) continue;

        // Strip the source prefix, add the target one. Checked rather than
        // assumed: the filter matched, so the prefix is there, but a rule
        // rebuilt after a configuration change might not have been.
        size_t offset = 0;
        if (!rule.sourcePrefix.empty()) {
            if (topic.length < rule.sourcePrefix.size()) continue;
            if (memcmp(topic.data, rule.sourcePrefix.data(),
                       rule.sourcePrefix.size()) != 0) {
                continue;
            }
            offset = rule.sourcePrefix.size();
        }

        // ONE BUFFER, REUSED, rather than a fresh std::string per message.
        //
        // This declared a local string, reserved, then ASSIGNED the prefix
        // over the top - and operator= from another string can reallocate,
        // so the reserve was often wasted and the allocation happened anyway.
        // Once per bridged message, for the life of the bridge.
        //
        // _outgoingTopic is a member, so after the first message of a given
        // size it has the room already and this is two memcpys and no
        // allocator.
        _outgoingTopic.clear();
        _outgoingTopic.append(rule.targetPrefix);
        _outgoingTopic.append((const char*)topic.data + offset,
                              (size_t)topic.length - offset);
        const std::string& outgoing = _outgoingTopic;

        // Never above what arrived, never above what the rule allows.
        const uint8_t qos = message.qos < rule.qos ? message.qos : rule.qos;

        const MqttResult result =
            target.publish(outgoing.c_str(), message.payload.data,
                           (size_t)message.payload.length, qos,
                           rule.forwardRetain && message.retain);

        // A refusal is COUNTED, never ignored. The far side being down, its
        // queue being full, or a payload larger than it will accept are all
        // things an operator needs to see as a number rather than infer from
        // missing data on a dashboard.
        if (mq_accepted(result)) forwarded++;
        else refused++;

        // One rule per message. Two rules matching the same topic would send
        // the message twice, and the second copy is never what anybody meant.
        return;
    }
}


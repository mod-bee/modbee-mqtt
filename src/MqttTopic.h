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
// TOPIC NAMES AND FILTERS
// =============================================================================
//
// V5:4.7, V311:4.7. Identical rules in both versions, so this file is shared -
// it is the only part of the protocol layer that is.
//
// Matching is ALLOCATION-FREE: two cursors walking the two strings, never a
// split into a vector of levels. A gateway matching every inbound publish
// against a few hundred filters does this on every message, and a device with
// no spare heap cannot allocate per match.
//
// Nothing here knows which end it is. Matching a filter against a name is the
// same operation for anything routing inbound messages to its own
// subscriptions and for anything routing outbound messages to someone else's,
// which is what lets a broker reuse this untouched.

#include "MqttWire.h"

#include <string>

#define MQ_TOPIC_LEVEL_SEPARATOR  '/'
#define MQ_TOPIC_MULTI_WILDCARD   '#'
#define MQ_TOPIC_SINGLE_WILDCARD  '+'

// V5:4.7.2 - the Server MUST NOT match a filter starting with a wildcard
// against a name beginning with this. $SYS is the widely adopted prefix for
// broker statistics, and a client subscribing to "#" for diagnostics should
// not be flooded with them.
#define MQ_TOPIC_DOLLAR           '$'

// V5:4.8.2 - the literal prefix marking a Shared Subscription filter.
#define MQ_SHARE_PREFIX           "$share/"
#define MQ_SHARE_PREFIX_LEN       7

// --- Validation ---

// A Topic Name is what a PUBLISH carries. It may contain no wildcard at all.
//
//   [V5:MQTT-4.7.0-1] wildcards MUST NOT be used within a Topic Name
//   [V5:MQTT-4.7.3-1] at least one character
//   [V5:MQTT-4.7.3-2] MUST NOT include U+0000  (already refused by the UTF-8
//                     validator, so this is a second gate rather than the only one)
//   [V5:MQTT-4.7.3-3] MUST NOT encode to more than 65,535 bytes
bool mq_topicNameIsValid(const MqttSpan& name);

// A Topic Filter is what a SUBSCRIBE carries. It may contain wildcards, in the
// two positions the standard allows and nowhere else.
//
//   [V5:MQTT-4.7.1-1] '#' stands alone or follows '/', and MUST be last
//   [V5:MQTT-4.7.1-2] '+' MUST occupy an entire level
//
// A `$share/{name}/{filter}` prefix is accepted and the remainder validated as
// an ordinary filter. V5:4.8.2.
bool mq_topicFilterIsValid(const MqttSpan& filter);

// --- Matching ---

// Whether a Topic Name matches a Topic Filter.
//
// The caller has already validated both. An invalid filter matches nothing
// rather than matching everything, which is the safe direction: a subscription
// that quietly matched every topic because its filter was malformed would
// flood the application.
//
// [V5:MQTT-4.7.3-4] No normalisation of any kind. Each non-wildcard level must
// match character for character - so two topics that differ only by a Unicode
// normalisation form are different topics, and this function says so.
bool mq_topicMatches(const MqttSpan& filter, const MqttSpan& name);

// --- Shared subscriptions, V5:4.8.2 ---

// Whether the filter begins `$share/`.
// Whether two FILTERS could both match some topic name.
//
// Not the same question as matching a filter against a name, and the
// difference is the whole point: "a/+/c" and "a/b/#" match no common literal
// text, but they both match "a/b/c", so anything published under one can
// arrive through the other.
//
// A bridge needs this to know whether what it publishes on one side can come
// back through its own subscription on that side - which is the difference
// between a working bridge and a plant network saturated by one message. A
// broker can use it to spot overlapping subscriptions.
//
// Errs towards saying yes. A false "these overlap" costs a configuration that
// must be written more explicitly; a false "these do not" costs a loop.
bool mq_filtersOverlap(const MqttSpan& left, const MqttSpan& right);

bool mq_isSharedFilter(const MqttSpan& filter);

// Splits `$share/{ShareName}/{filter}` into its two parts.
//
// False when the shape is wrong: [V5:MQTT-4.8.2-1] requires a ShareName of at
// least one character, and [V5:MQTT-4.8.2-2] requires it to contain no '/',
// '+' or '#' and to be followed by a '/' and then a Topic Filter.
bool mq_splitSharedFilter(const MqttSpan& filter, MqttSpan& shareName,
                          MqttSpan& innerFilter);

// --- Convenience over C strings ---
//
// For callers holding a literal rather than a decoded span.
MqttSpan mq_topicSpan(const char* text);


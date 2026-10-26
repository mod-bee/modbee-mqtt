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
// FOLLOWING A MESSAGE TO ITS SUBSCRIBERS (host only)
// =============================================================================
//
// A message published with a confirm id (MqttBrokerRouted::confirm) is
// followed: each broker it is routed on says how many subscribers matched and
// how many were sent it at QoS 1 or 2 (and so will be awaited), and each of
// those deliveries says how it ended - acknowledged by its subscriber, or lost
// (expired, too large for it, its session gone). Whoever published it can then
// know when every subscriber has it. A board compiles none of this.

#include "../MqttPlatform.h"

#if defined(MQTT_PLATFORM_HOST)

#include <stdint.h>

enum class MqttBrokerConfirm : uint8_t { Routed, Acknowledged, Lost };

typedef void (*MqttBrokerConfirmFn)(void* context, uint64_t confirm, MqttBrokerConfirm what,
                                    uint32_t matched, uint32_t awaited);

#endif  // MQTT_PLATFORM_HOST

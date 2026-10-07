// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// THE TLS LISTENER'S MATERIAL, from the settings - and back out again for
// whoever asks what is being served.
//
// The settings name the material two ways: a path in modbee.yaml
// (broker.tls.cert, key, client-ca) or the PEM text itself (cert-pem, key-pem,
// client-ca-pem - kept in the settings database with config.set, or set in
// the environment). The text wins, as every stored setting wins over the
// file. Both become PEM text here, read and checked, so the listeners, the
// live swap and config.get all deal in one form.
//
// WHAT IS LIVE. The certificate, its key, the client CA and the
// client-certificate mode are applied to running listeners the moment they
// are stored (ShardedBroker::replaceTls): new handshakes see them, connections
// already up carry on. The port, the address, and turning TLS on or off are
// what sockets are bound to, and still wait for a restart.

#include "ServerConfig.h"
#include "ShardedBroker.h"

#include <string>

// The listener's configuration from a broker section: enabled, port, and the
// material as PEM text, read from a file where the settings name a path.
// False, with `error` naming the setting, if a file cannot be read or what it
// holds will not load - a key that is not the certificate's own among them.
bool tlsFromSettings(const BrokerSection& broker, ShardedTlsConfig& out, std::string& error);

// Whether a stored setting is one a running server applies at once.
bool isLiveTlsSetting(const std::string& path);

// Whether two configurations would serve the same thing to a new handshake.
bool sameTlsMaterial(const ShardedTlsConfig& a, const ShardedTlsConfig& b);

// config.get's "tls": what is being served NOW, never the private key.
//
//   {"serving": true, "port": 8883, "engine": "wolfSSL",
//    "certificate": {"fingerprint", "subject", "not_after", "names"} | null,
//    "key": "set" | "not set",
//    "client_ca": {"fingerprint", "subject", "certificates"} | null,
//    "client_certificates": "required" | "optional" | "none",
//    "certificate_login": true | false}
std::string describeTls(const ShardedTlsConfig& served, bool certificateLogin);

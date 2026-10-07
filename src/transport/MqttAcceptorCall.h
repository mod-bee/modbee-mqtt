// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// TAKING A NEW CONNECTION FROM AN ARDUINO ACCEPTOR (WiFiServer, EthernetServer,
// ...), with the right call.
//
// `accept()` hands over each new connection exactly once - what a broker
// wants. `available()` is older and means different things on different
// cores: on ESP32 it was the same as accept() (and core 3.x now deprecates
// it, warning in every sketch that uses it), while on the Arduino Ethernet
// library it returns ANY connected client with data waiting - including one
// the broker already took, which it would then take twice.
//
// So accept() wherever the server has one - ESP32, ESP8266, RP2040 and
// Ethernet 2.x all do - and available() only where it does not. Chosen at
// compile time; nothing is paid for it at run time.

namespace mq_acceptor {

template <typename Acceptor>
auto acceptFrom(Acceptor& acceptor, int) -> decltype(acceptor.accept()) {
    return acceptor.accept();
}

template <typename Acceptor>
auto acceptFrom(Acceptor& acceptor, long) -> decltype(acceptor.available()) {
    return acceptor.available();
}

// The one to call: the int overload wins when both exist.
template <typename Acceptor>
auto accept(Acceptor& acceptor) -> decltype(acceptFrom(acceptor, 0)) {
    return acceptFrom(acceptor, 0);
}

} // namespace mq_acceptor

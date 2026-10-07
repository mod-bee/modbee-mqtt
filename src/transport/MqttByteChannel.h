// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#pragma once

// Something a TLS session can send and receive through that is not a socket.
//
// A board has no file descriptor to hand an engine. It has a WiFiClient or an
// EthernetClient - a thing with read(), write() and connected() and no number
// behind it - so everything below the engine goes through a call.
//
// IN ITS OWN HEADER, WITH NO ENGINE TYPES IN IT, and that is the point rather
// than tidiness. The header that declares the engine's types also drags in
// wolfSSL's OpenSSL compatibility layer, which defines SHA_CTX - and the ESP32
// ROM defines SHA_CTX too, through WiFiClientSecure.h. Any public header that
// pulls the engine in therefore breaks every ESP32 sketch that also uses TLS
// as a client, with a conflicting-typedef error a long way from the cause.
//
// So this interface is declared where a template can use it without the engine
// coming too, and the engine stays behind MqttTlsServer.cpp.

#include <stddef.h>
#include <stdint.h>

// The return convention is what both engines want from an I/O callback:
//
//   > 0   that many bytes moved
//   = 0   nothing right now, come back - NOT an error, and not end of stream
//   < 0   the channel is gone
class MqttByteChannel {
public:
    virtual ~MqttByteChannel() = default;

    virtual int channelSend(const uint8_t* data, int length) = 0;
    virtual int channelRecv(uint8_t* into, int length) = 0;
    virtual bool channelOpen() const = 0;
};

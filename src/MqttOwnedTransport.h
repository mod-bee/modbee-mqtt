// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// =============================================================================
// A TRANSPORT AN L4 CLASS MAKES FOR ITS CALLER
// =============================================================================
//
// L4. The Sparkplug node and host take their network the way MqttDevice does:
//
//   on a board   any Arduino Client - WiFiClient, EthernetClient,
//                TinyGsmClient, WiFiClientSecure, SSLClient - wrapped here in
//                an MqttStreamTransport that lives inside this object;
//   on a host    nothing at all - a TCP transport, or TLS once asked for;
//   anywhere     a transport the caller built and configured, which is
//                theirs and is never touched.
//
// WHY IT EXISTS. Both Sparkplug classes had a default constructor that made no
// transport, so connect() could only ever return false, and TLS calls that
// were declared and never defined. Copying MqttDevice's assembly into each of
// them would have been the third copy of the same platform knowledge; this is
// the one place it lives for them.
//
// RETARGETING. An owned transport is pointed at a host by target(), which the
// owner calls on every connect - so a Sparkplug node walking its server list
// really does connect to the next one. A borrowed transport is left alone:
// a class that repointed a transport it did not make would quietly undo
// whatever its caller had configured on it.
//
// Platform-aware by necessity, like MqttDevice - see scripts/check-layering.py.

#include "MqttFeatures.h"   // MQTT_TLS_SOCKETS: an undefined macro is 0 in #if
#include "MqttTransport.h"
#include "MqttTypes.h"
#include "transport/MqttStreamTransport.h"
#include "transport/MqttTcpTransport.h"
#include "transport/MqttTlsTransport.h"

#include <new>
#include <type_traits>

class MqttOwnedTransport {
public:
    MqttOwnedTransport() {
#if defined(MQTT_PLATFORM_HOST)
        _transport = &_tcp;
        _owned = true;
#endif
    }

    ~MqttOwnedTransport() {
        if (_destroy && _transport) _destroy(_transport);
    }

    MqttOwnedTransport(const MqttOwnedTransport&) = delete;
    MqttOwnedTransport& operator=(const MqttOwnedTransport&) = delete;

#if defined(MQTT_PLATFORM_ARDUINO)
    // The sketch's network channel. It is the sketch's, and must outlive this.
    template <typename ChannelType>
    void adopt(ChannelType& channel) {
        typedef MqttStreamTransport<ChannelType> Stream;
        static_assert(sizeof(Stream) <= sizeof(_storage),
                      "network channel transport does not fit - raise kStorageSize");
        static_assert(alignof(Stream) <= alignof(void*),
                      "network channel transport wants an alignment this storage "
                      "does not give it");
        release();
        _transport = new (_storage) Stream(channel);
        _retarget = &retargetStream<ChannelType>;
        _destroy = &destroyStream<ChannelType>;
        _owned = true;
    }
#else
    // Declared on a host too, so a class above can offer a channel
    // constructor without naming the platform - and refused only if a host
    // build actually calls it, with the reason.
    template <typename ChannelType>
    void adopt(ChannelType&) {
        static_assert(sizeof(ChannelType) == 0,
                      "a network channel (WiFiClient, EthernetClient...) is for a "
                      "board build - on a host use the no-argument constructor, "
                      "or pass an MqttTransport");
    }
#endif

    // A transport the caller built. Never configured, never destroyed here.
    void borrow(MqttTransport& transport) {
        release();
        _transport = &transport;
        _owned = false;
    }

    // Null only on a board that was given no channel.
    MqttTransport* get() const { return _transport; }
    bool owned() const { return _owned; }

    // Points an owned transport at a host. `host` must outlive the connection:
    // a stream transport keeps the pointer, not a copy.
    void target(const char* host, uint16_t port) {
        if (!_owned) return;
        if (_retarget) {
            _retarget(_transport, host, port);
            return;
        }
#if defined(MQTT_PLATFORM_HOST)
#if MQTT_TLS_SOCKETS
        if (_useTls) {
            _tls.configure(host, port);
            return;
        }
#endif
        _tcp.configure(host, port);
#else
        (void)host;
        (void)port;
#endif
    }

    // 8883 once TLS is on, 1883 otherwise. A channel's TLS is the channel's
    // business (WiFiClientSecure, SSLClient), so with one of those the caller
    // names port 8883 themselves.
    uint16_t defaultPort() const {
#if MQTT_TLS_SOCKETS
        if (_useTls) return MQ_PORT_TLS;
#endif
        return MQ_PORT_TCP;
    }

#if MQTT_TLS_SOCKETS
    // --- Host TLS, on the transport this object owns ---
    //
    // Each returns false for a borrowed transport: TLS on one of those is
    // configured by whoever built it, and pretending to set it here would be
    // a setting that silently does nothing.
    bool useTls(bool enabled) {
        if (!_owned || _retarget) return false;   // borrowed, or a channel
        _useTls = enabled;
        _transport = enabled ? static_cast<MqttTransport*>(&_tls)
                             : static_cast<MqttTransport*>(&_tcp);
        return true;
    }
    bool trustFile(const char* path) {
        if (path == nullptr || !useTls(true)) return false;
        _tls.setCaFile(path);
        return true;
    }
    bool pin(const char* fingerprint) {
        if (fingerprint == nullptr || !useTls(true)) return false;
        _tls.setPinnedKeyFingerprint(fingerprint);
        return true;
    }
    bool tls() const { return _useTls; }
#endif

private:
    void release() {
        if (_destroy && _transport) _destroy(_transport);
        _destroy = nullptr;
        _retarget = nullptr;
        _transport = nullptr;
        _owned = false;
#if MQTT_TLS_SOCKETS
        _useTls = false;
#endif
    }

#if defined(MQTT_PLATFORM_ARDUINO)
    template <typename ChannelType>
    static void retargetStream(void* transport, const char* host, uint16_t port) {
        ((MqttStreamTransport<ChannelType>*)transport)->target(host, port);
    }

    template <typename ChannelType>
    static void destroyStream(void* transport) {
        ((MqttStreamTransport<ChannelType>*)transport)->~MqttStreamTransport();
    }
#endif

    MqttTransport* _transport = nullptr;
    bool _owned = false;

    void (*_retarget)(void* transport, const char* host, uint16_t port) = nullptr;
    void (*_destroy)(void* transport) = nullptr;

#if defined(MQTT_PLATFORM_ARDUINO)
    // Where a sketch's channel transport is built (only a board has one): the
    // same size MqttDevice gives it - a channel reference, a host pointer, a
    // port and a few flags.
    static const size_t kStorageSize = 64;
    alignas(void*) unsigned char _storage[kStorageSize];
#endif

#if defined(MQTT_PLATFORM_HOST)
    MqttTcpTransport _tcp;
#if MQTT_TLS_SOCKETS
    MqttTlsTransport _tls;
    bool _useTls = false;
#endif
#endif
};

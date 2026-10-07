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
// THE OTHER END OF MqttTlsTransport
// =============================================================================
//
// MqttTcpListener accepts connections; MqttTlsTransport dials encrypted ones.
// Until this file there was nothing that ACCEPTED an encrypted one, so a broker
// could only ever be spoken to in clear text - the client half of TLS has been
// finished and tested for months while the server half did not exist.
//
// Nothing above had to change to let it in. MqttBroker.h already said what the
// shape would be: "plain TCP on 1883 and TLS on 8883 are two listeners over the
// same broker, and neither the sessions nor the routing can tell them apart."
// MqttListener::begin() already named "a certificate that will not load" as one
// of its failure modes. This is that file arriving, not a new seam.
//
// -----------------------------------------------------------------------------
// THE ONE THING A SERVER TRANSPORT MUST NOT COPY FROM THE CLIENT
// -----------------------------------------------------------------------------
//
// MqttTlsTransport::isConnected() returns FALSE while its handshake is in
// flight, and that is right for a client: MqttClient is asking "may I send
// CONNECT yet?".
//
// A server transport answering the same way would be destroyed on its first
// scan. MqttLink::peerGone() is `peerClosed() || !isConnected()`, and
// MqttBroker::serviceConnection turns a Closed link straight into
//
//     closeConnection(connection, 0, false, /*publishWill=*/true)
//
// So an accepted connection still shaking hands would be torn down before it
// could CONNECT - AND WOULD PUBLISH A WILL for a session that never existed.
// A broker that emits a death notice for a device that never finished saying
// hello is worse than one with no TLS at all.
//
// Therefore: an accepted TLS transport reports isConnected() TRUE from the
// moment it is accepted until the socket is actually gone. The handshake is a
// state inside it, not an absence of connection. A handshake that FAILS sets
// peerClosed(), which the broker already treats as an ordinary disconnect of a
// connection that never authenticated - no Will, because there is no session.

#include "../MqttFeatures.h"
#include "MqttSocketSupport.h"   // MQTT_SOCKET_READ_SIZE, and the socket shim
#include "../MqttListener.h"
#include "../MqttTransport.h"

#if MQTT_TLS_SOCKETS

#include <string>
#include <unordered_map>
#include <vector>

// One accepted, encrypted connection.
//
// An MqttTransport like any other: the broker cannot tell it from a plain one,
// which is what lets one broker serve 1883 and 8883 from the same routing.
class MqttAcceptedTlsTransport : public MqttTransport {
public:
    MqttAcceptedTlsTransport() = default;
    ~MqttAcceptedTlsTransport() override;

    MqttAcceptedTlsTransport(const MqttAcceptedTlsTransport&) = delete;
    MqttAcceptedTlsTransport& operator=(const MqttAcceptedTlsTransport&) = delete;

    // Takes ownership of an accepted descriptor and of `ssl`, which the
    // listener has already put into server mode.
    //
    // void*, not SSL*, because which engine is compiled in is the .cpp's
    // business: OpenSSL's handle is SSL and wolfSSL's is WOLFSSL, and a header
    // that named either would be wrong half the time and would put that
    // engine's headers on every consumer's include path.
    void adopt(int socket, void* ssl, const char* peerName);

    bool begin() override;
    void end() override;
    size_t readable() override;
    size_t read(uint8_t* into, size_t length) override;
    size_t write(const uint8_t* buffer, size_t length) override;

    // TRUE from adopt() until the socket is gone. See the long comment above:
    // this deliberately does NOT report the handshake.
    bool isConnected() const override;
    int waitHandle() const override { return _socket; }
    // Our own buffer OR the engine's. SSL_pending() is the half a readiness
    // set cannot see: a TLS record arrives whole, is decrypted whole, and the
    // plaintext of several MQTT packets can be sitting inside the SSL object
    // with the socket reporting nothing left to read.
    bool hasBufferedInput() const override;
    bool peerClosed() const override;

    const char* peerName() const override { return _peerName.c_str(); }

    bool inUse() const { return _socket >= 0; }

    // Whether the TLS handshake has completed. Diagnostics and tests - the
    // broker neither asks nor needs to.
    bool secured() const { return _secured; }

    // Empty until something goes wrong. The peer's words, not ours, wherever
    // OpenSSL has any.
    const std::string& lastError() const { return _lastError; }

private:
    int _socket = -1;
    void* _ssl = nullptr;       // SSL* / WOLFSSL*
    bool _handshaking = false;
    bool _secured = false;
    bool _peerClosed = false;
    std::string _peerName;
    std::string _lastError;

    // See MQTT_SOCKET_READ_SIZE in MqttSocketSupport.h - one definition
    // for all four transports, which is why this is not a number.
    //
    // ALLOCATED ON FIRST USE, as in MqttAcceptedTransport, and the broker
    // never uses it: MqttLink reads in bulk, and read() decrypts straight into
    // the caller's buffer whenever nothing is held here. It was an inline
    // array, so every pool slot - in use or not - cost 4 kB.
    static const size_t kReceiveBufferSize = MQTT_SOCKET_READ_SIZE;
    std::vector<uint8_t> _receive;
    size_t _receiveHead = 0;
    size_t _receiveTail = 0;

    void closeAll();
    void fail(const std::string& what);
    bool finishHandshake();
    bool fill();
    // One SSL_read into `into`; the outcome is recorded exactly as fill()
    // records it. 0 means nothing now, or the connection is gone.
    size_t decrypt(uint8_t* into, size_t length);
};

class MqttTlsListener : public MqttListener {
public:
    MqttTlsListener() = default;
    ~MqttTlsListener() override;

    MqttTlsListener(const MqttTlsListener&) = delete;
    MqttTlsListener& operator=(const MqttTlsListener&) = delete;

    // Where to listen. Same contract as MqttTcpListener: `address` may be null
    // or empty for every interface, and `maximum` is a pool sized once.
    void configure(const char* address, uint16_t port, size_t maximum = 32);

    // The certificate this broker presents, and its key. FILE PATHS.
    void useCertificateFile(const char* certificatePath, const char* keyPath);

    // The same, held in memory. A PLC that generated its own identity has it
    // as a string and has nowhere sensible to write it - see MqttIdentity.
    void useCertificate(const char* certificatePem, const char* keyPem);

    // Require a client certificate, verified against this CA. Mutual TLS: the
    // broker proves who it is AND asks the same of every device.
    //
    // Without this, clients are not asked for one - which is the ordinary
    // arrangement, where a client proves itself with a user name and password
    // over a channel it has already verified.
    void requireClientCertificate(const char* caPem);

    // Several listeners on the same port, one per broker of a multi-core
    // server - see MqttTcpListener::setReusePort(). Set before begin().
    void setReusePort(bool reuse) { _reusePort = reuse; }

    // Serve with `primary`'s certificate, key and client CA, and - where the
    // engine allows it (OpenSSL) - its very TLS context, so a session ticket
    // issued by one listener is honoured by every other: a device that
    // reconnects resumes its session whichever listener the kernel hands it
    // to, instead of paying for a full handshake. `primary` must have begun
    // and must outlive this listener's begin(). Set before begin().
    void shareContextWith(const MqttTlsListener& primary);

    bool begin() override;
    void end() override;
    MqttTransport* accept() override;
    void release(MqttTransport* transport) override;
    bool isListening() const override { return _socket >= 0; }
    int waitHandle() const override { return _socket; }
    size_t capacityRemaining() const override;

    uint16_t port() const { return _port; }
    uint16_t boundPort() const { return _boundPort; }
    const std::string& lastError() const { return _lastError; }
    size_t active() const;

private:
    std::string _address;
    uint16_t _port = 0;
    uint16_t _boundPort = 0;
    bool _reusePort = false;
    size_t _servingCapacity = 0;
    int _socket = -1;
    std::string _lastError;

    const MqttTlsListener* _sharedFrom = nullptr;

    std::string _certFile, _keyFile;
    std::string _certPem,  _keyPem;
    std::string _clientCaPem;
    bool _requireClientCert = false;

    void* _context = nullptr;   // SSL_CTX* / WOLFSSL_CTX*

    std::vector<MqttAcceptedTlsTransport*> _pool;

    // Free pool entries and each entry's place in the pool, as in
    // MqttTcpListener (internal/CLOUD-PLAN.md, L14): accept() and release()
    // walked the pool once per connection.
    std::vector<uint32_t> _free;
    std::vector<uint8_t> _isFree;
    std::unordered_map<const MqttTransport*, uint32_t> _poolIndex;
    void giveBack(uint32_t index);

    void releaseContext();
    bool buildContext();
    void fail(const std::string& what);
};

#endif // MQTT_TLS_SOCKETS

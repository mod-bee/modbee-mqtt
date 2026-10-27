// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTlsListener.h"

#if MQTT_TLS_SOCKETS


#include "MqttSocketSupport.h"
#include "MqttTlsSupport.h"
#include "MqttCertificate.h"

#include <string.h>
#include <stdio.h>

// =============================================================================
// ONE ACCEPTED CONNECTION
// =============================================================================

MqttAcceptedTlsTransport::~MqttAcceptedTlsTransport() { closeAll(); }

void MqttAcceptedTlsTransport::adopt(int socket, void* ssl, const char* peerName) {
    closeAll();
    _socket = socket;
    _ssl = ssl;
    _peerName = peerName ? peerName : "";
    _peerClosed = false;
    _secured = false;
    _lastError.clear();
    _peerCertificateName.clear();
    _peerCertificateFingerprint.clear();
    _receiveHead = _receiveTail = 0;

    // The socket is already non-blocking - the listener set it before handing
    // it over, because a blocking accept()ed socket would stall the one poll()
    // that drives every other connection on this broker.
    _handshaking = true;
}

bool MqttAcceptedTlsTransport::begin() {
    // Already open: it was accepted, not dialled.
    return _socket >= 0;
}

void MqttAcceptedTlsTransport::end() { closeAll(); }

void MqttAcceptedTlsTransport::closeAll() {
    if (_ssl) {
        // One shot. Waiting for the peer's close_notify would mean blocking in
        // a teardown path, and the socket is about to go anyway.
        SSL_shutdown((SSL*)_ssl);
        SSL_free((SSL*)_ssl);
        _ssl = nullptr;
    }
    if (_socket >= 0) {
        MQ_CLOSE(_socket);
        _socket = -1;
    }
    _handshaking = false;
    _secured = false;
    _peerCertificateName.clear();
    _peerCertificateFingerprint.clear();
    _receiveHead = _receiveTail = 0;
}

// The handshake has completed - noticed by whichever call got there first.
//
// WHO THE PEER PROVED TO BE, taken now and kept: the CN and the fingerprint of
// a client certificate the engine VERIFIED. SSL_get_verify_result() is asked
// as well as whether there is a certificate at all, because the two are
// different questions - an engine told to request a certificate without
// demanding one still hands back whatever it was shown - and only the answer
// to both is an identity.
void MqttAcceptedTlsTransport::secured() {
    if (_secured) return;
    _handshaking = false;
    _secured = true;
    if (_ssl == nullptr) return;
    X509* peer = SSL_get_peer_certificate((SSL*)_ssl);
    if (peer == nullptr) return;
    if (SSL_get_verify_result((SSL*)_ssl) == X509_V_OK) {
        std::vector<uint8_t> der;
        MqttCertificateFacts facts;
        if (mq_tls::certificateDer(peer, der) &&
            mqttCertificateFacts(der.data(), der.size(), facts)) {
            _peerCertificateName = facts.commonName;
            _peerCertificateFingerprint = facts.fingerprint;
        }
    }
    X509_free(peer);
}

void MqttAcceptedTlsTransport::fail(const std::string& what) {
    const std::string detail = mq_tls::engineError();
    _lastError = detail.empty() ? what : what + ": " + detail;

    // NOT closeAll(). The broker learns of this through peerClosed(), and it
    // needs the slot to still look in-use until it releases it - a slot that
    // freed itself underneath the broker would be handed to the next
    // connection while the old one was still being tidied up.
    _peerClosed = true;
}

// The handshake, one step per call, never waiting.
//
// Returns true once the connection is secured. A `false` return is not a
// failure on its own: WANT_READ and WANT_WRITE are the ordinary answer while
// the peer is still talking, and the caller simply comes back.
bool MqttAcceptedTlsTransport::finishHandshake() {
    if (!_handshaking) return _secured;
    if (!_ssl || _socket < 0) return false;

    mq_tls::clearErrors();
    const int result = SSL_accept((SSL*)_ssl);
    if (result == 1) {
        secured();
        return true;
    }

    const int reason = SSL_get_error((SSL*)_ssl, result);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
        return false;                      // still shaking hands; come back
    }

    // A real failure. Almost always the client's doing: no certificate when one
    // was required, a version or cipher neither end shares, or a scanner that
    // opened a port and sent something that is not TLS at all. The broker sees
    // peerClosed() and tidies the slot away without a Will, because nothing
    // ever CONNECTed on it.
    _handshaking = false;
    fail("TLS handshake failed");
    return false;
}

size_t MqttAcceptedTlsTransport::readable() {
    if (_receiveHead < _receiveTail) return _receiveTail - _receiveHead;
    if (!fill()) return 0;
    return _receiveTail - _receiveHead;
}

size_t MqttAcceptedTlsTransport::read(uint8_t* into, size_t length) {
    if (into == nullptr || length == 0) return 0;

    // STRAIGHT INTO THE CALLER'S BUFFER when nothing is held here - the
    // broker's case, every time (see the comment on _receive).
    if (_receiveHead >= _receiveTail) return decrypt(into, length);

    // Otherwise out of what readable() already decrypted.
    const size_t have = _receiveTail - _receiveHead;
    const size_t taken = have < length ? have : length;
    memcpy(into, _receive.data() + _receiveHead, taken);
    _receiveHead += taken;
    return taken;
}

bool MqttAcceptedTlsTransport::fill() {
    if (!_ssl || _socket < 0) return false;
    _receiveHead = _receiveTail = 0;
    if (_receive.empty()) _receive.resize(kReceiveBufferSize);
    _receiveTail = decrypt(_receive.data(), _receive.size());
    return _receiveTail > 0;
}

size_t MqttAcceptedTlsTransport::decrypt(uint8_t* into, size_t length) {
    if (!_ssl || _socket < 0) return 0;
    if (length > 0x7FFFFFFF) length = 0x7FFFFFFF;

    mq_tls::clearErrors();
    const int got = SSL_read((SSL*)_ssl, into, (int)length);
    if (got > 0) {
        // Application data means the handshake is over, whether or not
        // anything here drove it to the end: SSL_read finishes a handshake by
        // itself, and on this end it nearly always does - the first record
        // after it is the CONNECT.
        secured();
        return (size_t)got;
    }

    const int reason = SSL_get_error((SSL*)_ssl, got);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
        return 0;                          // nothing yet, which is ordinary
    }
    if (reason == SSL_ERROR_ZERO_RETURN) {
        _peerClosed = true;                // close_notify: a clean goodbye
        return 0;
    }
    _peerClosed = true;
    fail("TLS connection lost");
    return 0;
}

size_t MqttAcceptedTlsTransport::write(const uint8_t* buffer, size_t length) {
    if (!_ssl || _socket < 0 || buffer == nullptr || length == 0) return 0;
    if (_handshaking && !finishHandshake()) return 0;

    mq_tls::clearErrors();
    const int sent = SSL_write((SSL*)_ssl, buffer, (int)length);
    if (sent > 0) return (size_t)sent;

    const int reason = SSL_get_error((SSL*)_ssl, sent);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
        // Zero written, and the caller resumes from its own cursor. WANT_READ
        // on a write is not a contradiction: a renegotiation makes the write
        // need to read first.
        return 0;
    }
    _peerClosed = true;
    fail("TLS connection lost while sending");
    return 0;
}

// TRUE from adopt() until the socket is gone - NOT "the handshake finished".
// The whole argument is at the top of the header: answering the client's way
// here would have the broker publish a Will for a session that never existed.
bool MqttAcceptedTlsTransport::hasBufferedInput() const {
    if (_receiveHead < _receiveTail) return true;
    // SSL_pending() is decrypted plaintext already inside the engine, with
    // nothing left on the socket for a readiness set to notice.
    if (_ssl == nullptr) return false;
    return SSL_pending((SSL*)_ssl) > 0;
}

bool MqttAcceptedTlsTransport::isConnected() const { return _socket >= 0; }

bool MqttAcceptedTlsTransport::peerClosed() const { return _peerClosed; }

// =============================================================================
// THE LISTENER
// =============================================================================

MqttTlsListener::~MqttTlsListener() {
    end();
    for (MqttAcceptedTlsTransport* entry : _pool) delete entry;
    _pool.clear();
    releaseContext();
}

// SSL_CTX_free is a reference drop, so a context shared with other listeners
// (shareContextWith) lives until the last of them lets go - and OpenSSL's
// every SSL object holds a reference of its own as well.
void MqttTlsListener::releaseContext() {
    if (_context) {
        SSL_CTX_free((SSL_CTX*)_context);
        _context = nullptr;
    }
}

void MqttTlsListener::configure(const char* address, uint16_t port, size_t maximum) {
    _address = address ? address : "";
    _port = port;
    if (maximum == 0) maximum = 1;

    for (MqttAcceptedTlsTransport* entry : _pool) delete entry;
    _pool.clear();
    // One more than asked for; see MqttTcpListener::configure(). The spare is
    // where a connection that has to be refused gets told so in MQTT.
    _servingCapacity = maximum;
    _pool.reserve(maximum + 1);
    for (size_t i = 0; i < maximum + 1; i++) _pool.push_back(new MqttAcceptedTlsTransport());

    _poolIndex.clear();
    _poolIndex.reserve(_pool.size());
    _free.clear();
    _free.reserve(_pool.size());
    _isFree.assign(_pool.size(), 1);
    for (size_t index = _pool.size(); index > 0; index--) {
        _free.push_back((uint32_t)(index - 1));
        _poolIndex[_pool[index - 1]] = (uint32_t)(index - 1);
    }
}

void MqttTlsListener::giveBack(uint32_t index) {
    if (index >= _isFree.size() || _isFree[index]) return;   // never twice
    _isFree[index] = 1;
    _free.push_back(index);
}

void MqttTlsListener::shareContextWith(const MqttTlsListener& primary) {
    _sharedFrom = &primary;
    // The configuration too, so that where the engine cannot share a context
    // this listener builds an identical one of its own.
    _certFile = primary._certFile;
    _keyFile = primary._keyFile;
    _certPem = primary._certPem;
    _keyPem = primary._keyPem;
    _clientCaPem = primary._clientCaPem;
    _requireClientCert = primary._requireClientCert;
    _peerCertificateOptional = primary._peerCertificateOptional;
}

void MqttTlsListener::useCertificateFile(const char* certificatePath, const char* keyPath) {
    _certFile = certificatePath ? certificatePath : "";
    _keyFile = keyPath ? keyPath : "";
    _certPem.clear();
    _keyPem.clear();
}

void MqttTlsListener::useCertificate(const char* certificatePem, const char* keyPem) {
    _certPem = certificatePem ? certificatePem : "";
    _keyPem = keyPem ? keyPem : "";
    _certFile.clear();
    _keyFile.clear();
}

void MqttTlsListener::requireClientCertificate(const char* caPem) {
    _clientCaPem = caPem ? caPem : "";
    _requireClientCert = !_clientCaPem.empty();
    _peerCertificateOptional = false;
}

void MqttTlsListener::requestClientCertificate(const char* caPem) {
    _clientCaPem = caPem ? caPem : "";
    _requireClientCert = !_clientCaPem.empty();
    _peerCertificateOptional = _requireClientCert;
}

void MqttTlsListener::fail(const std::string& what) {
    const std::string detail = mq_tls::engineError();
    _lastError = detail.empty() ? what : what + ": " + detail;
}

// Everything about the certificate, resolved once. A certificate that will not
// load is a begin() failure with a sentence an operator can act on - which is
// exactly what MqttListener.h:44 says begin() is for.
bool MqttTlsListener::buildContext() {
    mq_tls::ensureEngine();
    releaseContext();

#if !MQTT_TLS_ENGINE_WOLFSSL
    // One context, shared. OpenSSL's SSL_CTX is safe to create connections
    // from on several threads at once, and its session-ticket keys live in it
    // - which is the reason for sharing (see shareContextWith()).
    if (_sharedFrom != nullptr && _sharedFrom->_context != nullptr) {
        if (SSL_CTX_up_ref((SSL_CTX*)_sharedFrom->_context) != 1) {
            fail("the shared TLS context could not be referenced");
            return false;
        }
        _context = _sharedFrom->_context;
        return true;
    }
#endif

    _context = createContext();
    return _context != nullptr;
}

// A new context from the settings, owned by the caller - begin() keeps it,
// makeContext() hands it to whoever is replacing a running listener's.
void* MqttTlsListener::createContext() {
    mq_tls::ensureEngine();
    SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) { fail("the TLS context would not be created"); return nullptr; }
    // Every failure below gives the half-built context back.
    struct Abandon {
        SSL_CTX*& ctx;
        ~Abandon() { if (ctx) SSL_CTX_free(ctx); }
    } abandon{ctx};

#if !MQTT_TLS_ENGINE_WOLFSSL && defined(SSL_MODE_RELEASE_BUFFERS)
    // An idle connection gives its record buffers back - some 34 kB of them
    // in OpenSSL - and borrows them again when a record arrives. A broker's
    // connections are idle nearly all the time; at a hundred thousand of them
    // this is the difference between gigabytes and not.
    SSL_CTX_set_mode(ctx, SSL_MODE_RELEASE_BUFFERS);
#endif
#if defined(SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER)
    // A write that could not finish is retried from wherever the bytes are
    // now. The broker's transmit buffer grows - and moves - while a write is
    // pending, and without this the retry is refused as a bad write retry and
    // a slow reader's connection is dropped.
    SSL_CTX_set_mode(ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
#endif

    // A client that goes away mid-handshake must produce an error code, never
    // a signal that terminates the broker. A broker is where this matters
    // most: every port scanner and every device that loses power is one of
    // these, and the default behaviour is to take the process down with it.
    mq_tls::useNonSignallingIo(ctx);

    // 1.2 is the floor. 1.0 and 1.1 are withdrawn, and a broker that still
    // offers them is a broker that can be talked down to them.
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

    if (!_certFile.empty() && !_keyFile.empty()) {
        // The CHAIN file call, not the single-certificate one: a CA-issued
        // certificate usually arrives as fullchain.pem, and a broker that
        // presents only the first entry is one a client cannot build a path
        // to its root from.
        if (SSL_CTX_use_certificate_chain_file(ctx, _certFile.c_str()) != 1) {
            fail("the certificate at " + _certFile + " would not load");
            return nullptr;
        }
        if (SSL_CTX_use_PrivateKey_file(ctx, _keyFile.c_str(), SSL_FILETYPE_PEM) != 1) {
            fail("the private key at " + _keyFile + " would not load");
            return nullptr;
        }
    } else if (!_certPem.empty() && !_keyPem.empty()) {
        // In memory, for an identity this PLC generated and never wrote down.
        BIO* certBio = BIO_new_mem_buf(_certPem.data(), (int)_certPem.size());
        X509* cert = certBio ? PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr) : nullptr;
        if (!cert) { if (certBio) BIO_free(certBio); fail("the certificate would not parse"); return nullptr; }
        const int certOk = SSL_CTX_use_certificate(ctx, cert);
        X509_free(cert);
        if (certOk != 1) { BIO_free(certBio); fail("the certificate was refused"); return nullptr; }
        // Whatever follows the first certificate is its chain, presented
        // with it - the same as the chain file above, for the same reason.
        while (X509* link = PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr)) {
            if (SSL_CTX_add_extra_chain_cert(ctx, link) != 1) X509_free(link);   // owned on success
        }
        BIO_free(certBio);
        mq_tls::clearErrors();   // the end of the text, reported as "no start line"

        BIO* keyBio = BIO_new_mem_buf(_keyPem.data(), (int)_keyPem.size());
        EVP_PKEY* key = keyBio ? PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr) : nullptr;
        if (keyBio) BIO_free(keyBio);
        if (!key) { fail("the private key would not parse"); return nullptr; }

        // Asked here, before OpenSSL is given the key, because
        // SSL_CTX_use_PrivateKey rejects a mismatched key with an error that
        // says only that the key was refused. The commonest cause by far is a
        // certificate and a key from two different generations, and an
        // operator who is told which of the two problems they have does not
        // have to guess.
        if (!mq_tls::keyMatchesCertificate(SSL_CTX_get0_certificate(ctx), key)) {
            EVP_PKEY_free(key);
            fail("the private key does not match the certificate");
            return nullptr;
        }

        const int keyOk = SSL_CTX_use_PrivateKey(ctx, key);
        EVP_PKEY_free(key);
        if (keyOk != 1) { fail("the private key was refused"); return nullptr; }
    } else {
        // Named plainly. A TLS listener with nothing to present is a
        // configuration mistake, not a runtime condition.
        _lastError = "no certificate: call useCertificateFile() or useCertificate()";
        return nullptr;
    }

    if (SSL_CTX_check_private_key(ctx) != 1) {
        fail("the private key does not match the certificate");
        return nullptr;
    }

    if (_requireClientCert) {
        BIO* caBio = BIO_new_mem_buf(_clientCaPem.data(), (int)_clientCaPem.size());
        X509_STORE* store = SSL_CTX_get_cert_store(ctx);
        int loaded = 0;
        if (caBio && store) {
            while (X509* ca = PEM_read_bio_X509(caBio, nullptr, nullptr, nullptr)) {
                if (X509_STORE_add_cert(store, ca) == 1) loaded++;
                X509_free(ca);
            }
        }
        if (caBio) BIO_free(caBio);
        if (loaded == 0) { fail("the client CA certificate would not parse"); return nullptr; }
        // Required, or only requested (requestClientCertificate()). Either
        // way a certificate that IS offered must verify, or the handshake
        // fails - "optional" is about a peer that offers none, never about one
        // that offers a bad one.
        SSL_CTX_set_verify(ctx, _peerCertificateOptional
                                    ? SSL_VERIFY_PEER
                                    : SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                           nullptr);

        // OpenSSL refuses to resume a session on a context that verifies
        // peers unless the context is named - and the refusal is a failed
        // handshake, not a full one. A client that cached a session would be
        // locked out on its second connection.
        static const unsigned char kSessionContext[] = "modbee";
        SSL_CTX_set_session_id_context(ctx, kSessionContext, sizeof kSessionContext - 1);

#if MQTT_TLS_ENGINE_WOLFSSL
        // EVERY CONNECTION SHOWS ITS CERTIFICATE AGAIN. A resumed session
        // skips the certificate exchange, and what wolfSSL hands back as the
        // peer's certificate afterwards depends on how it was built - so a
        // device that resumed could arrive with no verified identity, and a
        // login taken from its certificate would be refused for no reason it
        // could see. OpenSSL keeps the peer's certificate in the session and
        // has no such gap. A full handshake per connection is the price, and
        // devices reconnect rarely enough to pay it.
        SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);
        SSL_CTX_set_options(ctx, SSL_OP_NO_TICKET);
#endif
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    }

    SSL_CTX* made = ctx;
    ctx = nullptr;      // kept: Abandon has nothing to free
    return made;
}

void* MqttTlsListener::makeContext() {
    _lastError.clear();
    return createContext();
}

void MqttTlsListener::useContext(void* context) {
    if (context == nullptr) return;
    releaseContext();
    _context = context;
}

void* MqttTlsListener::shareContext(void* context) {
    if (context == nullptr) return nullptr;
#if MQTT_TLS_ENGINE_WOLFSSL
    // As in buildContext(): one context per listener on this engine.
    return nullptr;
#else
    return SSL_CTX_up_ref((SSL_CTX*)context) == 1 ? context : nullptr;
#endif
}

void MqttTlsListener::dropContext(void* context) {
    if (context != nullptr) SSL_CTX_free((SSL_CTX*)context);
}

bool MqttTlsListener::begin() {
    end();
    _lastError.clear();
    if (_pool.empty()) configure(_address.c_str(), _port, 32);

    if (!buildContext()) return false;     // _lastError already says why

    mq_socket::ensureWinsock();

    _socket = (int)socket(AF_INET, SOCK_STREAM, 0);
    if (_socket < 0) { fail("no socket could be created"); return false; }

    int reuse = 1;
    setsockopt(_socket, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof reuse);
#if defined(SO_REUSEPORT)
    if (_reusePort) {
        setsockopt(_socket, SOL_SOCKET, SO_REUSEPORT, (const char*)&reuse, sizeof reuse);
    }
#endif

    struct sockaddr_in address;
    memset(&address, 0, sizeof address);
    address.sin_family = AF_INET;
    address.sin_port = htons(_port);
    if (_address.empty()) {
        address.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, _address.c_str(), &address.sin_addr) != 1) {
        fail(_address + " is not an address to listen on");
        MQ_CLOSE(_socket); _socket = -1;
        return false;
    }

    if (::bind(_socket, (struct sockaddr*)&address, sizeof address) != 0) {
        char note[96];
        snprintf(note, sizeof note, "port %u is already in use", (unsigned)_port);
        fail(note);
        MQ_CLOSE(_socket); _socket = -1;
        return false;
    }

    // THE BACKLOG IS THE POOL, not sixteen.
    //
    // This was a fixed 16, which is the number of connections the KERNEL will
    // hold between one accept() and the next. Everything beyond it is dropped
    // before this process ever sees it - so a broker sized for a thousand
    // clients silently refused all but sixteen of them whenever they arrived
    // together, and the clients saw a connection that failed for no stated
    // reason. Measured with mqtt-benchmark: 1000 clients at once left ~300
    // unconnected, and the same thousand ramped over ten seconds connected
    // perfectly.
    //
    // The pool is the right size because it is exactly how many this broker is
    // willing to serve; anything the pool cannot hold is refused deliberately,
    // in MQTT, with a reason - which is a different and far better thing than
    // a TCP connection the kernel dropped. SOMAXCONN caps it, and the kernel
    // clamps silently anyway, so asking for more than it allows is harmless.
    int backlog = (int)_pool.size();
    if (backlog < 16) backlog = 16;
#if defined(SOMAXCONN)
    if (backlog > SOMAXCONN) backlog = SOMAXCONN;
#endif
    if (::listen(_socket, backlog) != 0) {
        fail("the socket would not listen");
        MQ_CLOSE(_socket); _socket = -1;
        return false;
    }

    if (!mq_socket::setNonBlocking(_socket)) {
        fail("the listening socket would not go non-blocking");
        MQ_CLOSE(_socket); _socket = -1;
        return false;
    }

    // What actually got bound, which differs from what was asked for whenever
    // port 0 was requested - how a test gets a free port without racing.
    struct sockaddr_in bound;
    socklen_t boundSize = sizeof bound;
    if (getsockname(_socket, (struct sockaddr*)&bound, &boundSize) == 0) {
        _boundPort = ntohs(bound.sin_port);
    } else {
        _boundPort = _port;
    }
    _spare = mq_socket::openSpareDescriptor();
    return true;
}

void MqttTlsListener::grow(size_t maximum) {
    if (_pool.empty() || maximum + 1 <= _pool.size()) return;
    _servingCapacity = maximum;
    const size_t from = _pool.size();
    _isFree.resize(maximum + 1, 1);
    for (size_t index = from; index < maximum + 1; index++) {
        _pool.push_back(new MqttAcceptedTlsTransport());
        _poolIndex[_pool[index]] = (uint32_t)index;
        _free.push_back((uint32_t)index);
    }
}

void MqttTlsListener::useSocket(int socket, uint16_t port, size_t maximum) {
    end();
    if (socket < 0) return;
    if (_pool.empty()) configure(_address.c_str(), port, maximum);
    _port = port;
    _boundPort = port;
    _socket = socket;
    _spare = mq_socket::openSpareDescriptor();
    _lastError.clear();
}

void MqttTlsListener::end() {
    if (_socket >= 0) {
        MQ_CLOSE(_socket);
        _socket = -1;
    }
    mq_socket::closeSpareDescriptor(_spare);
    // Connections already handed out are NOT closed here, exactly as
    // MqttTcpListener does not close them: the caller still holds them and is
    // entitled to finish what it was saying.
}

MqttTransport* MqttTlsListener::accept() {
    if (_socket < 0 || !_context) return nullptr;

    // A free slot BEFORE the accept, for the reason MqttTcpListener gives:
    // accepting a connection we have nowhere to put means closing it
    // immediately, which looks to the far end exactly like a broker that
    // crashed.
    MqttAcceptedTlsTransport* slot = nullptr;
    uint32_t slotIndex = 0;
    while (!_free.empty()) {
        slotIndex = _free.back();
        if (slotIndex < _pool.size() && !_pool[slotIndex]->inUse()) {
            slot = _pool[slotIndex];
            break;
        }
        // Ended by its holder rather than released; release() restores it.
        _free.pop_back();
        if (slotIndex < _isFree.size()) _isFree[slotIndex] = 0;
    }
    if (slot == nullptr) {
        // One walk before calling the pool full, for entries that came free
        // without release() - as MqttTcpListener::accept() does.
        for (size_t index = _pool.size(); index > 0; index--) {
            if (!_pool[index - 1]->inUse()) giveBack((uint32_t)(index - 1));
        }
        if (_free.empty()) return nullptr;
        slotIndex = _free.back();
        slot = _pool[slotIndex];
    }

    struct sockaddr_in from;
    socklen_t fromSize = sizeof from;
    const int accepted = (int)::accept(_socket, (struct sockaddr*)&from, &fromSize);
    if (accepted < 0) {
        // Out of descriptors: refuse the waiting connection rather than spin
        // on it (mq_socket::shedWaitingConnection).
        if (!MQ_WOULD_BLOCK) mq_socket::shedWaitingConnection(_socket, _spare);
        return nullptr;
    }

    if (!mq_socket::setNonBlocking(accepted)) { MQ_CLOSE(accepted); return nullptr; }
    mq_socket::suppressSigPipe(accepted);

    int nodelay = 1;
    setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, (const char*)&nodelay, sizeof nodelay);

    SSL* ssl = SSL_new((SSL_CTX*)_context);
    if (!ssl) { MQ_CLOSE(accepted); return nullptr; }
    if (!mq_tls::attachSocket(ssl, accepted)) { SSL_free(ssl); MQ_CLOSE(accepted); return nullptr; }

    // Server mode. The handshake itself happens later, one step at a time,
    // driven by the broker's ordinary poll() - accept() never waits for it.
    SSL_set_accept_state(ssl);

    char name[64];
    char host[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &from.sin_addr, host, sizeof host);
    snprintf(name, sizeof name, "%s:%u", host, (unsigned)ntohs(from.sin_port));

    slot->adopt(accepted, ssl, name);
    // Taken only now: most calls find nobody waiting and the slot stays free.
    _free.pop_back();
    _isFree[slotIndex] = 0;
    return slot;
}

void MqttTlsListener::release(MqttTransport* transport) {
    if (transport == nullptr) return;
    const auto found = _poolIndex.find(transport);
    if (found != _poolIndex.end()) {
        _pool[found->second]->end();
        giveBack(found->second);
        return;
    }
    // A transport from somewhere else. Ignored rather than refused, because
    // MqttListener.h says release() must tolerate exactly that so a caller
    // unwinding an error path never has to reason about which listener owns
    // what.
}

size_t MqttTlsListener::capacityRemaining() const {
    size_t free = 0;
    for (const auto* entry : _pool) {
        if (!entry->inUse()) free++;
    }
    // The reserve is not capacity: it exists to refuse, not to serve. Reporting
    // it would tell a caller it had room when the next client will be told no.
    return free > 0 ? free - 1 : 0;
}

size_t MqttTlsListener::active() const {
    size_t busy = 0;
    for (const MqttAcceptedTlsTransport* entry : _pool) if (entry->inUse()) busy++;
    return busy;
}

#endif // MQTT_TLS_SOCKETS

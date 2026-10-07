// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTlsAccept.h"

#if MQTT_TLS_AVAILABLE

// The engine comes in HERE and nowhere a sketch can see it. See the header.
#include "MqttTlsSupport.h"

MqttTlsAcceptContext::~MqttTlsAcceptContext() { end(); }

bool MqttTlsAcceptContext::begin() {
    if (_context != nullptr) return true;
    mq_tls::ensureEngine();

    if (!_identity.valid() && !_identity.generate()) {
        _lastError = _identity.lastError();
        return false;
    }

    SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
    if (ctx == nullptr) { _lastError = mq_tls::engineError(); return false; }
    _context = ctx;

    mq_tls::useChannelIo(ctx);

    if (!loadIdentity()) {
        SSL_CTX_free(ctx);
        _context = nullptr;
        return false;
    }
    return true;
}

void MqttTlsAcceptContext::end() {
    if (_context != nullptr) {
        SSL_CTX_free((SSL_CTX*)_context);
        _context = nullptr;
    }
}

bool MqttTlsAcceptContext::loadIdentity() {
    SSL_CTX* ctx = (SSL_CTX*)_context;
    const std::string& certPem = _identity.certificatePem();
    const std::string& keyPem = _identity.privateKeyPem();
    if (certPem.empty() || keyPem.empty()) {
        _lastError = "the identity has no certificate to serve";
        return false;
    }

#if MQTT_TLS_ENGINE_WOLFSSL
    // THE NATIVE BUFFER API, not BIO and PEM_read_bio_*.
    //
    // wolfSSL has a direct "here is a PEM in memory" call and it is the one
    // embedded builds are meant to use. Going through a BIO instead needs the
    // wider OpenSSL compatibility surface - OPENSSL_ALL rather than
    // OPENSSL_EXTRA - and OPENSSL_ALL force-enables HAVE_SESSION_TICKET, which
    // on a board collides with wolfSSL's own Arduino default of
    // NO_SESSION_CACHE and leaves the server side unlinkable. Two fewer
    // allocations as well.
    if (wolfSSL_CTX_use_certificate_buffer(ctx, (const unsigned char*)certPem.data(),
                                           (long)certPem.size(),
                                           WOLFSSL_FILETYPE_PEM) != WOLFSSL_SUCCESS) {
        _lastError = "the certificate was refused";
        return false;
    }
    if (wolfSSL_CTX_use_PrivateKey_buffer(ctx, (const unsigned char*)keyPem.data(),
                                          (long)keyPem.size(),
                                          WOLFSSL_FILETYPE_PEM) != WOLFSSL_SUCCESS) {
        _lastError = "the private key was refused";
        return false;
    }
    // wolfSSL checks the pair itself on the second call, and says which of the
    // two was wrong - so the explicit comparison the OpenSSL path needs is not
    // needed here.
    return true;
#else
    BIO* certBio = BIO_new_mem_buf(certPem.data(), (int)certPem.size());
    X509* cert = certBio ? PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr) : nullptr;
    if (certBio) BIO_free(certBio);
    if (cert == nullptr) { _lastError = "the certificate would not parse"; return false; }
    const int certOk = SSL_CTX_use_certificate(ctx, cert);
    X509_free(cert);
    if (certOk != 1) { _lastError = "the certificate was refused"; return false; }

    BIO* keyBio = BIO_new_mem_buf(keyPem.data(), (int)keyPem.size());
    EVP_PKEY* key = keyBio ? PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr) : nullptr;
    if (keyBio) BIO_free(keyBio);
    if (key == nullptr) { _lastError = "the private key would not parse"; return false; }

    // Asked before the engine is given the key, for the same reason
    // MqttTlsListener asks: a mismatched pair is refused with an error that
    // says only "key refused", and a certificate and key from two different
    // generations is by far the commonest cause of that.
    if (!mq_tls::keyMatchesCertificate(SSL_CTX_get0_certificate(ctx), key)) {
        EVP_PKEY_free(key);
        _lastError = "the certificate and the private key are not a pair";
        return false;
    }
    const int keyOk = SSL_CTX_use_PrivateKey(ctx, key);
    EVP_PKEY_free(key);
    if (keyOk != 1) { _lastError = "the private key was refused"; return false; }
    return true;
#endif
}

// =============================================================================

MqttTlsAcceptedSession::~MqttTlsAcceptedSession() { end(); }

bool MqttTlsAcceptedSession::begin(MqttTlsAcceptContext& context,
                                 MqttByteChannel* channel) {
    end();
    if (!context.ready() || channel == nullptr) return false;

    SSL* ssl = SSL_new((SSL_CTX*)context.handle());
    if (ssl == nullptr) return false;

    if (!mq_tls::attachChannel(ssl, channel)) {
        SSL_free(ssl);
        return false;
    }

    _ssl = ssl;
    _handshaking = true;
    _failed = false;
    return true;
}

void MqttTlsAcceptedSession::end() {
    if (_ssl != nullptr) { SSL_free((SSL*)_ssl); _ssl = nullptr; }
    _handshaking = false;
    _failed = false;
}

bool MqttTlsAcceptedSession::pump() {
    if (_failed || _ssl == nullptr) return false;
    if (!_handshaking) return true;

    mq_tls::clearErrors();
    const int result = SSL_accept((SSL*)_ssl);
    if (result == 1) { _handshaking = false; return true; }

    const int reason = SSL_get_error((SSL*)_ssl, result);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
        return false;      // ordinary: more bytes have to arrive
    }
    _failed = true;
    return false;
}

size_t MqttTlsAcceptedSession::read(uint8_t* into, size_t length) {
    if (into == nullptr || length == 0) return 0;
    if (!pump()) return 0;
    mq_tls::clearErrors();
    const int got = SSL_read((SSL*)_ssl, into, (int)length);
    if (got > 0) return (size_t)got;
    noteIfFatal(got);
    return 0;
}

size_t MqttTlsAcceptedSession::write(const uint8_t* data, size_t length) {
    if (data == nullptr || length == 0) return 0;
    if (!pump()) return 0;
    mq_tls::clearErrors();
    const int sent = SSL_write((SSL*)_ssl, data, (int)length);
    if (sent > 0) return (size_t)sent;
    noteIfFatal(sent);
    return 0;
}

size_t MqttTlsAcceptedSession::pending() const {
    if (_ssl == nullptr) return 0;
    const int waiting = SSL_pending((SSL*)_ssl);
    return waiting > 0 ? (size_t)waiting : 0;
}

void MqttTlsAcceptedSession::noteIfFatal(int result) {
    if (_ssl == nullptr) { _failed = true; return; }
    const int reason = SSL_get_error((SSL*)_ssl, result);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) return;
    if (reason == SSL_ERROR_NONE) return;
    _failed = true;
}

#endif  // MQTT_TLS_AVAILABLE

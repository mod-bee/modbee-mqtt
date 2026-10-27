// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttTlsTransport.h"

#if MQTT_TLS_SOCKETS


#include "MqttSocketSupport.h"   // setNonBlocking, WIN32-aware
#include "MqttTlsSupport.h"      // ensureEngine, engineError, keyFingerprint

#include <string.h>
#include <errno.h>



MqttTlsTransport::~MqttTlsTransport() { closeAll(); }

void MqttTlsTransport::configure(const char* host, uint16_t port) {
    _host = host ? host : "";
    _port = port;
}

void MqttTlsTransport::setCaFile(const char* path) { _caFile = path ? path : ""; }
void MqttTlsTransport::setCaCertificate(const char* pem) { _caPem = pem ? pem : ""; }

void MqttTlsTransport::setClientCertificate(const char* certificatePath,
                                            const char* keyPath) {
    _certFile = certificatePath ? certificatePath : "";
    _keyFile = keyPath ? keyPath : "";
}

void MqttTlsTransport::setInsecureNoCertificateValidation() { _validate = false; }

void MqttTlsTransport::setPinnedKeyFingerprint(const char* fingerprint) {
    _pin = mq_tls::normaliseFingerprint(fingerprint);
}

// Kept as a member so the call sites below read unchanged, but the words come
// from the shared helper - a client and a listener reporting the same OpenSSL
// failure differently is a difference nobody wants to debug at 2am.
std::string MqttTlsTransport::opensslError() const { return mq_tls::engineError(); }

void MqttTlsTransport::fail(const std::string& what) {
    const std::string detail = opensslError();
    _lastError = detail.empty() ? what : what + ": " + detail;
    closeAll();
    _failed = true;
}

void MqttTlsTransport::closeAll() {
    if (_ssl) {
        // A one-shot shutdown. A full bidirectional close would wait for the
        // peer's own close_notify, and nothing in this library waits.
        SSL_shutdown(((SSL*)_ssl));
        SSL_free(((SSL*)_ssl));
        _ssl = nullptr;
    }
    if (_context) {
        SSL_CTX_free(((SSL_CTX*)_context));
        _context = nullptr;
    }
    if (_socket >= 0) {
        MQ_CLOSE(_socket);
        _socket = -1;
    }
    _addresses.reset();          // candidates belong to one attempt
    _connecting = false;
    _handshaking = false;
    _connected = false;
    _peerClosed = false;
    _verified = false;
    _peerFingerprint.clear();
    _receiveHead = 0;
    _receiveTail = 0;
}

mq_socket::AddressList::Resolution MqttTlsTransport::openSocket() {
    // Every address the name has, not the first one. See mq_socket::AddressList
    // for why the obvious loop was only ever trying one - and looked up WITHOUT
    // WAITING, see mq_socket::NameLookup. A name still resolving is collected
    // by finishTcp() on a later poll.
    const mq_socket::AddressList::Resolution resolution =
        _addresses.start(_host.c_str(), _port);
    if (resolution == mq_socket::AddressList::Unresolvable) {
        _lastError = "cannot resolve " + _host + ": " + _addresses.why();
        _failed = true;
        return resolution;
    }
    if (resolution == mq_socket::AddressList::Resolving) return resolution;
    return nextAddress() ? mq_socket::AddressList::Resolved
                         : mq_socket::AddressList::Unresolvable;
}

// Start connecting to the next candidate, or give up having named how many
// were tried. Called from openSocket() and again from finishTcp() whenever an
// attempt turns out to have failed.
bool MqttTlsTransport::nextAddress() {
    int handle = -1;
    switch (_addresses.connectNext(handle)) {
    case mq_socket::AddressList::Connected:
        _socket = handle;
        _connecting = false;
        return true;
    case mq_socket::AddressList::InProgress:
        _socket = handle;
        _connecting = true;
        return true;
    default:
        break;
    }

    // NAMED, and it used to be silent. A connect that never completed left
    // _lastError empty, so the timeout above it reported a failure with no
    // reason at all - which is how an intermittent public-broker failure went
    // uninvestigated.
    char note[96];
    // Why, from the last address tried (connection refused, timed out, no route): a person acts on that.
    const std::string why = mq_socket::connectErrorWords(_connectError);
    if (!why.empty() && _addresses.tried() <= 1) snprintf(note, sizeof note, ": %s", why.c_str());
    else if (!why.empty()) snprintf(note, sizeof note, ": none of %u address(es) answered (the last: %s)", _addresses.tried(), why.c_str());
    else snprintf(note, sizeof note, ": none of %u address(es) answered",
             _addresses.tried());
    _lastError = "cannot connect to " + _host + note;
    _addresses.reset();
    _socket = -1;
    _connecting = false;
    _failed = true;
    return false;
}

bool MqttTlsTransport::begin() {
    mq_tls::ensureEngine();
    closeAll();
    _lastError.clear();
    _connectError = 0;
    _failed = false;

    if (_host.empty() || _port == 0) {
        _lastError = "no host or port configured";
        return false;
    }

    _context = SSL_CTX_new(TLS_client_method());
    if (!_context) { fail("cannot create a TLS context"); return false; }

    // A peer that goes away must produce an error code, never a signal that
    // terminates a running controller. See MqttTlsSupport.h.
    mq_tls::useNonSignallingIo((SSL_CTX*)_context);

    // TLS 1.2 is the floor. Everything below it has known breaks, and a plant
    // that has to talk to something older should say so deliberately rather
    // than have this library quietly agree to it.
    SSL_CTX_set_min_proto_version((SSL_CTX*)_context, TLS1_2_VERSION);

    // Pinning does its own checking, after the handshake, against the key
    // rather than against an authority - so OpenSSL is told not to verify at
    // all here. Leaving SSL_VERIFY_PEER on with no CA would fail the handshake
    // before there is a certificate to compare.
    if (_validate && !pinning()) {
        if (!_caFile.empty()) {
            if (SSL_CTX_load_verify_locations((SSL_CTX*)_context, _caFile.c_str(), nullptr) != 1) {
                fail("cannot load the CA file " + _caFile);
                return false;
            }
        } else if (!_caPem.empty()) {
            X509_STORE* store = SSL_CTX_get_cert_store((SSL_CTX*)_context);
            BIO* bio = BIO_new_mem_buf(_caPem.data(), (int)_caPem.size());
            bool loaded = false;
            if (bio) {
                X509* certificate = nullptr;
                while ((certificate = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr))) {
                    X509_STORE_add_cert(store, certificate);
                    X509_free(certificate);
                    loaded = true;
                }
                BIO_free(bio);
            }
            if (!loaded) { fail("the CA certificate would not parse"); return false; }
        } else if (SSL_CTX_set_default_verify_paths((SSL_CTX*)_context) != 1) {
            fail("cannot load the system certificate store");
            return false;
        }
        SSL_CTX_set_verify((SSL_CTX*)_context, SSL_VERIFY_PEER, nullptr);
    } else {
        SSL_CTX_set_verify((SSL_CTX*)_context, SSL_VERIFY_NONE, nullptr);
    }

    if (!_certFile.empty()) {
        if (SSL_CTX_use_certificate_file((SSL_CTX*)_context, _certFile.c_str(), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_use_PrivateKey_file((SSL_CTX*)_context, _keyFile.c_str(), SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key((SSL_CTX*)_context) != 1) {
            fail("the client certificate or key would not load");
            return false;
        }
    }

    switch (openSocket()) {
    case mq_socket::AddressList::Unresolvable:
        closeAll();
        return false;
    case mq_socket::AddressList::Resolving:
        // The session is attached once there is a socket to attach it to.
        _connecting = true;
        _handshaking = true;
        return true;
    default:
        return startSession();
    }
}

// The TLS session over the socket nextAddress() opened. Split from begin()
// because with a name still being looked up there is no socket yet, and
// finishTcp() calls this once there is.
bool MqttTlsTransport::startSession() {
    _ssl = SSL_new((SSL_CTX*)_context);
    if (!_ssl || !mq_tls::attachSocket((SSL*)_ssl, _socket)) {
        fail("cannot start a TLS session");
        return false;
    }

    // SNI. Without it a broker serving several names has no way to choose a
    // certificate, and hands over whichever one is first - which then fails
    // hostname verification for reasons that look nothing like the cause.
    SSL_set_tlsext_host_name((SSL*)_ssl, _host.c_str());

    if (_validate && !pinning()) {
        // Hostname verification, which is SEPARATE from chain verification and
        // is the half most often left out. A certificate can be perfectly
        // valid, signed by a CA we trust, and issued to somebody else.
        const mq_tls::HostnameCheck armed =
            mq_tls::expectHostname((SSL*)_ssl, _host.c_str());
        // Deferred rather than skipped - see expectHostname(). wolfSSL will
        // not take a hostname with no dot in it, so the comparison happens
        // against the peer's certificate in finishHandshake() instead.
        _checkHostAfterHandshake = (armed == mq_tls::HostnameCheckDeferred);
        if (armed == mq_tls::HostnameCheckFailed) {
            fail("cannot set the expected hostname");
            return false;
        }
    }

    _handshaking = true;
    return true;
}

bool MqttTlsTransport::finishTcp() {
    if (!_connecting) return true;

    if (_addresses.resolving()) {
        switch (_addresses.collect()) {
        case mq_socket::AddressList::Resolving:
            return false;                       // still looking; ask again
        case mq_socket::AddressList::Resolved:
            if (!nextAddress()) { closeAll(); _failed = true; return false; }
            if (!startSession()) return false;  // fail() has said why
            if (!_connecting) return true;      // connected on the spot
            break;                              // the connect is now under way
        default:
            _lastError = "cannot resolve " + _host + ": " + _addresses.why();
            closeAll();
            _failed = true;
            return false;
        }
    }

    int connectError = 0;
    const mq_socket::ConnectOutcome outcome = mq_socket::connectOutcome(_socket, &connectError);
    if (outcome == mq_socket::ConnectOutcome::Failed && connectError != 0) _connectError = connectError;
    if (outcome == mq_socket::ConnectOutcome::Pending) return false;
    if (outcome == mq_socket::ConnectOutcome::Failed) {
        // THIS ADDRESS failed - not the host. Close it and try the next one;
        // nextAddress() is what finally gives up, and only once every
        // candidate has been tried.
        MQ_CLOSE(_socket);
        _socket = -1;
        return nextAddress() && !_connecting;
    }
    _connecting = false;
    _addresses.reset();          // connected: the rest are not needed
    return true;
}

bool MqttTlsTransport::finishHandshake() {
    if (!_handshaking) return _connected;
    if (!finishTcp()) return false;

    mq_tls::clearErrors();
    const int result = SSL_connect(((SSL*)_ssl));
    if (result == 1) {
        _handshaking = false;
        _connected = true;

        // Always recorded, pinned or not. With no pin set this is what a
        // commissioning run reads out and stores; with one set it is what gets
        // compared. Either way the caller can report what it was actually
        // shown rather than what it hoped for.
        {
            X509* peer = SSL_get_peer_certificate(((SSL*)_ssl));
            _peerFingerprint = mq_tls::keyFingerprint(peer);
            if (peer) X509_free(peer);
        }

        if (pinning()) {
            if (_peerFingerprint.empty()) {
                fail("the broker presented no certificate to pin against");
                return false;
            }
            if (_peerFingerprint != _pin) {
                // Both halves, because the useful question is always which one
                // moved - a broker that was reinstalled, or the wrong address.
                fail("the broker's key is not the pinned one: expected " + _pin +
                     ", got " + _peerFingerprint);
                return false;
            }
            _verified = true;
            return true;
        }

        _verified = !_validate ? false
                               : SSL_get_verify_result(((SSL*)_ssl)) == X509_V_OK;
        if (_validate && !_verified) {
            fail("the peer's certificate did not verify");
            return false;
        }

        // The deferred hostname check. The chain verified above says the
        // certificate is genuine; this says it was issued to the host we
        // asked for, which is a separate question and the one more often
        // skipped.
        if (_validate && _checkHostAfterHandshake &&
            !mq_tls::verifyPeerHostname((SSL*)_ssl, _host.c_str())) {
            fail("the broker's certificate was not issued to " + _host);
            return false;
        }
        return true;
    }

    const int reason = SSL_get_error(((SSL*)_ssl), result);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
        // The handshake is several round trips and this is the ordinary case:
        // it continues on a later poll. Blocking here would stall a scan cycle
        // for as long as a TLS handshake takes, which on a slow link is the
        // worst possible moment.
        return false;
    }

    // A verification failure arrives here rather than as a return of 1, and
    // its reason is worth reporting precisely: "certificate has expired" sends
    // somebody to a different place from "self signed certificate".
    const long verify = SSL_get_verify_result(((SSL*)_ssl));
    if (verify != X509_V_OK) {
        fail(std::string("TLS handshake failed: ") +
             X509_verify_cert_error_string(verify));
    } else {
        fail("TLS handshake failed");
    }
    return false;
}

void MqttTlsTransport::end() { closeAll(); }

bool MqttTlsTransport::hasBufferedInput() const {
    if (_receiveHead < _receiveTail) return true;
    // SSL_pending() is decrypted plaintext already inside the engine, with
    // nothing left on the socket for a readiness set to notice.
    if (_ssl == nullptr) return false;
    return SSL_pending((SSL*)_ssl) > 0;
}

bool MqttTlsTransport::isConnected() const {
    MqttTlsTransport* self = const_cast<MqttTlsTransport*>(this);
    if (self->_handshaking) self->finishHandshake();
    return self->_connected && self->_ssl != nullptr;
}

bool MqttTlsTransport::peerClosed() const { return _peerClosed; }

bool MqttTlsTransport::fill() {
    if (_receiveHead < _receiveTail) return true;
    _receiveHead = 0;
    _receiveTail = 0;
    if (!isConnected()) return false;

    mq_tls::clearErrors();
    const int got = SSL_read(((SSL*)_ssl), _receive, (int)kReceiveBufferSize);
    if (got > 0) {
        _receiveTail = (size_t)got;
        return true;
    }

    const int reason = SSL_get_error(((SSL*)_ssl), got);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) return false;
    if (reason == SSL_ERROR_ZERO_RETURN) {
        // close_notify: the peer has finished. The TLS equivalent of FIN, and
        // the only clean way to learn it.
        _peerClosed = true;
        return false;
    }
    _peerClosed = true;
    fail("TLS connection lost");
    return false;
}

size_t MqttTlsTransport::readable() {
    if (_receiveHead < _receiveTail) return _receiveTail - _receiveHead;
    if (!fill()) return 0;
    return _receiveTail - _receiveHead;
}

size_t MqttTlsTransport::read(uint8_t* into, size_t length) {
    if (into == nullptr || length == 0) return 0;

    // STRAIGHT OUT OF THE BUFFER THIS TRANSPORT ALREADY FILLED. The bytes are
    // here because fill() did one bulk recv() for them; handing them back one
    // at a time through a virtual call - which is what the old interface
    // required - was pure ceremony.
    if (_receiveHead >= _receiveTail && !fill()) return 0;

    const size_t have = _receiveTail - _receiveHead;
    const size_t taken = have < length ? have : length;
    if (taken == 0) return 0;
    memcpy(into, _receive + _receiveHead, taken);
    _receiveHead += taken;
    return taken;
}

size_t MqttTlsTransport::write(const uint8_t* buffer, size_t length) {
    if (!buffer || length == 0 || !isConnected()) return 0;

    mq_tls::clearErrors();
    const int sent = SSL_write(((SSL*)_ssl), buffer, (int)length);
    if (sent > 0) return (size_t)sent;

    const int reason = SSL_get_error(((SSL*)_ssl), sent);
    if (reason == SSL_ERROR_WANT_READ || reason == SSL_ERROR_WANT_WRITE) {
        // Nothing taken. The caller resumes from its cursor on the next poll
        // rather than blocking here - which is also why a renegotiation, which
        // makes a write want to READ, costs nothing but a poll.
        return 0;
    }
    _peerClosed = true;
    fail("TLS connection lost while sending");
    return 0;
}

#endif // MQTT_TLS_SOCKETS

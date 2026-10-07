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
// THE OPENSSL BITS BOTH ENDS OF A TLS CONNECTION NEED
// =============================================================================
//
// A client dialling out and a listener accepting want the same library
// initialisation and the same error strings. The transport had them privately
// and the listener would otherwise have grown a second copy - which is how
// MqttTlsTransport ended up with a POSIX-only setNonBlocking while the two
// socket files had a Win32-aware one. Once is enough.
//
// R1 SAFE: an error string and a one-time init know nothing about whether they
// are serving a client or a broker.
//
// This header is ONLY included from a .cpp. It pulls in <openssl/ssl.h>, and
// MqttTlsTransport.h goes to some trouble to keep that out of the public
// headers by forward-declaring SSL and SSL_CTX - a consumer of this library
// should not need OpenSSL on their include path to read our types.

#include "../MqttFeatures.h"
#include "MqttByteChannel.h"

#if MQTT_TLS_AVAILABLE

// Host only, and only where the socket classes are compiled: a board has no
// <netdb.h> and no <poll.h>, and MqttIdentity needs none of it.
#if MQTT_TLS_SOCKETS
#include "MqttSocketSupport.h"   // send/recv, and MSG_NOSIGNAL where there is one
#endif

// =============================================================================
// ONE ENGINE OR THE OTHER
// =============================================================================
//
// wolfSSL's compatibility layer is genuinely a compatibility layer: everything
// the transports, the listener and the identity need is there under its
// OpenSSL name, certificate WRITING included - X509_new, X509V3_EXT_conf_nid,
// X509_sign, EVP_PKEY keygen, i2d_X509_PUBKEY, the lot. That was measured
// against wolfSSL 5.9.2 rather than assumed, and it is why there is one set of
// transport code here and not two.
//
// EXACTLY TWO THINGS DIFFER, and both are below rather than scattered through
// the .cpp files:
//
//   1. Attaching a socket without SIGPIPE. OpenSSL needs a custom BIO method,
//      because its own socket BIO writes with write(2). wolfSSL has no BIO
//      method machinery to build one with - and does not need it, because
//      replacing its I/O is a first-class thing to do there.
//
//   2. Hostname verification. OpenSSL is SSL_set1_host; wolfSSL is
//      wolfSSL_check_domain_name. Same check, different spelling.
#if MQTT_TLS_ENGINE_WOLFSSL
    // THE CONFIGURATION HAS TO BE IN SCOPE BEFORE ANY OTHER WOLFSSL HEADER, and
    // there are two ways it can arrive:
    //
    //   an installed wolfSSL   <wolfssl/options.h>, written by its build
    //   a vendored one         user_settings.h, read by settings.h when
    //                          WOLFSSL_USER_SETTINGS is defined
    //
    // Getting this wrong does not fail to compile. It compiles with a
    // DIFFERENT feature set from the one the library was built with, and the
    // symptom is a structure whose size disagrees between two translation
    // units - which presents as memory corruption a long way from here.
    #if __has_include(<wolfssl/options.h>)
        #include <wolfssl/options.h>
    #else
        #ifndef WOLFSSL_USER_SETTINGS
            #define WOLFSSL_USER_SETTINGS
        #endif
        #include <wolfssl/wolfcrypt/settings.h>
    #endif
    #include <wolfssl/ssl.h>
    #include <wolfssl/error-ssl.h>
    // wolfio.h declares the I/O callback types, and to do it it needs the
    // platform's socket types - SOCKADDR among them. A board has no such
    // header, and MqttIdentity has no use for one either: it is pure
    // cryptography. So it comes in only where sockets do.
    #if MQTT_TLS_SOCKETS
        #include <wolfssl/wolfio.h>
    #endif
    #include <wolfssl/openssl/ssl.h>
    #include <wolfssl/openssl/err.h>
    #include <wolfssl/openssl/x509.h>
    #include <wolfssl/openssl/x509v3.h>
    #include <wolfssl/openssl/pem.h>
    #include <wolfssl/openssl/evp.h>
    #include <wolfssl/openssl/bn.h>
    #include <wolfssl/openssl/rand.h>
    #include <wolfssl/openssl/sha.h>
    #include <wolfssl/wolfcrypt/asn_public.h>   /* wc_GetSubjectPubKeyInfoDerFromCert */
    #include <wolfssl/wolfcrypt/hash.h>         /* wc_Sha256Hash */
#else
    #include <openssl/ssl.h>
    #include <openssl/bio.h>
    #include <openssl/err.h>
    #include <openssl/x509.h>
    #include <openssl/x509v3.h>
    #include <openssl/pem.h>
    #include <openssl/evp.h>
    #include <openssl/bn.h>
    #include <openssl/rand.h>
    #include <openssl/sha.h>
#endif

#include <string>
#include <vector>
#include <string.h>
#include <new>

namespace mq_tls {

/** The engine initialised once per process. Thread-safe since C++11. */
inline void ensureEngine() {
    static const bool ready = [] {
#if MQTT_TLS_ENGINE_WOLFSSL
        wolfSSL_Init();
#else
        SSL_library_init();
        SSL_load_error_strings();
        OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, nullptr);
#endif
        return true;
    }();
    (void)ready;
}

/** The thread's OpenSSL error queue, emptied.
 *
 *  MUST be called immediately before every SSL_accept, SSL_connect, SSL_read
 *  and SSL_write. This is not tidiness, it is correctness, and OpenSSL's own
 *  manual says so: "the current thread's error queue must be empty before the
 *  TLS/SSL I/O operation is attempted, or SSL_get_error() will not work
 *  reliably."
 *
 *  WHAT IT COSTS TO SKIP IT, measured rather than imagined. The queue is
 *  per-THREAD, not per-connection, and a broker services every connection on
 *  one thread. So one client that fails - a wrong pin, a scanner, a device
 *  that lost power - leaves entries behind, and the NEXT connection's
 *  SSL_read returns -1 meaning "nothing to read yet" while SSL_get_error
 *  reads the stale entry and answers SSL_ERROR_SSL. The library then reports a
 *  healthy connection as lost, the broker closes it and publishes its Will.
 *
 *  That is exactly what test_tls_server caught: a wrong-pin client that
 *  disconnected took a completely unrelated connection down with it. */
inline void clearErrors() { ERR_clear_error(); }

/** The top of OpenSSL's error queue as text, or "" when it is empty.
 *
 *  One entry reported, not the whole queue: the first is the one that
 *  describes what went wrong and the rest are the unwinding. An operator
 *  reading "certificate verify failed" does not want four more lines under it.
 *
 *  The rest are DISCARDED rather than left, for the reason above - anything
 *  still queued when the next connection does its I/O is read as that
 *  connection's failure. */
inline std::string engineError() {
    const unsigned long code = ERR_get_error();
    if (code == 0) return std::string();
    char buffer[256];
    ERR_error_string_n(code, buffer, sizeof buffer);
    ERR_clear_error();
    return std::string(buffer);
}

/** SHA-256 of a certificate's DER SubjectPublicKeyInfo, as 64 lowercase
 *  hexadecimal characters. Empty if there is no certificate.
 *
 *  Both ends need the identical answer or pinning does not work at all: the
 *  broker prints this for an engineer to write down and the client compares
 *  what it was told against what it was shown. One implementation, so the two
 *  cannot drift.
 *
 *  It is of the KEY, not of the certificate, so it survives the certificate
 *  being reissued from the same key - RFC 7469's reasoning, and the reason a
 *  pinned plant link does not break the day a device renews itself. */
inline std::string keyFingerprint(X509* certificate) {
    if (certificate == nullptr) return std::string();

    unsigned char digest[SHA256_DIGEST_LENGTH];
#if MQTT_TLS_ENGINE_WOLFSSL
    // WOLFSSL: THE NATIVE ROUTE, from the certificate's own DER. The OpenSSL
    // names below go through wolfSSL's EC_KEY and BIGNUM objects, which need
    // its general big-number maths - and a board's wolfSSL is built with only
    // the small P-256 maths (user_settings.h, the board profile), where that
    // path returns nothing and the identity would not serialise. These bytes
    // are the same SubjectPublicKeyInfo either way, so the fingerprint - and
    // every pin already written down - is unchanged.
    int certLength = 0;
    const unsigned char* certDer = wolfSSL_X509_get_der(certificate, &certLength);
    if (certDer == nullptr || certLength <= 0) return std::string();
    word32 spkiLength = 0;
    if (wc_GetSubjectPubKeyInfoDerFromCert(certDer, (word32)certLength, nullptr, &spkiLength) != 0 ||
        spkiLength == 0) {
        return std::string();
    }
    std::vector<unsigned char> spki(spkiLength);
    if (wc_GetSubjectPubKeyInfoDerFromCert(certDer, (word32)certLength, spki.data(), &spkiLength) != 0 ||
        wc_Sha256Hash(spki.data(), spkiLength, digest) != 0) {
        return std::string();
    }
#else
    X509_PUBKEY* spki = X509_get_X509_PUBKEY(certificate);
    if (spki == nullptr) return std::string();

    unsigned char* der = nullptr;
    const int length = i2d_X509_PUBKEY(spki, &der);
    if (length <= 0 || der == nullptr) return std::string();

    SHA256(der, (size_t)length, digest);
    OPENSSL_free(der);
#endif

    static const char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(SHA256_DIGEST_LENGTH * 2);
    for (size_t i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        out.push_back(kHex[digest[i] >> 4]);
        out.push_back(kHex[digest[i] & 0x0F]);
    }
    return out;
}

/** Whether a private key is the one this certificate was issued for.
 *
 *  NOT X509_check_private_key, which is the obvious call and is WRONG on one
 *  of the two engines. wolfSSL 5.9.2 answers success for a certificate and a
 *  key that have nothing to do with each other - measured, by handing it a
 *  mismatched pair and watching it agree. Left in place, a broker would start
 *  with a key that cannot sign anything the certificate promises, and fail at
 *  the first handshake with an error about a decrypt failure that names
 *  nothing.
 *
 *  Nor i2d_PUBKEY on the private key, which was the next thing tried: wolfSSL
 *  returns zero bytes for a private EVP_PKEY, so every pair would have looked
 *  mismatched and no broker would have started at all.
 *
 *  EVP_PKEY_cmp compares the PUBLIC halves and is correct on both - 1 for the
 *  same key, 0 for a different one - so that is what is used. */
inline bool keyMatchesCertificate(X509* certificate, EVP_PKEY* key) {
    if (certificate == nullptr || key == nullptr) return false;

    EVP_PKEY* fromCertificate = X509_get_pubkey(certificate);
    if (fromCertificate == nullptr) return false;

    // EVP_PKEY_eq is the same function under the name OpenSSL 3.0 renamed it
    // to; the old spelling still works there but warns, and the broker image
    // builds with -Werror. wolfSSL has only the old name.
#if !MQTT_TLS_ENGINE_WOLFSSL && OPENSSL_VERSION_NUMBER >= 0x30000000L
    const int same = EVP_PKEY_eq(fromCertificate, key);
#else
    const int same = EVP_PKEY_cmp(fromCertificate, key);
#endif
    EVP_PKEY_free(fromCertificate);
    return same == 1;
}

/** A fingerprint as somebody typed it, reduced to what can be compared.
 *
 *  Colons and spaces come off and case is flattened, because every tool that
 *  prints a fingerprint prints it differently - `openssl x509 -fingerprint`
 *  uses upper case with colons, this library prints lower case without - and a
 *  plant link that refuses to come up because of punctuation would be blamed
 *  on TLS rather than on formatting. */
inline std::string normaliseFingerprint(const char* text) {
    std::string out;
    if (text == nullptr) return out;
    for (const char* p = text; *p != '\0'; p++) {
        const char c = *p;
        if (c == ':' || c == ' ' || c == '-' || c == '\t') continue;
        if (c >= 'A' && c <= 'F') out.push_back((char)(c - 'A' + 'a'));
        else out.push_back(c);
    }
    return out;
}

// =============================================================================
// A SOCKET OPENSSL CANNOT KILL THE PROCESS THROUGH
// =============================================================================
//
// OpenSSL's own socket BIO sends with write(2). Writing to a socket whose peer
// has gone raises SIGPIPE, whose default disposition terminates the process -
// so a broker that a client walked away from mid-handshake took the whole
// runtime down with it. That is not a TLS fault and it is not rare: it is
// every reset connection, every port scanner, every device that lost power
// while connected.
//
// MqttTcpTransport already avoids it by sending with MSG_NOSIGNAL, and the TLS
// path must too - but the send is inside OpenSSL, so the flag has to be put
// where OpenSSL will reach it. This is a BIO that is the stock socket BIO in
// every respect except that its read and write go through recv() and send()
// with MSG_NOSIGNAL set.
//
// THE ALTERNATIVE, AND WHY NOT. The usual advice is signal(SIGPIPE, SIG_IGN),
// and it works - but it is a process-wide change made by a library the
// application merely linked. A controller whose own code relies on SIGPIPE
// would have that taken away from it silently, by something it asked only to
// speak MQTT. Fifty lines here is the smaller imposition.

// =============================================================================
// TLS OVER SOMETHING THAT IS NOT A SOCKET
// =============================================================================
//
// A board has no file descriptor to hand an SSL object. It has a WiFiClient or
// an EthernetClient - a thing with read(), write() and connected() and no
// number behind it - so everything below the engine has to go through a call
// rather than a descriptor.
//
// This is what lets an ESP32 SERVE TLS rather than only dial it. The engine,
// the certificate generation and the handshake state machine were already
// platform-neutral; a socket was the only reason the listener was not.
//
// It is also why it is written as an interface rather than inline in the
// Arduino listener: with the channel abstracted, the same listener compiles on
// a host against a test double, so the TLS server path is exercised here
// rather than only on hardware nobody can run in CI.

// Declared in MqttByteChannel.h, which deliberately has no engine types in it
// so a public header can name it without dragging wolfSSL's OpenSSL
// compatibility layer - and its SHA_CTX - into an ESP32 sketch.
typedef MqttByteChannel ByteChannel;

#if MQTT_TLS_ENGINE_WOLFSSL

// wolfSSL asks for I/O through callbacks when WOLFSSL_USER_IO is set, which is
// exactly how the vendored Arduino build is configured - it has no socket
// layer compiled in at all.
inline int channelSendCallback(WOLFSSL*, char* data, int length, void* context) {
    ByteChannel* channel = (ByteChannel*)context;
    if (channel == nullptr) return WOLFSSL_CBIO_ERR_GENERAL;
    const int sent = channel->channelSend((const uint8_t*)data, length);
    if (sent > 0) return sent;
    if (sent == 0) return WOLFSSL_CBIO_ERR_WANT_WRITE;
    return WOLFSSL_CBIO_ERR_CONN_CLOSE;
}

inline int channelRecvCallback(WOLFSSL*, char* into, int length, void* context) {
    ByteChannel* channel = (ByteChannel*)context;
    if (channel == nullptr) return WOLFSSL_CBIO_ERR_GENERAL;
    const int got = channel->channelRecv((uint8_t*)into, length);
    if (got > 0) return got;
    if (got == 0) return WOLFSSL_CBIO_ERR_WANT_READ;
    return WOLFSSL_CBIO_ERR_CONN_CLOSE;
}

inline void useChannelIo(SSL_CTX* context) {
    if (context == nullptr) return;
    wolfSSL_CTX_SetIOSend(context, channelSendCallback);
    wolfSSL_CTX_SetIORecv(context, channelRecvCallback);
}

inline bool attachChannel(SSL* ssl, ByteChannel* channel) {
    if (ssl == nullptr || channel == nullptr) return false;
    wolfSSL_SetIOWriteCtx(ssl, channel);
    wolfSSL_SetIOReadCtx(ssl, channel);
    return true;
}

#else   // OpenSSL

// OpenSSL asks through a BIO, so the channel is carried as the BIO's data -
// the same shape as the non-signalling socket BIO above, with a call where
// that one has a descriptor.
namespace detail {

inline ByteChannel* channelOf(BIO* bio) {
    return bio ? (ByteChannel*)BIO_get_data(bio) : nullptr;
}

inline int channelBioWrite(BIO* bio, const char* data, int length) {
    ByteChannel* channel = channelOf(bio);
    BIO_clear_retry_flags(bio);
    if (channel == nullptr) return -1;
    const int sent = channel->channelSend((const uint8_t*)data, length);
    if (sent > 0) return sent;
    if (sent == 0) { BIO_set_retry_write(bio); return -1; }
    return -1;
}

inline int channelBioRead(BIO* bio, char* into, int length) {
    ByteChannel* channel = channelOf(bio);
    BIO_clear_retry_flags(bio);
    if (channel == nullptr) return -1;
    const int got = channel->channelRecv((uint8_t*)into, length);
    if (got > 0) return got;
    if (got == 0) { BIO_set_retry_read(bio); return -1; }
    return 0;    // the channel is gone: zero is end of stream, not a retry
}

inline long channelBioCtrl(BIO*, int command, long, void*) {
    switch (command) {
        case BIO_CTRL_FLUSH: return 1;    // nothing is buffered here
        case BIO_CTRL_DUP:   return 1;
        default:             return 0;
    }
}

inline int channelBioCreate(BIO* bio) {
    BIO_set_data(bio, nullptr);
    BIO_set_init(bio, 1);
    BIO_set_flags(bio, 0);
    return 1;
}

inline int channelBioDestroy(BIO* bio) {
    if (bio == nullptr) return 0;
    // The channel is the transport's, not this BIO's. Nothing to free.
    BIO_set_data(bio, nullptr);
    BIO_set_init(bio, 0);
    return 1;
}

inline BIO_METHOD* channelMethod() {
    static BIO_METHOD* method = [] () -> BIO_METHOD* {
        BIO_METHOD* made = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_SOURCE_SINK,
                                        "modbee channel");
        if (made == nullptr) return nullptr;
        BIO_meth_set_write(made, channelBioWrite);
        BIO_meth_set_read(made, channelBioRead);
        BIO_meth_set_ctrl(made, channelBioCtrl);
        BIO_meth_set_create(made, channelBioCreate);
        BIO_meth_set_destroy(made, channelBioDestroy);
        return made;
    }();
    return method;
}

} // namespace detail

inline void useChannelIo(SSL_CTX*) { /* per-connection on OpenSSL; see below */ }

inline bool attachChannel(SSL* ssl, ByteChannel* channel) {
    if (ssl == nullptr || channel == nullptr) return false;
    BIO_METHOD* method = detail::channelMethod();
    if (method == nullptr) return false;
    BIO* bio = BIO_new(method);
    if (bio == nullptr) return false;
    BIO_set_data(bio, channel);
    SSL_set_bio(ssl, bio, bio);    // one BIO for both directions; SSL owns it
    return true;
}

#endif  // engine

#if MQTT_TLS_SOCKETS

namespace detail {

// errno is cleared before every socket call, because what reads it afterwards -
// BIO_sock_should_retry on OpenSSL, MQ_WOULD_BLOCK on wolfSSL - cannot tell a
// fresh value from one an unrelated failure left behind, and a stale one turns
// "nothing to read yet" into "this connection is broken". OpenSSL's own socket
// BIO does exactly this, for exactly this reason.
inline void clearSocketError() {
#if defined(_WIN32)
    WSASetLastError(0);
#else
    errno = 0;
#endif
}

} // namespace detail

#if !MQTT_TLS_ENGINE_WOLFSSL

namespace detail {

// The socket, and whether this BIO is the one that closes it. OpenSSL 3 made
// BIO fields opaque, so what the stock socket BIO keeps in `num` and
// `shutdown` is kept here instead, hung off the BIO's data pointer.
struct SocketState {
    int descriptor = -1;
    int closeOnDestroy = 1;
};

inline SocketState* stateOf(BIO* bio) {
    return bio ? (SocketState*)BIO_get_data(bio) : nullptr;
}

inline int bioSocket(BIO* bio) {
    const SocketState* state = stateOf(bio);
    return state ? state->descriptor : -1;
}

inline int nonSignallingWrite(BIO* bio, const char* data, int length) {
    if (bio == nullptr || data == nullptr || length <= 0) return 0;

    clearSocketError();
    BIO_clear_retry_flags(bio);
#if defined(MSG_NOSIGNAL)
    const int sent = (int)::send(bioSocket(bio), data, (size_t)length, MSG_NOSIGNAL);
#else
    // Windows never raises SIGPIPE, and macOS is covered by SO_NOSIGPIPE on
    // the socket itself, so there is nothing to ask for here.
    const int sent = (int)::send(bioSocket(bio), data, length, 0);
#endif
    // A non-blocking socket that would have waited is not an error, and saying
    // so through the retry flag is the whole reason a BIO has them: OpenSSL
    // turns it into SSL_ERROR_WANT_WRITE rather than into a broken session.
    if (sent <= 0 && BIO_sock_should_retry(sent)) BIO_set_retry_write(bio);
    return sent;
}

inline int nonSignallingRead(BIO* bio, char* into, int length) {
    if (bio == nullptr || into == nullptr || length <= 0) return 0;

    clearSocketError();
    BIO_clear_retry_flags(bio);
    const int got = (int)::recv(bioSocket(bio), into, (size_t)length, 0);
    if (got <= 0) {
        if (BIO_sock_should_retry(got)) BIO_set_retry_read(bio);
        // Zero is the peer closing, which is not a retry and not a fault.
        // OpenSSL asks about it through BIO_CTRL_EOF, and answering wrongly
        // turns a clean goodbye into a reported error.
        else if (got == 0) BIO_set_flags(bio, BIO_FLAGS_IN_EOF);
    }
    return got;
}

inline int nonSignallingPuts(BIO* bio, const char* text) {
    if (text == nullptr) return 0;
    return nonSignallingWrite(bio, text, (int)strlen(text));
}

inline int nonSignallingCreate(BIO* bio) {
    SocketState* state = new (std::nothrow) SocketState();
    if (state == nullptr) return 0;
    BIO_set_data(bio, state);
    BIO_set_init(bio, 0);
    BIO_set_flags(bio, 0);
    return 1;
}

inline int nonSignallingDestroy(BIO* bio) {
    if (bio == nullptr) return 0;
    SocketState* state = stateOf(bio);
    if (state != nullptr) {
        // Only if this BIO was told it owns the socket. Both transports here
        // pass BIO_NOCLOSE and close their own, so this normally does nothing
        // - but a BIO that closed a descriptor its owner had already closed
        // and the process had already reissued would take down an unrelated
        // connection, which is a fault worth being careful about.
        if (state->closeOnDestroy && BIO_get_init(bio) && state->descriptor >= 0) {
            MQ_CLOSE(state->descriptor);
        }
        delete state;
        BIO_set_data(bio, nullptr);
    }
    BIO_set_init(bio, 0);
    return 1;
}

// Only what an SSL object over a socket actually asks for. Everything else
// answers zero, which is what the stock socket BIO does with anything it does
// not recognise.
inline long nonSignallingCtrl(BIO* bio, int command, long argument, void* pointer) {
    SocketState* state = stateOf(bio);
    if (state == nullptr) return 0;

    switch (command) {
    case BIO_C_SET_FD:
        if (pointer == nullptr) return 0;
        state->descriptor = *(int*)pointer;
        state->closeOnDestroy = (int)argument;
        BIO_set_init(bio, 1);
        return 1;

    case BIO_C_GET_FD:
        if (!BIO_get_init(bio)) return -1;
        if (pointer != nullptr) *(int*)pointer = state->descriptor;
        return state->descriptor;

    case BIO_CTRL_GET_CLOSE:
        return state->closeOnDestroy;

    case BIO_CTRL_SET_CLOSE:
        state->closeOnDestroy = (int)argument;
        return 1;

    case BIO_CTRL_EOF:
        return (BIO_test_flags(bio, BIO_FLAGS_IN_EOF) != 0) ? 1 : 0;

    // There is no buffer here, so a flush has nothing to do and must still say
    // it succeeded - SSL_shutdown flushes, and a zero would read as a failure.
    case BIO_CTRL_FLUSH:
    case BIO_CTRL_DUP:
        return 1;

    default:
        return 0;
    }
}

} // namespace detail

/** The BIO method above, built once. Null if OpenSSL would not make one,
 *  which the caller treats as "use the stock socket BIO" rather than as a
 *  failure: SIGPIPE is a risk worth taking over refusing to connect at all. */
inline BIO_METHOD* nonSignallingSocketMethod() {
    static BIO_METHOD* method = [] () -> BIO_METHOD* {
        BIO_METHOD* made = BIO_meth_new(
            BIO_get_new_index() | BIO_TYPE_SOURCE_SINK | BIO_TYPE_DESCRIPTOR,
            "modbee non-signalling socket");
        if (made == nullptr) return nullptr;

        // Written out rather than borrowed from BIO_s_socket. Copying its
        // function pointers needs BIO_meth_get_ctrl and friends, which OpenSSL
        // 3.5 deprecated - and the broker image builds with -Werror, so a
        // deprecation is a build failure rather than a note. Six small
        // functions is a smaller liability than an API on its way out.
        BIO_meth_set_write(made, detail::nonSignallingWrite);
        BIO_meth_set_read(made, detail::nonSignallingRead);
        BIO_meth_set_puts(made, detail::nonSignallingPuts);
        BIO_meth_set_ctrl(made, detail::nonSignallingCtrl);
        BIO_meth_set_create(made, detail::nonSignallingCreate);
        BIO_meth_set_destroy(made, detail::nonSignallingDestroy);
        return made;
    }();
    return method;
}

#endif // !MQTT_TLS_ENGINE_WOLFSSL

/** Give an SSL object its socket, without the SIGPIPE.
 *
 *  Replaces SSL_set_fd, which would attach the stock socket BIO and with it the
 *  SIGPIPE. Both ends of the library use this and neither uses SSL_set_fd. */
inline bool attachSocket(SSL* ssl, int descriptor) {
    if (ssl == nullptr || descriptor < 0) return false;

#if MQTT_TLS_ENGINE_WOLFSSL
    // wolfSSL has no BIO method machinery to build a custom socket BIO with,
    // and does not need one: replacing its I/O is a first-class operation.
    // wolfSSL_set_fd sets the read and write contexts to the descriptor, which
    // is what the callbacks below are handed - exactly as wolfSSL's own
    // EmbedSend and EmbedReceive read it.
    return wolfSSL_set_fd(ssl, descriptor) == WOLFSSL_SUCCESS;
#else
    BIO_METHOD* method = nonSignallingSocketMethod();
    if (method == nullptr) return SSL_set_fd(ssl, descriptor) == 1;

    BIO* bio = BIO_new(method);
    if (bio == nullptr) return false;

    // BIO_NOCLOSE: the transport closes its own socket, and a BIO that closed
    // it as well would close a descriptor the process had already reissued.
    BIO_set_fd(bio, descriptor, BIO_NOCLOSE);

    // Both directions on one BIO. SSL_set_bio takes ownership of it either
    // way, so nothing here frees it - SSL_free does.
    SSL_set_bio(ssl, bio, bio);
    return true;
#endif
}

#if MQTT_TLS_ENGINE_WOLFSSL
namespace detail {

// wolfSSL's I/O callbacks. Same job as the BIO above and the same reasoning:
// the peer going away must produce an error code, never a signal that
// terminates a running controller.
inline int wolfSend(WOLFSSL*, char* data, int length, void* context) {
    if (data == nullptr || length <= 0 || context == nullptr) {
        return WOLFSSL_CBIO_ERR_GENERAL;
    }
    const int descriptor = *(int*)context;

    clearSocketError();
#if defined(MSG_NOSIGNAL)
    const int sent = (int)::send(descriptor, data, (size_t)length, MSG_NOSIGNAL);
#else
    const int sent = (int)::send(descriptor, data, length, 0);
#endif
    if (sent > 0) return sent;
    if (sent == 0) return WOLFSSL_CBIO_ERR_CONN_CLOSE;
    if (MQ_WOULD_BLOCK) return WOLFSSL_CBIO_ERR_WANT_WRITE;
#if !defined(_WIN32)
    if (errno == EPIPE) return WOLFSSL_CBIO_ERR_CONN_CLOSE;
    if (errno == ECONNRESET) return WOLFSSL_CBIO_ERR_CONN_RST;
    if (errno == EINTR) return WOLFSSL_CBIO_ERR_ISR;
#endif
    return WOLFSSL_CBIO_ERR_GENERAL;
}

inline int wolfReceive(WOLFSSL*, char* into, int length, void* context) {
    if (into == nullptr || length <= 0 || context == nullptr) {
        return WOLFSSL_CBIO_ERR_GENERAL;
    }
    const int descriptor = *(int*)context;

    clearSocketError();
    const int got = (int)::recv(descriptor, into, (size_t)length, 0);
    if (got > 0) return got;
    // Zero is the peer closing cleanly. wolfSSL turns this into a clean
    // shutdown rather than an error, which is what it is.
    if (got == 0) return WOLFSSL_CBIO_ERR_CONN_CLOSE;
    if (MQ_WOULD_BLOCK) return WOLFSSL_CBIO_ERR_WANT_READ;
#if !defined(_WIN32)
    if (errno == ECONNRESET) return WOLFSSL_CBIO_ERR_CONN_RST;
    if (errno == EINTR) return WOLFSSL_CBIO_ERR_ISR;
#endif
    return WOLFSSL_CBIO_ERR_GENERAL;
}

} // namespace detail
#endif

/** Point a context's I/O at the callbacks above.
 *
 *  Called once per SSL_CTX, right after it is made. A no-op on OpenSSL, where
 *  the equivalent is attached per connection by attachSocket(). */
inline void useNonSignallingIo(SSL_CTX* context) {
#if MQTT_TLS_ENGINE_WOLFSSL
    if (context == nullptr) return;
    wolfSSL_SetIOSend(context, detail::wolfSend);
    wolfSSL_SetIORecv(context, detail::wolfReceive);
#else
    (void)context;
#endif
}

/** Verify that the certificate was issued to the host we asked for.
 *
 *  SEPARATE from chain verification and the half most often left out: a
 *  certificate can be perfectly valid, signed by a CA we trust, and issued to
 *  somebody else entirely. */
enum HostnameCheck {
    HostnameCheckFailed = 0,    ///< refuse the connection
    HostnameCheckArmed = 1,     ///< the engine will check it during the handshake
    HostnameCheckDeferred = 2,  ///< we must check it ourselves afterwards
};

inline HostnameCheck expectHostname(SSL* ssl, const char* host) {
    if (ssl == nullptr || host == nullptr || *host == '\0') {
        return HostnameCheckFailed;
    }
#if MQTT_TLS_ENGINE_WOLFSSL
    if (wolfSSL_check_domain_name(ssl, host) == WOLFSSL_SUCCESS) {
        return HostnameCheckArmed;
    }

    // WOLFSSL REFUSES A NAME WITH NO DOT IN IT, and a plant is full of them.
    //
    // wolfSSL_check_domain_name() (ssl.c) rejects anything that is not a valid
    // FQDN unless it is exactly "localhost":
    //
    //     if ((!wolfssl_local_IsValidFQDN(dn, dn_len)) &&
    //         (XSTRCMP(dn, "localhost") != 0))
    //         return WOLFSSL_FAILURE;
    //
    // and IsValidFQDN is `labelCount > 1 && curLabelHasAlpha` - it requires a
    // dot. So `plc-broker`, `scada1`, a container name, a Windows machine name
    // are all refused before a byte is sent, and the handshake is abandoned
    // with an alert the broker reports as "unexpected message". OpenSSL's
    // SSL_set1_host() accepts a single label without complaint, so the library
    // worked on every developer's machine and failed on every controller.
    //
    // THE ANSWER IS NOT TO SKIP THE CHECK. A certificate can be perfectly
    // valid, signed by a CA we trust, and issued to somebody else; dropping
    // hostname verification to make a connection succeed is how that goes
    // unnoticed. So the check is DEFERRED to just after the handshake, where
    // wolfSSL_X509_check_host() will do it against the peer's certificate -
    // the same comparison, without the FQDN restriction.
    return HostnameCheckDeferred;
#else
    // No partial wildcards: *.example.com is a certificate for a domain,
    // while w*.example.com is a certificate for an argument.
    SSL_set_hostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    return SSL_set1_host(ssl, host) == 1 ? HostnameCheckArmed
                                         : HostnameCheckFailed;
#endif
}

/** The deferred half of the check above, run once the peer has presented its
 *  certificate. Only wolfSSL ever needs it. */
inline bool verifyPeerHostname(SSL* ssl, const char* host) {
    if (ssl == nullptr || host == nullptr || *host == '\0') return false;
#if MQTT_TLS_ENGINE_WOLFSSL
    X509* peer = SSL_get_peer_certificate(ssl);
    if (peer == nullptr) return false;

    // FLAGS ZERO, AND NOT BECAUSE NONE WERE WANTED.
    //
    // wolfSSL_X509_check_host() (x509.c) refuses every flag it is given:
    //
    //     if (flags & WOLFSSL_NO_PARTIAL_WILDCARDS) {
    //         WOLFSSL_MSG("... not yet implemented");
    //         return WOLFSSL_FAILURE;
    //     }
    //
    // so asking for the strictness OpenSSL applies turns the check into an
    // outright failure and the connection is refused with a message saying the
    // certificate was issued to somebody else - which is not what happened.
    //
    // The difference this leaves: on OpenSSL a certificate for w*.example.com
    // is rejected for example.com, and on wolfSSL its own matching decides.
    // Said here rather than left as a surprise, because the two engines are
    // meant to fail the same way and on this one point they do not.
    //
    // Length passed explicitly rather than 0, which wolfSSL treats as "call
    // strlen" - the same answer, and being explicit costs nothing.
    const int ok = wolfSSL_X509_check_host(peer, host, strlen(host), 0,
                                           nullptr);
    X509_free(peer);
    return ok == WOLFSSL_SUCCESS;
#else
    // OpenSSL armed the check before the handshake and has already applied it.
    (void)ssl;
    (void)host;
    return true;
#endif
}

#endif // MQTT_TLS_SOCKETS

} // namespace mq_tls

#endif // MQTT_TLS_AVAILABLE

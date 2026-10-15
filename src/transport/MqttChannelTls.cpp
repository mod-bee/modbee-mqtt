// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttChannelTls.h"

#include <cstdlib>

#if MQTT_CHANNEL_TLS

// The engine comes in HERE and nowhere a sketch can see it. See the header.
#include <mbedtls/ssl.h>
#include <mbedtls/ssl_ciphersuites.h>
#include <mbedtls/asn1.h>        // these six for their *_ALLOC_FAILED codes only
#include <mbedtls/bignum.h>
#include <mbedtls/cipher.h>
#include <mbedtls/ecp.h>
#include <mbedtls/md.h>
#include <mbedtls/pem.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/net_sockets.h>   // MBEDTLS_ERR_NET_CONN_RESET, a constant only
#include <mbedtls/pk.h>
#include <mbedtls/platform.h>
#include <mbedtls/sha256.h>
#include <mbedtls/x509_crt.h>

#if defined(ARDUINO_ARCH_ESP32) && !defined(MQTT_CHANNEL_TLS_INTERNAL_RAM)
#include <esp_heap_caps.h>
#define MQ_CHANNEL_TLS_PSRAM 1
#endif

#include <new>
#include <stdio.h>
#include <string.h>
#include "../MqttNoExcept.h"

// =============================================================================
// WHAT IS OFFERED
// =============================================================================
//
// THE CIPHER SUITES, NAMED RATHER THAN DEFAULTED. mbedTLS's default list is
// every suite its configuration compiled - on the ESP32 core well over fifty,
// PSK and CCM and CBC-SHA1 among them - which is a 400-byte ClientHello on a
// metered modem link and an offer nobody here intends to make.
//
// What stays is what brokers actually choose. A suite says nothing about the
// CERTIFICATE CHAIN - RSA, P-256 and P-384 chains are verified whatever is
// negotiated, through the signature algorithms mbedTLS offers by default - so
// trimming this cannot cost a public broker:
//
//   ECDHE with ECDSA or RSA keys, AES-GCM   every modern broker, AWS IoT, Azure,
//                                          HiveMQ, Mosquitto, EMQX
//   ECDHE with AES-CBC-SHA256 / SHA384     brokers on an older TLS stack
//   RSA key transport with AES-GCM         the rare broker with no ECDHE at all
//
// ChaCha20 is not here because the ESP32 core does not compile it; a suite
// the engine lacks is skipped by mbedTLS rather than refused, so the list is
// safe either way, but it should say what is really sent.
static const int kSuites[] = {
    MBEDTLS_TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
    MBEDTLS_TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
    MBEDTLS_TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    MBEDTLS_TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
    MBEDTLS_TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
    MBEDTLS_TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,
    MBEDTLS_TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
    MBEDTLS_TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,
    MBEDTLS_TLS_RSA_WITH_AES_128_GCM_SHA256,
    MBEDTLS_TLS_RSA_WITH_AES_256_GCM_SHA384,
    0
};

// =============================================================================
// PSRAM
// =============================================================================

#if defined(MQ_CHANNEL_TLS_PSRAM)
namespace {

// The core's mbedTLS is built to allocate everything in internal RAM
// (CONFIG_MBEDTLS_INTERNAL_MEM_ALLOC), whatever the board decides for other
// allocations. This hands its blocks to the ordinary allocator instead, so they
// follow the board's own rule (heap_caps_malloc_extmem_enable: the ModBee Node
// puts blocks from 16 bytes in PSRAM). A handshake with a Let's Encrypt broker
// (P-384, a three-certificate chain) otherwise ran internal RAM down to a few
// bytes and failed.
void* tlsCalloc(size_t count, size_t size) {
    if (count != 0 && size > (size_t)-1 / count) return nullptr;
    return calloc(count, size);
}

// heap_caps_free() frees a block from ANY heap. That is what makes switching
// allocators safe while other sessions are open: the core's own allocator
// frees through the same call.
void tlsFree(void* block) { heap_caps_free(block); }

}  // namespace
#endif

bool mq_channelTlsUseExternalRam() {
#if defined(MQ_CHANNEL_TLS_PSRAM)
    static bool decided = false;
    static bool external = false;
    if (!decided) {
        decided = true;
        if (heap_caps_get_total_size(MALLOC_CAP_SPIRAM) > 0) {
            mbedtls_platform_set_calloc_free(tlsCalloc, tlsFree);
            external = true;
        }
    }
    return external;
#else
    return false;
#endif
}

// =============================================================================
// THE SESSION
// =============================================================================

struct MqttChannelTlsSession::Engine {
    mbedtls_ssl_context ssl;
    mbedtls_ssl_config conf;
    mbedtls_entropy_context entropy;
    mbedtls_ctr_drbg_context random;
    mbedtls_x509_crt authority;
    mbedtls_x509_crt own;
    mbedtls_pk_context ownKey;

    bool validate = true;
    bool pinning = false;
    char pin[65] = {0};
    bool pinMalformed = false;
    const char* host = nullptr;

    Engine() {
        mbedtls_ssl_init(&ssl);
        mbedtls_ssl_config_init(&conf);
        mbedtls_entropy_init(&entropy);
        mbedtls_ctr_drbg_init(&random);
        mbedtls_x509_crt_init(&authority);
        mbedtls_x509_crt_init(&own);
        mbedtls_pk_init(&ownKey);
    }
    ~Engine() {
        mbedtls_ssl_free(&ssl);
        mbedtls_ssl_config_free(&conf);
        mbedtls_ctr_drbg_free(&random);
        mbedtls_entropy_free(&entropy);
        mbedtls_x509_crt_free(&authority);
        mbedtls_x509_crt_free(&own);
        mbedtls_pk_free(&ownKey);
    }
};

namespace {

// The I/O mbedTLS asks for, over MqttByteChannel's convention: > 0 moved,
// 0 nothing now, < 0 gone.
int channelSend(void* context, const unsigned char* data, size_t length) {
    MqttByteChannel* channel = (MqttByteChannel*)context;
    const int sent = channel->channelSend(data, (int)length);
    if (sent > 0) return sent;
    if (sent == 0) return MBEDTLS_ERR_SSL_WANT_WRITE;
    return MBEDTLS_ERR_NET_CONN_RESET;
}

int channelRecv(void* context, unsigned char* into, size_t length) {
    MqttByteChannel* channel = (MqttByteChannel*)context;
    const int got = channel->channelRecv(into, (int)length);
    if (got > 0) return got;
    if (got == 0) return MBEDTLS_ERR_SSL_WANT_READ;
    return 0;   // end of stream: mbedTLS reports MBEDTLS_ERR_SSL_CONN_EOF
}

void toHex(const uint8_t* bytes, size_t length, char* out) {
    static const char digits[] = "0123456789abcdef";
    for (size_t index = 0; index < length; index++) {
        out[2 * index] = digits[bytes[index] >> 4];
        out[2 * index + 1] = digits[bytes[index] & 0x0f];
    }
    out[2 * length] = '\0';
}

// The same normalisation as mq_tls::normaliseFingerprint() on a host:
// colons, spaces, dashes and tabs off, capitals down. Anything that is then
// not 64 characters cannot match a SHA-256 and is reported as malformed.
bool normalisePin(const char* text, char out[65]) {
    size_t used = 0;
    for (const char* p = text; *p != '\0'; p++) {
        char c = *p;
        if (c == ':' || c == ' ' || c == '-' || c == '\t') continue;
        if (c >= 'A' && c <= 'F') c = (char)(c - 'A' + 'a');
        if (used == 64) return false;
        out[used++] = c;
    }
    out[used] = '\0';
    return used == 64;
}

// SHA-256 over the leaf's DER SubjectPublicKeyInfo, straight from the bytes
// the certificate parser kept (pk_raw) rather than re-encoding the key: the
// same bytes either way, without linking mbedTLS's key writer for it.
bool keyFingerprint(const mbedtls_x509_crt* certificate, char out[65]) {
    if (certificate == nullptr || certificate->pk_raw.p == nullptr ||
        certificate->pk_raw.len == 0) {
        return false;
    }
    uint8_t digest[32];
    if (mbedtls_sha256(certificate->pk_raw.p, certificate->pk_raw.len, digest, 0) != 0) {
        return false;
    }
    toHex(digest, sizeof digest, out);
    return true;
}

}  // namespace

// =============================================================================
// WHY, AS A VALUE
// =============================================================================

const char* mq_tlsErrorText(MqttTlsError error) {
    // At most 30 characters each: with " -0x7780" after it, a line fits a
    // STRING(64) LAST_ERROR pin with room to spare. "cannot connect" leads
    // Unreachable as it leads the host transport's sentence.
    switch (error) {
    case MqttTlsError::None:                 return "";
    case MqttTlsError::NoHost:               return "no host or port configured";
    case MqttTlsError::Unreachable:          return "cannot connect to the broker";
    case MqttTlsError::NoMemory:             return "TLS: out of memory";
    case MqttTlsError::NoCa:                 return "TLS: no CA certificate given";
    case MqttTlsError::BadCa:                return "TLS: CA certificate unreadable";
    case MqttTlsError::BadClientCertificate: return "TLS: client cert/key unusable";
    case MqttTlsError::EngineSetup:          return "TLS: engine would not start";
    case MqttTlsError::Untrusted:            return "TLS: certificate not trusted";
    case MqttTlsError::WrongHost:            return "TLS: wrong host name";
    case MqttTlsError::BadCertificate:       return "TLS: bad certificate";
    case MqttTlsError::NoPeerCertificate:    return "TLS: broker sent no cert";
    case MqttTlsError::PinMismatch:          return "TLS: not the pinned key";
    case MqttTlsError::Refused:              return "TLS: broker refused us";
    case MqttTlsError::NoCommonProtocol:     return "TLS: no common version/cipher";
    case MqttTlsError::Handshake:            return "TLS handshake failed";
    case MqttTlsError::Timeout:              return "TLS handshake timed out";
    case MqttTlsError::Closed:               return "broker closed the connection";
    case MqttTlsError::Dropped:              return "TLS: connection dropped";
    case MqttTlsError::ReadFailed:           return "TLS read failed";
    case MqttTlsError::WriteFailed:          return "TLS write failed";
    }
    return "TLS error";
}

// The codes a client session can meet that say something more specific than
// the step that failed. A mbedTLS code is a high-level part (0x1000-0x7F80)
// OR-ed with a low-level one (0x0002-0x007F) - a certificate that will not
// parse because the heap ran out is X509_INVALID_FORMAT | ASN1_ALLOC_FAILED -
// so each part is looked at on its own.
MqttTlsError mq_tlsErrorFromEngine(int code, MqttTlsError otherwise) {
    if (code >= 0) return otherwise;
    const int high = -((-code) & 0x7F80);
    const int low = -((-code) & 0x007F);
    switch (low) {
    case MBEDTLS_ERR_MPI_ALLOC_FAILED:
    case MBEDTLS_ERR_ASN1_ALLOC_FAILED:
        return MqttTlsError::NoMemory;
    case MBEDTLS_ERR_NET_CONN_RESET:
        return MqttTlsError::Dropped;
    default:
        break;
    }
    switch (high) {
    case MBEDTLS_ERR_SSL_ALLOC_FAILED:
    case MBEDTLS_ERR_X509_ALLOC_FAILED:
    case MBEDTLS_ERR_PK_ALLOC_FAILED:
    case MBEDTLS_ERR_ECP_ALLOC_FAILED:
    case MBEDTLS_ERR_MD_ALLOC_FAILED:
    case MBEDTLS_ERR_CIPHER_ALLOC_FAILED:
    case MBEDTLS_ERR_PEM_ALLOC_FAILED:
        return MqttTlsError::NoMemory;
    case MBEDTLS_ERR_SSL_FATAL_ALERT_MESSAGE:
        return MqttTlsError::Refused;
    case MBEDTLS_ERR_SSL_BAD_PROTOCOL_VERSION:
    case MBEDTLS_ERR_SSL_HANDSHAKE_FAILURE:
        return MqttTlsError::NoCommonProtocol;
    case MBEDTLS_ERR_SSL_BAD_CERTIFICATE:
        return MqttTlsError::BadCertificate;
    case MBEDTLS_ERR_SSL_TIMEOUT:
        return MqttTlsError::Timeout;
    case MBEDTLS_ERR_SSL_CONN_EOF:
        return MqttTlsError::Dropped;
    case MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY:
        return MqttTlsError::Closed;
    default:
        return otherwise;
    }
}

void MqttChannelTlsSession::fail(MqttTlsError error, int engineCode) {
    _error = error;
    _engineCode = engineCode < 0 ? engineCode : 0;
    const char* text = mq_tlsErrorText(error);
    if (_engineCode != 0) {
        snprintf(_lastError, sizeof _lastError, "%s -0x%04X", text, (unsigned)-_engineCode);
    } else {
        snprintf(_lastError, sizeof _lastError, "%s", text);
    }
    _state = kFailed;
}

bool MqttChannelTlsSession::begin(const Trust& trust, const char* host,
                                  MqttByteChannel* channel) {
    end();
    _error = MqttTlsError::None;
    _engineCode = 0;
    _lastError[0] = '\0';
    _peerKey[0] = '\0';
    _state = kIdle;
    _verified = false;
    _retryLength = 0;
    if (host == nullptr || channel == nullptr) {
        fail(MqttTlsError::NoHost);
        return false;
    }
    if ((trust.certificatePem != nullptr) != (trust.keyPem != nullptr)) {
        fail(MqttTlsError::BadClientCertificate);
        return false;
    }

    mq_channelTlsUseExternalRam();

    _engine = new (std::nothrow) Engine();
    if (_engine == nullptr) { fail(MqttTlsError::NoMemory); return false; }
    Engine& e = *_engine;
    e.host = host;
    e.validate = trust.validate;
    e.pinning = trust.pin != nullptr && trust.pin[0] != '\0';
    if (e.pinning) e.pinMalformed = !normalisePin(trust.pin, e.pin);

    static const char personal[] = "modbee-mqtt";
    int rc = mbedtls_ctr_drbg_seed(&e.random, mbedtls_entropy_func, &e.entropy,
                                   (const unsigned char*)personal, sizeof personal - 1);
    if (rc != 0) { fail(mq_tlsErrorFromEngine(rc, MqttTlsError::EngineSetup), rc); return false; }

    rc = mbedtls_ssl_config_defaults(&e.conf, MBEDTLS_SSL_IS_CLIENT,
                                     MBEDTLS_SSL_TRANSPORT_STREAM,
                                     MBEDTLS_SSL_PRESET_DEFAULT);
    if (rc != 0) { fail(mq_tlsErrorFromEngine(rc, MqttTlsError::EngineSetup), rc); return false; }
    mbedtls_ssl_conf_rng(&e.conf, mbedtls_ctr_drbg_random, &e.random);
    mbedtls_ssl_conf_ciphersuites(&e.conf, kSuites);

    // Pinning does its own checking, after the handshake, against the key -
    // so the engine is told not to verify at all, exactly as the host
    // transport tells OpenSSL. Validating with no CA would fail the handshake
    // before there was a certificate to compare.
    if (e.validate && !e.pinning) {
        if (trust.caPem == nullptr || trust.caPem[0] == '\0') {
            fail(MqttTlsError::NoCa);
            return false;
        }
        rc = mbedtls_x509_crt_parse(&e.authority, (const unsigned char*)trust.caPem,
                                    strlen(trust.caPem) + 1);
        // A bundle where some certificates parse is usable; none is not.
        if (rc < 0 || e.authority.raw.p == nullptr) {
            fail(mq_tlsErrorFromEngine(rc, MqttTlsError::BadCa), rc);
            return false;
        }
        mbedtls_ssl_conf_ca_chain(&e.conf, &e.authority, nullptr);
        mbedtls_ssl_conf_authmode(&e.conf, MBEDTLS_SSL_VERIFY_REQUIRED);
    } else {
        mbedtls_ssl_conf_authmode(&e.conf, MBEDTLS_SSL_VERIFY_NONE);
    }

    if (trust.certificatePem != nullptr) {
        rc = mbedtls_x509_crt_parse(&e.own, (const unsigned char*)trust.certificatePem,
                                    strlen(trust.certificatePem) + 1);
        if (rc == 0) {
            rc = mbedtls_pk_parse_key(&e.ownKey, (const unsigned char*)trust.keyPem,
                                      strlen(trust.keyPem) + 1, nullptr, 0,
                                      mbedtls_ctr_drbg_random, &e.random);
        }
        if (rc == 0) rc = mbedtls_ssl_conf_own_cert(&e.conf, &e.own, &e.ownKey);
        if (rc != 0) {
            fail(mq_tlsErrorFromEngine(rc, MqttTlsError::BadClientCertificate), rc);
            return false;
        }
    }

    rc = mbedtls_ssl_setup(&e.ssl, &e.conf);
    if (rc != 0) { fail(mq_tlsErrorFromEngine(rc, MqttTlsError::EngineSetup), rc); return false; }

    // SNI always - a broker serving several names chooses its certificate by
    // it. The NAME IS CHECKED only when validating, because the authmode
    // above decides whether the certificate is examined at all.
    rc = mbedtls_ssl_set_hostname(&e.ssl, host);
    if (rc != 0) { fail(mq_tlsErrorFromEngine(rc, MqttTlsError::EngineSetup), rc); return false; }

    mbedtls_ssl_set_bio(&e.ssl, channel, channelSend, channelRecv, nullptr);
    return true;
}

int MqttChannelTlsSession::pump() {
    if (_state != kIdle || _engine == nullptr) return _state;

    const int rc = mbedtls_ssl_handshake(&_engine->ssl);
    if (rc == MBEDTLS_ERR_SSL_WANT_READ || rc == MBEDTLS_ERR_SSL_WANT_WRITE) {
        // The ordinary case: a handshake is several round trips, and on a
        // modem each is hundreds of milliseconds. It continues on a later
        // poll; waiting here would stop the scan for all of them.
        return kIdle;
    }
    if (rc == 0) return finish() ? kUp : kFailed;

    if (rc == MBEDTLS_ERR_X509_CERT_VERIFY_FAILED) {
        const uint32_t flags = mbedtls_ssl_get_verify_result(&_engine->ssl);
        // The name first: a certificate for another name that is also
        // unsigned is fixed by the HOST pin before the CA is worth a look.
        if (flags & MBEDTLS_X509_BADCERT_CN_MISMATCH) {
            fail(MqttTlsError::WrongHost);
        } else if (flags & MBEDTLS_X509_BADCERT_NOT_TRUSTED) {
            fail(MqttTlsError::Untrusted);
        } else {
            fail(MqttTlsError::BadCertificate);
        }
        return kFailed;
    }
    fail(mq_tlsErrorFromEngine(rc, MqttTlsError::Handshake), rc);
    return kFailed;
}

// The handshake has completed. Record the key, and - when pinning - compare
// it before a single application byte is sent.
bool MqttChannelTlsSession::finish() {
    Engine& e = *_engine;

    // Always recorded, pinned or not: this is what a commissioning run reads
    // out and stores (BROKER_KEY), and what a pinned run compares.
    const bool haveKey = keyFingerprint(mbedtls_ssl_get_peer_cert(&e.ssl), _peerKey);

    if (e.pinning) {
        if (!haveKey) {
            fail(MqttTlsError::NoPeerCertificate);
        } else if (e.pinMalformed || strcmp(e.pin, _peerKey) != 0) {
            // The key the broker DID present stays readable through
            // peerKeyFingerprint() (BROKER_KEY), and the pinned one is the
            // caller's own - so the two can still be compared side by side.
            fail(MqttTlsError::PinMismatch);
        }
        if (_state == kFailed) {
            mbedtls_ssl_close_notify(&e.ssl);
            return false;
        }
        _verified = true;
    } else {
        // VERIFY_REQUIRED has already refused anything that did not chain and
        // name-match, so reaching here validating IS verified.
        _verified = e.validate;
    }
    _state = kUp;
    return true;
}

int MqttChannelTlsSession::read(uint8_t* into, size_t length) {
    if (into == nullptr || length == 0) return 0;
    if (pump() != kUp) return _state == kFailed ? -1 : 0;
    const int got = mbedtls_ssl_read(&_engine->ssl, into, length);
    if (got > 0) return got;
    if (got == MBEDTLS_ERR_SSL_WANT_READ || got == MBEDTLS_ERR_SSL_WANT_WRITE) return 0;
    if (got == 0 || got == MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY) {
        fail(MqttTlsError::Closed);
        return -1;
    }
    fail(mq_tlsErrorFromEngine(got, MqttTlsError::ReadFailed), got);
    return -1;
}

int MqttChannelTlsSession::write(const uint8_t* data, size_t length) {
    if (data == nullptr || length == 0) return 0;
    if (pump() != kUp) return _state == kFailed ? -1 : 0;

    // AFTER WANT_WRITE, mbedTLS has the record already built and expects the
    // SAME call again - and a longer one would be reported as sent in full
    // when only the first part was. MqttLink retries from the same cursor,
    // possibly with more queued behind it, so the retry is clamped to what was
    // offered the first time.
    if (_retryLength != 0 && length > _retryLength) length = _retryLength;
    const int sent = mbedtls_ssl_write(&_engine->ssl, data, length);
    if (sent > 0) { _retryLength = 0; return sent; }
    if (sent == MBEDTLS_ERR_SSL_WANT_WRITE || sent == MBEDTLS_ERR_SSL_WANT_READ) {
        _retryLength = length;
        return 0;
    }
    fail(mq_tlsErrorFromEngine(sent, MqttTlsError::WriteFailed), sent);
    return -1;
}

void MqttChannelTlsSession::unreachable(const char* host, uint16_t port) {
    (void)host;
    (void)port;
    end();
    fail(MqttTlsError::Unreachable);
}

size_t MqttChannelTlsSession::pending() const {
    if (_engine == nullptr || _state != kUp) return 0;
    return mbedtls_ssl_get_bytes_avail(&_engine->ssl);
}

void MqttChannelTlsSession::end() {
    if (_engine != nullptr) {
        // One-shot, like the host transport: a full close would wait for the
        // peer's own close_notify, and nothing in this library waits.
        if (_state == kUp) mbedtls_ssl_close_notify(&_engine->ssl);
        // A handshake given up half way. Recorded AFTER the engine is freed,
        // and only then, so a session that never began says nothing.
        const bool unfinished = _state == kIdle;
        delete _engine;
        _engine = nullptr;
        if (unfinished) fail(MqttTlsError::Timeout);
    }
    if (_state == kUp) _state = kIdle;
}

#endif  // MQTT_CHANNEL_TLS

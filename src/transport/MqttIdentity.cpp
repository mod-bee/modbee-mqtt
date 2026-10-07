// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttIdentity.h"

#if MQTT_TLS_AVAILABLE

#include "MqttTlsSupport.h"

#if MQTT_TLS_ENGINE_WOLFSSL
// After MqttTlsSupport.h, which has already put the configuration in scope.
#include <wolfssl/wolfcrypt/ecc.h>
#include <wolfssl/wolfcrypt/asn_public.h>
#include <wolfssl/wolfcrypt/random.h>
#endif


#include <stdio.h>
#include <string.h>
#if !defined(_WIN32)
#include <sys/stat.h>
#endif

namespace {

std::string pemOfCertificate(X509* cert) {
    BIO* bio = BIO_new(BIO_s_mem());
    if (bio == nullptr) return std::string();
    std::string out;
    if (PEM_write_bio_X509(bio, cert) == 1) {
        char* data = nullptr;
        const long length = BIO_get_mem_data(bio, &data);
        if (data != nullptr && length > 0) out.assign(data, (size_t)length);
    }
    BIO_free(bio);
    return out;
}

std::string pemOfKey(EVP_PKEY* key) {
    BIO* bio = BIO_new(BIO_s_mem());
    if (bio == nullptr) return std::string();
    std::string out;
    // Unencrypted: a passphrase a device would have to store beside the key it
    // protects buys nothing, and asking an unattended controller for one at
    // boot is not a thing that can work.
    if (PEM_write_bio_PrivateKey(bio, key, nullptr, nullptr, 0, nullptr, nullptr) == 1) {
        char* data = nullptr;
        const long length = BIO_get_mem_data(bio, &data);
        if (data != nullptr && length > 0) out.assign(data, (size_t)length);
    }
    BIO_free(bio);
    return out;
}

}  // namespace

std::string mqttKeyFingerprint(const char* certificatePem) {
    if (certificatePem == nullptr || *certificatePem == '\0') return std::string();
    mq_tls::ensureEngine();

    BIO* bio = BIO_new_mem_buf(certificatePem, (int)strlen(certificatePem));
    X509* cert = bio ? PEM_read_bio_X509(bio, nullptr, nullptr, nullptr) : nullptr;
    if (bio) BIO_free(bio);
    if (cert == nullptr) return std::string();

    const std::string out = mq_tls::keyFingerprint(cert);
    X509_free(cert);
    return out;
}

MqttIdentity::~MqttIdentity() { clear(); }

void MqttIdentity::clear() {
    if (_cert) { X509_free((X509*)_cert); _cert = nullptr; }
    if (_key) { EVP_PKEY_free((EVP_PKEY*)_key); _key = nullptr; }
    _certPem.clear();
    _keyPem.clear();
    _fingerprint.clear();
}

bool MqttIdentity::fail(const char* what) {
    const std::string detail = mq_tls::engineError();
    _lastError = detail.empty() ? std::string(what) : std::string(what) + ": " + detail;
    return false;
}

void MqttIdentity::setCommonName(const char* name) {
    _commonName = (name && *name) ? name : "modbee";
}

void MqttIdentity::setSubjectAltNames(const char* names) {
    _altNames = names ? names : "";
}

bool MqttIdentity::capture() {
    _certPem = pemOfCertificate((X509*)_cert);
    _keyPem = pemOfKey((EVP_PKEY*)_key);
    _fingerprint = mq_tls::keyFingerprint((X509*)_cert);
    if (_certPem.empty() || _keyPem.empty() || _fingerprint.empty()) {
        return fail("the identity would not serialise");
    }
    return true;
}

#if MQTT_TLS_ENGINE_WOLFSSL

// =============================================================================
// GENERATING A CERTIFICATE ON WOLFSSL
// =============================================================================
//
// wolfSSL's OpenSSL compatibility layer carries almost the whole of this file
// - it reads and writes PEM, it signs, it hands back the SubjectPublicKeyInfo.
// The one thing it does NOT do is build an X.509v3 extension from a string:
// wolfSSL_X509V3_EXT_conf_nid is a stub that returns NULL for every NID, and
// there is no wolfSSL_X509_set_isCA either. Verified against 5.9.2 by calling
// it rather than by reading about it.
//
// That matters because basicConstraints CA:TRUE is not decoration here. It is
// what lets a client load this very certificate as its trust anchor and verify
// the ordinary way instead of pinning - and a trust anchor without it is
// refused as an invalid CA.
//
// So generation uses wolfCrypt directly, which is wolfSSL's own documented
// route and gives every field the OpenSSL path sets: isCA, a critical
// keyUsage, an extendedKeyUsage, a subject key identifier and subject
// alternative names. The result is structurally identical - `openssl x509
// -text` on one and on the other differ only in the key.
//
// The PEM it produces is then handed to load(), so everything after this point
// - the handles, the fingerprint, valid() - is the same code on both engines
// and cannot drift apart.

namespace {

// One ASN.1 length, short or long form. A DNS name may be 253 bytes and a
// list of them easily more, so the two-byte form is not theoretical.
size_t writeLength(std::string& out, size_t length) {
    if (length < 0x80) {
        out.push_back((char)length);
        return 1;
    }
    if (length < 0x100) {
        out.push_back((char)0x81);
        out.push_back((char)length);
        return 2;
    }
    out.push_back((char)0x82);
    out.push_back((char)((length >> 8) & 0xFF));
    out.push_back((char)(length & 0xFF));
    return 3;
}

bool parseIPv4(const std::string& text, unsigned char out[4]) {
    unsigned values[4];
    if (sscanf(text.c_str(), "%u.%u.%u.%u", &values[0], &values[1], &values[2],
               &values[3]) != 4) {
        return false;
    }
    for (int i = 0; i < 4; i++) {
        if (values[i] > 255) return false;
        out[i] = (unsigned char)values[i];
    }
    return true;
}

// "DNS:plc.local,IP:10.0.0.10" into the GeneralNames SEQUENCE wolfSSL expects
// in Cert::altNames - which is exactly what its own FlattenAltNames writes:
// a SEQUENCE of [context-specific | type] LENGTH VALUE.
//
// The SPELLING IS OPENSSL'S on purpose. A caller must not have to know which
// engine was compiled in, and setSubjectAltNames() is documented in one place
// for both.
bool buildAltNames(const std::string& spec, std::string& der) {
    std::string body;

    size_t at = 0;
    while (at <= spec.size()) {
        size_t comma = spec.find(',', at);
        if (comma == std::string::npos) comma = spec.size();
        std::string entry = spec.substr(at, comma - at);
        at = comma + 1;

        // Trim, because "DNS:a, IP:b" is what a person types.
        while (!entry.empty() && (entry.front() == ' ' || entry.front() == '\t')) entry.erase(0, 1);
        while (!entry.empty() && (entry.back() == ' ' || entry.back() == '\t')) entry.pop_back();
        if (entry.empty()) continue;

        const size_t colon = entry.find(':');
        if (colon == std::string::npos) return false;
        const std::string kind = entry.substr(0, colon);
        const std::string value = entry.substr(colon + 1);
        if (value.empty()) return false;

        if (kind == "DNS") {
            body.push_back((char)(0x80 | ASN_DNS_TYPE));
            writeLength(body, value.size());
            body.append(value);
        } else if (kind == "IP") {
            unsigned char octets[4];
            if (!parseIPv4(value, octets)) return false;   // IPv6 is not offered
            body.push_back((char)(0x80 | ASN_IP_TYPE));
            writeLength(body, 4);
            body.append((const char*)octets, 4);
        } else {
            return false;   // named rather than silently dropped
        }
    }

    if (body.empty()) return false;

    der.clear();
    der.push_back((char)0x30);            // SEQUENCE
    writeLength(der, body.size());
    der.append(body);
    return der.size() <= CTC_MAX_ALT_SIZE;
}

}  // namespace

bool MqttIdentity::generate(uint32_t validDays) {
    mq_tls::ensureEngine();
    clear();
    _lastError.clear();

    WC_RNG rng;
    if (wc_InitRng(&rng) != 0) return fail("no randomness");

    ecc_key key;
    if (wc_ecc_init(&key) != 0) { wc_FreeRng(&rng); return fail("no key"); }

    // P-256, for the reason in the header: about a second on an ESP32 where
    // RSA-2048 is minutes, and minutes of a blocked scan is not a thing a PLC
    // can do.
    if (wc_ecc_make_key_ex(&rng, 32, &key, ECC_SECP256R1) != 0) {
        wc_ecc_free(&key);
        wc_FreeRng(&rng);
        return fail("the key would not generate");
    }

    Cert cert;
    if (wc_InitCert(&cert) != 0) {
        wc_ecc_free(&key);
        wc_FreeRng(&rng);
        return fail("no certificate");
    }

    strncpy(cert.subject.commonName, _commonName.c_str(), CTC_NAME_SIZE - 1);
    cert.subject.commonName[CTC_NAME_SIZE - 1] = '\0';
    strncpy(cert.subject.org, "ModBee", CTC_NAME_SIZE - 1);
    cert.subject.org[CTC_NAME_SIZE - 1] = '\0';

    cert.daysValid  = (int)validDays;
    cert.selfSigned = 1;
    cert.sigType    = CTC_SHA256wECDSA;

    // CA:TRUE because this certificate is also the only trust anchor there is;
    // a client that would rather validate a chain than pin a key loads this
    // very file as its CA, and an end-entity certificate cannot be loaded that
    // way.
    cert.isCA = 1;

    bool ok =
        wc_SetKeyUsage(&cert, "digitalSignature,keyEncipherment,keyCertSign") == 0 &&
        wc_SetExtKeyUsage(&cert, "serverAuth,clientAuth") == 0 &&
        wc_SetSubjectKeyIdFromPublicKey_ex(&cert, ECC_TYPE, &key) == 0;

    if (ok) {
        // Names a verifying client may match. Without at least one, modern
        // clients refuse on hostname grounds alone - CN has not been consulted
        // for that since RFC 2818 was replaced.
        std::string names = _altNames.empty() ? ("DNS:" + _commonName) : _altNames;
        std::string der;
        ok = buildAltNames(names, der);
        if (ok) {
            memcpy(cert.altNames, der.data(), der.size());
            cert.altNamesSz = (int)der.size();
        } else {
            _lastError = "the subject alternative names could not be encoded: " + names;
        }
    }

    if (ok) {
        // A random serial, positive per RFC 5280. Two devices generating in
        // the same second must not present the same one to something that
        // caches by issuer and serial.
        ok = wc_RNG_GenerateBlock(&rng, cert.serial, 16) == 0;
        // Positive (top bit clear) AND never a leading zero byte. With only
        // the 0x7F mask, one serial in 256 started 0x00 with the next byte
        // below 0x80 - a non-minimal DER INTEGER, which wolfSSL's own parser
        // then refused: the board's certificate "would not parse" about once
        // in every 256 generations, found by generating 3000 in a row.
        // 0x40-0x7F keeps 126 random bits.
        cert.serial[0] = (uint8_t)((cert.serial[0] & 0x3F) | 0x40);
        cert.serialSz = 16;
    }

    std::string certPem;
    std::string keyPem;

    if (ok) {
        unsigned char der[4096];
        const int bodySz = wc_MakeCert(&cert, der, (word32)sizeof der, nullptr, &key, &rng);
        ok = bodySz > 0;
        if (ok) {
            const int derSz = wc_SignCert(cert.bodySz, cert.sigType, der,
                                          (word32)sizeof der, nullptr, &key, &rng);
            ok = derSz > 0;
            if (ok) {
                unsigned char pem[8192];
                const int pemSz = wc_DerToPem(der, (word32)derSz, pem,
                                              (word32)sizeof pem, CERT_TYPE);
                ok = pemSz > 0;
                if (ok) certPem.assign((const char*)pem, (size_t)pemSz);
            }
        }
        if (!ok && _lastError.empty()) _lastError = "the certificate would not be made";
    }

    if (ok) {
        // PKCS#8 rather than SEC1, because that is what every other tool hands
        // back for an EC key and what a person pasting it elsewhere expects.
        unsigned char der[1024];
        word32 derSz = (word32)sizeof der;
        const int wrote = wc_EccKeyToPKCS8(&key, der, &derSz);
        ok = wrote > 0;
        if (ok) {
            unsigned char pem[4096];
            const int pemSz = wc_DerToPem(der, (word32)wrote, pem,
                                          (word32)sizeof pem, PKCS8_PRIVATEKEY_TYPE);
            ok = pemSz > 0;
            if (ok) keyPem.assign((const char*)pem, (size_t)pemSz);
        }
        if (!ok && _lastError.empty()) _lastError = "the private key would not be written";
    }

    wc_ecc_free(&key);
    wc_FreeRng(&rng);

    if (!ok) return false;

    // Everything from here is the shared path: load() parses what was just
    // written and fills in the handles, the PEM and the fingerprint, so a
    // generated identity and a loaded one are indistinguishable afterwards.
    return load(certPem.c_str(), keyPem.c_str());
}

#else  // MQTT_TLS_ENGINE_OPENSSL

namespace {

// One X509v3 extension, by name, added to a certificate that is its own issuer.
bool addExtension(X509* cert, int nid, const char* value) {
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, cert, cert, nullptr, nullptr, 0);

    // const_cast because wolfSSL's compatibility layer takes char* here where
    // OpenSSL takes const char*. The value is read, never written - both
    // engines copy it into the extension they build - so the cast is honest
    // about the API and not about the data.
    X509_EXTENSION* extension =
        X509V3_EXT_conf_nid(nullptr, &ctx, nid, const_cast<char*>(value));
    if (extension == nullptr) return false;
    const int added = X509_add_ext(cert, extension, -1);
    X509_EXTENSION_free(extension);
    return added == 1;
}

}  // namespace

bool MqttIdentity::generate(uint32_t validDays) {
    mq_tls::ensureEngine();
    clear();
    _lastError.clear();

    // --- the key -----------------------------------------------------------
    EVP_PKEY* key = nullptr;
    EVP_PKEY_CTX* keyCtx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
    if (keyCtx == nullptr) return fail("no key context");
    if (EVP_PKEY_keygen_init(keyCtx) != 1 ||
        EVP_PKEY_CTX_set_ec_paramgen_curve_nid(keyCtx, NID_X9_62_prime256v1) != 1 ||
        EVP_PKEY_keygen(keyCtx, &key) != 1) {
        EVP_PKEY_CTX_free(keyCtx);
        return fail("the key would not generate");
    }
    EVP_PKEY_CTX_free(keyCtx);
    _key = key;

    // --- the certificate ---------------------------------------------------
    X509* cert = X509_new();
    if (cert == nullptr) { clear(); return fail("no certificate"); }
    _cert = cert;

    // Version 3. The number stored is one less than the name, which is an
    // ASN.1 convention rather than an off-by-one.
    X509_set_version(cert, 2);

    // A random serial. Two devices that generate on the same second must not
    // present the same serial to something that caches by issuer and serial.
    {
        unsigned char bytes[16];
        if (RAND_bytes(bytes, (int)sizeof bytes) != 1) { clear(); return fail("no randomness"); }
        bytes[0] &= 0x7F;                       // positive, per RFC 5280
        BIGNUM* serial = BN_bin2bn(bytes, (int)sizeof bytes, nullptr);
        if (serial == nullptr) { clear(); return fail("no serial"); }
        BN_to_ASN1_INTEGER(serial, X509_get_serialNumber(cert));
        BN_free(serial);
    }

    // Valid from yesterday. A controller whose clock has not been set yet, or
    // whose clock is a few hours out, must not be holding a certificate that
    // is not valid quite yet - which looks identical to an attack and is
    // impossible to diagnose from the client's end.
    if (X509_gmtime_adj(X509_getm_notBefore(cert), -24L * 60L * 60L) == nullptr ||
        X509_gmtime_adj(X509_getm_notAfter(cert),
                        (long)validDays * 24L * 60L * 60L) == nullptr) {
        clear();
        return fail("the validity period would not be set");
    }

    if (X509_set_pubkey(cert, key) != 1) { clear(); return fail("the public key would not attach"); }

    X509_NAME* name = X509_get_subject_name(cert);
    if (X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   (const unsigned char*)_commonName.c_str(),
                                   -1, -1, 0) != 1) {
        clear();
        return fail("the common name would not be set");
    }
    X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC,
                               (const unsigned char*)"ModBee", -1, -1, 0);

    // Self-signed: the issuer IS the subject.
    if (X509_set_issuer_name(cert, name) != 1) { clear(); return fail("the issuer would not be set"); }

    // CA:TRUE because this certificate is also the only trust anchor there is.
    // A client that would rather validate a chain than pin a key loads this
    // very file as its CA, and an end-entity certificate cannot be loaded that
    // way.
    if (!addExtension(cert, NID_basic_constraints, "critical,CA:TRUE") ||
        !addExtension(cert, NID_key_usage,
                      "critical,digitalSignature,keyEncipherment,keyCertSign") ||
        !addExtension(cert, NID_ext_key_usage, "serverAuth,clientAuth") ||
        !addExtension(cert, NID_subject_key_identifier, "hash")) {
        clear();
        return fail("a certificate extension would not be added");
    }

    // Names a verifying client may match. Without at least one, modern clients
    // refuse on hostname grounds alone - CN has not been consulted for that
    // since RFC 2818 was replaced - so the common name is repeated here as a
    // DNS name when the caller named nothing else.
    {
        std::string alt = _altNames;
        if (alt.empty()) alt = "DNS:" + _commonName;
        if (!addExtension(cert, NID_subject_alt_name, alt.c_str())) {
            clear();
            return fail("the subject alternative names would not be added");
        }
    }

    if (X509_sign(cert, key, EVP_sha256()) == 0) { clear(); return fail("the certificate would not sign"); }

    return capture();
}

#endif // MQTT_TLS_ENGINE_WOLFSSL

bool MqttIdentity::load(const char* certificatePem, const char* keyPem) {
    mq_tls::ensureEngine();
    if (certificatePem == nullptr || keyPem == nullptr) {
        _lastError = "no certificate or key was given";
        return false;
    }
    clear();
    _lastError.clear();

    BIO* certBio = BIO_new_mem_buf(certificatePem, (int)strlen(certificatePem));
    X509* cert = certBio ? PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr) : nullptr;
    if (certBio) BIO_free(certBio);
    if (cert == nullptr) return fail("the certificate would not parse");
    _cert = cert;

    BIO* keyBio = BIO_new_mem_buf(keyPem, (int)strlen(keyPem));
    EVP_PKEY* key = keyBio ? PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr) : nullptr;
    if (keyBio) BIO_free(keyBio);
    if (key == nullptr) { clear(); return fail("the private key would not parse"); }
    _key = key;

    // Said here rather than discovered at the first handshake, where it would
    // be a client-side error about a bad record MAC and no sign of the cause.
    if (!mq_tls::keyMatchesCertificate(cert, key)) {
        clear();
        return fail("the private key does not match the certificate");
    }

    return capture();
}

#if defined(MQTT_PLATFORM_HOST)

bool MqttIdentity::loadFiles(const char* certificatePath, const char* keyPath) {
    mq_tls::ensureEngine();
    if (certificatePath == nullptr || keyPath == nullptr) {
        _lastError = "no certificate or key path was given";
        return false;
    }
    clear();
    _lastError.clear();

    FILE* certFile = fopen(certificatePath, "rb");
    if (certFile == nullptr) {
        _lastError = std::string("cannot read ") + certificatePath;
        return false;
    }
    X509* cert = PEM_read_X509(certFile, nullptr, nullptr, nullptr);
    fclose(certFile);
    if (cert == nullptr) return fail("the certificate would not parse");
    _cert = cert;

    FILE* keyFile = fopen(keyPath, "rb");
    if (keyFile == nullptr) {
        clear();
        _lastError = std::string("cannot read ") + keyPath;
        return false;
    }
    EVP_PKEY* key = PEM_read_PrivateKey(keyFile, nullptr, nullptr, nullptr);
    fclose(keyFile);
    if (key == nullptr) { clear(); return fail("the private key would not parse"); }
    _key = key;

    if (!mq_tls::keyMatchesCertificate(cert, key)) {
        clear();
        return fail("the private key does not match the certificate");
    }

    return capture();
}

bool MqttIdentity::saveFiles(const char* certificatePath, const char* keyPath) const {
    if (!valid() || certificatePath == nullptr || keyPath == nullptr) return false;

    FILE* certFile = fopen(certificatePath, "wb");
    if (certFile == nullptr) return false;
    const size_t certWritten = fwrite(_certPem.data(), 1, _certPem.size(), certFile);
    fclose(certFile);
    if (certWritten != _certPem.size()) return false;

    FILE* keyFile = fopen(keyPath, "wb");
    if (keyFile == nullptr) return false;
    const size_t keyWritten = fwrite(_keyPem.data(), 1, _keyPem.size(), keyFile);
    fclose(keyFile);
    if (keyWritten != _keyPem.size()) return false;

    // The key is readable by its owner and by nobody else. Not fatal where the
    // platform has no such notion - a board with no file permissions still
    // gets a key written - but on a runtime this is the difference between a
    // private key and a published one.
#if !defined(_WIN32)
    chmod(keyPath, 0600);
#endif
    return true;
}

bool MqttIdentity::loadOrGenerate(const char* certificatePath, const char* keyPath,
                                  uint32_t validDays) {
    if (certificatePath != nullptr && keyPath != nullptr &&
        loadFiles(certificatePath, keyPath)) {
        return true;
    }
    if (!generate(validDays)) return false;
    if (certificatePath != nullptr && keyPath != nullptr) {
        // A generated identity that cannot be written down is still usable for
        // this run - it just will not be the same one after a restart, and the
        // fingerprint moves. Worth saying, not worth refusing to start over.
        if (!saveFiles(certificatePath, keyPath)) {
            _lastError = std::string("the new identity could not be written to ") +
                         certificatePath + " - it will not survive a restart";
        }
    }
    return true;
}

#endif // MQTT_PLATFORM_HOST

#endif // MQTT_TLS_AVAILABLE

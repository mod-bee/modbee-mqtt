// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#ifndef MODBEE_MQTT_CERTIFICATE_H
#define MODBEE_MQTT_CERTIFICATE_H

// What a certificate says about itself: who it names, until when, for which
// host names, and the fingerprint that identifies it exactly.
//
// TWO ENGINES, ONE READING. The library runs on wolfSSL or OpenSSL, and a
// certificate's subject read through each engine's own API comes back in two
// different spellings - one engine's "/CN=plc-01" is the other's
// "CN = plc-01", and a name in a BMPString is a third story. A login taken
// from a certificate must be the SAME login whichever engine accepted the
// handshake, or a device that works against one build is refused by the
// other. So the engine is asked for exactly one thing - the certificate's DER
// bytes, natively on wolfSSL and through i2d on OpenSSL - and everything
// below reads those bytes with one parser. The two cannot drift because there
// is only one of it, and test_certificate holds both builds to the same
// answers.
//
// THE FINGERPRINT HERE IS OF THE WHOLE CERTIFICATE, not of its key - which is
// the opposite of MqttIdentity::fingerprint(), and deliberately. A pin wants
// to survive a reissue from the same key (RFC 7469's reasoning). A login
// bound to one certificate wants the reverse: a reissued certificate is a new
// credential, and an administrator should have to say so. It is SHA-256 over
// the DER, 64 lowercase hexadecimal characters with no colons - what
// `openssl x509 -fingerprint -sha256` prints, once its colons are taken out
// and its case is lowered.

#include "../MqttFeatures.h"   // MQTT_TLS_AVAILABLE, before anything tests it

#if MQTT_TLS_AVAILABLE

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

struct MqttCertificateFacts {
    // SHA-256 over the certificate's DER: 64 lowercase hexadecimal characters.
    std::string fingerprint;

    // The subject's Common Name. EMPTY when the subject has none, or has more
    // than one: a certificate naming two people names nobody in particular,
    // and taking the first would let whichever order a CA happened to write
    // decide who logs in.
    std::string commonName;

    // The whole subject, in the certificate's own order: "O=Plant, CN=plc-01".
    std::string subject;

    // When it stops being valid, as ISO 8601 UTC ("2036-10-04T09:33:00Z"),
    // and as seconds since 1970 for arithmetic.
    std::string notAfter;
    int64_t notAfterSeconds = 0;

    // The subjectAltName entries, in OpenSSL's spelling: "DNS:plc-01",
    // "IP:10.0.0.10", "email:...", "URI:...".
    std::vector<std::string> names;
};

// What the DER bytes of one certificate say. False if they are not a
// certificate this can read - in which case nothing in `out` is to be trusted.
//
// Every length in the bytes is bounded against the buffer before it is used,
// for the reason SECURITY.md gives for the MQTT codecs: these are bytes
// somebody else chose. A client's certificate is read only after the engine
// has verified it, but "verified" means signed by a CA, not well-formed for
// this parser.
bool mqttCertificateFacts(const uint8_t* der, size_t length, MqttCertificateFacts& out);

// The same, for the first certificate in a PEM text. False, with `error`
// saying why, if there is no certificate in it that parses.
bool mqttReadCertificate(const char* pem, MqttCertificateFacts& out, std::string& error);

// How many certificates in a PEM text parse - a client CA may be a bundle.
size_t mqttCountCertificates(const char* pem);

// Whether `keyPem` parses and is the private key `certificatePem` was issued
// for. False, with `error` naming which of the three it was, otherwise.
bool mqttCertificateMatchesKey(const char* certificatePem, const char* keyPem,
                               std::string& error);

#endif // MQTT_TLS_AVAILABLE
#endif // MODBEE_MQTT_CERTIFICATE_H

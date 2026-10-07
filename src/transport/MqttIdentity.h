// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#ifndef MODBEE_MQTT_IDENTITY_H
#define MODBEE_MQTT_IDENTITY_H

// A key pair and a certificate of its own, made on the device.
//
// This is what lets a controller serve TLS without anybody having issued it
// anything. It starts, finds it has no identity, makes one, and from then on
// presents the same one - so the fingerprint a commissioning engineer wrote
// down stays true.
//
// WHY ELLIPTIC CURVE AND NOT RSA. P-256 keygen is roughly a second on an
// ESP32; RSA-2048 is minutes, and minutes of a blocked scan is not a thing a
// PLC can do. Both ends have supported P-256 since TLS 1.2, so nothing is
// given up for it.
//
// WHY THE FINGERPRINT IS OF THE PUBLIC KEY AND NOT OF THE CERTIFICATE. The
// certificate expires and gets regenerated; the key does not have to. A key
// fingerprint survives that, so pinning does not break the first time a device
// renews itself. This is the same choice RFC 7469 made for HPKP, and for the
// same reason.
//
// It is SHA-256 over the DER SubjectPublicKeyInfo, written as 64 lowercase
// hexadecimal characters. That length is not arbitrary: a retained IEC STRING
// is a fixed 127 bytes whatever width it was declared, and a value longer than
// that is truncated rather than refused - so 64 characters fit with room, and
// the PEM of the key itself (178 characters) would have been silently halved.

#include "../MqttFeatures.h"   // MQTT_TLS_AVAILABLE, before anything tests it

#if MQTT_TLS_AVAILABLE

#include <stdint.h>
#include <string>

class MqttIdentity {
public:
    MqttIdentity() = default;
    ~MqttIdentity();

    MqttIdentity(const MqttIdentity&) = delete;
    MqttIdentity& operator=(const MqttIdentity&) = delete;

    // The name the certificate carries, and the names a verifying client may
    // match against it. Both are optional; call them before generate().
    //
    // Alternative names are given in OpenSSL's own spelling so there is no
    // half-guessing about whether something is a host name or an address:
    //   "DNS:plc.local,DNS:scada,IP:10.0.0.10"
    void setCommonName(const char* name);
    void setSubjectAltNames(const char* names);

    // Make a key and a certificate for it. Ten years by default, because the
    // alternative is a controller that stops being able to talk to itself on a
    // date nobody has in a diary.
    bool generate(uint32_t validDays = 3650);

    // Take an identity that already exists, from PEM text or from files. Both
    // forms exist because where the identity is kept is the caller's business:
    // a runtime has a file system, a board may have nothing but a blob it
    // pulled out of flash.
    bool load(const char* certificatePem, const char* keyPem);

    // THE FILE FORMS EXIST ONLY WHERE THERE ARE FILES. A board is built with
    // NO_FILESYSTEM, so the engine does not even compile the FILE* variants of
    // the PEM readers - and that is right rather than a limitation: a
    // controller keeps its identity in flash, or in a retained variable, which
    // is the caller's business and not this class's. load() and
    // certificatePem() are the whole interface there.
#if defined(MQTT_PLATFORM_HOST)
    bool loadFiles(const char* certificatePath, const char* keyPath);
    bool saveFiles(const char* certificatePath, const char* keyPath) const;

    // Load if it is there, otherwise make one and write it down. The ordinary
    // way to start on a runtime: the first boot mints an identity, every later
    // boot finds the same one, and the fingerprint does not move underneath
    // anybody.
    bool loadOrGenerate(const char* certificatePath, const char* keyPath,
                        uint32_t validDays = 3650);
#endif

    bool valid() const { return _key != nullptr && _cert != nullptr; }

    const std::string& certificatePem() const { return _certPem; }
    const std::string& privateKeyPem() const { return _keyPem; }

    // 64 lowercase hexadecimal characters, or empty if there is no identity.
    const std::string& fingerprint() const { return _fingerprint; }

    const std::string& lastError() const { return _lastError; }

private:
    void clear();
    bool fail(const char* what);
    bool capture();            // PEM text and fingerprint, from the loaded pair

    void* _key = nullptr;      // EVP_PKEY*
    void* _cert = nullptr;     // X509*

    std::string _commonName = "modbee";
    std::string _altNames;

    std::string _certPem;
    std::string _keyPem;
    std::string _fingerprint;
    std::string _lastError;
};

// The same fingerprint, computed from a certificate the caller did not make -
// which is how a client checks that the broker answering today holds the key
// it was pinned to. Empty if the PEM will not parse.
std::string mqttKeyFingerprint(const char* certificatePem);

#endif // MQTT_TLS_AVAILABLE
#endif // MODBEE_MQTT_IDENTITY_H

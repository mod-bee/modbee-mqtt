// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
//
// Deliberately no SPDX-License-Identifier: the additional terms mean this is
// not plain AGPL-3.0, and stamping that identifier on it would tell automated
// tooling the opposite of what LICENSE says.

#include "MqttCertificate.h"

#if MQTT_TLS_AVAILABLE

#include "MqttTlsSupport.h"

#include <stdio.h>
#include <string.h>
#include "../MqttNoExcept.h"

// =============================================================================
// DER, READ CAUTIOUSLY
// =============================================================================
//
// Just enough of X.690 to walk a certificate: one-byte tags, definite lengths
// of at most four bytes, and nothing else. Indefinite lengths and multi-byte
// tags are not DER and are refused rather than guessed at - the same rule the
// MQTT codecs keep, that one thing has one encoding.

namespace {

struct Cursor {
    const uint8_t* at = nullptr;
    const uint8_t* end = nullptr;
    bool empty() const { return at >= end; }
};

struct Tlv {
    uint8_t tag = 0;
    const uint8_t* content = nullptr;
    size_t length = 0;
    Cursor inside() const {
        Cursor c;
        c.at = content;
        c.end = content + length;
        return c;
    }
};

// One element from `c`, which then points past it. Every length is checked
// against what is left of the buffer before anything is read through it.
bool next(Cursor& c, Tlv& out) {
    if (c.at == nullptr || c.end - c.at < 2) return false;
    const uint8_t tag = c.at[0];
    if ((tag & 0x1F) == 0x1F) return false;          // a multi-byte tag
    size_t length = c.at[1];
    const uint8_t* body = c.at + 2;
    if (length & 0x80) {
        const size_t bytes = length & 0x7F;
        if (bytes == 0 || bytes > 4) return false;   // indefinite, or absurd
        if ((size_t)(c.end - body) < bytes) return false;
        length = 0;
        for (size_t i = 0; i < bytes; i++) length = (length << 8) | body[i];
        body += bytes;
    }
    if ((size_t)(c.end - body) < length) return false;
    out.tag = tag;
    out.content = body;
    out.length = length;
    c.at = body + length;
    return true;
}

// The next element, which must carry `tag`.
bool expect(Cursor& c, uint8_t tag, Tlv& out) {
    return next(c, out) && out.tag == tag;
}

bool sameBytes(const Tlv& t, const uint8_t* bytes, size_t length) {
    return t.length == length && memcmp(t.content, bytes, length) == 0;
}

// --- Names -------------------------------------------------------------------

const uint8_t kOidCommonName[] = {0x55, 0x04, 0x03};
const uint8_t kOidSubjectAltName[] = {0x55, 0x1D, 0x11};

struct KnownAttribute {
    uint8_t oid[10];
    size_t length;
    const char* label;
};

// The attribute types that appear in practice, under the short names
// RFC 4514 gives them. Anything else is written as its dotted OID.
const KnownAttribute kAttributes[] = {
    {{0x55, 0x04, 0x03}, 3, "CN"},
    {{0x55, 0x04, 0x06}, 3, "C"},
    {{0x55, 0x04, 0x07}, 3, "L"},
    {{0x55, 0x04, 0x08}, 3, "ST"},
    {{0x55, 0x04, 0x0A}, 3, "O"},
    {{0x55, 0x04, 0x0B}, 3, "OU"},
    {{0x55, 0x04, 0x05}, 3, "serialNumber"},
    {{0x55, 0x04, 0x09}, 3, "street"},
    {{0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x09, 0x01}, 9, "emailAddress"},
    {{0x09, 0x92, 0x26, 0x89, 0x93, 0xF2, 0x2C, 0x64, 0x01, 0x19}, 10, "DC"},
    {{0x09, 0x92, 0x26, 0x89, 0x93, 0xF2, 0x2C, 0x64, 0x01, 0x01}, 10, "UID"},
};

std::string dottedOid(const Tlv& oid) {
    if (oid.length == 0) return "?";
    char part[24];
    snprintf(part, sizeof part, "%u.%u", (unsigned)(oid.content[0] / 40),
             (unsigned)(oid.content[0] % 40));
    std::string out = part;
    unsigned long value = 0;
    for (size_t i = 1; i < oid.length; i++) {
        value = (value << 7) | (oid.content[i] & 0x7F);
        if ((oid.content[i] & 0x80) == 0) {
            snprintf(part, sizeof part, ".%lu", value);
            out += part;
            value = 0;
        }
    }
    return out;
}

void appendUtf8(std::string& out, uint32_t code) {
    if (code < 0x80) {
        out.push_back((char)code);
    } else if (code < 0x800) {
        out.push_back((char)(0xC0 | (code >> 6)));
        out.push_back((char)(0x80 | (code & 0x3F)));
    } else if (code < 0x10000) {
        out.push_back((char)(0xE0 | (code >> 12)));
        out.push_back((char)(0x80 | ((code >> 6) & 0x3F)));
        out.push_back((char)(0x80 | (code & 0x3F)));
    } else {
        out.push_back((char)(0xF0 | (code >> 18)));
        out.push_back((char)(0x80 | ((code >> 12) & 0x3F)));
        out.push_back((char)(0x80 | ((code >> 6) & 0x3F)));
        out.push_back((char)(0x80 | (code & 0x3F)));
    }
}

// A directory string as UTF-8. The byte-for-byte types are taken as they are;
// BMPString (UTF-16BE) and UniversalString (UTF-32BE) are converted, because
// a name a Windows CA wrote in a BMPString is still the name. False for a type
// that is not text at all, or for text holding a NUL - the classic trick of
// "trusted.example\0.attacker" that a C string comparison stops reading at.
bool directoryText(const Tlv& value, std::string& out) {
    out.clear();
    switch (value.tag) {
    case 0x0C:   // UTF8String
    case 0x12:   // NumericString
    case 0x13:   // PrintableString
    case 0x14:   // TeletexString
    case 0x16:   // IA5String
    case 0x1A:   // VisibleString
        out.assign((const char*)value.content, value.length);
        break;
    case 0x1E:   // BMPString
        if (value.length % 2 != 0) return false;
        for (size_t i = 0; i < value.length; i += 2) {
            appendUtf8(out, (uint32_t)((value.content[i] << 8) | value.content[i + 1]));
        }
        break;
    case 0x1C:   // UniversalString
        if (value.length % 4 != 0) return false;
        for (size_t i = 0; i < value.length; i += 4) {
            const uint32_t code = ((uint32_t)value.content[i] << 24) |
                                  ((uint32_t)value.content[i + 1] << 16) |
                                  ((uint32_t)value.content[i + 2] << 8) | value.content[i + 3];
            if (code > 0x10FFFF) return false;
            appendUtf8(out, code);
        }
        break;
    default:
        return false;
    }
    return out.find('\0') == std::string::npos;
}

// A Name: SEQUENCE OF SET OF {type, value}. Fills the one-line form and the
// Common Name, which is left empty unless there is exactly one.
bool readName(const Tlv& name, std::string& line, std::string& commonName) {
    line.clear();
    commonName.clear();
    size_t commonNames = 0;
    Cursor rdns = name.inside();
    while (!rdns.empty()) {
        Tlv set;
        if (!expect(rdns, 0x31, set)) return false;
        Cursor attributes = set.inside();
        while (!attributes.empty()) {
            Tlv pair, oid, value;
            if (!expect(attributes, 0x30, pair)) return false;
            Cursor inner = pair.inside();
            if (!expect(inner, 0x06, oid) || !next(inner, value)) return false;
            std::string text;
            const bool readable = directoryText(value, text);
            std::string label = dottedOid(oid);
            for (const KnownAttribute& known : kAttributes) {
                if (sameBytes(oid, known.oid, known.length)) { label = known.label; break; }
            }
            if (!line.empty()) line += ", ";
            line += label + "=" + (readable ? text : std::string("(not text)"));
            if (sameBytes(oid, kOidCommonName, sizeof kOidCommonName)) {
                commonNames++;
                if (readable) commonName = text;
            }
        }
    }
    if (commonNames != 1) commonName.clear();
    return true;
}

// --- Time --------------------------------------------------------------------

bool digits(const uint8_t* text, size_t count, int& out) {
    out = 0;
    for (size_t i = 0; i < count; i++) {
        if (text[i] < '0' || text[i] > '9') return false;
        out = out * 10 + (text[i] - '0');
    }
    return true;
}

// Days since 1970-01-01 of a civil date - Howard Hinnant's algorithm, which
// is exact over the whole proleptic Gregorian calendar and needs no table and
// no timegm(), which a board does not have.
int64_t daysFromCivil(int year, int month, int day) {
    year -= month <= 2 ? 1 : 0;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned yoe = (unsigned)(year - era * 400);
    const unsigned doy = (unsigned)((153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1);
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (int64_t)era * 146097 + (int64_t)doe - 719468;
}

// UTCTime (YYMMDDHHMMSSZ) or GeneralizedTime (YYYYMMDDHHMMSSZ) - the only
// two forms RFC 5280 allows in a certificate, and only in UTC.
bool readTime(const Tlv& t, std::string& iso, int64_t& seconds) {
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    const uint8_t* p = t.content;
    if (t.tag == 0x17 && t.length == 13) {
        if (!digits(p, 2, year)) return false;
        year += year < 50 ? 2000 : 1900;   // RFC 5280 4.1.2.5.1
        p += 2;
    } else if (t.tag == 0x18 && t.length == 15) {
        if (!digits(p, 4, year)) return false;
        p += 4;
    } else {
        return false;
    }
    if (!digits(p, 2, month) || !digits(p + 2, 2, day) || !digits(p + 4, 2, hour) ||
        !digits(p + 6, 2, minute) || !digits(p + 8, 2, second) || p[10] != 'Z') {
        return false;
    }
    if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 60) {
        return false;
    }
    char text[96];      // room for any int the compiler can imagine, so -Wformat-truncation has nothing to say
    snprintf(text, sizeof text, "%04d-%02d-%02dT%02d:%02d:%02dZ", year, month, day, hour,
             minute, second);
    iso = text;
    seconds = daysFromCivil(year, month, day) * 86400 + hour * 3600 + minute * 60 + second;
    return true;
}

// --- subjectAltName ----------------------------------------------------------

void readAltNames(const Tlv& octets, std::vector<std::string>& names) {
    Cursor outer = octets.inside();
    Tlv list;
    if (!expect(outer, 0x30, list)) return;
    Cursor c = list.inside();
    Tlv name;
    while (!c.empty() && next(c, name)) {
        const std::string text((const char*)name.content, name.length);
        switch (name.tag) {
        case 0x81: names.push_back("email:" + text); break;    // rfc822Name
        case 0x82: names.push_back("DNS:" + text); break;      // dNSName
        case 0x86: names.push_back("URI:" + text); break;      // uniformResourceIdentifier
        case 0x87: {                                           // iPAddress
            char ip[48];
            if (name.length == 4) {
                snprintf(ip, sizeof ip, "%u.%u.%u.%u", name.content[0], name.content[1],
                         name.content[2], name.content[3]);
                names.push_back(std::string("IP:") + ip);
            } else if (name.length == 16) {
                std::string six;
                for (size_t i = 0; i < 16; i += 2) {
                    snprintf(ip, sizeof ip, "%s%x", i ? ":" : "",
                             (unsigned)((name.content[i] << 8) | name.content[i + 1]));
                    six += ip;
                }
                names.push_back("IP:" + six);
            }
            break;
        }
        default: break;   // other kinds of name are not ones a person looks for
        }
    }
}

void readExtensions(const Tlv& wrapper, std::vector<std::string>& names) {
    Cursor outer = wrapper.inside();
    Tlv list;
    if (!expect(outer, 0x30, list)) return;
    Cursor c = list.inside();
    while (!c.empty()) {
        Tlv extension, oid, item;
        if (!expect(c, 0x30, extension)) return;
        Cursor inner = extension.inside();
        if (!expect(inner, 0x06, oid)) return;
        if (!next(inner, item)) return;
        if (item.tag == 0x01 && !next(inner, item)) return;   // the critical flag
        if (item.tag != 0x04) return;
        if (sameBytes(oid, kOidSubjectAltName, sizeof kOidSubjectAltName)) {
            readAltNames(item, names);
        }
    }
}

std::string pemCertificateError(const char* pem) {
    if (pem == nullptr || *pem == '\0') return "there is no certificate";
    if (strstr(pem, "-----BEGIN CERTIFICATE-----") == nullptr) {
        return "it is not a PEM certificate (no -----BEGIN CERTIFICATE----- line)";
    }
    return "the certificate would not parse";
}

} // namespace

// =============================================================================
// THE CERTIFICATE
// =============================================================================

bool mqttCertificateFacts(const uint8_t* der, size_t length, MqttCertificateFacts& out) {
    out = MqttCertificateFacts();
    if (der == nullptr || length == 0) return false;

    Cursor whole;
    whole.at = der;
    whole.end = der + length;
    Tlv certificate, tbs;
    if (!expect(whole, 0x30, certificate)) return false;
    Cursor c = certificate.inside();
    if (!expect(c, 0x30, tbs)) return false;

    Cursor t = tbs.inside();
    Tlv field;
    if (!next(t, field)) return false;
    if (field.tag == 0xA0 && !next(t, field)) return false;     // [0] version
    if (field.tag != 0x02) return false;                        // serialNumber
    Tlv signature, issuer, validity, subject, key;
    if (!expect(t, 0x30, signature) || !expect(t, 0x30, issuer) ||
        !expect(t, 0x30, validity) || !expect(t, 0x30, subject) || !expect(t, 0x30, key)) {
        return false;
    }

    Cursor v = validity.inside();
    Tlv notBefore, notAfter;
    if (!next(v, notBefore) || !next(v, notAfter)) return false;
    if (!readTime(notAfter, out.notAfter, out.notAfterSeconds)) return false;

    if (!readName(subject, out.subject, out.commonName)) return false;

    // What follows the key is optional: [1] and [2] unique identifiers, then
    // [3] the extensions, where the alternative names are.
    while (!t.empty()) {
        if (!next(t, field)) return false;
        if (field.tag == 0xA3) readExtensions(field, out.names);
    }

    out.fingerprint = mq_tls::sha256Hex(der, length);
    return !out.fingerprint.empty();
}

bool mqttReadCertificate(const char* pem, MqttCertificateFacts& out, std::string& error) {
    out = MqttCertificateFacts();
    if (pem == nullptr || *pem == '\0') { error = pemCertificateError(pem); return false; }
    mq_tls::ensureEngine();

    BIO* bio = BIO_new_mem_buf(pem, (int)strlen(pem));
    X509* cert = bio ? PEM_read_bio_X509(bio, nullptr, nullptr, nullptr) : nullptr;
    if (bio) BIO_free(bio);
    mq_tls::clearErrors();
    if (cert == nullptr) { error = pemCertificateError(pem); return false; }

    std::vector<uint8_t> der;
    const bool ok = mq_tls::certificateDer(cert, der) &&
                    mqttCertificateFacts(der.data(), der.size(), out);
    X509_free(cert);
    if (!ok) error = "the certificate parsed, but its contents could not be read";
    return ok;
}

size_t mqttCountCertificates(const char* pem) {
    if (pem == nullptr || *pem == '\0') return 0;
    mq_tls::ensureEngine();
    BIO* bio = BIO_new_mem_buf(pem, (int)strlen(pem));
    if (bio == nullptr) return 0;
    size_t count = 0;
    while (X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr)) {
        count++;
        X509_free(cert);
    }
    BIO_free(bio);
    // Running off the end of the text leaves "no start line" queued, which is
    // how the loop ends and not a failure - and must not be left for the next
    // TLS operation on this thread to read as its own (MqttTlsSupport.h).
    mq_tls::clearErrors();
    return count;
}

bool mqttCertificateMatchesKey(const char* certificatePem, const char* keyPem,
                               std::string& error) {
    mq_tls::ensureEngine();
    if (certificatePem == nullptr || *certificatePem == '\0') {
        error = "there is no certificate";
        return false;
    }
    if (keyPem == nullptr || *keyPem == '\0') {
        error = "there is no private key";
        return false;
    }
    BIO* certBio = BIO_new_mem_buf(certificatePem, (int)strlen(certificatePem));
    X509* cert = certBio ? PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr) : nullptr;
    if (certBio) BIO_free(certBio);
    BIO* keyBio = BIO_new_mem_buf(keyPem, (int)strlen(keyPem));
    EVP_PKEY* key = keyBio ? PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr) : nullptr;
    if (keyBio) BIO_free(keyBio);
    mq_tls::clearErrors();

    bool ok = false;
    if (cert == nullptr) {
        error = pemCertificateError(certificatePem);
    } else if (key == nullptr) {
        error = "the private key would not parse (an unencrypted PEM key is needed)";
    } else if (!mq_tls::keyMatchesCertificate(cert, key)) {
        error = "the private key does not match the certificate";
    } else {
        ok = true;
    }
    if (key) EVP_PKEY_free(key);
    if (cert) X509_free(cert);
    return ok;
}

#endif // MQTT_TLS_AVAILABLE

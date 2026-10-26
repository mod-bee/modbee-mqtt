// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#include "AuthHash.h"

#include <cstdlib>
#include <cstring>
#include <vector>

#include "ServerOs.h"

namespace auth {

namespace {

// FIPS 180-4.
const uint32_t K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
};

inline uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

struct Sha256 {
    uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                     0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    uint8_t block[64];
    size_t used = 0;
    uint64_t total = 0;

    void compress(const uint8_t* p) {
        uint32_t w[64];
        for (int i = 0; i < 16; i++) {
            w[i] = (uint32_t)p[4 * i] << 24 | (uint32_t)p[4 * i + 1] << 16 |
                   (uint32_t)p[4 * i + 2] << 8 | (uint32_t)p[4 * i + 3];
        }
        for (int i = 16; i < 64; i++) {
            const uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
            const uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }
        uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], k = h[7];
        for (int i = 0; i < 64; i++) {
            const uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            const uint32_t ch = (e & f) ^ (~e & g);
            const uint32_t t1 = k + S1 + ch + K[i] + w[i];
            const uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            const uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t t2 = S0 + maj;
            k = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += k;
    }

    void update(const uint8_t* data, size_t length) {
        total += length;
        while (length > 0) {
            const size_t take = length < 64 - used ? length : 64 - used;
            std::memcpy(block + used, data, take);
            used += take;
            data += take;
            length -= take;
            if (used == 64) {
                compress(block);
                used = 0;
            }
        }
    }

    void finish(uint8_t out[32]) {
        const uint64_t bits = total * 8;
        const uint8_t one = 0x80, zero = 0;
        update(&one, 1);
        while (used != 56) update(&zero, 1);
        uint8_t length[8];
        for (int i = 0; i < 8; i++) length[i] = (uint8_t)(bits >> (56 - 8 * i));
        update(length, 8);
        for (int i = 0; i < 8; i++) {
            out[4 * i] = (uint8_t)(h[i] >> 24);
            out[4 * i + 1] = (uint8_t)(h[i] >> 16);
            out[4 * i + 2] = (uint8_t)(h[i] >> 8);
            out[4 * i + 3] = (uint8_t)h[i];
        }
    }
};

bool fromHex(const std::string& text, std::vector<uint8_t>& out) {
    if (text.size() % 2 != 0) return false;
    out.clear();
    for (size_t i = 0; i < text.size(); i += 2) {
        int v = 0;
        for (int j = 0; j < 2; j++) {
            const char c = text[i + j];
            v <<= 4;
            if (c >= '0' && c <= '9') v |= c - '0';
            else if (c >= 'a' && c <= 'f') v |= c - 'a' + 10;
            else return false;
        }
        out.push_back((uint8_t)v);
    }
    return true;
}

// The same time whatever the contents: every byte is looked at.
bool sameBytes(const uint8_t* a, const uint8_t* b, size_t length) {
    uint8_t difference = 0;
    for (size_t i = 0; i < length; i++) difference |= (uint8_t)(a[i] ^ b[i]);
    return difference == 0;
}

std::vector<std::string> split(const std::string& text, char at) {
    std::vector<std::string> parts;
    size_t from = 0;
    for (;;) {
        const size_t next = text.find(at, from);
        parts.push_back(text.substr(from, next - from));
        if (next == std::string::npos) return parts;
        from = next + 1;
    }
}

} // namespace

void sha256(const uint8_t* data, size_t length, uint8_t out[32]) {
    Sha256 s;
    s.update(data, length);
    s.finish(out);
}

void hmacSha256(const uint8_t* key, size_t keyLength, const uint8_t* data, size_t length,
                uint8_t out[32]) {
    uint8_t k[64] = {0};
    if (keyLength > 64) sha256(key, keyLength, k);
    else std::memcpy(k, key, keyLength);
    uint8_t pad[64];
    for (int i = 0; i < 64; i++) pad[i] = (uint8_t)(k[i] ^ 0x36);
    Sha256 inner;
    inner.update(pad, 64);
    inner.update(data, length);
    uint8_t innerHash[32];
    inner.finish(innerHash);
    for (int i = 0; i < 64; i++) pad[i] = (uint8_t)(k[i] ^ 0x5c);
    Sha256 outer;
    outer.update(pad, 64);
    outer.update(innerHash, 32);
    outer.finish(out);
}

// RFC 8018 section 5.2.
void pbkdf2Sha256(const uint8_t* password, size_t passwordLength, const uint8_t* salt,
                  size_t saltLength, uint32_t iterations, uint8_t* out, size_t outLength) {
    // A salt is 16 bytes. The bound is here because GCC's release build
    // cannot see that saltLength + 4 never wraps, warns that the copy below
    // could be 2^64 bytes, and -Werror makes that warning a failed build.
    if (saltLength > 4096) {
        std::memset(out, 0, outLength);
        return;
    }
    std::vector<uint8_t> first(saltLength + 4);
    std::memcpy(first.data(), salt, saltLength);
    for (uint32_t block = 1; outLength > 0; block++) {
        first[saltLength] = (uint8_t)(block >> 24);
        first[saltLength + 1] = (uint8_t)(block >> 16);
        first[saltLength + 2] = (uint8_t)(block >> 8);
        first[saltLength + 3] = (uint8_t)block;
        uint8_t u[32], t[32];
        hmacSha256(password, passwordLength, first.data(), first.size(), u);
        std::memcpy(t, u, 32);
        for (uint32_t i = 1; i < iterations; i++) {
            hmacSha256(password, passwordLength, u, 32, u);
            for (int j = 0; j < 32; j++) t[j] ^= u[j];
        }
        const size_t take = outLength < 32 ? outLength : 32;
        std::memcpy(out, t, take);
        out += take;
        outLength -= take;
    }
}

bool randomBytes(uint8_t* out, size_t length) {
    return server_os::randomBytes(out, length);
}

std::string generateSecret(size_t length) {
    static const char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::string out;
    while (out.size() < length) {
        uint8_t byte = 0;
        if (!randomBytes(&byte, 1)) continue;
        // 248 = 62 * 4: rejecting the rest keeps every character equally likely.
        if (byte >= 248) continue;
        out.push_back(kAlphabet[byte % 62]);
    }
    return out;
}

std::string toHex(const uint8_t* data, size_t length) {
    static const char kDigits[] = "0123456789abcdef";
    std::string out;
    for (size_t i = 0; i < length; i++) {
        out.push_back(kDigits[data[i] >> 4]);
        out.push_back(kDigits[data[i] & 15]);
    }
    return out;
}

std::string hashGenerated(const std::string& secret) {
    uint8_t salt[16];
    randomBytes(salt, sizeof salt);
    Sha256 s;
    s.update(salt, sizeof salt);
    s.update((const uint8_t*)secret.data(), secret.size());
    uint8_t hash[32];
    s.finish(hash);
    return "sha256$" + toHex(salt, sizeof salt) + "$" + toHex(hash, sizeof hash);
}

std::string hashChosen(const std::string& password, uint32_t iterations) {
    uint8_t salt[16];
    randomBytes(salt, sizeof salt);
    uint8_t hash[32];
    pbkdf2Sha256((const uint8_t*)password.data(), password.size(), salt, sizeof salt, iterations,
                 hash, sizeof hash);
    return "pbkdf2-sha256$" + std::to_string(iterations) + "$" + toHex(salt, sizeof salt) + "$" +
           toHex(hash, sizeof hash);
}

bool verify(const std::string& stored, const uint8_t* offered, size_t offeredLength) {
    const std::vector<std::string> parts = split(stored, '$');
    std::vector<uint8_t> salt, expected;
    uint8_t actual[32];
    if (parts.size() == 3 && parts[0] == "sha256") {
        if (!fromHex(parts[1], salt) || !fromHex(parts[2], expected) || expected.size() != 32) return false;
        Sha256 s;
        s.update(salt.data(), salt.size());
        s.update(offered, offeredLength);
        s.finish(actual);
    } else if (parts.size() == 4 && parts[0] == "pbkdf2-sha256") {
        const unsigned long iterations = std::strtoul(parts[1].c_str(), nullptr, 10);
        if (iterations == 0 || iterations > 10000000ul) return false;
        if (!fromHex(parts[2], salt) || !fromHex(parts[3], expected) || expected.size() != 32) return false;
        pbkdf2Sha256(offered, offeredLength, salt.data(), salt.size(), (uint32_t)iterations, actual, 32);
    } else {
        return false;
    }
    return sameBytes(actual, expected.data(), 32);
}

} // namespace auth

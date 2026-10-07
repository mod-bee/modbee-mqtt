// ModBee MQTT.
//
// Licensed under the GNU Affero General Public License, Version 3, WITH
// ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

#pragma once

// Hashing for device logins: SHA-256, HMAC-SHA256, PBKDF2-HMAC-SHA256, and
// the two stored forms - written here, small and dependency-free, so the
// login store needs no crypto library and builds the same with or without a
// TLS engine.
//
//   "sha256$<salt>$<hash>"           a secret modbee GENERATED: 32 random
//                                    characters cannot be guessed, so one
//                                    salted SHA-256 protects it fully - and a
//                                    million reconnects cost seconds
//   "pbkdf2-sha256$<n>$<salt>$<hash>" a password a PERSON chose: 4096 rounds,
//                                    EMQX's default (Mosquitto uses 1000)
//
// Salt and hash are hex. Verification runs over the whole value whatever the
// offered password is, so it takes the same time right or wrong.

#include <cstddef>
#include <cstdint>
#include <string>

namespace auth {

void sha256(const uint8_t* data, size_t length, uint8_t out[32]);
void hmacSha256(const uint8_t* key, size_t keyLength, const uint8_t* data, size_t length,
                uint8_t out[32]);
void pbkdf2Sha256(const uint8_t* password, size_t passwordLength, const uint8_t* salt,
                  size_t saltLength, uint32_t iterations, uint8_t* out, size_t outLength);

// From the kernel (getrandom). False only if the kernel would not give any.
bool randomBytes(uint8_t* out, size_t length);

// A secret for a device or a provisioning key: `length` characters from
// [A-Za-z0-9], every one uniformly random.
std::string generateSecret(size_t length = 32);

std::string hashGenerated(const std::string& secret);
std::string hashChosen(const std::string& password, uint32_t iterations = 4096);
bool verify(const std::string& stored, const uint8_t* offered, size_t offeredLength);

std::string toHex(const uint8_t* data, size_t length);

} // namespace auth

#pragma once

// PLACEHOLDER. Replace this file with the one SSLClient's trust anchor tool
// generates from your broker's certificate authority:
//
//   https://openslab-osu.github.io/bearssl-certificate-utility/
//
// It produces exactly this shape - an array of trust anchors and a count - so
// the sketch beside it needs no change at all.
//
// WHY THIS IS NOT OPTIONAL. A TLS client with no trust anchor has nothing to
// check the broker against, and "it connected" then means only that something
// answered on port 8883. The point of the certificate is to know WHO answered.

#include <SSLClient.h>

// Empty on purpose, so a sketch that still has the placeholder compiles and
// then fails to verify anything - which is far easier to diagnose than one
// that silently trusts everybody.
static const br_x509_trust_anchor* TAs = nullptr;
static const size_t TAs_NUM = 0;

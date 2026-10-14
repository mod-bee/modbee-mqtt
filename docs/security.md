# Security

MQTT has no security of its own. Every credential, every topic name and every
payload travels in clear on a plain connection, and the standard says so
(V5:5, V311:5). TLS is what makes any of it private, and this document is about
what it does and does not prove.

---

## Which engine, and why you almost never have to care

TLS needs a cryptography library, and this one uses whichever is installed.
Nothing is configured; `src/MqttFeatures.h` asks the compiler.

| | |
| --- | --- |
| `<wolfssl/ssl.h>` present | wolfSSL is the engine |
| otherwise `<openssl/ssl.h>` on a host | OpenSSL is the engine |
| neither | `MqttTlsTransport` compiles to nothing and asking for TLS reports on a pin |

**wolfSSL first, and the reason is the board.** It is the only engine that does
the whole job there: it builds for every Arduino architecture, it can serve
TLS, and — the part that decides it — it can **write** an X.509 certificate.
BearSSL ships three certificate decoders and no writer at all, so a controller
running BearSSL could never mint an identity of its own, and a broker with no
identity cannot serve TLS to anybody. Using the same engine on the host means a
runtime and a board run the same code and fail the same way, rather than being
two stories to document and two to debug.

**OpenSSL stays** because it is already proven here against three public
brokers, and because a host that would rather use the system library it already
patches should be able to. Every test in this repository passes on both.

`MQTT_TLS_ENGINE_NAME` is the name, for a log line or a status pin.

```bash
# Point CMake at a wolfSSL that is not on the default paths:
cmake -S . -B build -Dwolfssl_DIR=<prefix>/lib/cmake/wolfssl
```

One sharp edge, stated before somebody meets it: **the header and the link line
must agree.** The compiler decides from what it can `#include`; the linker is
told by whoever wrote the link line. A machine with wolfSSL's headers and
`-lssl -lcrypto` on the command line compiles one engine and links the other,
and the result is a page of undefined references naming the engine that was
*not* chosen. CMake gets this right by itself.

---

## TLS is a transport

```cpp
// Host, wherever an engine is present - no build flag
MqttTlsTransport transport;
transport.configure("broker.example.com", 8883);
transport.setCaFile("/etc/ssl/certs/plant-ca.crt");
```

```cpp
// An ESP32, over ANY channel - Wi-Fi, Ethernet or a modem's plain socket. The
// TLS is done on the controller by the mbedTLS the core already links, with
// the same checks as MqttTlsTransport: validate, pin one key, or neither.
WiFiClient plain;
MqttChannelTlsTransport<WiFiClient> tls(plain, "broker.example.com", 8883);
tls.setCaCertificate(ROOT_CA);
MqttDevice mqtt(tls);
```

```cpp
// Arduino, on a board with native TLS (ESP32, ESP8266). A secure channel is an
// ordinary Arduino network channel, so the same template covers it and TLS
// costs no extra code at all.
WiFiClientSecure secure;
secure.setCACert(ROOT_CA);
MqttDevice mqtt(secure);
```

```cpp
// Arduino, on a board whose network chip has NO TLS - which is most Ethernet
// chips and most cellular modems in transparent mode. SSLClient wraps any
// Arduino Client in BearSSL and is itself one, so again nothing else changes.
EthernetClient wired;
SSLClient secure(wired, TAs, TAs_NUM, A5);   // A5 must be UNCONNECTED: entropy
MqttDevice mqtt(secure);
```

**TLS is not a WiFi feature.** It is available over wired Ethernet and over
cellular exactly as it is over a radio link — see
[transports.md](transports.md), which has the cost of each.

Nothing above the transport changes. The client does not know whether its bytes
are encrypted, which is why WebSocket and anything else can be added later as
another transport rather than as a change to the protocol layer.

---

## Validation is on by default

And the opt-out is named at length on purpose:

```cpp
transport.setInsecureNoCertificateValidation();
```

That call turns a TLS connection into an encrypted conversation with **whoever
answered the port**. A short name for it — `setInsecure()`, as several libraries
use — reads in a diff as a configuration detail rather than as the security
decision it is, and the person who typed it six months ago will not be the one
reading the incident report.

The transport reports what actually happened rather than what was asked for:

```cpp
transport.certificateVerified();   // false after an insecure connection
```

An application that logs "TLS: verified" after validation was disabled has put
a false assurance in an audit trail.

---

## Two checks, not one

Chain verification and hostname verification are **different**, and the second
is the one most often left out. A certificate can be perfectly valid, signed by
a CA you trust, and issued to somebody else entirely.

This library does both. What each case means, and what it would mean if it
behaved the other way:

| Case | Expected | Why |
| --- | --- | --- |
| Valid chain, correct hostname | connects | otherwise TLS is unusable |
| Signed by an untrusted CA | **refused** | accepting one means any machine that answers the port can impersonate the broker |
| Expired certificate | **refused** | a rotated or revoked key stays usable for ever otherwise |
| Valid chain, **wrong hostname** | **refused** | chain verification alone does not say you are talking to the right machine |
| No CA configured, private CA in use | **refused** | the system store does not contain your plant's CA |
| Validation explicitly disabled | connects, and reports unverified | |
| Client certificate required and supplied | connects | |
| Client certificate required and absent | **refused** | |

Every refusal carries its reason, so a failure is diagnosable rather than just
red:

```
expired      is refused
             reason: TLS handshake failed: certificate has expired
wronghost    is refused
             reason: TLS handshake failed: hostname mismatch
```

The certificates are generated by the rig, never committed.

---

## TLS 1.2 is the floor

`SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION)`. Everything below it
has known breaks, and a plant that has to talk to something older should say so
deliberately rather than have a library quietly agree to it.

---

## A note on Arduino

**An ESP32 does not judge certificate dates** - not through `WiFiClientSecure`
and not through `MqttChannelTlsTransport`, because the core's mbedTLS is built
without a calendar. So "it connected" is weaker evidence there than a developer
expects: an expired certificate that this library refuses on a host may be
accepted on a board. It is deliberate rather than an oversight - a controller
that booted believing it is 1970 must still connect - and it is the reason
`MQTT_TLS_PINNED` skips dates on a host too.

**`MqttChannelTlsTransport` is tested off the board** (`test/test_channel_tls`)
against an mbedTLS configured as the core's is, built by
`scripts/host-mbedtls.sh`: every check and every refusal, RSA, P-256 and P-384
chains, mutual TLS and a whole MQTT session over one-byte reads. What that
cannot prove is the core's own build of the engine, so the board build and a
bench connection remain part of a release.

**`SSLClient` needs real entropy.** Its last constructor argument is an analog
pin, and that pin must be **unconnected**. Wired to anything with a steady
voltage, the seed is predictable and the session keys are predictable with it —
a TLS connection that looks identical from the outside and is not secure.

**`SSLClient` costs roughly 40–50 kB of flash** plus its buffers. On an UNO R4
that is most of what is spare, which is a reason to prefer a board
with native TLS where the choice exists — not a reason to give up on
encrypting a wired link.

Those are the platforms' behaviour, not something this library can override,
and they are stated here rather than left to be discovered. The Arduino side is not
covered by the automated TLS matrix for the same reason it is not covered by the
broker rig: it cannot run in a container. That is said here rather than left as
implied coverage.

---

## Why a board refused: `MqttTlsError`

`MqttChannelTlsTransport` reports a refusal as a value, not as mbedTLS's own
sentences: `mbedtls_strerror()` and its text for every code in every module
cost about 11.7 kB of a board's flash and say no more than this list does.

```cpp
tls.lastErrorCode();    // MqttTlsError, below
tls.lastEngineCode();   // the mbedTLS code (negative), or 0 when it is not the engine's
tls.lastError();        // both as one line for a status pin, at most 38 characters:
                        //   "TLS: broker refused us -0x7780"
mq_tlsErrorText(MqttTlsError::WrongHost);   // the line alone
```

The numbers are fixed; a value may be added at the end, never moved. The line
is at most 30 characters, and is followed by ` -0x` and the engine's code in
hexadecimal when there is one.

| # | `MqttTlsError` | Line | What it means, and what to do |
| ---: | --- | --- | --- |
| 0 | `None` | | nothing has failed |
| 1 | `NoHost` | no host or port configured | the transport was begun with no host or port |
| 2 | `Unreachable` | cannot connect to the broker | the plain channel would not open: nothing listening, no network, the modem not registered |
| 3 | `NoMemory` | TLS: out of memory | the heap could not hold the session (about 40 kB; in PSRAM where the board has it) |
| 4 | `NoCa` | TLS: no CA certificate given | validating with no CA: give one, or pin the broker's key |
| 5 | `BadCa` | TLS: CA certificate unreadable | the CA is not PEM, or not a certificate; the code says where it stopped |
| 6 | `BadClientCertificate` | TLS: client cert/key unusable | a certificate without its key, or either will not parse |
| 7 | `EngineSetup` | TLS: engine would not start | the random generator or the session would not initialise - a fault, not a configuration |
| 8 | `Untrusted` | TLS: certificate not trusted | the broker's chain does not lead to the CA given - a different CA, or a server that omits its intermediate |
| 9 | `WrongHost` | TLS: wrong host name | the certificate is for another name: use the name it was issued to, not the address |
| 10 | `BadCertificate` | TLS: bad certificate | the certificate failed another check (key usage, signature), or its message would not parse |
| 11 | `NoPeerCertificate` | TLS: broker sent no cert | pinned, and the broker presented no certificate to compare |
| 12 | `PinMismatch` | TLS: not the pinned key | the broker was replaced, or the address is wrong; `peerKeyFingerprint()` is what it showed |
| 13 | `Refused` | TLS: broker refused us | the broker ended the handshake with an alert - most often it wants a client certificate, or refuses the one given |
| 14 | `NoCommonProtocol` | TLS: no common version/cipher | no TLS 1.2 or no cipher suite in common: a broker set to TLS 1.3 only, or to suites the board does not offer |
| 15 | `Handshake` | TLS handshake failed | any other handshake failure; the code says which |
| 16 | `Timeout` | TLS handshake timed out | the handshake was given up before it finished - the client's connect timeout on a slow or lossy link |
| 17 | `Closed` | broker closed the connection | the broker said goodbye (close_notify, or end of stream) |
| 18 | `Dropped` | TLS: connection dropped | the connection went in the middle of a record |
| 19 | `ReadFailed` | TLS read failed | any other failure reading; the code says which |
| 20 | `WriteFailed` | TLS write failed | any other failure writing; the code says which |

The engine code is mbedTLS's own number, so it can be looked up in mbedTLS's
headers (`include/mbedtls/ssl.h`, `x509.h`, ...) or with its
`programs/util/strerror` on a host. `mq_tlsErrorFromEngine()` is the mapping,
and `test/test_channel_tls` holds it to this table. A host's `MqttTlsTransport`
keeps OpenSSL's or wolfSSL's own words, which cost a host nothing.

---

## Which CA, and one trap

With no CA file configured, the transport falls back to the operating system's
own trust store — `SSL_CTX_set_default_verify_paths()` — which is what a device
in a plant actually has.

Supply one when the broker uses a private CA, which most plant brokers do:

```cpp
transport.setCaFile("/etc/modbee/site-ca.crt");   // a file
transport.setCaCertificate(ROOT_CA_PEM);          // or a PEM in flash
```

**One trap worth knowing before you go looking for a defect that is not there.**
Public demo brokers often present a certificate signed by their **own** CA, not
by a public authority. Validating one of those against the system trust store
fails — **correctly** — with "unable to get local issuer certificate".

That is your client working. Reaching for `setCaFile()` to make it go away
turns a check of the public chain into a check of a chain somebody handed you.
If you want to prove that validation works against a real public authority,
point at a port that actually presents one.

---

## Pinning: when nobody signed for the broker

A CA answers "is this certificate vouched for". On a plant broker nobody
vouched for anything — a ModBee PLC serving its own certificate, or a site
Mosquitto somebody generated one for on a Tuesday — and there is no authority
to ask. Pinning trusts **one key** instead.

```cpp
MqttTlsTransport transport;
transport.configure("10.0.0.10", 8883);
transport.setPinnedKeyFingerprint(
    "23c3ca67edd89db88be43bc88bd9806588f1766bd272e9e701c659365eb3ca1a");
```

64 hexadecimal characters: SHA-256 over the certificate's DER
SubjectPublicKeyInfo. Colons, spaces and capitals are accepted, because
`openssl x509 -fingerprint` prints them one way and this library prints them
another, and a plant link that refused to come up over punctuation would be
blamed on TLS rather than on formatting.

**It is of the KEY, not of the certificate**, which is RFC 7469's reasoning: a
certificate expires and gets reissued, and a key does not have to — so a pin
written down at commissioning still means something afterwards.

**Pinning replaces chain and hostname validation rather than adding to it**, and
that includes the expiry date. Deliberately: a controller with no clock cannot
judge one, and a link that drops because a PLC booted believing it is 1970 is
worse than one that trusts a key it recognises.

### Do not pin a cloud broker

AWS IoT, HiveMQ and the rest renew every ninety days, and the key usually
renews with the certificate. A pinned cloud link breaks quarterly, in the
field, at whatever hour the renewal ran. Use `setCaFile()` or the system store
— which is already proven here, 9/9 against three public brokers with no
configuration at all.

### Learning the key the first time

Bring the link up once without a pin and read what it was shown:

```cpp
transport.setInsecureNoCertificateValidation();   // once, while commissioning
// ...connect...
const std::string key = transport.peerKeyFingerprint();   // store this
```

`peerKeyFingerprint()` is filled in whether or not anything is pinned, so an
application can always report what it was actually shown rather than what it
hoped for. A mismatch afterwards fails with **both** fingerprints in
`lastError()` on a host — because the useful question is always which one
moved, a broker that was reinstalled or an address that is wrong. On a board
it fails with `MqttTlsError::PinMismatch`, and `peerKeyFingerprint()` still
holds the key that was presented, to set beside the one that was pinned.

---

## Serving TLS

`MqttTlsListener` is `MqttTcpListener`'s encrypted peer: the broker cannot tell
them apart, so one broker can serve 1883 and 8883 at once and a subscriber on
one receives what a publisher on the other sends.

```cpp
MqttIdentity identity;                       // an EC P-256 key and a
identity.setCommonName("plc-01");            // certificate of its own
identity.setSubjectAltNames("DNS:plc-01,IP:10.0.0.10");
identity.loadOrGenerate("plc.crt", "plc.key");

MqttTlsListener secure;
secure.configure("0.0.0.0", 8883, 32);
secure.useCertificate(identity.certificatePem().c_str(),
                      identity.privateKeyPem().c_str());
secure.begin();

MqttBroker broker;
broker.begin();
broker.addListener(secure);
```

`identity.fingerprint()` is what clients pin against. `loadOrGenerate` mints one
on the first start and finds the same one every start after, so the number
somebody wrote down stays true.

### Three ways to hold a certificate, and self-signed is the default not the ceiling

| the broker holds | how it got there | clients use |
| --- | --- | --- |
| a generated self-signed key | `loadOrGenerate`, zero configuration | the pin |
| a real CA-signed certificate | files, named on the listener | ordinary validation |
| an enterprise-CA certificate | the same, from the site's own CA | ordinary validation |

A public certificate is worth having and painful to keep: Let's Encrypt renews
every ninety days and a PLC is a poor host for a renewal agent. The realistic
shapes are a **long-lived certificate from the site's own CA**, or the
self-signed default. Both are first-class; neither is a workaround.

### Mutual TLS

```cpp
secure.requireClientCertificate(caPem);   // and the handshake demands one
secure.requestClientCertificate(caPem);   // or asks, verifies one if offered, and lets none through
```

A certificate that is offered and does not verify fails the handshake either
way. One that does verify reaches the authorisation hook as its CN and
fingerprint (`MqttBrokerRequest::certificateName`, [broker.md](broker.md)),
which is how `modbee serve` lets a certificate be a login
([logins.md](logins.md#logging-in-with-a-certificate)).

**With a client CA, wolfSSL makes every connection a full handshake.** A
resumed session skips the certificate exchange, and what wolfSSL reports as
the peer's certificate afterwards depends on how it was built - so a device
that resumed could arrive with no verified identity. Resumption is turned off
for a listener that verifies clients on that engine. OpenSSL keeps the peer's
certificate in the session, and resumes as usual.

### The broker image's wolfSSL

`docker/Dockerfile` builds wolfSSL from a pinned release (`WOLFSSL_VERSION`
and `WOLFSSL_SHA256`, changed together) with exactly what this library asks of
it, and says why beside each option: `opensslall` and `opensslextra` (the
OpenSSL-named API the transports are written against), `certgen`, `certext`
and `keygen` (`MqttIdentity`), `tls13`, `session-ticket`, `sni`,
`curve25519` (the key exchange today's clients offer first), and the
assembly paths. One difference from OpenSSL remains: across the shards of
`modbee serve` OpenSSL shares one context and so one session-ticket key,
while each wolfSSL shard has its own - a device that reconnects to another
shard makes a full handshake rather than resuming.

---

## Credentials, and the order they go in

```cpp
mqtt.secure("/etc/ssl/certs/ca-certificates.crt");   // FIRST
mqtt.credentials("plant-operator", "s3cret");        // then this
mqtt.connect("broker.example.com", "gateway-1");
```

**A user name and password travel in the CONNECT, in the clear, unless the
transport is TLS.** There is no challenge, no nonce and no hashing anywhere in
MQTT: the password is bytes in the first packet the client sends. On a plant
segment nobody else is on, that may be a reasonable trade. On anything that
leaves the building it is a password published to whoever is on the path.

The two calls are independent — `credentials()` does not imply `secure()` and
nothing warns you — so the order above is a habit worth having rather than
something the library enforces.

**A rejected credential backs right off — but never gives up.** The refusal
arrives as `0x86 Bad User Name or Password` or `0x87 Not Authorized` in the
CONNACK. The client does not repeat it at its network backoff, because a device
sending a rejected password every second is how it gets itself banned — by
address, on some brokers, which takes the whole site with it. It retries every
`MqttClientConfig::rejectedCredentialsRetryMs` instead, five minutes by
default: twelve attempts an hour, which no rate limiter notices, and a remote
installation whose broker is fixed at the other end comes back by itself rather
than needing a site visit. Zero restores the old behaviour of standing down for
good. `retryRejectedCredentials` retries at the ordinary backoff instead, for
a broker whose access list loads after it starts listening.

Reading the refusal: `status().lastReasonCode` is what the broker said, and at
3.1.1 it is that version's return code (4 for bad credentials, 5 for not
authorised) rather than a 5.0 reason code.

**Enhanced authentication is not implemented.** MQTT 5.0's AUTH packet and its
`Authentication Method` property are decoded and refused with `0x8C`, so a
client that asks for SCRAM is told no rather than told nothing. User name and
password is the whole of what this library can send.

---

## What TLS does not do

- **It does not authenticate the client**, unless you configure a client
  certificate. A user name and password over TLS is a password over TLS.
- **It does not authorise anything.** Which topics a client may publish to or
  subscribe to is the broker's business. If you are running
  [our broker](broker.md), that is what `onAuthorise` is for — and
  `allowAnonymous = false` is enforced before any hook runs, so a deployment
  that turns anonymous access off is safe even with no hook installed at
  all.
- **It does not make a retained message trustworthy.** A connected, verified
  broker will happily serve a retained value written by a device that died last
  Tuesday. See the freshness discussion in
  [getting-started.md](getting-started.md).

---

## Payloads

The library validates **topics** — V5:4.7, UTF-8, wildcards, the `$` rule — and
never validates payloads. A payload is bytes and interpreting it is the
application's business, which is what lets a Sparkplug protobuf message or an
encrypted blob pass through untouched.

If your payloads carry commands, sign or authenticate them at the application
layer. TLS protects them in transit between two endpoints; it says nothing
about what a compromised endpoint publishes.

# Transports — WiFi, Ethernet, cellular, and TLS on all three

**This library is not WiFi-only.** It talks to a broker over anything that
carries an ordered, reliable stream of bytes, and on Arduino that means
anything that is an Arduino `Client`.

For a long time every example here used WiFi, which made the library *read* as
WiFi-only. It was never true, and a panel in a plant room is far more likely to
be wired or cellular than on a radio link.

---

## How it works, in one paragraph

On Arduino, `MqttStreamTransport<ChannelType>` is a template over the network
channel. It calls exactly six methods — `connect`, `stop`, `available`,
`read(buf, size)`, `write`, `connected` — which is the whole of the Arduino
`Client` interface that matters. Anything implementing them works, today, with
no change to this library and no change to your sketch beyond the two lines
that name the channel.

**`read` here means the BULK one**, `int read(uint8_t* buf, size_t size)`,
which every Arduino `Client` has: `WiFiClient`, `EthernetClient`,
`WiFiClientSecure`. `MqttStreamTransport` uses it because reading a packet one
byte at a time through a virtual call costs more than the packet does.

If you are writing your own `MqttTransport` rather than using a `Client`, you
may implement either the bulk pair or the byte-wise `available()`/`read()` —
the base class bridges whichever you leave out. See `docs/extending.md`.

Note that `Stream::readBytes()` is deliberately **not** used, even though it
looks like the bulk read. It blocks until it has what was asked for or its
timeout elapses — one second by default — and nothing in this library may
block: `poll()` is called once per PLC scan.

`MqttDevice` takes the channel directly, so in practice you write:

```cpp
EthernetClient net;
MqttDevice mqtt(net);
```

and everything else is identical to every other example.

---

## The channels

| Channel | What it is | Notes |
| --- | --- | --- |
| `WiFiClient` | ESP32, ESP8266, UNO R4 WiFi, MKR WiFi, Pico W | |
| `WiFiClientSecure` | the same, with native TLS | ESP32/ESP8266 |
| `EthernetClient` | W5100, W5500, ENC28J60 shields and modules | **what a panel usually is** |
| `TinyGsmClient` | 2G / LTE-M / NB-IoT modems, via TinyGSM | remote sites |
| `SSLClient` | TLS over *any* of the above, in software | boards with no native TLS |
| anything else | your own `Client` subclass | LoRa-over-IP, RS-232-to-Ethernet, a test double |

On a host (Linux, macOS, Windows, a container) there is no channel to supply —
the sockets are already there:

```cpp
MqttDevice mqtt;                      // plain TCP
mqtt.secure("/etc/ssl/certs/ca.crt"); // TLS, wherever OpenSSL is present
```

---

## Wired Ethernet

The one most plant panels actually use, and the one every MQTT tutorial skips.

```cpp
#include <Ethernet.h>
#include <ModBeeMqtt.h>

byte MAC[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0x01 };
IPAddress STATIC_IP(192, 168, 1, 51);

EthernetClient net;
MqttDevice mqtt(net);

void setup() {
    Ethernet.begin(MAC, STATIC_IP);          // blocks; belongs in setup()
    mqtt.statusTopic("plant/tank-01/status");
    mqtt.connect("192.168.1.10", "tank-01");
}

void loop() {
    mqtt.poll();
    Ethernet.maintain();      // renews a DHCP lease; harmless on a static address
}
```

Worth knowing:

- **Use a static address** unless you have a reason not to. A plant network
  rarely runs DHCP, and a device that cannot get a lease sits there publishing
  into nothing.
- **`Ethernet.maintain()` in the loop** if you *are* on DHCP. Without it the
  lease expires and the link dies in a way that looks like a broker fault.
- **Check `Ethernet.hardwareStatus()`** once at start-up. A shield that is not
  seated reports `EthernetNoHardware`, and every other symptom looks like a
  network problem.
- **Each device needs its own MAC address.** Two boards flashed with the same
  sketch and the same MAC is an afternoon nobody enjoys.

Full example: `examples/arduino/Tank_Level_Ethernet`.

---

## Cellular, through TinyGSM

```cpp
#define TINY_GSM_MODEM_SIM7600
#include <TinyGsmClient.h>
#include <ModBeeMqtt.h>

TinyGsm modem(Serial1);
TinyGsmClient net(modem);
MqttDevice mqtt(net);
```

The MQTT part is unchanged. What changes is everything around the timing, and
it changes enough to deserve its own settings:

```cpp
mqtt.rememberSession(3600);   // an outage becomes a pause, not a loss
mqtt.queueSize(64);           // room to hold readings through one
mqtt.keepAlive(300);          // fewer pings; a plan is sold by the megabyte
mqtt.autoReconnect(false);    // not on an ESP32 - see "the one place something blocks"
```

**Keep alive is a trade, not a default.** It is the only thing that notices a
link that went away without saying so — a bearer that vanished, a NAT entry
that timed out — so a longer interval is less data and a slower failure. Pick
it deliberately. A broker's Server Keep Alive can shorten it but never switch it
off or stretch it - see [Slow links](#slow-links-keepalive-and-acknowledgements).

Full example: `examples/arduino/Remote_Site_Cellular`.

---

## THE ONE PLACE SOMETHING BLOCKS, AND IT IS NOT OURS

This library's guarantee is that **nothing in it waits for the network**,
`connect()` included. That is a guarantee about this library. It cannot make
somebody else's network stack asynchronous, and pretending otherwise would be
worse than saying so.

**Arduino's `Client::connect()` is synchronous on every core that implements
it.** It does a DNS lookup and a TCP handshake and returns when they are done.

| Channel | Roughly what `connect()` costs |
| --- | --- |
| `WiFiClient`, on an associated network | milliseconds |
| `EthernetClient`, static address, local broker | milliseconds |
| `EthernetClient`, DNS name | one DNS round trip, plus the handshake |
| `WiFiClientSecure` / `SSLClient` | the above, plus a TLS handshake — the slow part on a small board |
| `TinyGsmClient` | **seconds** — it is a sequence of AT commands (TinyGSM's own limit is 75 s; the library tells it the client's connect timeout, 20 s by default) |
| `MqttChannelTlsTransport` over any of these | only the plain channel's `connect()`; the TLS handshake runs a step per `poll()` and never waits for the network, though a step's arithmetic is the poll's |

**The connect is bounded by the client's connect timeout where the channel can
be told one**: TinyGSM's `connect(host, port, seconds)`, and the ESP32 core's
`WiFiClientSecure` handshake (`setHandshakeTimeout`, 120 s by its own default).
Only ever shortened, never lengthened; any other channel connects exactly as
before.

### Where it can happen

`begin()` on the transport is the only call that can stall, and it is reached
from exactly two places:

1. **`MqttDevice::connect()`** — which you call, from `setup()` or from
   somewhere in your own control, so the stall is where you expect one.
2. **A reconnect attempt inside `poll()`** — which is inside your loop.

Everything else — publishing, receiving, the keepalive, servicing the queue —
is non-blocking in this library, on every channel. **The channel itself may
not be**: `TinyGsmClient`'s `write()`, `available()` and `connected()` are AT
command exchanges with the modem, and take hundreds of milliseconds - seconds,
when the modem's buffer is full on a slow link. On a cellular board, the
modem's own PPP mode with the board's lwIP sockets (an ordinary `WiFiClient`-
style client over the PPP interface) avoids that, where the modem and core
support it.

**What the library does about a channel's `write()`.** An Arduino channel's
`write()` returns when it has taken everything or given up, and on a dead link
giving up is slow: the ESP32 core's `WiFiClient` waits up to ten seconds for
room, its `WiFiClientSecure` until the socket timeout. So `MqttStreamTransport`
(and `MqttChannelTlsTransport` underneath its TLS):

- hands the channel at most `MQTT_STREAM_WRITE_CHUNK` (1024) bytes per call;
- on an ESP32, does not call `write()` at all when the socket has no room
  (`select()` on the channel's `fd()`), so a full socket costs nothing;
- stops writing for this `poll()` once it has spent
  `MQTT_STREAM_WRITE_BUDGET_MS` (20 ms) in `write()` - the rest goes on the
  next poll, from exactly where it stopped.

A 200 kB publish through a modem is therefore spread over many polls instead
of being one `poll()` that lasts the whole transfer.

### What to do about the second one

If your loop can afford it — most sketches can, and a WiFi or wired reconnect
is milliseconds — do nothing. Auto-reconnect is on by default and it is the
right answer.

If it cannot — a scan cycle with a deadline, or a cellular modem where the
stall is seconds — turn it off and reconnect on your own terms:

```cpp
mqtt.autoReconnect(false);

void loop() {
    mqtt.poll();                        // never stalls

    if (!mqtt.isConnected() && millis() >= nextAttempt) {
        nextAttempt = millis() + backoff;    // and back off, so a site with no
        if (backoff < 300000) backoff *= 2;  // coverage does not find out
                                             // once a second, for ever
        mqtt.connect(BROKER, "bore-7");      // the one call that may stall
    }
}
```

`examples/arduino/Remote_Site_Cellular` does exactly this, including bringing
the bearer back up.

### On an ESP32: the connect runs on a task of its own - automatically

Every ESP32 runs FreeRTOS, so there the stall is moved off your loop
altogether, **and `MqttDevice` and the Sparkplug classes now do it for the
channel you give them**: `MqttDevice mqtt(net)` on an ESP32 puts `net`'s
connect on a short task of its own (`MqttConnectTask`, 8 kB of stack while an
attempt runs, nothing while the link is up). Auto-reconnect can stay on; a
reconnect no longer stops `loop()` for the modem's connect time. A Sparkplug
edge node, which always reconnects by itself, gets the same.

**The one thing this asks of a sketch: while `mqtt.connecting()` is true,
leave the channel and the modem alone.** The attempt is using them on another
task, and TinyGSM is not safe to call from two tasks at once - asking the modem
for its signal or bearer state mid-attempt interleaves its AT commands with
the connect's. Guard such calls:

```cpp
if (!mqtt.isConnected() && !mqtt.connecting() && !modem.isGprsConnected()) {
    bringBearerUp();
}
```

or keep the old behaviour - the stall, in your loop - with
`mqtt.connectInBackground(false)`. `connect()` itself returns false while an
attempt is still opening the channel; the running attempt carries on.

If the heap has no room for the task's stack, that attempt opens the channel
inline instead, as before - a device that could never connect again because
its heap was fragmented would be worse than one stall.

The rest of this section is how to do the same by hand, for a transport you
build yourself. `MqttConnectTask` wraps the transport that opens the medium; its
`begin()` starts a short task that calls the medium's `begin()` - and, for
`MqttChannelTlsTransport`, drives the whole handshake - then hands the medium
back. Meanwhile the client is `Connecting`, `poll()` returns at once, and the
connect timeout and the reconnect backoff are the client's as always. An open
that fails is answered exactly as a `begin()` that returned false.

```cpp
WiFiClient wifi;
MqttStreamTransport<WiFiClient> stream(wifi, "broker.local", 1883);
MqttConnectTask task;                 // MQTT_CONNECT_STACK bytes, internal RAM
task.wrap(stream);
client.begin(task, connection);       // auto-reconnect stays on
```

While an attempt runs the task owns the medium: the wrapper answers "nothing
yet" for it, and an `end()` asked for meanwhile is done by the task when the
medium's connect returns. Never free the medium while `busy()`: hand it to
`retire()`, which frees it after the attempt. The task exists only while an
attempt runs. `MQTT_CLIENT`, the OpenPLC block, does all of this on an ESP32.
A board without threads (`MQTT_CONNECT_TASK` 0) keeps the stall where it was.
`MqttSparkplugNode` reconnects by itself; given a channel on an ESP32 it now
puts it on a task as above, and a transport you build yourself should be
wrapped in `MqttConnectTask` this way - it cannot use the `autoReconnect(false)`
pattern above.

On a **host** none of this applies: `MqttTcpTransport` and `MqttTlsTransport`
are non-blocking throughout, connect included. The limitation is Arduino's
`Client` interface, not the design.

---

## How a host dials: every address, raced, and DNS that is allowed to fail

`MqttTcpTransport` and `MqttTlsTransport` (Linux gateways, containers, a PLC
runtime) look the name up and connect without ever waiting inside `poll()`.
Two things make that hold up on a flaky link.

### Every address, raced (RFC 8305, Happy Eyeballs)

A name with several addresses is not tried one at a time. The addresses are
put in an order that alternates IPv6 and IPv4, the first is dialled, and if it
has not connected within **250 ms** (`MQTT_CONNECT_ATTEMPT_DELAY_MS`) the next
is started *beside* it - the first is not abandoned, so a slow cellular round
trip still wins on its first address. An address that fails starts the next
at once; the first to connect wins and the rest are closed. Up to eight are in
flight at once.

So a **blackholed** address - an IPv6 route that is broken, an A record for a
server that was switched off, whose SYNs are simply never answered - costs
250 ms, not the whole connect timeout. Before, the connect sat on it until the
kernel gave up (about two minutes) and the client's 20 s timeout ended the
attempt first: the working address behind it was never tried.

### The last good address, when DNS is down (RFC 8767, serve-stale)

The resolver is the piece of a carrier network that fails first. So the last
answer each **host name** resolved to is remembered (per name, so a failover
server under its own name is remembered separately), and used under these
rules - and only these:

| When | What is dialled |
| --- | --- |
| DNS answers within 1.8 s | **the fresh answer**, always. It also replaces what is remembered, at once. |
| DNS fails ("temporary failure", no answer, too many lookups already stuck) | the remembered addresses, if the answer is **less than 24 hours old** |
| DNS has not answered after **1.8 s** (`MQTT_DNS_STALE_AFTER_MS`) | the remembered addresses, if under 24 hours old; the lookup carries on in the background and, when it answers, replaces what is remembered for the next attempt |
| Nothing remembered, or older than 24 hours | nothing: the attempt waits for DNS (up to the connect timeout) and fails if it does not answer. An entry past its age is deleted. |

- **Every attempt looks the name up first**, including the one after a
  connection made on a remembered address. The memory is never used in
  preference to an answer, so a server that moves is followed on the next
  attempt that DNS answers.
- **24 hours is counted from when the answer was received**; using it does not
  make it younger. Change it per transport with
  `setStaleAddressLimit(seconds)` (`0` turns the memory off), or for a build
  with `MQTT_DNS_STALE_LIMIT_S`. `getaddrinfo()` does not report the record's
  TTL, so this bound is all there is.
- **A remembered address that is refused or times out is forgotten at once**,
  and not tried again until DNS gives it back. A failure that says this machine
  has no network at all (network unreachable, network down) does not count
  against the address.
- A numeric address (`192.168.1.10`) is never a lookup and never remembered.
- An error reached on remembered addresses says so: "... (DNS is not answering;
  these were its last known addresses)".

### The kernel's backstop

Each dialled socket also gets `SO_KEEPALIVE` (120 s idle, then four probes
30 s apart), `TCP_USER_TIMEOUT` of 300 s (`setDeadLinkTimeout()`, 0 for the
kernel's own fifteen minutes), where the platform has them. They matter when the MQTT keepalive is off or long: a dead
path with data outstanding ends within five minutes rather than fifteen. Not shorter: on a lossy cellular link the
retransmission timer doubles through a burst of loss, so one segment can wait over two minutes for its ACK on a link
that is passing traffic again, and 120 s ended live connections in the bad-network rig.

---

## Slow links: keepalive and acknowledgements

A kilobyte a second is an ordinary cellular link on a weak signal. At that
rate a 30 kB packet takes half a minute to cross, and nothing whole arrives
from the broker meanwhile because it is waiting for the rest of it. The
library's timers are measured accordingly:

- **Bytes count as signs of life**, not only whole packets: bytes of a large
  packet still arriving, and our own backlog draining after the transport had
  refused it (the far end acknowledging data). The keepalive still ends a
  connection on which **nothing has moved, in either direction, for two
  intervals** - a link that has stopped is still found.
- **A PINGREQ is not queued behind a backlog**: it could not be answered until
  the backlog had gone, and the bytes leaving already prove what it would.
- **"Sent" means the last byte has left** the library's buffer, for the
  keepalive and for `acknowledgementTimeoutMs`: a QoS 1 or 2 publish's
  acknowledgement deadline starts when its last byte has gone, not when it was
  queued. (On a host the kernel may still hold some of it; the deadline's
  default leaves room for that.)
- **A broker's Server Keep Alive** is the interval the client is obliged to
  keep, but the client pings at the **shorter** of the broker's and its own:
  a Server Keep Alive of 0 does not switch off dead-link detection, and one of
  an hour does not let a carrier NAT forget the connection in between. Only the
  application's own `keepAlive(0)` turns it off. `activeKeepAlive()` reports
  the interval in force.

### A fleet that powers up together

The reconnect backoff is jittered (full jitter, doubling to a minute). The
**first** connect after `begin()` is not, by default - as in the common
clients, so a bench device or a restarted gateway connects at once. Where
many devices share a broker and a power supply, spread it:

```cpp
mqtt.behaviour().firstConnectSpreadMs = 30000;   // a thousand devices: ~30 a second
```

Each device waits a fixed time drawn from its client identifier, between 0 and
the setting.

---

## TLS, on each of them

TLS is a transport, not a feature. Which means on Arduino it usually costs no
extra code at all — a secure channel *is* a channel.

### An ESP32, over any channel — Wi-Fi, Ethernet or a modem

**This is the one to use on an ESP32.** `MqttChannelTlsTransport` takes a
*plain* channel and does the TLS itself, with the mbedTLS the ESP32 core
already links for its own Wi-Fi — so it adds no TLS library, about 4 kB of
code, and it checks the broker the same way whatever the medium:

```cpp
WiFiClient plain;                        // or EthernetClient, or a modem's client
MqttChannelTlsTransport<WiFiClient> tls(plain, "broker.example.com", 8883);
tls.setCaCertificate(ROOT_CA);           // or setPinnedKeyFingerprint(), see below
MqttDevice mqtt(tls);
mqtt.connect("broker.example.com", 8883, "tank-01");
```

What it gives that a medium's own secure client does not:

| | `WiFiClientSecure` | `SSLClient` | a modem's own TLS | `MqttChannelTlsTransport` |
| --- | --- | --- | --- | --- |
| Verifies by CA, and the host name | yes | yes | **no** | yes |
| Pins one key (`setPinnedKeyFingerprint`) | no | no | no | yes |
| Reports the broker's key (`peerKeyFingerprint`) | no | no | no | yes |
| Mutual TLS (`setClientCertificate`) | yes | yes | varies | yes |
| Ethernet and cellular too | no | yes | cellular only | yes |
| Handshake blocks the loop | yes | yes | yes | **no** — a step per `poll()` |
| Extra flash on an ESP32 | 0 | 40–50 kB | 0 | about 4 kB |

The checks are `MqttTlsTransport`'s, the host's TLS transport: validate by
default and refuse without a CA, pinning replaces chain and name checking, and
`setInsecureNoCertificateValidation()` checks nothing. A refusal is reported
as a value, `lastErrorCode()`, with a short line in `lastError()` — the list is
in [security.md](security.md#why-a-board-refused-mqtttlserror). Certificate dates are
never judged: the core's mbedTLS is built without a calendar, because a
controller that booted thinking it is 1970 must still connect.

**Memory.** A session holds about 40 kB while it is up — two 16 kB record
buffers the core compiles in — and nothing while it is down. On a board with
PSRAM, the first session moves mbedTLS's larger allocations there
(`mq_channelTlsUseExternalRam()`). That is global: mbedTLS has one allocator,
so Wi-Fi's own cryptography moves too, exactly as ESP-IDF's
`CONFIG_MBEDTLS_EXTERNAL_MEM_ALLOC` would do it. Define
`MQTT_CHANNEL_TLS_INTERNAL_RAM` to keep everything in internal RAM.

**Bytes on the wire.** The cipher suites offered are the ECDHE-AES-GCM and
ECDHE-AES-CBC-SHA256/384 families plus RSA-AES-GCM — what brokers choose —
rather than everything the core compiled, which keeps the first message about
150 bytes on a metered link. RSA, P-256 and P-384 certificate chains all
verify; the suite list does not affect that.

ESP32 only (`MQTT_CHANNEL_TLS` in `MqttFeatures.h`): no other core ships an
mbedTLS to reuse. Elsewhere, use one of the two below.

### A board with native TLS (ESP32, ESP8266)

```cpp
WiFiClientSecure net;
net.setCACert(ROOT_CA);
MqttDevice mqtt(net);
mqtt.connect("broker.example.com", 8883, "tank-01");
```

### A board without it — Ethernet, or cellular

Most Ethernet chips an Arduino talks to have no TLS. Neither do most cellular
modems in transparent mode. `SSLClient` (OPEnSLab) wraps **any** Arduino
`Client` in BearSSL, and is itself a `Client`:

```cpp
EthernetClient plain;
SSLClient secure(plain, TAs, TAs_NUM, A5);   // A5 must be an UNCONNECTED pin
MqttDevice mqtt(secure);
```

The analog pin is BearSSL's entropy source. It must be unconnected, or the
randomness is not random.

Costs, honestly: BearSSL wants roughly 40–50 kB of flash and a few kilobytes of
RAM for its buffers. On an UNO R4 that is most of what is spare —
an ESP32 with native TLS is far cheaper if you have the choice.

The same wrapping works over cellular: `SSLClient` over `TinyGsmClient`.

Full example: `examples/arduino/TLS_Over_Ethernet`.

### On a host

```cpp
MqttDevice mqtt;
mqtt.secure("/etc/ssl/certs/ca.crt");   // or secureWithCertificate(pem)
mqtt.clientCertificate("client.crt", "client.key");
mqtt.connect("broker.example.com", "gateway-1");   // port defaults to 8883
```

Detected, not configured: the transport compiles when `<openssl/ssl.h>` is
there. OpenSSL is not a dependency imposed on anyone
who does not want it.

See [security.md](security.md) for what certificate validation does and does
not prove.

---

## Bringing your own transport

Anything that is an `MqttTransport` works, on any platform:

```cpp
MyWebSocketTransport ws;
ws.configure("broker.example.com", 443, "/mqtt");
MqttDevice mqtt(ws);                  // yours to configure; this will not touch it
mqtt.connect("broker.example.com", 443, "gateway-1");
```

`MqttTransport` is seven methods and no platform types. See
[extending.md](extending.md) for what a new one has to honour — partial writes
and half-close being the two that matter.

---

## One thing that does not work on ESP8266

A payload held in `PROGMEM` cannot be published directly there.

On an ESP32, an RP2040 and most 32-bit targets, flash is memory-mapped and an
ordinary pointer reads it. **On an ESP8266 it is not**: a `PROGMEM` array has
to be read through `memcpy_P`, and handing this library the pointer would
publish whatever happened to be at that address in RAM.

Copy it into RAM first, which you have to do anyway to have a contiguous
buffer:

```cpp
static const char CONFIG[] PROGMEM = "...";

char buffer[sizeof CONFIG];
memcpy_P(buffer, CONFIG, sizeof CONFIG);
mqtt.publish("plant/node-1/config", (const uint8_t*)buffer, sizeof CONFIG - 1);
```

There is no flash-aware publish that would avoid that copy, because the copy is
where a payload has to end up regardless: it is queued, and it may be replayed
from the session minutes later, long after any flash pointer stopped meaning
anything.

---

## What has run on real hardware, and what has not

Said plainly, because it changes what you should check on a bench.

| | |
| --- | --- |
| The host transports, TCP and TLS | run against real brokers, at both protocol versions |
| Every Arduino channel above | **compiled against their real APIs. Not bench-tested by us.** |

What a channel changes is the route the bytes take and the timing — not the
MQTT. The protocol underneath is the same code on every target. That is an
argument rather than a measurement, and it is stated as one.

The behaviour that genuinely differs per channel is the one this page is
mostly about: `Client::connect()` is synchronous, and on a cellular modem it
is seconds rather than milliseconds. That is the one to watch.

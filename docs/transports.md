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
mqtt.autoReconnect(false);    // reconnect where you can afford the stall - below
```

**Keep alive is a trade, not a default.** It is the only thing that notices a
link that went away without saying so — a bearer that vanished, a NAT entry
that timed out — so a longer interval is less data and a slower failure. Pick
it deliberately.

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
| `TinyGsmClient` | **seconds** — it is a sequence of AT commands |

### Where it can happen

`begin()` on the transport is the only call that can stall, and it is reached
from exactly two places:

1. **`MqttDevice::connect()`** — which you call, from `setup()` or from
   somewhere in your own control, so the stall is where you expect one.
2. **A reconnect attempt inside `poll()`** — which is inside your loop.

Everything else — publishing, receiving, the keepalive, servicing the queue —
is non-blocking on every channel, always.

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

On a **host** none of this applies: `MqttTcpTransport` and `MqttTlsTransport`
are non-blocking throughout, connect included. The limitation is Arduino's
`Client` interface, not the design.

---

## TLS, on each of them

TLS is a transport, not a feature. Which means on Arduino it usually costs no
extra code at all — a secure channel *is* a channel.

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

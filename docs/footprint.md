# Footprint

What this library costs in flash and in RAM, measured rather than claimed.

Measured on 2026-10-28, by the footprint script in the
development tree - never typed in by hand.
Compiler: c++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0.

Figures are `.text` plus read-only data of the static library, built
`-Os` with no sanitizers - the code and constants that land in flash.
RAM is measured separately, further down.

**These are HOST builds, and a board build is smaller.** The host
client links `MqttTcpTransport` (BSD sockets, a background DNS lookup)
and the host-only buffer growth in `MqttLink` (`MQTT_LINK_GROWS`, for a
broker holding many idle connections); a board links neither - it uses
`MqttStreamTransport` over its own network client, and fixed buffers.
Measured 2026-10-04: buffer growth alone is about 1 kB of the long form.

| | Flash |
| --- | ---: |
| the whole library - client, broker, bridge, typed layer, Sparkplug | 312730 |

## What this means

**That is one number because there is one build.** There are no
feature flags: the client, the broker, the bridge and the typed layer
are all compiled, always. This table used to have seven rows - client,
client+tls, client+broker and so on - and every one of them measured
the same figure, because the flags they switched had already been
deleted. A table of seven identical numbers is worse than one honest
one.

What you pay is decided at LINK time, not compile time. A role you
never instantiate is a class nothing references, and the linker drops
it - which is measured below for `MqttDevice` rather than asserted,
down to checking that no symbol of it survives into a program that
does not name it. The same mechanism is what keeps the broker out of
a sketch that only publishes.

Two things this number does not include: TLS, which lives in whatever
engine you link (wolfSSL or OpenSSL) and not in here, and the buffers,
which are RAM and are below.

## RAM

Flash is the table above; RAM is usually what a board runs out of
first. These are `sizeof` on the measuring host - **a 32-bit target
is meaningfully smaller**, because every `std::string` and
`std::vector` inside them is.

| | |
| --- | ---: |
| `MqttClient` | 2336 bytes |
| `MqttDevice` | 12928 bytes |
| `MqttOperations (the send queue)` | 80 bytes |
| `MqttSession` | 584 bytes |
| `one queue slot (MqttOperation)` | 112 bytes |
| `one property set (MqttOwnedProperties)` | 144 bytes |

**The buffers are on top of this and are yours to size.**
`bufferSize()` allocates a receive buffer and a transmit buffer -
so 4096 costs about 8 kB, and that is the single biggest number on a
small board. It used to be three buffers and about 12 kB; packets are
now encoded straight into the transmit queue instead of into a
scratch area and copied, which gave one of them back. See **What a
large payload costs** in api.md.

A queue slot is smaller than a property set, which it can only be
because 5.0 properties are held behind a pointer. Inline they cost
MQTT_QUEUE_CAPACITY of them on every client, used or not - 4,608
bytes, measured, before this was fixed. The script fails if that
ever comes back.

## What MqttDevice costs

The table above is the whole static library. An application links only
what it reaches, PROVIDED the build discards what it does not:
`-ffunction-sections -fdata-sections` when compiling and
`-Wl,--gc-sections` when linking. Arduino's own build recipes do this
and so does the measurement below - check your own `platform.txt` if
you are on a core this library has not been built for.

Measured rather than asserted: two programs, one written against
`MqttClient` and one against `MqttDevice`, linked and compared.

| | |
| --- | ---: |
| `MqttClient`, long form | 53734 bytes |
| `MqttDevice`, short form | 70268 bytes |
| **the facade costs** | **16534 bytes** |

That buys the subscription table, per-filter routing, the change
reporter, the NUL-terminated payload copy, the conversions on it -
and the three transports the facade holds by value, so that
`secure()` is one call rather than a rebuild. The long form names
one transport and links one.

Formatting numbers as text is a small part of it, measured rather
than guessed: the same program publishing a string instead of a
float is 64 bytes smaller, because `strtod` is already
linked for `toNumber()`.

And the other half of the claim is checked rather than asserted: the
long-form program's symbol table contains **no `MqttDevice` symbols at
all**. A sketch that does not use the facade does not carry it.

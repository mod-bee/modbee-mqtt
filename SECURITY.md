# Security

This library parses bytes it did not produce. A broker, or anything that can
reach the port a broker listens on, hands it length fields, string prefixes and
property blocks that it must not trust — so a parsing defect here is a remote
one.

## Reporting a vulnerability

Email **info@modbee.org** with `modbee-mqtt` in the subject. Please include
what you did, what happened, and the version or commit.

**Do not open a public issue for a security defect.** Give us a chance to
release a fix before it is public; we will credit you unless you would rather
we did not.

## What is in scope

- anything reachable by sending bytes to a client or to the broker: a crash, a
  read or write outside a buffer, a hang, unbounded memory growth
- a TLS check that does not happen when the documentation says it does
- an authorisation hook that can be bypassed
- a message delivered to a subscriber that should not have matched

## What is not

- **Certificate validation turned off on purpose.**
  `setInsecureNoCertificateValidation()` is named at that length because it is
  a decision, not a default. See [docs/security.md](docs/security.md).
- **A broker refusing to do something.** A reason code is the protocol
  working.
- **Behaviour on a platform where `Client::connect()` blocks.** That is the
  network stack's, and [docs/transports.md](docs/transports.md) says so.

## What this library does to make defects less likely

Not a guarantee — a description of the approach, so you know what to look at.

- **Every length is bounded against the real buffer**, in the units the copy is
  performed in. MQTT declares four independently: the Remaining Length, the
  Property Length, each two-byte string or binary prefix, and the residual
  payload. Deriving one from another is how buffer overruns happen here.
- **Nothing is written without being measured first.** A packet that does not
  fit is refused, never truncated.
- **The stream never resynchronises.** A malformed packet ends the connection
  rather than starting a search for a boundary — MQTT has no delimiter, so a
  parser that hunts for one can be steered.
- **Non-canonical encodings are refused**, not accepted leniently. One packet
  must have one byte representation, or anything comparing or forwarding
  encoded bytes can be desynchronised from anything parsing them.
- **No exceptions and no dynamic dispatch on the parse path**, and every bound
  is a compile-time constant or a value you configured.

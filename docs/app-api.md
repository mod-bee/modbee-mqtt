# The Application API

Devices speak MQTT to `modbee serve`. **Your application** - a platform
backend, a gateway's own software - can speak to it a different way: over
the Application API, gRPC on a local socket, with no MQTT library at all.

The contract is [`proto/modbee/app/v1/app.proto`](../proto/modbee/app/v1/app.proto).
Today the server answers two of its services:

| Service | What it is for |
| --- | --- |
| `System.Hello` | What this server is, which API version it speaks, and what it can do (`features`) |
| `Authority` | **Your application decides who may log in, and what each login may do** - below |

The other services in the file (`Messages`, `Connections`, `Admin`) answer
`UNIMPLEMENTED` for now; `Hello`'s `features` lists what a server serves, so
an application asks rather than guesses.

---

## Turning it on

```yaml
api:
  grpc: ["unix:/run/modbee/api.sock"]
```

`modbee serve` says where it listens:

```
modbee serve: Application API (gRPC) on unix:/run/modbee/api.sock
```

- **The socket is the lock.** It is made readable and writable by the server's
  own user and group only (`0660`): put your application in that group, or
  run it as that user. A Unix socket's path is at most 107 bytes.
- **From another host**, add a TCP address. It must have TLS and a token -
  the server refuses to start otherwise:

  ```yaml
  api:
    grpc: ["unix:/run/modbee/api.sock", "tcp://0.0.0.0:7660"]
    tls:
      cert: /etc/modbee/api.pem
      key: /etc/modbee/api.key
      client-ca: /etc/modbee/apps-ca.pem    # optional: callers present a certificate it signed
    token-file: /run/secrets/modbee-api
  ```

  Every call then carries the token as gRPC metadata,
  `authorization: Bearer <token>`, on the socket as well; one without it is
  refused `UNAUTHENTICATED`.
- Any gRPC library reaches it: `@grpc/grpc-js` for Node.js
  (`unix:/run/modbee/api.sock` is a valid target), `google.golang.org/grpc`
  for Go, `grpcio` for Python. The standard `grpc.health.v1.Health` service
  answers too, for a supervisor's health check.
- The server needs gRPC to serve it: a build where gRPC was not found says
  so and does not start with an `api:` section (Ubuntu/Debian:
  `libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev`).

---

## Logins decided by your application

```yaml
authentication:
  username: platform              # your application's own login: never asked
  password: ...
  authority: {}                   # every other login is asked of your application
api:
  grpc: ["unix:/run/modbee/api.sock"]
```

Logins then come from your application instead of a login database
(`authentication: database` and `authority` cannot be used together). The
fixed logins in the file - `username`/`password` (an admin) and `users:` -
are still decided by the server itself: that is how your application, and the
`modbee` command, log in without asking themselves.

### What is asked, and when

Your application opens `Authority.Serve` - a stream - and sends `open`. From
then on, when a device connects with a login the server has no answer for, it
sends your application a `ConnectQuestion`: the client id, the user name and
password, where it came from, the MQTT version, and the client certificate
when the TLS handshake verified one. Your application sends back a `Decision`
with the question's `id`.

**Only CONNECT is asked, and only once per login.** The answer says how long
it holds (`ttl_seconds`); until then the same client id, login and password
are answered from what was kept, without asking. The answer to "may it
connect" also says **what the login may do** - its scope - and the server
checks every PUBLISH and SUBSCRIBE against that itself, so no message ever
waits on your application.

While a CONNECT is asked, nothing waits: that connection is parked until the
answer comes, and every other connection carries on.

### The answer

| `Decision` field | |
| --- | --- |
| `allow` | |
| `refusal` | When `allow` is false - the table below |
| `ttl_seconds` | How long it holds. `0`: ask at every CONNECT |
| `scope` | What the login may do (when allowed) |
| `login` | Optional: the session's login, when it is not the CONNECT's user name |
| `replace_session` | Optional: it may take over a session another login holds under this client id |

| `refusal` | MQTT 5.0 | MQTT 3.1.1 |
| --- | --- | --- |
| `REFUSAL_BAD_CREDENTIALS` | 0x86 | 4 |
| `REFUSAL_NOT_AUTHORIZED` | 0x87 | 5 |
| `REFUSAL_BANNED` | 0x8A | 5 |
| `REFUSAL_QUOTA` | 0x97 | 3 |
| `REFUSAL_UNAVAILABLE` | 0x88 | 3 |

**Say `REFUSAL_UNAVAILABLE` when you could not check** - your database is
down - never "bad credentials". A device told its password is wrong stops
trying; one told the server is unavailable comes back. An unavailable answer
is never kept, and a login the server still holds an earlier answer for is let
in on that instead.

### The scope

| `Scope` field | Means |
| --- | --- |
| `publish` | Topic filters it may publish to - its Will included |
| `subscribe` | Topic filters whose messages it may **receive**. A SUBSCRIBE inside one of them is granted |
| `subscribe_wide` | Filters it may also subscribe **with**, receiving only what `subscribe` allows |
| `deny` | Never, whatever the lists above say: nothing published there, nothing delivered from there |
| `max_sessions`, `max_retained` | This login's limits (absent: `authentication: max-sessions-per-login` / `max-retained-per-login`) |
| `tag` | Up to 255 bytes, yours: kept with each session of the login |

- `${username}` and `${clientid}` in a filter are put in for each session. A
  value containing `/`, `+` or `#` is never put into a filter: that rule then
  grants nothing (so a login named `acme/pump-07` gets its topics from
  explicit rules, as with the [login database](logins.md#names-with--in-them)).
- `$` topics are reached only by naming them; a filter starting with a
  wildcard never reaches them. `$modbee/admin/...` is for the fixed admin
  login only, whatever a scope says.
- **`subscribe_wide`** is for a subscriber that asks for more than it may
  have - a Sparkplug host subscribing `spBv1.0/#`, which is Ignition's
  default. It is granted, and each message through it is checked: on a
  server shared by several customers the host receives its own groups and no
  one else's, retained messages included.
- **`deny`** keeps a login out of levels inside a namespace it otherwise
  owns - `acme/+/$ota/#`, every device's firmware level, for a shared login.
  A subscription that reaches into one (`acme/#`) is granted, and what it
  would deliver from there is held back.

A platform's shapes, as an example:

| Login | `publish` | `subscribe` | `subscribe_wide` | `deny` |
| --- | --- | --- | --- | --- |
| A device's own | `acme/pump-07/#` | `acme/pump-07/#` | | |
| A shared login for a namespace | `acme/#` | `acme/#` | | `acme/+/$ota/#` |
| A Sparkplug host | `spBv1.0/acme/#`, `spBv1.0/STATE/acme-host` | `spBv1.0/acme/#`, `spBv1.0/STATE/+` | `spBv1.0/#` | |
| A read-only key | (none) | `acme/#` | | |

### When your application is away

| Situation | What happens |
| --- | --- |
| No instance connected, or none answers within `timeout-ms` | A login answered before is let in on that answer, for `stale-for` after it expired; any other is refused `0x88` (3.1.1: `3`), which a client retries |
| Several instances connected | Questions are dealt round them |
| An instance stops with questions it has not answered | They are asked of another, once |
| It comes back | It is asked again as before |

```yaml
authentication:
  authority:
    timeout-ms: 2000         # how long a CONNECT waits for an answer
    stale-for: 86400         # seconds an expired answer still serves while your application is away
    cache-size: 1000000      # answers kept, whole server
    max-pending: 10000       # CONNECTs waiting for an answer at once; one more is told 0x89 Server busy (3.1.1: 3)
```

### Changing a login while it is connected

| Call | Does |
| --- | --- |
| `Authority.Revoke(login)` | Forgets its kept answers and closes its connections (MQTT 5: `0x87`). It is asked again when it reconnects |
| `Authority.Regrant(login, [client_id], scope)` | Its live sessions take the new scope at once, and stay up; a subscription the new scope no longer allows is removed |
| `Authority.Forget([login])` | Forgets kept answers - one login's, or all - so the next CONNECT asks again; live sessions are untouched |

Each answers once every CPU core's share of the server has done it, with how
many connections, sessions and answers it touched.

### An example

[`examples/app-api/nodejs/authority.js`](../examples/app-api/nodejs/authority.js)
is a whole authority in Node.js - `Hello`, `Serve`, and coming back after a
restart - with a small table standing in for your database:

```bash
cd examples/app-api/nodejs && npm install
MODBEE_API=unix:/run/modbee/api.sock node authority.js
```

```
modbee-mqtt 0.1.0 (API 1.0, authority) at unix:/run/modbee/api.sock
asked about "acme/pump-07" (client pump-07): allowed
```

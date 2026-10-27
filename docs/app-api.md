# The Application API

Devices speak MQTT to `modbee-mqtt serve`. **Your application** - a platform
backend, a gateway's own software - can speak to it a different way: over
the Application API, gRPC on a local socket, with no MQTT library at all.

The contract is [`proto/modbee/app/v1/app.proto`](../proto/modbee/app/v1/app.proto).
The server answers all five of its services:

| Service | What it is for |
| --- | --- |
| `System.Hello` | What this server is, which API version it speaks, and what it can do (`features`) |
| `Messages` | **Every device's messages, shared by your application's instances, and messages back to the devices** - [below](#messages) |
| `Connections` | **MQTT connections the server keeps to other brokers for your application** - [below](#connections-to-other-brokers) |
| `Authority` | **Your application decides who may log in, and what each login may do** - [below](#logins-decided-by-your-application) |
| `Admin` | **Logins, site keys, sessions, kicks, retained messages, TLS, counters** - [below](#administration-and-rest), and the same calls as JSON over HTTP |

Every call is also a command - `modbee-mqtt sessions list`, `modbee-mqtt listeners set --port 1884` - with the
call's own fields as its flags ([cli.md](cli.md#the-application-api-from-the-command-line)), and the server is
started with flags alone (`serve --api unix:<path> --api-token-file <file> --authority --events`).

`Hello`'s `features` lists what a server serves (`authority`, `messages`, `connections`, `admin`), and what
it added since (`connections.subscriptions`: subscription options on a connection; `admin.listeners`:
`PutListeners`), so an application asks rather than guesses.

**`System.Attach`** is an instance's presence. Open it with the messages to
publish should the instance vanish - its Wills, as an MQTT client has one:

```js
const attach = system.Attach(metadata());
attach.write({ open: { instance: 'backend-1', wills: [
  { topic: 'spBv1.0/STATE/host-1', payload: offline, qos: 1, retain: true } ] } });
// ... and on a clean shutdown:
attach.write({ close: {} }); attach.end();
```

A stream that ends without `close` - the process killed, the socket gone -
has its Wills published, in order. One that closes first has none.

---

## Turning it on

On a server installed with `modbee-mqtt service install`, one command - TCP on
this machine, with a token you choose:

```bash
modbee-mqtt config set api.grpc=tcp://127.0.0.1:7660 api.token=<token> --restart -u admin -P <password>
```

In a `modbee.yaml` (the Docker image's way):

```yaml
api:
  grpc: ["unix:/run/modbee/api.sock"]
```

`modbee-mqtt serve` says where it listens:

```
modbee-mqtt serve: Application API (gRPC) on unix:/run/modbee/api.sock
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
    token-file: /run/secrets/modbee-api     # or token: <the token itself>
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
  `libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev`). The Docker image
  and the Windows build have it.
- **An application on the same machine, on Windows as on Linux**, can use TCP
  on loopback with the token alone - no TLS, since nothing off the machine
  reaches it:

  ```yaml
  api:
    grpc: ["tcp://127.0.0.1:7660"]
    token-file: /run/secrets/modbee-api
  ```

  That is how it is reached on Windows ([windows.md](windows.md)); the
  server refuses a loopback address without a token.
- **The server in Docker, your application beside it** (on the host, or in
  another container): TCP inside the container with `plain: true` - no TLS,
  the token still required - and the port published on `127.0.0.1` only, or
  kept on the Docker network. Every setting comes from the environment as
  well ([docker.md](docker.md#the-application-api)):

  ```yaml
  environment:
    MODBEE_API_GRPC: tcp://0.0.0.0:7660
    MODBEE_API_PLAIN: "true"
    MODBEE_API_TOKEN: ${MODBEE_API_TOKEN}     # or MODBEE_API_TOKEN_FILE: /run/secrets/...
  ports:
    - "127.0.0.1:7660:7660"
  ```

  `api.plain` is a promise that nothing else reaches the port; the server
  cannot check it, so it is never the default.
- **The token**: `api.token` (the token itself) or `api.token-file` (a file
  holding it) - or `MODBEE_API_TOKEN` / `MODBEE_API_TOKEN_FILE`.

---

## Messages

### Reading every device's messages: `Messages.Consume`

Open a stream, send `open` with a **group** name and the topic filters it
wants, and the messages arrive, one `Delivery` each. Acknowledge each one
(`ack`, by its `id`) once you have handled it - stored it, acted on it - not
before.

```js
call.write({ open: { group: 'platform', filters: ['#'], maxInFlight: 100 } });
call.on('data', (down) => {
  if (down.delivery) { handle(down.delivery); call.write({ ack: { ids: [down.delivery.id] } }); }
});
```

**Run as many instances as you like with the same group: they share it.**

| | |
| --- | --- |
| **One device, one instance, in order** | Every message is dealt to the instance its publisher's client id hashes to, so one device's messages reach one instance, in the order the server took them. An instance joining or leaving moves only its own share (`pick: PICK_ROUND_ROBIN` turns this off) |
| **Nothing is lost when an instance stops** | What it had been sent and not acknowledged - killed, crashed, restarting - goes to the others, marked `redelivered`. A message can therefore arrive twice: handle it so a repeat is harmless (store it under the device and its own time) |
| **The group outlives its instances** | With none connected, a named group keeps collecting, and what waited arrives marked `replay` when one is back. One nobody opens for `expiry_seconds` (a day by default) is dropped |
| **And the server, with `sessions: store: file`** | A named group and every message it holds that nobody has acknowledged are kept beside the sessions (`groups.db`), and a QoS 1 or 2 device's acknowledgement waits until its message is written - so a restart, an upgrade or modbee killed outright loses none of them. After it the group is collecting again before any instance opens it, and what it held arrives marked `replay` and `redelivered` (the server cannot know which had been sent). Without it, a restart starts every group afresh. A stream of its own (`group: ""`) is never kept |
| **The group must take it** | A group holding `api.max-queued` messages (100,000) refuses more: a device publishing at QoS 1 or 2 is told `0x97` Quota exceeded and keeps the message to send again; QoS 0 is dropped and counted. With `on_full: ON_FULL_DROP`, it is dropped instead |
| **Flow** | An instance is given at most `max_in_flight` unacknowledged messages at once; acknowledge to get more |
| **Not now** | `retry` with an `after_ms` sends one again later, to any instance |

Each delivery says who published it, which a device cannot fake: its
`publisher` (`client_id`, the `login` the server checked, and the login's
`tag` from its [scope](#the-scope)), `broker_time_ms` - when the server took
it, a clock you can trust - and the MQTT 5 properties it was published with.

- **`$` topics** come only when a filter names them (`$modbee/provision/+/request`).
- **`events: true`** adds client events - connected, disconnected, subscribed,
  unsubscribed - dealt like the messages: to the instance that gets that
  client's messages, in order with them, acknowledged by `id`. The server
  needs `events: enabled` ([events.md](events.md)).
- **`exclude_origins`** keeps messages your application published itself
  (with that `origin`) out of the group.
- **`group: ""`** is a stream of its own, gone when it ends.
- `Messages.ListGroups` lists the groups; `Messages.DeleteGroup` removes one
  nobody is connected to.

[`examples/app-api/nodejs/consumer.js`](../examples/app-api/nodejs/consumer.js)
is a whole member: start two, kill one mid-stream, and every message still
arrives.

### Sending to the devices: `Messages.Publish`

`Publish` routes a message - topic, payload, QoS, retain, MQTT 5 properties -
to the devices subscribed to it. `PublishStream` does the same for a stream
of them, each answered by its `request_id`. `origin` names who published it,
for `exclude_origins` and for the `publisher.origin` a group sees. When the
answer comes is up to you (`confirm`):

| `confirm` | Answered |
| --- | --- |
| `CONFIRM_ACCEPTED` (the default) | Once the server has taken it: `PUBLISH_ACCEPTED` |
| `CONFIRM_DELIVERED` | Once **every subscriber it reached has acknowledged it** - PUBACK at QoS 1, PUBCOMP at QoS 2 - on whichever CPU core's share of the server each one is: `PUBLISH_DELIVERED`, with how many it `matched`. Nobody subscribed: `PUBLISH_NO_SUBSCRIBERS` at once |

The wait is bounded by the call's gRPC deadline (ten seconds without one).
When it comes first the answer is `PUBLISH_PARTIAL`, with how many
`acknowledged` and how many are `pending` - a subscriber that is offline
counts as pending, and its message waits in its session. A subscriber that
never could have it (its queue full, the message expired or too large for
it) also makes it `PARTIAL`. Use it for a command that must have reached its
device before your application goes on; a store-and-forward outbox deletes
only what came back `PUBLISH_DELIVERED`.

---

## Connections to other brokers

Your application can have the server hold an MQTT connection to another
broker for it - a customer's, the platform's (a gateway's uplink), one a
Sparkplug host or edge node lives on - with no MQTT library of its own. The
server's own client does it, so the connection stays up while your
application restarts.

`Connections.Put` with a `ConnectionSpec`: an `id`, the `servers` in order of
preference, the client id, login, TLS, keep-alive, session, Will.

| | |
| --- | --- |
| **Failover** | An attempt that fails moves to the next server. Connected to any but the first, it checks the first every `primary_check_seconds` (60) and goes back to it once it answers |
| **Kept** | Written to `connections.json` in `api.state-directory` (`/var/lib/modbee`), readable by the server's user only, and opened again when the server starts. `Put` again with a change reconnects; with none, nothing happens. `Delete` closes and forgets it |
| **Messages in** | A `Consume` stream with `connection` set to its id: the group's filters become the connection's subscriptions, and what the other broker sends arrives as any delivery does, its `connection` set |
| **How it subscribes** | QoS 1 for a filter in `filters`. `subscriptions` sets each filter's own: `qos` 0 to 2, and MQTT 5's `no_local`, `retain_as_published` and `retain_handling`. Two groups asking for one filter share one subscription - the higher QoS, and the retained messages if either wants them. Opening the group again with other options subscribes again with them; `ListGroups` shows what each filter has |
| **Messages out** | `Publish` with `connection` set. `CONFIRM_DELIVERED` answers when **the other broker** has acknowledged it (PUBACK, PUBCOMP); `PUBLISH_NOT_CONNECTED` when the connection is down - nothing was sent |
| **`RECONNECT_WHEN_ASKED`** | A connection that drops stays down (`CONNECTION_WAITING`) until `Connections.Reconnect`, which can bring a new Will for the next session - what a Sparkplug edge node needs, its NDEATH carrying a new `bdSeq` each time |
| **A refused login** | `CONNECTION_REFUSED`, with what the other broker said in `last_error`, and not tried again until `Put` or `Reconnect`: a wrong password sent every second gets a site banned |

`Get` and `List` say each one's state, which server it is on, and its counts -
never its password or key. `Watch` streams every change of state.

TLS: `ca_pem` (empty: the system's trust store), and `certificate_pem` and
`key_pem` for mutual TLS, written beside `connections.json`. The name checked
is the server's `host`; `server_name` is not used yet.

**A Sparkplug host on a broker you do not own**, built on nothing but this:
[`examples/app-api/sparkplug-host`](../examples/app-api/sparkplug-host/README.md),
which passes the Eclipse Sparkplug TCK's Host Application profile through the
API - its Will renewed on every connect with `RECONNECT_WHEN_ASKED`, its
STATE birth answered by the other broker's PUBACK.

---

## Administration, and REST

`Admin` is what an operator - or the platform's own console - does to a
running server, without restarting it:

| Calls | |
| --- | --- |
| `ListLogins` `GetLogin` `PutLogin` `DeleteLogin` `ResetLogin` | The logins in the server's login database (`authentication.database`). `PutLogin` without a password generates one and returns it, once. `ResetLogin` gives a new one and closes the login's connections. With logins decided by your application, there is no database: `FAILED_PRECONDITION` |
| `ListKeys` `PutKey` `DeleteKey` | Site keys devices register with. A new key's secret, `<name>.<secret>`, is returned once |
| `ListSessions` | Every session on every shard, connected or not: its login, address, protocol, subscriptions, what is queued and in flight - and whether it is talking: `messages_in`, `messages_out`, `bytes_in`, `bytes_out`, `last_activity_ms` (its last packet) and `connects` (more than 1: it has reconnected). The counts run since the session began and carry over when the same client and login start clean while its session is still held; a restart of the server starts them again. `client_id_prefix` and `login` are applied on each shard before anything is gathered, so asking about one device is cheap |
| `Kick` | Closes a client's connection, or every connection of a login, with DISCONNECT `0x98` (administrative action); its Will is published. `discard_session` ends the session too |
| `ListRetained` `DeleteRetained` | Retained messages under a filter, with payloads if asked; deleting is the same as a device publishing an empty retained message to each |
| `GetListeners` `PutTls` | The listeners, and the TLS certificate they serve - replaced live: new handshakes use it, open connections keep theirs |
| `PutListeners` | Changed live, all or nothing: the plain and TLS ports (`0` turns one off; turning TLS on brings its certificate in `tls`), `max_connections` and `max_packet`. A listener that moves keeps its connections - they have their sockets already; one turned off closes its own with DISCONNECT `0x98`. `max_connections` goes as far as `max_connections_ceiling`, the room kept for sessions at the start (`--max-sessions`); lowered, it closes nobody and refuses new connections (`0x89`) until enough have gone. A connection already up keeps the packet size its CONNACK told it. A taken port is `FAILED_PRECONDITION`, with nothing changed. The queue caps too: `queue` (messages a session holds), `queue_bytes` (its bytes) and `queue_bytes_total` (every session's together, the whole server's; `0`: no cap) - a queue already over a lowered cap keeps what it holds and refuses new messages, counted as dropped, until it drains below it. Like `PutTls` it is not kept: the next start uses what the server is started with |
| `Stats` | The server's counters: connections, sessions, messages in and out, what is queued right now (`queued_messages`, `queued_bytes`), logins asked of your application and answered from its cache |
| `GetSettings` `CheckSettings` `SetSettings` `ApplySettings` `ResetSettings` | The settings kept in the login database - what `modbee-mqtt config` does over MQTT ([settings.md](settings.md)): single settings by their path (`set`, `remove`), or a whole `modbee.yaml` (`document`), validated as the file is and applied at the next start, or at once with `restart`; the TLS material is applied live. A change that would not load is `INVALID_ARGUMENT`, naming the setting; `CheckSettings` answers with the problems instead. Without a login database, `FAILED_PRECONDITION` |

### REST

The same calls, as JSON, on the HTTP listener `/metrics` is on:

```yaml
metrics:
  address: 127.0.0.1
  port: 9464
api:
  grpc: ["unix:/run/modbee/api.sock"]
  rest: true
```

```
modbee-mqtt serve: Application API (REST) on http://127.0.0.1:9464/v1/
```

```sh
curl -X PUT localhost:9464/v1/logins/sensor-7 -d '{"login":{"publish":["plant/sensor-7/#"]}}'
# {"login":{"name":"sensor-7","enabled":true,...},"password":"YT4SDLSozVV1cuLkvKLnKeB6xCC49mBb"}
curl 'localhost:9464/v1/retained?filter=plant/%23&payloads=true'
curl -X POST localhost:9464/v1/sessions/pump-1:kick
```

| REST | gRPC |
| --- | --- |
| `GET /v1/logins?prefix=&pageSize=&pageToken=` | `Admin.ListLogins` |
| `GET` · `PUT` · `DELETE /v1/logins/{name}` | `GetLogin` · `PutLogin` · `DeleteLogin` |
| `POST /v1/logins/{name}:reset` · `POST /v1/logins/{name}:kick` | `ResetLogin` · `Kick` |
| `GET /v1/keys` · `PUT` · `DELETE /v1/keys/{name}` | `ListKeys` · `PutKey` · `DeleteKey` |
| `GET /v1/sessions?clientIdPrefix=&login=&connectedOnly=` · `POST /v1/sessions/{clientId}:kick` | `ListSessions` · `Kick` |
| `GET /v1/retained?filter=&payloads=` · `DELETE /v1/retained?filter=` | `ListRetained` · `DeleteRetained` |
| `GET /v1/listeners` · `PUT /v1/listeners` · `PUT /v1/listeners/tls` | `GetListeners` · `PutListeners` · `PutTls` |
| `GET /v1/stats` · `GET /v1/health` | `Stats` · health |
| `GET /v1/settings` · `PATCH` · `PUT /v1/settings` · `POST /v1/settings:check` · `POST /v1/settings:reset` | `GetSettings` · `SetSettings` · `ApplySettings` · `CheckSettings` · `ResetSettings` |
| `GET /v1/connections` · `GET` · `PUT` · `DELETE /v1/connections/{id}` · `POST /v1/connections/{id}:reconnect` | `Connections.*` (`Watch` is gRPC only) |
| `GET /v1/groups` · `DELETE /v1/groups/{group}?connection=` | `Messages.ListGroups` · `DeleteGroup` |
| `POST /v1/publish` | `Messages.Publish` (the streams are gRPC only) |

- **The JSON is protobuf's own mapping**: field names in lowerCamelCase,
  64-bit numbers as strings, `bytes` (a password, a payload) as base64, enums
  by name. A field the message does not have is refused, `400`.
- **Errors** are the gRPC status as HTTP - `400` invalid, `401` no token,
  `404` not found, `409` exists, `503` unavailable - with
  `{"code": 404, "message": "..."}`.
- **Who may call.** With a token (`api.token`), every request carries
  `Authorization: Bearer <token>`. Without one, REST must be on a loopback
  address (`metrics.address: 127.x`); the server refuses to start otherwise.
  The listener has no TLS: reach it from another host through a proxy that
  adds it.
- One request at a time, as `/metrics` is: it is for an operator and a
  console, not for traffic. Messages in volume go over gRPC.

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
`modbee-mqtt` command, log in without asking themselves.

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

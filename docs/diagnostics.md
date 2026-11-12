# The diagnostics log

When a device will not connect, the broker can say why. `modbee-mqtt serve`
keeps a **diagnostics log** on disk: every connection and how it ended,
every login it refused **and the reason, in words**, every device that signed
up with a key and the answer it got, every change an admin made to a login or
a key, the server starting and stopping, and its own warnings and errors.
It is on by default and kept for a week.

```bash
modbee-mqtt log --client tank-01 --since 2h
```

```
2026-11-12 09:14:03.120  connect-refused     client tank-01 (its login) from 10.0.4.7:51544 - [134] Bad User Name or Password: wrong password
2026-11-12 09:20:11.403  admin               client modbee-admin-4411 login admin - user.password tank-01 (password=(set))
2026-11-12 09:20:30.017  connected           client tank-01 (its login) from 10.0.4.7:51602 (a new session)
2026-11-12 09:31:44.517  disconnected        client tank-01 (its login) from 10.0.4.7:51602 - [141] the server closed it - Keep Alive timeout: nothing heard from it for one and a half keep-alives
```

**Why a log of its own.** [Client events](events.md) are published as they
happen, are off by default, and are kept only if something reads them - and a
refused login was never one. The diagnostics log is kept whether events are
on or not, by the server itself, so the answer is there afterwards: no
subscriber that had to be running, no Kafka history to dig through.

---

## Where it is

In its own SQLite file, `diagnostics.db`, apart from the login database - it
is written all the time, is not part of a backup of the logins, and can be
deleted without losing anything the broker needs:

| The server runs | The log is |
| --- | --- |
| `serve --data <folder>` (and so `modbee-mqtt service install`) | `<folder>/diagnostics.db` |
| With `authentication: database:` | beside the login database |
| In the Docker image | `/var/lib/modbee/diagnostics.db` - on the volume, if one is mounted there |
| Otherwise | not kept, unless `diagnostics: file:` names one |

`modbee-mqtt serve` says where at the start:

```
modbee-mqtt serve: diagnostics log in /var/lib/modbee-mqtt/diagnostics.db (kept 7 day(s), up to 100 MB)
```

A file that cannot be opened is said, and the broker runs without it.

---

## What it keeps

Each entry has a **time**, a **kind**, the **client id**, the **login**, the
**address** it came from, the **reason code** sent or received, the
**reason in words**, and a **detail**.

| Kind | When | Reason and detail |
| --- | --- | --- |
| `connected` | A client's CONNECT was accepted | detail: `a new session` or `its session resumed` |
| `disconnected` | Its connection ended | `the client disconnected (DISCONNECT)`, `the connection closed without a DISCONNECT`, or the code the server closed it with and why - 141 Keep Alive timeout, 142 Session taken over (another connection with the same client id), 135 Not authorized (its login changed, disabled or removed, or its key turned off), 139 Server shutting down |
| `connect-refused` | A CONNECT was turned away | The code it was sent and why - see [below](#why-a-login-is-refused) |
| `subscribed`, `unsubscribed` | A subscription granted or removed | detail: the filter (and the QoS granted) |
| `publish-refused`, `subscribe-refused` | A PUBLISH or SUBSCRIBE its rules do not allow | The code and why; detail: the topic or filter |
| `handshake-failed` | A TLS handshake failed, before any CONNECT | What the TLS engine said; the address |
| `signup` | A device signed up with a key (`$provision`) | `ok - its login is ready`, `pending - waiting for an admin to approve it`, or `refused:` and why; detail: the key |
| `admin` | A login or key changed over `$modbee/admin` (`modbee-mqtt user`, `key`, `config`, or the Application API) | The command and its target (`user.disable tank-01`); detail: what it set - `publish=[plant/tank-01/#]`, `devices=2`; a password as `(set)`, never itself. The client and login are the admin's |
| `link` | A connection the server keeps to another broker (or over Kafka) came up, was lost, or was refused | The reason; detail: `up`, `lost - connecting again`, `backing off`, `refused - tried again later`. The client id is the connection's id |
| `started`, `stopped` | The server started (its version, shards and ports) or stopped | |
| `warning`, `error` | The server's own: the Kafka pipe, kept connections, the retained and session stores, TLS material, the login database's backups - what it says on stderr as well | The text; detail: where it came from (`kafka`, `connections`, `retained`, `sessions`, `tls`, `logins`, `server`) |

### Why a login is refused

| Code | Reason |
| --- | --- |
| 134 Bad User Name or Password | `wrong password`, `no login has that name`, `no user name - every client logs in`; signing up: `the password is not a key`, `no key is called "..."`, `the key's secret is wrong`, `the key has expired`, `the key is turned off (devices 0)`, `the key is for the device "..." only`, `the client id is not a device name`; a certificate login whose CN has no login, or a login bound to another certificate |
| 135 Not authorized | `the login is disabled`, `the login is waiting for an admin to approve it`, `its key "..." is turned off (devices 0)`, `its Will is on a topic its login may not publish to` (detail: the Will's topic) |
| 151 Quota exceeded | `its key "..." has its N device(s) at once online`; `the login holds all the sessions it may` (max-sessions-per-login) |
| 133 Client Identifier not valid | `the client id is held - a session of another login has it` (detail: which login) - a board signing up again while one is connected as it, say |
| 137 Server busy | `the server is at its connections or sessions` (max-connections, max-sessions) |

A login the application decides (`--authority`) is the application's to
explain; the log keeps the connections and what the broker itself refused.

---

## Reading it

**From the command line**, `modbee-mqtt log` ([cli.md](cli.md#the-diagnostics-log)):

```bash
modbee-mqtt log --kind connect-refused,signup --since 1d     # every refusal and sign-up today
modbee-mqtt log --login acme-press-7 --follow                # one login, and what comes next
modbee-mqtt log --client gw-12 --since "2026-11-12 06:00" --until "2026-11-12 07:00" --json
sudo modbee-mqtt log --data /var/lib/modbee-mqtt              # the file itself: works with the broker stopped
```

With an admin login it asks the broker over MQTT, so it reads a broker
across the world; with `--data` or `--file` it reads the file on the
server's own machine.

**Over MQTT**, from your own platform, with an admin login - the request
`log.list` on `$modbee/admin/request`, answered on
`$modbee/admin/<your client id>/reply`, as for logins
([logins.md](logins.md#from-your-own-platform)):

```json
{"id": "9", "command": "log.list", "client": "tank-01", "kinds": ["connect-refused"], "since": 1762905600000, "limit": 50}
```

```json
{"entries": [{"id": 4127, "time": 1762938843120, "kind": "connect-refused", "clientid": "tank-01", "login": "tank-01",
  "address": "10.0.4.7:51544", "reason_code": 134, "reason": "Bad User Name or Password: wrong password", "detail": ""}],
 "dropped": 0, "id": "9", "ok": true}
```

| Field | |
| --- | --- |
| `client`, `login` | That client id, that login - exactly |
| `kinds` | A list of kinds, or one string of them comma-separated |
| `since`, `until` | UTC milliseconds: at or after, before |
| `limit` | 100 by default, at most 1000 |
| `before` | The `next` of the page before: the older entries |
| `after` | Instead of a page: the entries after this id, oldest first - poll it to follow the log (`0`: from the oldest) |

Entries come newest first; `next` is there when there may be more. A page
stays under about 200 kB, so a reader's buffer of 256 kB takes any of them.
`dropped` counts the entries the server could not keep up with since it
started ([below](#the-broker-never-waits-for-it)).

**Over the [Application API](app-api.md#administration-and-rest)**:
`Admin.ListLog` (`client_id`, `login`, `kinds`, `since_ms`, `until_ms`,
`page_size`, `page_token`, `after_id`) and `GET /v1/log?clientId=&login=&kinds=a,b&sinceMs=&untilMs=&pageSize=&pageToken=&afterId=`.
Without a log it answers `FAILED_PRECONDITION`.

---

## How long it is kept

```yaml
diagnostics:
  enabled: true        # the default
  keep-days: 7         # 0: no age limit
  max-size-mb: 100     # the file stays at this; the oldest entries make room
  max-rows: 1000000    # 0: no row limit
  file: ""             # "": in the data folder (above)
```

Each is also an environment variable (`MODBEE_DIAGNOSTICS_KEEP_DAYS=30`) or a
stored setting (`modbee-mqtt config set diagnostics.keep-days=30`,
[settings.md](settings.md)).

- **Age.** Once a minute, entries older than `keep-days` are deleted.
- **Size.** When the file's pages are nine-tenths used, the oldest entries
  are deleted until it is three-quarters used, and new entries are written
  over the space they leave - so the file grows to `max-size-mb` and stays
  there, always holding the newest. A burst that would take it past the size
  makes room first. Its write-ahead log (`diagnostics.db-wal`) adds at most an
  eighth (4 MB at most) beside it. 100 MB holds several hundred thousand
  entries.
- **Rows.** Past `max-rows`, the oldest go.
- **An entry is at most a few kB**: a reason past 1 kB, or a detail past
  2 kB, is cut.

**Changed while it runs.** `config.set` of `diagnostics.enabled`,
`keep-days`, `max-size-mb` or `max-rows` applies at once, with no restart: a
lower limit deletes the oldest at once, and a lower `max-size-mb` gives the
space back. `diagnostics.file` waits for a restart. Turned off, nothing more
is kept; what is there stays until it ages out or the file is deleted.

---

## The broker never waits for it

An entry is put on a queue (50,000 at most) and the broker goes on; a thread
of its own writes the queue to the file, a few hundred entries to a
transaction. If the disk cannot keep up - or another program holds the file
locked - the queue fills and further entries are dropped and counted
(`dropped` in `log.list`, `Admin.ListLog`), and the broker is not slowed:
recording an entry takes a lock for as long as it takes to add one to a list.

Deleting the file while the server runs loses what it held; the server goes
on writing to it until it restarts. To start again, stop the server, delete
`diagnostics.db` (and `-wal`, `-shm`), and start it.

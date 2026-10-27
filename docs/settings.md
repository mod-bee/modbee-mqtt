# Settings over MQTT

A server with a login database ([logins.md](logins.md)) can also keep **its
settings** in that database, and you can read and change them over MQTT -
from the `modbee config` command, or from your own platform with nothing but
an admin login. Nobody needs a shell on the server or an editor on
`modbee.yaml`.

A change is **stored at once and applied when the server restarts**, and the
same request can ask for that restart. Logins are different: they change
while the broker runs ([logins.md](logins.md)). Settings - ports, limits,
Kafka mappings - are what a broker is built from when it starts, so they wait
for the next start rather than half-apply to a running one.

**The TLS certificate is the exception.** Its certificate, key and client CA
are applied the moment they are stored, with no restart
([below](#the-tls-material-applied-while-it-runs)), because a certificate is
renewed every few months and a broker a plant depends on should not stop for
it.

---

## What it needs

- **A login database.** A server installed with `modbee service install`
  has one - it runs `modbee serve --data <folder>`, which keeps everything in
  that folder, no file needed ([releases.md](releases.md#installing-it)).
  Otherwise `authentication: database:` in `modbee.yaml`, or
  `MODBEE_AUTHENTICATION_DATABASE`; the `docker/examples` setups already
  have it. The settings are kept in the same SQLite file as the logins, in a
  table of their own.
- **An admin login**, as for managing logins (`modbee admin` on the server).

With `--data` and no file, a stored setting is applied over the defaults;
with a file, over the file.

Without a login database, `modbee.yaml` and the environment are all there is.

---

## Two kinds of stored setting

| | What | Changed with |
| --- | --- | --- |
| **An override** | One setting, named by its path - `broker.port`, `broker.max-session-expiry`, `kafka.clusters[0].bootstrap-servers` | `config.set` |
| **A document** | A whole `modbee.yaml`, kept in the database **in place of** the file | `config.apply` |

The path of a setting is its place in `modbee.yaml`, and it is exactly the
setting an environment variable names: `broker.max-session-expiry` is
`MODBEE_BROKER_MAX_SESSION_EXPIRY`. An override is therefore what a variable
is - a single value, or a list written comma-separated - and it can do what a
variable can do and no more:

- It **changes a setting** the file (or document) could hold. A setting
  inside a section that is read only when it is written - `metrics:`,
  `broker: tls:` - can be overridden once that section exists, not before.
- It **cannot add** a Kafka cluster or mapping, or a section. That is a
  change to the shape of the configuration, so it takes a whole document.

---

## Which value wins

When the server starts, each setting comes from the first of these that has
it:

| | |
| --- | --- |
| 1 | A flag on the command line (`modbee serve -C modbee.yaml -p 1884`) |
| 2 | An environment variable (`MODBEE_BROKER_PORT`) |
| 3 | A stored override (`config.set`) |
| 4 | The stored document (`config.apply`) - or, when there is none, the file |
| 5 | The default |

**The environment wins over the database.** A setting a compose file pins
with a variable stays pinned, whatever is stored, and `config.get` lists
those variables as `pinned` so the reason is visible rather than guessed at.

**Authentication is the file's and the environment's alone.** A change over
MQTT that moved the login database, or changed the admin login, could lock
every admin out - and then nothing could change it back over MQTT. So
`config.set` refuses any `authentication...` path, and `config.apply`
refuses a document whose `authentication:` differs from what the file and
the environment give. That includes `authentication.certificate-login`
([logins.md](logins.md#logging-in-with-a-certificate)): turned on, an admin
presenting a certificate is the login its CN names, so it is set with
`MODBEE_AUTHENTICATION_CERTIFICATE_LOGIN=true` or in the file - and
`authentication.default-topics` (`MODBEE_AUTHENTICATION_DEFAULT_TOPICS`). A document must therefore carry the file's
`authentication:` section as it is.

**If the stored settings no longer load** when the server starts - a newer
version that reads a setting differently, a file that has changed under
them - the server starts from the file alone, prints each problem, and says
so:

```
modbee serve: the stored settings would not load - starting from /etc/modbee/modbee.yaml alone
```

It does not refuse to start, because a broker that will not start over a
stored setting is one nobody can reach to fix it. `config.get` then lists the
same problems.

---

## From the command line

`modbee config` uses the admin login, like `modbee user`. Inside a stack's
container it logs in by itself:

```bash
docker compose exec modbee modbee config get
```

```
settings from: file (/etc/modbee/modbee.yaml)
  broker.max-session-expiry = 86400
  pinned by the environment: MODBEE_AUTHENTICATION_USERNAME
  pinned by the environment: MODBEE_AUTHENTICATION_PASSWORD
the running server is behind these settings - restart it to apply them
```

| Command | Does |
| --- | --- |
| `modbee config get` | Where the settings come from, every override, and what the environment pins. `--json` prints the whole answer, the document included |
| `modbee config set broker.queue=128 broker.max-session-expiry=86400` | Store overrides |
| `modbee config set --remove broker.queue` | Drop an override, back to the file's value (`--remove` repeats) |
| `modbee config apply --file new.yaml` | Store a whole document in place of the file |
| `modbee config check --file new.yaml` or `modbee config check broker.port=1884` | Would these load? Prints every problem, stores nothing |
| `modbee config reset` | Drop the stored document and every override: back to the file alone |

`set`, `apply` and `reset` take **`--restart`**: once the change is stored,
the server restarts to apply it ([below](#applying-a-change-the-restart)).

A setting that does not exist, or a value the file would not accept either,
is refused with its problem, and nothing is stored:

```bash
docker compose exec modbee modbee config set broker.prot=1884
```

```
modbee: the settings were not stored: they would not load
  broker.prot: no setting is called "broker.prot"
```

A setting ending `-pem` takes `@file` for its value - the PEM text of a file
on the computer `modbee` runs on - because nobody types a certificate:

```bash
docker compose exec modbee modbee config set broker.tls.cert-pem=@/tmp/broker.pem broker.tls.key-pem=@/tmp/broker.key
```

```
stored - and the TLS listener is serving it now
```

`config get` then shows each stored PEM as `(PEM, n bytes)`, the stored key
as `(set)`, and what TLS is serving:

```
tls: serving on 8883 (wolfSSL)
  certificate: CN=mqtt.example.com, until 2027-01-05T10:00:00Z
               sha256 3dd049baf3c5e7eaeaea212abd68147b15c891250bf4f82f2c2b1e4974412dfc
  client certificates: required, from CN=Plant Devices CA
  certificate login: on
```

`--file` is read by the `modbee` command, on the computer it runs on. For a
document on your own computer, run `modbee` there against the broker -
`modbee config apply --file new.yaml -h mqtt.example.com --tls -u admin -P '...'` -
or copy it into the container first:

```bash
docker compose cp new.yaml modbee:/tmp/new.yaml
docker compose exec modbee modbee config apply --file /tmp/new.yaml --restart
```

---

## From your own platform

Every command is a JSON request, published to `$modbee/admin/request` with a
reply topic of `$modbee/admin/<your client id>/reply` subscribed - the same
channel as managing logins ([logins.md](logins.md#from-your-own-platform)).

| `command` | Fields | Answer |
| --- | --- | --- |
| `config.get` | | `source` (`file` or `database`), `file`, `document`, `overrides`, `pinned`, `problems`, `restart_required`, `tls` ([below](#the-tls-material-applied-while-it-runs)) |
| `config.check` | any of `document`, `set`, `remove` | `ok`, and `problems`. Stores nothing |
| `config.set` | `set`: `{"path": value, ...}`; optional `remove`: `[paths]`, `restart` | `problems`, `restart_required`, `restarting`, `tls_applied` |
| `config.apply` | `document`: the YAML, as text; optional `restart` | the same |
| `config.reset` | optional `restart` | the same |

```json
{"id": "7", "command": "config.set", "set": {"broker.port": 1884, "broker.max-session-expiry": 86400}}
```

```json
{"problems": [], "restart_required": true, "restarting": false, "id": "7", "ok": true}
```

- **A value** is a string, a number, `true`/`false`, or a list of those -
  which is stored comma-separated, as a variable would hold it.
- **`problems`** lists each one as `{"line", "path", "message", "warning"}`.
  `line` is the line in the document, or 0 for an override. A warning does not
  stop a change being stored.
- **Refused**, the answer is `"ok": false` with the `problems` and
  `"error": "the settings were not stored: they would not load"`.
- **`config.get`'s `document`** is the stored document, or the file when there
  is none, with every `password:` and `key-pem:` value shown as `(set)`; so
  is an override whose path ends in `password` or `key-pem`. A private key is
  never sent back, by any command. A document read back this way therefore
  cannot be applied as it is - its passwords would be the text `(set)`. Keep
  secrets out of documents: `password-file:`, or the environment.
- **`restart_required`** is true while what is stored differs from what the
  running server started with - leaving out the TLS material, which the
  running server takes as it is stored.
- **`tls_applied`** is true when the change gave the TLS listener new
  material, which it is serving now.

---

## The TLS material, applied while it runs

Four settings change what the TLS listener serves, and are applied to the
running server the moment they are stored - by `config.set`, `config.apply`
or `config.reset` alike:

| Path | |
| --- | --- |
| `broker.tls.cert-pem` | The certificate as PEM text - a full chain is presented whole |
| `broker.tls.key-pem` | Its private key, unencrypted PEM. Goes with `cert-pem`: one without the other is refused |
| `broker.tls.client-ca-pem` | One or more CA certificates. Set, client certificates are verified against it |
| `broker.tls.client-certificates` | `required` or `optional` ([kafka.md](kafka.md#tls-for-mqtt)) |

```json
{"id": "8", "command": "config.set", "set": {
  "broker.tls.cert-pem": "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n",
  "broker.tls.key-pem": "-----BEGIN PRIVATE KEY-----\nMIGH...\n-----END PRIVATE KEY-----\n"}}
```

```json
{"problems": [], "restart_required": false, "restarting": false, "tls_applied": true, "id": "8", "ok": true}
```

- **The text wins over the file's paths** (`broker.tls.cert`, `key`,
  `client-ca`), as every stored setting wins over the file.
- **It is checked before it is stored.** A certificate that will not parse, a
  key that is not its key, a CA that is not a certificate: each is refused
  with its problem while the old material is still being served. An expired
  certificate is stored, with a warning saying so. `config.check` asks the
  same question and stores nothing.
- **New handshakes get the new material; connections already up keep
  theirs.** Nothing is disconnected, and every CPU core of the server
  switches together.
- **With no `tls:` section in the file**, a certificate and its key stored
  this way turn TLS on, on `broker.tls.port` (8883 by default) - **at the next
  restart**: turning TLS on or off, and its port and address, are what a
  socket is bound to, so those still say `restart_required`. Ask for the
  restart with `"restart": true`.

`config.get`'s `tls` is what is being served **now**, never what is stored
for later, and never the key:

```json
"tls": {
  "serving": true,
  "port": 8883,
  "engine": "wolfSSL",
  "certificate": {
    "fingerprint": "3dd049baf3c5e7eaeaea212abd68147b15c891250bf4f82f2c2b1e4974412dfc",
    "subject": "CN=mqtt.example.com",
    "not_after": "2027-01-05T10:00:00Z",
    "names": ["DNS:mqtt.example.com", "IP:203.0.113.7"]
  },
  "key": "set",
  "client_ca": {"fingerprint": "1aa71917...", "subject": "CN=Plant Devices CA", "certificates": 1},
  "client_certificates": "required",
  "certificate_login": true
}
```

| Field | |
| --- | --- |
| `serving` | Whether there is a TLS listener. False: every other field is empty (`certificate` and `client_ca` are `null`) |
| `certificate.fingerprint` | SHA-256 of the certificate's DER: 64 lowercase hexadecimal characters - `openssl x509 -noout -fingerprint -sha256`, lowered, without its colons |
| `certificate.subject` | The subject, in the certificate's own order |
| `certificate.not_after` | When it expires, ISO 8601 UTC |
| `certificate.names` | Its subject alternative names: `DNS:`, `IP:`, `email:`, `URI:` |
| `client_ca` | The first CA certificate's `fingerprint` and `subject`, and how many `certificates` the bundle holds; `null` without one |
| `client_certificates` | `required`, `optional`, or `none` without a client CA |
| `certificate_login` | Whether a verified certificate logs in by itself ([logins.md](logins.md#logging-in-with-a-certificate)) |

A password set as an override is stored in the database as written - unlike a
login's secret, a setting has to be readable to be used - and goes into every
backup of it. `password-file:` keeps it out.

---

## Applying a change: the restart

A change applies when the server next starts. `--restart` (or
`"restart": true`) asks for that start now: once the answer has been sent,
the server stops **gracefully, exactly as `docker stop` stops it** - devices
are told the server is shutting down, and messages on their way to Kafka are
given their time.

What brings it back is whatever runs it:

- **Docker**, with a restart policy of `unless-stopped` (every compose file
  here has it) or `always`. A graceful stop exits with code 0, so `on-failure` does **not**
  bring it back.
- **systemd**, with `Restart=always`.
- **Nothing**, when the server was started by hand: it stays stopped.

`"restart": true` restarts only when it is needed: if what is stored is
already what the server is running, the answer has `"restarting": false` and
nothing stops.

---

## Backups

The settings are in the login database, so `modbee backup` and the daily copy
([logins.md](logins.md#backups)) hold them, and `modbee restore` puts them
back with the logins.

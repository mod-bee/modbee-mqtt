# Device logins

Every device has a login of its own - a user name and a secret - and you can
add, reset, disable and remove them **while the broker runs**. A change takes
effect at once: a new device can connect immediately, and one you disable is
disconnected on the spot. Nothing is restarted.

There are two ways a device gets its login:

| | How | Best for |
| --- | --- | --- |
| **It registers itself** (provisioning) | Every device is flashed with the same *provisioning key*. On first boot it asks the broker for a login, keeps it, and connects as itself | Many devices; devices you cannot easily reach |
| **You add it** | `modbee user add tank-01` prints a secret; you put it in the device | A few devices; equipment that cannot register itself |

Both are kept in a small database in the broker's data volume, and both can
be managed from the command line or from your own platform.

This is how the [`docker/examples`](../docker/examples) ship. [Without Docker](#without-docker)
is the same with one setting.

**Or let your own application decide.** A platform that already keeps its
devices' logins can answer for them itself, over the Application API -
asked once per login, and kept: [app-api.md](app-api.md#logins-decided-by-your-application).

---

## The admin login

`MQTT_USERNAME` (default `admin`) and `MQTT_PASSWORD` in `.env` are the
**admin** login. It is for you, your own apps and services, and the `modbee`
command - not for devices. It may use every topic, and it is the only login
that can manage the others (until you make another admin).

Inside the stack, the `modbee` command uses it by itself:

```bash
docker compose exec modbee modbee user list
```

From any other computer, give it: `modbee user list -h mqtt.example.com --tls -u admin -P '...'`.

---

## Adding a device by hand

```bash
docker compose exec modbee modbee user add tank-01 \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

```
added tank-01
  password: F3mQBcg8GVsexV2nd2GOTZcE5BtqIN65
  (shown once - it cannot be read back; `modbee user reset tank-01` issues a new one)
```

To choose the password yourself, add `--secret '<password>'`
(`modbee user password tank-01 --secret ...` changes it later).
Put `tank-01` and that password into the device (for this library:
`mqtt.credentials("tank-01", "F3mQ...")`). The `--publish` and `--subscribe`
rules are [what it may do](#what-a-device-may-do).

**The secret is shown once, and the broker cannot show it again** - it keeps
only a salted hash of it, so a copy of the database is no use to anyone. If a
secret is lost, you do not need it back: `modbee user reset tank-01` gives the
device a new one, and the old one stops working.

| Command | Does |
| --- | --- |
| `modbee user list` | Every login: status, how it was made, what it may do |
| `modbee user show tank-01` | One login |
| `modbee user reset tank-01` | A new secret; the old one stops working and the device is disconnected |
| `modbee user disable tank-01` / `enable` | Lock it out (disconnected at once) / let it back in |
| `modbee user permissions tank-01 --publish ... --subscribe ...` | Change what it may do |
| `modbee user password tank-01 --secret '...'` | A password you choose, for equipment where you must type one |
| `modbee user admin someone` | Make a login an admin (`--no-admin` to undo) |
| `modbee user set tank-01 --fingerprint ...` | Change several things at once - `--fingerprint`, `--publish`/`--subscribe`, `--admin`/`--no-admin`, `--secret` - and nothing else |
| `modbee user remove tank-01` | Delete it (disconnected at once) |

---

## Devices that register themselves

Create a **provisioning key** once, for a site or a product, and name the
site its devices belong to:

```bash
docker compose exec modbee modbee key create site-north --site north \
  --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
```

```
key site-north
  site-north.eDjPQkJQq6X9L0q9paHAxReD9KZc72PH
```

Flash that key into every device for the site. Each device also needs a
**unique name** (`tank-07`, a serial number, a tag) - it becomes its user name,
so it may not contain `/`, `+` or `#`, or start with `$`.
With this library that is two lines in the sketch, plus where to keep the
login ([`Provisioned_Device`](../examples/arduino/Provisioned_Device) is the
whole ESP32 sketch):

```cpp
mqtt.provision("site-north.eDjPQ...", &loadLogin, &saveLogin);
mqtt.connect("mqtt.example.com", "tank-07");
```

On first boot the device:

1. connects with the key (and can do nothing else with it),
2. is given its own login - its name and a secret only it holds,
3. keeps the login in flash (`saveLogin`), and
4. reconnects as itself, with the key's permissions.

From then on it connects with its own login, and nobody ever types, records
or looks up its secret.

**On an OpenPLC PLC** it is one block beside `MQTT_CLIENT`: `MQTT_PROVISION`,
with the key on its `KEY` pin and the PLC's name as the client's `CLIENT_ID`.
The login is kept in the block's retained memory. The modbee-openplc library's
page *13 Provisioning* has the whole of it.

**When things change, it sorts itself out:**

| Situation | What happens |
| --- | --- |
| A board is replaced | Flash the new one with the same name and a key of the same site. It registers, gets a new login, and the old board's login stops working |
| A device's flash is wiped | It has no login, so it registers again |
| You reset a device (`modbee user reset`) | Its stored login is refused, so it registers again by itself - no site visit |
| The broker's login store is lost, or restored from an old backup | Every device's login is refused at once. Each registers again with its key but **keeps its stored login** - it is replaced only when the broker issues a new one - and tries it again every five minutes, so a store that comes back lets the fleet straight back in, with nothing to approve |
| You disable a device | It stays out: registering again does not undo it |
| You remove the key (`modbee key remove`) | No new device can register with it; devices already registered keep working |
| You issue a new key for the site | Devices keep working (they log in with their own login, not the key); a new or wiped device registers with the new key |
| A key of **another** site is used with a device's name | Refused: a key registers again only devices of its own site, so one customer's key cannot take another's device |

A login **you** added by hand is never taken over by a device holding a key.
A key that wants each device approved (`--approval manual`) holds a device that
registers again as well: it waits, pending, until you approve it. While it
waits it still holds its old login, and tries it again every five minutes
(`rejectedCredentialsRetryMs`); on a PLC, `MQTT_PROVISION`'s retained login is
not cleared by a refusal, only overwritten by a login the broker has issued.

### Options for a key

| Option | Means |
| --- | --- |
| `--approval manual` | A new device waits until you approve it: `modbee user list` shows it `pending`, `modbee user approve tank-07` lets it in. Without this, devices are let in straight away |
| `--uses 50` | At most 50 devices may register with it |
| `--days 30` | It stops working after 30 days |
| `--site north` | The site its devices belong to: a key registers again only devices of its own site, so one customer's key cannot take another's device. Keys without a site share one - a broker for a single site needs none |
| `--device tank-07` | It may register only that one device, and every claim uses it up (`--uses 1`: claimed once) - e.g. a key your platform creates for one device it is setting up or replacing |
| `--data group=north-plant` | A value handed to each device it registers - repeat for more ([below](#what-a-key-hands-its-devices)) |

`modbee key list` shows every key; `modbee key remove site-north` retires one.

### What a key hands its devices

A key can carry a few named values that every device it registers is given, so
whoever sets the device up types only the key, the device's name and its
network. The broker keeps them and hands them out; it does not read them.

```bash
docker compose exec modbee modbee key create site-north --site north \
  --publish 'spBv1.0/north-plant/+/${username}' --publish 'acme/north/${username}/#' \
  --data group=north-plant --data prefix=acme/north
```

- **At sign-up** the answer carries them: `"data": {"group": "north-plant", "prefix": "acme/north"}`.
- **They stay with the device's login**, copied from the key when it registers.
  `modbee user set tank-07 --data group=... --data prefix=...` changes one
  device's; `modbee key set site-north --data ...` changes what the key hands
  devices that register from then on. A set replaces them all; `--data ''`
  clears them.
- **A device asks again whenever it likes** (its own values only, no new
  password) - devices of this library do so at every connect, so a change
  reaches them at their next connect.

Names are 1-32 letters, digits, `-`, `_` or `.`; values up to 255 bytes; 16 at
most. ModBee's PLC library reads `group` (its Sparkplug group) and `prefix`
(the start of its own topics, such as `prefix/<name>/$ota` for firmware
updates); anything else is yours to name.

### A device that cannot register itself

Register it from a computer instead, with the same key, and configure the
device with what comes back:

```bash
modbee provision 'site-north.eDjPQ...' --name gateway-3 -h mqtt.example.com --tls
```

---

## A login store from an earlier version

The login store's layout carries a version. This version keeps a site on every
key and login; a store an earlier version made is refused when the broker
starts:

```
modbee serve: the login store /var/lib/modbee/auth.db was made by an earlier version (schema 1, this one reads 2); remove it and add the logins again
```

Remove `auth.db` (and `auth.db-wal`, `auth.db-shm` beside it) from the
`modbee-data` volume - only those files: retained messages and Kafka are not in
it - start the broker, and add the logins and keys again.

---

## What a device may do

Each login has two lists of topic filters: what it may **publish** to and
what it may **subscribe** to. `${username}` and `${clientid}` in a filter are
replaced by the device's own, so one rule fits every device:

| Rule | `tank-07` may |
| --- | --- |
| publish `plant/${username}/#` | publish to `plant/tank-07/level`, `plant/tank-07/status`... and nowhere else |
| subscribe `plant/${username}/cmd` | receive its own commands - not another device's |
| no rules at all | use every topic (except the broker's own `$` topics), as on any broker without per-login rules - or, with `default-topics: none`, no topic at all |

**Separate devices by `${username}`, not `${clientid}`.** The user name is
proven by the password; the client identifier is whatever the client sends. A
login may not take an identifier another login's session holds, but it may
choose any identifier nobody is using - so `plant/${clientid}/#` lets a login
connecting as `tank-08` reach `tank-08`'s topics while that device is away.

A broker shared by several customers should not let a login without rules see
everything. Set, in `modbee.yaml`:

```yaml
authentication:
  default-topics: none     # all (default) | none - MODBEE_AUTHENTICATION_DEFAULT_TOPICS
```

and a login uses only the topics its rules name; one that should use every
topic is given `#` on purpose.

**One login, one share.** On a shared broker, no login may take what the
others need:

```yaml
authentication:
  max-sessions-per-login: 8      # client identifiers one login may hold (0 = no limit)
  max-retained-per-login: 1000   # retained topics one login may hold (0 = no limit)
```

A CONNECT that would give a login one session more is refused with 0x97
Quota exceeded (0x03 at 3.1.1); a retained message on a new topic past the
second limit is refused with 0x97 too, while replacing or clearing one of its
own always works. The admin login (`MQTT_USERNAME`) and `$provision` are not
limited; a database login marked admin is. Both are counted per shard.

A device's **Will** is held to its publish rules too: a CONNECT whose Will it
could not publish is refused. A Sparkplug node's NDEATH topic must be inside
them.

A subscription is allowed only if everything it could ever match is allowed,
so a device limited to `plant/tank-07/#` cannot subscribe to `plant/#`. When
you change a device's rules it is disconnected, and reconnects under the new
ones.

A key's rules are given to every device that registers with it.

### Names with `/` in them

A login you add may be named with a `/`: a platform that names its devices
`<namespace>/<device>` can give each one exactly that, `acme/riverbend/pump-plc`.
A login's name may be anything MQTT allows in a user name except empty,
longer than 256 bytes, starting with `$`, or holding a control character.

Such a login takes **explicit** rules, because a `${username}` rule never
fits it:

```bash
docker compose exec modbee modbee user add acme/riverbend/pump-plc \
  --publish 'acme/riverbend/#' --subscribe 'acme/riverbend/#'
```

**A `${username}` or `${clientid}` rule never widens.** The broker will not
put a value containing `/`, `+` or `#` into a filter - for such a login the
rule simply does not match, and grants nothing. A value is put in once and
never read again, so a login named `t${clientid}` connecting as `ank-07` is
`plant/t${clientid}/#`, not `plant/tank-07/#`. Otherwise `plant/${username}/#`
for a login named `+` would become `plant/+/#` - every device's topics - and
one named `acme/riverbend` would reach into the levels of every device below
it. (Mosquitto made the same choice for the same reason: CVE-2017-7650.)

**A name that becomes a topic keeps the strict rule.** The name a device
registers itself with, and a key's `--device`, may not contain `/`, `+` or
`#`, or start with `$`: the name is part of the device's provisioning topics,
and a key's rules are written with `${username}`. A key's own name is letters,
digits, `-` and `_`.

---

## Logging in with a certificate

A device can log in with **its client certificate instead of a password**.
Its certificate's Common Name is its login; the TLS handshake has already
proved it holds the key, so no password is asked for, and nothing secret is
ever typed into it or kept for it.

It needs TLS with a client CA - the authority that issues the devices'
certificates - and one setting, in `modbee.yaml` or the environment:

```yaml
broker:
  tls:
    cert: /etc/modbee/broker.pem
    key: /etc/modbee/broker.key
    client-ca: /etc/modbee/devices-ca.pem
    client-certificates: required        # or optional
authentication:
  database: /var/lib/modbee/auth.db
  certificate-login: true                # MODBEE_AUTHENTICATION_CERTIFICATE_LOGIN=true
```

The CA can be given over MQTT instead, and changed while the broker runs
([settings.md](settings.md#the-tls-material-applied-while-it-runs)).
`certificate-login` itself is file and environment only, like the rest of
`authentication:`.

Then add the login the certificate names, with its rules, as for any device:

```bash
docker compose exec modbee modbee user add tank-01 --publish 'plant/${username}/#'
```

and the device connects over TLS presenting a certificate the CA issued to
`CN=tank-01` - with no user name and no password.

| When a client certificate the handshake VERIFIED arrives | |
| --- | --- |
| The login | Its CN. The login's rules apply exactly as for a password, `${username}` is the CN, and the Kafka header `mqtt-username` and the client events carry it |
| The CONNECT has a user name | It must BE the CN, or the device is refused (0x86) - a certificate is not a ticket to be somebody else. Its password is not checked |
| No login has that name | Refused (0x86) |
| The login is disabled or waiting for approval | Refused (0x87) |
| The login has a `fingerprint` | Only the certificate with that fingerprint is accepted; another the same CA issued with the same CN is refused (0x86) |
| The certificate is expired, or from another CA | The handshake fails: there is no connection to refuse |
| The subject has two CNs | It names nobody, and is refused |

A device that presents **no** certificate - where `client-certificates` is
`optional` - logs in with a user name and password as usual. Every refusal is
counted with the rest (`status`, and the server's status line, which shows
certificate logins separately) and printed to the server's log with the CN,
where the device came from and the certificate's fingerprint:

```
modbee serve: certificate login refused for "tank-01" from 10.0.4.7:51544 (certificate cfb913...84ae): the login is bound to a different certificate (its fingerprint)
```

### Binding a login to one certificate

A CA may issue more than one certificate with the same CN - a replaced
device, a copied file. To accept **one**, give the login that certificate's
fingerprint:

```bash
openssl x509 -noout -fingerprint -sha256 -in tank-01.crt
docker compose exec modbee modbee user set tank-01 --fingerprint 'CF:B9:13:...:84:AE'
```

The fingerprint is SHA-256 of the certificate's DER, stored as 64 lowercase
hexadecimal characters; openssl's own spelling - upper case, colons,
`sha256 Fingerprint=` in front - is accepted and tidied. Binding a new one
disconnects a device on any other certificate at once. `--fingerprint ""`
clears it, and `modbee user add ... --fingerprint` sets it from the start. A
login with a fingerprint can still log in with its password, where it has
one.

---

## From your own platform

Everything above is also a JSON message over MQTT, which is how the `modbee`
command does it - so a platform, dashboard or script can manage logins with
nothing but an MQTT connection with an admin login. No REST API, no webhooks.

Subscribe to `$modbee/admin/<your client id>/reply`, then publish a request to
`$modbee/admin/request`:

```json
{"id": "42", "command": "user.add", "username": "tank-01",
 "publish": ["plant/${username}/#"], "subscribe": ["plant/${username}/cmd"]}
```

The answer arrives on your reply topic, with the same `id`:

```json
{"id": "42", "ok": true, "username": "tank-01", "password": "F3mQBcg8GVsexV2nd2GOTZcE5BtqIN65"}
```

or `{"id": "42", "ok": false, "error": "user tank-01 already exists"}`.

| `command` | Fields | Answer |
| --- | --- | --- |
| `user.add` | `username`; optional `password` (else one is generated), `publish`, `subscribe`, `admin`, `fingerprint`, `data` | `password` if generated |
| `user.set` | `username`, and any of `fingerprint` (`""` clears it), `publish`, `subscribe`, `admin`, `enabled`, `password`, `data` (the whole set; `{}` clears it) - only those named change | `user`, as it now is |
| `user.get` | `username` | `user`, with its `fingerprint` (`""` for none) |
| `user.list` | optional `prefix`, `after`, `limit` (up to 1000) | `users`, `total`, and `next` when there are more |
| `user.reset` | `username` | the new `password` |
| `user.password` | `username`, `password` | |
| `user.permissions` | `username`, `publish`, `subscribe` | |
| `user.enable`, `user.disable`, `user.approve`, `user.remove` | `username` | |
| `user.admin` | `username`, `admin` (true/false) | |
| `key.create` | `name`; optional `site`, `approval` (`auto`/`manual`), `publish`, `subscribe`, `uses`, `days`, `device`, `data` | `key` - shown once |
| `key.set` | `name`, `data` (the whole set; `{}` clears it): what the key hands devices that register from now on | `key`, as it now is |
| `key.list`, `key.remove` | `name` for remove | `keys` |
| `backup` | | `file` |
| `status` | | counts of logins, accepted, refused, provisioned, and of logins by certificate: `certificate_accepted`, `certificate_refused` |

A user or key record never includes a secret or its hash. A user's
`fingerprint` is a certificate's, which is public.

The same channel carries the server's **settings**: `config.get`,
`config.check`, `config.set`, `config.apply` and `config.reset` read and
change them, applied at the next restart ([settings.md](settings.md)). With
`events:` turned on, an admin can also follow what clients do - connect, go,
subscribe - on `$modbee/events/#` ([events.md](events.md)).

The device side is just as open, for firmware that does not use this library:
CONNECT with user name `$provision`, the key as password and the device's name
as Client Identifier; subscribe to `$modbee/provision/<name>/reply`; publish
`{}` to `$modbee/provision/<name>/request`. The answer is
`{"status": "ok", "username": "...", "password": "...", "data": {...}}`,
`{"status": "pending"}` or `{"status": "refused", "reason": "..."}`. A device
signed in with its own login asks again the same way, on the same two topics
of its Client Identifier (the only `$` topics it may use): the answer is
`{"status": "ok", "data": {...}}` - its login's data, never a password.
`data` is a JSON object, `"name": "value"` pairs, written without spaces.

---

## Backups

The logins live in `/var/lib/modbee/auth.db`, in the `modbee-data` volume. If
that volume were lost, every device's login would be gone - devices that
registered themselves would register again by themselves, but ones you added
by hand would be locked out. So:

- **A copy is made every 24 hours** in `/var/lib/modbee/backups/`, and the
  last 7 are kept.
- **`modbee backup`** makes one now:
  `docker compose exec modbee modbee backup`.
- **Keep a copy somewhere else**, as for any database:
  `docker compose cp modbee:/var/lib/modbee/backups ./modbee-backups`.

To restore one, stop modbee, restore, and start it:

```bash
docker compose stop modbee
docker compose run --rm --no-deps modbee restore /var/lib/modbee/backups/auth-20261005-010000.db
docker compose start modbee
```

`modbee restore` copies through SQLite itself, so a half-written log left
beside the old database is dealt with rather than replayed over the backup.

---

## How it holds up

- **The logins are in SQLite**, built into modbee: one file, nothing extra to
  run. Finding a login by name takes microseconds whether there are a thousand
  devices or ten million, and uses little memory.
- **Secrets are never stored**, only salted hashes. A secret the broker
  generated is 32 random characters, which cannot be guessed, so one fast
  SHA-256 hash protects it - and a million devices reconnecting after
  an outage cost the broker seconds, not hours. A password a person chose is
  stretched with PBKDF2 (4096 rounds) instead, because people choose
  guessable ones.
- **Changes are live on every CPU core of the server**: a new login works at
  its first CONNECT, and a removed, reset or disabled one is refused and
  disconnected within a fraction of a second.

---

## Without Docker

Installed as a service - `sudo modbee service install`, on Linux or Windows
([releases.md](releases.md#installing-it)) - the login database is already
there, in the data folder. The admin login is set on the server itself:

```bash
sudo modbee admin                  # asks for its password; again to change it
```

It works at once, on the running server. Then manage the logins with the
same `modbee user` and `modbee key` commands, giving the admin login with
`-u` and `-P` (or `MODBEE_AUTHENTICATION_USERNAME` and `_PASSWORD` in the
environment). Until there is an admin, nobody can log in.

With a `modbee.yaml` instead, `authentication: database:` names the file, and
`modbee admin --database <that file>` sets its admin - or the file's
`username` and `password` (or `password-file`) are an admin of their own.

Without a `database`, `username`/`password` (and a `users:` list) are a fixed
set of logins, read at start - simple, and enough for a handful of clients
that never change. A broker running on a board (`MqttBroker` in a sketch) has
the same fixed kind: [broker.md](broker.md).

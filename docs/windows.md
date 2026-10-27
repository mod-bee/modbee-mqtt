# modbee on Windows

`modbee.exe` is the same program as on Linux: the broker (`modbee serve`), the
Kafka pipe, the Application API and every command - built for Windows x64,
and run as a Windows service.

## Setting it up

Download `modbee-<version>-windows-x86_64.exe` from the
[Releases page](https://github.com/mod-bee/modbee-mqtt/releases), rename it
`modbee.exe`, and put it where it is to live - it is the whole program, with
nothing beside it ([releases.md](releases.md) shows how to check it is
genuine). Then, from an Administrator prompt:

```powershell
modbee.exe service install     # a Windows service, running now and at every start
modbee.exe admin               # the admin login: asks for its password
```

That is all. Everything it keeps - logins, settings, retained messages,
sessions - is in `C:\ProgramData\ModBee`, and everything after is the
`modbee` command: `modbee user add ...` for devices ([logins.md](logins.md)),
`modbee config set ...` for settings ([settings.md](settings.md)), and
`modbee admin` again for a new admin password - it works at once. Until there
is an admin, the broker runs and lets nobody in.

```powershell
modbee.exe service status      # modbee running
modbee.exe service stop
modbee.exe service start
modbee.exe service uninstall   # stops and removes it; what it kept stays
```

- **It starts with Windows**, and Windows starts it again if it ever stops
  unexpectedly (after 5 seconds, then 30).
- **Its output** goes to `C:\ProgramData\ModBee\modbee.log` (`--log <file>`
  chooses another). Started, stopped and failed are written to the Windows
  Event Log too, under the service's name.
- **A setting that needs a restart** (`modbee config set ... --restart`) stops
  the service with an error code, and Windows starts it again with the new
  setting.
- **More than one**: `--name <name>` on every command (default `modbee`), each
  keeping its own folder, `C:\ProgramData\ModBee\<name>` - and its own
  ports, set with `modbee config`.
- **Other `serve` options** go after `--`:
  `modbee.exe service install -- --config C:\ModBee\modbee.yaml` runs it from a
  `modbee.yaml` instead (a relative path is made absolute when it is
  installed - a service starts in `C:\Windows\System32`).

## Without the service

`modbee.exe serve --data C:\ModBee\data` runs it in a console, keeping
everything in that folder; Ctrl+C stops it the way a stop always does: every
client is told, and what is kept is kept. Or from a `modbee.yaml`, the same
file as on Linux ([kafka.md](kafka.md) lists every setting), with Windows
paths:

```yaml
sessions:
  store: file
  directory: C:\ProgramData\ModBee\sessions
retained:
  store: file
  file: C:\ProgramData\ModBee\retained.db
authentication:
  database: C:\ProgramData\ModBee\logins.db
```

`modbee.exe serve --config C:\ModBee\modbee.yaml`, and its admin is set with
`modbee.exe admin --database C:\ProgramData\ModBee\logins.db`.

## Your application, on the same machine

An application beside the server - a gateway's own software, a platform
backend - reaches the [Application API](app-api.md) over TCP on this machine
only, with a token. The same on Windows and Linux - one command, with a
token you choose:

```powershell
modbee.exe config set api.grpc=tcp://127.0.0.1:7660 api.token=<token> --restart -u admin -P <password>
```

(In a `modbee.yaml`: `api: {grpc: ["tcp://127.0.0.1:7660"], token-file: ...}`.)

Every call carries `authorization: Bearer <token>`; any gRPC library connects
to `127.0.0.1:7660`. (On Linux a Unix socket, `unix:/run/modbee/api.sock`, is
also there, locked by its file permissions.)

## How it differs from Linux, inside

Nothing you configure. On Linux every core's broker listens on the port and
the kernel spreads arriving connections between them; Windows has no such
thing, so the first broker accepts every connection and hands each to the
broker that owns its client id - which all of them do anyway, at CONNECT. Each
broker waits on its sockets through [wepoll](../third_party/wepoll/LICENSE),
Windows' equivalent of Linux's epoll.

# modbee on Windows

`modbee.exe` is the same program as on Linux: the broker (`modbee serve`), the
Kafka pipe, the Application API and every command - built for Windows x64,
and run as a Windows service.

## Running it

```powershell
modbee.exe serve --config C:\ModBee\modbee.yaml
```

The same `modbee.yaml` as on Linux ([kafka.md](kafka.md) lists every
setting). Paths are Windows paths:

```yaml
sessions:
  store: file
  directory: C:\ProgramData\ModBee\sessions
retained:
  store: file
  file: C:\ProgramData\ModBee\retained.db
authentication:
  username: admin
  password: change-me
  database: C:\ProgramData\ModBee\logins.db
```

Ctrl+C stops it the way a stop always does: every client is told, and what is
kept is kept.

## As a Windows service

From an Administrator prompt:

```powershell
modbee.exe service install -- --config C:\ModBee\modbee.yaml
modbee.exe service start
modbee.exe service status      # modbee running
modbee.exe service stop
modbee.exe service uninstall
```

- **It starts with Windows**, and Windows starts it again if it ever stops
  unexpectedly (after 5 seconds, then 30).
- **Everything after `--`** is what `modbee serve` is given. A relative
  `--config` path is made absolute when it is installed - a service starts in
  `C:\Windows\System32`.
- **Its output** goes to `C:\ProgramData\ModBee\modbee.log` (`--log <file>`
  chooses another). Started, stopped and failed are written to the Windows
  Event Log too, under the service's name.
- **More than one**: `--name <name>` on every command (default `modbee`), each
  with its own configuration and ports.
- **A setting that needs a restart** (`modbee config set ... --restart`) stops
  the service with an error code, and Windows starts it again with the new
  setting.

## Your application, on the same machine

An application beside the server - a gateway's own software, a platform
backend - reaches the [Application API](app-api.md) over TCP on this machine
only, with a token. The same on Windows and Linux:

```yaml
api:
  grpc: ["tcp://127.0.0.1:7660"]
  token-file: C:\ProgramData\ModBee\api-token
```

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

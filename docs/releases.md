# Releases

Each release on the [Releases page](https://github.com/mod-bee/modbee-mqtt/releases)
carries `modbee-mqtt` ready to run - no compiler, nothing to install beside it -
and the Docker image is published under the same version.

| For | x86-64 | ARM 64-bit (Raspberry Pi 4/5, ARM servers) |
|---|---|---|
| **Debian, Ubuntu** | `modbee-mqtt_<version>-1_amd64.deb` | `modbee-mqtt_<version>-1_arm64.deb` |
| **RHEL, Rocky, Alma, Fedora** | `modbee-mqtt-<version>-1.x86_64.rpm` | `modbee-mqtt-<version>-1.aarch64.rpm` |
| **Any Linux**: one file, nothing to install | `modbee-mqtt-<version>-linux-x86_64` | `modbee-mqtt-<version>-linux-arm64` |
| **Windows**: one file, also a Windows service | `modbee-mqtt-<version>-windows-x86_64.exe` | |
| **Docker** ([docker.md](docker.md)) | `ghcr.io/mod-bee/modbee-mqtt:<version>` | the same tag |

Beside them: `*.cdx.json`, each binary's SBOM (CycloneDX) - every library
inside it, with its version and licence - and `SHA256SUMS` with its
signature, `SHA256SUMS.sigstore.json`.

Every one is the whole of it: the broker (`modbee-mqtt serve`), the Kafka pipe,
the Application API and every command. It says so itself:

```
$ modbee-mqtt --version
modbee-mqtt 0.1.0 (TLS: OpenSSL)
  Kafka pipe: librdkafka 2.15.1 (gzip snappy ssl sasl regex lz4 sasl_gssapi sasl_plain sasl_scram plugins zstd sasl_oauthbearer)
  Application API: gRPC 1.81.1
```

The Linux binary is linked statically against musl, so it runs on any Linux
of its architecture - Debian, Ubuntu, RHEL, Alpine, a distribution from ten
years ago - with nothing else installed. The packages carry that same binary.

---

## Installing it

Put `modbee-mqtt` on the machine - one of these:

```sh
sudo apt install ./modbee-mqtt_0.1.0-1_amd64.deb        # Debian, Ubuntu
sudo dnf install ./modbee-mqtt-0.1.0-1.x86_64.rpm       # RHEL, Rocky, Alma, Fedora
sudo install modbee-mqtt-0.1.0-linux-x86_64 /usr/local/bin/modbee-mqtt     # any Linux
```

On Windows, rename `modbee-mqtt-0.1.0-windows-x86_64.exe` to `modbee-mqtt.exe` and put
it where it is to live ([windows.md](windows.md)).

Then the same two commands everywhere (Windows: in an Administrator prompt,
without `sudo`):

```sh
sudo modbee-mqtt service install      # runs it as a service, now and at every start
sudo modbee-mqtt admin                # the admin login: asks for its password
```

```
modbee-mqtt service: "modbee-mqtt" installed and running - it starts at boot, and again if it stops
  /usr/bin/modbee-mqtt serve --data /var/lib/modbee-mqtt
  everything it keeps: /var/lib/modbee-mqtt
  its log: journalctl -u modbee-mqtt
```

That is all there is to set up. Everything after is the `modbee-mqtt` command,
and all of it is kept in one database in the data folder - `/var/lib/modbee-mqtt`
(Windows: `C:\ProgramData\ModBee\modbee-mqtt`):

| | |
|---|---|
| Devices | `modbee-mqtt user add tank-01 -u admin -P ...` ([logins.md](logins.md)) |
| Settings | `modbee-mqtt config set broker.port=8883 --restart -u admin -P ...` ([settings.md](settings.md)) |
| The admin's password | `sudo modbee-mqtt admin` again - on the server, it works at once |
| The service | `modbee-mqtt service status`, `stop`, `start`, `uninstall` |

Until there is an admin, the broker runs and lets nobody in, and its log
says so. Every device needs a login of its own; retained messages and
sessions are kept across restarts. `modbee-mqtt service uninstall` (before
removing the package) stops and removes the service and keeps the data.

`modbee.yaml` is not needed for any of this. It is how the Docker image is
configured ([docker.md](docker.md)), and `modbee-mqtt serve -C modbee.yaml` still
reads one anywhere.

---

## Checking a download

Any one of these is enough. Each proves the file was built by this
repository's release workflow from the tagged commit, and not changed since.

**GitHub's attestation** - every file has one; with the [GitHub CLI](https://cli.github.com):

```sh
gh attestation verify modbee-mqtt_0.1.0-1_amd64.deb --repo mod-bee/modbee-mqtt
```

**The checksums, signed** - `SHA256SUMS` lists every file, and is signed
with Sigstore. Check the signature with
[cosign](https://docs.sigstore.dev/cosign/system_config/installation/), then
the files against it:

```sh
cosign verify-blob SHA256SUMS --bundle SHA256SUMS.sigstore.json \
  --certificate-identity https://github.com/mod-bee/modbee-mqtt/.github/workflows/release.yml@refs/tags/v0.1.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --check --ignore-missing SHA256SUMS
```

On Windows: `Get-FileHash modbee-mqtt-0.1.0-windows-x86_64.exe` and compare with
its line in `SHA256SUMS`.

---

## Making a release

1. **The version**, the same in both places: `project(... VERSION x.y.z)` in
   `CMakeLists.txt`, and `version=` in `library.properties` (the Arduino
   Library Manager reads it).
2. **The notes**: in [CHANGELOG.md](../CHANGELOG.md), what is under
   *Unreleased* moves to a new `## [x.y.z]` section. That section is the
   release's notes.
3. **Try it** (optional, recommended): *Actions → release → Run workflow* on
   `main`. It builds, tests, signs and checks everything a tag would, and
   leaves the files on the run instead of publishing them. The first run
   fills the build caches - gRPC from source, most of an hour - so the tagged
   run after it takes minutes.
4. **Tag and push**:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

The release workflow (`.github/workflows/release.yml`) refuses a tag that is
not the version in both files, or a version with no CHANGELOG section,
before it builds anything. A build without the Kafka pipe or the
Application API - or a Linux binary that is not static, or a Windows one
that needs a DLL Windows does not have - stops it too. Nothing is published
until all three binaries are built, tested and signed. `docker.yml` publishes
the image from the same tag.

To build the Linux binary on your own machine, with only Docker:

```sh
docker run --rm -v "$PWD":/src -w /src alpine:3.22 sh scripts/release/build-linux.sh
```

It leaves the binary and its SBOM in `dist/` ([build-linux.sh](../scripts/release/build-linux.sh));
`sh scripts/release/packages.sh <version>` then makes the `.deb` and `.rpm`
from the x86-64 and ARM64 binaries there.

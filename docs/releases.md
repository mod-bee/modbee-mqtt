# Releases

Each release on the [Releases page](https://github.com/mod-bee/modbee-mqtt/releases)
carries `modbee` ready to run - no compiler, nothing to install beside it -
and the Docker image is published under the same version.

| File | For |
|---|---|
| `modbee-<version>-linux-x86_64.tar.gz` | Linux on x86-64: one static binary, any distribution |
| `modbee-<version>-linux-arm64.tar.gz` | Linux on ARM 64-bit: a Raspberry Pi 4 or 5, an ARM server |
| `modbee-<version>-windows-x86_64.zip` | Windows on x86-64: `modbee.exe`, one file ([windows.md](windows.md)) |
| `*.cdx.json` | Each one's SBOM (CycloneDX): every library inside it, with its version and licence |
| `*.sigstore.json` | Each file's signature |
| `SHA256SUMS` | The checksum of every file |
| Docker | `ghcr.io/mod-bee/modbee-mqtt:<version>`, amd64 and arm64 ([docker.md](docker.md)) |

Every binary is the whole of it: the broker (`modbee serve`), the Kafka pipe,
the Application API and every command. It says so itself:

```
$ modbee --version
modbee 0.1.0 (TLS: OpenSSL)
  Kafka pipe: librdkafka 2.15.1 (gzip snappy ssl sasl regex lz4 sasl_gssapi sasl_plain sasl_scram plugins zstd sasl_oauthbearer)
  Application API: gRPC 1.81.1
```

The Linux binary is linked statically against musl, so it runs on any Linux
of its architecture - Debian, Ubuntu, RHEL, Alpine, a distribution from ten
years ago - with nothing else installed.

---

## Installing on Linux

```sh
tar xzf modbee-0.1.0-linux-x86_64.tar.gz
sudo install modbee-0.1.0-linux-x86_64/modbee /usr/local/bin/
modbee --version
```

Run as a service with systemd - `Restart=always` is also what brings it back
after a setting that needs a restart ([settings.md](settings.md#applying-a-change-the-restart)):

```ini
# /etc/systemd/system/modbee.service
[Unit]
Description=ModBee MQTT
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/modbee serve --config /etc/modbee/modbee.yaml
Restart=always
User=modbee
StateDirectory=modbee

[Install]
WantedBy=multi-user.target
```

```sh
sudo useradd --system --no-create-home modbee
sudo systemctl enable --now modbee
```

## Installing on Windows

Unzip it anywhere; `modbee.exe service install` makes it a Windows service
([windows.md](windows.md)).

---

## Checking a download

Three ways, each enough on its own. They prove the file was built by this
repository's release workflow from the tagged commit, and not changed since.

**GitHub's attestation** - with the [GitHub CLI](https://cli.github.com):

```sh
gh attestation verify modbee-0.1.0-linux-x86_64.tar.gz --repo mod-bee/modbee-mqtt
```

**Sigstore** - with [cosign](https://docs.sigstore.dev/cosign/system_config/installation/),
and the file's `.sigstore.json` beside it:

```sh
cosign verify-blob modbee-0.1.0-linux-x86_64.tar.gz \
  --bundle modbee-0.1.0-linux-x86_64.tar.gz.sigstore.json \
  --certificate-identity https://github.com/mod-bee/modbee-mqtt/.github/workflows/release.yml@refs/tags/v0.1.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

**The checksum** - against `SHA256SUMS`, which is itself signed and attested
the same way:

```sh
sha256sum --check --ignore-missing SHA256SUMS
```

On Windows: `Get-FileHash modbee-0.1.0-windows-x86_64.zip` and compare with
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

It leaves the same `.tar.gz` and SBOM in `dist/` ([build-linux.sh](../scripts/release/build-linux.sh)).

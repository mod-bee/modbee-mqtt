#!/bin/sh
# ModBee MQTT.
#
# Licensed under the GNU Affero General Public License, Version 3, WITH
# ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
#
# THE .deb AND .rpm PACKAGES, for x86-64 and ARM64, from the static binaries
# build-linux.sh left in dist/: the program, the modbee systemd service, a
# starting modbee.yaml, and an admin password chosen at install
# (scripts/release/packaging/). Runs on any x86-64 Linux, from the
# repository's root:
#
#   sh scripts/release/packages.sh 0.1.0
#
# Leaves in dist/: modbee_<v>-1_amd64.deb, modbee_<v>-1_arm64.deb,
# modbee-<v>-1.x86_64.rpm, modbee-<v>-1.aarch64.rpm.

set -eu

version=${1:?usage: packages.sh <version>}

NFPM_VERSION=2.47.0
NFPM_SHA256=0660ca602b2d2d2ae4781a06c692b3eeb9d437ffea05b831d76e41f4a3188783   # Linux_x86_64
tools=${TMPDIR:-/tmp}/modbee-nfpm-$NFPM_VERSION
if [ ! -x "$tools/nfpm" ]; then
    mkdir -p "$tools"
    curl -fsSL -o "$tools/nfpm.tar.gz" \
        "https://github.com/goreleaser/nfpm/releases/download/v$NFPM_VERSION/nfpm_${NFPM_VERSION}_Linux_x86_64.tar.gz"
    echo "$NFPM_SHA256  $tools/nfpm.tar.gz" | sha256sum -c - >/dev/null
    tar xzf "$tools/nfpm.tar.gz" -C "$tools" nfpm
fi

for arch in x86_64 arm64; do
    binary=dist/modbee-$version-linux-$arch
    test -f "$binary" || { echo "packages: no $binary - run build-linux.sh first" >&2; exit 1; }
    # nfpm's names (Go's); it writes each format's own - amd64 is x86_64 in an .rpm.
    case $arch in x86_64) goarch=amd64 ;; arm64) goarch=arm64 ;; esac
    # Where nfpm.yaml takes it from: nfpm expands variables in its fields but
    # not in a file's path.
    mkdir -p build-packages
    cp "$binary" build-packages/modbee
    for format in deb rpm; do
        MODBEE_VERSION=$version MODBEE_ARCH=$goarch \
            "$tools/nfpm" package --config scripts/release/packaging/nfpm.yaml \
            --packager "$format" --target dist/
    done
done
rm -rf build-packages
ls -l dist/*.deb dist/*.rpm

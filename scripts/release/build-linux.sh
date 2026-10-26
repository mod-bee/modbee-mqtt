#!/bin/sh
# ModBee MQTT.
#
# Licensed under the GNU Affero General Public License, Version 3, WITH
# ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.
#
# THE LINUX RELEASE BINARY: modbee as one file, fully static against musl, so
# it runs on any Linux of its architecture with nothing installed beside it -
# the broker, the Kafka pipe, the Application API and every command.
#
# Runs inside Alpine, as root, from the repository's root. The release
# workflow runs it on GitHub's x64 and ARM machines; the same works locally:
#
#   docker run --rm -v "$PWD":/src -w /src -v modbee-vcpkg:/cache \
#       -e VCPKG_DEFAULT_BINARY_CACHE=/cache alpine:3.22 sh scripts/release/build-linux.sh
#
# Leaves, in dist/:
#   modbee-<version>-linux-<arch>.tar.gz     modbee, LICENSE, README.md
#   modbee-<version>-linux-<arch>.cdx.json   its CycloneDX SBOM
#
# gRPC, protobuf, librdkafka and OpenSSL are built from source by vcpkg, at
# the version VCPKG_TAG pins - Alpine packages none of them as static
# libraries. That is most of an hour the first time; VCPKG_DEFAULT_BINARY_CACHE
# keeps what was built, and the workflow caches that directory.

set -eu

VCPKG_TAG=2026.10.16
VCPKG_ROOT=${VCPKG_ROOT:-/opt/vcpkg}
export VCPKG_FORCE_SYSTEM_BINARIES=1      # Alpine: its own cmake and ninja
export VCPKG_DISABLE_METRICS=1

case "$(uname -m)" in
    x86_64) arch=x86_64 triplet=x64-linux-static-release ;;
    aarch64) arch=arm64 triplet=arm64-linux-static-release ;;
    *) echo "build-linux: no release for $(uname -m)" >&2; exit 1 ;;
esac
test -f /etc/alpine-release || { echo "build-linux: run this inside Alpine (musl)" >&2; exit 1; }

version=$(sed -n 's/^ *VERSION \([0-9][0-9.]*\)$/\1/p' CMakeLists.txt | head -1)
test -n "$version" || { echo "build-linux: no VERSION in CMakeLists.txt" >&2; exit 1; }
name=modbee-$version-linux-$arch
echo "build-linux: $name"

apk add --no-cache build-base cmake ninja git curl zip unzip tar pkgconf perl \
    linux-headers bash python3 file >/dev/null

if [ ! -x "$VCPKG_ROOT/vcpkg" ]; then
    git clone --quiet --depth 1 --branch "$VCPKG_TAG" https://github.com/microsoft/vcpkg "$VCPKG_ROOT"
    "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics
fi
if [ -n "${VCPKG_DEFAULT_BINARY_CACHE:-}" ]; then mkdir -p "$VCPKG_DEFAULT_BINARY_CACHE"; fi

# shellcheck disable=SC2046 - the list is words on purpose
"$VCPKG_ROOT/vcpkg" install $(grep -v '^#' .github/vcpkg-packages.txt) \
    --overlay-triplets=.github/vcpkg-triplets --triplet "$triplet" --host-triplet "$triplet" \
    --clean-after-build

rm -rf build-release-linux
cmake -S . -B build-release-linux -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
    -DVCPKG_OVERLAY_TRIPLETS="$PWD/.github/vcpkg-triplets" \
    -DVCPKG_TARGET_TRIPLET="$triplet" -DVCPKG_HOST_TRIPLET="$triplet" \
    -DCMAKE_EXE_LINKER_FLAGS=-static \
    -DMQTT_BUILD_APP_API=ON -DMQTT_BUILD_EXAMPLES=OFF -DMQTT_BUILD_DEV_TESTS=OFF \
    | tee build-release-linux.configure.txt
# Everything, or no release: a modbee without Kafka or the API would pass
# every test below and still not be what the release promises.
for part in 'librdkafka .* found' 'gRPC .* found' 'OpenSSL found'; do
    grep -q "$part" build-release-linux.configure.txt || { echo "build-linux: not found: $part" >&2; exit 1; }
done

cmake --build build-release-linux
ctest --test-dir build-release-linux --output-on-failure

binary=build-release-linux/cli/modbee
strip "$binary"
file "$binary"
file "$binary" | grep -q 'statically linked\|static-pie linked' || { echo "build-linux: not static" >&2; exit 1; }
"$binary" --version | tee build-release-linux.version.txt
for part in 'Kafka pipe: librdkafka.*ssl' 'sasl_scram' 'zstd' 'Application API: gRPC'; do
    grep -q "$part" build-release-linux.version.txt || { echo "build-linux: --version lacks: $part" >&2; exit 1; }
done

mkdir -p "dist/$name"
cp "$binary" LICENSE README.md "dist/$name/"
tar -C dist -czf "dist/$name.tar.gz" "$name"
rm -rf "dist/$name"
python3 scripts/release/sbom.py --installed "$VCPKG_ROOT/installed/$triplet" --version "$version" \
    --platform "linux-$arch" --out "dist/$name.cdx.json"

# Owned by whoever owns the checkout, not root, when run from Docker.
owner=$(stat -c %u:%g .)
chown -R "$owner" dist build-release-linux build-release-linux.*.txt
ls -l dist

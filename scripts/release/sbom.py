#!/usr/bin/env python3
# ModBee MQTT.
#
# Licensed under the GNU Affero General Public License, Version 3, WITH
# ADDITIONAL TERMS - see LICENSE. Commercial use requires a separate licence.

"""The CycloneDX SBOM of one release binary: what is linked into it.

Every library vcpkg built for it - read from the SPDX record vcpkg writes for
each package it installs, so name, version, licence and source are vcpkg's own
- and the code vendored in this repository. The build-only helper ports
(vcpkg-cmake and friends) are left out: they run while building and none of
their code is in the binary.

    sbom.py --installed <vcpkg>/installed/<triplet> --version 0.1.0 \\
            --platform linux-x86_64 --out modbee-mqtt-0.1.0-linux-x86_64.cdx.json
"""

import argparse
import datetime
import glob
import json
import os
import uuid

# In the binary, from this repository. wepoll is Windows only.
VENDORED = [
    ("libyaml", "0.2.5", "MIT", "https://github.com/yaml/libyaml", None),
    ("sqlite", "3.53.4", "blessing", "https://sqlite.org", None),
    ("wepoll", "1.5.8", "BSD-2-Clause", "https://github.com/piscisaureus/wepoll", "windows"),
]

BUILD_ONLY = ("vcpkg-", "pkgconf")

# Ports whose vcpkg recipe declares no licence, with the one their source
# carries (librdkafka: LICENSE, BSD-2-Clause).
UNDECLARED = {"librdkafka": "BSD-2-Clause"}


def vcpkg_packages(installed):
    found = []
    for path in sorted(glob.glob(os.path.join(installed, "share", "*", "vcpkg.spdx.json"))):
        with open(path, encoding="utf-8") as f:
            spdx = json.load(f)
        port = os.path.basename(os.path.dirname(path))
        if port.startswith(BUILD_ONLY):
            continue
        package = next((p for p in spdx.get("packages", []) if p.get("name") == port), None)
        if package is None:
            continue
        # vcpkg fills one of the two and writes NOASSERTION in the other.
        licence = next((package[k] for k in ("licenseConcluded", "licenseDeclared")
                        if package.get(k) and package[k] != "NOASSERTION"
                        and not package[k].startswith("LicenseRef-vcpkg-null")), None)
        licence = licence or UNDECLARED.get(port, "NOASSERTION")
        source = package.get("downloadLocation") or "NOASSERTION"
        # "1.81.1#2": what follows # is vcpkg's own revision of its recipe, not the library's version.
        version = package.get("versionInfo", "").split("#", 1)[0]
        found.append((port, version, licence, source))
    return found


def component(name, version, licence, source):
    c = {
        "type": "library",
        "bom-ref": f"{name}@{version}",
        "name": name,
        "version": version,
        "purl": f"pkg:generic/{name}@{version}",
    }
    if licence and licence != "NOASSERTION":
        c["licenses"] = [{"expression": licence}]
    if source and source != "NOASSERTION":
        url = source.split("git+", 1)[-1].split("@", 1)[0] if source.startswith("git+") else source
        c["externalReferences"] = [{"type": "vcs" if source.startswith("git+") else "distribution", "url": url}]
    return c


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--installed", required=True, help="vcpkg's installed/<triplet> directory")
    parser.add_argument("--version", required=True)
    parser.add_argument("--platform", required=True, help="linux-x86_64, linux-arm64, windows-x86_64")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    packages = vcpkg_packages(args.installed)
    if not any(p[0] == "grpc" for p in packages) or not any(p[0] == "librdkafka" for p in packages):
        raise SystemExit(f"sbom: no grpc or librdkafka under {args.installed} - not the installed tree that was linked")

    components = [component(*p) for p in packages]
    for name, version, licence, source, only in VENDORED:
        if only and not args.platform.startswith(only):
            continue
        components.append(component(name, version, licence, source))

    bom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tools": {"components": [{"type": "application", "name": "modbee-mqtt scripts/release/sbom.py"}]},
            "component": {
                "type": "application",
                "bom-ref": "modbee-mqtt",
                "name": "modbee-mqtt",
                "version": args.version,
                "description": f"modbee-mqtt {args.version} for {args.platform}: the broker, the Kafka pipe, the Application API and the CLI",
                "licenses": [{"license": {"name": "AGPL-3.0 with additional ModBee terms (see LICENSE)"}}],
                "purl": f"pkg:github/mod-bee/modbee-mqtt@v{args.version}",
            },
        },
        "components": components,
        "dependencies": [{"ref": "modbee-mqtt", "dependsOn": [c["bom-ref"] for c in components]}],
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(bom, f, indent=2)
        f.write("\n")
    print(f"sbom: {len(components)} components -> {args.out}")


if __name__ == "__main__":
    main()

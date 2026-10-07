# libyaml 0.2.5 (vendored)

The YAML parser behind `modbee.yaml` (server/config). Plain C, MIT licence
(LICENSE here), errors returned with a line and column rather than thrown -
which is why it was chosen over rapidyaml, whose only error path without C++
exceptions is a longjmp out of C++ code.

- Source: https://github.com/yaml/libyaml/releases/download/0.2.5/yaml-0.2.5.tar.gz
- SHA-256 of that tarball: c642ae9b75fee120b2d96c712538bd2cf283228d2337df2cf2988e3c02678ef4
- Taken unmodified: `src/*.c`, `src/yaml_private.h`, `include/yaml.h`, `License`.
- Built by server/CMakeLists.txt as the static library `modbee-yaml`; the
  version macros its `config.h` would supply are passed as definitions there.

# What the Windows build links: static libraries and a static C runtime, so
# modbee-mqtt.exe is one file with nothing beside it. Release only - the debug
# build of gRPC would double CI's time and its disk for nothing we ship. Also
# the host triplet, so gRPC's code generator is built once, not twice.
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_BUILD_TYPE release)

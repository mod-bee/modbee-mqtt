# What the Linux release links: every library static, on musl (Alpine), so
# modbee is one file that runs on any Linux. Release only, and the host
# triplet too, as on Windows (x64-windows-static-release.cmake).
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
set(VCPKG_BUILD_TYPE release)

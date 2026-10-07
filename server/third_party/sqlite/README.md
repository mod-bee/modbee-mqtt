# SQLite, vendored

The amalgamation (`sqlite3.c`, `sqlite3.h`) of **SQLite 3.53.4**, from
https://www.sqlite.org/2026/sqlite-amalgamation-3530400.zip, verified against
the SHA3-256 sqlite.org publishes:
`628a44cfe82c66aed1ccbbe85a562d2e33ebe64b3288981ed76285612227934e`.

Unmodified. SQLite is in the public domain (https://www.sqlite.org/copyright.html).

Used by the server tier only - the device-login store (`server/auth/`). It is
never part of the Arduino library. Compile options are in
`server/CMakeLists.txt`.

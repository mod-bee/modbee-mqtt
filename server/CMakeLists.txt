# The server tier: one MqttBroker per core behind one port (ShardedBroker),
# its configuration file (modbee.yaml), and the Kafka pipe.
#
# Linux only (SO_REUSEPORT, epoll, eventfd), never part of the Arduino library,
# and outside src/ so no sketch ever sees it. C++17 like the tests; exceptions
# and RTTI off like everything else (R8).
#
# Dependencies: libyaml, vendored under third_party/ (C, MIT); and librdkafka
# for the Kafka pipe, used only when it is installed - without it `modbee serve`
# runs the broker alone and says so if a file asks for Kafka.

enable_language(C)

# --- libyaml, vendored ---
add_library(modbee-yaml STATIC
    third_party/libyaml/src/api.c
    third_party/libyaml/src/dumper.c
    third_party/libyaml/src/emitter.c
    third_party/libyaml/src/loader.c
    third_party/libyaml/src/parser.c
    third_party/libyaml/src/reader.c
    third_party/libyaml/src/scanner.c
    third_party/libyaml/src/writer.c
)
target_include_directories(modbee-yaml PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/third_party/libyaml/include)
target_compile_definitions(modbee-yaml
    PUBLIC YAML_DECLARE_STATIC
    PRIVATE YAML_VERSION_MAJOR=0 YAML_VERSION_MINOR=2 YAML_VERSION_PATCH=5
            "YAML_VERSION_STRING=\"0.2.5\"")
set_target_properties(modbee-yaml PROPERTIES C_STANDARD 99 POSITION_INDEPENDENT_CODE ON)
if(NOT MSVC)
    # Third-party code, built as its authors wrote it: their warnings are not ours to fix.
    target_compile_options(modbee-yaml PRIVATE -w)
endif()

# --- SQLite, vendored (third_party/sqlite/README.md) - the device-login store ---
add_library(modbee-sqlite STATIC third_party/sqlite/sqlite3.c)
target_include_directories(modbee-sqlite PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sqlite)
target_compile_definitions(modbee-sqlite PRIVATE
    SQLITE_DQS=0 SQLITE_OMIT_LOAD_EXTENSION SQLITE_DEFAULT_MEMSTATUS=0
    SQLITE_DEFAULT_WAL_SYNCHRONOUS=1 SQLITE_LIKE_DOESNT_MATCH_BLOBS)
set_target_properties(modbee-sqlite PROPERTIES C_STANDARD 99 POSITION_INDEPENDENT_CODE ON)
find_package(Threads REQUIRED)
target_link_libraries(modbee-sqlite PUBLIC Threads::Threads ${CMAKE_DL_LIBS})
if(NOT MSVC)
    target_compile_options(modbee-sqlite PRIVATE -w)
endif()

# --- modbee.yaml ---
add_library(modbee-config STATIC
    config/ServerConfig.cpp
)
target_link_libraries(modbee-config PUBLIC modbee-mqtt PRIVATE modbee-yaml)
target_include_directories(modbee-config PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/config)
target_compile_features(modbee-config PUBLIC cxx_std_17)

# --- the sharded broker ---
add_library(modbee-server STATIC
    ShardedBroker.cpp
    MetricsEndpoint.cpp
)
target_link_libraries(modbee-server PUBLIC modbee-mqtt Threads::Threads)
target_include_directories(modbee-server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(modbee-server PUBLIC cxx_std_17)

# --- device logins, live: authentication: database: (server/auth/) ---
add_library(modbee-auth STATIC auth/AuthHash.cpp auth/Json.cpp auth/AuthStore.cpp auth/AuthGate.cpp
    auth/AuthGateSettings.cpp auth/TlsSettings.cpp)
target_link_libraries(modbee-auth PUBLIC modbee-server modbee-sqlite modbee-config)
target_include_directories(modbee-auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/auth)
target_compile_features(modbee-auth PUBLIC cxx_std_17)
if(NOT MSVC)
    target_compile_options(modbee-auth PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
endif()

# --- what clients do, as messages: events: (server/events/) ---
add_library(modbee-events STATIC events/BrokerEvents.cpp)
target_link_libraries(modbee-events PUBLIC modbee-server)
target_include_directories(modbee-events PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/events)
target_compile_features(modbee-events PUBLIC cxx_std_17)

# --- retained messages that outlive a restart: retained: store: file ---
add_library(modbee-retained STATIC retained/RetainedKeeper.cpp)
target_link_libraries(modbee-retained PUBLIC modbee-server)
target_include_directories(modbee-retained PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/retained)
target_compile_features(modbee-retained PUBLIC cxx_std_17)

if(NOT MSVC)
    target_compile_options(modbee-config PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
    target_compile_options(modbee-server PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
    target_compile_options(modbee-retained PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
endif()

# --- the Kafka pipe, where librdkafka is installed ---
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(RDKAFKA QUIET IMPORTED_TARGET rdkafka)
endif()
if(RDKAFKA_FOUND)
    add_library(modbee-kafka STATIC kafka/KafkaPipe.cpp kafka/KafkaCommon.cpp kafka/KafkaRetained.cpp)
    target_link_libraries(modbee-kafka PUBLIC modbee-server modbee-config modbee-retained PkgConfig::RDKAFKA)
    target_include_directories(modbee-kafka PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/kafka)
    target_compile_definitions(modbee-kafka PUBLIC MODBEE_HAVE_KAFKA=1)
    target_compile_features(modbee-kafka PUBLIC cxx_std_17)
    if(NOT MSVC)
        target_compile_options(modbee-kafka PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
    endif()
    message(STATUS "modbee: librdkafka ${RDKAFKA_VERSION} found - the Kafka pipe is built")
else()
    message(STATUS "modbee: no librdkafka - modbee serve runs without the Kafka pipe")
endif()

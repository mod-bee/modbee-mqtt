# The server tier: one MqttBroker per core behind one port (ShardedBroker),
# its configuration file (modbee.yaml), and the Kafka pipe.
#
# Linux only (SO_REUSEPORT, epoll, eventfd), never part of the Arduino library,
# and outside src/ so no sketch ever sees it. C++17 like the tests; exceptions
# and RTTI off like everything else (R8).
#
# Dependencies: libyaml, vendored under third_party/ (C, MIT); and librdkafka
# for the Kafka pipe, used only when it is installed - without it `modbee-mqtt serve`
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
    WakeSignal.cpp
    ServerOs.cpp
    MetricsEndpoint.cpp
)
target_link_libraries(modbee-server PUBLIC modbee-mqtt Threads::Threads)
if(WIN32)
    target_link_libraries(modbee-server PUBLIC bcrypt)   # BCryptGenRandom (ServerOs.cpp)
endif()
target_include_directories(modbee-server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(modbee-server PUBLIC cxx_std_17)

# --- device logins, live: authentication: database: (server/auth/) ---
add_library(modbee-auth STATIC auth/AuthHash.cpp auth/Json.cpp auth/AuthStore.cpp auth/AuthGate.cpp auth/TopicRules.cpp
    auth/AuthGateSettings.cpp auth/TlsSettings.cpp)
target_link_libraries(modbee-auth PUBLIC modbee-server modbee-sqlite modbee-config)
target_include_directories(modbee-auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/auth)
target_compile_features(modbee-auth PUBLIC cxx_std_17)
if(NOT MSVC)
    target_compile_options(modbee-auth PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
endif()

# --- what clients do, as messages: events: (server/events/) ---
add_library(modbee-sessions STATIC sessions/SessionKeeper.cpp)
target_link_libraries(modbee-sessions PUBLIC modbee-server modbee-sqlite)
target_include_directories(modbee-sessions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/sessions)
target_compile_features(modbee-sessions PUBLIC cxx_std_17)
if(NOT MSVC)
    target_compile_options(modbee-sessions PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
endif()

add_library(modbee-events STATIC events/BrokerEvents.cpp events/SparkplugAware.cpp)
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
# Its CMake package (vcpkg, Windows) first; pkg-config (Linux packages) else.
find_package(RdKafka CONFIG QUIET)
# --- the kafka: section, changed live (no librdkafka: every build has it) ---
add_library(modbee-kafka-control STATIC kafka/KafkaControl.cpp)
target_link_libraries(modbee-kafka-control PUBLIC modbee-server modbee-config)
target_include_directories(modbee-kafka-control PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/kafka)
target_compile_features(modbee-kafka-control PUBLIC cxx_std_17)

if(RdKafka_FOUND)
    set(RDKAFKA_FOUND TRUE)
    set(RDKAFKA_VERSION ${RdKafka_VERSION})
    set(_modbee_rdkafka RdKafka::rdkafka)
else()
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(RDKAFKA QUIET IMPORTED_TARGET rdkafka)
    endif()
    set(_modbee_rdkafka PkgConfig::RDKAFKA)
endif()
if(RDKAFKA_FOUND)
    add_library(modbee-kafka STATIC kafka/KafkaPipe.cpp kafka/KafkaCommon.cpp kafka/KafkaEnvelope.cpp
        kafka/KafkaRetained.cpp)
    target_link_libraries(modbee-kafka PUBLIC modbee-server modbee-config modbee-retained modbee-kafka-control
                          ${_modbee_rdkafka})
    target_include_directories(modbee-kafka PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/kafka)
    target_compile_definitions(modbee-kafka PUBLIC MODBEE_HAVE_KAFKA=1)
    target_compile_features(modbee-kafka PUBLIC cxx_std_17)
    if(NOT MSVC)
        target_compile_options(modbee-kafka PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
    endif()
    message(STATUS "modbee: librdkafka ${RDKAFKA_VERSION} found - the Kafka pipe is built")
    set(MODBEE_KAFKA_LINKS TRUE)
else()
    message(STATUS "modbee: no librdkafka - modbee-mqtt serve runs without the Kafka pipe")
endif()

# --- what the Application API serves, without gRPC: logins decided by the
# application (Authority) and its messages (AppHub). Built - and tested -
# wherever the server is.
add_library(modbee-app STATIC api/Authority.cpp api/AppHub.cpp api/AppConnections.cpp api/AppAdmin.cpp
    api/GroupStore.cpp api/ConnectionOutbox.cpp)
target_link_libraries(modbee-app PUBLIC modbee-server modbee-auth)
target_include_directories(modbee-app PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/api)
target_compile_features(modbee-app PUBLIC cxx_std_17)
if(NOT MSVC)
    target_compile_options(modbee-app PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
endif()

# --- connections whose link is Kafka: a broker of their own, piped to Kafka ---
if(MODBEE_KAFKA_LINKS)
    add_library(modbee-kafka-link STATIC kafka/KafkaLinks.cpp)
    target_link_libraries(modbee-kafka-link PUBLIC modbee-kafka modbee-app)
    target_compile_features(modbee-kafka-link PUBLIC cxx_std_17)
    if(NOT MSVC)
        target_compile_options(modbee-kafka-link PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
    endif()
endif()

# --- the Application API (gRPC), where gRPC and protobuf are installed ---
#
# THE ONE PLACE gRPC IS BUILT, and the one target allowed exceptions and RTTI,
# which gRPC and protobuf need. The code is generated from proto/ at build
# time. Packages give it in seconds (Ubuntu: libgrpc++-dev
# protobuf-compiler-grpc libprotobuf-dev); written for gRPC 1.51 and later.
find_package(Protobuf QUIET)
find_package(gRPC CONFIG QUIET)
if(gRPC_FOUND AND Protobuf_FOUND)
    set(_modbee_api_default ON)
else()
    set(_modbee_api_default OFF)
endif()
option(MQTT_BUILD_APP_API "the Application API (gRPC): needs gRPC and protobuf" ${_modbee_api_default})
if(MQTT_BUILD_APP_API)
    if(NOT (gRPC_FOUND AND Protobuf_FOUND))
        message(FATAL_ERROR "MQTT_BUILD_APP_API needs gRPC and protobuf "
                            "(Ubuntu: libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev)")
    endif()
    set(_modbee_proto ${CMAKE_CURRENT_SOURCE_DIR}/../proto)
    set(_modbee_generated ${CMAKE_CURRENT_BINARY_DIR}/generated)
    set(_modbee_app ${_modbee_generated}/modbee/app/v1/app)
    add_custom_command(
        OUTPUT ${_modbee_app}.pb.cc ${_modbee_app}.pb.h ${_modbee_app}.grpc.pb.cc ${_modbee_app}.grpc.pb.h
        COMMAND ${CMAKE_COMMAND} -E make_directory ${_modbee_generated}
        COMMAND protobuf::protoc --proto_path=${_modbee_proto}
                --cpp_out=${_modbee_generated} --grpc_out=${_modbee_generated}
                --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
                modbee/app/v1/app.proto
        DEPENDS ${_modbee_proto}/modbee/app/v1/app.proto
        COMMENT "Generating the Application API from proto/modbee/app/v1/app.proto"
        VERBATIM)
    # Sparkplug B's own payload, for the API's codec (api/SparkplugCodec.cpp).
    set(_modbee_wire ${_modbee_generated}/modbee/sparkplug/v1/wire)
    add_custom_command(
        OUTPUT ${_modbee_wire}.pb.cc ${_modbee_wire}.pb.h
        COMMAND ${CMAKE_COMMAND} -E make_directory ${_modbee_generated}
        COMMAND protobuf::protoc --proto_path=${_modbee_proto} --cpp_out=${_modbee_generated}
                modbee/sparkplug/v1/wire.proto
        DEPENDS ${_modbee_proto}/modbee/sparkplug/v1/wire.proto
        COMMENT "Generating the Sparkplug B payload from proto/modbee/sparkplug/v1/wire.proto"
        VERBATIM)
    find_package(ZLIB REQUIRED)    # compressed Sparkplug payloads; gRPC needs it already
    add_library(modbee-api STATIC api/ApiServer.cpp api/SparkplugCodec.cpp ${_modbee_app}.pb.cc
        ${_modbee_app}.grpc.pb.cc ${_modbee_wire}.pb.cc)
    target_link_libraries(modbee-api PUBLIC modbee-app modbee-kafka-control gRPC::grpc++ protobuf::libprotobuf
                          ZLIB::ZLIB)
    target_include_directories(modbee-api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/api ${_modbee_generated})
    target_compile_definitions(modbee-api PUBLIC MODBEE_HAVE_APP_API=1)
    target_compile_features(modbee-api PUBLIC cxx_std_17)
    if(NOT MSVC)
        set_source_files_properties(api/ApiServer.cpp api/SparkplugCodec.cpp PROPERTIES COMPILE_OPTIONS "-Wall;-Wextra")
    endif()
    message(STATUS "modbee: gRPC ${gRPC_VERSION} found - the Application API is built")
else()
    message(STATUS "modbee: no gRPC - modbee-mqtt serve runs without the Application API")
endif()

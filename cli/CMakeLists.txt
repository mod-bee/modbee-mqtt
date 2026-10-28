# The `modbee-mqtt` command-line tool.
#
# A host tool, never part of the Arduino library: it lives outside src/, so the
# Arduino IDE never sees it. It may use C++17, as the tests do, and it builds
# with exceptions and RTTI off like everything else here (R8).

add_executable(modbee
    main.cpp
    Args.cpp
    Common.cpp
    cmd_pub.cpp
    cmd_sub.cpp
    cmd_req.cpp
    bench/Bench.cpp
    cmd_serve.cpp
    cmd_check.cpp
    cmd_admin.cpp
    cmd_service.cpp
    cmd_api.cpp
    KafkaCli.cpp
    # The JSON the admin topics speak - plain C++, so the admin commands work
    # from any computer, not only where the server tier builds.
    ${CMAKE_CURRENT_SOURCE_DIR}/../server/auth/Json.cpp
)
# The program is modbee-mqtt (modbee-mqtt.exe): a plain `modbee` would clash
# with any other ModBee program on the same machine. The target keeps its
# short name.
set_target_properties(modbee PROPERTIES OUTPUT_NAME modbee-mqtt)
target_link_libraries(modbee PRIVATE modbee-mqtt Threads::Threads)
if(WIN32)
    target_link_libraries(modbee PRIVATE advapi32)   # the service (cmd_service.cpp)
endif()
target_include_directories(modbee PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../server/auth)
# What `modbee-mqtt --version` says it is.
target_compile_definitions(modbee PRIVATE MODBEE_VERSION="${PROJECT_VERSION}")

if(NOT MSVC)
    target_compile_options(modbee PRIVATE -Wall -Wextra -fno-exceptions -fno-rtti)
endif()

# `modbee-mqtt serve` needs the server tier, which exists only where it builds.
if(TARGET modbee-server)
    target_link_libraries(modbee PRIVATE modbee-server modbee-config modbee-retained modbee-auth modbee-events modbee-sessions
                          modbee-kafka-control)
    target_compile_definitions(modbee PRIVATE MODBEE_HAVE_SERVER=1)
endif()

# The Kafka pipe, where librdkafka was found (server/CMakeLists.txt).
if(TARGET modbee-kafka)
    target_link_libraries(modbee PRIVATE modbee-kafka)
endif()

# The Application API, where gRPC was found (server/CMakeLists.txt).
if(TARGET modbee-api)
    target_link_libraries(modbee PRIVATE modbee-api)
    # Connections whose link is Kafka, where both are.
    if(TARGET modbee-kafka-link)
        target_link_libraries(modbee PRIVATE modbee-kafka-link)
    endif()
endif()

# `cmake --install build` puts it in <prefix>/bin - /usr/local/bin by default.
install(TARGETS modbee RUNTIME DESTINATION bin)

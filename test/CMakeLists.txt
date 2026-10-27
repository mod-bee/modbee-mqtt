# THE PUBLISHED TEST SUITE.
#
# Self-contained: a compiler and this repository, nothing else - no network
# beyond loopback, no Docker, no tools installed on somebody's machine. GitHub
# runs it on Linux, macOS and Windows (.github/workflows/ci.yml); `ctest`
# runs it anywhere. The development suite (internal/test/, not published) is
# where the sanitizers, the fuzzers, the soak and the interoperability rigs
# live.
#
# Listed explicitly, so adding one is a deliberate act.

set(MODBEE_TESTS codec_tests broker_tests)

foreach(name IN LISTS MODBEE_TESTS)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE modbee-mqtt)
    target_compile_features(${name} PRIVATE cxx_std_17)
    if(MSVC)
        target_compile_options(${name} PRIVATE /W4 /utf-8)
    else()
        target_compile_options(${name} PRIVATE -Wall -Wextra)
    endif()
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 300)
endforeach()

# The server tier, where it is built (Linux; Windows with wepoll).
if(TARGET modbee-server AND TARGET modbee-sessions AND TARGET modbee-retained)
    add_executable(server_tests server_tests.cpp)
    target_link_libraries(server_tests PRIVATE modbee-server modbee-sessions modbee-retained modbee-auth)
    target_compile_features(server_tests PRIVATE cxx_std_17)
    if(MSVC)
        target_compile_options(server_tests PRIVATE /W4 /utf-8)
    else()
        target_compile_options(server_tests PRIVATE -Wall -Wextra)
    endif()
    add_test(NAME server_tests COMMAND server_tests)
    set_tests_properties(server_tests PROPERTIES TIMEOUT 300)
endif()

# The Application API (gRPC), where it is built.
if(TARGET modbee-api AND TARGET modbee-retained)
    add_executable(api_tests api_tests.cpp)
    target_link_libraries(api_tests PRIVATE modbee-api modbee-retained modbee-events)
    target_compile_features(api_tests PRIVATE cxx_std_17)
    if(MSVC)
        target_compile_options(api_tests PRIVATE /W4 /utf-8)
    else()
        target_compile_options(api_tests PRIVATE -Wall -Wextra)
    endif()
    add_test(NAME api_tests COMMAND api_tests)
    set_tests_properties(api_tests PROPERTIES TIMEOUT 300)

    # The command line and the API, one to one (cli/ApiCommands.h, docs/cli.md).
    add_executable(cli_api_tests cli_api_tests.cpp)
    target_link_libraries(cli_api_tests PRIVATE modbee-api)
    target_compile_features(cli_api_tests PRIVATE cxx_std_17)
    target_compile_definitions(cli_api_tests PRIVATE MODBEE_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
    add_test(NAME cli_api_tests COMMAND cli_api_tests)
endif()

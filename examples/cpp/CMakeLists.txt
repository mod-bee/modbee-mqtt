# Listed explicitly, so adding one is a deliberate act.
#
# EVERY ONE OF THEM, ALWAYS. The broker and bridge examples used to be gated on
# MQTT_WITH_BROKER and MQTT_WITH_BRIDGE - options this library deleted, and
# which nothing has defined since. So the two of them had not been compiled by
# anything for as long as the flags had been gone, which is the same failure
# the flags were removed for: a thing nobody can reach is not a thing, it is a
# trap with documentation.
set(MQTT_EXAMPLES publish subscribe gateway broker bridge sparkplug_node sparkplug_host)

# The TLS one needs an engine. Not a flag - the same detection the library
# itself uses, asked here so a machine without one gets a smaller example set
# rather than a build failure.
include(CheckIncludeFileCXX)
check_include_file_cxx("wolfssl/ssl.h" MQTT_EXAMPLES_HAVE_WOLFSSL)
check_include_file_cxx("openssl/ssl.h" MQTT_EXAMPLES_HAVE_OPENSSL)
if(MQTT_EXAMPLES_HAVE_WOLFSSL OR MQTT_EXAMPLES_HAVE_OPENSSL)
    list(APPEND MQTT_EXAMPLES secure_broker)
else()
    message(STATUS "modbee-mqtt: no TLS engine - the secure_broker example is skipped")
endif()

foreach(example IN LISTS MQTT_EXAMPLES)
    add_executable(modbee_${example} ${example}/main.cpp)
    target_link_libraries(modbee_${example} PRIVATE modbee-mqtt)
    if(NOT MSVC)
        target_compile_options(modbee_${example} PRIVATE -Wall -Wextra)
    endif()
endforeach()

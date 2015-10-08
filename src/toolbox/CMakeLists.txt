cmake_minimum_required(VERSION 2.8)

# Make a project.
project(toolbox C CXX)

# Get CMake configured.
set(CMAKE_INCLUDE_CURRENT_DIR on)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(FeatureSummary)
include(CheckIncludeFile)
include(CMakeDependentOption)
include(${CMAKE_SOURCE_DIR}/cmake/Date.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerVersion.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerFlags.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/shared_name.cmake)

# Accept config options
option(WANT_RELEASE "Set options for package building, overriding all others" 0)
option(PROJECT_VERSION "Version string for build" "local")
CMAKE_DEPENDENT_OPTION(WANT_STATIC "Attempt to build and link statically" TRUE
                                    "NOT WANT_RELEASE" FALSE)


# Set preprocessor flags.
# TODO: This should be probably modified if we build on something different
# http://www.gnu.org/software/libc/manual/html_node/Feature-Test-Macros.html
# _REENTRANT - tells the std library to enable reentrant functions
# _GNU_SOURCE - uses GNU (not POSIX) functions
# _LARGEFILE64_SOURCE - this should probably be changed to _FILE_OFFSET_BITS
# TODO: Is this truely needed?
# LINUX=2 - APR seems to want it
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE")
set(CMAKE_C_FLAGS_DEBUG "-O0")

# Find dependencies.
find_package(OpenSSL REQUIRED)
find_package(APR-ACCRE REQUIRED)
find_package(APRUtil-ACCRE REQUIRED)
find_package(Zlib REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR} ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR})
SET(LIBS ${LIBS} ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} ${APR_LIBRARY} ${APRUTIL_LIBRARY})

list(APPEND LIBS ${ZLIB_LIBRARY} pthread m dl)
if(NOT APPLE)
    # OSX doesn't have/need librt
    list(APPEND LIBS rt)
endif(NOT APPLE)

# Detect compiler flags.
check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/toolbox_config.h.in ${CMAKE_SOURCE_DIR}/toolbox_config.h)

# Make the version file.
set(TOOLBOX_VERSION "toolbox: v${PROJECT_VERSION}")
site_name(BUILD_HOST)
Date(BUILD_DATE)
CompilerVersion(COMPILER_VERSION)
CompilerFlags(COMPILER_FLAGS)
configure_file("${PROJECT_SOURCE_DIR}/toolbox_version.c.in" "${PROJECT_SOURCE_DIR}/toolbox_version.c")

# common objects
SET(NETWORK_OBJS 
    net_1_ssl.c net_2_ssl.c net_fd.c net_phoebus.c net_sock.c network.c
    phoebus.c 
)

SET(TOOL_OBJS  
    append_printf.c apr_wrapper.c atomic_counter.c chksum.c dns_cache.c
    iniparse.c interval_skiplist.c log.c packer.c pigeon_coop.c pigeon_hole.c
    random.c skiplist.c stack.c string_token.c transfer_buffer.c varint.c
)

SET(TOOLBOX_OBJS ${TOOL_OBJS} ${NETWORK_OBJS})

# various executables
ADD_EXECUTABLE(sl_test sl_test.c)
ADD_EXECUTABLE(isl_test isl_test.c)
ADD_EXECUTABLE(varint_test varint_test.c)

ADD_LIBRARY(toolbox SHARED ${TOOLBOX_OBJS})
SET_TARGET_PROPERTIES(toolbox PROPERTIES CLEAN_DIRECT_OUTPUT 1)
if(WANT_STATIC)
    message(STATUS "Building a static library")
    ADD_LIBRARY(toolbox-static STATIC ${TOOLBOX_OBJS})
    SET_TARGET_PROPERTIES(toolbox-static PROPERTIES OUTPUT_NAME "toolbox")
    SET_TARGET_PROPERTIES(toolbox-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    SET(toolbox_lib toolbox-static)
else()
    message(STATUS "NOT building a static library")
    SET(toolbox_lib toolbox)
endif(WANT_STATIC)

TARGET_LINK_LIBRARIES(sl_test  ${toolbox_lib} ${LIBS})
TARGET_LINK_LIBRARIES(isl_test  ${toolbox_lib} ${LIBS})
TARGET_LINK_LIBRARIES(varint_test ${toolbox_lib} ${LIBS})

set(sodeps)
set(lpdeps)
foreach(lib ${LIBS})
    shared_name(${lib} soname)
    set(sodeps ${sodeps} ${soname})

    get_filename_component(ldir ${lib} PATH)
    if (ldir)
        set(lpdeps ${lpdeps} "-L${ldir}")
    endif()
endforeach()

TARGET_LINK_LIBRARIES(toolbox LINK_PUBLIC ${lpdeps} ${sodeps})

install(TARGETS toolbox DESTINATION lib)
install(FILES   apr_wrapper.h debug.h list.h net_2_ssl.h fmttypes.h
                interval_skiplist.h net_1_ssl.h net_phoebus.h net_fd.h
                network.h pigeon_hole.h stack.h net_sock.h pigeon_coop.h
                skiplist.h toolbox_config.h phoebus.h random.h string_token.h
                type_malloc.h transfer_buffer.h packer.h append_printf.h
                chksum.h varint.h atomic_counter.h dns_cache.h iniparse.h log.h
        DESTINATION include/toolbox 
        COMPONENT devel)

if(WANT_STATIC)
    install(TARGETS toolbox-static DESTINATION lib COMPONENT devel)
endif(WANT_STATIC)
# Below is used for building packages
set(CPACK_PACKAGE_NAME "accre-toolbox")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")

# Component configuration - currently broken. :(
set(CPACK_RPM_COMPONENT_INSTALL OFF)
set(CPACK_DEB_COMPONENT_INSTALL OFF)
set(CPACK_ARCHIVE_COMPONENT_INSTALL OFF)

# Generator specific config
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
include(CPack)

# Give the summary
feature_summary(WHAT ALL)

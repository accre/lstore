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
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

# Find and link universal deps
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

# Make the version file.
set(LSTORE_INC_VERSION_STRING "${LSTORE_PROJECT_NAME}: ${PROJECT_VERSION}")
site_name(BUILD_HOST)
Date(BUILD_DATE)
CompilerVersion(COMPILER_VERSION)
CompilerFlags(COMPILER_FLAGS)
configure_file(${CMAKE_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_version.c.in
               ${CMAKE_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_version.c)

add_library(library SHARED ${LSTORE_PROJECT_OBJS})
set_target_properties(library PROPERTIES OUTPUT_NAME "${LSTORE_PROJECT_NAME}")
set_target_properties(library PROPERTIES CLEAN_DIRECT_OUTPUT 1)
if(WANT_STATIC)
    message(STATUS "Building a static library")
    add_library(library-static STATIC ${LSTORE_PROJECT_OBJS})
    set_target_properties(library-static PROPERTIES OUTPUT_NAME "${LSTORE_PROJECT_NAME}")
    set_target_properties(library-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    SET(library_lib library-static)
else()
    message(STATUS "NOT building a static library")
    SET(library_lib library)
endif(WANT_STATIC)

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

target_link_libraries(library LINK_PUBLIC ${lpdeps} ${sodeps})

install(TARGETS library DESTINATION lib)
install(FILES ${LSTORE_PROJECT_INCLUDES} DESTINATION include/${LSTORE_PROJECT_NAME}
        COMPONENT devel)
if(WANT_STATIC)
    install(TARGETS library-static DESTINATION lib COMPONENT devel)
endif(WANT_STATIC)

# Below is used for building packages
set(CPACK_PACKAGE_NAME "accre-${LSTORE_PROJECT_NAME}")
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

# Function to try and find static versions of shared libraries
macro(static_name full_name static)
    get_filename_component(lname ${full_name} NAME_WE)
    get_filename_component(path_name ${full_name} PATH)
    if(WIN32)
        string(SUBSTRING ${lname} 3 -1 bname)
        set(file_name "${bname}.a")
    elseif(APPLE)
        set(file_name "${lname}.a")
    elseif(UNIX)
        set(file_name "${lname}.a")
    else()
        message(SEND_ERROR "Don't know how to mangle the static library name for platform")
        set(file_name "ERROR")
    endif()
    if(EXISTS "${path_name}/${file_name}")
        set(${static} "${path_name}/${file_name}")
    else()
        set(${static} ${full_name})
    endif()
endmacro(static_name)

include(FeatureSummary)
include(CheckIncludeFile)
include(CMakeDependentOption)
include(${CMAKE_SOURCE_DIR}/cmake/Date.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerVersion.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerFlags.cmake)

# Accept config options
set(LSTORE_PROJECT_VERSION "local" CACHE STRING "Version string for build")
set(LSTORE_PROJECT_REVISION "1" CACHE STRING "Revision number for packaging")
option(WANT_PACKAGE "Set options for package building, overriding all others"
                    OFF)
cmake_dependent_option(WANT_STATIC "Attempt to build and link statically" TRUE
                                   "NOT WANT_PACKAGE" FALSE)
cmake_dependent_option(WANT_DEBUG "Build with debug flags" TRUE
                                   "NOT WANT_PACKAGE" FALSE)
if(WANT_DEBUG)
    set(CMAKE_BUILD_TYPE "Debug")
else()
    set(CMAKE_BUILD_TYPE "Release")
endif(WANT_DEBUG)

# Set preprocessor flags. TODO: This should be moved to config.h
# TODO: This should be probably modified if we build on something different
# http://www.gnu.org/software/libc/manual/html_node/Feature-Test-Macros.html
# _REENTRANT - tells the std library to enable reentrant functions
# _GNU_SOURCE - uses GNU (not POSIX) functions
# _LARGEFILE64_SOURCE - Along with...
# _FILE_OFFSET_BITS - Tell gcc that we want 64 bit offsets
# TODO: Is this truely needed?
# LINUX=2 - APR seems to want it

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

# Find and link universal deps
find_package(OpenSSL REQUIRED)
find_package(APR-ACCRE REQUIRED)
find_package(APRUtil-ACCRE REQUIRED)
find_package(ZLIB REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR} ${APR_INCLUDE_DIR}
                    ${APRUTIL_INCLUDE_DIR})
list(APPEND LIBS ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} ${APR_LIBRARY}
                 ${APRUTIL_LIBRARY} ${ZLIB_LIBRARY} pthread m dl)
if(NOT APPLE)
    # OSX doesn't have/need librt
    list(APPEND LIBS rt)
endif(NOT APPLE)

# Make the version file.
set(LSTORE_INC_VERSION_STRING "${LSTORE_PROJECT_NAME}: ${LSTORE_PROJECT_VERSION}")
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

set(STATIC_LIBS)
foreach(lib ${LIBS})
    static_name(${lib} soname)
    list(APPEND STATIC_LIBS "${soname}")
endforeach()

target_link_libraries(library LINK_PUBLIC ${LIBS})
install(TARGETS library DESTINATION lib)
install(FILES ${LSTORE_PROJECT_INCLUDES} DESTINATION include/${LSTORE_PROJECT_NAME}
        COMPONENT devel)
if(WANT_STATIC)
    install(TARGETS library-static DESTINATION lib COMPONENT devel)
endif(WANT_STATIC)

#Add the exe build rules
foreach(f ${LSTORE_PROJECT_EXECUTABLES})
    add_executable(${f} ${f}.c)
    set_target_properties(${f} PROPERTIES LINKER_LANGUAGE CXX)
    if(WANT_STATIC)
        target_link_libraries(${f} ${library_lib} ${STATIC_LIBS})
    else()
        target_link_libraries(${f} ${library_lib})
    endif(WANT_STATIC)
endforeach(f)

# Below is used for building packages
set(CPACK_PACKAGE_NAME "accre-${LSTORE_PROJECT_NAME}")
set(CPACK_PACKAGE_VERSION "${LSTORE_PROJECT_VERSION}")
set(CPACK_GENERATOR "RPM;DEB")
set(CPACK_SOURCE_GENERATOR "RPM;DEB")
set(CPACK_PACKAGE_RELEASE ${LSTORE_PROJECT_REVISION})
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.source")

# Component configuration - currently broken. :(
set(CPACK_RPM_COMPONENT_INSTALL OFF)
set(CPACK_DEB_COMPONENT_INSTALL OFF)
set(CPACK_ARCHIVE_COMPONENT_INSTALL OFF)

# Generator specific config
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

if(WANT_PACKAGE)
    include(CPack)
endif(WANT_PACKAGE)

# Give the summary
feature_summary(WHAT ALL)

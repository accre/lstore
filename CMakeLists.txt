# Root CMake file for LStore
cmake_minimum_required(VERSION 2.8)
project(LStore C)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if(NOT LSTORE_PROJECT_VERSION)
    set(LSTORE_PROJECT_VERSION "0.5.1-unknown_commit")
endif()

# Options
option(BUILD_SHARED_LIBS "Build shared LStore libraries" ON)
option(BUILD_TESTS "Build tests" ON)
option(BUILD_APR "Build APR from source" ON)
option(BUILD_APU "Build APR-Util from source" ON)
option(BUILD_JERASURE "Build Jerasure from source" ON)

option(ENABLE_COVERAGE "Enable code coverage" OFF)
option(ENABLE_ASAN "Enable address sanitizer" OFF)
option(ENABLE_MSAN "Enable memory sanitizer" OFF)
option(ENABLE_TSAN "Enable thread sanitizer" OFF)
option(ENABLE_UBSAN "Enable undefined behaviour sanitizer" OFF)
option(ENABLE_FUZZ "Build with AFL fuzz support" OFF)

option(INSTALL_EXTERNALS "Install externals to \$CMAKE_INSTALL_PATH" OFF)
option(INSTALL_YUM_RELEASE "Install yum lstore-release (may require root)" OFF)
option(INSTALL_DEB_RELEASE "Install deb lstore-release (may require root)" OFF)
option(INSTALL_META "Install meta-package (may require root)" OFF)
option(INSTALL_TESTS "Install test binaries" OFF)

set(LSTORE_REPO_URL "http://repo.accre.vanderbilt.edu"
        CACHE STRING "URL for lstore-release")
set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/local"
        CACHE STRING "Installation path")
# Defines
set(USE_SUPERBUILD ON)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fvisibility=hidden")
set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_C_FLAGS_DEBUG "-O0 -g3 -Wall -Wextra -Wno-unused-parameter -Wno-deprecated-declarations -Werror=format")
set(CMAKE_MACOSX_RPATH ON)
if(INSTALL_EXTERNALS)
    set(EXTERNAL_INSTALL_DIR ${CMAKE_INSTALL_PREFIX})
else()
    set(EXTERNAL_INSTALL_DIR ${CMAKE_BINARY_DIR})
endif()

if(ENABLE_ASAN)
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer -O1")
endif()
if(ENABLE_MSAN)
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fsanitize=memory -fno-omit-frame-pointer -fno-optimize-sibling-calls -O1")
endif()
if(ENABLE_TSAN)
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fsanitize=thread -fno-omit-frame-pointer -pie")
endif()
if(ENABLE_UBSAN)
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fsanitize=undefined -fno-omit-frame-pointer")
endif()
if(ENABLE_COVERAGE)
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage")
endif()
if(ENABLE_FUZZ)
    if("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
        set(FUZZ_COMPILE_NAME "afl-gcc")
    elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
        set(FUZZ_COMPILE_NAME "afl-clang")
    else()
        message(FATAL "Could not find AFL, but ENABLE_FUZZ is set. Please "
                "install AFL from http://lcamtuf.coredump.cx/afl/")
    endif()
    find_program(CMAKE_C_COMPILER ${FUZZ_COMPILE_NAME})
endif()

# Use gnu-style paths
include(GNUInstallDirs)

# Find external deps we don't build
find_package(OpenSSL REQUIRED)
find_package(FUSE REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ZMQ REQUIRED)

# Find external deps we might build
find_package(APR-ACCRE)
find_package(APRUtil-ACCRE)
find_package(CZMQ)
find_package(Jerasure)

# Build external dependencies
set(REBUILD_DEPENDENCIES)
include(LStoreExternals)

add_custom_target(externals COMMENT "Target to build all externals")
if(REBUILD_DEPENDENCIES)
    message(STATUS "External dependencies exist")
    add_dependencies(externals ${REBUILD_DEPENDENCIES})
    include_directories("${EXTERNAL_INSTALL_DIR}/include")
    link_directories("${EXTERNAL_INSTALL_DIR}/lib")
endif()

# Pull in LStore modules
add_subdirectory(src/toolbox)
add_subdirectory(src/gop)
add_subdirectory(src/ibp)
add_subdirectory(src/lio)
if(INSTALL_YUM_RELEASE)
    add_subdirectory(src/release/rpm-release)
endif()
if(INSTALL_DEB_RELEASE)
    add_subdirectory(src/release/deb-release)
endif()
if(INSTALL_META)
    add_subdirectory(src/meta)
endif()

# Enable testing
if(BUILD_TESTS)
    # TODO: add test files with file(GLOB xx test/test-*.c)
    add_executable(run-tests test/run-tests.c
                             test/runner.c
                             test/runner-unix.c
                             test/test-harness.c
                             test/test-tb-stack.c)
    target_link_libraries(run-tests pthread lio)
    add_executable(run-benchmarks test/run-benchmarks.c
                             test/runner.c
                             test/runner-unix.c
                             test/benchmark-sizes.c)
    target_link_libraries(run-benchmarks pthread lio)
    target_include_directories(run-benchmarks SYSTEM PRIVATE ${APR_INCLUDE_DIR})
    if(INSTALL_TESTS)
        install(TARGETS run-tests run-benchmarks
                DESTINATION ${CMAKE_INSTALL_BINDIR}
                COMPONENT tests)
    endif()
endif()

# Import packaging
include(LStorePackaging)

# Summary
include(FeatureSummary)
feature_summary(WHAT ALL)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

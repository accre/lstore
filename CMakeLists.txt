# Root CMake file for LStore
cmake_minimum_required(VERSION 2.8)
project(LStore C)

set(LSTORE_PROJECT_VERSION "0.5.1-unknown_commit" CACHE STRING "Project version <major>.<minor>.<patch>[-<git-hash>[-<other-tags>]]")

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

if(NOT CMAKE_BUILD_TYPE)
    # Has to be handled differently :(
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build type" FORCE)
endif()

set(LSTORE_REPO_URL "http://repo.accre.vanderbilt.edu"
        CACHE STRING "URL for lstore-release")
set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/local"
        CACHE STRING "Installation path")
# Defines
set(USE_SUPERBUILD ON)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fvisibility=hidden -Werror=implicit-function-declaration")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra -Wno-unused-parameter -Wno-deprecated-declarations -Werror=format -Werror=implicit-function-declaration")
set(CMAKE_MACOSX_RPATH ON)
if(INSTALL_EXTERNALS)
    set(EXTERNAL_INSTALL_DIR ${CMAKE_INSTALL_PREFIX})
else()
    set(EXTERNAL_INSTALL_DIR ${CMAKE_BINARY_DIR})
endif()

if(ENABLE_ASAN)
    set(LSTORE_SANITIZE_FLAGS "${LSTORE_SANITIZE_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
    set(LSTORE_SANITIZE_LINK_FLAGS "${LSTORE_SANITIZE_LINK_FLAGS} -fsanitize=address")
endif()
if(ENABLE_MSAN)
    set(LSTORE_SANITIZE_FLAGS "${LSTORE_SANITIZE_FLAGS} -fsanitize=memory -fno-omit-frame-pointer -fno-optimize-sibling-calls -O1")
    set(LSTORE_SANITIZE_LINK_FLAGS "${LSTORE_SANITIZE_LINK_FLAGS} -fsanitize=memory")
endif()
if(ENABLE_TSAN)
    set(LSTORE_SANITIZE_FLAGS "${LSTORE_SANITIZE_FLAGS} -fsanitize=thread -fno-omit-frame-pointer -pie")
    set(LSTORE_SANITIZE_LINK_FLAGS "${LSTORE_SANITIZE_LINK_FLAGS} -fsanitize=thread")
endif()
if(ENABLE_UBSAN)
    set(LSTORE_SANITIZE_FLAGS "${LSTORE_SANITIZE_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer")
    set(LSTORE_SANITIZE_LINK_FLAGS "${LSTORE_SANITIZE_LINK_FLAGS} -fsanitize=undefined")
endif()
if(ENABLE_COVERAGE)
    set(LSTORE_SANITIZE_FLAGS "${LSTORE_SANITIZE_FLAGS} -fprofile-arcs -ftest-coverage")
    add_custom_target(coverage
        COMMAND lcov -q --zerocounters --directory .
        COMMAND lcov -q --initial --capture --directory . --output-file lstore.base
        COMMAND ./run-tests
        COMMAND lcov -q --capture --directory . --output-file lstore.unit --rc lcov_branch_coverage=1
        COMMAND lcov -q --add lstore.base --add lstore.unit --output-file lstore.total --rc lcov_branch_coverage=1
        COMMAND genhtml -q -o coverage-html --branch-coverage --function-coverage lstore.total --rc lcov_branch_coverage=1
        DEPENDS run-tests
        COMMENT "Executes code coverage tests")
endif()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${LSTORE_SANITIZE_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LSTORE_SANITIZE_LINK_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${LSTORE_SANITIZE_LINK_FLAGS}")

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

# Detect compiler flags
include(CheckCCompilerFlag)
check_c_compiler_flag(-Werror=incompatible-pointer-types HAS_INCOMPATIBLE_POINTER_WERROR)
if(HAS_INCOMPATIBLE_POINTER_WERROR)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror=incompatible-pointer-types")
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
    SET_TARGET_PROPERTIES(run-benchmarks PROPERTIES
                            COMPILE_FLAGS "-DLSTORE_HACK_EXPORT")
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

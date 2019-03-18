# Root CMake file for LStore
cmake_minimum_required(VERSION 2.8)

# Preliminary configuration. Need to find ccache to set the defaults for the
# corresponding option
find_program(FOUND_CCACHE ccache)
if(FOUND_CCACHE)
    set(ENABLE_CCACHE_DEFAULT ON)
else()
    set(ENABLE_CCACHE_DEFAULT OFF)
endif()

# Options
option(BUILD_SHARED_LIBS "Build shared LStore libraries" ON)
option(BUILD_SHARED_EXES "Build shared LStore executables" ON)
option(BUILD_TESTS "Build tests" ON)
option(BUILD_JERASURE "Build Jerasure from source" ON)
option(BUILD_GRIDFTP "Build GridFTP bindings" OFF)
option(BUILD_TCMU "Build TCMU-Runner bindings" OFF)
option(BUILD_HDFS "Build HDFS bindings" OFF)

option(ENABLE_COVERAGE "Enable code coverage" OFF)
option(ENABLE_ASAN "Enable address sanitizer" OFF)
option(ENABLE_MSAN "Enable memory sanitizer" OFF)
option(ENABLE_TSAN "Enable thread sanitizer" OFF)
option(ENABLE_UBSAN "Enable undefined behaviour sanitizer" OFF)
option(ENABLE_FUZZ "Build with AFL fuzz support" OFF)
option(ENABLE_CCACHE "Build with ccache" ${ENABLE_CCACHE_DEFAULT})
option(ENABLE_LTO "Enable Link-Time Optimization (LTO)" OFF)

option(INSTALL_EXTERNALS "Install externals to \$CMAKE_INSTALL_PATH" OFF)
option(INSTALL_YUM_RELEASE "Install yum lstore-release (may require root)" OFF)
option(INSTALL_DEB_RELEASE "Install deb lstore-release (may require root)" OFF)
option(INSTALL_META "Install meta-package (may require root)" OFF)
option(INSTALL_TESTS "Install test binaries" OFF)

set(LSTORE_VERSION "" CACHE STRING "Override LStore version")

if(NOT CMAKE_BUILD_TYPE)
    # Has to be handled differently :(
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build type" FORCE)
endif()

if(NOT BUILD_SHARED_EXES)
    IF(WIN32)
        SET(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
    ELSE(WIN32)
        SET(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
    ENDIF(WIN32)
endif()

set(LSTORE_REPO_URL "http://repo.accre.vanderbilt.edu"
        CACHE STRING "URL for lstore-release")
set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/local"
        CACHE STRING "Installation path")
if(ENABLE_FUZZ AND ENABLE_CCACHE)
    message(FATAL_MESSAGE "Don't have proper support for nested wrappers yet")
endif()

if(ENABLE_FUZZ)
    if(LINUX AND NOT OSX)
        set(FUZZ_COMPILER_NAME "afl-gcc" CACHE STRING "AFL Wrapper")
    elseif(OSX)
        set(FUZZ_COMPILER_NAME "afl-clang" CACHE STRING "AFL Wrapper")
    endif()
    if(CMAKE_C_COMPILER)
        set(ENV{AFL_CC} "${CMAKE_C_COMPILER}")
    endif()
    if(DEFINED ENV{CC})
        set(ENV{AFL_CC} "$ENV{CC}")
    endif()
    find_program(AFL_PATH ${FUZZ_COMPILE_NAME} afl-gcc afl-clang)
    if(NOT AFL_PATH)
        message(FATAL_ERROR "Could not find AFL, but ENABLE_FUZZ is set. "
                    "Please install AFL from http://lcamtuf.coredump.cx/afl/")
    endif()
    set(CMAKE_C_COMPILER "${AFL_PATH}")
endif()

if(ENABLE_CCACHE)
    message(STATUS "Enabling ccache")
    set(LSTORE_COMPILER_WRAPPER "ccache ")
    set(LSTORE_C_COMPILER_WRAPPER_CMAKE "-DCMAKE_C_COMPILER=ccache")
    if(CMAKE_C_COMPILER)
        list(APPEND LSTORE_C_COMPILER_WRAPPER_CMAKE
                    "-DCMAKE_C_COMPILER_ARG1=${CMAKE_C_COMPILER}")
    else()
        list(APPEND LSTORE_C_COMPILER_WRAPPER_CMAKE
                    "-DCMAKE_C_COMPILER_ARG1=cc")
    endif()
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
else()
    set(LSTORE_COMPILER_WRAPPER "")
    if(CMAKE_C_COMPILER)
        set(LSTORE_C_COMPILER_WRAPPER_CMAKE "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
    endif()
endif()

# Now that we know what compiler we're using, we can "start" the project. If
# you do it in the other order, cmake complains and reruns the CMakeLists.txt
# compilation.
project(LStore C)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(LStoreVersion)

# Use gnu-style paths
include(GNUInstallDirs)

# Setting RPATH lets us have relocatable tarballs
set(CMAKE_INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")

# Defines
set(USE_SUPERBUILD ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fvisibility=hidden -fno-strict-aliasing -Wall -Wextra -Werror -Wno-unused-parameter -Wno-deprecated-declarations -Wno-error=sign-compare")
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wl,--exclude-libs,ALL")
endif()

# Handle enabling LTO
if(ENABLE_LTO)
    set(LTO_FLAGS "-flto")
    if(CMAKE_COMPILER_IS_GNUC OR CMAKE_COMPILER_IS_GNUCC)
        message(STATUS "Overriding nm/ar/ranlib to be gcc-prefixed versions")
        set(LTO_FLAGS "${LTO_FLAGS} -ffunction-sections -fdata-sections -Wl,--gc-sections")
        if(NOT CMAKE_C_COMPILER_VERSION VERSION_LESS 4.7)
            set(LTO_FLAGS "${LTO_FLAGS} -fno-fat-lto-objects")
        endif()
        set(CMAKE_NM "gcc-nm")
        set(CMAKE_AR "gcc-ar")
        set(CMAKE_RANLIB "gcc-ranlib")
    endif()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${LTO_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LTO_FLAGS} -fwhole-program")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${LTO_FLAGS} -fwhole-program")
endif()


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
    set(LSTORE_SANITIZE_FLAGS "${LSTORE_SANITIZE_FLAGS} -fsanitize=thread -fno-omit-frame-pointer")
    set(LSTORE_SANITIZE_LINK_FLAGS "${LSTORE_SANITIZE_LINK_FLAGS} -fsanitize=thread")
    if("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
        set(LSTORE_SANITIZE_FLAGS "${LSTORE_SANITIZE_FLAGS} -fPIE -pie")
        set(LSTORE_SANITIZE_LINK_FLAGS "${LSTORE_SANITIZE_LINK_FLAGS} -fPIE -pie")
    endif()
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

# Detect compiler flags
include(CheckCCompilerFlag)
# For some reason, the test for Wno-unused-result will silently fail, but
# the same test for Wunused-result won't. If one is defined, both will be
# defined, so test for one but use the other
set(LSTORE_FLAG_UNUSED_RESULT "")
check_c_compiler_flag("-Werror=unused-result" HAS_UNUSED_RESULT)
if(HAS_UNUSED_RESULT)
    set(LSTORE_FLAG_UNUSED_RESULT "-Wno-error=unused-result")
endif()

check_c_compiler_flag("-Wno-error=maybe-uninitialized" HAS_MAYBE_UNINITIALIZED)

check_c_compiler_flag("-Wparentheses-equality" HAS_PARENTHESES_EQUALITY)

if(HAS_MAYBE_UNINITIALIZED)
    set(LSTORE_FLAG_SUPPRESS_MAYBE_UNINITIALIZED "-Wno-error=maybe-uninitialized")
endif()

# Check for a couple of compiler flags
check_c_compiler_flag("-Wimplicit-fallthrough=4" HAS_IMPLICIT_FALLTHROUGH)
if(HAS_IMPLICIT_FALLTHROUGH)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wimplicit-fallthrough=4")
endif()
check_c_compiler_flag("-Wno-format-truncation" HAS_NO_FORMAT_TRUNCATION)
if(HAS_NO_FORMAT_TRUNCATION)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-format-truncation")
endif()

# When compiling release modes, asserts disappear which cause some variables
# to be unused. Suppress errors in this case
set(LSTORE_CHEAT_FLAGS "-Wno-error=unused-variable -Wno-error=array-bounds ${LSTORE_FLAG_SUPPRESS_MAYBE_UNINITIALIZED} ${LSTORE_FLAG_UNUSED_RESULT}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${LSTORE_CHEAT_FLAGS}")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${LSTORE_CHEAT_FLAGS}")


# Find external deps we don't build
find_package(OpenSSL REQUIRED)
find_package(FUSE3 REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ZMQ REQUIRED)
find_package(LevelDB REQUIRED)
find_package(APR REQUIRED)
find_package(APRUtil REQUIRED)
find_package(BerkeleyDB REQUIRED)

# Find external deps we might build
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
add_subdirectory(src/ibp-server)
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
    add_executable(varint_test test/varint_test.c)
    target_link_libraries(varint_test pthread toolbox)
    add_executable(test_que test/test_que.c)
    target_link_libraries(test_que pthread toolbox)
    target_include_directories(test_que PRIVATE ${APR_INCLUDE_DIR})
    add_executable(skiplist_test test/skiplist_test.c)
    target_link_libraries(skiplist_test pthread toolbox)
    target_include_directories(skiplist_test PRIVATE ${APR_INCLUDE_DIR})
    add_executable(interval_skiplist_test test/interval_skiplist_test.c)
    target_link_libraries(interval_skiplist_test pthread toolbox)
    target_include_directories(interval_skiplist_test PRIVATE ${APR_INCLUDE_DIR})
    add_executable(mq_test test/mq_test.c)
    target_link_libraries(mq_test pthread toolbox gop)
    target_include_directories(mq_test PRIVATE ${APR_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})
    add_executable(mqs_test test/mqs_test.c)
    target_link_libraries(mqs_test pthread toolbox gop)
    target_include_directories(mqs_test PRIVATE ${APR_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})
    add_executable(ibp_test test/ibp_test.c test/ibp_sync.c test/ibp_errno.c)
    target_link_libraries(ibp_test pthread toolbox gop ibp)
    target_include_directories(ibp_test PRIVATE ${APR_INCLUDE_DIR})
    add_executable(inip_sanity_check test/inip_sanity_check.c)
    target_link_libraries(inip_sanity_check pthread toolbox)
    target_include_directories(inip_sanity_check PRIVATE ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR})
    add_executable(os_test test/os_test.c)
    target_link_libraries(os_test pthread toolbox gop lio)
    target_include_directories(os_test PRIVATE ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})
    add_executable(rw_test test/rw_test.c)
    target_link_libraries(rw_test pthread toolbox gop lio)
    target_include_directories(rw_test PRIVATE ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})


    add_executable(run-tests test/run-tests.c
                             test/runner.c
                             test/runner-unix.c
                             test/test-harness.c
                             test/test-tb-iniparse.c
                             test/test-tb-object.c
                             test/test-tb-ref.c
                             test/test-tb-stk.c
                             test/test-tb-stack.c)
    target_link_libraries(run-tests pthread lio)
    target_include_directories(run-tests PRIVATE ${APR_INCLUDE_DIR})
    add_executable(run-benchmarks test/run-benchmarks.c
                             test/runner.c
                             test/runner-unix.c
                             test/benchmark-sizes.c)
    target_link_libraries(run-benchmarks pthread lio)
    target_include_directories(run-benchmarks PRIVATE ${APR_INCLUDE_DIR})
    SET_TARGET_PROPERTIES(run-benchmarks PROPERTIES
                            COMPILE_FLAGS "-DLSTORE_HACK_EXPORT")
    add_executable(fuzz-config test/fuzz-config.c)
    target_include_directories(fuzz-config PRIVATE ${APR_INCLUDE_DIR})
    target_link_libraries(fuzz-config toolbox)
    if(INSTALL_TESTS)
        install(TARGETS run-tests run-benchmarks fuzz-config
                DESTINATION ${CMAKE_INSTALL_BINDIR}
                COMPONENT tests)
    endif()
endif()

# Build documentation
find_program(DOXYGEN_PATH doxygen
             DOC "Path to doxygen executable")
add_custom_target(docs-genapi WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/build/"
                    COMMAND "${CMAKE_SOURCE_DIR}/scripts/dump-api.sh"
                    COMMENT "Autogenerate API documentation")
add_custom_target(docs COMMENT "Build documentation")
add_custom_target(doxygen COMMAND "${DOXYGEN_PATH}"
                    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/doc/"
                    COMMENT "Document code with doxygen")
add_custom_target(sphinx-html COMMAND $(MAKE) "html"
                    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/doc/"
                    COMMENT "Produce sphinx documentation (HTML)")
add_custom_target(sphinx-man COMMAND $(MAKE) "man"
                    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/doc/"
                    COMMENT "Produce sphinx documentation (Man Pages)")
add_dependencies(sphinx-html doxygen docs-genapi)
add_dependencies(sphinx-man doxygen docs-genapi)
add_dependencies(docs sphinx-html sphinx-man)

# Optional packages
if(BUILD_GRIDFTP)
    add_subdirectory(binding/gridftp)
endif()
if(BUILD_TCMU)
    add_subdirectory(binding/tcmu)
endif()
if(BUILD_HDFS)
    add_subdirectory(binding/hdfs)
endif()

# Import packaging
include(LStorePackaging)

# Summary
include(FeatureSummary)
feature_summary(WHAT ALL)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

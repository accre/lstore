# Root CMake file for LStore
project(LStore CXX C)
cmake_minimum_required(VERSION 2.8)

# Build external dependencies
include(ExternalProject)
ExternalProject_add(apr-accre
        SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/apr-accre/"
        BINARY_DIR "${CMAKE_BINARY_DIR}/apr-accre/"
        CONFIGURE_COMMAND "${CMAKE_SOURCE_DIR}/vendor/apr-accre/configure"
                            "--prefix=${CMAKE_INSTALL_PREFIX}/"
                            "--includedir=${CMAKE_INSTALL_PREFIX}/include/apr-ACCRE-1"
                            "--with-installbuilddir=${CMAKE_INSTALL_PREFIX}/lib/apr-ACCRE-1/build"
        BUILD_COMMAND "make"
        TEST_COMMAND "make" "test"
        INSTALL_COMMAND "make" "install"
        TEST_EXCLUDE_FROM_MAIN 1
    )
ExternalProject_add(apr-util-accre
        DEPENDS apr-accre
        SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/apr-util-accre/"
        BINARY_DIR "${CMAKE_BINARY_DIR}/apr-util-accre/"
        CONFIGURE_COMMAND "${CMAKE_SOURCE_DIR}/vendor/apr-util-accre/configure"
                            "--with-apr=${CMAKE_INSTALL_PREFIX}/bin/apr-ACCRE-1-config"
                            "--prefix=${CMAKE_INSTALL_PREFIX}/"
                            "--includedir=${CMAKE_INSTALL_PREFIX}/include/apr-util-ACCRE-1"
                            "--with-installbuilddir=${CMAKE_INSTALL_PREFIX}/lib/apr-util-ACCRE-1/build"
        BUILD_COMMAND "make"
        TEST_COMMAND "make" "test"
        INSTALL_COMMAND "make" "install"
        TEST_EXCLUDE_FROM_MAIN 1
    )
ExternalProject_add(jerasure
        SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/jerasure/"
        BINARY_DIR "${CMAKE_BINARY_DIR}/jerasure/"
        CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_INSTALL_PREFIX} -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
        TEST_COMMAND ""
        INSTALL_COMMAND "make" "install"
    )
ExternalProject_add(czmq
        SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/czmq/"
        BINARY_DIR "${CMAKE_BINARY_DIR}/czmq/"
        CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_INSTALL_PREFIX} -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
        TEST_EXCLUDE_FROM_MAIN 1
        TEST_COMMAND "make" "test"
        INSTALL_COMMAND "make" "install"
)

# Pull in LStore modules
#add_subdirectory(src/toolbox)
#add_subdirectory(src/gop)
#add_subdirectory(src/ibp)
#add_subdirectory(src/lio)

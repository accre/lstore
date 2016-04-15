# Root CMake file for LStore
cmake_minimum_required(VERSION 2.8)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
# Options
# TODO: https://github.com/Sarcasm/cmake-superbuild
option(USE_SUPERBUILD "Trigger building one giant CMake project" ON)
option(ENABLE_ASAN "Build with address sanitizer" OFF)
option(ENABLE_MSAN "Build with memory sanitizer" OFF)
option(ENABLE_TSAN "Build with thread sanitizer" OFF)
option(ENABLE_UBSAN "Build with undefined behaviour sanitizer" OFF)

# Defines
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -fPIC")
set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_C_FLAGS_DEBUG "-O0 -g3 -Wall -Wextra -Wno-unused-parameter")
set(CMAKE_C_LINK_FLAGS    " ")

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
include(ExternalProject)
set(REBUILD_DEPENDENCIES)
if((NOT APR-ACCRE_FOUND) OR (APR_LIBRARY MATCHES "^${CMAKE_INSTALL_PREFIX}"))
    list(APPEND REBUILD_DEPENDENCIES extern-apr-accre)
    set(APR_LIBRARY "apr-ACCRE-1")
    set(APR_INCLUDE_DIR "${CMAKE_INSTALL_PREFIX}/include/apr-ACCRE-1")
	ExternalProject_add(extern-apr-accre
			PREFIX "${CMAKE_BINARY_DIR}/state/apr-accre/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/apr-accre/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/apr-accre/"
            INSTALL_DIR "${CMAKE_INSTALL_PREFIX}"
			CONFIGURE_COMMAND "${CMAKE_SOURCE_DIR}/vendor/apr-accre/configure"
								"--prefix=${CMAKE_INSTALL_PREFIX}/"
								"--includedir=${CMAKE_INSTALL_PREFIX}/include/apr-ACCRE-1"
								"--with-installbuilddir=${CMAKE_INSTALL_PREFIX}/lib/apr-ACCRE-1/build"
			BUILD_COMMAND "make"
			TEST_COMMAND "make" "test"
			INSTALL_COMMAND "make" "install"
			TEST_EXCLUDE_FROM_MAIN 1
		)
endif()
if((NOT APRUTIL-ACCRE_FOUND) OR (APU_LIBRARY MATCHES "^${CMAKE_INSTALL_PREFIX}")) 
    list(APPEND REBUILD_DEPENDENCIES extern-apr-util-accre)
    set(APRUTIL_LIBRARY "aprutil-ACCRE-1")
    set(APRUTIL_INCLUDE_DIR "${CMAKE_INSTALL_PREFIX}/include/apr-util-ACCRE-1")
	ExternalProject_add(extern-apr-util-accre
			DEPENDS extern-apr-accre
			PREFIX "${CMAKE_BINARY_DIR}/state/apr-util-accre/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/apr-util-accre/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/apr-util-accre/"
            INSTALL_DIR "${CMAKE_INSTALL_PREFIX}"
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
endif()
if((NOT JERASURE_FOUND) OR (JERASURE_LIBRARY MATCHES "^${CMAKE_INSTALL_PREFIX}"))
    list(APPEND REBUILD_DEPENDENCIES extern-jerasure)
    set(JERASURE_LIBRARY "jerasure")
    set(JERASURE_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
	ExternalProject_add(extern-jerasure
			PREFIX "${CMAKE_BINARY_DIR}/state/jerasure/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/jerasure/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/jerasure/"
            INSTALL_DIR "${CMAKE_INSTALL_PREFIX}"
			CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
			TEST_COMMAND ""
			INSTALL_COMMAND "make" "install"
		)
endif()
if((NOT CZMQ_FOUND) OR (CZMQ_LIBRARY MATCHES "^${CMAKE_INSTALL_PREFIX}"))
    list(APPEND REBUILD_DEPENDENCIES extern-czmq)
    set(CZMQ_LIBRARY "czmq")
	ExternalProject_add(extern-czmq
			PREFIX "${CMAKE_BINARY_DIR}/state/czmq/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/czmq/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/czmq/"
            INSTALL_DIR "${CMAKE_INSTALL_PREFIX}"
			CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
			TEST_EXCLUDE_FROM_MAIN 1
			TEST_COMMAND "make" "test"
			INSTALL_COMMAND "make" "install"
		)
endif()

add_custom_target(externals COMMENT "Target to build any externals")
if(REBUILD_DEPENDENCIES)
    message(STATUS "External dependencies exist")
    add_dependencies(externals ${REBUILD_DEPENDENCIES})
    include_directories("${CMAKE_INSTALL_PREFIX}/include")
    link_directories("${CMAKE_INSTALL_PREFIX}/lib")
endif()

# Pull in LStore modules
add_subdirectory(src/toolbox)
add_subdirectory(src/gop)
add_subdirectory(src/ibp)
add_subdirectory(src/lio)

# Pack it up
include(CPack)

# Summary
include(FeatureSummary)
feature_summary(WHAT ALL)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

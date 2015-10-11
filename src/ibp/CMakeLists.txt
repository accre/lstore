cmake_minimum_required(VERSION 2.8)

# Name this project
set(LSTORE_PROJECT_NAME "ibp")
project(${LSTORE_PROJECT_NAME} C CXX)

# Include modules 
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(CheckIncludeFile)

# Detect compiler flags.
# No-op for ibp

# Find additional dependencies.
find_package(ToolBox REQUIRED)
find_package(GOP REQUIRED)
include_directories(${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR})
list(APPEND LIBS ${TOOLBOX_LIBRARY} ${GOP_LIBRARY})

# Additional Compiler flags.
# No-op for ibp

# common objects
set(LSTORE_PROJECT_OBJS
    ibp_version.c ibp_config.c ibp_errno.c ibp_misc.c ibp_op.c
    ibp_sync.c ibp_types.c io_wrapper.c iovec_sync.c 
)

set(LSTORE_PROJECT_INCLUDES
    ibp_ClientLib.h ibp_errno.h ibp.h ibp_misc.h ibp_nfu.h ibp_op.h ibp_os.h
    ibp_protocol.h ibp_types.h iovec_sync.h io_wrapper.h
)

set(LSTORE_PROJECT_EXECUTABLES 
    ibp_perf ibp_test ibp_copyperf ibp_tool ibp_find_drive
)

# Add packaging flags
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>=2.7-18), openssl (>=1.0.1f), zlib1g (>= 1:1.1.4), czmq (>=1.4.1), libapr-accre1 (>= 1.5.0), libapr-accre-util1 (>=1.5.3), libzmq3 (>=4.0.1)")
set(CPACK_RPM_PACKAGE_DEPENDS "libc6 (>=2.7-18), openssl (>=1.0.1f), zlib (>=1.2.8), czmq (>=1.4.1), apr-accre (>= 1.5.0), apr-util-ACCRE (>=1.5.3), zeromq (=>4.0.1)")

# Common functionality is stored here
include(cmake/LStoreCommon.cmake)

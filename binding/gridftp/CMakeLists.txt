cmake_minimum_required(VERSION 2.6)
project(lstore-globus-gridftp-dsi C)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# These variables get compiled into the final module.
set(LSTORE_DSI_VERSION_MAJOR 0)
set(LSTORE_DSI_VERSION_MINOR 3)
include (Timestamp)
TIMESTAMP(LSTORE_DSI_TIMESTAMP)

find_package(GlobusCommon REQUIRED)
find_package(GlobusGridftpServer REQUIRED)
set(GRIDFTP_OBJS
        src/command.c
        src/lstore_dsi.c
        src/stat.c
        src/thunk.c
        src/util.c
        src/xfer.c
    )
set(GRIDFTP_LIBS
        toolbox
        gop
        ibp
        lio
        ${APR_LIBRARY}
        ${APRUTIL_LIBRARY}
        ${GLOBUS_COMMON_LIBRARY}
        ${GLOBUS_GRIDFTP_SERVER_LIBRARY}
    )
set(GRIDFTP_INC
        ${PROJECT_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/toolbox
        ${CMAKE_SOURCE_DIR}/src/gop
        ${CMAKE_SOURCE_DIR}/src/ibp
        ${CMAKE_SOURCE_DIR}/src/lio
        ${APR_INCLUDE_DIR}
        ${APRUTIL_INCLUDE_DIR}
        ${GLOBUS_COMMON_INCLUDE_DIRS}
        ${GLOBUS_GRIDFTP_SERVER_INCLUDE_DIRS}
    )

# Share object files
add_library(gridftp-objs OBJECT ${GRIDFTP_OBJS})
set_target_properties(gridftp-objs PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(gridftp-objs PUBLIC ${GRIDFTP_INC})

add_library(gridftp MODULE $<TARGET_OBJECTS:gridftp-objs>)
target_link_libraries(gridftp ${GRIDFTP_LIBS})
#target_include_directories(gridftp PUBLIC ${GRIDFTP_INC})

add_executable(gridftp-test tests/test.c $<TARGET_OBJECTS:gridftp-objs>)
target_link_libraries(gridftp-test ${GRIDFTP_LIBS})
target_include_directories(gridftp-test PUBLIC ${GRIDFTP_INC})

if (NOT DEFINED CMAKE_INSTALL_LIBDIR)
  set(CMAKE_INSTALL_LIBDIR "lib64")
endif()

install(
  TARGETS gridftp
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

set_target_properties(
  gridftp
  PROPERTIES
  VERSION   "${LSTORE_DSI_VERSION_MAJOR}.${LSTORE_DSI_VERSION_MINOR}.1"
  SOVERSION "0"
  OUTPUT_NAME "globus_gridftp_server_lstore"
  LINK_INTERFACE_LIBRARIES "")

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/version.h.in
                ${CMAKE_CURRENT_SOURCE_DIR}/src/version.h)

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

add_library(gridftp MODULE src/lstore_dsi.c)
target_link_libraries(gridftp
                        toolbox
                        gop
                        ibp
                        lio
                        ${APR_LIBRARY}
                        ${APRUTIL_LIBRARY}
                        ${GLOBUS_COMMON_LIBRARY}
                        ${GLOBUS_GRIDFTP_SERVER_LIBRARY})

target_include_directories(gridftp PUBLIC ${PROJECT_SOURCE_DIR}/src
                                            ${APR_INCLUDE_DIR}
                                            ${APRUTIL_INCLUDE_DIR}
                                            ${GLOBUS_COMMON_INCLUDE_DIRS}
                                            ${GLOBUS_GRIDFTP_SERVER_INCLUDE_DIRS})


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

cmake_minimum_required(VERSION 2.6)
project(lstore-tcmu C)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# These variables get compiled into the final module.
set(TCMUL_VERSION_MAJOR 0)
set(TCMUL_VERSION_MINOR 1)
include (Timestamp)
TIMESTAMP(TCMUL_TIMESTAMP)

find_package(TCMU REQUIRED)

set(TCMUL_OBJS
        src/handler_lstore.c
    )
set(TCMUL_LIBS
        toolbox
        gop
        ibp
        lio
        ${APR_LIBRARY}
        ${APRUTIL_LIBRARY}
    )
set(TCMUL_INC
        ${PROJECT_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/toolbox
        ${CMAKE_SOURCE_DIR}/src/gop
        ${CMAKE_SOURCE_DIR}/src/ibp
        ${CMAKE_SOURCE_DIR}/src/lio
        ${APR_INCLUDE_DIR}
        ${APRUTIL_INCLUDE_DIR}
        ${TCMU_INCLUDE_DIR}
    )

# Share object files
add_library(tcmul-objs OBJECT ${TCMUL_OBJS})
set_target_properties(tcmul-objs PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(tcmul-objs PUBLIC ${TCMUL_INC})

add_library(handler_lstore MODULE $<TARGET_OBJECTS:tcmul-objs>)
set_target_properties(
  handler_lstore
  PROPERTIES
  PREFIX    ""
  VERSION   "${TCMUL_VERSION_MAJOR}.${TCMUL_VERSION_MINOR}.1"
  SOVERSION "0"
  OUTPUT_NAME "handler_lstore"
  LINK_INTERFACE_LIBRARIES "")
target_link_libraries(handler_lstore ${TCMUL_LIBS})

if (NOT DEFINED CMAKE_INSTALL_LIBDIR)
  set(CMAKE_INSTALL_LIBDIR "lib64")
endif()

install(
  TARGETS handler_lstore
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/version.h.in
                ${CMAKE_CURRENT_SOURCE_DIR}/src/version.h)

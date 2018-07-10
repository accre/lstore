cmake_minimum_required(VERSION 2.6)
project(lstore-hdfs C)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

# These variables get compiled into the final module.
set(HDFSL_VERSION_MAJOR 0)
set(HDFSL_VERSION_MINOR 1)
include (Timestamp)
TIMESTAMP(HDFSL_TIMESTAMP)

set(HDFSL_OBJS
        src/hdfs_lstore.c
    )
set(HDFSL_LIBS
        toolbox
        gop
        ibp
        lio
        ${APR_LIBRARY}
        ${APRUTIL_LIBRARY}
    )
set(HDFSL_INC
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
add_library(hdfsl-objs OBJECT ${HDFSL_OBJS})
set_target_properties(hdfsl-objs PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(hdfsl-objs PUBLIC ${HDFSL_INC})

add_library(hdfs_lstore SHARED $<TARGET_OBJECTS:hdfsl-objs>)
set_target_properties(
  hdfs_lstore
  PROPERTIES
  LINKER_LANGUAGE C
  PREFIX    ""
  VERSION   "${HDFSL_VERSION_MAJOR}.${HDFSL_VERSION_MINOR}.1"
  SOVERSION "0"
  OUTPUT_NAME "hdfs_lstore"
)

target_link_libraries(hdfs_lstore ${HDFSL_LIBS})

add_executable(hdfsl_test src/hdfsl_test.c)
set_target_properties(hdfsl_test PROPERTIES LINKER_LANGUAGE C)
target_link_libraries(hdfsl_test hdfs_lstore ${HDFSL_LIBS})

if (NOT DEFINED CMAKE_INSTALL_LIBDIR)
  set(CMAKE_INSTALL_LIBDIR "lib64")
endif()

install(
  TARGETS hdfs_lstore
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/version.h.in
                ${CMAKE_CURRENT_SOURCE_DIR}/src/version.h)

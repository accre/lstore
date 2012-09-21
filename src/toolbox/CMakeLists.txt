cmake_minimum_required(VERSION 2.6)

# make a project
project( toolbox C CXX )

# Set some cmake defaults
set(CMAKE_BUILD_TYPE "Debug")
#set(CMAKE_BUILD_TYPE "Release")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_REQUIRED_FLAGS} -O0 -Wall -g -DHAVE_CONFIG_H -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE" )
set(CMAKE_C_FLAGS_RELEASE "-O ${CMAKE_REQUIRED_FLAGS} -DHAVE_CONFIG_H -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE" )

set(CMAKE_INCLUDE_CURRENT_DIR on)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
INCLUDE(CheckIncludeFile)
include(${CMAKE_SOURCE_DIR}/cmake/Date.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerVersion.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerFlags.cmake)

#Prefer to use static libs
if(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
else(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
endif(WIN32)

# get dependancies
find_package(OpenSSL REQUIRED)
find_package(APR REQUIRED)
find_package(APRUtil REQUIRED)
find_package(HwLoc REQUIRED)
find_package(Phoebus)
find_package(ZMQ REQUIRED)
find_package(CZMQ REQUIRED)

# config options
if (Phoebus_FOUND)
  set(PHOEBUS_OPT "-D_ENABLE_PHOEBUS")
  OPTION( _ENABLE_PHOEBUS "Enable Phoebus support" ON )
  include_directories(${PHOEBUS_INCLUDE_DIR})
  list(APPEND LIBS ${PHOEBUS_LIBRARIES})
else (Phoebus_FOUND)
  OPTION( _ENABLE_PHOEBUS "Enable Phoebus support" OFF )
endif (Phoebus_FOUND)

check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)

include_directories(${OPENSSL_INCLUDE_DIR} ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR} ${CZMQ_INCLUDE_DIR})
SET(LIBS ${LIBS} ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} ${APR_LIBRARY} ${APRUTIL_LIBRARY} ${HWLOC_LIBRARY} ${CZMQ_LIBRARY} ${ZMQ_LIBRARY} rt pthread m xml2 dl)

CONFIGURE_FILE( ${CMAKE_SOURCE_DIR}/toolbox_config.h.in ${CMAKE_SOURCE_DIR}/toolbox_config.h )

#Make the version file
set(TOOLBOX_VERSION "toolbox: v1.0.0")
site_name(BUILD_HOST)
Date(BUILD_DATE)
CompilerVersion(COMPILER_VERSION)
CompilerFlags(COMPILER_FLAGS)
configure_file("${PROJECT_SOURCE_DIR}/toolbox_version.c.in" "${PROJECT_BINARY_DIR}/toolbox_version.c")

# common objects
SET(NETWORK_OBJS 
    network.c 
    net_sock.c 
    net_1_ssl.c 
    net_2_ssl.c 
    net_fd.c 
    net_phoebus.c 
    phoebus.c 
    net_zsock.c
)

SET(TOOL_OBJS  
    string_token.c 
    dns_cache.c 
    log.c 
    stack.c
    append_printf.c
    iniparse.c 
    apr_wrapper.c
    pigeon_coop.c
    pigeon_hole.c
    skiplist.c
    interval_skiplist.c
    atomic_counter.c
    hwinfo.c
    random.c
    chksum.c 
    transfer_buffer.c
)

SET( TOOLBOX_OBJS ${TOOL_OBJS} ${NETWORK_OBJS} )

# various executables
#ADD_EXECUTABLE( sl_test sl_test.c skiplist.c log.c atomic_counter.c iniparse.c stack.c string_token.c )
#ADD_EXECUTABLE( isl_test isl_test.c interval_skiplist.c skiplist.c log.c atomic_counter.c iniparse.c stack.c string_token.c )
#ADD_EXECUTABLE( hw_info hw_info.c hwinfo.c)
ADD_EXECUTABLE( sl_test sl_test.c )
ADD_EXECUTABLE( isl_test isl_test.c )
ADD_EXECUTABLE( hw_info hw_info.c)
ADD_EXECUTABLE( netzsock_test netzsock_test.c )

ADD_LIBRARY( toolbox SHARED ${TOOLBOX_OBJS})
ADD_LIBRARY( toolbox-static STATIC ${TOOLBOX_OBJS})
SET_TARGET_PROPERTIES( toolbox-static PROPERTIES OUTPUT_NAME "toolbox" )
SET_TARGET_PROPERTIES(toolbox PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(toolbox-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(netzsock_test PROPERTIES LINKER_LANGUAGE CXX)
#Make static executables by default
SET(toolbox_lib toolbox-static)
#To use shared libs
#SET(toolbox_lib toolbox)

TARGET_LINK_LIBRARIES( sl_test  ${toolbox_lib} ${LIBS})
TARGET_LINK_LIBRARIES( isl_test  ${toolbox_lib} ${LIBS})
TARGET_LINK_LIBRARIES( hw_info ${toolbox_lib} ${LIBS} )
TARGET_LINK_LIBRARIES( netzsock_test ${toolbox_lib} ${LIBS} )

install(TARGETS toolbox DESTINATION lib)
install(TARGETS toolbox-static DESTINATION lib)
install(FILES append_printf.h   chksum.h     fmttypes.h  interval_skiplist.h  net_1_ssl.h  net_phoebus.h  phoebus.h      random.h    string_token.h    type_malloc.h
              apr_wrapper.h     debug.h      hwinfo.h    list.h               net_2_ssl.h  net_sock.h     pigeon_coop.h  skiplist.h  toolbox_config.h
              atomic_counter.h  dns_cache.h  iniparse.h  log.h                net_fd.h     network.h      pigeon_hole.h  stack.h     transfer_buffer.h	net_zsock.h
              DESTINATION include/toolbox )

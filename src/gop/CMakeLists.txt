cmake_minimum_required(VERSION 2.6)

# make a project
project( gop C CXX )

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
find_package(ToolBox REQUIRED)
find_package(ZMQ REQUIRED)
find_package(CZMQ REQUIRED)
find_package(Zlib REQUIRED)

#Make the version file
set(GOP_VERSION "gop: v1.0.0")
site_name(BUILD_HOST)
Date(BUILD_DATE)
CompilerVersion(COMPILER_VERSION)
CompilerFlags(COMPILER_FLAGS)
configure_file("${PROJECT_SOURCE_DIR}/gop_version.c.in" "${PROJECT_BINARY_DIR}/gop_version.c")

SET(GOP_OBJS callback.c gop.c  hconnection.c  hportal.c  opque.c  thread_pool_config.c  thread_pool_op.c mq_msg.c mq_zmq.c mq_portal.c mq_ongoing.c mq_stream.c mq_helpers.c )

ADD_LIBRARY( gop SHARED ${GOP_OBJS})
ADD_LIBRARY( gop-static STATIC ${GOP_OBJS})
SET_TARGET_PROPERTIES(gop-static PROPERTIES OUTPUT_NAME "gop" )
SET_TARGET_PROPERTIES(gop PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(gop-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

include_directories(${TOOLBOX_INCLUDE_DIR} ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR} ${CZMQ_INCLUDE_DIR} ${ZLIB_INCLUDE_DIR} )

SET(LIBS ${LIBS} ${ZMQ_LIBRARY} ${CZMQ_LIBRARY} ${TOOLBOX_LIBRARY} ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} 
    ${PROTOBUF_LIBRARY} ${APR_LIBRARY} ${APRUTIL_LIBRARY} ${XML_LIB} ${ZLIB_LIBRARY} pthread m dl rt pciaccess)

ADD_EXECUTABLE(mq_test mq_test.c)
SET_TARGET_PROPERTIES(mq_test PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES(mq_test gop-static ${LIBS})

ADD_EXECUTABLE(mqs_test mqs_test.c)
SET_TARGET_PROPERTIES(mqs_test PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES(mqs_test gop-static ${LIBS})

ADD_EXECUTABLE(zmq_test zmq_test.c)
SET_TARGET_PROPERTIES(zmq_test PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES(zmq_test gop-static ${LIBS})

install(TARGETS ${LIO_EXE} DESTINATION bin)
install(TARGETS gop DESTINATION lib)
install(TARGETS gop-static DESTINATION lib)
install(FILES callback.h gop_config.h  host_portal.h  opque.h  thread_pool.h mq_portal.h mq_helpers.h mq_stream.h mq_ongoing.h
              DESTINATION include/gop )

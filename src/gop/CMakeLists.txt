cmake_minimum_required(VERSION 2.8)

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
include(${CMAKE_SOURCE_DIR}/cmake/shared_name.cmake)

#Prefer to use static libs
if(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
else(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
endif(WIN32)

# get dependancies
find_package(OpenSSL REQUIRED)
find_package(APR-ACCRE REQUIRED)
find_package(APRUtil-ACCRE REQUIRED)
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

SET(GOP_OBJS callback.c gop.c  hconnection.c  hportal.c  opque.c  thread_pool_config.c  thread_pool_op.c mq_msg.c mq_zmq.c mq_portal.c mq_ongoing.c mq_stream.c mq_helpers.c mq_roundrobin.c)

ADD_LIBRARY( gop SHARED ${GOP_OBJS})
ADD_LIBRARY( gop-static STATIC ${GOP_OBJS})
SET_TARGET_PROPERTIES(gop-static PROPERTIES OUTPUT_NAME "gop" )
SET_TARGET_PROPERTIES(gop PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(gop-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

include_directories(${TOOLBOX_INCLUDE_DIR} ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR} ${CZMQ_INCLUDE_DIR} ${ZLIB_INCLUDE_DIR} )

SET(LIBS ${LIBS} ${ZMQ_LIBRARY} ${CZMQ_LIBRARY} ${TOOLBOX_LIBRARY} ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} 
    ${PROTOBUF_LIBRARY} ${APR_LIBRARY} ${APRUTIL_LIBRARY} ${XML_LIB} ${ZLIB_LIBRARY} pthread m dl rt)


#Make the executables
foreach(exe mq_test mqs_test zmq_test)
  ADD_EXECUTABLE(${exe} "${exe}.c")
  SET_TARGET_PROPERTIES(${exe} PROPERTIES LINKER_LANGUAGE CXX)
  TARGET_LINK_LIBRARIES(${exe} gop-static ${LIBS})
endforeach()

set(sodeps)
set(lpdeps)
foreach(lib ${LIBS} )
  shared_name(${lib} soname)
  set(sodeps ${sodeps} ${soname})

  get_filename_component(ldir ${lib} PATH )
  if (ldir)
     set(lpdeps ${lpdeps} "-L${ldir}" )
  endif()
endforeach()

#link_directories(/usr/local/lib)
TARGET_LINK_LIBRARIES( gop LINK_PUBLIC ${lpdeps} ${sodeps} )

ADD_EXECUTABLE(rr_mq_test rr_mq_test.c)
SET_TARGET_PROPERTIES(rr_mq_test PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES(rr_mq_test gop-static ${LIBS})

ADD_EXECUTABLE(rr_mq_server rr_mq_server.c)
SET_TARGET_PROPERTIES(rr_mq_server PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES(rr_mq_server gop-static ${LIBS})

ADD_EXECUTABLE(rr_mq_client rr_mq_client.c)
SET_TARGET_PROPERTIES(rr_mq_client PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES(rr_mq_client gop-static ${LIBS})

ADD_EXECUTABLE(rr_mq_worker rr_mq_worker.c)
SET_TARGET_PROPERTIES(rr_mq_worker PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES(rr_mq_worker gop-static ${LIBS})

install(TARGETS ${LIO_EXE} DESTINATION bin)
install(TARGETS gop DESTINATION lib)
install(TARGETS gop-static DESTINATION lib)
install(FILES callback.h gop_config.h  host_portal.h  opque.h  thread_pool.h mq_portal.h mq_helpers.h mq_stream.h mq_ongoing.h mq_roundrobin.h mqs_roundrobin.h
              DESTINATION include/gop )

# Packaging directives

set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_GENERATOR "RPM")
set(CPACK_PACKAGE_NAME "accre-gop")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Andrew Melo")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
#SET(CPACK_RPM_PACKAGE_REQUIRES "openssl >= 1.0.1e")
#set(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
set(CPACK_PACKAGING_INSTALL_PREFIX "/usr/local")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "/usr/local")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
include(CPack)

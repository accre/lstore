cmake_minimum_required(VERSION 2.6)

# make a project
project( LIO C )

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
find_package(Protobufc REQUIRED)
find_package(APR REQUIRED)
find_package(APRUtil REQUIRED)
#find_package(APRUtil REQUIRED)
find_package(Phoebus)
find_package(HwLoc REQUIRED)
find_package(Jerasure REQUIRED)
find_package(ToolBox REQUIRED)
find_package(GOP REQUIRED)
find_package(IBP REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR} ${PROTOBUF_INCLUDE_DIR} ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${PHOEBUS_INCLUDE_DIR}
   ${HWLOC_INCLUDE_DIR} ${JERASURE_INCLUDE_DIR} ${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR} ${IBP_INCLUDE_DIR} )
SET(LIBS ${LIBS} ${IBP_LIBRARY} ${GOP_LIBRARY} ${TOOLBOX_LIBRARY} ${JERASURE_LIBRARY} ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} 
   ${PROTOBUF_LIBRARY} ${APR_LIBRARY} ${APRUTIL_LIBRARY} ${HWLOC_LIBRARY} pthread xml2 m dl)

#Make the version file
set(LIO_CLIENT_VERSION "lio_client: v1.0.0")
site_name(BUILD_HOST)
Date(BUILD_DATE)
CompilerVersion(COMPILER_VERSION)
CompilerFlags(COMPILER_FLAGS)
configure_file("${PROJECT_SOURCE_DIR}/lio_client_version.c.in" "${PROJECT_BINARY_DIR}/lio_client_version.c")

set(LIO_OBJS
   data_block.c
   data_service_base.c
   ds_ibp.c
   os_base.c
   os_file.c
   ex3_global.c
   ex3_header.c
   ex3_compare.c
   exnode.c
   exnode3.pb-c.c
   exnode_config.c
   ex_id.c
   lio_config.c
   rs_base.c
   rs_query_base.c
   rs_simple.c
   segment_base.c
   segment_linear.c
   segment_file.c
   segment_cache.c
   segment_lun.c
   segment_log.c
   view_base.c
   cache_base.c
   cache_lru.c
   cache_amp.c
   segment_jerasure.c
   erasure_tools.c
   raid4.c
)

set(LIBLIO_TRACE_OBJS liblio_trace.c )

#message("proto src is ${PROTO_SRC}")
# various executables
ADD_EXECUTABLE( mk_linear mk_linear.c ${EX3_OBJS} )
ADD_EXECUTABLE( ex_load ex_load.c ${EX3_OBJS} )
ADD_EXECUTABLE( ex_get ex_get.c ${EX3_OBJS} )
ADD_EXECUTABLE( ex_put ex_put.c ${EX3_OBJS} )
ADD_EXECUTABLE( ex_inspect ex_inspect.c ${EX3_OBJS} )
ADD_EXECUTABLE( ex_clone ex_clone.c ${EX3_OBJS} )
ADD_EXECUTABLE( ex_rw_test ex_rw_test.c ${EX3_OBJS} )
ADD_EXECUTABLE( trace_replay trace_replay.c trace.c ${EX3_OBJS} )
ADD_EXECUTABLE( log_test log_test.c ${EX3_OBJS} )
ADD_EXECUTABLE( rs_test rs_test.c ${EX3_OBJS} )

ADD_LIBRARY( lio_trace SHARED ${LIBLIO_TRACE_OBJS})
ADD_LIBRARY( lio SHARED ${LIO_OBJS})
ADD_LIBRARY( lio-static STATIC ${LIO_OBJS})
SET_TARGET_PROPERTIES( lio-static PROPERTIES OUTPUT_NAME "lio" )
SET_TARGET_PROPERTIES(lio PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(lio-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

#Make static executables by default
SET(lio_lib lio-static)
#To use shared libs
#SET(lio_lib lio)

TARGET_LINK_LIBRARIES( mk_linear ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( ex_load ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( ex_get ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( ex_put ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( ex_inspect ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( ex_clone ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( ex_rw_test ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( trace_replay ${lio_lib} ${LIBS} m )
TARGET_LINK_LIBRARIES( log_test ${lio_lib} ${LIBS})
TARGET_LINK_LIBRARIES( rs_test ${lio_lib} ${LIBS})

install(TARGETS lio DESTINATION lib)
install(TARGETS lio-static DESTINATION lib)
install(FILES 
   cache_amp.h       data_block.h             ex3_abstract.h  ex3_linear_priv.h  exnode.h         rs_simple.h           segment_jerasure.h  segment_lun_priv.h
   cache_amp_priv.h  data_service_abstract.h  ex3_compare.h   ex3_system.h       lio.h            rs_simple_priv.h      segment_linear.h    trace.h
   cache_lru.h       ds_ibp.h                 ex3_fmttypes.h  ex3_types.h        os_file.h        segment_cache.h       segment_log.h       transfer_buffer.h
   cache_lru_priv.h  ds_ibp_priv.h            ex3_header.h    exnode3.h          raid4.h          segment_cache_priv.h  segment_log_priv.h  view_layout.h
   cache_priv.h      erasure_tools.h          ex3_linear.h    exnode3.pb-c.h     rs_query_base.h  segment_file.h        segment_lun.h       cache.h
   DESTINATION include/lio )



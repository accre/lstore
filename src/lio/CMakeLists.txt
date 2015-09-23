cmake_minimum_required(VERSION 2.8)

# make a project
project( LIO C CXX)

# Set some cmake defaults
set(CMAKE_BUILD_TYPE "Debug")
#set(CMAKE_BUILD_TYPE "Release")

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
find_package(Phoebus)
find_package(Jerasure REQUIRED)
find_package(ToolBox REQUIRED)
find_package(GOP REQUIRED)
find_package(IBP REQUIRED)
find_package(ZMQ REQUIRED)
find_package(CZMQ REQUIRED)
find_package(FUSE REQUIRED)
find_package(Zlib REQUIRED)

# check for xattr
configure_file("${PROJECT_SOURCE_DIR}/config.h.in" "${PROJECT_SOURCE_DIR}/config.h")
check_include_file(attr/xattr.h HAVE_ATTR_XATTR_H)
check_include_file(sys/xattr.h HAVE_SYS_XATTR_H)

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_REQUIRED_FLAGS} -O0 -Wall -g -DHAVE_CONFIG_H -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 ${XATTR_FOUND}" )
set(CMAKE_C_FLAGS_RELEASE "-O ${CMAKE_REQUIRED_FLAGS} -DHAVE_CONFIG_H -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 ${XATTR_FOUND}" )


include_directories(${OPENSSL_INCLUDE_DIR} ${APR_INCLUDES} ${APU_INCLUDES} ${PHOEBUS_INCLUDE_DIR} ${ZLIB_INCLUDE_DIR}
   ${JERASURE_INCLUDE_DIR} ${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR} ${IBP_INCLUDE_DIR} ${FUSE_INCLUDE_DIR} 
   ${CZMQ_INCLUDE_DIR} ${XATTR_INCLUDE_DIR} )

SET(LIBS ${LIBS} ${IBP_LIBRARY} ${GOP_LIBRARY} ${CZMQ_LIBRARY} ${TOOLBOX_LIBRARY} ${JERASURE_LIBRARY} ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} 
    ${APR_LIBRARY} ${APU_LIBRARY} ${ZMQ_LIBRARY} ${FUSE_LIBRARIES} ${ZLIB_LIBRARY} ${XATTR_LIBRARY} gssapi_krb5 pthread m dl)

#Make the version file
set(LIO_CLIENT_VERSION "lio_client: v1.0.0")
site_name(BUILD_HOST)
Date(BUILD_DATE)
CompilerVersion(COMPILER_VERSION)
CompilerFlags(COMPILER_FLAGS)
configure_file("${PROJECT_SOURCE_DIR}/lio_client_version.c.in" "${PROJECT_SOURCE_DIR}/lio_client_version.c")

set(LIO_OBJS
   authn_fake.c
   cred_default.c
   data_block.c
   ds_ibp.c
   os_base.c
   os_file.c
   os_timecache.c
   os_remote_client.c
   os_remote_server.c
   osaz_fake.c
   ex3_global.c
   ex3_header.c
   ex3_compare.c
   exnode.c
   exnode_config.c
   ex_id.c
   lio_config.c
   lio_core.c
   lio_core_io.c
   lio_core_os.c
   lio_fuse_core.c
   rs_query_base.c
   rs_simple.c
   rs_space.c
   rs_remote_client.c
   rs_remote_server.c
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
   cache_round_robin.c
   segment_jerasure.c
   erasure_tools.c
   raid4.c
   service_manager.c
   zhelpers.c
   archive.c
)

set(LIBLIO_TRACE_OBJS liblio_trace.c )

if (NOT APPLE)
ADD_LIBRARY( lio_trace SHARED ${LIBLIO_TRACE_OBJS})
endif (NOT APPLE)
ADD_LIBRARY( lio SHARED ${LIO_OBJS})
ADD_LIBRARY( lio-static STATIC ${LIO_OBJS})
SET_TARGET_PROPERTIES( lio-static PROPERTIES OUTPUT_NAME "lio" )
SET_TARGET_PROPERTIES(lio PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(lio-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

if (NOT APPLE)
ADD_LIBRARY(dynfile SHARED segment_dynfile.c )
SET_TARGET_PROPERTIES(dynfile PROPERTIES CLEAN_DIRECT_OUTPUT 1)
endif (NOT APPLE)
set(LIO_EXE 
    mk_linear ex_load ex_get ex_put ex_inspect ex_clone ex_rw_test log_test rs_test os_test os_fsck
    lio_touch lio_mkdir lio_rmdir lio_rm lio_ln lio_find lio_ls lio_du lio_setattr lio_getattr lio_mv lio_cp
    lio_put lio_get lio_signature lio_warm lio_inspect lio_fsck lio_rs lio_server lio_fuse arc_tag_create
    arc_tag_destroy arc_tag arc_tag_ls arc_create arc_restore ds_read
)

#Make static executables by default
SET(lio_lib lio-static)
#To use shared libs
#SET(lio_lib lio)

#Add the exe build rules
foreach (f ${LIO_EXE})
  add_executable(${f} ${f}.c ${EXE3_OBJS})
  set_target_properties(${f} PROPERTIES LINKER_LANGUAGE CXX)
  target_link_libraries(${f} ${lio_lib} ${LIBS})
endforeach(f)

add_executable( trace_replay trace_replay.c trace.c ${EX3_OBJS} )
set_target_properties(trace_replay PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries( trace_replay ${lio_lib} ${LIBS} m )

add_executable( zadler32 zadler32.c )
set_target_properties(zadler32 PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(zadler32 ${TOOLBOX_LIBRARY} ${ZLIB_LIBRARY} m )

add_executable( ldiff ldiff.c )
set_target_properties(ldiff PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(ldiff ${TOOLBOX_LIBRARY} m )

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

TARGET_LINK_LIBRARIES( lio LINK_PUBLIC ${lpdeps} ${sodeps} )

install(TARGETS ${LIO_EXE} DESTINATION bin)
install(TARGETS lio DESTINATION lib)
install(TARGETS lio-static DESTINATION lib)
install(FILES 
   cache_amp.h       data_block.h             ex3_abstract.h  ex3_linear_priv.h  exnode.h         rs_simple.h	        segment_jerasure.h  segment_lun_priv.h
   cache_amp_priv.h  data_service_abstract.h  ex3_compare.h   ex3_system.h       lio.h            rs_simple_priv.h	segment_linear.h    trace.h
   cache_lru.h       ds_ibp.h                 ex3_fmttypes.h  ex3_types.h        os_file.h        segment_cache.h       segment_log.h
   cache_lru_priv.h  ds_ibp_priv.h            ex3_header.h    exnode3.h          raid4.h          segment_cache_priv.h  segment_log_priv.h  view_layout.h
   cache_priv.h      erasure_tools.h          ex3_linear.h    rs_query_base.h    segment_file.h   segment_lun.h         cache.h
   authn_abstract.h  authn_fake.h      	      osaz_fake.h     rs_remote.h        archive.h
   lio_abstract.h    lio_fuse.h
   cache_round_robin.h
   resource_service_abstract.h
   object_service_abstract.h
   service_manager.h
   rs_zmq.h
   os_remote.h
   os_timecache.h
   DESTINATION include/lio )


# Packaging directives
set(PKG_PREFIX "/usr/local")
if (DEFINED ENV{PREFIX})
    set(PKG_PREFIX "$ENV{PREFIX}")
    message("Using install prefix from ENV, ${PKG_PREFIX}")
else()
    message("Using default install prefix, ${PKG_PREFIX}")
endif()

set(CPACK_PROJECT_CONFIG_FILE ${CMAKE_SOURCE_DIR}/cmake/cpack_settings.cmake) #This is where the build type specific settings are kept
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_GENERATOR "DEB;RPM")
set(CPACK_PACKAGE_NAME "accre-lio")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(CPACK_PACKAGING_INSTALL_PREFIX "${PKG_PREFIX}")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "${PKG_PREFIX}")
include(CPack)

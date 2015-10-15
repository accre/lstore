cmake_minimum_required(VERSION 2.8)
if(NOT (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 3.0))
	# only CMAKE 3.0 and greater supports this (NOT ... LESS because CMAKE does not have a GREATER_OR_EQUAL_TO conditional)
	# Not needed for older versions because older versions obviously use the old policy without specially requesting it
	cmake_policy(SET CMP0048 OLD)
	# ^^^ use the old VERSION variable behavior
endif()
# Name this project
set(LSTORE_PROJECT_NAME "lio")
project(${LSTORE_PROJECT_NAME} C CXX)

# Include modules 
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(CheckIncludeFile)

# Detect compiler flags.
check_include_file(attr/xattr.h HAVE_ATTR_XATTR_H)
check_include_file(sys/xattr.h HAVE_SYS_XATTR_H)
configure_file(${CMAKE_SOURCE_DIR}/config.h.in
               ${CMAKE_SOURCE_DIR}/config.h)

# Find dependencies.
find_package(Jerasure REQUIRED)
find_package(ToolBox REQUIRED)
find_package(GOP REQUIRED)
find_package(IBP REQUIRED)
find_package(ZMQ REQUIRED)
find_package(CZMQ REQUIRED)
find_package(FUSE REQUIRED)
include_directories(   
    ${JERASURE_INCLUDE_DIR} ${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR}
    ${IBP_INCLUDE_DIR} ${FUSE_INCLUDE_DIR} ${CZMQ_INCLUDE_DIR}
)

list(APPEND LIBS
    ${IBP_LIBRARY} ${GOP_LIBRARY} ${CZMQ_LIBRARY} ${TOOLBOX_LIBRARY}
    ${JERASURE_LIBRARY} ${ZMQ_LIBRARY} ${FUSE_LIBRARIES}
)

# common objects
set(LSTORE_PROJECT_OBJS
    archive.c authn_fake.c cache_amp.c cache_base.c cache_lru.c
    cache_round_robin.c cred_default.c data_block.c ds_ibp.c erasure_tools.c
    ex3_compare.c ex3_global.c ex3_header.c ex_id.c exnode.c exnode_config.c
    lio_config.c lio_core.c lio_core_io.c lio_core_os.c lio_fuse_core.c
    os_base.c os_file.c os_remote_client.c os_remote_server.c os_timecache.c
    osaz_fake.c raid4.c rs_query_base.c rs_remote_client.c rs_remote_server.c
    rs_simple.c rs_space.c segment_base.c segment_cache.c segment_file.c
    segment_jerasure.c segment_linear.c segment_log.c segment_lun.c
    service_manager.c view_base.c
)
    
set(LSTORE_PROJECT_INCLUDES
    cache_amp.h data_block.h ex3_abstract.h ex3_linear_priv.h exnode.h
    rs_simple.h	 segment_jerasure.h segment_lun_priv.h cache_amp_priv.h
    data_service_abstract.h ex3_compare.h ex3_system.h lio.h rs_simple_priv.h
    segment_linear.h trace.h cache_lru.h ds_ibp.h ex3_fmttypes.h ex3_types.h
    os_file.h segment_cache.h segment_log.h cache_lru_priv.h ds_ibp_priv.h
    ex3_header.h exnode3.h raid4.h segment_cache_priv.h segment_log_priv.h
    view_layout.h cache_priv.h erasure_tools.h ex3_linear.h rs_query_base.h
    segment_file.h segment_lun.h cache.h authn_abstract.h authn_fake.h
    osaz_fake.h rs_remote.h archive.h lio_abstract.h lio_fuse.h
    cache_round_robin.h resource_service_abstract.h object_service_abstract.h
    service_manager.h rs_zmq.h os_remote.h os_timecache.h
)

set(LSTORE_PROJECT_EXECUTABLES
     arc_restore ds_read lio_find lio_ls lio_du lio_setattr lio_getattr lio_mv
     lio_cp lio_put lio_fuse arc_tag_create arc_tag_destroy arc_tag arc_tag_ls
     arc_create lio_get lio_signature lio_warm lio_inspect lio_fsck lio_rs
     lio_server mk_linear ex_load ex_get ex_put ex_inspect ex_clone ex_rw_test
     log_test rs_test os_test os_fsck lio_touch lio_mkdir lio_rmdir lio_rm
     lio_ln
)

# Common functionality is stored here
include(cmake/LStoreCommon.cmake)

# Add additional executables
add_executable(trace_replay trace_replay.c trace)
set_target_properties(trace_replay PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(trace_replay ${library_lib} ${LIBS})

add_executable(zadler32 zadler32.c)
set_target_properties(zadler32 PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(zadler32 ${TOOLBOX_LIBRARY} ${ZLIB_LIBRARY} m)

add_executable(ldiff ldiff.c)
set_target_properties(ldiff PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(ldiff ${TOOLBOX_LIBRARY} m)

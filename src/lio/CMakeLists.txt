cmake_minimum_required(VERSION 2.8)

if(POLICY CMP0048)
	cmake_policy(SET CMP0048 OLD)
endif()

# Name this project
set(LSTORE_PROJECT_NAME "lio")
project(${LSTORE_PROJECT_NAME} C)

# Include modules
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
endif()
include(CheckIncludeFile)

# Detect compiler flags.
check_include_file(attr/xattr.h HAVE_ATTR_XATTR_H)
check_include_file(sys/xattr.h HAVE_SYS_XATTR_H)
configure_file(${PROJECT_SOURCE_DIR}/config.h.in
               ${PROJECT_SOURCE_DIR}/config.h)

# Find dependencies.
if(NOT USE_SUPERBUILD)
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
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64")
endif()
message(STATUS "APU ${APRUTIL_INCLUDE_DIR}")
set(LSTORE_LIBS  ${APR_LIBRARY}
                    ${APRUTIL_LIBRARY}
                    ${ZLIB_LIBRARY}
                    ${OPENSSL_LIBRARIES}
                    ${CZMQ_LIBRARY}
                    ${ZMQ_LIBRARY}
                    ${FUSE_LIBRARY}
                    ${JERASURE_LIBRARY}
                    toolbox
                    gop
                    ibp)
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR}
                    ${APRUTIL_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${ZMQ_INCLUDE_DIR}
                    ${CZMQ_INCLUDE_DIR}
                    ${ZLIB_INCLUDE_DIR}
                    ${FUSE_INCLUDE_DIR}
                    ${JERASURE_INCLUDE_DIR})
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

# common objects
set(LSTORE_PROJECT_OBJS
    authn_fake.c cache_amp.c cache_base.c
    cache_round_robin.c constructor.c cred_default.c data_block.c ds_ibp.c
    erasure_tools.c ex3_compare.c ex3_global.c ex3_header.c ex_id.c exnode.c
    exnode_config.c lio_config.c lio_core.c lio_core_io.c lio_core_os.c
    lio_fuse_core.c os_base.c os_file.c os_remote_client.c os_remote_server.c
    os_timecache.c osaz_fake.c raid4.c rs_query_base.c rs_remote_client.c
    rs_remote_server.c rs_simple.c rs_space.c segment_base.c segment_cache.c
    segment_file.c segment_jerasure.c segment_linear.c segment_log.c
    segment_lun.c service_manager.c view_base.c
)

set(LSTORE_PROJECT_INCLUDES_OLD
    cache_amp.h blacklist.h data_block.h ex3_abstract.h ex3_linear_priv.h
    rs_simple.h	 segment_jerasure.h segment_lun_priv.h cache_amp_priv.h
    data_service_abstract.h ex3_compare.h ex3_system.h lio.h rs_simple_priv.h
    segment_linear.h trace.h ds_ibp.h ex3_fmttypes.h ex3_types.h
    os_file.h segment_cache.h segment_log.h ds_ibp_priv.h
    ex3_header.h exnode.h raid4.h segment_cache_priv.h segment_log_priv.h
    view_layout.h cache_priv.h erasure_tools.h ex3_linear.h rs_query_base.h
    segment_file.h segment_lun.h cache.h authn_abstract.h authn_fake.h
    osaz_fake.h rs_remote.h lio_abstract.h lio_fuse.h
    cache_round_robin.h resource_service_abstract.h object_service_abstract.h
    service_manager.h os_remote.h os_timecache.h
)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE lio)
set(LSTORE_PROJECT_INCLUDES
        lio/lio_visibility.h
    )

set(LSTORE_PROJECT_EXECUTABLES
     ds_read lio_find lio_ls lio_du lio_setattr lio_getattr lio_mv
     lio_cp lio_put lio_fuse
     lio_get lio_signature lio_warm lio_inspect lio_fsck lio_rs
     lio_server mk_linear ex_load ex_get ex_put ex_inspect ex_clone
     os_fsck lio_touch lio_mkdir lio_rmdir lio_rm
     lio_ln zadler32 ldiff
)

# Common functionality is stored here
if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
    add_executable(trace_replay trace_replay.c trace.c)
    set_target_properties(trace_replay PROPERTIES LINKER_LANGUAGE CXX)
    target_link_libraries(trace_replay ${library_lib} ${LIBS})
else()
    include(LStoreFunctions)
    lstore_project_common(lio)
    add_executable(trace_replay trace_replay.c trace.c)
    target_link_libraries(trace_replay lio ${LSTORE_LIBS})
    if(NOT BUILD_SHARED_EXES)
        set_target_properties(trace_replay PROPERTIES LINK_SEARCH_START_STATIC 1)
        set_target_properties(trace_replay PROPERTIES LINK_SEARCH_END_STATIC 1)
    endif()
    set_source_files_properties(trace_replay.c PROPERTIES COMPILE_FLAGS "-Wno-strict-overflow")
    set_source_files_properties(trace.c PROPERTIES COMPILE_FLAGS "${LSTORE_FLAG_UNUSED_RESULT}")
    target_include_directories(trace_replay SYSTEM PRIVATE ${LSTORE_INCLUDE_SYSTEM})
    target_include_directories(trace_replay PUBLIC ${LSTORE_INCLUDE_PUBLIC})
endif()

# Temporarily disable errors around unused results
foreach(f erasure_tools.c exnode.c lio_config.c ds_read.c ldiff.c)
    get_source_file_property(OLD_COMPILE_FLAGS ${f} COMPILE_FLAGS)
    if(NOT OLD_COMPILE_FLAGS)
        set(OLD_COMPILE_FLAGS)
    endif()
    set_source_files_properties(${f} PROPERTIES COMPILE_FLAGS "${OLD_COMPILE_FLAGS} ${LSTORE_FLAG_UNUSED_RESULT}")
endforeach()

# Temporarily disable errors around strict overflow
#foreach(f lio_rm.c)
#    get_source_file_property(OLD_COMPILE_FLAGS ${f} COMPILE_FLAGS)
#    if(NOT OLD_COMPILE_FLAGS)
#        set(OLD_COMPILE_FLAGS)
#    endif()
#    set_source_files_properties(${f} PROPERTIES COMPILE_FLAGS "${OLD_COMPILE_FLAGS} -Wno-strict-overflow")
#endforeach()

foreach(f ${LSTORE_PROJECT_EXECUTABLES} trace_replay)
    set_target_properties(${f} PROPERTIES COMPILE_FLAGS "-DLSTORE_HACK_EXPORT")
endforeach()

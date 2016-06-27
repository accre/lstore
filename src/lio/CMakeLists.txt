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
set(LSTORE_LIBS toolbox
                gop
                ibp
                ${APR_LIBRARY}
                ${APRUTIL_LIBRARY}
                ${ZLIB_LIBRARY}
                ${OPENSSL_LIBRARIES}
                ${CZMQ_LIBRARY}
                ${ZMQ_LIBRARY}
                ${FUSE_LIBRARY}
                ${JERASURE_LIBRARY}
                )
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
		authn_fake.c
		cache_amp.c
		cache_base.c
		cache_round_robin.c
		constructor.c
		cred_default.c
		data_block.c
		ds_ibp.c
		erasure_tools.c
		ex3_compare.c
		ex3_global.c
		ex3_header.c
		ex_id.c
		exnode.c
		exnode_config.c
		lio_config.c
		lio_core.c
		lio_core_io.c
		lio_core_os.c
		lio_fuse_core.c
		lio_version.c
		os_base.c
		os_file.c
		os_remote_client.c
		os_remote_server.c
		os_timecache.c
		osaz_fake.c
		raid4.c
		rs_query_base.c
		rs_remote_client.c
		rs_remote_server.c
		rs_simple.c
		rs_space.c
		segment_base.c
		segment_cache.c
		segment_dynfile.c
		segment_file.c
		segment_jerasure.c
		segment_linear.c
		segment_log.c
		segment_lun.c
		service_manager.c
		trace.c
		trace_replay.c
		view_base.c
)

set(LSTORE_PROJECT_INCLUDES_OLD)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE lio)
set(LSTORE_PROJECT_INCLUDES
        lio/authn.h
        lio/blacklist.h
        lio/cache.h
        lio/data_block.h
        lio/ds.h
        lio/erasure_tools.h
        lio/ex3.h
        lio/ex3_header.h
        lio/ex3_system.h
        lio/ex3_types.h
        lio/lio.h
        lio/lio_fuse.h
        lio/lio_visibility.h
        lio/os.h
        lio/rs.h
        lio/segment.h
        lio/service_manager.h
        lio/trace.h
)

set(LSTORE_PROJECT_EXECUTABLES
		ds_read
		ex_clone
		ex_get
		ex_inspect
		ex_load
		ex_put
		ldiff
		lio_cp
		lio_du
		lio_find
		lio_fsck
		lio_fuse
		lio_get
		lio_getattr
		lio_inspect
		lio_ln
		lio_ls
		lio_mkdir
		lio_mv
		lio_put
		lio_rm
		lio_rmdir
		lio_rs
		lio_server
		lio_setattr
		lio_signature
		lio_touch
		lio_warm
		mk_linear
		os_fsck
		zadler32
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
foreach(f erasure_tools.c exnode.c lio_config.c bin/ds_read.c bin/ldiff.c)
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

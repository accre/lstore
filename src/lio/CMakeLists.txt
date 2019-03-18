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
    find_package(FUSE3)
    if (NOT HAS_FUSE3)
        find_package(FUSE REQUIRED)
    endif()
    find_package(LevelDB REQUIRED)
    include_directories(
        ${JERASURE_INCLUDE_DIR} ${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR}
        ${IBP_INCLUDE_DIR} ${FUSE_INCLUDE_DIR} ${LEVELDB_INCLUDE_DIR}
    )

    list(APPEND LIBS
        ${IBP_LIBRARY} ${GOP_LIBRARY} ${TOOLBOX_LIBRARY}
        ${JERASURE_LIBRARY} ${ZMQ_LIBRARY} ${FUSE_LIBRARIES} ${LEVELDB_LIBRARY}
    )
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64")
endif()

if (HAS_FUSE3)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAS_FUSE3")
endif()

set(LSTORE_LIBS toolbox
                gop
                ibp
                ${APR_LIBRARY}
                ${APRUTIL_LIBRARY}
                ${ZLIB_LIBRARY}
                ${OPENSSL_LIBRARIES}
                ${ZMQ_LIBRARY}
                ${FUSE_LIBRARY}
                ${JERASURE_LIBRARY}
                ${LEVELDB_LIBRARY}
                )
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR}
                    ${APRUTIL_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${ZMQ_INCLUDE_DIR}
                    ${ZLIB_INCLUDE_DIR}
                    ${FUSE_INCLUDE_DIR}
                    ${JERASURE_INCLUDE_DIR}
                    ${LEVELDB_INCLUDE_DIR})
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

# common objects
set(LSTORE_PROJECT_OBJS
		authn/fake.c
		blacklist.c
		cache/amp.c
		cache/base.c
		cache/round_robin.c
		constructor.c
		cred_default.c
		data_block.c
		ds/ibp.c
        erasure_tools.c
		ex3.c
		ex3/compare.c
		ex3/global.c
		ex3/header.c
		ex3/id.c
		ex3/service.c
		lio_base_rw.c
		lio_config.c
		lio_core_io.c
		lio_core_misc.c
		lio_core_os.c
		lio_fuse_core.c
		lio_version.c
		os/base.c
		os/file.c
		os/remote_client.c
		os/remote_server.c
		os/test.c
		os/timecache.c
		osaz/fake.c
        raid4.c
        remote_config.c
		rs/query_base.c
		rs/remote_client.c
		rs/remote_server.c
		rs/simple.c
		rs/space.c
		rw_test.c
		segment.c
		segment/cache.c
		segment/dynfile.c
		segment/file.c
		segment/jerasure.c
		segment/linear.c
		segment/log.c
		segment/lun.c
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
        lio/ex3_fwd.h
        lio/lio.h
        lio/lio_fuse.h
        lio/os.h
        lio/rs.h
        lio/segment.h
        lio/service_manager.h
        lio/trace.h
        lio/version.h
        lio/visibility.h
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
		warmer_query
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
    if("${CMAKE_VERSION}" VERSION_GREATER 3.0.0)
        target_include_directories(trace_replay
                                    SYSTEM PRIVATE
                                    ${LSTORE_INCLUDE_SYSTEM})
    else()
        target_include_directories(trace_replay
                                    PRIVATE
                                    ${LSTORE_INCLUDE_SYSTEM})
    endif()
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

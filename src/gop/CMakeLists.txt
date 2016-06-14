cmake_minimum_required(VERSION 2.8)

if(POLICY CMP0048)
	cmake_policy(SET CMP0048 OLD)
endif()

# Name this project
set(LSTORE_PROJECT_NAME "gop")
project(${LSTORE_PROJECT_NAME} C CXX)

# Include modules
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
endif()
include(CheckIncludeFile)

# Detect compiler flags.
# No-op for GOP

# Find additional dependencies.
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
    find_package(ToolBox REQUIRED)
    find_package(ZMQ REQUIRED)
    find_package(CZMQ REQUIRED)
    include_directories(${TOOLBOX_INCLUDE_DIR} ${CZMQ_INCLUDE_DIR}
                        ${ZMQ_INCLUDE_DIR}
    )
    list(APPEND LIBS ${ZMQ_LIBRARY} ${CZMQ_LIBRARY} ${TOOLBOX_LIBRARY})
endif()

# common objects
set(LSTORE_PROJECT_OBJS
    callback.c constructor.c dummy.c  gop.c hconnection.c hportal.c opque.c
    thread_pool_config.c thread_pool_op.c mq_msg.c mq_zmq.c mq_portal.c
    mq_ongoing.c mq_stream.c mq_helpers.c
)
set(LSTORE_PROJECT_INCLUDES
        gop/callback.h
        gop/gop.h
        gop/gop_config.h
        gop/gop_visibility.h
        gop/host_portal.h
        gop/mq_helpers.h
        gop/mq_ongoing.h
        gop/mq_portal.h
        gop/mq_stream.h
        gop/mq_zmq.h
        gop/mqs_roundrobin.h
        gop/opque.h
        gop/portal.h
        gop/template.h
        gop/thread_pool.h
        gop/types.h)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE gop)

set(LSTORE_LIBS ${APR_LIBRARY}
                    ${APRUTIL_LIBRARY}
                    ${CZMQ_LIBRARY}
                    ${ZMQ_LIBRARY}
                    toolbox
    )
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR}
                    ${APRUTIL_INCLUDE_DIR}
                    ${ZMQ_INCLUDE_DIR}
                    ${CZMQ_INCLUDE_DIR}
    )
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR} ${toolbox_INCLUDE_DIR})

if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
else()
    include(LStoreFunctions)
    lstore_project_common(gop)
endif()

foreach(f ${LSTORE_PROJECT_EXECUTABLES})
    set_target_properties(${f} PROPERTIES COMPILE_FLAGS "-DLSTORE_HACK_EXPORT")
endforeach()


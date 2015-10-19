cmake_minimum_required(VERSION 2.8)
if(NOT (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 3.0))
	# only CMAKE 3.0 and greater supports this (NOT ... LESS because CMAKE does not have a GREATER_OR_EQUAL_TO conditional)
	# Not needed for older versions because older versions obviously use the old policy without specially requesting it
	cmake_policy(SET CMP0048 OLD)
	# ^^^ use the old VERSION variable behavior
endif()

# Name this project
set(LSTORE_PROJECT_NAME "gop")
project(${LSTORE_PROJECT_NAME} C CXX)

# Include modules 
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(CheckIncludeFile)

# Detect compiler flags.
# No-op for GOP

# Find additional dependencies.
find_package(ToolBox REQUIRED)
find_package(ZMQ REQUIRED)
find_package(CZMQ REQUIRED)
include_directories(${TOOLBOX_INCLUDE_DIR} ${CZMQ_INCLUDE_DIR}
                    ${ZMQ_INCLUDE_DIR}
)
list(APPEND LIBS ${ZMQ_LIBRARY} ${CZMQ_LIBRARY} ${TOOLBOX_LIBRARY})

# Additional Compiler flags.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DMQ_PIPE_COMM")

# common objects
set(LSTORE_PROJECT_OBJS 
    callback.c gop.c hconnection.c hportal.c opque.c thread_pool_config.c
    thread_pool_op.c mq_msg.c mq_zmq.c mq_portal.c mq_ongoing.c mq_stream.c
    mq_helpers.c mq_roundrobin.c
)

set(LSTORE_PROJECT_INCLUDES
    callback.h gop_config.h host_portal.h opque.h thread_pool.h mq_portal.h
    mq_helpers.h mq_stream.h mq_ongoing.h
)
if(NOT APPLE)
    # OSX doesn't have eventfd.h
    set(LSTORE_PROJECT_EXECUTABLES
        rr_mq_client rr_mq_server rr_mq_test rr_mq_worker
    )
endif(NOT APPLE)


# Common functionality is stored here
include(cmake/LStoreCommon.cmake)

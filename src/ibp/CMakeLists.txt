cmake_minimum_required(VERSION 2.8)

if(POLICY CMP0048)
	cmake_policy(SET CMP0048 OLD)
endif()

# Name this project
set(LSTORE_PROJECT_NAME "ibp")
project(${LSTORE_PROJECT_NAME} C)

# Include modules
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
endif()
include(CheckIncludeFile)

# Detect compiler flags.
# No-op for ibp

# Find additional dependencies.
if(NOT USE_SUPERBUILD)
    find_package(ToolBox REQUIRED)
    find_package(GOP REQUIRED)
    include_directories(${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR})
    list(APPEND LIBS ${TOOLBOX_LIBRARY} ${GOP_LIBRARY})
endif()
set(LSTORE_LIBS toolbox
                gop
                ${APR_LIBRARY}
    )
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR})
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

# Additional Compiler flags.
# No-op for ibp

# common objects
set(LSTORE_PROJECT_OBJS
    constructor.c
    config.c
    error.c
    misc.c
    op.c
    op_cmd.c
    types.c
    io_wrapper.c
    iovec_sync.c
)

set(LSTORE_PROJECT_INCLUDES_OLD)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE ibp)
set(LSTORE_PROJECT_INCLUDES
        ibp/ibp.h
        ibp/op.h
        ibp/protocol.h
        ibp/types.h
        ibp/visibility.h
    )

set(LSTORE_PROJECT_EXECUTABLES
    ibp_copyperf
    ibp_perf
)

# Common functionality is stored here

if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
else()
    include(LStoreFunctions)
    lstore_project_common(ibp)
endif()

foreach(f ${LSTORE_PROJECT_EXECUTABLES})
    set_target_properties(${f} PROPERTIES COMPILE_FLAGS "-DLSTORE_HACK_EXPORT")
endforeach()

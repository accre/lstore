cmake_minimum_required(VERSION 2.8)

if(POLICY CMP0048)
	cmake_policy(SET CMP0048 OLD)
endif()

# Name this project
set(LSTORE_PROJECT_NAME "toolbox")
project(${LSTORE_PROJECT_NAME} C)

# Include modules
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
endif()
include(CheckIncludeFile)

# Detect compiler flags.
check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)
configure_file(${PROJECT_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_config.h.in
               ${PROJECT_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_config.h)


# Find dependencies.
# common objects
set(NETWORK_OBJS
    net_sock.c network.c
)

set(TOOL_OBJS
    append_printf.c atomic_counter.c chksum.c constructor.c dns_cache.c
    iniparse.c interval_skiplist.c log.c packer.c pigeon_coop.c pigeon_hole.c
    random.c skiplist.c stack.c string_token.c transfer_buffer.c varint.c
)

set(LSTORE_PROJECT_OBJS ${TOOL_OBJS} ${NETWORK_OBJS})
set(LSTORE_PROJECT_INCLUDES
                tbx/apr_wrapper.h tbx/constructor_wrapper.h
                tbx/fmttypes.h tbx/interval_skiplist.h tbx/list.h
                tbx/network.h tbx/pigeon_hole.h tbx/stack.h tbx/net_sock.h
                tbx/pigeon_coop.h tbx/skiplist.h toolbox_config.h tbx/random.h
                tbx/string_token.h tbx/type_malloc.h tbx/transfer_buffer.h tbx/packer.h
                tbx/append_printf.h tbx/chksum.h tbx/varint.h tbx/atomic_counter.h tbx/dns_cache.h
                tbx/iniparse.h tbx/log.h tbx/assert_result.h tbx/visibility.h
                tbx/tbx_decl.h)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE tbx)
set(LSTORE_PROJECT_EXECUTABLES)
set(LSTORE_LIBS ${APR_LIBRARY}
                    ${APRUTIL_LIBRARY}
                    ${ZLIB_LIBRARY}
                    ${OPENSSL_LIBRARIES}
    )
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR}
                    ${APRUTIL_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${ZLIB_INCLUDE_DIR}
    )
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})


# Common functionality is stored here
if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
else()
    include(LStoreFunctions)
    lstore_project_common(toolbox)
endif()

cmake_minimum_required(VERSION 2.8)

if(POLICY CMP0048)
	cmake_policy(SET CMP0048 OLD)
endif()

# Name this project
set(LSTORE_PROJECT_NAME "toolbox")
project(${LSTORE_PROJECT_NAME} C CXX)

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
    net_1_ssl.c net_2_ssl.c net_fd.c net_phoebus.c net_sock.c network.c
    phoebus.c 
)

set(TOOL_OBJS  
    append_printf.c atomic_counter.c chksum.c dns_cache.c
    iniparse.c interval_skiplist.c log.c packer.c pigeon_coop.c pigeon_hole.c
    random.c skiplist.c stack.c string_token.c transfer_buffer.c varint.c
)

set(LSTORE_PROJECT_OBJS ${TOOL_OBJS} ${NETWORK_OBJS})
set(LSTORE_PROJECT_INCLUDES_OLD
                apr_wrapper.h constructor_wrapper.h debug.h list.h net_2_ssl.h
                fmttypes.h interval_skiplist.h net_1_ssl.h net_phoebus.h
                net_fd.h network.h pigeon_hole.h stack.h net_sock.h
                pigeon_coop.h skiplist.h toolbox_config.h phoebus.h random.h
                string_token.h type_malloc.h transfer_buffer.h packer.h
                append_printf.h chksum.h varint.h atomic_counter.h dns_cache.h
                iniparse.h log.h assert_result.h)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE tbx)
set(LSTORE_PROJECT_INCLUDES
        tbx/toolbox_visibility.h
    )
set(LSTORE_PROJECT_EXECUTABLES sl_test isl_test varint_test constructor_test)
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
set(LSTORE_INCLUDE_PUBLIC)


# Common functionality is stored here
if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
else()
    include(LStoreFunctions)
    lstore_project_common(toolbox)
endif()

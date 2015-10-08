cmake_minimum_required(VERSION 2.8)

# Name this project
set(LSTORE_PROJECT_NAME "toolbox")
project(${LSTORE_PROJECT_NAME} C CXX)

# Include modules 
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(CheckIncludeFile)

# Detect compiler flags.
check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_config.h.in
               ${CMAKE_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_config.h)


# Find dependencies.
# common objects
SET(NETWORK_OBJS 
    net_1_ssl.c net_2_ssl.c net_fd.c net_phoebus.c net_sock.c network.c
    phoebus.c 
)

SET(TOOL_OBJS  
    append_printf.c apr_wrapper.c atomic_counter.c chksum.c dns_cache.c
    iniparse.c interval_skiplist.c log.c packer.c pigeon_coop.c pigeon_hole.c
    random.c skiplist.c stack.c string_token.c transfer_buffer.c varint.c
)

SET(LSTORE_PROJECT_OBJS ${TOOL_OBJS} ${NETWORK_OBJS})
SET(LSTORE_PROJECT_INCLUDES
                apr_wrapper.h debug.h list.h net_2_ssl.h fmttypes.h
                interval_skiplist.h net_1_ssl.h net_phoebus.h net_fd.h
                network.h pigeon_hole.h stack.h net_sock.h pigeon_coop.h
                skiplist.h toolbox_config.h phoebus.h random.h string_token.h
                type_malloc.h transfer_buffer.h packer.h append_printf.h
                chksum.h varint.h atomic_counter.h dns_cache.h iniparse.h log.h)


# Common functionality is extracted here
include(cmake/LStoreCommon.cmake)

target_link_libraries(sl_test  ${library_lib} ${LIBS})
target_link_libraries(isl_test  ${library_lib} ${LIBS})
target_link_libraries(varint_test ${library_lib} ${LIBS})
ADD_EXECUTABLE(sl_test sl_test.c)
ADD_EXECUTABLE(isl_test isl_test.c)
ADD_EXECUTABLE(varint_test varint_test.c)

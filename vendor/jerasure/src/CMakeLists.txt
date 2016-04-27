cmake_minimum_required(VERSION 2.6)

# make a project
project( jerasure C )
include(GNUInstallDirs)

set(JERASURE_VERSION_MAJOR 1)
set(JERASURE_VERSION_MINOR 2)
set(JERASURE_VERSION_PATCH 0)
set(JERASURE_VERSION_STRING ${JERASURE_VERSION_MAJOR}.${JERASURE_VERSION_MINOR}.${JERASURE_VERSION_PATCH})

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -Wall -g" )
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -Wall -g" )

set(CMAKE_INCLUDE_CURRENT_DIR on)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

set(JERASURE_OBJS jerasure.c galois.c reed_sol.c cauchy.c liberation.c ) 

set(LIBS pthread m)

if(WANT_SHARED)
    add_library(jerasure SHARED ${JERASURE_OBJS})
    set_target_properties(jerasure PROPERTIES VERSION ${JERASURE_VERSION_STRING} SOVERSION ${JERASURE_VERSION_MAJOR})
    set_target_properties(jerasure PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    install(TARGETS jerasure DESTINATION lib)
endif()

if(WANT_STATIC)
    add_library(jerasure-static STATIC ${JERASURE_OBJS})
    set_target_properties(jerasure-static PROPERTIES OUTPUT_NAME "jerasure" )
    set_target_properties(jerasure-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    install(TARGETS jerasure-static DESTINATION lib)
endif()

install(FILES jerasure.h galois.h cauchy.h reed_sol.h liberation.h DESTINATION include/jerasure)

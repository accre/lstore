cmake_minimum_required(VERSION 2.6)

# make a project
project( jerasure C )

# Set some cmake defaults
#set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_BUILD_TYPE "Release")

set(JERASURE_VERSION_MAJOR 1)
set(JERASURE_VERSION_MINOR 2)
set(JERASURE_VERSION_PATCH 0)
set(JERASURE_VERSION_STRING ${JERASURE_VERSION_MAJOR}.${JERASURE_VERSION_MINOR}.${JERASURE_VERSION_PATCH})

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_REQUIRED_FLAGS} -O0 -Wall -g" )
set(CMAKE_C_FLAGS_RELEASE "-O ${CMAKE_REQUIRED_FLAGS} -O3 -Wall -g" )

set(CMAKE_INCLUDE_CURRENT_DIR on)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

include(GNUInstallDirs)
# ^^^ fallback needed or does this module handle non GNU OSes gracefully?

#check_include_file("stdint.h" HAVE_STDINT_H)
#check_include_file("inttypes.h" HAVE_INTTYPES_H)

set(JERASURE_OBJS jerasure.c galois.c reed_sol.c cauchy.c liberation.c ) 

#message("proto src is ${PROTO_SRC}")
#ADD_EXECUTABLE( trace_replay trace_replay.c trace.c ${IBP_OBJS} ${EX3_OBJS} ${TP_OBJS} )


set(LIBS pthread m)
add_library(jerasure SHARED ${JERASURE_OBJS})
add_library(jerasure-static STATIC ${JERASURE_OBJS})
 
set_target_properties(jerasure-static PROPERTIES OUTPUT_NAME "jerasure" )
set_target_properties(jerasure-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties(jerasure PROPERTIES VERSION ${JERASURE_VERSION_STRING} SOVERSION ${JERASURE_VERSION_MAJOR})
set_target_properties(jerasure PROPERTIES CLEAN_DIRECT_OUTPUT 1)

#Make static executables by default
#SET(ibp_lib ibp-static)
#To use shared libs
#SET(ibp_lib ibp)
#TARGET_LINK_LIBRARIES( ibp_perf ${ibp_lib} ${LIBS})

configure_file(jerasure.pc.in jerasure.pc @ONLY) 

install(TARGETS jerasure jerasure-static DESTINATION lib)
install(FILES jerasure.h galois.h cauchy.h reed_sol.h liberation.h DESTINATION include/jerasure)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/jerasure.pc" DESTINATION lib/pkgconfig)

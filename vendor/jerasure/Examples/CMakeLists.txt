cmake_minimum_required(VERSION 2.6)

# make a project
project( jerasure C )

# Set some cmake defaults
#set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_BUILD_TYPE "Release")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_REQUIRED_FLAGS} -O0 -Wall -g" )
set(CMAKE_C_FLAGS_RELEASE "-O ${CMAKE_REQUIRED_FLAGS} -O3 -Wall" )

set(CMAKE_INCLUDE_CURRENT_DIR on)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

#check_include_file("stdint.h" HAVE_STDINT_H)
#check_include_file("inttypes.h" HAVE_INTTYPES_H)


#Prefer to use static libs
if(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
else(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
endif(WIN32)

find_package(Jerasure REQUIRED)
SET(LIBS ${JERASURE_LIBRARY})
INCLUDE_DIRECTORIES(${JERASURE_INCLUDE_DIR})

SET(EXEC 
    jerasure_01 jerasure_02 jerasure_03 jerasure_04 jerasure_05 jerasure_06 jerasure_07 jerasure_08
    reed_sol_01 reed_sol_02 reed_sol_03 reed_sol_04
    cauchy_01 cauchy_02 cauchy_03 cauchy_04
    encoder decoder
)

FOREACH(ex ${EXEC})
   ADD_EXECUTABLE(${ex} ${ex}.c )
   TARGET_LINK_LIBRARIES( ${ex} ${LIBS})
   SET_PROPERTY(TARGET ${ex} PROPERTY LINK_SEARCH_START_STATIC 1)  
   SET_PROPERTY(TARGET ${ex} PROPERTY LINK_SEARCH_END_STATIC 1)  
ENDFOREACH(ex)


#ADD_EXECUTABLE(jerasure_01 jerasure_01.c )
#TARGET_LINK_LIBRARIES( jerasure_01 ${LIBS})
#SET_PROPERTY(TARGET jerasure_01 PROPERTY LINK_SEARCH_START_STATIC 1)
#ADD_EXECUTABLE(jerasure_02 jerasure_02.c )
#TARGET_LINK_LIBRARIES( jerasure_02 ${LIBS})
#ADD_EXECUTABLE(jerasure_03 jerasure_03.c )
#TARGET_LINK_LIBRARIES( jerasure_03 ${LIBS})
#ADD_EXECUTABLE(jerasure_04 jerasure_04.c )
#TARGET_LINK_LIBRARIES( jerasure_04 ${LIBS})
#ADD_EXECUTABLE(jerasure_05 jerasure_05.c )
#TARGET_LINK_LIBRARIES( jerasure_05 ${LIBS})
#ADD_EXECUTABLE(jerasure_06 jerasure_06.c )
#TARGET_LINK_LIBRARIES( jerasure_06 ${LIBS})
#ADD_EXECUTABLE(jerasure_07 jerasure_07.c )
#TARGET_LINK_LIBRARIES( jerasure_07 ${LIBS})
#ADD_EXECUTABLE(jerasure_08 jerasure_08.c )
#TARGET_LINK_LIBRARIES( jerasure_08 ${LIBS})

#ADD_EXECUTABLE(reed_sol_01 reed_sol_01.c )
#TARGET_LINK_LIBRARIES( reed_sol_01 ${LIBS})
#ADD_EXECUTABLE(reed_sol_02 reed_sol_02.c )
#TARGET_LINK_LIBRARIES( reed_sol_02 ${LIBS})
#ADD_EXECUTABLE(reed_sol_03 reed_sol_03.c )
#TARGET_LINK_LIBRARIES( reed_sol_03 ${LIBS})
#ADD_EXECUTABLE(reed_sol_04 reed_sol_04.c )
#TARGET_LINK_LIBRARIES( reed_sol_04 ${LIBS})

#ADD_EXECUTABLE(cauchy_01 cauchy_01.c )
#TARGET_LINK_LIBRARIES( cauchy_01 ${LIBS})
#ADD_EXECUTABLE(cauchy_02 cauchy_02.c )
#TARGET_LINK_LIBRARIES( cauchy_02 ${LIBS})
#ADD_EXECUTABLE(cauchy_03 cauchy_03.c )
#TARGET_LINK_LIBRARIES(cauchy_03 ${LIBS})
#ADD_EXECUTABLE(cauchy_04 cauchy_04.c )
#TARGET_LINK_LIBRARIES(cauchy_04 ${LIBS})

#ADD_EXECUTABLE(encoder encoder.c )
#TARGET_LINK_LIBRARIES(encoder ${LIBS})
#ADD_EXECUTABLE(decoder encoder.c )
#TARGET_LINK_LIBRARIES(decoder ${LIBS})




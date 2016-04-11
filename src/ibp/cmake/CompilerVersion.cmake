MACRO (CompilerVersion RESULT)
    if ("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
       execute_process(COMMAND ${CMAKE_C_COMPILER} "--version" OUTPUT_VARIABLE tmp )
       string(REGEX REPLACE "[\r]*[\n].*" "" ${RESULT} ${tmp} )
    else ("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
       set(${RESULT} ${CMAKE_C_COMPILE})
    endif ("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")

#    message(STATUS "gcc version: ${COMPILER_VERSION}")
ENDMACRO (CompilerVersion)


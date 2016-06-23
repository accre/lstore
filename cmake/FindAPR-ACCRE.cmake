# Source: http://svn.trolocsis.com/repos/projects/templates/apr/build/FindAPR.cmake
# Locate APR include paths and libraries

# Options
# APR_USE_SYSTEM - if true, use system and not the "-ACCRE" version of APR

# This module defines
# APR_INCLUDES, where to find apr.h, etc.
# APR_LIBS, linker switches to use with ld to link against APR
# APR_EXTRALIBS, additional libraries to link against
# APR_CFLAGS, the flags to use to compile
# APR_FOUND, set to TRUE if found, FALSE otherwise
# APR_VERSION, the version of APR that was found
cmake_policy(PUSH)
if(NOT (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 3.1))
	# only CMAKE 3.1 and greater supports this (NOT ... LESS because CMAKE does not have a GREATER_OR_EQUAL_TO conditional)
	# Not needed for older versions because older versions obviously use the old policy without specially requesting it
	cmake_policy(SET CMP0054 OLD)
endif()

if(APR_USE_SYSTEM)
    message(STATUS "Using non -ACCRE version of APR")
    set(__APR_SUFFIX_UPPER)
    set(__APR_SUFFIX_LOWER)
else()
    message(STATUS "Using -ACCRE version of APR")
    set(__APR_SUFFIX_UPPER "-ACCRE")
    set(__APR_SUFFIX_LOWER "-accre")
endif()

set(APR-ACCRE_FOUND FALSE)
find_program(APR_CONFIG_EXECUTABLE "apr${__APR_SUFFIX_UPPER}-1-config")
if(${APR_CONFIG_EXECUTABLE} STREQUAL "APR_CONFIG_EXECUTABLE-NOTFOUND")
    cmake_policy(POP)
    unset(__APR_SUFFIX_UPPER)
    unset(__APR_SUFFIX_LOWER)
    return()
endif()
mark_as_advanced(APR_CONFIG_EXECUTABLE)

macro(_apr_invoke _varname _regexp)
    execute_process(
        COMMAND ${APR_CONFIG_EXECUTABLE} ${ARGN}
        OUTPUT_VARIABLE _apr_output
        RESULT_VARIABLE _apr_failed
    )

    if(_apr_failed)
        message(FATAL_ERROR "${APR_CONFIG_EXECUTABLE} ${ARGN} failed")
    else()
        string(REGEX REPLACE "[\r\n]"  "" _apr_output "${_apr_output}")

        if(NOT ${_regexp} STREQUAL "")
            string(REGEX REPLACE "${_regexp}" " " _apr_output "${_apr_output}")
        endif()

        # trim string after processing the _regexp arg because it may introduce a <space> at the ends of the string
        string(REGEX REPLACE " +$"     "" _apr_output "${_apr_output}")
        string(REGEX REPLACE "^ +"     "" _apr_output "${_apr_output}")

        # XXX: We don't want to invoke separate_arguments() for APR_CFLAGS;
        # just leave as-is
        if(NOT ${_varname} STREQUAL "APR_CFLAGS")
            separate_arguments(_apr_output)
        endif(NOT ${_varname} STREQUAL "APR_CFLAGS")

        set(${_varname} "${_apr_output}")
    endif()
endmacro(_apr_invoke)

_apr_invoke(APR_CFLAGS    ""        --cppflags --cflags)
_apr_invoke(APR_INCLUDES  "(^| )-I" --includes)
_apr_invoke(APR_LIBS      ""        --link-ld)
_apr_invoke(APR_LIBTOOL   "(^| )-L" --link-libtool)
_apr_invoke(APR_EXTRALIBS "(^| )-l" --libs)
_apr_invoke(APR_VERSION   ""        --version)

list(GET APR_LIBTOOL 0 APR_LIBTOOL_ARG0)

get_filename_component(APR_LIBTOOL_BASE ${APR_LIBTOOL_ARG0} PATH ) 
FIND_LIBRARY(APR_LIBRARY NAMES "apr${__APR_SUFFIX_LOWER}-1" "apr${__APR_SUFFIX_UPPER}-1" PATHS ${APR_LIBTOOL_BASE})

# compatibility, allow this CMake module to work with the various CMakeList.txt files
set(APR_INCLUDE_DIR "${APR_INCLUDES}")
if(APR_LIBRARY AND APR_INCLUDES)
    set(APR-ACCRE_FOUND 1)
endif()
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(APR DEFAULT_MSG APR_INCLUDES APR_LIBRARY APR_LIBS APR_VERSION)
cmake_policy(POP)
unset(__APR_SUFFIX_UPPER)
unset(__APR_SUFFIX_LOWER)

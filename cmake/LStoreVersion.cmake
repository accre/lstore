# Try and best extract the LStore version using git or falling back to a static
# file

# Not sure what the meaning of the revision is
set(CPACK_PACKAGE_RELEASE 1)

find_program(GIT_EXECUTABLE git)
if(NOT GIT_EXECUTABLE)
    message(STATUS "Git not found, version can't be extracted")
endif()

# Extract from git
if(NOT LSTORE_VERSION AND GIT_EXECUTABLE)
    message(STATUS "Detecting version from git: ${CMAKE_SOURCE_DIR}")
    execute_process(COMMAND "${GIT_EXECUTABLE}" describe --abbrev=32 "--dirty=-dev" --candidates=100 --match "v[0-9].[0-9].[0-9]*"
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        OUTPUT_VARIABLE LSTORE_GITVERSION_RAW
        RESULT_VARIABLE GIT_RESULT
        ERROR_QUIET
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(GIT_RESULT EQUAL 0)
        string(REGEX REPLACE "v(.*)" "\\1" LSTORE_VERSION "${LSTORE_GITVERSION_RAW}")
        set(LSTORE_GITVERSION 1)
        message(STATUS "Detecting version from git - Success")
    else()
        message(STATUS "Detecting version from git - FAILED")
        set(LSTORE_GITVERSION 0)
    endif()
else()
    set(LSTORE_GITVERSION 0)
endif()

# Otherwise extract from the source tree
set(LSTORE_VERSION_TMPL "${CMAKE_SOURCE_DIR}/src/lio/lio/version.h.in")
set(LSTORE_VERSION_FILE "${CMAKE_SOURCE_DIR}/src/lio/lio/version.h")
set(LSTORE_DISTRIBUTION_VERSION_FILE "${CMAKE_SOURCE_DIR}/VERSION")
file(STRINGS "${LSTORE_DISTRIBUTION_VERSION_FILE}" LSTORE_DISTRIBUTION_VERSION
        REGEX "^[^#].*"
        LIMIT_COUNT 1
    )
if(NOT LSTORE_VERSION AND NOT LSTORE_GITVERSION)
    set(LSTORE_VERSION "${LSTORE_DISTRIBUTION_VERSION}")
    set(LSTORE_DISTVERSION 1)
else()
    set(LSTORE_DISTVERSION 0)
endif()

if(NOT LSTORE_VERSION)
    message(FATAL_ERROR "Could not detect the LStore version")
endif()
string(REGEX REPLACE "-" "_" LSTORE_VERSION "${LSTORE_VERSION}")
string(REGEX REPLACE "([0-9]+).([0-9]+).(.*)" "\\1" LSTORE_VERSION_MAJOR "${LSTORE_VERSION}")
string(REGEX REPLACE "([0-9]+).([0-9]+).(.*)" "\\2" LSTORE_VERSION_MINOR "${LSTORE_VERSION}")
string(REGEX REPLACE "([0-9]+).([0-9]+).(.*)" "\\3" LSTORE_VERSION_PATCH "${LSTORE_VERSION}")

# create a dependency on version file
# we never use output of the following command but cmake will rerun automatically if the version file changes

set(CPACK_PACKAGE_VERSION_MAJOR "${LSTORE_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${LSTORE_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${LSTORE_VERSION_PATCH}")
set(CPACK_PACKAGE_VERSION "${LSTORE_VERSION}")

message(STATUS "LStore version: ${LSTORE_VERSION}")

configure_file("${LSTORE_VERSION_TMPL}" "${LSTORE_VERSION_FILE}")

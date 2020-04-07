# Find the FUSE3 includes and library
#
#  FUSE_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE_LIBRARIES   - List of libraries when using FUSE.
#  FUSE3_FOUND      - True if FUSE3 header and libs are found used by CMake 
#  HAS_FUSE3        - True if FUSE3 found and intended for use by the compiler

# check if already in cache, be silent
IF (FUSE_INCLUDE_DIR)
        SET (FUSE_FIND_QUIETLY TRUE)
ENDIF (FUSE_INCLUDE_DIR)

# find includes
FIND_PATH (FUSE_INCLUDE_DIR fuse3/fuse.h
  HINTS /usr/local/include
        /usr/local/include
        /usr/include
)

# find lib
if (APPLE)
    SET(FUSE_NAMES libosxfuse.dylib fuse)
else (APPLE)
    SET(FUSE_NAMES fuse3)
endif (APPLE)
FIND_LIBRARY(FUSE_LIBRARIES
        NAMES ${FUSE_NAMES}
)

SET(FUSE_LIBRARY ${FUSE_LIBRARIES})

if (FUSE_LIBRARY AND FUSE_INCLUDE_DIR)
    SET(FUSE3_FOUND 1)
    SET(HAS_FUSE3 1)
endif (FUSE_LIBRARY AND FUSE_INCLUDE_DIR)

if (NOT FUSE3_FOUND)
#   message(STATUS "Found FUSE3: ${FUSE_LIBRARY} ${FUSE_INCLUDE_DIR}")
#else
   message(STATUS "Could not find FUSE3 library: LIB=${FUSE_LIBRARY} INC=${FUSE_INCLUDE_DIR}")
endif()

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("FUSE3" DEFAULT_MSG
    FUSE_INCLUDE_DIR FUSE_LIBRARIES)

mark_as_advanced (FUSE_INCLUDE_DIR FUSE_LIBRARIES)

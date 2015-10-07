# Find the FUSE includes and library
#
#  FUSE_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE_LIBRARIES   - List of libraries when using FUSE.
#  FUSE_FOUND       - True if FUSE lib is found.

# check if already in cache, be silent
IF (FUSE_INCLUDE_DIR)
        SET (FUSE_FIND_QUIETLY TRUE)
ENDIF (FUSE_INCLUDE_DIR)

# find includes
FIND_PATH (FUSE_INCLUDE_DIR fuse.h
        /usr/local/include/osxfuse
        /usr/local/include
        /usr/include
)

# find lib
if (APPLE)
    SET(FUSE_NAMES libosxfuse.dylib fuse)
else (APPLE)
    SET(FUSE_NAMES fuse)
endif (APPLE)
FIND_LIBRARY(FUSE_LIBRARIES
        NAMES ${FUSE_NAMES}
        PATHS /lib64 /lib /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)
SET(FUSE_LIBRARY ${FUSE_LIBRARIES})

if (FUSE_LIBRARY AND FUSE_INCLUDE_DIR)
    SET(FUSE_FOUND 1)
endif (FUSE_LIBRARY AND FUSE_INCLUDE_DIR)

if (FUSE_FOUND)
   message(STATUS "Found FUSE: ${FUSE_LIBRARY} ${FUSE_INCLUDE_DIR}")
else (FUSE_FOUND)
   message(STATUS "Could not find FUSE library")
endif (FUSE_FOUND)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("FUSE" DEFAULT_MSG
    FUSE_INCLUDE_DIR FUSE_LIBRARIES)

mark_as_advanced (FUSE_INCLUDE_DIR FUSE_LIBRARIES)


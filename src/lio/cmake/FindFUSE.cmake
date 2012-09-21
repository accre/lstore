# -*- cmake -*-

# - Find FUSE libraries and includes
#
# This module defines
#    FUSE_INCLUDE_DIR - where to find header files
#    FUSE_LIBRARIES - the libraries needed to use FUSE.
#    FUSE_FOUND - If false didn't find FUSE

# Find the include path
find_path(fuse_inc fuse/fuse_lowlevel.h)

if (fuse_inc)
   find_path(FUSE_INCLUDE_DIR fuse_lowlevel.h ${fuse_inc}/fuse)
else (fuse_inc)
   find_path(FUSE_INCLUDE_DIR fuse_lowlevel.h)
endif (fuse_inc)

find_library(FUSE_LIBRARY NAMES fuse)

if (FUSE_LIBRARY AND FUSE_INCLUDE_DIR)
    SET(FUSE_FOUND "YES")
endif (FUSE_LIBRARY AND FUSE_INCLUDE_DIR)


if (FUSE_FOUND)
   message(STATUS "Found FUSE: ${FUSE_LIBRARY} ${FUSE_INCLUDE_DIR}")
else (FUSE_FOUND)
   message(STATUS "Could not find FUSE library")
endif (FUSE_FOUND)


MARK_AS_ADVANCED(
  FUSE_LIBRARY
  FUSE_INCLUDE_DIR
  FUSE_FOUND
)


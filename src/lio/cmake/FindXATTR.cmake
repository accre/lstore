# -*- cmake -*-

# - Find XATTR libraries and includes
#
# This module defines
#    XATTR_INCLUDE_DIR - where to find header files
#    XATTR_LIBRARIES - the libraries needed to use ZATTR
#    XATTR_FOUND - If false didn't find XATTR

# Find the include path
find_path(xattr_inc attr/xattr.h)

if (xattr_inc)
   find_path(XATTR_INCLUDE_DIR xattr.h ${xattr_inc}/attr)
endif (xattr_inc)

find_library(XATTR_LIBRARY NAMES attr PATHS /usr/lib64 /usr/lib /usr/local/lib)

if (XATTR_LIBRARY AND XATTR_INCLUDE_DIR)
    SET(XATTR_FOUND "-DHAVE_XATTR")
endif (XATTR_LIBRARY AND XATTR_INCLUDE_DIR)


if (XATTR_FOUND)
   message(STATUS "Found XATTR: ${XATTR_LIBRARY} ${XATTR_INCLUDE_DIR}")
else (XATTR_FOUND)
   message(STATUS "Could not find XATTR library")
endif (XATTR_FOUND)


MARK_AS_ADVANCED(
  XATTR_LIBRARY
  XATTR_INCLUDE_DIR
  XATTR_FOUND
)


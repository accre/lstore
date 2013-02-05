# -*- cmake -*-

# - Find HWloc libraries and includes
#
# This module defines
#    HWLOC_INCLUDE_DIR - where to find hwloc header
#    HWLOC_LIBRARIES - the libraries needed to use hwloc.
#    HWLOC_FOUND - If false didn't find hwloc installed

# Find the include path

find_path(HWLOC_INCLUDE_DIR hwloc.h)

find_library(HWLOC_LIBRARY NAMES hwloc)

if (HWLOC_LIBRARY AND HWLOC_INCLUDE_DIR)
    SET(HWLOC_FOUND "YES")
endif (HWLOC_LIBRARY AND HWLOC_INCLUDE_DIR)


if (HWLOC_FOUND)
   message(STATUS "Found HWLOC: ${HWLOC_LIBRARY}")
else (HWLOC_FOUND)
   message(STATUS "Could not find HWLOC library")
endif (HWLOC_FOUND)


MARK_AS_ADVANCED(
  HWLOC_LIBRARY
  HWLOC_INCLUDE_DIR
  HWLOC_FOUND
)


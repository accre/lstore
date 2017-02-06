# -*- cmake -*-

# - Find GOP libraries and includes
#
# This module defines
#    GOP_INCLUDE_DIR - where to find GOP header files
#    GOP_LIBRARIES - the libraries needed to use GOP.
#    GOP_FOUND - If false didn't find GOP

# Find the include path
find_path(gop_inc gop/gop_config.h)

if (gop_inc)
   find_path(GOP_INCLUDE_DIR gop_config.h ${gop_inc}/gop)
endif (gop_inc)

find_library(GOP_LIBRARY NAMES gop)

if (GOP_LIBRARY AND GOP_INCLUDE_DIR)
    SET(GOP_FOUND "YES")
endif (GOP_LIBRARY AND GOP_INCLUDE_DIR)


if (GOP_FOUND)
   message(STATUS "Found GOP: ${GOP_LIBRARY} ${GOP_INCLUDE_DIR}")
else (GOP_FOUND)
   message(STATUS "Could not find GOP library")
endif (GOP_FOUND)


MARK_AS_ADVANCED(
  GOP_LIBRARY
  GOP_INCLUDE_DIR
  GOP_FOUND
)


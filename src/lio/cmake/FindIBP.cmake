# -*- cmake -*-

# - Find IBP libraries and includes
#
# This module defines
#    IBP_INCLUDE_DIR - where to find header files
#    IBP_LIBRARIES - the libraries needed to use IBP.
#    IBP_FOUND - If false didn't find IBP

# Find the include path
find_path(ibp_inc ibp/ibp.h)

if (ibp_inc)
   find_path(IBP_INCLUDE_DIR ibp.h ${ibp_inc}/ibp)
endif (ibp_inc)

find_library(IBP_LIBRARY NAMES ibp)

if (IBP_LIBRARY AND IBP_INCLUDE_DIR)
    SET(IBP_FOUND "YES")
endif (IBP_LIBRARY AND IBP_INCLUDE_DIR)


if (IBP_FOUND)
   message(STATUS "Found IBP: ${IBP_LIBRARY} ${IBP_INCLUDE_DIR}")
else (IBP_FOUND)
   message(STATUS "Could not find IBP library")
endif (IBP_FOUND)


MARK_AS_ADVANCED(
  IBP_LIBRARY
  IBP_INCLUDE_DIR
  IBP_FOUND
)


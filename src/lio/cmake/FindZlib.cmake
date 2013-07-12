# - Find zlib
# Find the native ZLIB includes and library
#
# ZLIB_INCLUDE_DIRS - where to find zlib.h, etc.
# ZLIB_LIBRARIES - List of libraries when using zlib.
# ZLIB_FOUND - True if zlib found.

#=============================================================================
# Copyright 2001-2009 Kitware, Inc.
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distributed this file outside of CMake, substitute the full
# License text for the above reference.)


# Find the include path
find_path(ZLIB_INCLUDE_DIR zlib.h)

find_library(ZLIB_LIBRARY NAMES z zlib zdll)

if (ZLIB_LIBRARY AND ZLIB_INCLUDE_DIR)
    SET(ZLIB_FOUND "YES")
endif (ZLIB_LIBRARY AND ZLIB_INCLUDE_DIR)


if (ZLIB_FOUND)
   message(STATUS "Found Zlib: ${ZLIB_LIBRARY} ${ZLIB_INCLUDE_DIR}")
else (ZLIB_FOUND)
   message(STATUS "Could not find ZLIB library")
endif (ZLIB_FOUND)


MARK_AS_ADVANCED(
  ZLIB_LIBRARY
  ZLIB_INCLUDE_DIR
  ZLIB_FOUND
)

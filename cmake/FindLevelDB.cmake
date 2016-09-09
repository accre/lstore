# -*- cmake -*-

# - Find LevelDB libraries and C includes
#
# This module defines
#    LEVELDB_INCLUDE_DIR - where to find the header files
#    LEVELDB_LIBRARY - the libraries needed.
#    LEVELDB_FOUND - If false didn't find LevelDB

# Find the include path
find_path(LEVELDB_INCLUDE_DIR leveldb/c.h)

find_library(LEVELDB_LIBRARY NAMES leveldb)

if (LEVELDB_LIBRARY AND LEVELDB_INCLUDE_DIR)
    SET(LEVELDB_FOUND "YES")
endif ()


if (LEVELDB_FOUND)
   message(STATUS "Found LevelDB: ${LEVELDB_LIBRARY} ${LEVELDB_INCLUDE_DIR}")
else ()
   message(STATUS "Could not find LevelDB library")
endif ()


MARK_AS_ADVANCED(
  LEVELDB_LIBRARY
  LEVELDB_INCLUDE_DIR
  LEVELDB_FOUND
)


# -*- cmake -*-

# - Find BerkeleyDB libraries and includes
#
# This module defines
#    BerkeleyDB_INCLUDE_DIR - where to find header files
#    BerkeleyDB_LIBRARIES - the libraries needed to use ZMQ.
#    BerkeleyDB_FOUND - If false didn't find ZMQ

# Find the include path
find_path(BERKELEYDB_inc db.h)

if (BERKELEYDB_inc)
   find_path(BERKELEYDB_INCLUDE_DIR db.h)
endif (BERKELEYDB_inc)

find_library(BERKELEYDB_LIBRARY NAMES db)

if (BERKELEYDB_LIBRARY AND BERKELEYDB_INCLUDE_DIR)
    SET(BERKELEYDB_FOUND "YES")
endif (BERKELEYDB_LIBRARY AND BERKELEYDB_INCLUDE_DIR)


if (BERKELEYDB_FOUND)
   message(STATUS "Found BerkeleyDB: ${BERKELEYDB_LIBRARY} ${BERKELEYDB_INCLUDE_DIR}")
else (BERKELEYDB_FOUND)
   message(STATUS "Could not find BerkeleyDB library")
endif (BERKELEYDB_FOUND)


MARK_AS_ADVANCED(
  BERKELEYDB_LIBRARY
  BERKELEYDB_INCLUDE_DIR
  BERKELEYDB_FOUND
)


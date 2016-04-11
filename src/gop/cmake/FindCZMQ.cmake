# -*- cmake -*-

# - Find CZMQ libraries and includes
#
# This module defines
#    CZMQ_INCLUDE_DIR - where to find header files
#    CZMQ_LIBRARIES - the libraries needed to use ZMQ.
#    CZMQ_FOUND - If false didn't find ZMQ

# Find the include path
find_path(czmq_inc czmq.h)

if (czmq_inc)
   find_path(CZMQ_INCLUDE_DIR czmq.h ${zmq_inc}/czmq)
endif (czmq_inc)

find_library(CZMQ_LIBRARY NAMES czmq)

if (CZMQ_LIBRARY AND CZMQ_INCLUDE_DIR)
    SET(CZMQ_FOUND "YES")
endif (CZMQ_LIBRARY AND CZMQ_INCLUDE_DIR)


if (CZMQ_FOUND)
   message(STATUS "Found CZMQ: ${CZMQ_LIBRARY} ${CZMQ_INCLUDE_DIR}")
else (CZMQ_FOUND)
   message(STATUS "Could not find CZMQ library")
endif (CZMQ_FOUND)


MARK_AS_ADVANCED(
  CZMQ_LIBRARY
  CZMQ_INCLUDE_DIR
  CZMQ_FOUND
)


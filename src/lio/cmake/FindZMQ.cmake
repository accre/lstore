# -*- cmake -*-

# - Find ZMQ libraries and includes
#
# This module defines
#    ZMQ_INCLUDE_DIR - where to find header files
#    ZMQ_LIBRARIES - the libraries needed to use ZMQ.
#    ZMQ_FOUND - If false didn't find ZMQ

# Find the include path
find_path(zmq_inc zmq.h)

if (zmq_inc)
   find_path(ZMQ_INCLUDE_DIR zmq.h ${zmq_inc}/zmq)
endif (zmq_inc)

find_library(ZMQ_LIBRARY NAMES zmq)

if (ZMQ_LIBRARY AND ZMQ_INCLUDE_DIR)
    SET(ZMQ_FOUND "YES")
endif (ZMQ_LIBRARY AND ZMQ_INCLUDE_DIR)


if (ZMQ_FOUND)
   message(STATUS "Found ZMQ: ${ZMQ_LIBRARY} ${ZMQ_INCLUDE_DIR}")
else (ZMQ_FOUND)
   message(STATUS "Could not find ZMQ library")
endif (ZMQ_FOUND)


MARK_AS_ADVANCED(
  ZMQ_LIBRARY
  ZMQ_INCLUDE_DIR
  ZMQ_FOUND
)


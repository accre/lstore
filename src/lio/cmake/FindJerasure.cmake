# -*- cmake -*-

# - Find Jerasure Libraries
# Find the Jerasure includes and libraries
# This module defines
#  JERASURE_INCLUDE_DIR and JERASURE_INCLUDE_DIR, where to find apr.h, etc.
#  JERASURE_LIBRARIES and JERASURE_LIBRARIES, the libraries needed to use APR.
#  JERASURE_FOUND and JERASURE_FOUND, If false, do not try to use APR.
# also defined, but not for general use are
#  JERASURE_LIBRARY and JERASURE_LIBRARY, where to find the APR library.

FIND_PATH(JERASURE_INCLUDE_DIR_BASE NAMES jerasure/jerasure.h
                    jerasure/galois.h 
                    jerasure/cauchy.h 
                    jerasure/reed_sol.h 
                    jerasure/liberation.h)
if(JERASURE_INCLUDE_DIR_BASE)
   SET(JERASURE_INCLUDE_DIR ${JERASURE_INCLUDE_DIR}/jerasure)
else(JERASURE_INCLUDE_DIR_BASE)
   FIND_PATH(JERASURE_INCLUDE_DIR NAMES jerasure/jerasure.h
                    jerasure/galois.h 
                    jerasure/cauchy.h 
                    jerasure/reed_sol.h 
                    jerasure/liberation.h)
endif(JERASURE_INCLUDE_DIR_BASE)


SET(JERASURE_NAMES jerasure Jerasure)
FIND_LIBRARY(JERASURE_LIBRARY NAMES ${JERASURE_NAMES})

IF (JERASURE_LIBRARY AND JERASURE_INCLUDE_DIR)
    SET(JERASURE_LIBRARIES ${JERASURE_LIBRARY})
    SET(JERASURE_FOUND "YES")
ELSE (JERASURE_LIBRARY AND JERASURE_INCLUDE_DIR)
  SET(JERASURE_FOUND "NO")
ENDIF (JERASURE_LIBRARY AND JERASURE_INCLUDE_DIR)


IF (JERASURE_FOUND)
   IF (NOT JERASURE_FIND_QUIETLY)
      MESSAGE(STATUS "Found JERASURE: ${JERASURE_LIBRARIES} ${JERASURE_INCLUDE_DIR}")
   ENDIF (NOT JERASURE_FIND_QUIETLY)
ELSE (JERASURE_FOUND)
   MESSAGE(STATUS "Could not find JERASURE library: ${JERASURE_LIBRARIES} ${JERASURE_INCLUDE_DIR}")
ENDIF (JERASURE_FOUND)

# Deprecated declarations.
SET (NATIVE_JERASURE_INCLUDE_PATH ${JERASURE_INCLUDE_DIR} )
GET_FILENAME_COMPONENT (NATIVE_JERASURE_LIB_PATH ${JERASURE_LIBRARY} PATH)

MARK_AS_ADVANCED(
  JERASURE_LIBRARY
  JERASURE_INCLUDE_DIR
)

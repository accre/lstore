# -*- cmake -*-

# - Find Apache Portable Runtime
# Find the APR includes and libraries
# This module defines
#  APRUTIL_INCLUDE_DIR and APRUTIL_INCLUDE_DIR, where to find apr.h, etc.
#  APRUTIL_LIBRARIES and APRUTIL_LIBRARIES, the libraries needed to use APR.
#  APRUTIL_FOUND and APRUTIL_FOUND, If false, do not try to use APR.
# also defined, but not for general use are
#  APRUTIL_LIBRARY and APRUTIL_LIBRARY, where to find the APR library.

# APR first.

# Find the *relative* include path
find_path(apr_inc apr-ACCRE-1/apr.h)

# Now convert it to the full path
if (apr_inc)     
   find_path(APRUTIL_INCLUDE_DIR apr.h ${apr_inc}/apr-ACCRE-1 )
else (apr_inc)
   find_path(APRUTIL_INCLUDE_DIR apr.h)
endif (apr_inc)

FIND_LIBRARY(APRUTIL_LIBRARY NAMES aprutil-ACCRE-1)

IF (APRUTIL_LIBRARY AND APRUTIL_INCLUDE_DIR)
    SET(APRUTIL_LIBRARIES ${APRUTIL_LIBRARY})
    SET(APRUTIL_FOUND "YES")
ELSE (APRUTIL_LIBRARY AND APRUTIL_INCLUDE_DIR)
  SET(APRUTIL_FOUND "NO")
ENDIF (APRUTIL_LIBRARY AND APRUTIL_INCLUDE_DIR)


IF (APRUTIL_FOUND)
   IF (NOT APRUTIL_FIND_QUIETLY)
      MESSAGE(STATUS "Found APRUtil-ACCRE: ${APRUTIL_LIBRARIES}")
   ENDIF (NOT APRUTIL_FIND_QUIETLY)
ELSE (APRUTIL_FOUND)
   IF (APRUTIL_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find APRUtil-ACCRE library")
   ENDIF (APRUTIL_FIND_REQUIRED)
ENDIF (APRUTIL_FOUND)

# Deprecated declarations.
SET (NATIVE_APRUTIL_INCLUDE_PATH ${APRUTIL_INCLUDE_DIR} )
GET_FILENAME_COMPONENT (NATIVE_APRUTIL_LIB_PATH ${APRUTIL_LIBRARY} PATH)

MARK_AS_ADVANCED(
  APRUTIL_LIBRARY
  APRUTIL_INCLUDE_DIR
  )

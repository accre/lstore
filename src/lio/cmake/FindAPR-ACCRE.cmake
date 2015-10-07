# -*- cmake -*-

# - Find Apache Portable Runtime
# Find the APR includes and libraries
# This module defines
#  APR_INCLUDE_DIR and APRUTIL_INCLUDE_DIR, where to find apr.h, etc.
#  APR_LIBRARIES and APRUTIL_LIBRARIES, the libraries needed to use APR.
#  APR_FOUND and APRUTIL_FOUND, If false, do not try to use APR.
# also defined, but not for general use are
#  APR_LIBRARY and APRUTIL_LIBRARY, where to find the APR library.

# APR first.

# Find the *relative* include path
find_path(apr_inc apr-1/apr.h)

# Now convert it to the full path
if (apr_inc)     
   find_path(APR_INCLUDE_DIR apr.h ${apr_inc}/apr-1 )
else (apr_inc)
   find_path(APR_INCLUDE_DIR apr.h)
endif (apr_inc)

FIND_LIBRARY(APR_LIBRARY NAMES apr-ACCRE-1)

IF (APR_LIBRARY AND APR_INCLUDE_DIR)
    SET(APR_LIBRARIES ${APR_LIBRARY})
    SET(APR_FOUND "YES")
ELSE (APR_LIBRARY AND APR_INCLUDE_DIR)
  SET(APR_FOUND "NO")
ENDIF (APR_LIBRARY AND APR_INCLUDE_DIR)


IF (APR_FOUND)
   IF (NOT APR_FIND_QUIETLY)
      MESSAGE(STATUS "Found APR-ACCRE: ${APR_LIBRARIES}")
   ENDIF (NOT APR_FIND_QUIETLY)
ELSE (APR_FOUND)
   IF (APR_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find APR-ACCRE library")
   ENDIF (APR_FIND_REQUIRED)
ENDIF (APR_FOUND)

# Deprecated declarations.
SET (NATIVE_APR_INCLUDE_PATH ${APR_INCLUDE_DIR} )
GET_FILENAME_COMPONENT (NATIVE_APR_LIB_PATH ${APR_LIBRARY} PATH)

MARK_AS_ADVANCED(
  APR_LIBRARY
  APR_INCLUDE_DIR
  )


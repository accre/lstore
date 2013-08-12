# Find the native XML2 includes and library
#
# XML2_INCLUDE_DIRS - where to find header files.
# XML2_LIBRARIES - List of libraries.
# XML2_FOUND - True if XML2 is found.


# Find the include path
find_path(XML2_INCLUDE_DIR libxml2/libxml/xpath.h)

find_library(XML2_LIBRARY NAMES xml2)

if (XML2_LIBRARY AND XML2_INCLUDE_DIR)
    SET(XML2_FOUND "YES")
endif (XML2_LIBRARY AND XML2_INCLUDE_DIR)


if (XML2_FOUND)
   message(STATUS "Found XML2: ${XML2_LIBRARY} ${XML2_INCLUDE_DIR}")
else (XML2_FOUND)
   message(STATUS "Could not find XML2 library")
endif (XML2_FOUND)


MARK_AS_ADVANCED(
  XML2_LIBRARY
  XML2_INCLUDE_DIR
  XML2_FOUND
)

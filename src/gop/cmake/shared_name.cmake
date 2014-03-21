#
#  Translates a full library path into the shared library name
#
MACRO (shared_name full_name shared)
  get_filename_component(lname ${full_name} NAME_WE)

  if (WIN32)
     string(SUBSTRING ${lname} 3 -1 bname)
     set(${shared} "${bname}.lib")
  elseif (APPLE)
     set(${shared} "${lname}.dylib")
  elseif (UNIX)
     set(${shared} "${lname}.so")
  else ()
     message(SEND_ERROR "Don't know how to mangle the shared library name for platform")
     set(${shared} "ERROR")
  endif ()

ENDMACRO (shared_name)


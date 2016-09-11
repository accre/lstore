
# - Try to find GLOBUS_COMMON
# Once done this will define
#  GLOBUS_COMMON_FOUND - System has globus_common
#  GLOBUS_COMMON_INCLUDE_DIRS - The globus_common include directories
#  GLOBUS_COMMON_LIBRARIES - The libraries needed to use globus_common
#  GLOBUS_COMMON_DEFINITIONS - Compiler switches required for using globus_common

find_package(PkgConfig)
pkg_check_modules(PC_GLOBUS_COMMON QUIET globus-common)
set(GLOBUS_COMMON_DEFINITIONS ${PC_GLOBUS_COMMON_CFLAGS_OTHER})

find_path(GLOBUS_COMMON_INCLUDE_DIR globus_common.h globus_config.h
          HINTS ${PC_GLOBUS_COMMON_INCLUDEDIR} ${PC_GLOBUS_COMMON_INCLUDE_DIRS}
          PATH_SUFFIXES globus )

find_library(GLOBUS_COMMON_LIBRARY NAMES globus_common
             HINTS ${PC_GLOBUS_COMMON_LIBDIR} ${PC_GLOBUS_COMMON_LIBRARY_DIRS} )

set(GLOBUS_COMMON_LIBRARIES ${GLOBUS_COMMON_LIBRARY} )
set(GLOBUS_COMMON_INCLUDE_DIRS ${GLOBUS_COMMON_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set GLOBUS_COMMON_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(GLOBUS_COMMON DEFAULT_MSG
                                  GLOBUS_COMMON_LIBRARY GLOBUS_COMMON_INCLUDE_DIR)

mark_as_advanced( GLOBUS_COMMON_FOUND GLOBUS_COMMON_INCLUDE_DIR GLOBUS_COMMON_LIBRARY )



# - Try to find GLOBUS_GRIDFTP_SERVER
# Once done this will define
#  GLOBUS_GRIDFTP_SERVER_FOUND - System has globus_gridftp_server
#  GLOBUS_GRIDFTP_SERVER_INCLUDE_DIRS - The globus_gridftp_server include directories
#  GLOBUS_GRIDFTP_SERVER_LIBRARIES - The libraries needed to use globus_gridftp_server
#  GLOBUS_GRIDFTP_SERVER_DEFINITIONS - Compiler switches required for using globus_gridftp_server

find_package(PkgConfig)
pkg_check_modules(PC_GLOBUS_GRIDFTP_SERVER QUIET globus-gridftp-server)
set(GLOBUS_GRIDFTP_SERVER_DEFINITIONS ${PC_GLOBUS_GRIDFTP_SERVER_CFLAGS_OTHER})

find_path(GLOBUS_GRIDFTP_SERVER_INCLUDE_DIR globus_gridftp_server.h
          HINTS ${PC_GLOBUS_GRIDFTP_SERVER_INCLUDEDIR} ${PC_GLOBUS_GRIDFTP_SERVER_INCLUDE_DIRS}
          PATH_SUFFIXES globus )

find_library(GLOBUS_GRIDFTP_SERVER_LIBRARY NAMES globus_gridftp_server
             HINTS ${PC_GLOBUS_GRIDFTP_SERVER_LIBDIR} ${PC_GLOBUS_GRIDFTP_SERVER_LIBRARY_DIRS} )

set(GLOBUS_GRIDFTP_SERVER_LIBRARIES ${GLOBUS_GRIDFTP_SERVER_LIBRARY} )
set(GLOBUS_GRIDFTP_SERVER_INCLUDE_DIRS ${GLOBUS_GRIDFTP_SERVER_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set GLOBUS_GRIDFTP_SERVER_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(GLOBUS_GRIDFTP_SERVER DEFAULT_MSG
                                  GLOBUS_GRIDFTP_SERVER_LIBRARY GLOBUS_GRIDFTP_SERVER_INCLUDE_DIR)

mark_as_advanced( GLOBUS_GRIDFTP_SERVER_FOUND GLOBUS_GRIDFTP_SERVER_INCLUDE_DIR GLOBUS_GRIDFTP_SERVER_LIBRARY )


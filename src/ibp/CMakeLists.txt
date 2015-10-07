cmake_minimum_required(VERSION 2.8)

# make a project
project( IBP C )

# Set some cmake defaults
set(CMAKE_BUILD_TYPE "Debug")
#set(CMAKE_BUILD_TYPE "Release")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_REQUIRED_FLAGS} -O0 -Wall -g -DHAVE_CONFIG_H -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE" )
set(CMAKE_C_FLAGS_RELEASE "-O ${CMAKE_REQUIRED_FLAGS} -DHAVE_CONFIG_H -DLINUX=2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE" )

set(CMAKE_INCLUDE_CURRENT_DIR on)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
INCLUDE(CheckIncludeFile)
include(${CMAKE_SOURCE_DIR}/cmake/Date.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerVersion.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/CompilerFlags.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/shared_name.cmake)

#Prefer to use static libs
if(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
else(WIN32)
 set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
endif(WIN32)

# get dependancies
find_package(OpenSSL REQUIRED)
find_package(APR-ACCRE REQUIRED)
find_package(APRUtil-ACCRE REQUIRED)
find_package(Phoebus)
find_package(ToolBox REQUIRED)
find_package(GOP REQUIRED)
find_package(ZLIB)

check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)

include_directories(${OPENSSL_INCLUDE_DIR} ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${PHOEBUS_INCLUDE_DIR} 
   ${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR} ${IBP_INCLUDE_DIR})
SET(LIBS ${LIBS} ${TOOLBOX_LIBRARY} ${GOP_LIBRARY} ${OPENSSL_LIBRARIES} ${CRYPTO_LIBRARIES} ${APR_LIBRARY} ${APRUTIL_LIBRARY} ${ZLIB_LIBRARY} gssapi_krb5 pthread m dl)

# config options
if (Phoebus_FOUND)
  set(PHOEBUS_OPT "-D_ENABLE_PHOEBUS")
  OPTION( _ENABLE_PHOEBUS "Enable Phoebus support" ON )
  include_directories(${PHOEBUS_INCLUDE_DIR})
  list(APPEND LIBS ${PHOEBUS_LIBRARIES})
else (Phoebus_FOUND)
  OPTION( _ENABLE_PHOEBUS "Enable Phoebus support" OFF )
endif (Phoebus_FOUND)


#Make the version file
set(IBP_CLIENT_VERSION "ibp_client: v1.1.0")
site_name(BUILD_HOST)
Date(BUILD_DATE)
CompilerVersion(COMPILER_VERSION)
CompilerFlags(COMPILER_FLAGS)
#configure_file("${PROJECT_SOURCE_DIR}/ibp_client_version.c.in" "${PROJECT_BINARY_DIR}/ibp_client_version.c")
configure_file("${PROJECT_SOURCE_DIR}/ibp_client_version.c.in" "${PROJECT_SOURCE_DIR}/ibp_client_version.c")

# common objects

SET(IBP_OBJS 
    ibp_config.c
    ibp_op.c 
    ibp_misc.c 
    ibp_types.c 
    ibp_sync.c 
    ibp_errno.c 
    ibp_client_version.c 
    iovec_sync.c 
    io_wrapper.c
)

ADD_LIBRARY( ibp SHARED ${IBP_OBJS})
ADD_LIBRARY( ibp-static STATIC ${IBP_OBJS})
SET_TARGET_PROPERTIES( ibp-static PROPERTIES OUTPUT_NAME "ibp" )
SET_TARGET_PROPERTIES(ibp PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(ibp-static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

#Make static executables by default
SET(ibp_lib ibp-static)
#To use shared libs
#SET(ibp_lib ibp)

SET(EXE ibp_perf ibp_test ibp_copyperf ibp_tool ibp_find_drive)
foreach (f ${EXE})
   ADD_EXECUTABLE( ${f} ${f}.c ${IBP_OBJS} )
   TARGET_LINK_LIBRARIES( ${f} ${ibp_lib} ${LIBS})
endforeach(f)

set(sodeps)
set(lpdeps)
foreach(lib ${LIBS} )
  shared_name(${lib} soname)
  set(sodeps ${sodeps} ${soname})

  get_filename_component(ldir ${lib} PATH )
  if (ldir)
     set(lpdeps ${lpdeps} "-L${ldir}" )
  endif()
endforeach()

TARGET_LINK_LIBRARIES( ibp ${lpdeps} ${sodeps} )

install(TARGETS ${EXE} DESTINATION bin)
install(TARGETS ibp DESTINATION lib)
install(TARGETS ibp-static DESTINATION lib)
install(FILES 
   ibp_ClientLib.h  ibp_errno.h  ibp.h  ibp_misc.h  ibp_nfu.h  ibp_op.h  ibp_os.h  ibp_protocol.h  ibp_types.h  iovec_sync.h  io_wrapper.h
   DESTINATION include/ibp )


# Packaging directives
set(CPACK_PROJECT_CONFIG_FILE ${CMAKE_SOURCE_DIR}/cmake/cpack_settings.cmake) #This is where the build type specific settings are kept
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_GENERATOR "DEB;RPM")
set(CPACK_PACKAGE_NAME "accre-ibp-client")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(CPACK_PACKAGING_INSTALL_PREFIX "/usr/local")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "/usr/local")
include(CPack)

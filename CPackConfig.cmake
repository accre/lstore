# For help take a look at:
# http://www.cmake.org/Wiki/CMake:CPackConfiguration

### overrides
set(APPLICATION_NAME "apr-util-accre")
set(APPLICATION_VERSION_MAJOR 1)
set(APPLICATION_VERSION_MINOR 5)
set(APPLICATION_VERSION_PATCH 4)
set(APPLICATION_VERSION "${APPLICATION_VERSION_MAJOR}.${APPLICATION_VERSION_MINOR}.${APPLICATION_VERSION_PATCH}")
set(CPACK_INSTALL_COMMANDS "${CMAKE_CURRENT_LIST_DIR}/configure --prefix=/usr --includedir=/usr/include/apr-util-ACCRE-1 --with-installbuilddir=/usr/lib/apr-util-ACCRE-1/build --with-apr=/usr/bin/apr-ACCRE-1-config" "make install")

### general settings
set(CPACK_PACKAGE_NAME ${APPLICATION_NAME})
set(CPACK_DEBIAN_PACKAGE_NAME "libapr-accre-util1")     
# ^^^ use this package name for debs b/c it is what makes dpkg-shlibdeps happy
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "ACCRE-modified libapr-util")
set(CPACK_PACKAGE_DESCRIPTION "APR-Util+some additional ACCRE patches")
#set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/README")
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "ACCRE")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
#set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/COPYING")


### versions
set(CPACK_PACKAGE_VERSION_MAJOR ${APPLICATION_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${APPLICATION_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${APPLICATION_VERSION_PATCH})
set(CPACK_PACKAGE_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")


### source generator
set(CPACK_SOURCE_GENERATOR "TXZ")
set(CPACK_SOURCE_IGNORE_FILES "~$;[.]swp$;/[.]svn/;/[.]git/;.gitignore;/build/;/obj/;tags;cscope.*")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")

if (WIN32)
    set(CPACK_GENERATOR "ZIP")

    ### nsis generator
    find_package(NSIS)
    if (NSIS_MAKE)
        set(CPACK_GENERATOR "${CPACK_GENERATOR};NSIS")
        set(CPACK_NSIS_DISPLAY_NAME ${APPLICATION_NAME})
        set(CPACK_NSIS_COMPRESSOR "/SOLID zlib")
        set(CPACK_NSIS_MENU_LINKS "http://apr.apache.org/" "libapr homepage")
    endif (NSIS_MAKE)
endif (WIN32)

set(CPACK_PACKAGE_INSTALL_DIRECTORY "libapr-util")

set(CPACK_PACKAGE_FILE_NAME ${APPLICATION_NAME}-${CPACK_PACKAGE_VERSION})

set(CPACK_COMPONENT_LIBRARIES_DISPLAY_NAME "Libraries")
set(CPACK_COMPONENT_HEADERS_DISPLAY_NAME "C/C++ Headers")
set(CPACK_COMPONENT_LIBRARIES_DESCRIPTION
  "Libraries used to build programs which use libapr")
set(CPACK_COMPONENT_HEADERS_DESCRIPTION
  "C/C++ header files for use with libapr")
set(CPACK_COMPONENT_HEADERS_DEPENDS libraries)
#set(CPACK_COMPONENT_APPLICATIONS_GROUP "Runtime")
set(CPACK_COMPONENT_LIBRARIES_GROUP "Development")
set(CPACK_COMPONENT_HEADERS_GROUP "Development")

#include(CPack)

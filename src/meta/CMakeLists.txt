cmake_minimum_required(VERSION 2.8)
install(DIRECTORY etc/lio DESTINATION "/etc/"
        PATTERN "*.sh" PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_READ)
install(FILES etc/logrotate.d/lstore DESTINATION "/etc/logrotate.d/")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/etc/logrotate.d")
set(CPACK_RPM_USER_FILELIST "%config(noreplace) /etc/logrotate.d/lstore")
set(CPACK_PACKAGING_INSTALL_PREFIX "/")
set(CPACK_PACKAGE_NAME "lstore")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
  "L-Store metapackage, installs the appropriate libraries and default configurations")
# At least set a short description ^^^^ because the decription file vvvv seems to get ignored
set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_CURRENT_SOURCE_DIR}/pkg-description)
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Matthew Heller")
set(CPACK_PACKAGE_MAINTAINER "Matthew Heller <matthew.f.heller@accre.vanderbilt.edu>")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(PKG_ARCHITECTURE "all")
set(CPACK_PACKAGE_FILE_NAME "lstore-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}_${PKG_ARCHITECTURE}")

# DEB specific stuff
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "http://www.lstore.org/")
set(CPACK_DEBIAN_PACKAGE_DEPENDS
  "accre-gop, accre-ibp, accre-jerasure, accre-lio, accre-toolbox, libapr-accre1, libapr-accre-util1, libczmq3")
set(CPACK_DEBIAN_PACKAGE_SUGGESTS "accre-gridftp")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${PKG_ARCHITECTURE})

include(CPack)

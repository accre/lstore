cmake_minimum_required(VERSION 2.8)
install(DIRECTORY lio DESTINATION "./")
install(DIRECTORY etc/lio DESTINATION "etc/"
        PATTERN "*.sh" PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_READ)
set(CPACK_PACKAGING_INSTALL_PREFIX "/")
set(CPACK_PACKAGE_NAME "lstore-vandy-cfg")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Basic starting configuration for using the primary Vanderbilt L-Store system")
# At least set a short description ^^^^ because the decription file vvvv seems to get ignored
set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_CURRENT_SOURCE_DIR}/pkg-description)
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Matthew Heller")
set(CPACK_PACKAGE_MAINTAINER "Matthew Heller <matthew.f.heller@accre.vanderbilt.edu>")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(PKG_ARCHITECTURE "all")
set(CPACK_PACKAGE_FILE_NAME "lstore-vandy-cfg-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}_${PKG_ARCHITECTURE}")

# DEB specific stuff
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "http://www.lstore.org/")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "accre-gop, accre-ibp, accre-jerasure, accre-lio, accre-toolbox, libapr-accre1, libapr-accre-util1, czmq")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${PKG_ARCHITECTURE})
# use one of the lines below once we have a top level "lstore" package "recommends" is stronger hint than "suggests"
#set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "lstore")
#set(CPACK_DEBIAN_PACKAGE_SUGGESTS "lstore")


include(CPack)

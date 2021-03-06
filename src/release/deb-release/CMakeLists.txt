cmake_minimum_required(VERSION 2.8)
set(LSTORE_REPO_URL "http://repo.accre.vanderbilt.edu"
    CACHE STRING "Repository URL for LStore")
if(INSTALL_DEB_RELEASE OR (NOT DEFINED INSTALL_DEB_RELEASE))
    configure_file(lstore-repo.list.in ${CMAKE_BINARY_DIR}/lstore-repo.list)
    install(FILES ${CMAKE_BINARY_DIR}/lstore-repo.list
                DESTINATION "usr/share/doc/lstore-repo/"
                COMPONENT lstore-deb-release)
endif()

set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${CMAKE_CURRENT_SOURCE_DIR}/postinst)
set(CPACK_PACKAGING_INSTALL_PREFIX "/")
set(CPACK_PACKAGE_NAME "lstore-release")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "Convenience package for adding the ACCRE LStore repository")
# At least set a short description ^^^^ because the decription file vvvv seems to get ignored
set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_CURRENT_SOURCE_DIR}/pkg-description)
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_DEBIAN_PACKAGE_PREDEPENDS "lsb-release")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "all")
# use one of the lines below once we have a top level "lstore" package "recommends" is stronger hint than "suggests"
#set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "lstore")
#set(CPACK_DEBIAN_PACKAGE_SUGGESTS "lstore")
set(CPACK_PACKAGE_CONTACT "Matthew Heller")
set(CPACK_PACKAGE_MAINTAINER "Matthew Heller <matthew.f.heller@accre.vanderbilt.edu>")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "http://www.lstore.org/")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(CPACK_PACKAGE_FILE_NAME "lstore-release-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}_${CPACK_DEBIAN_PACKAGE_ARCHITECTURE}")
include(CPack)

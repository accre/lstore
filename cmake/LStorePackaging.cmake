# Pack it up
if(NOT DEFINED ${CPACK_GENERATOR})
    set(CPACK_GENERATOR "TGZ")
endif()
if(NOT DEFINED ${CPACK_SOURCE_GENERATOR})
    set(CPACK_SOURCE_GENERATOR "TGZ")
endif()

# Global configuration
set(CPACK_PACKAGE_NAME "LStore")
set(CPACK_PACKAGE_VERSION "0.5.1")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "5")
set(CPACK_PACKAGE_VERSION_PATCH "1")
set(CPACK_PACKAGE_RELEASE ${LSTORE_PROJECT_REVISION})
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CPACK_SOURCE_IGNORE_FILES "^${CMAKE_SOURCE_DIR}/build" 
                                "^${PROJECT_SOURCE_DIR}/scripts"
                                "^${PROJECT_SOURCE_DIR}/\\\\.git/"
                                "apr-util.spec"
                                "apr.spec"
                                ${CPACK_SOURCE_IGNORE_FILES})
set(CPACK_PACKAGE_RELOCATABLE OFF)

# RPM
set(CPACK_RPM_PACKAGE_DEBUG ON)
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_RELOCATABLE OFF)
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/cmake/rpm-ldconfig.sh")
set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/cmake/rpm-ldconfig.sh")

#
# Component configuration - currently broken. :(
#
set(CPACK_DEB_COMPONENT_INSTALL OFF)
set(CPACK_ARCHIVE_COMPONENT_INSTALL OFF)

# 
set(CPACK_COMPONENT_DEVEL_DISPLAY_NAME "Development Files")
set(CPACK_COMPONENT_DEVEL_DESCRIPTION "Necessary files to build/link against LStore")

# Generator specific config
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

# Targetski
add_custom_target(dist COMMAND ${CMAKE_MAKE_PROGRAM} package_source)
if(UNIX)
    ADD_CUSTOM_TARGET(rpm rpmbuild -ta ${CPACK_SOURCE_PACKAGE_FILE_NAME}.tar.gz)
    ADD_DEPENDENCIES(rpm dist)
    ADD_CUSTOM_TARGET(deb dpkg-buildpackage -uc -us)
endif()

include(CPack)

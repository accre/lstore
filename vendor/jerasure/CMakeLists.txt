
cmake_minimum_required(VERSION 2.6)

# make a project
project( lstore-jerasure C )

option(WANT_SHARED "Produce a shared library" ON)
option(WANT_STATIC "Produce a static library" ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

# Build subdirectories
add_subdirectory(src)

# Don't build the examples into the main package, just copy the folder into a -dev or -doc package if desired
#add_subdirectory (Examples)
# TODO: automate doxygen documentation rebuild?

set(CPACK_PACKAGE_VERSION "1.2a")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/cmake/rpm-ldconfig.sh")
set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/cmake/rpm-ldconfig.sh")
if(NOT DEFINED CPACK_GENERATOR)
    set(CPACK_GENERATOR "DEB;RPM")
endif(NOT DEFINED CPACK_GENERATOR)
set(CPACK_PACKAGE_NAME "accre-jerasure")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
include(CPack)


cmake_minimum_required(VERSION 2.6)

# make a project
project( lstore-jerasure C )

# Build subdirectories
add_subdirectory(src)

# Don't build the examples into the main package, just copy the folder into a -dev or -doc package if desired
#add_subdirectory (Examples)
# TODO: automate doxygen documentation rebuild?

set(CPACK_PACKAGE_VERSION "1.2a")
if(NOT DEFINED CPACK_GENERATOR)
    set(CPACK_GENERATOR "DEB;RPM")
endif(NOT DEFINED CPACK_GENERATOR)
set(CPACK_PACKAGE_NAME "accre-jerasure")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Andrew Melo or Alan Tackett")
set(CPACK_PACKAGE_VENDOR "Advanced Computing Center for Research and Education, Vanderbilt University")
include(CPack)

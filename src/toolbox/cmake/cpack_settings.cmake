#Tweaks for the different package types
message(STATUS "========loading ${CPACK_GENERATOR} config==========")

if (${CPACK_GENERATOR} STREQUAL "DEB")
   message(STATUS "Building DEBIAN packages")
   set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
#   set(CPACK_DEB_COMPONENT_INSTALL ON)
#   set(CPACK_COMPONENTS_ALL libraries headers)
elseif (${CPACK_GENERATOR} STREQUAL "RPM")
   message(STATUS "Building RPM packages")
else()
   message(STATUS "Building ${CPACK_GENERATOR} packages")
endif()


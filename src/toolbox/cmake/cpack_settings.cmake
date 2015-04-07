#Tweaks for the different package types

if (${CPACK_GENERATOR} STREQUAL "DEB")
   set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
elseif (${CPACK_GENERATOR} STREQUAL "RPM")
   #set(CPACK_RPM_PACKAGE_REQUIRES "openssl >= 1.0.1e")
   #set(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
endif()


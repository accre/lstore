#Tweaks for the different package types

if (${CPACK_GENERATOR} STREQUAL "DEB")
   #set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
   set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>=2.7-18), openssl (>=1.0.1f), zlib (>=1.2.8), czmq (>=1.4.1), apr-accre (>= 1.5.0), apr-util-ACCRE (>=1.5.3), zeromq (=>4.0.1)")
elseif (${CPACK_GENERATOR} STREQUAL "RPM")
   set(CPACK_RPM_PACKAGE_DEPENDS "libc6 (>=2.7-18), openssl (>=1.0.1f), zlib (>=1.2.8), czmq (>=1.4.1), apr-accre (>= 1.5.0), apr-util-ACCRE (>=1.5.3), zeromq (=>4.0.1)")
   #set(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
endif()


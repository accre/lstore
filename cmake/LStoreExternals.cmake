include(ExternalProject)
if(BUILD_APR OR (NOT APR-ACCRE_FOUND) OR (APR_LIBRARY MATCHES "^${EXTERNAL_INSTALL_DIR}"))
    list(APPEND REBUILD_DEPENDENCIES extern-apr-accre)
    set(APR_LIBRARY "apr-ACCRE-1")
    set(APR_INCLUDE_DIR "${EXTERNAL_INSTALL_DIR}/include/apr-ACCRE-1")
	ExternalProject_add(extern-apr-accre
			PREFIX "${CMAKE_BINARY_DIR}/state/apr-accre/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/apr-accre/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/apr-accre/"
            INSTALL_DIR "${EXTERNAL_INSTALL_DIR}"
			CONFIGURE_COMMAND "${CMAKE_SOURCE_DIR}/vendor/apr-accre/configure"
								"--prefix=${EXTERNAL_INSTALL_DIR}/"
								"--includedir=${EXTERNAL_INSTALL_DIR}/include/apr-ACCRE-1"
								"--with-installbuilddir=${EXTERNAL_INSTALL_DIR}/lib/apr-ACCRE-1/build"
                                "--enable-static=yes" "--enable-shared=no" "--with-pic"
			BUILD_COMMAND "make"
			TEST_COMMAND "make" "test"
			INSTALL_COMMAND "make" "install"
			TEST_EXCLUDE_FROM_MAIN 1
		)
endif()
if(BUILD_APU OR (NOT APRUTIL-ACCRE_FOUND) OR (APU_LIBRARY MATCHES "^${EXTERNAL_INSTALL_DIR}")) 
    list(APPEND REBUILD_DEPENDENCIES extern-apr-util-accre)
    set(APRUTIL_LIBRARY "aprutil-ACCRE-1")
    set(APRUTIL_INCLUDE_DIR "${EXTERNAL_INSTALL_DIR}/include/apr-util-ACCRE-1")
	ExternalProject_add(extern-apr-util-accre
			DEPENDS extern-apr-accre
			PREFIX "${CMAKE_BINARY_DIR}/state/apr-util-accre/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/apr-util-accre/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/apr-util-accre/"
            INSTALL_DIR "${EXTERNAL_INSTALL_DIR}"
			CONFIGURE_COMMAND "${CMAKE_SOURCE_DIR}/vendor/apr-util-accre/configure"
								"--with-apr=${EXTERNAL_INSTALL_DIR}/bin/apr-ACCRE-1-config"
								"--prefix=${EXTERNAL_INSTALL_DIR}/"
								"--includedir=${EXTERNAL_INSTALL_DIR}/include/apr-util-ACCRE-1"
								"--with-installbuilddir=${EXTERNAL_INSTALL_DIR}/lib/apr-util-ACCRE-1/build"
                                "--enable-static=yes" "--enable-shared=no" "--with-pic"
			BUILD_COMMAND "make"
			TEST_COMMAND "make" "test"
			INSTALL_COMMAND "make" "install"
			TEST_EXCLUDE_FROM_MAIN 1
		)
endif()
if(BUILD_JERASURE OR (NOT JERASURE_FOUND) OR (JERASURE_LIBRARY MATCHES "^${EXTERNAL_INSTALL_DIR}"))
    list(APPEND REBUILD_DEPENDENCIES extern-jerasure)
    set(JERASURE_LIBRARY "jerasure")
    set(JERASURE_INCLUDE_DIR ${EXTERNAL_INSTALL_DIR}/include)
	ExternalProject_add(extern-jerasure
			PREFIX "${CMAKE_BINARY_DIR}/state/jerasure/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/jerasure/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/jerasure/"
            INSTALL_DIR "${EXTERNAL_INSTALL_DIR}"
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_DIR}
                        -DWANT_SHARED:BOOL=OFF -DWANT_STATIC:BOOL=ON
			TEST_COMMAND ""
			INSTALL_COMMAND "make" "install"
		)
endif()
if((NOT CZMQ_FOUND) OR (CZMQ_LIBRARY MATCHES "^${EXTERNAL_INSTALL_DIR}"))
    list(APPEND REBUILD_DEPENDENCIES extern-czmq)
    set(CZMQ_LIBRARY "czmq")
	ExternalProject_add(extern-czmq
			PREFIX "${CMAKE_BINARY_DIR}/state/czmq/"
			SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/czmq/"
			BINARY_DIR "${CMAKE_BINARY_DIR}/czmq/"
            INSTALL_DIR "${EXTERNAL_INSTALL_DIR}"
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_DIR}
                        -DBUILD_SHARED_LIBS:BOOL=OFF
			TEST_EXCLUDE_FROM_MAIN 1
			TEST_COMMAND "make" "test"
			INSTALL_COMMAND "make" "install"
		)
endif()


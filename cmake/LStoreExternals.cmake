include(ExternalProject)
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
                        ${LSTORE_C_COMPILER_WRAPPER_CMAKE}
			TEST_COMMAND ""
            INSTALL_COMMAND $(MAKE) "install"
		)
endif()

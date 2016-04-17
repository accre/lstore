macro(lstore_project_common LSTORE_PROJECT_NAME)
    # Make the version file.
    include(${CMAKE_SOURCE_DIR}/cmake/Date.cmake)
    include(${CMAKE_SOURCE_DIR}/cmake/CompilerVersion.cmake)
    include(${CMAKE_SOURCE_DIR}/cmake/CompilerFlags.cmake)
    set(LSTORE_INC_VERSION_STRING "${LSTORE_PROJECT_NAME}: ${LSTORE_PROJECT_VERSION}")
    site_name(BUILD_HOST)
    Date(BUILD_DATE)
    CompilerVersion(COMPILER_VERSION)
    CompilerFlags(COMPILER_FLAGS)
    configure_file(${PROJECT_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_version.c.in
                    ${PROJECT_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_version.c)
    set(LSTORE_PROJECT_OBJS ${LSTORE_PROJECT_OBJS} ${LSTORE_PROJECT_NAME}_version.c)

    # Build library
    add_library(${LSTORE_PROJECT_NAME} SHARED ${LSTORE_PROJECT_OBJS})
    target_link_libraries(${LSTORE_PROJECT_NAME} LINK_PUBLIC ${LSTORE_LIBS})
    target_include_directories(${LSTORE_PROJECT_NAME} SYSTEM PRIVATE ${LSTORE_INCLUDE_SYSTEM})
    target_include_directories(${LSTORE_PROJECT_NAME} PUBLIC ${LSTORE_INCLUDE_PUBLIC})
    add_dependencies(${LSTORE_PROJECT_NAME} externals)

    # Build externals
    foreach(f ${LSTORE_PROJECT_EXECUTABLES})
        add_executable(${f} ${f}.c)
        target_link_libraries(${f} ${LSTORE_PROJECT_NAME})
        target_include_directories(${f} SYSTEM PRIVATE ${LSTORE_INCLUDE_SYSTEM})
        target_include_directories(${f} PUBLIC ${LSTORE_INCLUDE_PUBLIC})
        install(TARGETS ${f} DESTINATION ${CMAKE_INSTALL_BINDIR})
    endforeach(f)

    # Install products
    install(TARGETS ${LSTORE_PROJECT_NAME}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    install(FILES ${LSTORE_PROJECT_INCLUDES}
            DESTINATION include/${LSTORE_LSTORE_PROJECT_NAME}
            COMPONENT devel)

    # Export component info
	set(${LSTORE_PROJECT_NAME}_INCLUDE_DIR ${PROJECT_SOURCE_DIR}
		CACHE INTERNAL "${LSTORE_PROJECT_NAME}: Include Directories" FORCE)
endmacro()

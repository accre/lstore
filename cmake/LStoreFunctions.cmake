macro(lstore_project_common LSTORE_PROJECT_NAME)
    # Extract version components, version string is expected be in the form:
    #  <major>.<minor>.<patch>-<git-hash>[-<other-tags>]
    
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
    add_library(${LSTORE_PROJECT_NAME} ${LSTORE_PROJECT_OBJS})

    # Use the project version as the version for each library (for now at least)
    set(LSTORE_LIBRARY_VERSION_MAJOR ${LSTORE_VERSION_MAJOR})
    set(LSTORE_LIBRARY_VERSION_MINOR ${LSTORE_VERSION_MINOR})
    set(LSTORE_LIBRARY_VERSION_PATCH ${LSTORE_VERSION_PATCH})
    set(LSTORE_LIBRARY_VERSION_STRING ${LSTORE_LIBRARY_VERSION_MAJOR}.${LSTORE_LIBRARY_VERSION_MINOR}.${LSTORE_LIBRARY_VERSION_PATCH})
    # Give our libraries versioning info
    set_target_properties(${LSTORE_PROJECT_NAME} PROPERTIES VERSION ${LSTORE_LIBRARY_VERSION_STRING} SOVERSION ${LSTORE_LIBRARY_VERSION_MAJOR})

    configure_file(${LSTORE_PROJECT_NAME}.pc.in ${LSTORE_PROJECT_NAME}.pc @ONLY)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${LSTORE_PROJECT_NAME}.pc" DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

    target_link_libraries(${LSTORE_PROJECT_NAME} LINK_PUBLIC ${LSTORE_LIBS} m pthread)

    if(NOT BUILD_SHARED_LIBS)
        target_link_libraries(${LSTORE_PROJECT_NAME} LINK_PUBLIC ${LSTORE_LIBS} dl)
        set_target_properties(${LSTORE_PROJECT_NAME} PROPERTIES COMPILE_FLAGS
                                "-D${LSTORE_PROJECT_NAME}_EXPORTS -DLSTORE_HACK_EXPORT")
    else()
        set_target_properties(${LSTORE_PROJECT_NAME} PROPERTIES
                            COMPILE_FLAGS "-DLSTORE_HACK_EXPORT")
    endif()
    if("${CMAKE_VERSION}" VERSION_GREATER 3.0.0)
        target_include_directories(${LSTORE_PROJECT_NAME}
                                    SYSTEM PRIVATE
                                    ${LSTORE_INCLUDE_SYSTEM})
    else()
        target_include_directories(${LSTORE_PROJECT_NAME}
                                    PRIVATE
                                    ${LSTORE_INCLUDE_SYSTEM})
    endif()
    target_include_directories(${LSTORE_PROJECT_NAME} PUBLIC ${LSTORE_INCLUDE_PUBLIC})
    add_dependencies(${LSTORE_PROJECT_NAME} externals)

    # Build externals
    foreach(f ${LSTORE_PROJECT_EXECUTABLES_OLD})
        add_executable(${f} ${f}.c)
    endforeach()
    foreach(f ${LSTORE_PROJECT_EXECUTABLES})
        add_executable(${f} bin/${f}.c)
    endforeach()
    foreach(f ${LSTORE_PROJECT_EXECUTABLES_OLD} ${LSTORE_PROJECT_EXECUTABLES})
        target_link_libraries(${f} ${LSTORE_PROJECT_NAME})
        if("${CMAKE_VERSION}" VERSION_GREATER 3.0.0)
            target_include_directories(${f}
                                        SYSTEM PRIVATE
                                        ${LSTORE_INCLUDE_SYSTEM})
        else()
            target_include_directories(${f}
                                        PRIVATE
                                        ${LSTORE_INCLUDE_SYSTEM})
        endif()
        target_include_directories(${f} PUBLIC ${LSTORE_INCLUDE_PUBLIC})
        if(NOT BUILD_SHARED_EXES)
            set_target_properties(${f} PROPERTIES LINK_SEARCH_START_STATIC 1)
            set_target_properties(${f} PROPERTIES LINK_SEARCH_END_STATIC 1)
        endif()
        install(TARGETS ${f} DESTINATION ${CMAKE_INSTALL_BINDIR}
                             COMPONENT bin)
    endforeach(f)

    # Install products
    install(TARGETS ${LSTORE_PROJECT_NAME}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    install(FILES ${LSTORE_PROJECT_INCLUDES}
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${LSTORE_PROJECT_INCLUDES_NAMESPACE}
            COMPONENT devel)
    install(FILES ${LSTORE_PROJECT_INCLUDES_OLD}
            DESTINATION include/${LSTORE_PROJECT_NAME}
            COMPONENT devel)


    # Export component info
	set(${LSTORE_PROJECT_NAME}_INCLUDE_DIR ${PROJECT_SOURCE_DIR}
		CACHE INTERNAL "${LSTORE_PROJECT_NAME}: Include Directories" FORCE)
endmacro()

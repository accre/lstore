
# - Try to find TCMU runner
# Once done this will define
#  TCMU_FOUND - System has TCMU runner installed
#  TCMU_INCLUDE_DIR - The TCMU runner include directories


find_path(TCMU_BASE_DIR NAME tcmu-runner.h
           NO_DEFAULT_PATH PATHS ${TCMU_PREFIX})


if (TCMU_BASE_DIR)
    set(TCMU_INCLUDE_DIR ${TCMU_BASE_DIR} ${TCMU_BASE_DIR}/ccan)
    SET(TCMU_FOUND "YES")
endif (TCMU_BASE_DIR)

if (TCMU_FOUND)
    message(STATUS "Found TCMU: ${TCMU_INCLUDE_DIR}")
else (TCMU_FOUND)
    message(STATUS "Could not find TCMU")
endif (TCMU_FOUND)
mark_as_advanced( TCMU_FOUND TCMU_INCLUDE_DIR )


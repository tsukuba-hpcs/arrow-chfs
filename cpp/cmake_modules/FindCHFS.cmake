

find_package(PkgConfig QUIET)
pkg_check_modules(CHFS REQUIRED chfs)
pkg_check_modules(MARGO REQUIRED margo)

message(STATUS "FindCHFS begin CHFS_FOUND=${CHFS_FOUND} CHFS_LIBDIR=${CHFS_LIBDIR} CHFS_LINK_LIBRARIES=${CHFS_LINK_LIBRARIES} CHFS_INCLUDE_DIRS=${CHFS_INCLUDE_DIRS}" )


if (CHFS_FOUND)
  list(APPEND CHFS_LIBRARY_DIRS "${CHFS_LIBDIR}")
  find_library(CHFS_LIB chfs
               PATHS ${CHFS_LIBRARY_DIRS}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  message(STATUS "CHFS_LIB=${CHFS_LIB}" )

  find_package_handle_standard_args(CHFS REQUIRED_VARS CHFS_INCLUDE_DIRS CHFS_LIB)

  add_library(chfs::chfs UNKNOWN IMPORTED)
  set_target_properties(chfs::chfs
                        PROPERTIES IMPORTED_LOCATION "${CHFS_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${CHFS_INCLUDE_DIRS}")
  
  list(APPEND MARGO_LIBRARY_DIRS "${MARGO_LIBDIR}")
  find_library(MARGO_LIB margo
               PATHS ${MARGO_LIBRARY_DIRS}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  message(STATUS "MARGO_LIB=${MARGO_LIB}" )

  find_package_handle_standard_args(MARGO REQUIRED_VARS MARGO_INCLUDE_DIRS MARGO_LIB)

  add_library(margo::margo UNKNOWN IMPORTED)
  set_target_properties(margo::margo
                        PROPERTIES IMPORTED_LOCATION "${MARGO_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${MARGO_INCLUDE_DIRD}")
endif()
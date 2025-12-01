cmake_minimum_required(VERSION 3.10.0)
project(RackRadar VERSION 0.1.0 LANGUAGES C)

include(FindPkgConfig)
#find_package(libconfig  REQUIRED)
pkg_check_modules(LIBCONFIG REQUIRED IMPORTED_TARGET "libconfig")
pkg_check_modules(MARIADB_CLIENT REQUIRED IMPORTED_TARGET "libmariadb")

add_executable(RackRadar
  src/main.c
  src/log.c
  src/config.c
  src/db.c
)

include_directories(RackRadar PRIVATE
  include
  ${LIBCONFIG_INCLUDE_DIRS}
  ${MARIADB_CLIENT_INCLUDE_DIRS}
)

target_link_libraries(RackRadar
  ${LIBCONFIG_LIBRARIES}
  ${MARIADB_CLIENT_LIBRARIES}
)
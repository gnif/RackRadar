cmake_minimum_required(VERSION 3.10.0)
project(RackRadar VERSION 0.1.0 LANGUAGES C)

include(FindPkgConfig)
#find_package(libconfig  REQUIRED)
pkg_check_modules(LIBCONFIG REQUIRED IMPORTED_TARGET "libconfig")
pkg_check_modules(MARIADB_CLIENT REQUIRED IMPORTED_TARGET "libmariadb")
pkg_check_modules(ZLIB REQUIRED IMPORTED_TARGET "zlib")
pkg_check_modules(ICU REQUIRED IMPORTED_TARGET "icu-uc icu-i18n")
find_package(Iconv REQUIRED)

add_executable(RackRadar
  src/main.c
  src/log.c
  src/config.c
  src/db.c
  src/rpsl.c
)

include_directories(RackRadar PRIVATE
  include
  ${LIBCONFIG_INCLUDE_DIRS}
  ${MARIADB_CLIENT_INCLUDE_DIRS}
  ${ZLIB_INCLUDE_DIRS}
  ${ICU_INCLUDE_DIRS}
  ${Iconv_INCLUDE_DIRS}
)

target_link_libraries(RackRadar
  ${LIBCONFIG_LIBRARIES}
  ${MARIADB_CLIENT_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${ICU_LIBRARIES}  
  ${Iconv_LIBRARIES}
)
cmake_minimum_required(VERSION 3.10.0)
project(RackRadar VERSION 0.1.0 LANGUAGES C)

include(GNUInstallDirs)

include(FindPkgConfig)
pkg_check_modules(LIBCONFIG REQUIRED IMPORTED_TARGET "libconfig")
pkg_check_modules(MARIADB_CLIENT REQUIRED IMPORTED_TARGET "libmariadb")
pkg_check_modules(CURL REQUIRED IMPORTED_TARGET "libcurl")
pkg_check_modules(ZLIB REQUIRED IMPORTED_TARGET "zlib")
pkg_check_modules(MINIZIP REQUIRED IMPORTED_TARGET "minizip")
pkg_check_modules(EXPAT REQUIRED IMPORTED_TARGET "expat")
pkg_check_modules(ICU REQUIRED IMPORTED_TARGET "icu-uc icu-i18n")
pkg_check_modules(LIBMICROHTTPD REQUIRED IMPORTED_TARGET "libmicrohttpd")
find_package(Iconv REQUIRED)

add_compile_options(
  "-Wall"
  "-Wextra"
  "-Wno-sign-compare"
  "-Wno-unused-parameter"
  "-Wno-unknown-pragmas"
  "$<$<COMPILE_LANGUAGE:C>:-Wstrict-prototypes>"
  "$<$<C_COMPILER_ID:GNU>:-Wimplicit-fallthrough=2>"
  "-Werror"
  "-Wfatal-errors"
)

add_executable(RackRadar
  src/main.c
  src/log.c
  src/util.c
  src/config.c
  src/download.c
  src/zip.c
  src/db.c
  src/query.c
  src/import.c
  src/http.c
  src/rpsl.c
  src/arin.c
)

add_compile_definitions(RackRadar
  _GNU_SOURCE
)

include_directories(RackRadar PRIVATE
  include
  ${LIBCONFIG_INCLUDE_DIRS}
  ${MARIADB_CLIENT_INCLUDE_DIRS}
  ${CURL_INCLUDE_DIRS}
  ${ZLIB_INCLUDE_DIRS}
  ${EXPAT_INCLUDE_DIRS}
  ${MINIZIP_INCLUDE_DIRS}
  ${ICU_INCLUDE_DIRS}
  ${Iconv_INCLUDE_DIRS}
  ${LIBMICROHTTPD_INCLUDE_DIRS}
)

target_link_libraries(RackRadar
  ${LIBCONFIG_LIBRARIES}
  ${MARIADB_CLIENT_LIBRARIES}
  ${CURL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${EXPAT_LIBRARIES}
  ${MINIZIP_LIBRARIES}
  ${ICU_LIBRARIES}
  ${Iconv_LIBRARIES}
  ${LIBMICROHTTPD_LIBRARIES}
  pthread
)

install(TARGETS RackRadar
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(FILES rackradar.service
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system
)

cmake_minimum_required(VERSION 3.10.0)
project(RackRadar VERSION 0.1.0 LANGUAGES C)

find_package(libconfig REQUIRED)

add_executable(RackRadar
  src/main.c
  src/log.c
)

include_directories(RackRadar PRIVATE
  include
  ${LIBCONFIG_INCLUDE_DIRS}
)

target_link_libraries(RackRadar
  ${LIBCONFIG_LIBRARIES}
)
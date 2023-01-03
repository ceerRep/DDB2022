cmake_minimum_required(VERSION 3.16)

set(_project_name seastar-kv-test)

project(${_project_name} C CXX)

find_package (Seastar REQUIRED)
find_package (SQLite3 REQUIRED)
find_package (yaml-cpp REQUIRED)
find_package (Boost REQUIRED)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(${CMAKE_SOURCE_DIR}/SQLiteCpp)

find_library(LIBSQLPARSER NAMES sqlparser libsqlparser)
set (LIBSQLPARSER "${LIBSQLPARSER}")
if (NOT LIBSQLPARSER)
	message(FATAL_ERROR "libsqlparser not found")
endif()
message ("-- Found libsqlparser: ${LIBSQLPARSER}")

include_directories(${SQLite3_INCLUDE_DIRS})
include_directories(${CMAKE_SOURCE_DIR}/headers ${CMAKE_SOURCE_DIR}/sqlite_modern_cpp/hdr)
include_directories(${YAML_CPP_INCLUDE_DIRS})

add_executable (test
  src/main.cc
  src/init.cpp
  src/queryparser.cc
  src/parsesql.cc)

  add_executable (querytest
  src/parsesql.cc
  src/querymain.cc)

target_link_libraries (test SQLiteCpp ${SQLite3_LIBRARIES})
target_link_libraries (test Seastar::seastar)
target_link_libraries (test ${YAML_CPP_LIBRARIES})
target_link_libraries (test ${LIBSQLPARSER})


target_link_libraries (querytest ${LIBSQLPARSER})
target_link_libraries (querytest SQLiteCpp ${SQLite3_LIBRARIES})
target_link_libraries (querytest fmt)

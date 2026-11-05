cmake_minimum_required(VERSION 3.16)
project(limit_order_book LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(LOB_SANITIZE "" CACHE STRING "Sanitizer to build with: thread, address, or empty")
if(LOB_SANITIZE)
  add_compile_options(-fsanitize=${LOB_SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${LOB_SANITIZE})
endif()

find_package(Threads REQUIRED)

add_library(lobcore STATIC src/server.cpp)
target_include_directories(lobcore PUBLIC include)
target_link_libraries(lobcore PUBLIC Threads::Threads)
target_compile_options(lobcore PRIVATE -Wall -Wextra)

add_executable(engine src/main.cpp)
target_link_libraries(engine PRIVATE lobcore)

add_executable(bench src/bench.cpp)
target_link_libraries(bench PRIVATE lobcore)

add_executable(loadgen tools/loadgen.cpp)
target_link_libraries(loadgen PRIVATE lobcore)

add_executable(feed_client tools/feed_client.cpp)
target_link_libraries(feed_client PRIVATE lobcore)

option(LOB_BUILD_TESTS "Build GoogleTest suite" ON)
if(LOB_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(googletest URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip)
  set(DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  enable_testing()
  add_executable(lob_tests tests/test_lob.cpp tests/test_server.cpp)
  target_include_directories(lob_tests PRIVATE tests)
  target_link_libraries(lob_tests PRIVATE lobcore GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(lob_tests)
endif()

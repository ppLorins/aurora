cmake_minimum_required(VERSION 3.3)
project(Aurora)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

option(GLOG_INC_DIR,"Use include" ON)

message("project src:" ${PROJECT_SOURCE_DIR})

file(GLOB_RECURSE ALL_SRC ${PROJECT_SOURCE_DIR}/src *.cc)

foreach(ARG ${ALL_SRC})
    message("one source:${ARG}")           
endforeach()

list(FILTER ALL_SRC EXCLUDE REGEX .*trivial_lock_deque.cc)
list(FILTER ALL_SRC EXCLUDE REGEX .*trivial_lock_hash.cc)
list(FILTER ALL_SRC EXCLUDE REGEX .*trivial_lock_list.cc)
list(FILTER ALL_SRC EXCLUDE REGEX .*trivial_lock_queue.cc)

set(MAIN_SRC ${ALL_SRC})
set(TEST_SRC ${ALL_SRC})
list(FILTER MAIN_SRC EXCLUDE REGEX .*gtest_main.cc)
list(FILTER TEST_SRC EXCLUDE REGEX .*[/\\]main.cc)

message("main src found:" ${MAIN_SRC})
message("test src found:" ${TEST_SRC})

# add the executable
add_executable(aurora ${ALL_SRC})
add_executable(aurora_test ${ALL_SRC})


target_include_directories(aurora PUBLIC "${PROJECT_SOURCE_DIR}/src")



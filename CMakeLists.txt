cmake_minimum_required(VERSION 3.14)
project(ex3s)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS -pthread)

add_executable(ex3s FileWordCounter.cpp MapReduceFramework.cpp)
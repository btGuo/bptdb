cmake_minimum_required(VERSION 3.0)
project(bptdb)
add_compile_options(-std=c++17)
aux_source_directory(. SRC_LIST)
add_library(bptdb ${SRC_LIST})

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -Wall -g -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

set(CMAKE_INSTALL_PREFIX /usr/local)
install(TARGETS bptdb 
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/bptdb
    DESTINATION include
)

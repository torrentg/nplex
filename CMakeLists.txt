cmake_minimum_required(VERSION 3.20)

project(
    nplex
    VERSION 0.1.0
    LANGUAGES C CXX
)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

find_package(
    Threads REQUIRED
)

find_package(
    libuv REQUIRED
)

find_package(
    fmt 7 REQUIRED
)

find_package(
    flatbuffers REQUIRED
)

find_package(
    spdlog 1.8 REQUIRED
)

# Create messages_generated.hpp from schema (src/messages.fbs)
flatbuffers_generate_headers(
    TARGET flatbuffers_generated
    SCHEMAS ${PROJECT_SOURCE_DIR}/src/messages.fbs
    FLAGS --cpp --filename-ext hpp --cpp-std c++17 --scoped-enums
)

# Copy messages_generated.hpp to src/messages.hpp
add_custom_command(
    OUTPUT "${PROJECT_SOURCE_DIR}/src/messages.hpp"
    COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_BINARY_DIR}/flatbuffers_generated/messages_generated.hpp"
        "${PROJECT_SOURCE_DIR}/src/messages.hpp"
    DEPENDS
        GENERATE_flatbuffers_generated
        ${PROJECT_SOURCE_DIR}/src/messages.fbs
    COMMENT "Copying generated messages.hpp to src directory"
)

add_compile_options(
    -march=native
    -Wall
    -Wextra
    -Wshadow
    -Wpedantic
    -Wconversion
    -Wsign-conversion
    -Wnull-dereference
    -Wdisabled-optimization
    -Wundef
    -fno-strict-aliasing
)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(
        -Wduplicated-branches
        -Wduplicated-cond
        -Wuseless-cast
        -Wlogical-op
    )
endif()

option(ENABLE_SANITIZERS "Enables AddressSanitizer and UndefinedBehaviorSanitizer." OFF)
option(ENABLE_THREAD_SANITIZER "Enables ThreadSanitizer." OFF)

if(ENABLE_SANITIZERS AND ENABLE_THREAD_SANITIZER)
    message(FATAL_ERROR "ENABLE_SANITIZERS and ENABLE_THREAD_SANITIZER cannot be enabled at the same time.")
endif()

if(ENABLE_SANITIZERS)
    add_compile_options(
        -fno-omit-frame-pointer
        -fsanitize=address
        -fsanitize=undefined
        -fsanitize=float-cast-overflow
        -fsanitize-address-use-after-scope
        -fno-sanitize-recover
    )
endif()

if(ENABLE_THREAD_SANITIZER)
    add_compile_options(
        -fno-omit-frame-pointer
        -fsanitize=thread
    )
endif()

option(ENABLE_COVERAGE "Compile and link code for coverage" OFF)
if(ENABLE_COVERAGE)
    add_compile_options(
        --coverage
        -O0
    )
endif()

set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)

#set(
#    # See .clang-tidy file for config
#    # See https://clang.llvm.org/extra/clang-tidy/checks/list.html
#    CMAKE_CXX_CLANG_TIDY
#    clang-tidy
#)

add_library(
    nplex_lib
    STATIC
)

set_target_properties(
    nplex_lib
    PROPERTIES
    OUTPUT_NAME nplex
)

target_sources(
    nplex_lib
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/deps/ini.c
    ${CMAKE_CURRENT_LIST_DIR}/deps/journal.c
    ${CMAKE_CURRENT_LIST_DIR}/deps/match.c
    ${CMAKE_CURRENT_LIST_DIR}/src/addr.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/common.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/config.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/connection.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/context.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/journal_writer.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/messages.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/messaging.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/server.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/session.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/storage.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/store.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/tasks.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/user.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/utils.cpp
)

add_executable(
    nplex
    ${CMAKE_CURRENT_LIST_DIR}/src/main.cpp
)

add_executable(
    journalctl
    ${CMAKE_CURRENT_LIST_DIR}/deps/journalctl.c
)

add_executable(
    snpbulk
    ${CMAKE_CURRENT_LIST_DIR}/examples/snpbulk.c
)

add_compile_definitions(
    SPDLOG_ACTIVE_LEVEL=0
    SPDLOG_FMT_EXTERNAL
)

target_include_directories(
    nplex_lib
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/deps
)

target_include_directories(
    journalctl
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/deps
)

set_source_files_properties(
    ${CMAKE_CURRENT_LIST_DIR}/deps/journal.c
    PROPERTIES 
    COMPILE_DEFINITIONS _POSIX_C_SOURCE=200809L
    COMPILE_OPTIONS "-Wno-useless-cast"
)

set_source_files_properties(
    ${CMAKE_CURRENT_LIST_DIR}/deps/ini.c
    PROPERTIES 
    COMPILE_OPTIONS "-Wno-useless-cast"
)

target_link_libraries(
    nplex_lib
    "$<$<STREQUAL:${ENABLE_SANITIZERS},ON>:-lasan>"
    "$<$<STREQUAL:${ENABLE_SANITIZERS},ON>:-lubsan>"
    "$<$<STREQUAL:${ENABLE_THREAD_SANITIZER},ON>:-ltsan>"
    "$<$<STREQUAL:${ENABLE_COVERAGE},ON>:-lgcov>"
    Threads::Threads
    libuv::uv
    fmt::fmt
    flatbuffers::flatbuffers_shared
)

target_link_libraries(
    nplex
    PRIVATE
    nplex_lib
)

target_link_libraries(
    journalctl
    PRIVATE
    nplex_lib
)

target_link_libraries(
    snpbulk
    PRIVATE
    nplex_lib
)

#include(CTest)
#enable_testing()
#add_subdirectory(test)

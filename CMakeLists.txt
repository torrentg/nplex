cmake_minimum_required(VERSION 3.20)

project(
    nplex
    VERSION 0.6.0
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

# Requires libuv >= 1.35
find_package(
    libuv REQUIRED
)

find_package(
    fmt 8 REQUIRED
)

find_package(
    flatbuffers 22.01.01 REQUIRED
)

find_package(
    spdlog 1.8 REQUIRED
)

# ---------------------------------------------------------------------------
# FlatBuffers schema pipeline
#
# The schema is split into three files with an include chain:
#
#   schema1.fbs  (KeyValue, Update)       -- used by: journal
#   schema2.fbs  (Snapshot)               -- used by: snapshot files (.snp)
#   schema3.fbs  (protocol messages)      -- used by: client-server protocol
#
# schema2 includes schema1; schema3 includes schema2 (transitively includes all).
#
# From these files we need two things:
#   1. src/schema.hpp    -- C++ bindings generated from schema3, included by
#                           the rest of the codebase. Generated from the binary
#                           schema (schema3.bfbs) so the output is self-contained
#                           (no cascading #include directives in the generated file).
#   2. SCHEMAn_HASH (n=1,2,3) -- 32-bit tokens (first 4 bytes of SHA-256 of
#                           each .bfbs file) embedded in buildinfo.hpp.
#                           SCHEMA1_HASH identifies journal format compatibility.
#                           SCHEMA2_HASH identifies snapshot file compatibility.
#                           SCHEMA3_HASH is sent by clients in LoginRequest.schema
#                           so the server can reject connections built against a
#                           different protocol version.
#
# Hashes are computed over the *binary* schema (.bfbs) rather than the text
# source (.fbs), making them insensitive to cosmetic edits (whitespace, comments,
# line endings). Each .bfbs is self-contained (flatc flattens included schemas
# into the binary), so SCHEMA2_HASH changes whenever schema1 or schema2 changes,
# and SCHEMA3_HASH changes whenever any of the three files changes.
#
# The pipeline runs in two phases:
#
#   Configure time (cmake):
#     schema1.fbs -flatc -b-> schema1.bfbs -SHA-256-> SCHEMA1_HASH
#     schema2.fbs -flatc -b-> schema2.bfbs -SHA-256-> SCHEMA2_HASH  }--> buildinfo.hpp
#     schema3.fbs -flatc -b-> schema3.bfbs -SHA-256-> SCHEMA3_HASH
#
#   Build time (make):
#     schema3.bfbs -flatc --cpp-> schema3_generated.hpp -copy-> src/schema.hpp
#
# execute_process() is used for the configure phase so that all three hashes are
# available when configure_file() writes buildinfo.hpp. CMAKE_CONFIGURE_DEPENDS
# ensures CMake re-runs configure automatically when any .fbs file is modified.
# ---------------------------------------------------------------------------

# Re-configure when any .fbs file changes (to recompute schema hashes)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
    ${PROJECT_SOURCE_DIR}/src/schema1.fbs
    ${PROJECT_SOURCE_DIR}/src/schema2.fbs
    ${PROJECT_SOURCE_DIR}/src/schema3.fbs
)

# Find flatc executable
find_program(FLATC_EXECUTABLE NAMES flatc REQUIRED)

# Generate binary schemas at configure time and compute their hashes
foreach(_SCHEMA IN ITEMS schema1 schema2 schema3)
    execute_process(
        COMMAND ${FLATC_EXECUTABLE}
            -b --schema
            -o ${CMAKE_CURRENT_BINARY_DIR}
            ${PROJECT_SOURCE_DIR}/src/${_SCHEMA}.fbs
        RESULT_VARIABLE FLATC_BFBS_RESULT
        ERROR_VARIABLE  FLATC_BFBS_ERROR
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/src
    )
    if(NOT FLATC_BFBS_RESULT EQUAL 0)
        message(FATAL_ERROR "flatc -b --schema ${_SCHEMA}.fbs failed: ${FLATC_BFBS_ERROR}")
    endif()
    string(TOUPPER "${_SCHEMA}" _SCHEMA_UPPER)
    file(SHA256 "${CMAKE_CURRENT_BINARY_DIR}/${_SCHEMA}.bfbs" _HASH_FULL)
    string(SUBSTRING "${_HASH_FULL}" 0 8 ${_SCHEMA_UPPER}_HASH)
    message(STATUS "${_SCHEMA_UPPER}_HASH (bfbs): 0x${${_SCHEMA_UPPER}_HASH}")
endforeach()

# Generate buildinfo.hpp
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/buildinfo.hpp.in
    ${CMAKE_CURRENT_BINARY_DIR}/buildinfo.hpp
    @ONLY
)

# Copy buildinfo.hpp to src/buildinfo.hpp
add_custom_command(
    OUTPUT "${PROJECT_SOURCE_DIR}/src/buildinfo.hpp"
    COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.hpp"
        "${PROJECT_SOURCE_DIR}/src/buildinfo.hpp"
    DEPENDS
        "${CMAKE_CURRENT_BINARY_DIR}/buildinfo.hpp"
    COMMENT "Copying buildinfo.hpp to src directory"
)

# Create schema3_generated.hpp from binary schema
set(SCHEMA_GENERATED_HPP "${CMAKE_BINARY_DIR}/flatbuffers_generated/schema3_generated.hpp")
file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/flatbuffers_generated")

add_custom_command(
    OUTPUT ${SCHEMA_GENERATED_HPP}
    COMMAND ${FLATC_EXECUTABLE}
        --cpp
        --filename-ext hpp
        --cpp-std c++17
        --scoped-enums
        -o "${CMAKE_BINARY_DIR}/flatbuffers_generated"
        "${CMAKE_CURRENT_BINARY_DIR}/schema3.bfbs"
    DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/schema3.bfbs"
    COMMENT "Generating schema3_generated.hpp from schema3.bfbs"
)

add_custom_target(
    GENERATE_flatbuffers_generated ALL
    DEPENDS ${SCHEMA_GENERATED_HPP}
)

# Copy schema3_generated.hpp to src/schema.hpp
add_custom_command(
    OUTPUT "${PROJECT_SOURCE_DIR}/src/schema.hpp"
    COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_BINARY_DIR}/flatbuffers_generated/schema3_generated.hpp"
        "${PROJECT_SOURCE_DIR}/src/schema.hpp"
    DEPENDS
        GENERATE_flatbuffers_generated
        "${CMAKE_CURRENT_BINARY_DIR}/schema3.bfbs"
    COMMENT "Copying schema.hpp to src directory"
)

# Copy generated files to src/ directory
add_custom_target(
    copy_generated
    ALL
    DEPENDS
        "${PROJECT_SOURCE_DIR}/src/buildinfo.hpp"
        "${PROJECT_SOURCE_DIR}/src/schema.hpp"
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

option(ENABLE_PROFILER "Compile and link code for gprof profiling" OFF)
if(ENABLE_PROFILER)
    add_compile_options(
        -O2
        -pg
    )
    add_link_options(
        -pg
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

add_dependencies(
    nplex_lib
    copy_generated
)

set_target_properties(
    nplex_lib
    PROPERTIES
    OUTPUT_NAME nplex
)

target_sources(
    nplex_lib
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/deps/cppcrc.h
    ${CMAKE_CURRENT_LIST_DIR}/deps/cqueue.hpp
    ${CMAKE_CURRENT_LIST_DIR}/deps/cstring.hpp
    ${CMAKE_CURRENT_LIST_DIR}/deps/ini.c
    ${CMAKE_CURRENT_LIST_DIR}/deps/ini.h
    ${CMAKE_CURRENT_LIST_DIR}/deps/journal.c
    ${CMAKE_CURRENT_LIST_DIR}/deps/journal.h
    ${CMAKE_CURRENT_LIST_DIR}/deps/match.c
    ${CMAKE_CURRENT_LIST_DIR}/deps/match.h
    ${CMAKE_CURRENT_LIST_DIR}/deps/utf8.h
    ${CMAKE_CURRENT_LIST_DIR}/src/addr.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/addr.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/buildinfo.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/common.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/common.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/config.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/config.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/connection.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/connection.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/context.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/context.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/exception.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/journal_writer.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/journal_writer.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/json.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/json.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/messaging.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/messaging.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/params.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/schema.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/schema1.fbs
    ${CMAKE_CURRENT_LIST_DIR}/src/schema2.fbs
    ${CMAKE_CURRENT_LIST_DIR}/src/schema3.fbs
    ${CMAKE_CURRENT_LIST_DIR}/src/server.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/server.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/session.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/session.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/storage.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/storage.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/store.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/store.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/tasks.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/tasks.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/user.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/user.hpp
    ${CMAKE_CURRENT_LIST_DIR}/src/utils.cpp
    ${CMAKE_CURRENT_LIST_DIR}/src/utils.hpp
)

add_executable(
    nplex
    ${CMAKE_CURRENT_LIST_DIR}/src/main.cpp
)

add_executable(
    nplexcat
    ${CMAKE_CURRENT_LIST_DIR}/src/nplexcat.cpp
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
    nplexcat
    PRIVATE
    nplex_lib
)

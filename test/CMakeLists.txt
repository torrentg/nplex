add_executable(
    tests
)

set_target_properties(
    tests
    PROPERTIES
    CXX_CLANG_TIDY ""
)

target_include_directories(
    tests
    PRIVATE
    ${PROJECT_SOURCE_DIR}/deps
    ${PROJECT_SOURCE_DIR}/src
)

target_sources(
    tests
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/addr_test.cpp
    ${CMAKE_CURRENT_LIST_DIR}/exception_test.cpp
    ${CMAKE_CURRENT_LIST_DIR}/journal_catalog_test.cpp
    ${CMAKE_CURRENT_LIST_DIR}/main.cpp
    ${CMAKE_CURRENT_LIST_DIR}/match_test.cpp
    ${CMAKE_CURRENT_LIST_DIR}/utils_test.cpp
)

set_source_files_properties(
    ${CMAKE_CURRENT_LIST_DIR}/messaging_test.cpp
    PROPERTIES 
    COMPILE_OPTIONS "-Wno-conversion"
)

target_link_libraries(
    tests
    "$<$<STREQUAL:${ENABLE_SANITIZERS},ON>:-lasan>"
    "$<$<STREQUAL:${ENABLE_SANITIZERS},ON>:-lubsan>"
    "$<$<STREQUAL:${ENABLE_COVERAGE},ON>:-lgcov>"
    Threads::Threads
    nplex_lib
    fmt::fmt
    flatbuffers::flatbuffers_shared
)

add_test(
    NAME tests
    COMMAND $<TARGET_FILE:tests>
)

cmake_minimum_required(VERSION 3.22)

project(graph_nomemory LANGUAGES CXX)

set(CMAKE_CXX_EXTENSIONS OFF)

set(_TBANK_FORBIDDEN_FAST_MATH "(^|[ \t])(-Ofast|-ffast-math)([ \t]|$)")
foreach(
    _TBANK_FLAG_VARIABLE
    CMAKE_CXX_FLAGS
    CMAKE_CXX_FLAGS_DEBUG
    CMAKE_CXX_FLAGS_RELEASE
    CMAKE_CXX_FLAGS_RELWITHDEBINFO
    CMAKE_CXX_FLAGS_MINSIZEREL
)
    if(DEFINED ${_TBANK_FLAG_VARIABLE})
        string(
            REGEX MATCH
            "${_TBANK_FORBIDDEN_FAST_MATH}"
            _TBANK_FORBIDDEN_FLAG
            "${${_TBANK_FLAG_VARIABLE}}"
        )
        if(_TBANK_FORBIDDEN_FLAG)
            message(
                FATAL_ERROR
                "${_TBANK_FLAG_VARIABLE} contains forbidden numerical flag: "
                "${_TBANK_FORBIDDEN_FLAG}"
            )
        endif()
    endif()
endforeach()
unset(_TBANK_FORBIDDEN_FLAG)
unset(_TBANK_FLAG_VARIABLE)
unset(_TBANK_FORBIDDEN_FAST_MATH)

add_library(tbank_warnings INTERFACE)
target_compile_options(
    tbank_warnings
    INTERFACE
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic;-Wconversion;-Wsign-conversion;-fno-fast-math;-fno-finite-math-only>
)

find_package(Threads REQUIRED)

add_library(
    tbank_parallel
    STATIC
    src/parallel/fixed_executor.cpp
)
target_compile_features(tbank_parallel PUBLIC cxx_std_20)
target_compile_definitions(tbank_parallel PRIVATE _GNU_SOURCE)
target_include_directories(
    tbank_parallel
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(
    tbank_parallel
    PRIVATE
        Threads::Threads
        tbank_warnings
)

add_library(
    tbank_io
    STATIC
    src/io/edge_csv.cpp
    src/io/rank_csv.cpp
)
target_compile_features(tbank_io PUBLIC cxx_std_20)
target_include_directories(
    tbank_io
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(tbank_io PRIVATE tbank_warnings)

add_library(
    tbank_platform
    STATIC
    src/platform/checked_io.cpp
    src/platform/publication.cpp
)
target_compile_features(tbank_platform PUBLIC cxx_std_20)
target_include_directories(
    tbank_platform
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(tbank_platform PRIVATE tbank_warnings)

add_library(
    tbank_storage
    STATIC
    src/storage/binary.cpp
    src/storage/file_reader.cpp
    src/storage/graph.cpp
    src/storage/manifest.cpp
)
target_compile_features(tbank_storage PUBLIC cxx_std_20)
target_include_directories(
    tbank_storage
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(
    tbank_storage
    PRIVATE
        tbank_platform
        tbank_warnings
)

add_library(
    tbank_tasks
    STATIC
    src/tasks/partitioner.cpp
)
target_compile_features(tbank_tasks PUBLIC cxx_std_20)
target_include_directories(
    tbank_tasks
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(tbank_tasks PRIVATE tbank_warnings)
target_link_libraries(tbank_storage PRIVATE tbank_tasks)

add_library(
    tbank_resources
    STATIC
    src/resources/page_rank.cpp
)
target_compile_features(tbank_resources PUBLIC cxx_std_20)
target_include_directories(
    tbank_resources
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(
    tbank_resources
    PRIVATE
        tbank_parallel
        tbank_platform
        tbank_storage
        tbank_warnings
)

add_library(
    tbank_pagerank
    STATIC
    src/pagerank/analyze.cpp
    src/pagerank/config.cpp
    src/pagerank/numerics.cpp
    src/pagerank/parallel_traversal.cpp
    src/pagerank/pagerank.cpp
)
target_compile_features(tbank_pagerank PUBLIC cxx_std_20)
target_include_directories(
    tbank_pagerank
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(
    tbank_pagerank
    PRIVATE
        tbank_io
        tbank_parallel
        tbank_platform
        tbank_resources
        tbank_storage
        tbank_warnings
)

add_library(
    tbank_preprocess
    STATIC
    src/preprocess/compact_edge_merge.cpp
    src/preprocess/compact_edge_runs.cpp
    src/preprocess/counted_graph.cpp
    src/preprocess/ingest.cpp
    src/preprocess/prepare.cpp
    src/preprocess/run_file.cpp
    src/preprocess/vertex_id_merge.cpp
)
target_compile_features(tbank_preprocess PUBLIC cxx_std_20)
target_include_directories(
    tbank_preprocess
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(
    tbank_preprocess
    PRIVATE
        tbank_io
        tbank_platform
        tbank_storage
        tbank_tasks
        tbank_warnings
)

add_library(
    tbank_cli_support
    STATIC
    src/run/cli.cpp
)
target_compile_features(tbank_cli_support PUBLIC cxx_std_20)
target_include_directories(
    tbank_cli_support
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(
    tbank_cli_support
    PRIVATE
        tbank_warnings
)

add_library(
    tbank_run_config
    STATIC
    src/run/config.cpp
)
target_compile_features(tbank_run_config PUBLIC cxx_std_20)
target_include_directories(
    tbank_run_config
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(
    tbank_run_config
    PRIVATE
        tbank_cli_support
        tbank_pagerank
        tbank_preprocess
        tbank_resources
        tbank_warnings
)

add_executable(
    main
    src/main.cpp
)
target_compile_features(main PRIVATE cxx_std_20)
target_link_libraries(
    main
    PRIVATE
        tbank_cli_support
        tbank_pagerank
        tbank_platform
        tbank_preprocess
        tbank_resources
        tbank_run_config
        tbank_warnings
)

include(GNUInstallDirs)
install(
    TARGETS
        main
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

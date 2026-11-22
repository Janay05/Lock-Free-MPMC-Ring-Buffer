cmake_minimum_required(VERSION 3.20)
project(mpmc_ring_buffer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# HFT compilation flags:
# -O3: Maximum compiler optimization
# -march=native: Generate instructions for the host CPU (AVX2, BMI2, etc.)
# -Wall -Wextra -Wpedantic: Strict warnings
# -fno-rtti: Avoid virtual method table/typeinfo overhead
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -O3
        -march=native
        -Wall
        -Wextra
        -Wpedantic
        -fno-rtti
    )
endif()

# Header-only library interface
add_library(mpmc_queue INTERFACE)
target_include_directories(mpmc_queue INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

enable_testing()
add_subdirectory(tests)

# Core-pinned benchmark target
add_executable(bench_pinned benchmarks/bench_pinned.cpp)
target_link_libraries(bench_pinned PRIVATE mpmc_queue)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_libraries(bench_pinned PRIVATE -pthread)
endif()

# ── External Baselines for Comparative Benchmarking ──────────────────────────
include(FetchContent)

# 1. rigtorp::MPMCQueue (Industry reference bounded Vyukov MPMC)
FetchContent_Declare(
    rigtorp_mpmc
    GIT_REPOSITORY https://github.com/rigtorp/MPMCQueue.git
    GIT_TAG        master
)

# 2. moodycamel::ConcurrentQueue (Lock-free unbounded block-based queue)
FetchContent_Declare(
    moodycamel_cq
    GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
    GIT_TAG        master
)

FetchContent_MakeAvailable(rigtorp_mpmc moodycamel_cq)

# Comparative Benchmark Target
add_executable(bench_comparison benchmarks/bench_comparison.cpp)
target_link_libraries(bench_comparison PRIVATE mpmc_queue)
target_include_directories(bench_comparison PRIVATE
    ${rigtorp_mpmc_SOURCE_DIR}/include
    ${moodycamel_cq_SOURCE_DIR}
)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_libraries(bench_comparison PRIVATE -pthread)
    target_compile_options(bench_comparison PRIVATE -Wno-stringop-overflow)
endif()

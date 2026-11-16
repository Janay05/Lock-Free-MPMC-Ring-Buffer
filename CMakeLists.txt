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

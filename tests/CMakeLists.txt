add_executable(unit_tests
    test_unit.cpp
)

target_link_libraries(unit_tests PRIVATE mpmc_queue)

# Stress & Chaos Test Target
add_executable(stress_tests
    test_stress.cpp
)
target_link_libraries(stress_tests PRIVATE mpmc_queue)

# Enable pthread on MinGW/GCC for multi-threading
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_libraries(unit_tests PRIVATE -pthread)
    target_link_libraries(stress_tests PRIVATE -pthread)
endif()

add_test(NAME unit_tests COMMAND unit_tests)
add_test(NAME stress_tests COMMAND stress_tests)

# ThreadSanitizer targets for POSIX CI (Linux/Clang/GCC)
if (NOT WIN32 AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    foreach(suite unit stress)
        add_executable(tsan_${suite}_tests test_${suite}.cpp)
        target_link_libraries(tsan_${suite}_tests PRIVATE mpmc_queue -pthread)
        target_compile_options(tsan_${suite}_tests PRIVATE -fsanitize=thread -g -O1)
        target_link_options(tsan_${suite}_tests PRIVATE -fsanitize=thread)
        add_test(NAME tsan_${suite}_tests COMMAND tsan_${suite}_tests)
    endforeach()
endif()

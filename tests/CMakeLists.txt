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

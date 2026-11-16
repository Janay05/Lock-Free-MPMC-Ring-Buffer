#pragma once

#include <cstddef>
#include <cstdint>
#include <new>

#if defined(_MSC_VER)
    #include <intrin.h>
#else
    #include <x86intrin.h>
    #include <immintrin.h>
#endif

namespace mpmc {

// Hardware L1 Data Cache line size on modern x86-64 processors.
// While C++17 introduces std::hardware_destructive_interference_size, some compilers
// issue warnings or define it inconsistently. 64 bytes is the standard line size for x86.
inline constexpr std::size_t kCacheLineSize = 64;

// Cache line prefetch hints.
// In low latency architectures, prefetching the next slot cache line into L1D
// hides memory bus and L2/L3 access latency before the core executes CAS.
namespace cache {

    // Prefetch for reading with highest temporal locality (L1 cache)
    [[maybe_unused]] inline void prefetch_read(const void* ptr) noexcept {
#if defined(__GNUC__) || defined(__clang__)
        __builtin_prefetch(ptr, 0, 3);
#elif defined(_MSC_VER)
        _mm_prefetch(static_cast<const char*>(ptr), _MM_HINT_T0);
#endif
    }

    // Prefetch for writing (RFO - Request For Ownership) with highest temporal locality
    // Crucial for producers claiming slots, bringing line into Exclusive/Modified state (MESI/MOESI)
    [[maybe_unused]] inline void prefetch_write(const void* ptr) noexcept {
#if defined(__GNUC__) || defined(__clang__)
        __builtin_prefetch(ptr, 1, 3);
#elif defined(_MSC_VER)
        _mm_prefetch(static_cast<const char*>(ptr), _MM_HINT_T0);
#endif
    }

    // Low-overhead CPU cycle counter (RDTSC)
    [[nodiscard]] inline uint64_t rdtsc() noexcept {
#if defined(_MSC_VER)
        return __rdtsc();
#else
        return static_cast<uint64_t>(__rdtsc());
#endif
    }

    // Serializing cycle counter (RDTSCP + CPUID / fence equivalent)
    [[nodiscard]] inline uint64_t rdtscp() noexcept {
        unsigned int aux;
#if defined(_MSC_VER)
        return __rdtscp(&aux);
#else
        return static_cast<uint64_t>(__rdtscp(&aux));
#endif
    }

    // Hardware pause hint: translates to `rep nop` / `pause` instruction on x86.
    // De-pipelines execution in tight spin loops, preventing memory order mispredictions
    // and reducing core thermal/power consumption without yielding to the OS scheduler.
    inline void cpu_relax() noexcept {
#if defined(_MSC_VER) || defined(__GNUC__) || defined(__clang__)
        _mm_pause();
#else
        #warning "Unknown architecture for cpu_relax"
#endif
    }

} // namespace cache

} // namespace mpmc

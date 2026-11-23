#pragma once

#include <cstddef>
#include <cstdint>
#include <new>

// ── Platform detection ──────────────────────────────────────────────────────
// Pull in architecture-specific intrinsic headers.
// _mm_pause  : x86-64 (Intel/AMD)   – REP NOP (opcode F3 90)
// YIELD      : AArch64 (ARM64)       – WFE-lite spin hint
// NOP        : RISC-V / unknown      – safe no-op fallback
// ───────────────────────────────────────────────────────────────────────────
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    // x86 / x86-64 ────────────────────────────────────────────────────────
    #if defined(_MSC_VER)
        #include <intrin.h>
    #else
        #include <x86intrin.h>
        #include <immintrin.h>
    #endif
    #define MPMC_ARCH_X86 1

#elif defined(__aarch64__) || defined(_M_ARM64)
    // ARM64 (Apple Silicon, AWS Graviton, Qualcomm, NVIDIA Orin, etc.) ─────
    // No extra header needed; __asm__ is available in all GCC/Clang AArch64.
    #define MPMC_ARCH_ARM64 1

#elif defined(__riscv)
    // RISC-V – no ISA-specified pause hint yet; falls back to NOP ──────────
    #define MPMC_ARCH_RISCV 1

#else
    #error "Unsupported architecture: add a cpu_relax() hint for your ISA in cache.hpp"
#endif

namespace mpmc {

// Hardware L1 Data Cache line size on modern x86-64 / AArch64 processors.
// While C++17 introduces std::hardware_destructive_interference_size, some compilers
// issue warnings or define it inconsistently. 64 bytes is correct for x86 and all
// current ARM64 server/desktop/mobile SoCs.
inline constexpr std::size_t kCacheLineSize = 64;

namespace cache {

    // ── Prefetch for reading ────────────────────────────────────────────────
    // Brings the cache line at ptr into L1D (highest temporal locality).
    // Used by consumers before the slot's sequence number becomes ready.
    [[maybe_unused]] inline void prefetch_read(const void* ptr) noexcept {
#if defined(MPMC_ARCH_X86)
    #if defined(__GNUC__) || defined(__clang__)
        __builtin_prefetch(ptr, 0, 3);
    #elif defined(_MSC_VER)
        _mm_prefetch(static_cast<const char*>(ptr), _MM_HINT_T0);
    #endif
#elif defined(MPMC_ARCH_ARM64)
        __asm__ volatile("prfm pldl1keep, [%0]" :: "r"(ptr));
#else
        (void)ptr; // No prefetch available; ignore.
#endif
    }

    // ── Prefetch for writing (RFO – Request For Ownership) ─────────────────
    // Issues an RFO to bring the line into Exclusive/Modified state (MESI).
    // Hides L2/L3 latency before the producer issues the atomic CAS.
    [[maybe_unused]] inline void prefetch_write(const void* ptr) noexcept {
#if defined(MPMC_ARCH_X86)
    #if defined(__GNUC__) || defined(__clang__)
        __builtin_prefetch(ptr, 1, 3);
    #elif defined(_MSC_VER)
        _mm_prefetch(static_cast<const char*>(ptr), _MM_HINT_T0);
    #endif
#elif defined(MPMC_ARCH_ARM64)
        __asm__ volatile("prfm pstl1keep, [%0]" :: "r"(ptr));
#else
        (void)ptr;
#endif
    }

    // ── RDTSC: invariant hardware cycle counter ─────────────────────────────
    // On modern Intel/AMD CPUs the TSC is invariant (constant rate regardless
    // of P-state / turbo). It measures wall-clock time scaled to a fixed
    // reference frequency printed by the kernel as "tsc:" in /proc/cpuinfo.
    // On AArch64 the equivalent is CNTVCT_EL0 (virtual counter, 64-bit).
    // These values are wall-clock ticks, NOT CPU cycles at a variable clock.
    [[nodiscard]] inline uint64_t rdtsc() noexcept {
#if defined(MPMC_ARCH_X86)
    #if defined(_MSC_VER)
        return __rdtsc();
    #else
        return static_cast<uint64_t>(__rdtsc());
    #endif
#elif defined(MPMC_ARCH_ARM64)
        uint64_t val;
        __asm__ volatile("mrs %0, cntvct_el0" : "=r"(val));
        return val;
#else
        return 0; // Fallback; latency measurements will be zeroed.
#endif
    }

    // ── RDTSCP: serializing cycle counter ───────────────────────────────────
    // Prevents out-of-order execution from pushing the RDTSC across the
    // measurement boundary. Use rdtscp() at the START of a timed region and
    // rdtsc() (or lfence+rdtsc) at the END to get a one-sided fence.
    [[nodiscard]] inline uint64_t rdtscp() noexcept {
#if defined(MPMC_ARCH_X86)
        unsigned int aux;
    #if defined(_MSC_VER)
        return __rdtscp(&aux);
    #else
        return static_cast<uint64_t>(__rdtscp(&aux));
    #endif
#elif defined(MPMC_ARCH_ARM64)
        // isb is the AArch64 instruction-serialization barrier – the nearest
        // equivalent to RDTSCP's pipeline serialization effect.
        uint64_t val;
        __asm__ volatile("isb; mrs %0, cntvct_el0" : "=r"(val));
        return val;
#else
        return 0;
#endif
    }

    // ── Serialized Timing Boundaries (Intel IA-32/64 Benchmarking Standard) ──
    // rdtsc_start(): LFENCE; RDTSC; LFENCE prevents prior/subsequent instruction leakage.
    // rdtsc_end():   RDTSCP; LFENCE prevents prior instruction leakage past measurement.
    [[nodiscard]] inline uint64_t rdtsc_start() noexcept {
#if defined(MPMC_ARCH_X86)
    #if defined(_MSC_VER)
        _mm_lfence();
        uint64_t val = __rdtsc();
        _mm_lfence();
        return val;
    #else
        _mm_lfence();
        uint64_t val = static_cast<uint64_t>(__rdtsc());
        _mm_lfence();
        return val;
    #endif
#elif defined(MPMC_ARCH_ARM64)
        uint64_t val;
        __asm__ volatile("isb; mrs %0, cntvct_el0; isb" : "=r"(val) :: "memory");
        return val;
#else
        return rdtsc();
#endif
    }

    [[nodiscard]] inline uint64_t rdtsc_end() noexcept {
#if defined(MPMC_ARCH_X86)
        unsigned int aux;
    #if defined(_MSC_VER)
        uint64_t val = __rdtscp(&aux);
        _mm_lfence();
        return val;
    #else
        uint64_t val = static_cast<uint64_t>(__rdtscp(&aux));
        _mm_lfence();
        return val;
    #endif
#elif defined(MPMC_ARCH_ARM64)
        uint64_t val;
        __asm__ volatile("isb; mrs %0, cntvct_el0" : "=r"(val) :: "memory");
        return val;
#else
        return rdtsc();
#endif
    }

    // ── cpu_relax(): spin-loop pause hint ───────────────────────────────────
    //
    // x86-64 — PAUSE (REP NOP, opcode F3 90):
    //   Serializes the out-of-order pipeline in the spin loop, reducing the
    //   frequency of speculative memory-ordering violations detected on the
    //   subsequent LOCK CMPXCHG. Without PAUSE, the CPU issues ~20 retires/ns
    //   of speculative loads that all become mispredictions on CAS; PAUSE
    //   inserts a ~140-cycle delay loop serialization, cuts power consumption
    //   in tight hot loops, and signals the memory subsystem to process
    //   pending RFOs. Documented in Intel SDM Vol. 2B, PAUSE reference.
    //
    // AArch64 — YIELD:
    //   A micro-architectural hint that the current thread is in a spin loop.
    //   The CPU may switch to another hardware thread (SMT) or enter a
    //   low-power pipe stage while holding the lock. ARM Architecture Reference
    //   Manual DDI 0487, section C5.9.
    //
    // RISC-V / unknown — NOP:
    //   RISC-V does not yet have a universally implemented spin-loop hint.
    //   The Zihintpause extension (PAUSE = FENCE with bits [27:12]=0) exists
    //   but is not guaranteed. Using NOP is safe.
    //
    // Called on EVERY CAS failure in the hot path. Must be noexcept, inline,
    // and compile to ≤ 2 instructions.
    inline void cpu_relax() noexcept {
#if defined(MPMC_ARCH_X86)
        _mm_pause();
#elif defined(MPMC_ARCH_ARM64)
        __asm__ volatile("yield" ::: "memory");
#elif defined(MPMC_ARCH_RISCV)
        __asm__ volatile("nop" ::: "memory");
#endif
    }

} // namespace cache

} // namespace mpmc

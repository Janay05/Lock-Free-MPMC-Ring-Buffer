#pragma once

#include "mpmc/affinity.hpp"
#include "mpmc/cache.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace bench {

inline constexpr std::size_t kThroughputRuns = 11;

// TSC period measured against steady_clock over 500 ms.
inline double ns_per_tick() {
    static const double value = [] {
        using clock = std::chrono::steady_clock;
        const auto t0 = clock::now();
        const uint64_t c0 = mpmc::cache::rdtsc_start();
        while (clock::now() - t0 < std::chrono::milliseconds(500)) {}
        const uint64_t c1 = mpmc::cache::rdtsc_end();
        const auto t1 = clock::now();
        return std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(c1 - c0);
    }();
    return value;
}

// Pin to a distinct physical core. Hosts with too few cores (CI runners) wrap
// around and say so, since their numbers are not meaningful.
inline void pin(std::size_t index) {
    static const std::size_t cores = mpmc::affinity::physical_core_cpus().size();
    if (cores == 0) return;
    if (index >= cores) {
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true)) {
            std::fprintf(stderr, "WARNING: only %zu physical cores; threads share cores, results invalid\n", cores);
        }
    }
    mpmc::affinity::pin_current_thread_to_physical_core(static_cast<uint32_t>(index % cores));
}

inline void print_host() {
    std::printf("Physical cores available (logical CPU ids):");
    for (uint32_t cpu : mpmc::affinity::physical_core_cpus()) std::printf(" %u", cpu);
    std::printf("\nMeasured TSC period: %.6f ns/tick (%.4f GHz)\n\n", ns_per_tick(), 1.0 / ns_per_tick());
    std::fflush(stdout);
}

// All workers spin here; the last to arrive timestamps and releases them, so
// thread creation is outside the timed region.
struct StartGate {
    explicit StartGate(std::size_t n) : parties(n) {}
    void arrive() {
        if (arrived.fetch_add(1, std::memory_order_acq_rel) + 1 == parties) {
            start = std::chrono::steady_clock::now();
            open.store(true, std::memory_order_release);
        }
        while (!open.load(std::memory_order_acquire)) mpmc::cache::cpu_relax();
    }
    // Call only after joining the workers.
    double seconds_since_open() const {
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    }
    const std::size_t parties;
    std::atomic<std::size_t> arrived{0};
    std::atomic<bool> open{false};
    std::chrono::steady_clock::time_point start{};
};

struct Summary {
    double median{0}, min{0}, max{0};
};

template <typename Fn>
Summary repeat(Fn&& run) {
    std::vector<double> v;
    for (std::size_t i = 0; i < kThroughputRuns; ++i) v.push_back(run());
    std::sort(v.begin(), v.end());
    return {v[v.size() / 2], v.front(), v.back()};
}

inline std::string format(const Summary& s) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.1f [%.1f-%.1f]", s.median, s.min, s.max);
    return buf;
}

} // namespace bench

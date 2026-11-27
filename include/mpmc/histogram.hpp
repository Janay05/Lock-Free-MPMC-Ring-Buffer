#pragma once

#include "cache.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <algorithm>
#include <numeric>
#include <iostream>
#include <iomanip>
#include <string>

namespace mpmc {

/**
 * Atomic Latency Histogram
 *
 * Designed for sub-nanosecond / cycle-accurate latency profiling in HFT pipelines.
 * Features:
 *  1. Zero dynamic allocations after initialization.
 *  2. O(1) record operation: uses CPU hardware instruction `clz` (count leading zeros)
 *     via __builtin_clzll / _BitScanReverse64. No division (`idiv`), no locks.
 *  3. Sub-bucket logarithmic quantization: maintains constant relative precision (~6%)
 *     from 1 cycle up to billions of cycles.
 *  4. Thread-safe recording via relaxed atomic increments.
 */
class LatencyHistogram {
public:
    // 64 powers of two * 16 sub-buckets per octave = 1024 buckets
    static constexpr std::size_t kSubBucketBits = 4;
    static constexpr std::size_t kSubBuckets = 1ULL << kSubBucketBits; // 16
    static constexpr std::size_t kSubBucketMask = kSubBuckets - 1;
    static constexpr std::size_t kNumBuckets = 64 * kSubBuckets;       // 1024

    LatencyHistogram() noexcept {
        reset();
    }

    void reset() noexcept {
        for (std::size_t i = 0; i < kNumBuckets; ++i) {
            buckets_[i].store(0, std::memory_order_relaxed);
        }
        total_count_.store(0, std::memory_order_relaxed);
        total_sum_.store(0, std::memory_order_relaxed);
        min_val_.store(UINT64_MAX, std::memory_order_relaxed);
        max_val_.store(0, std::memory_order_relaxed);
    }

    /**
     * Map a 64-bit value to a bucket index in O(1) without integer division.
     */
    [[nodiscard]] static constexpr std::size_t value_to_bucket(uint64_t val) noexcept {
        if (__builtin_expect(val == 0, 0)) {
            return 0;
        }

#if defined(_MSC_VER) && !defined(__clang__)
        unsigned long msb;
        _BitScanReverse64(&msb, val);
#else
        uint32_t msb = 63 - __builtin_clzll(val);
#endif

        if (msb < kSubBucketBits) {
            return static_cast<std::size_t>(val);
        }

        uint32_t sub_bucket = (val >> (msb - kSubBucketBits)) & kSubBucketMask;
        return static_cast<std::size_t>((msb << kSubBucketBits) | sub_bucket);
    }

    /**
     * Approximate inverse mapping: return the representative value for a bucket index.
     */
    [[nodiscard]] static constexpr uint64_t bucket_to_value(std::size_t bucket) noexcept {
        if (bucket < kSubBuckets) {
            return static_cast<uint64_t>(bucket);
        }
        uint32_t msb = static_cast<uint32_t>(bucket >> kSubBucketBits);
        uint32_t sub_bucket = static_cast<uint32_t>(bucket & kSubBucketMask);
        return (1ULL << msb) | (static_cast<uint64_t>(sub_bucket) << (msb - kSubBucketBits));
    }

    /**
     * Record a latency observation (e.g. cycles or nanoseconds).
     * O(1), no locks, no syscalls.
     */
    inline void record(uint64_t value) noexcept {
        std::size_t b = value_to_bucket(value);
        buckets_[b].fetch_add(1, std::memory_order_relaxed);
        total_count_.fetch_add(1, std::memory_order_relaxed);
        total_sum_.fetch_add(value, std::memory_order_relaxed);

        // Update min value without locks
        uint64_t current_min = min_val_.load(std::memory_order_relaxed);
        while (value < current_min && 
               !min_val_.compare_exchange_weak(current_min, value, std::memory_order_relaxed)) {
        }

        // Update max value without locks
        uint64_t current_max = max_val_.load(std::memory_order_relaxed);
        while (value > current_max && 
               !max_val_.compare_exchange_weak(current_max, value, std::memory_order_relaxed)) {
        }
    }

    [[nodiscard]] uint64_t count() const noexcept {
        return total_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t min() const noexcept {
        uint64_t m = min_val_.load(std::memory_order_relaxed);
        return (m == UINT64_MAX) ? 0 : m;
    }

    [[nodiscard]] uint64_t max() const noexcept {
        return max_val_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] double mean() const noexcept {
        uint64_t c = count();
        return (c == 0) ? 0.0 : (static_cast<double>(total_sum_.load(std::memory_order_relaxed)) / c);
    }

    /**
     * Compute the value at the given percentile (0.0 to 100.0).
     */
    [[nodiscard]] uint64_t percentile(double p) const noexcept {
        uint64_t total = count();
        if (total == 0) return 0;
        if (p <= 0.0) return min();
        if (p >= 100.0) return max();

        uint64_t target_rank = static_cast<uint64_t>(std::ceil((p / 100.0) * total));
        uint64_t accumulated = 0;

        for (std::size_t i = 0; i < kNumBuckets; ++i) {
            accumulated += buckets_[i].load(std::memory_order_relaxed);
            if (accumulated >= target_rank) {
                return bucket_to_value(i);
            }
        }
        return max();
    }

    void print_summary(std::ostream& os = std::cout, const std::string& unit = "cycles") const {
        uint64_t total = count();
        if (total == 0) {
            os << "  Histogram is empty.\n";
            return;
        }

        auto prev_flags = os.flags();
        auto prev_prec = os.precision();

        os << std::fixed << std::setprecision(2);
        os << "  Samples:  " << total << "\n";
        os << "  Min:      " << min() << " " << unit << "\n";
        os << "  Mean:     " << mean() << " " << unit << "\n";
        os << "  p50:      " << percentile(50.0) << " " << unit << "\n";
        os << "  p90:      " << percentile(90.0) << " " << unit << "\n";
        os << "  p99:      " << percentile(99.0) << " " << unit << "\n";
        os << "  p99.9:    " << percentile(99.9) << " " << unit << "\n";
        os << "  p99.99:   " << percentile(99.99) << " " << unit << "\n";
        os << "  Max:      " << max() << " " << unit << "\n";

        os.flags(prev_flags);
        os.precision(prev_prec);
    }

private:
    alignas(kCacheLineSize) std::atomic<uint64_t> buckets_[kNumBuckets];
    alignas(kCacheLineSize) std::atomic<uint64_t> total_count_{0};
    std::atomic<uint64_t> total_sum_{0};
    std::atomic<uint64_t> min_val_{UINT64_MAX};
    std::atomic<uint64_t> max_val_{0};
};

} // namespace mpmc

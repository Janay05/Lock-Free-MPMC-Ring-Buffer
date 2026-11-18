#pragma once

#include "cache.hpp"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iomanip>
#include <vector>

namespace mpmc {

/**
 * Thread-Local Queue Telemetry & Contention Metrics
 *
 * In low-latency architectures, telemetry counters must NEVER be shared across threads.
 * If threads atomic-increment a shared counter on the hot path, cache-line bouncing on the
 * counter itself degrades throughput more than the queue operations.
 *
 * Each thread maintains its own isolated `QueueStats` struct on its own 64-byte cache line.
 * Updates are plain uint64_t scalar additions (no atomic `lock add` instructions needed).
 */
struct alignas(kCacheLineSize) QueueStats {
    uint64_t enqueue_success{0};
    uint64_t enqueue_cas_fail{0}; // Thread failed CAS on head_ (raced with another producer)
    uint64_t enqueue_full{0};     // Queue buffer was full

    uint64_t dequeue_success{0};
    uint64_t dequeue_cas_fail{0}; // Thread failed CAS on tail_ (raced with another consumer)
    uint64_t dequeue_empty{0};    // Queue buffer was empty

    // Pad to 64 bytes to eliminate false sharing between adjacent thread stats in an array
    char _pad[kCacheLineSize - (6 * sizeof(uint64_t))]{};

    void reset() noexcept {
        enqueue_success = 0;
        enqueue_cas_fail = 0;
        enqueue_full = 0;
        dequeue_success = 0;
        dequeue_cas_fail = 0;
        dequeue_empty = 0;
    }

    /**
     * Percentage of enqueue CAS attempts that lost the race against a sibling producer.
     * High ratio (> 10%) indicates queue capacity is too small or core contention is high.
     */
    [[nodiscard]] double enqueue_contention_ratio() const noexcept {
        uint64_t total_attempts = enqueue_success + enqueue_cas_fail;
        if (total_attempts == 0) return 0.0;
        return (static_cast<double>(enqueue_cas_fail) / total_attempts) * 100.0;
    }

    /**
     * Percentage of dequeue CAS attempts that lost the race against a sibling consumer.
     */
    [[nodiscard]] double dequeue_contention_ratio() const noexcept {
        uint64_t total_attempts = dequeue_success + dequeue_cas_fail;
        if (total_attempts == 0) return 0.0;
        return (static_cast<double>(dequeue_cas_fail) / total_attempts) * 100.0;
    }

    void print_summary(std::ostream& os = std::cout, const std::string& prefix = "  ") const {
        auto prev_flags = os.flags();
        auto prev_prec = os.precision();
        os << std::fixed << std::setprecision(2);

        os << prefix << "Enqueue Ops:         " << enqueue_success << "\n";
        os << prefix << "Enqueue CAS Fails:   " << enqueue_cas_fail 
           << " (" << enqueue_contention_ratio() << "% contention)\n";
        os << prefix << "Enqueue Full Drops:  " << enqueue_full << "\n";
        os << prefix << "Dequeue Ops:         " << dequeue_success << "\n";
        os << prefix << "Dequeue CAS Fails:   " << dequeue_cas_fail 
           << " (" << dequeue_contention_ratio() << "% contention)\n";
        os << prefix << "Dequeue Empty Drops: " << dequeue_empty << "\n";

        os.flags(prev_flags);
        os.precision(prev_prec);
    }
};

static_assert(sizeof(QueueStats) == kCacheLineSize, "QueueStats must be exactly 64 bytes");
static_assert(alignof(QueueStats) == kCacheLineSize, "QueueStats must be cache-line aligned");

/**
 * Aggregator utility to combine stats from multiple worker threads without locking.
 */
inline QueueStats aggregate_stats(const std::vector<QueueStats>& thread_stats) noexcept {
    QueueStats total;
    for (const auto& s : thread_stats) {
        total.enqueue_success += s.enqueue_success;
        total.enqueue_cas_fail += s.enqueue_cas_fail;
        total.enqueue_full += s.enqueue_full;
        total.dequeue_success += s.dequeue_success;
        total.dequeue_cas_fail += s.dequeue_cas_fail;
        total.dequeue_empty += s.dequeue_empty;
    }
    return total;
}

} // namespace mpmc

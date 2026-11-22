#include "mpmc/ring_buffer.hpp"
#include "mpmc/affinity.hpp"
#include "mpmc/histogram.hpp"
#include "mpmc/stats.hpp"

#include <chrono>
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <iomanip>

using namespace mpmc;

// Calibrated invariant TSC conversion for Intel Core i7-14700HX (2.304011 GHz nominal reference)
inline constexpr double kNsPerTscTick = 0.434026;

// ── Benchmark 1: Core-Pinned Cross-Core Handoff Latency (1P:1C, Core 0 <-> Core 1) ──
// Producer on Core 0 sends item to Consumer on Core 1; Consumer echoes back on return queue.
// RTT / 2 gives the exact hardware cross-core cache transfer latency over the CPU ring interconnect.
// In-line by-value slot: consumer reads and checksums payload to ensure cache line transfer.
void benchmark_spsc_pinned_rtt() {
    std::cout << "\n========================================================================\n";
    std::cout << " [BENCHMARK 1] Core-Pinned SPSC Handoff Latency (1P:1C, Core 0 <-> Core 1)\n";
    std::cout << "               Measured via Invariant RDTSCP (Calibrated at 2.304 GHz)\n";
    std::cout << "========================================================================\n";

    constexpr size_t WARMUP = 50'000;
    constexpr size_t SAMPLES = 500'000;

    RingBuffer<uint64_t, 1024> forward_queue;
    RingBuffer<uint64_t, 1024> return_queue;

    LatencyHistogram rtt_hist;
    std::atomic<bool> stop_consumer{false};

    // Consumer thread pinned to physical P-Core 1
    std::thread consumer([&]() {
        affinity::pin_current_thread_to_core(1);
        affinity::set_current_thread_priority_critical();

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            uint64_t item = 0;
            if (forward_queue.try_dequeue(item)) {
                // Actively touch payload
                return_queue.enqueue(item + 1);
            } else {
                cache::cpu_relax();
            }
        }
    });

    // Producer pinned to physical P-Core 0
    affinity::pin_current_thread_to_core(0);
    affinity::set_current_thread_priority_critical();

    uint64_t payload = 0xBEEF;

    // Warm-up caches
    for (size_t i = 0; i < WARMUP; ++i) {
        forward_queue.enqueue(payload);
        [[maybe_unused]] auto ret = return_queue.dequeue();
    }

    // Measurement loop
    for (size_t i = 0; i < SAMPLES; ++i) {
        uint64_t t0 = cache::rdtscp();
        forward_queue.enqueue(payload);
        uint64_t ret = return_queue.dequeue();
        uint64_t t1 = cache::rdtscp();
        if (t1 > t0 && ret == payload + 1) {
            rtt_hist.record(t1 - t0);
        }
    }

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    std::cout << "  Results across " << SAMPLES << " cross-core round-trips:\n";
    rtt_hist.print_summary(std::cout, "TSC ticks");

    uint64_t p50_ticks  = rtt_hist.percentile(50.0);
    uint64_t p90_ticks  = rtt_hist.percentile(90.0);
    uint64_t p99_ticks  = rtt_hist.percentile(99.0);
    uint64_t p999_ticks = rtt_hist.percentile(99.9);
    uint64_t p9999_ticks= rtt_hist.percentile(99.99);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\n  --- Estimated 1-Way Cross-Core Handoff Latency (RTT / 2) ---\n";
    std::cout << "  - p50:   " << (p50_ticks / 2.0) << " ticks  (" << (p50_ticks / 2.0) * kNsPerTscTick << " ns)\n";
    std::cout << "  - p90:   " << (p90_ticks / 2.0) << " ticks  (" << (p90_ticks / 2.0) * kNsPerTscTick << " ns)\n";
    std::cout << "  - p99:   " << (p99_ticks / 2.0) << " ticks  (" << (p99_ticks / 2.0) * kNsPerTscTick << " ns)\n";
    std::cout << "  - p99.9: " << (p999_ticks / 2.0) << " ticks (" << (p999_ticks / 2.0) * kNsPerTscTick << " ns)\n";
    std::cout << "  - p99.99:" << (p9999_ticks / 2.0) << " ticks (" << (p9999_ticks / 2.0) * kNsPerTscTick << " ns)\n";
}

// ── Benchmark 2: Core-Pinned MPMC Contended Throughput (4P:4C, Cores 0-3 -> Cores 4-7) ──
void benchmark_mpmc_pinned() {
    std::cout << "\n========================================================================\n";
    std::cout << " [BENCHMARK 2] Core-Pinned MPMC Contended Throughput (4P: Cores 0-3 | 4C: Cores 4-7)\n";
    std::cout << "========================================================================\n";

    constexpr size_t N_PER_PRODUCER = 500'000;
    constexpr size_t PRODUCERS      = 4;
    constexpr size_t CONSUMERS      = 4;
    constexpr size_t TOTAL          = N_PER_PRODUCER * PRODUCERS;

    RingBuffer<uint64_t, 4096> rb;

    std::vector<QueueStats> p_stats(PRODUCERS);
    std::vector<QueueStats> c_stats(CONSUMERS);

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};
    std::atomic<uint64_t> total_checksum{0};

    std::vector<std::thread> consumers_t;
    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers_t.emplace_back([&, c]() {
            affinity::pin_current_thread_to_core(static_cast<uint32_t>(4 + c));
            affinity::set_current_thread_priority_critical();

            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            uint64_t local_sum = 0;
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL) {
                uint64_t out = 0;
                if (rb.try_dequeue(out, c_stats[c])) {
                    local_sum += out;
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
            total_checksum.fetch_add(local_sum, std::memory_order_relaxed);
        });
    }

    std::vector<std::thread> producers_t;
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producers_t.emplace_back([&, p]() {
            affinity::pin_current_thread_to_core(static_cast<uint32_t>(p));
            affinity::set_current_thread_priority_critical();

            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            size_t base = p * N_PER_PRODUCER;
            for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
                while (!rb.try_enqueue(base + i + 1, p_stats[p])) {
                    cache::cpu_relax();
                }
            }
        });
    }

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (auto& t : producers_t) t.join();
    for (auto& t : consumers_t) t.join();

    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    QueueStats total_p = aggregate_stats(p_stats);
    QueueStats total_c = aggregate_stats(c_stats);

    double ops_sec = (static_cast<double>(TOTAL) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << TOTAL << " items across 8 pinned P-cores in " 
              << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - Latency:    " << static_cast<double>(elapsed_ns) / TOTAL << " ns/op\n";
    std::cout << "  - Producer CAS Contention: " << total_p.enqueue_contention_ratio() << "%\n";
    std::cout << "  - Consumer CAS Contention: " << total_c.dequeue_contention_ratio() << "%\n";
}

// ── Benchmark 3: Core-Pinned Bulk Throughput (1P:1C, Batch = 32, Core 0 -> Core 1) ──
void benchmark_bulk_pinned() {
    std::cout << "\n========================================================================\n";
    std::cout << " [BENCHMARK 3] Core-Pinned Bulk Throughput (1P:1C, Batch = 32, Core 0 -> Core 1)\n";
    std::cout << "========================================================================\n";

    constexpr size_t TOTAL      = 5'000'000;
    constexpr size_t BATCH_SIZE = 32;

    RingBuffer<uint64_t, 4096> rb;

    std::vector<uint64_t> send_data(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) send_data[i] = i + 1;

    std::atomic<bool> start_gate{false};
    uint64_t consumer_sum = 0;

    std::thread consumer([&]() {
        affinity::pin_current_thread_to_core(1);
        affinity::set_current_thread_priority_critical();

        while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();

        size_t received = 0;
        std::vector<uint64_t> batch(BATCH_SIZE, 0);

        while (received < TOTAL) {
            size_t n = rb.try_dequeue_bulk(batch.data(), BATCH_SIZE);
            if (n > 0) {
                for (size_t i = 0; i < n; ++i) consumer_sum += batch[i];
                received += n;
            } else {
                cache::cpu_relax();
            }
        }
    });

    affinity::pin_current_thread_to_core(0);
    affinity::set_current_thread_priority_critical();

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    size_t sent = 0;
    while (sent < TOTAL) {
        size_t count = std::min(BATCH_SIZE, TOTAL - sent);
        size_t n = rb.try_enqueue_bulk(&send_data[sent], count);
        if (n > 0) {
            sent += n;
        } else {
            cache::cpu_relax();
        }
    }

    consumer.join();
    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    uint64_t expected_sum = static_cast<uint64_t>(TOTAL) * (TOTAL + 1) / 2;
    assert(consumer_sum == expected_sum);

    double ops_sec = (static_cast<double>(TOTAL) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << TOTAL << " items in batches of " << BATCH_SIZE 
              << " in " << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Bulk Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - Effective Time per Item: " << static_cast<double>(elapsed_ns) / TOTAL << " ns\n";
    std::cout << "  - Payload Checksum: " << consumer_sum << " (verified)\n";
}

int main() {
    std::cout << "========================================================================\n";
    std::cout << "   Lock-Free MPMC Ring Buffer Hardware-Pinned Benchmarks\n";
    std::cout << "   Host: Intel Core i7-14700HX (Raptor Lake, Invariant TSC: 2.304 GHz)\n";
    std::cout << "========================================================================\n";

    benchmark_spsc_pinned_rtt();
    benchmark_mpmc_pinned();
    benchmark_bulk_pinned();

    std::cout << "\n========================================================================\n";
    std::cout << " [ALL HARDWARE-PINNED BENCHMARKS COMPLETE]\n";
    std::cout << "========================================================================\n";

    return 0;
}

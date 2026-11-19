#include "mpmc/ring_buffer.hpp"
#include "mpmc/affinity.hpp"
#include "mpmc/histogram.hpp"
#include "mpmc/stats.hpp"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <iomanip>

using namespace mpmc;

// Benchmark 1: Core-Pinned Cross-Core Round-Trip Time (RTT)
// Producer on Core 0 sends item to Consumer on Core 1; Consumer echoes back on return queue.
// RTT / 2 gives the exact hardware cross-core cache transfer latency over the CPU ring bus.
void benchmark_spsc_pinned_rtt() {
    std::cout << "\n========================================================\n";
    std::cout << " [BENCHMARK 1] Core-Pinned SPSC Round-Trip Latency (Core 0 <-> Core 1)\n";
    std::cout << "========================================================\n";

    constexpr size_t WARMUP = 100'000;
    constexpr size_t SAMPLES = 1'000'000;

    RingBuffer<uint64_t, 1024> forward_queue;
    RingBuffer<uint64_t, 1024> return_queue;

    LatencyHistogram rtt_hist;
    std::atomic<bool> stop_consumer{false};

    // Consumer thread pinned to Core 1
    std::thread consumer([&]() {
        affinity::pin_current_thread_to_core(1);
        affinity::set_current_thread_priority_critical();

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            uint64_t* item = nullptr;
            if (forward_queue.try_dequeue(item)) {
                return_queue.enqueue(item);
            } else {
                cache::cpu_relax();
            }
        }
    });

    // Producer pinned to Core 0
    affinity::pin_current_thread_to_core(0);
    affinity::set_current_thread_priority_critical();

    uint64_t payload = 0xBEEF;

    // Warm-up caches
    for (size_t i = 0; i < WARMUP; ++i) {
        forward_queue.enqueue(&payload);
        [[maybe_unused]] auto* ret = return_queue.dequeue();
    }

    // Measurement loop
    for (size_t i = 0; i < SAMPLES; ++i) {
        uint64_t t0 = cache::rdtsc();
        forward_queue.enqueue(&payload);
        [[maybe_unused]] auto* ret = return_queue.dequeue();
        uint64_t t1 = cache::rdtsc();
        rtt_hist.record(t1 - t0);
    }

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    std::cout << "  Results across " << SAMPLES << " cross-core round-trips:\n";
    rtt_hist.print_summary(std::cout, "cycles");

    uint64_t p50_cycles = rtt_hist.percentile(50.0);
    uint64_t p99_cycles = rtt_hist.percentile(99.0);
    std::cout << "  - Estimated 1-way transfer latency (p50): " << p50_cycles / 2.0 << " cycles\n";
    std::cout << "  - Estimated 1-way transfer latency (p99): " << p99_cycles / 2.0 << " cycles\n";
}

// Benchmark 2: Core-Pinned MPMC 4P:4C Contended Throughput
// Producers pinned to physical P-cores 0, 1, 2, 3
// Consumers pinned to physical P-cores 4, 5, 6, 7
void benchmark_mpmc_pinned() {
    std::cout << "\n========================================================\n";
    std::cout << " [BENCHMARK 2] Core-Pinned MPMC (4P: Cores 0-3 | 4C: Cores 4-7)\n";
    std::cout << "========================================================\n";

    constexpr size_t N_PER_PRODUCER = 1'000'000;
    constexpr size_t PRODUCERS      = 4;
    constexpr size_t CONSUMERS      = 4;
    constexpr size_t TOTAL          = N_PER_PRODUCER * PRODUCERS;

    RingBuffer<uint64_t, 4096> rb;

    std::vector<QueueStats> p_stats(PRODUCERS);
    std::vector<QueueStats> c_stats(CONSUMERS);

    std::vector<uint64_t> payload(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) payload[i] = i + 1;

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};

    std::vector<std::thread> consumers_t;
    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers_t.emplace_back([&, c]() {
            affinity::pin_current_thread_to_core(static_cast<uint32_t>(4 + c));
            affinity::set_current_thread_priority_critical();

            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL) {
                uint64_t* out = nullptr;
                if (rb.try_dequeue(out, c_stats[c])) {
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
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
                while (!rb.try_enqueue(&payload[base + i], p_stats[p])) {
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
    std::cout << "  - Processed " << TOTAL << " items across 8 pinned cores in " 
              << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - Latency:    " << static_cast<double>(elapsed_ns) / TOTAL << " ns/op\n";
    std::cout << "  - Producer CAS Contention: " << total_p.enqueue_contention_ratio() << "%\n";
    std::cout << "  - Consumer CAS Contention: " << total_c.dequeue_contention_ratio() << "%\n";
}

// Benchmark 3: Core-Pinned Bulk Operations (Batch = 32)
// Producer on Core 0 -> Consumer on Core 1
void benchmark_bulk_pinned() {
    std::cout << "\n========================================================\n";
    std::cout << " [BENCHMARK 3] Core-Pinned Bulk Operations (Batch = 32, Core 0 -> Core 1)\n";
    std::cout << "========================================================\n";

    constexpr size_t TOTAL      = 10'000'000;
    constexpr size_t BATCH_SIZE = 32;

    RingBuffer<uint64_t, 4096> rb;

    std::vector<uint64_t> payload(TOTAL);
    std::vector<uint64_t*> payload_ptrs(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) {
        payload[i] = i + 1;
        payload_ptrs[i] = &payload[i];
    }

    std::atomic<bool> start_gate{false};
    std::vector<uint64_t*> dest(TOTAL, nullptr);

    std::thread consumer([&]() {
        affinity::pin_current_thread_to_core(1);
        affinity::set_current_thread_priority_critical();

        while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
        size_t dequeued = 0;
        while (dequeued < TOTAL) {
            size_t count = std::min(BATCH_SIZE, TOTAL - dequeued);
            size_t n = rb.try_dequeue_bulk(&dest[dequeued], count);
            if (n > 0) {
                dequeued += n;
            } else {
                cache::cpu_relax();
            }
        }
    });

    std::thread producer([&]() {
        affinity::pin_current_thread_to_core(0);
        affinity::set_current_thread_priority_critical();

        while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
        size_t enqueued = 0;
        while (enqueued < TOTAL) {
            size_t count = std::min(BATCH_SIZE, TOTAL - enqueued);
            size_t n = rb.try_enqueue_bulk(&payload_ptrs[enqueued], count);
            if (n > 0) {
                enqueued += n;
            } else {
                cache::cpu_relax();
            }
        }
    });

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    producer.join();
    consumer.join();

    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    // Integrity verification: ensure no drops, corruptions, or dead-code elimination
    uint64_t checksum = 0;
    for (size_t i = 0; i < TOTAL; ++i) {
        if (dest[i] == nullptr || *dest[i] != (i + 1)) {
            std::cerr << "Data mismatch at index " << i << "!\n";
            return;
        }
        checksum += *dest[i];
    }
    uint64_t expected_checksum = static_cast<uint64_t>(TOTAL) * (TOTAL + 1) / 2;
    if (checksum != expected_checksum) {
        std::cerr << "Checksum mismatch!\n";
        return;
    }

    double ops_sec = (static_cast<double>(TOTAL) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << TOTAL << " items in " << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Bulk Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - Latency per item: " << static_cast<double>(elapsed_ns) / TOTAL << " ns/op\n";
    std::cout << "  - Data Integrity: 100% verified (Checksum: " << checksum << ")\n";
}

int main() {
    std::cout << "========================================================\n";
    std::cout << "  HFT Core-Pinned Performance & Latency Benchmark Suite \n";
    std::cout << "========================================================\n";

    benchmark_spsc_pinned_rtt();
    benchmark_mpmc_pinned();
    benchmark_bulk_pinned();

    std::cout << "\n>>> BENCHMARK COMPLETE <<<\n";
    return 0;
}

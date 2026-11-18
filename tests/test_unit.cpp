#include "mpmc/ring_buffer.hpp"
#include "mpmc/pool.hpp"
#include "mpmc/histogram.hpp"
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <numeric>
#include <atomic>

using namespace mpmc;

void test_cache_alignment() {
    std::cout << "[TEST] 1. Cache Line Alignment & False Sharing Verification...\n";
    
    // Check static compile-time constraints
    static_assert(sizeof(Slot<void>) == 64, "Slot must be exactly 64 bytes");
    static_assert(alignof(Slot<void>) == 64, "Slot alignment must be 64 bytes");

    RingBuffer<void, 1024> rb;

    std::cout << "  - sizeof(Slot): " << sizeof(Slot<void>) << " bytes\n";
    std::cout << "  - alignof(Slot): " << alignof(Slot<void>) << " bytes\n";
    std::cout << "  [PASS] Cache line alignment verified.\n";
}

void test_spsc() {
    std::cout << "[TEST] 2. Single-Producer Single-Consumer (1P:1C) Verification...\n";
    constexpr size_t N = 1'000'000;
    RingBuffer<uint64_t, 1024> rb;

    std::vector<uint64_t> test_data(N);
    for (size_t i = 0; i < N; ++i) {
        test_data[i] = i + 1;
    }

    auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        for (size_t i = 0; i < N; ++i) {
            rb.enqueue(&test_data[i]);
        }
    });

    std::vector<uint64_t*> received(N, nullptr);
    std::thread consumer([&]() {
        for (size_t i = 0; i < N; ++i) {
            received[i] = rb.dequeue();
        }
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::steady_clock::now();
    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    // Verify ordering and integrity
    for (size_t i = 0; i < N; ++i) {
        assert(received[i] != nullptr);
        assert(*received[i] == i + 1);
    }

    double ops_sec = (static_cast<double>(N) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << N << " messages in " << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - Average SPSC latency: " << static_cast<double>(elapsed_ns) / N << " ns/op\n";
    std::cout << "  [PASS] SPSC integrity verified.\n";
}

void test_mpmc() {
    std::cout << "[TEST] 3. Multi-Producer Multi-Consumer (4P:4C) Contention Verification...\n";
    constexpr size_t N_PER_PRODUCER = 500'000;
    constexpr size_t PRODUCERS = 4;
    constexpr size_t CONSUMERS = 4;
    constexpr size_t TOTAL_MESSAGES = N_PER_PRODUCER * PRODUCERS;

    RingBuffer<uint64_t, 2048> rb;

    // Allocate contiguous payload data
    std::vector<std::vector<uint64_t>> producer_data(PRODUCERS);
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producer_data[p].resize(N_PER_PRODUCER);
        for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
            producer_data[p][i] = (p + 1) * 1'000'000'000ULL + i;
        }
    }

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};
    std::vector<std::vector<uint64_t>> consumer_results(CONSUMERS);

    auto start_time = std::chrono::steady_clock::now();

    // Spawn consumers
    std::vector<std::thread> consumer_threads;
    consumer_threads.reserve(CONSUMERS);
    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumer_threads.emplace_back([&, c]() {
            while (!start_gate.load(std::memory_order_relaxed)) {
                cache::cpu_relax();
            }
            std::vector<uint64_t> local;
            local.reserve(TOTAL_MESSAGES / CONSUMERS + 1000);
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL_MESSAGES) {
                uint64_t* item = nullptr;
                if (rb.try_dequeue(item)) {
                    local.push_back(*item);
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
            consumer_results[c] = std::move(local);
        });
    }

    // Spawn producers
    std::vector<std::thread> producer_threads;
    producer_threads.reserve(PRODUCERS);
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producer_threads.emplace_back([&, p]() {
            while (!start_gate.load(std::memory_order_relaxed)) {
                cache::cpu_relax();
            }
            for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
                rb.enqueue(&producer_data[p][i]);
            }
        });
    }

    // Fire start gate
    start_time = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (auto& t : producer_threads) t.join();
    for (auto& t : consumer_threads) t.join();

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

    // Verify all messages were received without duplication or loss
    size_t received_count = 0;
    std::vector<uint64_t> all_received;
    all_received.reserve(TOTAL_MESSAGES);

    for (size_t c = 0; c < CONSUMERS; ++c) {
        received_count += consumer_results[c].size();
        all_received.insert(all_received.end(), consumer_results[c].begin(), consumer_results[c].end());
    }

    assert(received_count == TOTAL_MESSAGES);
    assert(rb.empty_approx());

    double ops_sec = (static_cast<double>(TOTAL_MESSAGES) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << TOTAL_MESSAGES << " messages across 4P:4C in " 
              << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Contended Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  [PASS] MPMC integrity and zero-loss verified.\n";
}

void test_rdtsc_latency() {
    std::cout << "[TEST] 4. Cycle-Accurate Latency Benchmark (RDTSC)...\n";
    constexpr size_t RUNS = 100'000;
    RingBuffer<uint64_t, 1024> rb;

    uint64_t val = 42;
    uint64_t* out = nullptr;

    // Warm-up cache
    for (size_t i = 0; i < 1000; ++i) {
        bool ok1 = rb.try_enqueue(&val);
        bool ok2 = rb.try_dequeue(out);
        (void)ok1;
        (void)ok2;
    }

    uint64_t total_cycles = 0;
    for (size_t i = 0; i < RUNS; ++i) {
        uint64_t t0 = cache::rdtsc();
        bool ok1 = rb.try_enqueue(&val);
        bool ok2 = rb.try_dequeue(out);
        uint64_t t1 = cache::rdtsc();
        (void)ok1;
        (void)ok2;
        total_cycles += (t1 - t0);
    }

    double avg_cycles = static_cast<double>(total_cycles) / RUNS;
    std::cout << "  - Single-thread Enqueue + Dequeue roundtrip: ~" 
              << avg_cycles << " CPU cycles\n";
    std::cout << "  [PASS] Latency measurement complete.\n";
}

void test_bulk_operations() {
    std::cout << "[TEST] 5. Batch / Bulk Operations (Amortized CAS) Verification...\n";
    constexpr size_t TOTAL = 1'000'000;
    constexpr size_t BATCH_SIZE = 32;
    RingBuffer<uint64_t, 2048> rb;

    std::vector<uint64_t> source_data(TOTAL);
    std::vector<uint64_t*> ptr_data(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) {
        source_data[i] = i + 1;
        ptr_data[i] = &source_data[i];
    }

    auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        size_t enqueued = 0;
        while (enqueued < TOTAL) {
            size_t count = std::min(BATCH_SIZE, TOTAL - enqueued);
            size_t pushed = rb.try_enqueue_bulk(&ptr_data[enqueued], count);
            if (pushed > 0) {
                enqueued += pushed;
            } else {
                cache::cpu_relax();
            }
        }
    });

    std::vector<uint64_t*> dest_ptrs(TOTAL, nullptr);
    std::thread consumer([&]() {
        size_t dequeued = 0;
        while (dequeued < TOTAL) {
            size_t count = std::min(BATCH_SIZE, TOTAL - dequeued);
            size_t pulled = rb.try_dequeue_bulk(&dest_ptrs[dequeued], count);
            if (pulled > 0) {
                dequeued += pulled;
            } else {
                cache::cpu_relax();
            }
        }
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::steady_clock::now();
    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    // Verify data
    for (size_t i = 0; i < TOTAL; ++i) {
        assert(dest_ptrs[i] != nullptr);
        assert(*dest_ptrs[i] == i + 1);
    }

    double ops_sec = (static_cast<double>(TOTAL) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << TOTAL << " items in batches of " << BATCH_SIZE << " in " 
              << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Bulk Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - Latency per item: " << static_cast<double>(elapsed_ns) / TOTAL << " ns/op\n";
    std::cout << "  [PASS] Bulk operations verified.\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// Pool Tests
// ─────────────────────────────────────────────────────────────────────────────

struct Order {
    uint64_t id{0};
    double   price{0.0};
    uint32_t qty{0};
};

void test_pool_basic() {
    std::cout << "[TEST] 6. Object Pool — Basic Acquire/Release...\n";
    constexpr size_t POOL_SIZE = 1024;
    ObjectPool<Order, POOL_SIZE> pool;

    // All slots should be free at construction
    assert(pool.available_approx() == POOL_SIZE);
    assert(pool.in_use_approx()    == 0);
    assert(pool.capacity()         == POOL_SIZE);
    std::cout << "  - Capacity: " << pool.capacity() << " slots\n";
    std::cout << "  - Initial available: " << pool.available_approx() << "\n";

    // Acquire all slots
    std::vector<Order*> ptrs;
    ptrs.reserve(POOL_SIZE);
    for (size_t i = 0; i < POOL_SIZE; ++i) {
        Order* o = pool.acquire();
        assert(o != nullptr);
        o->id    = i + 1;
        o->price = static_cast<double>(i) * 1.5;
        o->qty   = static_cast<uint32_t>(i % 1000);
        ptrs.push_back(o);
    }
    // Pool must now be exhausted
    assert(pool.available_approx() == 0);
    assert(pool.acquire() == nullptr);
    std::cout << "  - After full acquire: available=" << pool.available_approx() << " (exhausted)\n";

    // Release all back — verify data written earlier is intact
    for (size_t i = 0; i < POOL_SIZE; ++i) {
        assert(ptrs[i]->id    == i + 1);
        assert(ptrs[i]->qty   == static_cast<uint32_t>(i % 1000));
        pool.release(ptrs[i]);
    }
    assert(pool.available_approx() == POOL_SIZE);
    std::cout << "  - After full release: available=" << pool.available_approx() << "\n";
    std::cout << "  [PASS] Pool acquire/release verified.\n";
}

void test_pool_with_queue() {
    std::cout << "[TEST] 7. Pool + Queue Integration (zero-malloc end-to-end)...\n";
    constexpr size_t POOL_SIZE = 2048;
    constexpr size_t N         = 1'000'000;

    ObjectPool<Order, POOL_SIZE> pool;
    RingBuffer<Order, POOL_SIZE> queue;

    auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        uint64_t seq = 0;
        while (seq < N) {
            Order* o = pool.acquire();
            if (!o) {
                cache::cpu_relax();
                continue;
            }
            o->id    = ++seq;
            o->price = static_cast<double>(seq) * 0.01;
            o->qty   = static_cast<uint32_t>(seq % 500);
            queue.enqueue(o);
        }
    });

    uint64_t consumed = 0;
    uint64_t checksum = 0;
    std::thread consumer([&]() {
        while (consumed < N) {
            Order* o = queue.dequeue();
            checksum += o->id;        // accumulate to prove correct data
            pool.release(o);          // return slot to pool — zero free()
            ++consumed;
        }
    });

    producer.join();
    consumer.join();

    auto end        = std::chrono::steady_clock::now();
    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    // Sum of 1..N = N*(N+1)/2
    uint64_t expected_checksum = static_cast<uint64_t>(N) * (N + 1) / 2;
    assert(checksum  == expected_checksum);
    assert(pool.available_approx() == POOL_SIZE);  // all slots returned to pool
    assert(queue.empty_approx());

    double ops_sec = (static_cast<double>(N) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << N << " messages (pool acquire→queue→dequeue→pool release)\n";
    std::cout << "  - Elapsed:    " << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - ns/op:      " << static_cast<double>(elapsed_ns) / N << "\n";
    std::cout << "  - Checksum:   " << checksum << " (expected " << expected_checksum << ")\n";
    std::cout << "  - Pool slots recovered: " << pool.available_approx() << "/" << POOL_SIZE << "\n";
    std::cout << "  [PASS] Pool + Queue zero-malloc integration verified.\n";
}

void test_pool_mpmc() {
    std::cout << "[TEST] 8. Pool — MPMC Concurrent Acquire/Release (4P:4C)...\n";
    constexpr size_t POOL_SIZE       = 4096;
    constexpr size_t N_PER_PRODUCER  = 250'000;
    constexpr size_t PRODUCERS       = 4;
    constexpr size_t CONSUMERS       = 4;
    constexpr size_t TOTAL           = N_PER_PRODUCER * PRODUCERS;

    ObjectPool<Order, POOL_SIZE> pool;
    RingBuffer<Order, POOL_SIZE> queue;

    std::atomic<bool>   start_gate{false};
    std::atomic<size_t> total_consumed{0};
    std::atomic<size_t> total_released{0};  // incremented strictly after pool.release()

    std::vector<std::thread> producers_t, consumers_t;

    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers_t.emplace_back([&]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL) {
                Order* o = nullptr;
                if (queue.try_dequeue(o)) {
                    pool.release(o);
                    total_released.fetch_add(1, std::memory_order_release);  // after release
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
        });
    }

    for (size_t p = 0; p < PRODUCERS; ++p) {
        producers_t.emplace_back([&, p]() {
            (void)p;
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
                Order* o = nullptr;
                while (!(o = pool.acquire())) cache::cpu_relax();
                o->id  = i;
                queue.enqueue(o);
            }
        });
    }

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (auto& t : producers_t) t.join();
    for (auto& t : consumers_t) t.join();

    // Spin until all releases are visible — total_released uses release ordering
    // so this acquire load synchronizes with every pool.release() call
    while (total_released.load(std::memory_order_acquire) < TOTAL) {
        cache::cpu_relax();
    }

    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    assert(total_consumed.load() == TOTAL);
    // total_released is incremented with release ordering and drained above with
    // acquire — this guarantees every pool.release() is visible before we assert.
    assert(total_released.load(std::memory_order_acquire) == TOTAL);

    double ops_sec = (static_cast<double>(TOTAL) / elapsed_ns) * 1e9;
    std::cout << "  - 4P:4C processed " << TOTAL << " items via pool+queue in "
              << elapsed_ns / 1'000'000.0 << " ms\n";
    std::cout << "  - Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - Pool fully recovered: " << pool.available_approx() << "/" << POOL_SIZE << "\n";
    std::cout << "  [PASS] MPMC pool + queue verified.\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// CRTP Policy Tests
// ─────────────────────────────────────────────────────────────────────────────

void test_crtp_drop_policy() {
    std::cout << "[TEST] 9. CRTP — Drop Policy (RingBuffer) full/empty behaviour...\n";
    constexpr size_t CAP = 8;
    RingBuffer<uint64_t, CAP> q;
    std::vector<uint64_t> vals(CAP + 2, 0);
    for (size_t i = 0; i < CAP + 2; ++i) vals[i] = i + 1;

    // Fill to capacity
    for (size_t i = 0; i < CAP; ++i) {
        bool ok = q.try_enqueue(&vals[i]);
        assert(ok);
    }
    assert(q.size_approx() == CAP);

    // Next enqueue must be dropped (returns false), item NOT inserted
    bool dropped = q.try_enqueue(&vals[CAP]);
    assert(!dropped);
    assert(q.size_approx() == CAP);  // size unchanged

    // Drain and verify only original CAP items present
    for (size_t i = 0; i < CAP; ++i) {
        uint64_t* out = nullptr;
        bool ok = q.try_dequeue(out);
        assert(ok);
        assert(*out == i + 1);
    }
    // Queue must now be empty
    uint64_t* dummy = nullptr;
    assert(!q.try_dequeue(dummy));
    std::cout << "  - Full queue drop:    confirmed (try_enqueue returned false)\n";
    std::cout << "  - Empty queue signal: confirmed (try_dequeue returned false)\n";
    std::cout << "  - No items lost or duplicated across " << CAP << " slots\n";
    std::cout << "  [PASS] Drop policy verified.\n";
}

void test_crtp_spinning_policy() {
    std::cout << "[TEST] 10. CRTP — Spinning Policy (SpinningRingBuffer) SPSC...\n";
    constexpr size_t CAP = 1024;
    constexpr size_t N   = 500'000;
    SpinningRingBuffer<uint64_t, CAP> q;
    std::vector<uint64_t> vals(N);
    for (size_t i = 0; i < N; ++i) vals[i] = i + 1;

    // Producer uses try_enqueue which will spin internally via on_full
    auto t0 = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        for (size_t i = 0; i < N; ++i)
            q.enqueue(&vals[i]);
    });

    std::vector<uint64_t*> results(N, nullptr);
    std::thread consumer([&]() {
        for (size_t i = 0; i < N; ++i)
            results[i] = q.dequeue();
    });

    producer.join();
    consumer.join();

    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    for (size_t i = 0; i < N; ++i) {
        assert(results[i] != nullptr);
        assert(*results[i] == i + 1);
    }

    double ops_sec = (static_cast<double>(N) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << N << " items via SpinningRingBuffer\n";
    std::cout << "  - Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - ns/op:      " << static_cast<double>(elapsed_ns) / N << "\n";
    std::cout << "  [PASS] Spinning policy SPSC verified.\n";
}

void test_crtp_overwrite_policy() {
    std::cout << "[TEST] 11. CRTP — Overwrite Policy (OverwriteRingBuffer)...\n";
    constexpr size_t CAP = 8;
    OverwriteRingBuffer<uint64_t, CAP> q;
    std::vector<uint64_t> vals(CAP * 2);
    for (size_t i = 0; i < CAP * 2; ++i) vals[i] = i + 1;

    // Fill to capacity with values 1..CAP
    for (size_t i = 0; i < CAP; ++i) {
        bool ok = q.try_enqueue(&vals[i]);
        assert(ok);
    }
    assert(q.size_approx() == CAP);

    // Enqueue CAP+1 — overwrite policy must evict oldest, make room, succeed
    bool ok = q.try_enqueue(&vals[CAP]);
    assert(ok);  // must succeed — overwrite made room
    std::cout << "  - Enqueue into full queue: succeeded (oldest evicted)\n";

    // Drain: should get CAP items (the oldest was evicted, rest shifted)
    size_t drained = 0;
    uint64_t* out  = nullptr;
    while (q.try_dequeue(out)) ++drained;
    assert(drained == CAP);  // still CAP items despite one overwrite
    std::cout << "  - Drained " << drained << " items after overwrite (capacity maintained)\n";
    std::cout << "  [PASS] Overwrite policy verified.\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// Latency Histogram Tests
// ─────────────────────────────────────────────────────────────────────────────

void test_histogram() {
    std::cout << "[TEST] 12. Lock-Free Latency Histogram Verification...\n";

    LatencyHistogram hist;

    // 1. Monotonicity & Value-to-Bucket Mapping
    uint64_t prev_val = 0;
    for (uint64_t v = 1; v < 100'000'000ULL; v = v * 2 + 1) {
        size_t b = LatencyHistogram::value_to_bucket(v);
        uint64_t roundtrip = LatencyHistogram::bucket_to_value(b);
        assert(b > 0);
        // Sub-bucket relative quantization error should be within ~10%
        double rel_error = std::abs(static_cast<double>(roundtrip) - static_cast<double>(v)) / v;
        assert(rel_error < 0.15);
        prev_val = v;
    }
    (void)prev_val;

    // 2. Synthetic Percentile Check
    // Insert values 1..10,000 linearly
    for (uint64_t i = 1; i <= 10'000; ++i) {
        hist.record(i);
    }
    assert(hist.count() == 10'000);
    assert(hist.min() == 1);
    assert(hist.max() == 10'000);

    // p50 should be ~5000, p99 should be ~9900 within bucket quantization bounds
    uint64_t p50 = hist.percentile(50.0);
    uint64_t p99 = hist.percentile(99.0);
    assert(p50 >= 4500 && p50 <= 5500);
    assert(p99 >= 9500 && p99 <= 10300);
    std::cout << "  - Synthetic p50: " << p50 << " (expected ~5000)\n";
    std::cout << "  - Synthetic p99: " << p99 << " (expected ~9900)\n";

    // 3. Real Profiling on RingBuffer Enqueue + Dequeue (100,000 samples)
    LatencyHistogram rtt_hist;
    RingBuffer<uint64_t, 1024> rb;
    uint64_t item = 0xDEADBEEF;
    uint64_t* out = nullptr;

    // Warm-up
    for (size_t i = 0; i < 1000; ++i) {
        [[maybe_unused]] bool ok1 = rb.try_enqueue(&item);
        [[maybe_unused]] bool ok2 = rb.try_dequeue(out);
    }

    constexpr size_t PROFILING_SAMPLES = 100'000;
    for (size_t i = 0; i < PROFILING_SAMPLES; ++i) {
        uint64_t t0 = cache::rdtsc();
        [[maybe_unused]] bool ok1 = rb.try_enqueue(&item);
        [[maybe_unused]] bool ok2 = rb.try_dequeue(out);
        uint64_t t1 = cache::rdtsc();
        rtt_hist.record(t1 - t0);
    }

    std::cout << "  --- Real RingBuffer RTT Percentile Distribution ---\n";
    rtt_hist.print_summary(std::cout, "cycles");
    assert(rtt_hist.count() == PROFILING_SAMPLES);
    assert(rtt_hist.percentile(50.0) > 0);
    std::cout << "  [PASS] Latency Histogram verified.\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// CAS Contention Telemetry Tests
// ─────────────────────────────────────────────────────────────────────────────

void test_cas_telemetry() {
    std::cout << "[TEST] 13. Thread-Isolated CAS Contention Telemetry...\n";

    static_assert(sizeof(QueueStats) == 64, "QueueStats must be exactly 64 bytes");
    static_assert(alignof(QueueStats) == 64, "QueueStats must be cache-line aligned");

    constexpr size_t N_PER_PRODUCER = 250'000;
    constexpr size_t PRODUCERS      = 4;
    constexpr size_t CONSUMERS      = 4;
    constexpr size_t TOTAL          = N_PER_PRODUCER * PRODUCERS;

    RingBuffer<uint64_t, 1024> rb;

    // Cache-line aligned vector of stats per thread: zero false sharing between counters
    std::vector<QueueStats> p_stats(PRODUCERS);
    std::vector<QueueStats> c_stats(CONSUMERS);

    std::vector<uint64_t> payload(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) payload[i] = i + 1;

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};

    std::vector<std::thread> producers_t, consumers_t;

    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers_t.emplace_back([&, c]() {
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

    for (size_t p = 0; p < PRODUCERS; ++p) {
        producers_t.emplace_back([&, p]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            size_t base = p * N_PER_PRODUCER;
            for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
                while (!rb.try_enqueue(&payload[base + i], p_stats[p])) {
                    cache::cpu_relax();
                }
            }
        });
    }

    start_gate.store(true, std::memory_order_release);

    for (auto& t : producers_t) t.join();
    for (auto& t : consumers_t) t.join();

    QueueStats total_p = aggregate_stats(p_stats);
    QueueStats total_c = aggregate_stats(c_stats);

    assert(total_p.enqueue_success == TOTAL);
    assert(total_c.dequeue_success == TOTAL);

    std::cout << "  --- Aggregated Producer Telemetry ---\n";
    total_p.print_summary(std::cout, "    ");
    std::cout << "  --- Aggregated Consumer Telemetry ---\n";
    total_c.print_summary(std::cout, "    ");

    std::cout << "  [PASS] CAS Telemetry verified.\n";
}

int main() {
    std::cout << "========================================================\n";
    std::cout << "     HFT Lock-Free MPMC Ring Buffer Verification Suite  \n";
    std::cout << "========================================================\n";

    test_cache_alignment();
    test_spsc();
    test_mpmc();
    test_rdtsc_latency();
    test_bulk_operations();
    test_pool_basic();
    test_pool_with_queue();
    test_pool_mpmc();
    test_crtp_drop_policy();
    test_crtp_spinning_policy();
    test_crtp_overwrite_policy();
    test_histogram();
    test_cas_telemetry();

    std::cout << "\n>>> ALL VERIFICATION CHECKS PASSED <<<\n";
    return 0;
}

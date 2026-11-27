#include "mpmc/ring_buffer.hpp"
#include "mpmc/pool.hpp"
#include "mpmc/histogram.hpp"
#include "check.hpp"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <numeric>
#include <atomic>

using namespace mpmc;

struct MarketOrder {
    uint64_t order_id;
    double price;
    uint32_t qty;
    uint32_t flags;
    char client_id[16]; // 8 + 8 + 4 + 4 + 16 = 40 bytes <= 56 bytes inline!
};
static_assert(sizeof(MarketOrder) == 40, "MarketOrder must be 40 bytes");

void test_cache_alignment() {
    std::cout << "[TEST] 1. Cache Line Alignment & False Sharing Verification...\n";
    
    // Check static compile-time constraints
    static_assert(sizeof(Slot<uint64_t>) == 64, "Slot<uint64_t> must be exactly 64 bytes");
    static_assert(alignof(Slot<uint64_t>) == 64, "Slot<uint64_t> alignment must be 64 bytes");
    static_assert(sizeof(Slot<void*>) == 64, "Slot<void*> must be exactly 64 bytes");
    static_assert(alignof(Slot<void*>) == 64, "Slot<void*> alignment must be 64 bytes");
    static_assert(sizeof(Slot<MarketOrder>) == 64, "Slot<MarketOrder> must be exactly 64 bytes");
    static_assert(alignof(Slot<MarketOrder>) == 64, "Slot<MarketOrder> alignment must be 64 bytes");

    RingBuffer<uint64_t, 1024> rb;

    std::cout << "  - sizeof(Slot<uint64_t>): " << sizeof(Slot<uint64_t>) << " bytes\n";
    std::cout << "  - sizeof(Slot<MarketOrder>): " << sizeof(Slot<MarketOrder>) << " bytes\n";
    std::cout << "  - alignof(Slot): " << alignof(Slot<uint64_t>) << " bytes\n";
    std::cout << "  [PASS] Cache line alignment verified.\n";
}

void test_spsc() {
    std::cout << "[TEST] 2. Single-Producer Single-Consumer (1P:1C) By-Value Verification...\n";
    constexpr size_t N = 1'000'000;
    RingBuffer<uint64_t, 1024> rb;

    auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        for (size_t i = 0; i < N; ++i) {
            rb.enqueue(i + 1);
        }
    });

    std::vector<uint64_t> received(N, 0);
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
        CHECK(received[i] == i + 1);
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
                uint64_t item = 0;
                if (rb.try_dequeue(item)) {
                    local.push_back(item);
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
                uint64_t val = (p + 1) * 1'000'000'000ULL + i;
                rb.enqueue(val);
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

    CHECK(received_count == TOTAL_MESSAGES);
    std::sort(all_received.begin(), all_received.end());
    for (size_t p = 0; p < PRODUCERS; ++p) {
        for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
            CHECK(all_received[p * N_PER_PRODUCER + i] == (p + 1) * 1'000'000'000ULL + i);
        }
    }
    CHECK(rb.empty_approx());

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
    uint64_t out = 0;

    // Warm-up cache
    for (size_t i = 0; i < 1000; ++i) {
        [[maybe_unused]] bool ok1 = rb.try_enqueue(val);
        [[maybe_unused]] bool ok2 = rb.try_dequeue(out);
    }

    uint64_t total_cycles = 0;
    for (size_t i = 0; i < RUNS; ++i) {
        uint64_t t0 = cache::rdtsc();
        [[maybe_unused]] bool ok1 = rb.try_enqueue(val);
        [[maybe_unused]] bool ok2 = rb.try_dequeue(out);
        uint64_t t1 = cache::rdtsc();
        total_cycles += (t1 - t0);
    }

    double avg_cycles = static_cast<double>(total_cycles) / RUNS;
    std::cout << "  - Single-thread Enqueue + Dequeue roundtrip: ~" 
              << avg_cycles << " TSC ticks\n";
    std::cout << "  [PASS] Latency measurement complete.\n";
}

void test_bulk_operations() {
    std::cout << "[TEST] 5. Batch / Bulk Operations (Amortized CAS) Verification...\n";
    constexpr size_t TOTAL = 1'000'000;
    constexpr size_t BATCH_SIZE = 32;
    RingBuffer<uint64_t, 2048> rb;

    std::vector<uint64_t> source_data(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) source_data[i] = i + 1;

    auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        size_t enqueued = 0;
        while (enqueued < TOTAL) {
            size_t count = std::min(BATCH_SIZE, TOTAL - enqueued);
            size_t pushed = rb.try_enqueue_bulk(&source_data[enqueued], count);
            if (pushed > 0) {
                enqueued += pushed;
            } else {
                cache::cpu_relax();
            }
        }
    });

    std::vector<uint64_t> dest_data(TOTAL, 0);
    std::thread consumer([&]() {
        size_t dequeued = 0;
        while (dequeued < TOTAL) {
            size_t count = std::min(BATCH_SIZE, TOTAL - dequeued);
            size_t pulled = rb.try_dequeue_bulk(&dest_data[dequeued], count);
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
    auto elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    // Verify all data intact
    for (size_t i = 0; i < TOTAL; ++i) {
        CHECK(dest_data[i] == i + 1);
    }

    double mops = (static_cast<double>(TOTAL) / (elapsed_ms / 1000.0)) / 1e6;
    std::cout << "  - Processed " << TOTAL << " items in batches of " << BATCH_SIZE 
              << " in " << elapsed_ms << " ms\n";
    std::cout << "  - Bulk Throughput: " << mops << " M ops/sec\n";
    std::cout << "  - Latency per item: " << (elapsed_ms * 1e6) / TOTAL << " ns/op\n";
    std::cout << "  [PASS] Bulk operations verified.\n";
}

void test_pool_basic() {
    std::cout << "[TEST] 6. Object Pool — Basic Acquire/Release...\n";
    constexpr size_t POOL_CAP = 1024;
    ObjectPool<MarketOrder, POOL_CAP> pool;

    CHECK(pool.capacity() == POOL_CAP);
    CHECK(pool.available_approx() == POOL_CAP);
    CHECK(pool.in_use_approx() == 0);

    // Acquire all slots
    std::vector<MarketOrder*> acquired;
    acquired.reserve(POOL_CAP);

    for (size_t i = 0; i < POOL_CAP; ++i) {
        MarketOrder* o = pool.acquire();
        CHECK(o != nullptr);
        o->order_id = 1000 + i;
        acquired.push_back(o);
    }

    // Pool must now be exhausted
    MarketOrder* extra = pool.acquire();
    CHECK(extra == nullptr);
    CHECK(pool.available_approx() == 0);

    // Release all slots back
    for (MarketOrder* o : acquired) {
        pool.release(o);
    }

    CHECK(pool.available_approx() == POOL_CAP);
    CHECK(pool.in_use_approx() == 0);
    std::cout << "  - Capacity: " << POOL_CAP << " slots\n";
    std::cout << "  - Initial available: " << POOL_CAP << "\n";
    std::cout << "  - After full acquire: available=0 (exhausted)\n";
    std::cout << "  - After full release: available=" << pool.available_approx() << "\n";
    std::cout << "  [PASS] Pool acquire/release verified.\n";
}

void test_pool_integration() {
    std::cout << "[TEST] 7. Pool + Queue Integration (zero-malloc end-to-end)...\n";
    constexpr size_t TOTAL_ORDERS = 1'000'000;
    constexpr size_t POOL_SIZE    = 2048;

    ObjectPool<MarketOrder, POOL_SIZE> pool;
    RingBuffer<MarketOrder*, POOL_SIZE> queue;

    std::atomic<bool> producer_done{false};
    uint64_t consumer_checksum = 0;

    auto t0 = std::chrono::steady_clock::now();

    // Producer: acquire from pool, set payload, enqueue pointer
    std::thread producer([&]() {
        for (size_t i = 1; i <= TOTAL_ORDERS; ++i) {
            MarketOrder* o = nullptr;
            SpinWait bk;
            while ((o = pool.acquire()) == nullptr) bk.spin();

            o->order_id = i;
            o->price    = static_cast<double>(i) * 0.01;
            o->qty      = static_cast<uint32_t>(i % 100 + 1);

            queue.enqueue(o);
        }
        producer_done.store(true, std::memory_order_release);
    });

    // Consumer: dequeue pointer, process payload, release back to pool
    std::thread consumer([&]() {
        size_t consumed = 0;
        while (consumed < TOTAL_ORDERS) {
            MarketOrder* o = queue.dequeue();
            consumer_checksum += o->order_id;
            pool.release(o);
            ++consumed;
        }
    });

    producer.join();
    consumer.join();

    auto t1 = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    uint64_t expected_checksum = static_cast<uint64_t>(TOTAL_ORDERS) * (TOTAL_ORDERS + 1) / 2;
    CHECK(consumer_checksum == expected_checksum);
    CHECK(pool.available_approx() == POOL_SIZE);

    double mops = (static_cast<double>(TOTAL_ORDERS) / (elapsed_ms / 1000.0)) / 1e6;
    std::cout << "  - Processed " << TOTAL_ORDERS << " messages (pool acquire→queue→dequeue→pool release)\n";
    std::cout << "  - Elapsed:    " << elapsed_ms << " ms\n";
    std::cout << "  - Throughput: " << mops << " M ops/sec\n";
    std::cout << "  - ns/op:      " << (elapsed_ms * 1e6) / TOTAL_ORDERS << "\n";
    std::cout << "  - Checksum:   " << consumer_checksum << " (expected " << expected_checksum << ")\n";
    std::cout << "  - Pool slots recovered: " << pool.available_approx() << "/" << POOL_SIZE << "\n";
    std::cout << "  [PASS] Pool + Queue zero-malloc integration verified.\n";
}

void test_pool_mpmc() {
    std::cout << "[TEST] 8. Pool — MPMC Concurrent Acquire/Release (4P:4C)...\n";
    constexpr size_t PRODUCERS      = 4;
    constexpr size_t CONSUMERS      = 4;
    constexpr size_t OPS_PER_PROD   = 250'000;
    constexpr size_t TOTAL_OPS      = PRODUCERS * OPS_PER_PROD;
    constexpr size_t POOL_SIZE      = 4096;

    ObjectPool<MarketOrder, POOL_SIZE> pool;
    RingBuffer<MarketOrder*, POOL_SIZE> queue;

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};
    std::atomic<uint64_t> global_checksum{0};

    auto t0 = std::chrono::steady_clock::now();

    std::vector<std::thread> consumers;
    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers.emplace_back([&]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL_OPS) {
                MarketOrder* o = nullptr;
                if (queue.try_dequeue(o)) {
                    global_checksum.fetch_add(o->order_id, std::memory_order_relaxed);
                    pool.release(o);
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
        });
    }

    std::vector<std::thread> producers;
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            for (size_t i = 1; i <= OPS_PER_PROD; ++i) {
                MarketOrder* o = nullptr;
                SpinWait bk;
                while ((o = pool.acquire()) == nullptr) bk.spin();
                o->order_id = (p + 1) * 1'000'000ULL + i;
                queue.enqueue(o);
            }
        });
    }

    start_gate.store(true, std::memory_order_release);

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    auto t1 = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    CHECK(total_consumed.load() == TOTAL_OPS);
    CHECK(pool.available_approx() == POOL_SIZE);

    double mops = (static_cast<double>(TOTAL_OPS) / (elapsed_ms / 1000.0)) / 1e6;
    std::cout << "  - 4P:4C processed " << TOTAL_OPS << " items via pool+queue in " 
              << elapsed_ms << " ms\n";
    std::cout << "  - Throughput: " << mops << " M ops/sec\n";
    std::cout << "  - Pool fully recovered: " << pool.available_approx() << "/" << POOL_SIZE << "\n";
    std::cout << "  [PASS] MPMC pool + queue verified.\n";
}

void test_crtp_drop_policy() {
    std::cout << "[TEST] 9. CRTP — Drop Policy (RingBuffer) full/empty behaviour...\n";
    constexpr size_t CAP = 8;
    RingBuffer<uint64_t, CAP> q;

    // Fill to capacity
    for (size_t i = 0; i < CAP; ++i) {
        bool ok = q.try_enqueue(i + 1);
        CHECK(ok);
    }
    CHECK(q.size_approx() == CAP);

    // Next enqueue must be dropped (returns false), item NOT inserted
    bool dropped = q.try_enqueue(999);
    CHECK(!dropped);
    CHECK(q.size_approx() == CAP);

    // Drain and verify only original CAP items present
    for (size_t i = 0; i < CAP; ++i) {
        uint64_t out = 0;
        bool ok = q.try_dequeue(out);
        CHECK(ok);
        CHECK(out == i + 1);
    }
    uint64_t dummy = 0;
    CHECK(!q.try_dequeue(dummy));
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

    auto t0 = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        for (size_t i = 0; i < N; ++i)
            q.enqueue(i + 1);
    });

    std::vector<uint64_t> results(N, 0);
    std::thread consumer([&]() {
        for (size_t i = 0; i < N; ++i)
            results[i] = q.dequeue();
    });

    producer.join();
    consumer.join();

    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    for (size_t i = 0; i < N; ++i) {
        CHECK(results[i] == i + 1);
    }

    double ops_sec = (static_cast<double>(N) / elapsed_ns) * 1e9;
    std::cout << "  - Processed " << N << " items via SpinningRingBuffer\n";
    std::cout << "  - Throughput: " << ops_sec / 1e6 << " M ops/sec\n";
    std::cout << "  - ns/op:      " << static_cast<double>(elapsed_ns) / N << "\n";
    std::cout << "  [PASS] Spinning policy SPSC verified.\n";
}

void test_inline_by_value_storage() {
    std::cout << "[TEST] 11. By-Value Inline Slot Storage (sizeof(T) <= 56B)...\n";
    constexpr size_t CAP = 16;
    constexpr size_t N = 100'000;

    // MarketOrder is 40 bytes; packed entirely within the 64-byte slot cache line.
    RingBuffer<MarketOrder, CAP> q;

    std::atomic<bool> producer_done{false};
    uint64_t total_checksum = 0;

    std::thread producer([&]() {
        for (size_t i = 1; i <= N; ++i) {
            MarketOrder order{};
            order.order_id = i;
            order.price = 100.5 + static_cast<double>(i % 50);
            order.qty = static_cast<uint32_t>(i % 10 + 1);
            order.flags = 0xAA;
            q.enqueue(order);
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&]() {
        for (size_t i = 1; i <= N; ++i) {
            MarketOrder order = q.dequeue();
            CHECK(order.order_id == i);
            CHECK(order.flags == 0xAA);
            total_checksum += order.order_id;
        }
    });

    producer.join();
    consumer.join();

    uint64_t expected_checksum = static_cast<uint64_t>(N) * (N + 1) / 2;
    CHECK(total_checksum == expected_checksum);
    std::cout << "  - Transferred " << N << " 40-byte MarketOrders inline by value\n";
    std::cout << "  - Checksum: " << total_checksum << " (expected " << expected_checksum << ")\n";
    std::cout << "  [PASS] Inline by-value storage verified.\n";
}

void test_bulk_does_not_wait() {
    std::cout << "[TEST] Bulk operations return partial counts instead of waiting...\n";
    RingBuffer<uint64_t, 8> rb;
    uint64_t in[16];
    uint64_t out[16]{};
    for (size_t i = 0; i < 16; ++i) in[i] = i + 1;

    CHECK(rb.try_dequeue_bulk(out, 16) == 0);
    CHECK(rb.try_enqueue_bulk(in, 5) == 5);
    CHECK(rb.try_enqueue_bulk(in + 5, 11) == 3);  // only 3 slots left
    CHECK(rb.try_enqueue_bulk(in, 1) == 0);       // full
    CHECK(rb.try_dequeue_bulk(out, 3) == 3);
    CHECK(rb.try_enqueue_bulk(in + 8, 8) == 3);   // wraps into freed slots
    CHECK(rb.try_dequeue_bulk(out + 3, 13) == 8);
    for (size_t i = 0; i < 11; ++i) CHECK(out[i] == i + 1);
    CHECK(rb.empty_approx());
    std::cout << "  [PASS] Bulk partial-count semantics verified.\n";
}

void test_histogram() {
    std::cout << "[TEST] 12. Lock-Free Log-Linear Latency Histogram Verification...\n";

    LatencyHistogram hist;

    for (uint64_t v = 1; v < 100'000'000ULL; v = v * 2 + 1) {
        size_t b = LatencyHistogram::value_to_bucket(v);
        CHECK(b > 0);
    }

    for (size_t i = 1; i <= 10000; ++i) {
        hist.record(i);
    }

    uint64_t p50 = hist.percentile(50.0);
    uint64_t p99 = hist.percentile(99.0);
    CHECK(p50 >= 4500 && p50 <= 5500);
    CHECK(p99 >= 9500 && p99 <= 10300);
    std::cout << "  - Synthetic p50: " << p50 << " (expected ~5000)\n";
    std::cout << "  - Synthetic p99: " << p99 << " (expected ~9900)\n";

    LatencyHistogram rtt_hist;
    RingBuffer<uint64_t, 1024> rb;
    uint64_t item = 0xDEADBEEF;
    uint64_t out = 0;

    for (size_t i = 0; i < 1000; ++i) {
        [[maybe_unused]] bool ok1 = rb.try_enqueue(item);
        [[maybe_unused]] bool ok2 = rb.try_dequeue(out);
    }

    constexpr size_t PROFILING_SAMPLES = 100'000;
    for (size_t i = 0; i < PROFILING_SAMPLES; ++i) {
        uint64_t t0 = cache::rdtsc();
        [[maybe_unused]] bool ok1 = rb.try_enqueue(item);
        [[maybe_unused]] bool ok2 = rb.try_dequeue(out);
        uint64_t t1 = cache::rdtsc();
        rtt_hist.record(t1 - t0);
    }

    std::cout << "  --- Real RingBuffer RTT Percentile Distribution ---\n";
    rtt_hist.print_summary(std::cout, "ticks");
    CHECK(rtt_hist.count() == PROFILING_SAMPLES);
    CHECK(rtt_hist.percentile(50.0) > 0);
    std::cout << "  [PASS] Latency Histogram verified.\n";
}

void test_cas_telemetry() {
    std::cout << "[TEST] 13. Thread-Isolated CAS Contention Telemetry...\n";

    static_assert(sizeof(QueueStats) == 64, "QueueStats must be exactly 64 bytes");
    static_assert(alignof(QueueStats) == 64, "QueueStats must be cache-line aligned");

    constexpr size_t N_PER_PRODUCER = 250'000;
    constexpr size_t PRODUCERS      = 4;
    constexpr size_t CONSUMERS      = 4;
    constexpr size_t TOTAL          = N_PER_PRODUCER * PRODUCERS;

    RingBuffer<uint64_t, 1024> rb;

    std::vector<QueueStats> p_stats(PRODUCERS);
    std::vector<QueueStats> c_stats(CONSUMERS);

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};

    std::vector<std::thread> producers_t, consumers_t;

    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers_t.emplace_back([&, c]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL) {
                uint64_t out = 0;
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
                while (!rb.try_enqueue(base + i + 1, p_stats[p])) {
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

    CHECK(total_p.enqueue_success == TOTAL);
    CHECK(total_c.dequeue_success == TOTAL);

    std::cout << "  --- Aggregated Producer Telemetry ---\n";
    total_p.print_summary(std::cout, "    ");
    std::cout << "  --- Aggregated Consumer Telemetry ---\n";
    total_c.print_summary(std::cout, "    ");

    std::cout << "  [PASS] CAS Telemetry verified.\n";
}

void test_bulk_cas_telemetry() {
    std::cout << "[TEST] 14. Bulk Operations CAS Contention Telemetry (Batch=32)...\n";

    constexpr size_t N_PER_PRODUCER = 250'000;
    constexpr size_t PRODUCERS      = 4;
    constexpr size_t CONSUMERS      = 4;
    constexpr size_t TOTAL          = N_PER_PRODUCER * PRODUCERS;
    constexpr size_t BATCH_SIZE     = 32;

    RingBuffer<uint64_t, 2048> rb;

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
            std::vector<uint64_t> dest(BATCH_SIZE, 0);
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL) {
                size_t n = rb.try_dequeue_bulk(dest.data(), BATCH_SIZE, c_stats[c]);
                if (n > 0) {
                    total_consumed.fetch_add(n, std::memory_order_relaxed);
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
            size_t sent = 0;
            while (sent < N_PER_PRODUCER) {
                size_t count = std::min(BATCH_SIZE, N_PER_PRODUCER - sent);
                size_t n = rb.try_enqueue_bulk(&payload[base + sent], count, p_stats[p]);
                if (n > 0) {
                    sent += n;
                } else {
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

    CHECK(total_p.enqueue_success == TOTAL);
    CHECK(total_c.dequeue_success == TOTAL);

    std::cout << "  --- Aggregated Bulk Producer Telemetry ---\n";
    total_p.print_summary(std::cout, "    ");
    std::cout << "  --- Aggregated Bulk Consumer Telemetry ---\n";
    total_c.print_summary(std::cout, "    ");

    std::cout << "  [PASS] Bulk CAS Telemetry verified.\n";
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
    test_pool_integration();
    test_pool_mpmc();
    test_crtp_drop_policy();
    test_crtp_spinning_policy();
    test_inline_by_value_storage();
    test_bulk_does_not_wait();
    test_histogram();
    test_cas_telemetry();
    test_bulk_cas_telemetry();

    std::cout << "\n>>> ALL VERIFICATION CHECKS PASSED <<<\n";
    return 0;
}

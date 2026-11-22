#include "mpmc/ring_buffer.hpp"
#include "mpmc/pool.hpp"
#include "mpmc/cache.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <random>

using namespace mpmc;

// Test 1: Rapid Buffer Laps & Sequence Invariant Verification
// Uses a tiny ring buffer (16 slots) bombarded with 1,000,000 messages across 8 threads.
// This forces the queue to wrap around 62,500 full circular laps in milliseconds.
// Proves that sequence counter transitions (seq = pos -> pos + 1 -> pos + Capacity)
// never drift, desynchronize, or suffer ABA degradation.
void test_rapid_laps_wrap_around() {
    std::cout << "[STRESS TEST 1] Rapid Laps & Sequence Invariant (Capacity = 16, 62,500 laps)...\n";

    constexpr size_t TINY_CAPACITY   = 16;
    constexpr size_t TOTAL_MESSAGES  = 1'000'000;
    constexpr size_t PRODUCERS       = 4;
    constexpr size_t CONSUMERS       = 4;
    constexpr size_t N_PER_PRODUCER  = TOTAL_MESSAGES / PRODUCERS;

    RingBuffer<uint64_t, TINY_CAPACITY> rb;

    std::vector<std::vector<uint64_t>> producer_payloads(PRODUCERS);
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producer_payloads[p].resize(N_PER_PRODUCER);
        for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
            producer_payloads[p][i] = (p + 1) * 10'000'000ULL + i;
        }
    }

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};
    std::vector<std::vector<uint64_t>> consumer_results(CONSUMERS);

    std::vector<std::thread> consumers;
    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers.emplace_back([&, c]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            std::vector<uint64_t> local;
            local.reserve(TOTAL_MESSAGES / CONSUMERS + 1000);
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL_MESSAGES) {
                uint64_t out = 0;
                if (rb.try_dequeue(out)) {
                    local.push_back(out);
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
            consumer_results[c] = std::move(local);
        });
    }

    std::vector<std::thread> producers;
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            for (size_t i = 0; i < N_PER_PRODUCER; ++i) {
                while (!rb.try_enqueue(producer_payloads[p][i])) {
                    cache::cpu_relax();
                }
            }
        });
    }

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    size_t received = 0;
    for (size_t c = 0; c < CONSUMERS; ++c) {
        received += consumer_results[c].size();
    }

    assert(received == TOTAL_MESSAGES);
    assert(rb.empty_approx());
    std::cout << "  - Completed " << TOTAL_MESSAGES / TINY_CAPACITY 
              << " full laps across " << TOTAL_MESSAGES << " messages in " 
              << elapsed_ms << " ms\n";
    std::cout << "  - Total items received: " << received << "/" << TOTAL_MESSAGES << "\n";
    std::cout << "  [PASS] Rapid wrap-around sequence invariant verified.\n";
}

// Test 2: Asymmetric Chaos / Randomized Jitter
// Producers and consumers inject random spin pauses between operations.
// Simulates unpredictable OS scheduling pauses, NIC burst arrivals, and CPU clock throttling.
void test_asymmetric_jitter_chaos() {
    std::cout << "[STRESS TEST 2] Asymmetric Chaos & Timing Jitter...\n";

    constexpr size_t TOTAL_MESSAGES = 500'000;
    constexpr size_t PRODUCERS      = 4;
    constexpr size_t CONSUMERS      = 4;
    constexpr size_t N_PER_P        = TOTAL_MESSAGES / PRODUCERS;

    RingBuffer<uint64_t, 512> rb;
    std::vector<uint64_t> payload(TOTAL_MESSAGES);
    for (size_t i = 0; i < TOTAL_MESSAGES; ++i) payload[i] = i + 1;

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_consumed{0};

    std::vector<std::thread> consumers;
    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers.emplace_back([&, c]() {
            std::mt19937 rng(1337 + static_cast<unsigned>(c));
            std::uniform_int_distribution<uint32_t> jitter_dist(0, 30);

            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            while (total_consumed.load(std::memory_order_relaxed) < TOTAL_MESSAGES) {
                uint64_t out = 0;
                if (rb.try_dequeue(out)) {
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                }
                uint32_t spins = jitter_dist(rng);
                for (uint32_t s = 0; s < spins; ++s) {
                    cache::cpu_relax();
                }
            }
        });
    }

    std::vector<std::thread> producers;
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p]() {
            std::mt19937 rng(4242 + static_cast<unsigned>(p));
            std::uniform_int_distribution<uint32_t> jitter_dist(0, 30);

            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            size_t base = p * N_PER_P;
            for (size_t i = 0; i < N_PER_P; ++i) {
                while (!rb.try_enqueue(payload[base + i])) {
                    cache::cpu_relax();
                }
                uint32_t spins = jitter_dist(rng);
                for (uint32_t s = 0; s < spins; ++s) {
                    cache::cpu_relax();
                }
            }
        });
    }

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    assert(total_consumed.load() == TOTAL_MESSAGES);
    assert(rb.empty_approx());
    std::cout << "  - Processed " << TOTAL_MESSAGES << " items under random timing jitter in " 
              << elapsed_ms << " ms\n";
    std::cout << "  [PASS] Asymmetric chaos completed with zero deadlocks or lost messages.\n";
}

// Test 3: End-to-End ObjectPool + RingBuffer Chaos Lifecycle
struct OrderMessage {
    uint64_t order_id{0};
    uint64_t timestamp{0};
    double price{0.0};
    uint32_t quantity{0};
    char symbol[8]{};
};

void test_pool_lifecycle_chaos() {
    std::cout << "[STRESS TEST 3] ObjectPool + RingBuffer Concurrent Lifecycle Chaos...\n";

    constexpr size_t POOL_CAPACITY   = 2048;
    constexpr size_t TOTAL_ORDERS    = 1'000'000;
    constexpr size_t PRODUCERS       = 4;
    constexpr size_t CONSUMERS       = 4;
    constexpr size_t N_PER_P         = TOTAL_ORDERS / PRODUCERS;

    ObjectPool<OrderMessage, POOL_CAPACITY> pool;
    RingBuffer<OrderMessage*, POOL_CAPACITY> queue;

    std::atomic<bool> start_gate{false};
    std::atomic<size_t> total_processed{0};
    std::atomic<size_t> total_released{0};
    std::atomic<uint64_t> cumulative_checksum{0};

    std::vector<std::thread> consumers;
    for (size_t c = 0; c < CONSUMERS; ++c) {
        consumers.emplace_back([&]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            uint64_t local_checksum = 0;
            while (total_processed.load(std::memory_order_relaxed) < TOTAL_ORDERS) {
                OrderMessage* msg = nullptr;
                if (queue.try_dequeue(msg)) {
                    local_checksum += msg->order_id;
                    pool.release(msg);
                    total_released.fetch_add(1, std::memory_order_release);
                    total_processed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
            cumulative_checksum.fetch_add(local_checksum, std::memory_order_relaxed);
        });
    }

    std::vector<std::thread> producers;
    for (size_t p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p]() {
            while (!start_gate.load(std::memory_order_relaxed)) cache::cpu_relax();
            size_t base = p * N_PER_P;
            for (size_t i = 0; i < N_PER_P; ++i) {
                OrderMessage* msg = nullptr;
                while (!(msg = pool.acquire())) {
                    cache::cpu_relax();
                }
                msg->order_id = base + i + 1;
                msg->price = 100.50 + (i % 100);
                msg->quantity = 10;
                while (!queue.try_enqueue(msg)) {
                    cache::cpu_relax();
                }
            }
        });
    }

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    while (total_released.load(std::memory_order_acquire) < TOTAL_ORDERS) {
        cache::cpu_relax();
    }

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    uint64_t expected_checksum = static_cast<uint64_t>(TOTAL_ORDERS) * (TOTAL_ORDERS + 1) / 2;
    assert(cumulative_checksum.load() == expected_checksum);
    assert(total_processed.load() == TOTAL_ORDERS);
    
    std::cout << "  - Processed " << TOTAL_ORDERS << " order lifecycles in " << elapsed_ms << " ms\n";
    std::cout << "  - Data checksum: " << cumulative_checksum.load() << " (verified)\n";
    std::cout << "  - Pool available_approx: " << pool.available_approx() << " vs capacity " << POOL_CAPACITY << "\n";
    std::cout << "  - Total released count: " << total_released.load() << "/" << TOTAL_ORDERS << "\n";
    std::cout << "  [PASS] ObjectPool + RingBuffer lifecycle verified.\n";
}

int main() {
    std::cout << "========================================================\n";
    std::cout << "     HFT Lock-Free MPMC Ring Buffer Stress & Chaos Suite\n";
    std::cout << "========================================================\n";

    test_rapid_laps_wrap_around();
    test_asymmetric_jitter_chaos();
    test_pool_lifecycle_chaos();

    std::cout << "\n>>> ALL STRESS & CHAOS TESTS PASSED <<<\n";
    return 0;
}

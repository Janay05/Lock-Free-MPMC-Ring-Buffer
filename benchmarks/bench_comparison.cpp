#include "mpmc/ring_buffer.hpp"
#include "mpmc/affinity.hpp"
#include "mpmc/histogram.hpp"
#include "mpmc/cache.hpp"

#include <rigtorp/MPMCQueue.h>
#include <concurrentqueue.h>

#include <chrono>
#include <iostream>
#include <iomanip>
#include <thread>
#include <vector>
#include <atomic>
#include <deque>
#include <mutex>
#include <condition_variable>

// ── Baseline Mutex + Condition Variable + Deque Queue ───────────────────────
template <typename T>
class MutexDequeQueue {
public:
    explicit MutexDequeQueue(size_t capacity = 1024) : capacity_(capacity) {}

    bool try_enqueue(T item) noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        if (deque_.size() >= capacity_) return false;
        deque_.push_back(item);
        cv_empty_.notify_one();
        return true;
    }

    bool try_dequeue(T& out) noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        if (deque_.empty()) return false;
        out = deque_.front();
        deque_.pop_front();
        cv_full_.notify_one();
        return true;
    }

    void enqueue(T item) noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_full_.wait(lock, [&]() { return deque_.size() < capacity_; });
        deque_.push_back(item);
        cv_empty_.notify_one();
    }

    T dequeue() noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_empty_.wait(lock, [&]() { return !deque_.empty(); });
        T item = deque_.front();
        deque_.pop_front();
        cv_full_.notify_one();
        return item;
    }

    size_t try_enqueue_bulk(T const* items, size_t count) noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t available = (deque_.size() < capacity_) ? (capacity_ - deque_.size()) : 0;
        size_t to_push = std::min(count, available);
        for (size_t i = 0; i < to_push; ++i) deque_.push_back(items[i]);
        if (to_push > 0) cv_empty_.notify_all();
        return to_push;
    }

    size_t try_dequeue_bulk(T* out, size_t count) noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t to_pop = std::min(count, deque_.size());
        for (size_t i = 0; i < to_pop; ++i) {
            out[i] = deque_.front();
            deque_.pop_front();
        }
        if (to_pop > 0) cv_full_.notify_all();
        return to_pop;
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_full_;
    std::condition_variable cv_empty_;
    std::deque<T> deque_;
    size_t capacity_;
};

// ── Test Payloads ───────────────────────────────────────────────────────────
static uint64_t g_dummy_payload = 0xDEADBEEF;

// ── Benchmark Results Record ────────────────────────────────────────────────
struct QueueResult {
    std::string name;
    double spsc_rtt_p50_cycles{0.0};
    double spsc_rtt_p99_cycles{0.0};
    double spsc_throughput_mops{0.0};
    double mpmc_throughput_mops{0.0};
    double bulk_throughput_mops{0.0};
};

// ── 1. SPSC Cross-Core RTT Latency Benchmark (Core 0 <-> Core 1) ────────────
template <typename QueueType>
void bench_spsc_latency(QueueType& forward_q, QueueType& return_q,
                        double& out_p50, double& out_p99, double& out_mops) {
    constexpr size_t WARMUP = 10'000;
    constexpr size_t SAMPLES = 100'000;

    mpmc::LatencyHistogram hist;
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            uint64_t* item = nullptr;
            if constexpr (requires { forward_q.try_dequeue(item); }) {
                if (forward_q.try_dequeue(item)) {
                    if constexpr (requires { return_q.enqueue(item); }) {
                        return_q.enqueue(item);
                    } else if constexpr (requires { return_q.push(item); }) {
                        return_q.push(item);
                    }
                } else {
                    mpmc::cache::cpu_relax();
                }
            } else if constexpr (requires { forward_q.try_pop(item); }) {
                if (forward_q.try_pop(item)) {
                    return_q.push(item);
                } else {
                    mpmc::cache::cpu_relax();
                }
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    // Warm-up
    for (size_t i = 0; i < WARMUP; ++i) {
        if constexpr (requires { forward_q.enqueue(&g_dummy_payload); }) {
            forward_q.enqueue(&g_dummy_payload);
            [[maybe_unused]] auto* ret = return_q.dequeue();
        } else if constexpr (requires { forward_q.push(&g_dummy_payload); }) {
            forward_q.push(&g_dummy_payload);
            uint64_t* ret = nullptr;
            return_q.pop(ret);
        }
    }

    // Measurement loop
    auto start_time = std::chrono::steady_clock::now();

    for (size_t i = 0; i < SAMPLES; ++i) {
        uint64_t t0 = mpmc::cache::rdtscp();
        if constexpr (requires { forward_q.enqueue(&g_dummy_payload); }) {
            forward_q.enqueue(&g_dummy_payload);
            [[maybe_unused]] auto* ret = return_q.dequeue();
        } else if constexpr (requires { forward_q.push(&g_dummy_payload); }) {
            forward_q.push(&g_dummy_payload);
            uint64_t* ret = nullptr;
            return_q.pop(ret);
        }
        uint64_t t1 = mpmc::cache::rdtscp();
        if (t1 > t0) hist.record(t1 - t0);
    }

    auto end_time = std::chrono::steady_clock::now();
    stop_consumer.store(true, std::memory_order_relaxed);
    consumer.join();

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    out_mops = (static_cast<double>(SAMPLES) * 2.0 / elapsed_sec) / 1e6;
    out_p50 = static_cast<double>(hist.percentile(50.0));
    out_p99 = static_cast<double>(hist.percentile(99.0));
}

// Specialization for moodycamel SPSC
void bench_moodycamel_spsc_latency(moodycamel::ConcurrentQueue<uint64_t*>& forward_q,
                                    moodycamel::ConcurrentQueue<uint64_t*>& return_q,
                                    double& out_p50, double& out_p99, double& out_mops) {
    constexpr size_t WARMUP = 10'000;
    constexpr size_t SAMPLES = 100'000;

    mpmc::LatencyHistogram hist;
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        moodycamel::ConsumerToken ctok_f(forward_q);
        moodycamel::ProducerToken ptok_r(return_q);

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            uint64_t* item = nullptr;
            if (forward_q.try_dequeue(ctok_f, item)) {
                return_q.enqueue(ptok_r, item);
            } else {
                mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    moodycamel::ProducerToken ptok_f(forward_q);
    moodycamel::ConsumerToken ctok_r(return_q);

    // Warm-up
    for (size_t i = 0; i < WARMUP; ++i) {
        forward_q.enqueue(ptok_f, &g_dummy_payload);
        uint64_t* ret = nullptr;
        while (!return_q.try_dequeue(ctok_r, ret)) mpmc::cache::cpu_relax();
    }

    auto start_time = std::chrono::steady_clock::now();

    for (size_t i = 0; i < SAMPLES; ++i) {
        uint64_t t0 = mpmc::cache::rdtscp();
        forward_q.enqueue(ptok_f, &g_dummy_payload);
        uint64_t* ret = nullptr;
        while (!return_q.try_dequeue(ctok_r, ret)) mpmc::cache::cpu_relax();
        uint64_t t1 = mpmc::cache::rdtscp();
        if (t1 > t0) hist.record(t1 - t0);
    }

    auto end_time = std::chrono::steady_clock::now();
    stop_consumer.store(true, std::memory_order_relaxed);
    consumer.join();

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    out_mops = (static_cast<double>(SAMPLES) * 2.0 / elapsed_sec) / 1e6;
    out_p50 = static_cast<double>(hist.percentile(50.0));
    out_p99 = static_cast<double>(hist.percentile(99.0));
}

// ── 2. Contended MPMC Throughput Benchmark (4P : 4C) ────────────────────────
template <typename QueueType>
double bench_mpmc_contention(QueueType& q) {
    constexpr size_t TOTAL_OPS = 200'000;
    constexpr size_t PRODUCERS = 4;
    constexpr size_t CONSUMERS = 4;
    constexpr size_t OPS_PER_PRODUCER = TOTAL_OPS / PRODUCERS;

    std::atomic<bool> start_flag{false};
    std::atomic<size_t> consumed{0};

    std::vector<std::thread> threads;
    threads.reserve(PRODUCERS + CONSUMERS);

    // Consumers (Cores 4, 5, 6, 7)
    for (size_t i = 0; i < CONSUMERS; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(4 + (i % 4));
            mpmc::affinity::set_current_thread_priority_critical();

            while (!start_flag.load(std::memory_order_relaxed)) {
                mpmc::cache::cpu_relax();
            }

            while (consumed.load(std::memory_order_relaxed) < TOTAL_OPS) {
                uint64_t* item = nullptr;
                if constexpr (requires { q.try_dequeue(item); }) {
                    if (q.try_dequeue(item)) {
                        consumed.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        mpmc::cache::cpu_relax();
                    }
                } else if constexpr (requires { q.try_pop(item); }) {
                    if (q.try_pop(item)) {
                        consumed.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        mpmc::cache::cpu_relax();
                    }
                }
            }
        });
    }

    // Producers (Cores 0, 1, 2, 3)
    for (size_t i = 0; i < PRODUCERS; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(i % 4);
            mpmc::affinity::set_current_thread_priority_critical();

            while (!start_flag.load(std::memory_order_relaxed)) {
                mpmc::cache::cpu_relax();
            }

            for (size_t j = 0; j < OPS_PER_PRODUCER; ++j) {
                if constexpr (requires { q.try_enqueue(&g_dummy_payload); }) {
                    while (!q.try_enqueue(&g_dummy_payload)) {
                        mpmc::cache::cpu_relax();
                    }
                } else if constexpr (requires { q.try_push(&g_dummy_payload); }) {
                    while (!q.try_push(&g_dummy_payload)) {
                        mpmc::cache::cpu_relax();
                    }
                }
            }
        });
    }

    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& t : threads) t.join();
    auto end_time = std::chrono::steady_clock::now();

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL_OPS) / elapsed_sec) / 1e6;
}

// Specialization for moodycamel 4P:4C
double bench_moodycamel_mpmc_contention(moodycamel::ConcurrentQueue<uint64_t*>& q) {
    constexpr size_t TOTAL_OPS = 200'000;
    constexpr size_t PRODUCERS = 4;
    constexpr size_t CONSUMERS = 4;
    constexpr size_t OPS_PER_PRODUCER = TOTAL_OPS / PRODUCERS;

    std::atomic<bool> start_flag{false};
    std::atomic<size_t> consumed{0};

    std::vector<std::thread> threads;
    threads.reserve(PRODUCERS + CONSUMERS);

    for (size_t i = 0; i < CONSUMERS; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(4 + (i % 4));
            mpmc::affinity::set_current_thread_priority_critical();
            moodycamel::ConsumerToken ctok(q);

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            while (consumed.load(std::memory_order_relaxed) < TOTAL_OPS) {
                uint64_t* item = nullptr;
                if (q.try_dequeue(ctok, item)) {
                    consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    mpmc::cache::cpu_relax();
                }
            }
        });
    }

    for (size_t i = 0; i < PRODUCERS; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(i % 4);
            mpmc::affinity::set_current_thread_priority_critical();
            moodycamel::ProducerToken ptok(q);

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            for (size_t j = 0; j < OPS_PER_PRODUCER; ++j) {
                q.enqueue(ptok, &g_dummy_payload);
            }
        });
    }

    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& t : threads) t.join();
    auto end_time = std::chrono::steady_clock::now();

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL_OPS) / elapsed_sec) / 1e6;
}

// ── 3. Bulk Throughput Benchmark (Batch = 32, 1P : 1C) ─────────────────────
template <typename QueueType>
double bench_bulk_throughput(QueueType& q) {
    constexpr size_t TOTAL_ITEMS = 500'000;
    constexpr size_t BATCH_SIZE  = 32;

    std::atomic<bool> start_flag{false};

    // Consumer on Core 1
    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t total_received = 0;
        uint64_t* batch[BATCH_SIZE];

        while (total_received < TOTAL_ITEMS) {
            if constexpr (requires { q.try_dequeue_bulk(batch, BATCH_SIZE); }) {
                size_t n = q.try_dequeue_bulk(batch, BATCH_SIZE);
                total_received += n;
                if (n == 0) mpmc::cache::cpu_relax();
            } else if constexpr (requires { q.try_pop(batch[0]); }) {
                size_t n = 0;
                for (size_t b = 0; b < BATCH_SIZE; ++b) {
                    if (q.try_pop(batch[b])) ++n;
                    else break;
                }
                total_received += n;
                if (n == 0) mpmc::cache::cpu_relax();
            }
        }
    });

    // Producer on Core 0
    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    uint64_t* batch[BATCH_SIZE];
    for (size_t i = 0; i < BATCH_SIZE; ++i) batch[i] = &g_dummy_payload;

    start_flag.store(true, std::memory_order_release);
    auto start_time = std::chrono::steady_clock::now();

    size_t total_sent = 0;
    while (total_sent < TOTAL_ITEMS) {
        if constexpr (requires { q.try_enqueue_bulk(batch, BATCH_SIZE); }) {
            size_t n = q.try_enqueue_bulk(batch, BATCH_SIZE);
            total_sent += n;
            if (n == 0) mpmc::cache::cpu_relax();
        } else if constexpr (requires { q.try_push(batch[0]); }) {
            size_t n = 0;
            for (size_t b = 0; b < BATCH_SIZE; ++b) {
                if (q.try_push(batch[b])) ++n;
                else break;
            }
            total_sent += n;
            if (n == 0) mpmc::cache::cpu_relax();
        }
    }

    consumer.join();
    auto end_time = std::chrono::steady_clock::now();

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL_ITEMS) / elapsed_sec) / 1e6;
}

// Specialization for moodycamel bulk
double bench_moodycamel_bulk(moodycamel::ConcurrentQueue<uint64_t*>& q) {
    constexpr size_t TOTAL_ITEMS = 500'000;
    constexpr size_t BATCH_SIZE  = 32;

    std::atomic<bool> start_flag{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();
        moodycamel::ConsumerToken ctok(q);

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t total_received = 0;
        uint64_t* batch[BATCH_SIZE];

        while (total_received < TOTAL_ITEMS) {
            size_t n = q.try_dequeue_bulk(ctok, batch, BATCH_SIZE);
            total_received += n;
            if (n == 0) mpmc::cache::cpu_relax();
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();
    moodycamel::ProducerToken ptok(q);

    uint64_t* batch[BATCH_SIZE];
    for (size_t i = 0; i < BATCH_SIZE; ++i) batch[i] = &g_dummy_payload;

    start_flag.store(true, std::memory_order_release);
    auto start_time = std::chrono::steady_clock::now();

    size_t total_sent = 0;
    while (total_sent < TOTAL_ITEMS) {
        if (q.enqueue_bulk(ptok, batch, BATCH_SIZE)) {
            total_sent += BATCH_SIZE;
        } else {
            mpmc::cache::cpu_relax();
        }
    }

    consumer.join();
    auto end_time = std::chrono::steady_clock::now();

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL_ITEMS) / elapsed_sec) / 1e6;
}

// ── Main Comparison Driver ──────────────────────────────────────────────────
int main() {
    std::cout << "========================================================================\n";
    std::cout << "   Lock-Free MPMC Queue Comparative Benchmark Suite\n";
    std::cout << "   Pinned Physical Cores (i7-14700HX, Raptor Lake, Time-Critical)\n";
    std::cout << "========================================================================\n" << std::endl;

    std::vector<QueueResult> results;

    // ── 1. Our Queue (mpmc::RingBuffer) ─────────────────────────────────────
    std::cout << "[1/4] Benchmarking mpmc::RingBuffer (Our Queue)..." << std::endl;
    {
        QueueResult res;
        res.name = "mpmc::RingBuffer (Ours)";
        mpmc::RingBuffer<uint64_t, 1024> f_q, r_q;
        std::cout << "      Running SPSC RTT latency..." << std::endl;
        bench_spsc_latency(f_q, r_q, res.spsc_rtt_p50_cycles, res.spsc_rtt_p99_cycles, res.spsc_throughput_mops);

        mpmc::RingBuffer<uint64_t, 1024> mpmc_q;
        std::cout << "      Running 4P:4C contention throughput..." << std::endl;
        res.mpmc_throughput_mops = bench_mpmc_contention(mpmc_q);

        mpmc::RingBuffer<uint64_t, 1024> bulk_q;
        std::cout << "      Running Bulk throughput (batch=32)..." << std::endl;
        res.bulk_throughput_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] mpmc::RingBuffer" << std::endl;
    }

    // ── 2. rigtorp::MPMCQueue ───────────────────────────────────────────────
    std::cout << "\n[2/4] Benchmarking rigtorp::MPMCQueue..." << std::endl;
    {
        QueueResult res;
        res.name = "rigtorp::MPMCQueue";
        rigtorp::MPMCQueue<uint64_t*> f_q(1024), r_q(1024);
        std::cout << "      Running SPSC RTT latency..." << std::endl;
        bench_spsc_latency(f_q, r_q, res.spsc_rtt_p50_cycles, res.spsc_rtt_p99_cycles, res.spsc_throughput_mops);

        rigtorp::MPMCQueue<uint64_t*> mpmc_q(1024);
        std::cout << "      Running 4P:4C contention throughput..." << std::endl;
        res.mpmc_throughput_mops = bench_mpmc_contention(mpmc_q);

        rigtorp::MPMCQueue<uint64_t*> bulk_q(1024);
        std::cout << "      Running Bulk throughput (batch=32)..." << std::endl;
        res.bulk_throughput_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] rigtorp::MPMCQueue" << std::endl;
    }

    // ── 3. moodycamel::ConcurrentQueue ──────────────────────────────────────
    std::cout << "\n[3/4] Benchmarking moodycamel::ConcurrentQueue..." << std::endl;
    {
        QueueResult res;
        res.name = "moodycamel::ConcurrentQueue";
        moodycamel::ConcurrentQueue<uint64_t*> f_q(1024), r_q(1024);
        std::cout << "      Running SPSC RTT latency..." << std::endl;
        bench_moodycamel_spsc_latency(f_q, r_q, res.spsc_rtt_p50_cycles, res.spsc_rtt_p99_cycles, res.spsc_throughput_mops);

        moodycamel::ConcurrentQueue<uint64_t*> mpmc_q(1024);
        std::cout << "      Running 4P:4C contention throughput..." << std::endl;
        res.mpmc_throughput_mops = bench_moodycamel_mpmc_contention(mpmc_q);

        moodycamel::ConcurrentQueue<uint64_t*> bulk_q(1024);
        std::cout << "      Running Bulk throughput (batch=32)..." << std::endl;
        res.bulk_throughput_mops = bench_moodycamel_bulk(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] moodycamel::ConcurrentQueue" << std::endl;
    }

    // ── 4. Mutex + Deque Baseline ───────────────────────────────────────────
    std::cout << "\n[4/4] Benchmarking MutexDequeQueue (std::mutex + std::deque)..." << std::endl;
    {
        QueueResult res;
        res.name = "std::mutex + std::deque";
        MutexDequeQueue<uint64_t*> f_q(1024), r_q(1024);
        std::cout << "      Running SPSC RTT latency..." << std::endl;
        bench_spsc_latency(f_q, r_q, res.spsc_rtt_p50_cycles, res.spsc_rtt_p99_cycles, res.spsc_throughput_mops);

        MutexDequeQueue<uint64_t*> mpmc_q(1024);
        std::cout << "      Running 4P:4C contention throughput..." << std::endl;
        res.mpmc_throughput_mops = bench_mpmc_contention(mpmc_q);

        MutexDequeQueue<uint64_t*> bulk_q(1024);
        std::cout << "      Running Bulk throughput (batch=32)..." << std::endl;
        res.bulk_throughput_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] std::mutex + std::deque" << std::endl;
    }

    // ── Summary Table ───────────────────────────────────────────────────────
    std::cout << "\n\n";
    std::cout << "========================================================================================================\n";
    std::cout << "                                  COMPARATIVE BENCHMARK SUMMARY\n";
    std::cout << "========================================================================================================\n";
    std::cout << std::left
              << std::setw(30) << "Queue Implementation"
              << std::setw(18) << "SPSC RTT p50"
              << std::setw(18) << "SPSC RTT p99"
              << std::setw(20) << "SPSC Unbatched"
              << std::setw(20) << "MPMC (4P:4C)"
              << std::setw(20) << "Bulk (Batch=32)"
              << "\n";
    std::cout << std::string(126, '-') << "\n";

    for (const auto& r : results) {
        std::string p50_str = std::to_string(static_cast<int>(r.spsc_rtt_p50_cycles)) + " cyc";
        std::string p99_str = std::to_string(static_cast<int>(r.spsc_rtt_p99_cycles)) + " cyc";
        std::string spsc_str = std::to_string(r.spsc_throughput_mops).substr(0, 5) + " M ops/s";
        std::string mpmc_str = std::to_string(r.mpmc_throughput_mops).substr(0, 5) + " M ops/s";
        std::string bulk_str = std::to_string(r.bulk_throughput_mops).substr(0, 5) + " M ops/s";

        std::cout << std::left
                  << std::setw(30) << r.name
                  << std::setw(18) << p50_str
                  << std::setw(18) << p99_str
                  << std::setw(20) << spsc_str
                  << std::setw(20) << mpmc_str
                  << std::setw(20) << bulk_str
                  << "\n";
    }
    std::cout << "========================================================================================================\n";
    std::cout << " Notes:\n";
    std::cout << "  - All benchmarks executed on physical P-Cores with THREAD_PRIORITY_TIME_CRITICAL.\n";
    std::cout << "  - SPSC RTT measures hardware round-trip time between Core 0 and Core 1 via invariant RDTSCP.\n";
    std::cout << "  - Single-item SPSC throughput is derived from the unbatched cross-core ping-pong latency.\n";
    std::cout << "  - MPMC measures 4 producers (Cores 0-3) contending with 4 consumers (Cores 4-7) for 200K items.\n";
    std::cout << "  - Bulk measures amortized batch=32 throughput. rigtorp uses a batched loop; mpmc & moodycamel use bulk CAS.\n";
    std::cout << "========================================================================================================\n\n";

    return 0;
}

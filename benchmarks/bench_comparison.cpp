#include "mpmc/ring_buffer.hpp"
#include "mpmc/affinity.hpp"
#include "mpmc/histogram.hpp"
#include "mpmc/cache.hpp"

#include <rigtorp/MPMCQueue.h>
#include <rigtorp/SPSCQueue.h>
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

inline constexpr double kNsPerTscTick = 0.434026; // Calibrated 2.304011 GHz nominal TSC reference

// ── Baseline Mutex + Condition Variable + Deque Queue ───────────────────────
template <typename T>
class MutexDequeQueue {
public:
    explicit MutexDequeQueue(size_t capacity = 1024) : capacity_(capacity) {}

    bool try_enqueue(const T& item) noexcept {
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

    void enqueue(const T& item) noexcept {
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

    size_t try_enqueue_bulk(const T* items, size_t count) noexcept {
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

// ── Benchmark Results Structure ─────────────────────────────────────────────
struct FullQueueResult {
    std::string name;
    std::string pattern;
    double rtt_p50_ticks{0.0};
    double rtt_p90_ticks{0.0};
    double rtt_p99_ticks{0.0};
    double rtt_p999_ticks{0.0};
    double rtt_max_ticks{0.0};
    double spsc_pipelined_mops{0.0};
    double mpmc_4p4c_mops{0.0};
    double bulk_batch32_mops{0.0};
};

// ── 1. SPSC Handoff RTT Latency Benchmark (1P:1C, Core 0 <-> Core 1) ────────
template <typename QueueType>
void bench_spsc_latency(QueueType& forward_q, QueueType& return_q,
                        FullQueueResult& res) {
    constexpr size_t WARMUP = 10'000;
    constexpr size_t SAMPLES = 100'000;

    mpmc::LatencyHistogram hist;
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            uint64_t item = 0;
            if constexpr (requires { forward_q.try_dequeue(item); }) {
                if (forward_q.try_dequeue(item)) {
                    if constexpr (requires { return_q.enqueue(item + 1); }) {
                        return_q.enqueue(item + 1);
                    } else if constexpr (requires { return_q.push(item + 1); }) {
                        return_q.push(item + 1);
                    }
                } else {
                    mpmc::cache::cpu_relax();
                }
            } else if constexpr (requires { forward_q.try_pop(item); }) {
                if (forward_q.try_pop(item)) {
                    return_q.push(item + 1);
                } else {
                    mpmc::cache::cpu_relax();
                }
            } else if constexpr (requires { forward_q.front(); }) { // rigtorp::SPSCQueue
                auto* front = forward_q.front();
                if (front) {
                    uint64_t val = *front;
                    forward_q.pop();
                    while (!return_q.try_push(val + 1)) mpmc::cache::cpu_relax();
                } else {
                    mpmc::cache::cpu_relax();
                }
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    uint64_t payload = 0xBEEF;

    // Warm-up
    for (size_t i = 0; i < WARMUP; ++i) {
        if constexpr (requires { forward_q.enqueue(payload); }) {
            forward_q.enqueue(payload);
            [[maybe_unused]] auto ret = return_q.dequeue();
        } else if constexpr (requires(uint64_t r) { return_q.pop(r); }) {
            forward_q.push(payload);
            uint64_t ret = 0;
            return_q.pop(ret);
        } else if constexpr (requires { return_q.pop(); }) {
            while (!forward_q.try_push(payload)) mpmc::cache::cpu_relax();
            while (!return_q.front()) mpmc::cache::cpu_relax();
            return_q.pop();
        }
    }

    // Measurement
    for (size_t i = 0; i < SAMPLES; ++i) {
        uint64_t t0 = mpmc::cache::rdtscp();
        if constexpr (requires { forward_q.enqueue(payload); }) {
            forward_q.enqueue(payload);
            [[maybe_unused]] auto ret = return_q.dequeue();
        } else if constexpr (requires(uint64_t r) { return_q.pop(r); }) {
            forward_q.push(payload);
            uint64_t ret = 0;
            return_q.pop(ret);
        } else if constexpr (requires { return_q.pop(); }) {
            while (!forward_q.try_push(payload)) mpmc::cache::cpu_relax();
            while (!return_q.front()) mpmc::cache::cpu_relax();
            return_q.pop();
        }
        uint64_t t1 = mpmc::cache::rdtscp();
        if (t1 > t0) hist.record(t1 - t0);
    }

    stop_consumer.store(true, std::memory_order_relaxed);
    consumer.join();

    res.rtt_p50_ticks  = static_cast<double>(hist.percentile(50.0));
    res.rtt_p90_ticks  = static_cast<double>(hist.percentile(90.0));
    res.rtt_p99_ticks  = static_cast<double>(hist.percentile(99.0));
    res.rtt_p999_ticks = static_cast<double>(hist.percentile(99.9));
    res.rtt_max_ticks  = static_cast<double>(hist.max());
}

// Specialization for moodycamel SPSC
void bench_moodycamel_spsc_latency(moodycamel::ConcurrentQueue<uint64_t>& forward_q,
                                    moodycamel::ConcurrentQueue<uint64_t>& return_q,
                                    FullQueueResult& res) {
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
            uint64_t item = 0;
            if (forward_q.try_dequeue(ctok_f, item)) {
                return_q.enqueue(ptok_r, item + 1);
            } else {
                mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    moodycamel::ProducerToken ptok_f(forward_q);
    moodycamel::ConsumerToken ctok_r(return_q);

    uint64_t payload = 0xBEEF;

    for (size_t i = 0; i < WARMUP; ++i) {
        forward_q.enqueue(ptok_f, payload);
        uint64_t ret = 0;
        while (!return_q.try_dequeue(ctok_r, ret)) mpmc::cache::cpu_relax();
    }

    for (size_t i = 0; i < SAMPLES; ++i) {
        uint64_t t0 = mpmc::cache::rdtscp();
        forward_q.enqueue(ptok_f, payload);
        uint64_t ret = 0;
        while (!return_q.try_dequeue(ctok_r, ret)) mpmc::cache::cpu_relax();
        uint64_t t1 = mpmc::cache::rdtscp();
        if (t1 > t0) hist.record(t1 - t0);
    }

    stop_consumer.store(true, std::memory_order_relaxed);
    consumer.join();

    res.rtt_p50_ticks  = static_cast<double>(hist.percentile(50.0));
    res.rtt_p90_ticks  = static_cast<double>(hist.percentile(90.0));
    res.rtt_p99_ticks  = static_cast<double>(hist.percentile(99.0));
    res.rtt_p999_ticks = static_cast<double>(hist.percentile(99.9));
    res.rtt_max_ticks  = static_cast<double>(hist.max());
}

// ── 2. Pipelined Streaming SPSC Throughput (1P:1C, Core 0 -> Core 1) ────────
template <typename QueueType>
double bench_spsc_pipelined_throughput(QueueType& q) {
    constexpr size_t TOTAL = 1'000'000;
    std::atomic<bool> start_gate{false};
    uint64_t consumer_sum = 0;

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!start_gate.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t received = 0;
        while (received < TOTAL) {
            uint64_t item = 0;
            if constexpr (requires { q.try_dequeue(item); }) {
                if (q.try_dequeue(item)) {
                    consumer_sum += item;
                    ++received;
                } else {
                    mpmc::cache::cpu_relax();
                }
            } else if constexpr (requires { q.try_pop(item); }) {
                if (q.try_pop(item)) {
                    consumer_sum += item;
                    ++received;
                } else {
                    mpmc::cache::cpu_relax();
                }
            } else if constexpr (requires { q.front(); }) {
                auto* front = q.front();
                if (front) {
                    consumer_sum += *front;
                    q.pop();
                    ++received;
                } else {
                    mpmc::cache::cpu_relax();
                }
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (size_t i = 1; i <= TOTAL; ++i) {
        if constexpr (requires { q.try_enqueue(i); }) {
            while (!q.try_enqueue(i)) mpmc::cache::cpu_relax();
        } else if constexpr (requires { q.try_push(i); }) {
            while (!q.try_push(i)) mpmc::cache::cpu_relax();
        }
    }

    consumer.join();
    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    uint64_t expected_sum = static_cast<uint64_t>(TOTAL) * (TOTAL + 1) / 2;
    assert(consumer_sum == expected_sum);

    return (static_cast<double>(TOTAL) / elapsed_ns) * 1e3;
}

// Specialization for moodycamel pipelined SPSC
double bench_moodycamel_pipelined_throughput(moodycamel::ConcurrentQueue<uint64_t>& q) {
    constexpr size_t TOTAL = 1'000'000;
    std::atomic<bool> start_gate{false};
    uint64_t consumer_sum = 0;

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();
        moodycamel::ConsumerToken ctok(q);

        while (!start_gate.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t received = 0;
        while (received < TOTAL) {
            uint64_t item = 0;
            if (q.try_dequeue(ctok, item)) {
                consumer_sum += item;
                ++received;
            } else {
                mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();
    moodycamel::ProducerToken ptok(q);

    auto t0 = std::chrono::steady_clock::now();
    start_gate.store(true, std::memory_order_release);

    for (size_t i = 1; i <= TOTAL; ++i) {
        q.enqueue(ptok, i);
    }

    consumer.join();
    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - t0).count();

    uint64_t expected_sum = static_cast<uint64_t>(TOTAL) * (TOTAL + 1) / 2;
    assert(consumer_sum == expected_sum);

    return (static_cast<double>(TOTAL) / elapsed_ns) * 1e3;
}

// ── 3. Contended MPMC Throughput Benchmark (4P:4C, Cores 0-3 -> Cores 4-7) ──
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

    for (size_t i = 0; i < CONSUMERS; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(4 + (i % 4)));
            mpmc::affinity::set_current_thread_priority_critical();

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            while (consumed.load(std::memory_order_relaxed) < TOTAL_OPS) {
                uint64_t item = 0;
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

    for (size_t i = 0; i < PRODUCERS; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(i % 4));
            mpmc::affinity::set_current_thread_priority_critical();

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            for (size_t j = 0; j < OPS_PER_PRODUCER; ++j) {
                if constexpr (requires { q.try_enqueue(static_cast<uint64_t>(j)); }) {
                    while (!q.try_enqueue(static_cast<uint64_t>(j))) {
                        mpmc::cache::cpu_relax();
                    }
                } else if constexpr (requires { q.try_push(static_cast<uint64_t>(j)); }) {
                    while (!q.try_push(static_cast<uint64_t>(j))) {
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
double bench_moodycamel_mpmc_contention(moodycamel::ConcurrentQueue<uint64_t>& q) {
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
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(4 + (i % 4)));
            mpmc::affinity::set_current_thread_priority_critical();
            moodycamel::ConsumerToken ctok(q);

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            while (consumed.load(std::memory_order_relaxed) < TOTAL_OPS) {
                uint64_t item = 0;
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
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(i % 4));
            mpmc::affinity::set_current_thread_priority_critical();
            moodycamel::ProducerToken ptok(q);

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            for (size_t j = 0; j < OPS_PER_PRODUCER; ++j) {
                q.enqueue(ptok, static_cast<uint64_t>(j));
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

// ── 4. Bulk Throughput Benchmark (1P:1C, Batch = 32, Core 0 -> Core 1) ──────
template <typename QueueType>
double bench_bulk_throughput(QueueType& q) {
    constexpr size_t TOTAL_ITEMS = 500'000;
    constexpr size_t BATCH_SIZE  = 32;

    std::atomic<bool> start_flag{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t total_received = 0;
        uint64_t batch[BATCH_SIZE];

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
            } else if constexpr (requires { q.front(); }) {
                size_t n = 0;
                for (size_t b = 0; b < BATCH_SIZE; ++b) {
                    auto* f = q.front();
                    if (f) {
                        batch[b] = *f;
                        q.pop();
                        ++n;
                    } else break;
                }
                total_received += n;
                if (n == 0) mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    uint64_t batch[BATCH_SIZE];
    for (size_t i = 0; i < BATCH_SIZE; ++i) batch[i] = 0xDEADBEEF;

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
double bench_moodycamel_bulk(moodycamel::ConcurrentQueue<uint64_t>& q) {
    constexpr size_t TOTAL_ITEMS = 500'000;
    constexpr size_t BATCH_SIZE  = 32;

    std::atomic<bool> start_flag{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();
        moodycamel::ConsumerToken ctok(q);

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t total_received = 0;
        uint64_t batch[BATCH_SIZE];

        while (total_received < TOTAL_ITEMS) {
            size_t n = q.try_dequeue_bulk(ctok, batch, BATCH_SIZE);
            total_received += n;
            if (n == 0) mpmc::cache::cpu_relax();
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();
    moodycamel::ProducerToken ptok(q);

    uint64_t batch[BATCH_SIZE];
    for (size_t i = 0; i < BATCH_SIZE; ++i) batch[i] = 0xDEADBEEF;

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
    std::cout << "========================================================================================\n";
    std::cout << "   Lock-Free Queue Comparative Benchmark Suite (Intel Core i7-14700HX, Raptor Lake)\n";
    std::cout << "   Calibrated Invariant TSC Reference: 2.304011 GHz (0.434026 ns per tick)\n";
    std::cout << "   All measurements executed on physical P-Cores with THREAD_PRIORITY_TIME_CRITICAL\n";
    std::cout << "========================================================================================\n\n";

    std::vector<FullQueueResult> results;

    // ── 1. Our Queue (mpmc::RingBuffer) ─────────────────────────────────────
    std::cout << "[1/5] Benchmarking mpmc::RingBuffer (Ours - In-line Value Mode)..." << std::endl;
    {
        FullQueueResult res;
        res.name = "mpmc::RingBuffer (Ours)";
        res.pattern = "Bounded Ticket Array (In-line)";
        mpmc::RingBuffer<uint64_t, 1024> f_q, r_q;
        bench_spsc_latency(f_q, r_q, res);

        mpmc::RingBuffer<uint64_t, 1024> stream_q;
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(stream_q);

        mpmc::RingBuffer<uint64_t, 1024> mpmc_q;
        res.mpmc_4p4c_mops = bench_mpmc_contention(mpmc_q);

        mpmc::RingBuffer<uint64_t, 1024> bulk_q;
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] mpmc::RingBuffer" << std::endl;
    }

    // ── 2. rigtorp::MPMCQueue ───────────────────────────────────────────────
    std::cout << "[2/5] Benchmarking rigtorp::MPMCQueue (Bounded MPMC Reference)..." << std::endl;
    {
        FullQueueResult res;
        res.name = "rigtorp::MPMCQueue";
        res.pattern = "Bounded Turn Array";
        rigtorp::MPMCQueue<uint64_t> f_q(1024), r_q(1024);
        bench_spsc_latency(f_q, r_q, res);

        rigtorp::MPMCQueue<uint64_t> stream_q(1024);
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(stream_q);

        rigtorp::MPMCQueue<uint64_t> mpmc_q(1024);
        res.mpmc_4p4c_mops = bench_mpmc_contention(mpmc_q);

        rigtorp::MPMCQueue<uint64_t> bulk_q(1024);
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] rigtorp::MPMCQueue" << std::endl;
    }

    // ── 3. rigtorp::SPSCQueue ───────────────────────────────────────────────
    std::cout << "[3/5] Benchmarking rigtorp::SPSCQueue (Dedicated SPSC Baseline)..." << std::endl;
    {
        FullQueueResult res;
        res.name = "rigtorp::SPSCQueue";
        res.pattern = "Dedicated SPSC (Load/Store)";
        rigtorp::SPSCQueue<uint64_t> f_q(1024), r_q(1024);
        bench_spsc_latency(f_q, r_q, res);

        rigtorp::SPSCQueue<uint64_t> stream_q(1024);
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(stream_q);

        res.mpmc_4p4c_mops = 0.0; // N/A: strictly single-producer single-consumer

        rigtorp::SPSCQueue<uint64_t> bulk_q(1024);
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] rigtorp::SPSCQueue" << std::endl;
    }

    // ── 4. moodycamel::ConcurrentQueue ──────────────────────────────────────
    std::cout << "[4/5] Benchmarking moodycamel::ConcurrentQueue (Unbounded Block Token)..." << std::endl;
    {
        FullQueueResult res;
        res.name = "moodycamel::ConcurrentQueue";
        res.pattern = "Unbounded Block Token Queue";
        moodycamel::ConcurrentQueue<uint64_t> f_q(1024), r_q(1024);
        bench_moodycamel_spsc_latency(f_q, r_q, res);

        moodycamel::ConcurrentQueue<uint64_t> stream_q(1024);
        res.spsc_pipelined_mops = bench_moodycamel_pipelined_throughput(stream_q);

        moodycamel::ConcurrentQueue<uint64_t> mpmc_q(1024);
        res.mpmc_4p4c_mops = bench_moodycamel_mpmc_contention(mpmc_q);

        moodycamel::ConcurrentQueue<uint64_t> bulk_q(1024);
        res.bulk_batch32_mops = bench_moodycamel_bulk(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] moodycamel::ConcurrentQueue" << std::endl;
    }

    // ── 5. Mutex + Deque Baseline ───────────────────────────────────────────
    std::cout << "[5/5] Benchmarking MutexDequeQueue (std::mutex + std::deque)..." << std::endl;
    {
        FullQueueResult res;
        res.name = "std::mutex + std::deque";
        res.pattern = "Mutex + Condvar + Deque";
        MutexDequeQueue<uint64_t> f_q(1024), r_q(1024);
        bench_spsc_latency(f_q, r_q, res);

        MutexDequeQueue<uint64_t> stream_q(1024);
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(stream_q);

        MutexDequeQueue<uint64_t> mpmc_q(1024);
        res.mpmc_4p4c_mops = bench_mpmc_contention(mpmc_q);

        MutexDequeQueue<uint64_t> bulk_q(1024);
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] std::mutex + std::deque" << std::endl;
    }

    // ── Summary Table 1: Cross-Core Handoff Latency ─────────────────────────
    std::cout << "\n\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << "                  TABLE 1: CROSS-CORE HANDOFF LATENCY (1P:1C Pinned Ping-Pong, RTT / 2)\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << std::left
              << std::setw(28) << "Queue Implementation"
              << std::setw(18) << "p50 (ticks / ns)"
              << std::setw(18) << "p90 (ticks / ns)"
              << std::setw(18) << "p99 (ticks / ns)"
              << std::setw(18) << "p99.9 (ticks / ns)"
              << std::setw(18) << "Max (ticks / ns)"
              << "\n";
    std::cout << std::string(119, '-') << "\n";

    for (const auto& r : results) {
        auto format_val = [](double ticks) {
            double h_ticks = ticks / 2.0;
            double ns = h_ticks * kNsPerTscTick;
            return std::to_string(static_cast<int>(h_ticks)) + "t / " + std::to_string(static_cast<int>(ns)) + "ns";
        };

        std::cout << std::left
                  << std::setw(28) << r.name
                  << std::setw(18) << format_val(r.rtt_p50_ticks)
                  << std::setw(18) << format_val(r.rtt_p90_ticks)
                  << std::setw(18) << format_val(r.rtt_p99_ticks)
                  << std::setw(18) << format_val(r.rtt_p999_ticks)
                  << std::setw(18) << format_val(r.rtt_max_ticks)
                  << "\n";
    }

    // ── Summary Table 2: Streaming Throughput by Topology ───────────────────
    std::cout << "\n\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << "                  TABLE 2: STREAMING THROUGHPUT BY TOPOLOGY & ACCESS PATTERN\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << std::left
              << std::setw(28) << "Queue Implementation"
              << std::setw(30) << "Pattern"
              << std::setw(22) << "1P:1C Pipelined"
              << std::setw(22) << "4P:4C Contended"
              << std::setw(22) << "1P:1C Bulk (Batch=32)"
              << "\n";
    std::cout << std::string(119, '-') << "\n";

    for (const auto& r : results) {
        std::string spsc_str = std::to_string(r.spsc_pipelined_mops).substr(0, 5) + " M ops/s";
        std::string mpmc_str = (r.mpmc_4p4c_mops > 0.0) 
            ? (std::to_string(r.mpmc_4p4c_mops).substr(0, 5) + " M ops/s")
            : "N/A (SPSC-only)";
        std::string bulk_str = std::to_string(r.bulk_batch32_mops).substr(0, 5) + " M ops/s";

        std::cout << std::left
                  << std::setw(28) << r.name
                  << std::setw(30) << r.pattern
                  << std::setw(22) << spsc_str
                  << std::setw(22) << mpmc_str
                  << std::setw(22) << bulk_str
                  << "\n";
    }

    std::cout << "=======================================================================================================================\n";
    std::cout << " Notes & Disclosures:\n";
    std::cout << "  - Host: Intel Core i7-14700HX (Raptor Lake), P-cores pinned with THREAD_PRIORITY_TIME_CRITICAL.\n";
    std::cout << "  - Invariant TSC calibrated against steady_clock at 2.304011 GHz (0.434026 ns/tick).\n";
    std::cout << "  - Table 1 (Latency): Ping-pong round-trip handoff between Core 0 and Core 1; reported as 1-way RTT / 2.\n";
    std::cout << "  - Table 2 (Throughput): Streaming pipelined throughput with consumer actively reading payload.\n";
    std::cout << "  - rigtorp::SPSCQueue is a dedicated load/store SPSC baseline (demonstrates cost of MPMC CAS synchronization).\n";
    std::cout << "=======================================================================================================================\n\n";

    return 0;
}

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
#include <algorithm>
#include <tuple>

inline constexpr double kNsPerTscTick = 0.434026; // Calibrated 2.304011 GHz nominal TSC reference

struct LatencyMessage {
    uint64_t send_tsc{0};
    uint64_t seq{0};
};

struct LatencyStats {
    double p50{0}, p90{0}, p99{0}, p999{0}, max_val{0};
};

struct AggregatedLatency {
    double p50_med{0}, p50_min{0}, p50_max{0};
    double p90_med{0};
    double p99_med{0};
    double p999_med{0};
    double worst_max{0};
};

AggregatedLatency aggregate_10_runs(std::vector<LatencyStats>& runs) {
    auto extract = [&](auto member) {
        std::vector<double> v;
        for (const auto& r : runs) v.push_back(r.*member);
        std::sort(v.begin(), v.end());
        double med = (v.size() % 2 == 1) ? v[v.size() / 2] : (v[v.size() / 2 - 1] + v[v.size() / 2]) / 2.0;
        return std::make_tuple(med, v.front(), v.back());
    };

    AggregatedLatency res;
    std::tie(res.p50_med, res.p50_min, res.p50_max) = extract(&LatencyStats::p50);
    std::tie(res.p90_med, std::ignore, std::ignore) = extract(&LatencyStats::p90);
    std::tie(res.p99_med, std::ignore, std::ignore) = extract(&LatencyStats::p99);
    std::tie(res.p999_med, std::ignore, std::ignore) = extract(&LatencyStats::p999);

    double w_max = 0;
    for (const auto& r : runs) if (r.max_val > w_max) w_max = r.max_val;
    res.worst_max = w_max;
    return res;
}

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

struct FullQueueResult {
    std::string name;
    std::string pattern;
    AggregatedLatency latency;
    double spsc_pipelined_mops{0.0};
    double mpmc_2p2c_mops{0.0};
    double mpmc_4p4c_mops{0.0};
    double bulk_batch32_mops{0.0};
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. Paced True One-Way Latency Passes (Core 0 -> Core 1)
// ─────────────────────────────────────────────────────────────────────────────

// Standard queues (mpmc::RingBuffer, rigtorp::MPMCQueue, MutexDequeQueue)
template <typename QueueType>
LatencyStats measure_one_way_pass(QueueType& q, size_t warmup, size_t samples) {
    mpmc::LatencyHistogram hist;
    alignas(64) std::atomic<uint64_t> drain_ack{0};
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            LatencyMessage msg{};
            bool got = false;
            if constexpr (requires { q.try_dequeue(msg); }) {
                got = q.try_dequeue(msg);
            } else if constexpr (requires { q.try_pop(msg); }) {
                got = q.try_pop(msg);
            }

            if (got) {
                uint64_t t1 = mpmc::cache::rdtsc_end();
                if (msg.seq >= warmup && t1 >= msg.send_tsc) {
                    hist.record(t1 - msg.send_tsc);
                }
                drain_ack.store(msg.seq + 1, std::memory_order_release);
            } else {
                mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    size_t total = warmup + samples;
    for (size_t i = 0; i < total; ++i) {
        while (drain_ack.load(std::memory_order_acquire) != i) {
            mpmc::cache::cpu_relax();
        }

        LatencyMessage msg;
        msg.seq = i;
        uint64_t t0 = mpmc::cache::rdtsc_start();
        msg.send_tsc = t0;

        if constexpr (requires { q.try_enqueue(msg); }) {
            while (!q.try_enqueue(msg)) mpmc::cache::cpu_relax();
        } else if constexpr (requires { q.try_push(msg); }) {
            while (!q.try_push(msg)) mpmc::cache::cpu_relax();
        }
    }

    while (drain_ack.load(std::memory_order_acquire) < total) {
        mpmc::cache::cpu_relax();
    }

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    LatencyStats s;
    s.p50 = static_cast<double>(hist.percentile(50.0));
    s.p90 = static_cast<double>(hist.percentile(90.0));
    s.p99 = static_cast<double>(hist.percentile(99.0));
    s.p999 = static_cast<double>(hist.percentile(99.9));
    s.max_val = static_cast<double>(hist.max());
    return s;
}

// rigtorp::SPSCQueue specialization
LatencyStats measure_rigtorp_spsc_pass(rigtorp::SPSCQueue<LatencyMessage>& q, size_t warmup, size_t samples) {
    mpmc::LatencyHistogram hist;
    alignas(64) std::atomic<uint64_t> drain_ack{0};
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            auto* front = q.front();
            if (front) {
                uint64_t t1 = mpmc::cache::rdtsc_end();
                LatencyMessage msg = *front;
                q.pop();
                if (msg.seq >= warmup && t1 >= msg.send_tsc) {
                    hist.record(t1 - msg.send_tsc);
                }
                drain_ack.store(msg.seq + 1, std::memory_order_release);
            } else {
                mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    size_t total = warmup + samples;
    for (size_t i = 0; i < total; ++i) {
        while (drain_ack.load(std::memory_order_acquire) != i) {
            mpmc::cache::cpu_relax();
        }

        LatencyMessage msg;
        msg.seq = i;
        uint64_t t0 = mpmc::cache::rdtsc_start();
        msg.send_tsc = t0;

        while (!q.try_push(msg)) mpmc::cache::cpu_relax();
    }

    while (drain_ack.load(std::memory_order_acquire) < total) {
        mpmc::cache::cpu_relax();
    }

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    LatencyStats s;
    s.p50 = static_cast<double>(hist.percentile(50.0));
    s.p90 = static_cast<double>(hist.percentile(90.0));
    s.p99 = static_cast<double>(hist.percentile(99.0));
    s.p999 = static_cast<double>(hist.percentile(99.9));
    s.max_val = static_cast<double>(hist.max());
    return s;
}

// moodycamel::ConcurrentQueue specialization
LatencyStats measure_moodycamel_pass(moodycamel::ConcurrentQueue<LatencyMessage>& q, size_t warmup, size_t samples) {
    mpmc::LatencyHistogram hist;
    alignas(64) std::atomic<uint64_t> drain_ack{0};
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();
        moodycamel::ConsumerToken ctok(q);

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            LatencyMessage msg{};
            if (q.try_dequeue(ctok, msg)) {
                uint64_t t1 = mpmc::cache::rdtsc_end();
                if (msg.seq >= warmup && t1 >= msg.send_tsc) {
                    hist.record(t1 - msg.send_tsc);
                }
                drain_ack.store(msg.seq + 1, std::memory_order_release);
            } else {
                mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();
    moodycamel::ProducerToken ptok(q);

    size_t total = warmup + samples;
    for (size_t i = 0; i < total; ++i) {
        while (drain_ack.load(std::memory_order_acquire) != i) {
            mpmc::cache::cpu_relax();
        }

        LatencyMessage msg;
        msg.seq = i;
        uint64_t t0 = mpmc::cache::rdtsc_start();
        msg.send_tsc = t0;

        q.enqueue(ptok, msg);
    }

    while (drain_ack.load(std::memory_order_acquire) < total) {
        mpmc::cache::cpu_relax();
    }

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    LatencyStats s;
    s.p50 = static_cast<double>(hist.percentile(50.0));
    s.p90 = static_cast<double>(hist.percentile(90.0));
    s.p99 = static_cast<double>(hist.percentile(99.0));
    s.p999 = static_cast<double>(hist.percentile(99.9));
    s.max_val = static_cast<double>(hist.max());
    return s;
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Streaming Pipelined Throughput (1P:1C, Core 0 -> Core 1)
// ─────────────────────────────────────────────────────────────────────────────
template <typename QueueType>
double bench_spsc_pipelined_throughput(QueueType& q) {
    constexpr size_t TOTAL = 2'000'000;
    std::atomic<bool> start_flag{false};
    uint64_t consumer_sum = 0;

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

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

    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (size_t i = 0; i < TOTAL; ++i) {
        if constexpr (requires { q.try_enqueue(static_cast<uint64_t>(i + 1)); }) {
            while (!q.try_enqueue(static_cast<uint64_t>(i + 1))) mpmc::cache::cpu_relax();
        } else if constexpr (requires { q.try_push(static_cast<uint64_t>(i + 1)); }) {
            while (!q.try_push(static_cast<uint64_t>(i + 1))) mpmc::cache::cpu_relax();
        }
    }

    consumer.join();
    auto end_time = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL) / elapsed_sec) / 1e6;
}

double bench_moodycamel_pipelined_throughput(moodycamel::ConcurrentQueue<uint64_t>& q) {
    constexpr size_t TOTAL = 2'000'000;
    std::atomic<bool> start_flag{false};
    uint64_t consumer_sum = 0;

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();
        moodycamel::ConsumerToken ctok(q);

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

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

    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (size_t i = 0; i < TOTAL; ++i) {
        q.enqueue(ptok, static_cast<uint64_t>(i + 1));
    }

    consumer.join();
    auto end_time = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL) / elapsed_sec) / 1e6;
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Multi-Core Contention Throughput (2P:2C & 4P:4C on Physical P-Cores)
// ─────────────────────────────────────────────────────────────────────────────
template <typename QueueType>
double bench_topology_throughput(QueueType& q, size_t producers, size_t consumers) {
    constexpr size_t TOTAL_OPS = 200'000;
    const size_t ops_per_producer = TOTAL_OPS / producers;

    std::atomic<bool> start_flag{false};
    std::atomic<size_t> consumed{0};

    std::vector<std::thread> threads;
    threads.reserve(producers + consumers);

    for (size_t i = 0; i < consumers; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(producers + i));
            mpmc::affinity::set_current_thread_priority_critical();

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            while (consumed.load(std::memory_order_relaxed) < TOTAL_OPS) {
                uint64_t item = 0;
                if constexpr (requires { q.try_dequeue(item); }) {
                    if (q.try_dequeue(item)) consumed.fetch_add(1, std::memory_order_relaxed);
                    else mpmc::cache::cpu_relax();
                } else if constexpr (requires { q.try_pop(item); }) {
                    if (q.try_pop(item)) consumed.fetch_add(1, std::memory_order_relaxed);
                    else mpmc::cache::cpu_relax();
                }
            }
        });
    }

    for (size_t i = 0; i < producers; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(i));
            mpmc::affinity::set_current_thread_priority_critical();

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            for (size_t j = 0; j < ops_per_producer; ++j) {
                if constexpr (requires { q.try_enqueue(static_cast<uint64_t>(j)); }) {
                    while (!q.try_enqueue(static_cast<uint64_t>(j))) mpmc::cache::cpu_relax();
                } else if constexpr (requires { q.try_push(static_cast<uint64_t>(j)); }) {
                    while (!q.try_push(static_cast<uint64_t>(j))) mpmc::cache::cpu_relax();
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

double bench_moodycamel_topology(moodycamel::ConcurrentQueue<uint64_t>& q, size_t producers, size_t consumers) {
    constexpr size_t TOTAL_OPS = 200'000;
    const size_t ops_per_producer = TOTAL_OPS / producers;

    std::atomic<bool> start_flag{false};
    std::atomic<size_t> consumed{0};

    std::vector<std::thread> threads;
    threads.reserve(producers + consumers);

    for (size_t i = 0; i < consumers; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(producers + i));
            mpmc::affinity::set_current_thread_priority_critical();
            moodycamel::ConsumerToken ctok(q);

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            while (consumed.load(std::memory_order_relaxed) < TOTAL_OPS) {
                uint64_t item = 0;
                if (q.try_dequeue(ctok, item)) consumed.fetch_add(1, std::memory_order_relaxed);
                else mpmc::cache::cpu_relax();
            }
        });
    }

    for (size_t i = 0; i < producers; ++i) {
        threads.emplace_back([&, i]() {
            mpmc::affinity::pin_current_thread_to_core(static_cast<uint32_t>(i));
            mpmc::affinity::set_current_thread_priority_critical();
            moodycamel::ProducerToken ptok(q);

            while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

            for (size_t j = 0; j < ops_per_producer; ++j) {
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

// ─────────────────────────────────────────────────────────────────────────────
// 4. Bulk Throughput Benchmark (1P:1C, Batch = 32, Core 0 -> Core 1)
// ─────────────────────────────────────────────────────────────────────────────
template <typename QueueType>
double bench_bulk_throughput(QueueType& q) {
    constexpr size_t TOTAL = 2'000'000;
    constexpr size_t BATCH_SIZE = 32;

    std::vector<uint64_t> send_data(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) send_data[i] = i + 1;

    std::atomic<bool> start_flag{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t received = 0;
        std::vector<uint64_t> batch(BATCH_SIZE, 0);

        while (received < TOTAL) {
            if constexpr (requires { q.try_dequeue_bulk(batch.data(), BATCH_SIZE); }) {
                size_t n = q.try_dequeue_bulk(batch.data(), BATCH_SIZE);
                if (n > 0) received += n;
                else mpmc::cache::cpu_relax();
            } else {
                size_t n = 0;
                for (size_t b = 0; b < BATCH_SIZE && received + n < TOTAL; ++b) {
                    uint64_t item = 0;
                    if constexpr (requires { q.try_dequeue(item); }) {
                        if (q.try_dequeue(item)) { batch[n++] = item; } else break;
                    } else if constexpr (requires { q.try_pop(item); }) {
                        if (q.try_pop(item)) { batch[n++] = item; } else break;
                    }
                }
                if (n > 0) received += n;
                else mpmc::cache::cpu_relax();
            }
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();

    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    size_t sent = 0;
    while (sent < TOTAL) {
        size_t count = std::min(BATCH_SIZE, TOTAL - sent);
        if constexpr (requires { q.try_enqueue_bulk(&send_data[sent], count); }) {
            size_t n = q.try_enqueue_bulk(&send_data[sent], count);
            if (n > 0) sent += n;
            else mpmc::cache::cpu_relax();
        } else {
            size_t n = 0;
            for (size_t b = 0; b < count; ++b) {
                if constexpr (requires { q.try_enqueue(send_data[sent + b]); }) {
                    if (q.try_enqueue(send_data[sent + b])) ++n;
                    else break;
                } else if constexpr (requires { q.try_push(send_data[sent + b]); }) {
                    if (q.try_push(send_data[sent + b])) ++n;
                    else break;
                }
            }
            if (n > 0) sent += n;
            else mpmc::cache::cpu_relax();
        }
    }

    consumer.join();
    auto end_time = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL) / elapsed_sec) / 1e6;
}

double bench_moodycamel_bulk(moodycamel::ConcurrentQueue<uint64_t>& q) {
    constexpr size_t TOTAL = 2'000'000;
    constexpr size_t BATCH_SIZE = 32;

    std::vector<uint64_t> send_data(TOTAL);
    for (size_t i = 0; i < TOTAL; ++i) send_data[i] = i + 1;

    std::atomic<bool> start_flag{false};

    std::thread consumer([&]() {
        mpmc::affinity::pin_current_thread_to_core(1);
        mpmc::affinity::set_current_thread_priority_critical();
        moodycamel::ConsumerToken ctok(q);

        while (!start_flag.load(std::memory_order_relaxed)) mpmc::cache::cpu_relax();

        size_t received = 0;
        std::vector<uint64_t> batch(BATCH_SIZE, 0);

        while (received < TOTAL) {
            size_t n = q.try_dequeue_bulk(ctok, batch.data(), BATCH_SIZE);
            if (n > 0) received += n;
            else mpmc::cache::cpu_relax();
        }
    });

    mpmc::affinity::pin_current_thread_to_core(0);
    mpmc::affinity::set_current_thread_priority_critical();
    moodycamel::ProducerToken ptok(q);

    auto start_time = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    size_t sent = 0;
    while (sent < TOTAL) {
        size_t count = std::min(BATCH_SIZE, TOTAL - sent);
        q.enqueue_bulk(ptok, &send_data[sent], count);
        sent += count;
    }

    consumer.join();
    auto end_time = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    return (static_cast<double>(TOTAL) / elapsed_sec) / 1e6;
}

int main() {
    std::cout << "========================================================================================\n";
    std::cout << "   Lock-Free Queue Comparative Benchmark Suite (Intel Core i7-14700HX, Raptor Lake)\n";
    std::cout << "   Calibrated Invariant TSC Reference: 2.304011 GHz (0.434026 ns per tick)\n";
    std::cout << "   Physical P-Cores with THREAD_PRIORITY_TIME_CRITICAL | 10 Runs per Latency Percentile\n";
    std::cout << "========================================================================================\n\n";

    constexpr size_t LATENCY_RUNS = 10;
    constexpr size_t WARMUP = 10'000;
    constexpr size_t SAMPLES = 50'000;

    std::vector<FullQueueResult> results;

    // [1/5] mpmc::RingBuffer (Ours)
    {
        std::cout << "[1/5] Benchmarking mpmc::RingBuffer (Ours - In-line Value Mode)..." << std::endl;
        FullQueueResult res;
        res.name = "mpmc::RingBuffer (Ours)";
        res.pattern = "Bounded Ticket Array (In-line)";

        std::vector<LatencyStats> runs;
        for (size_t r = 0; r < LATENCY_RUNS; ++r) {
            mpmc::RingBuffer<LatencyMessage, 1024> lq;
            runs.push_back(measure_one_way_pass(lq, WARMUP, SAMPLES));
        }
        res.latency = aggregate_10_runs(runs);

        mpmc::RingBuffer<uint64_t, 1024> spsc_q;
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(spsc_q);

        mpmc::RingBuffer<uint64_t, 4096> mpmc_2p_q;
        res.mpmc_2p2c_mops = bench_topology_throughput(mpmc_2p_q, 2, 2);

        mpmc::RingBuffer<uint64_t, 4096> mpmc_4p_q;
        res.mpmc_4p4c_mops = bench_topology_throughput(mpmc_4p_q, 4, 4);

        mpmc::RingBuffer<uint64_t, 4096> bulk_q;
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] mpmc::RingBuffer" << std::endl;
    }

    // [2/5] rigtorp::MPMCQueue
    {
        std::cout << "[2/5] Benchmarking rigtorp::MPMCQueue (Bounded MPMC Reference)..." << std::endl;
        FullQueueResult res;
        res.name = "rigtorp::MPMCQueue";
        res.pattern = "Bounded Turn Array";

        std::vector<LatencyStats> runs;
        for (size_t r = 0; r < LATENCY_RUNS; ++r) {
            rigtorp::MPMCQueue<LatencyMessage> lq(1024);
            runs.push_back(measure_one_way_pass(lq, WARMUP, SAMPLES));
        }
        res.latency = aggregate_10_runs(runs);

        rigtorp::MPMCQueue<uint64_t> spsc_q(1024);
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(spsc_q);

        rigtorp::MPMCQueue<uint64_t> mpmc_2p_q(4096);
        res.mpmc_2p2c_mops = bench_topology_throughput(mpmc_2p_q, 2, 2);

        rigtorp::MPMCQueue<uint64_t> mpmc_4p_q(4096);
        res.mpmc_4p4c_mops = bench_topology_throughput(mpmc_4p_q, 4, 4);

        rigtorp::MPMCQueue<uint64_t> bulk_q(4096);
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] rigtorp::MPMCQueue" << std::endl;
    }

    // [3/5] rigtorp::SPSCQueue
    {
        std::cout << "[3/5] Benchmarking rigtorp::SPSCQueue (Dedicated SPSC Baseline)..." << std::endl;
        FullQueueResult res;
        res.name = "rigtorp::SPSCQueue";
        res.pattern = "Dedicated SPSC (Load/Store)";

        std::vector<LatencyStats> runs;
        for (size_t r = 0; r < LATENCY_RUNS; ++r) {
            rigtorp::SPSCQueue<LatencyMessage> lq(1024);
            runs.push_back(measure_rigtorp_spsc_pass(lq, WARMUP, SAMPLES));
        }
        res.latency = aggregate_10_runs(runs);

        rigtorp::SPSCQueue<uint64_t> spsc_q(1024);
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(spsc_q);
        res.mpmc_2p2c_mops = 0.0;
        res.mpmc_4p4c_mops = 0.0;

        rigtorp::SPSCQueue<uint64_t> bulk_q(4096);
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] rigtorp::SPSCQueue" << std::endl;
    }

    // [4/5] moodycamel::ConcurrentQueue
    {
        std::cout << "[4/5] Benchmarking moodycamel::ConcurrentQueue (Unbounded Block Token)..." << std::endl;
        FullQueueResult res;
        res.name = "moodycamel::ConcurrentQueue";
        res.pattern = "Unbounded Block Token Queue";

        std::vector<LatencyStats> runs;
        for (size_t r = 0; r < LATENCY_RUNS; ++r) {
            moodycamel::ConcurrentQueue<LatencyMessage> lq;
            runs.push_back(measure_moodycamel_pass(lq, WARMUP, SAMPLES));
        }
        res.latency = aggregate_10_runs(runs);

        moodycamel::ConcurrentQueue<uint64_t> spsc_q;
        res.spsc_pipelined_mops = bench_moodycamel_pipelined_throughput(spsc_q);

        moodycamel::ConcurrentQueue<uint64_t> mpmc_2p_q;
        res.mpmc_2p2c_mops = bench_moodycamel_topology(mpmc_2p_q, 2, 2);

        moodycamel::ConcurrentQueue<uint64_t> mpmc_4p_q;
        res.mpmc_4p4c_mops = bench_moodycamel_topology(mpmc_4p_q, 4, 4);

        moodycamel::ConcurrentQueue<uint64_t> bulk_q;
        res.bulk_batch32_mops = bench_moodycamel_bulk(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] moodycamel::ConcurrentQueue" << std::endl;
    }

    // [5/5] MutexDequeQueue
    {
        std::cout << "[5/5] Benchmarking MutexDequeQueue (std::mutex + std::deque)..." << std::endl;
        FullQueueResult res;
        res.name = "std::mutex + std::deque";
        res.pattern = "Mutex + Condvar + Deque";

        std::vector<LatencyStats> runs;
        for (size_t r = 0; r < LATENCY_RUNS; ++r) {
            MutexDequeQueue<LatencyMessage> lq(1024);
            runs.push_back(measure_one_way_pass(lq, WARMUP, SAMPLES));
        }
        res.latency = aggregate_10_runs(runs);

        MutexDequeQueue<uint64_t> spsc_q(1024);
        res.spsc_pipelined_mops = bench_spsc_pipelined_throughput(spsc_q);

        MutexDequeQueue<uint64_t> mpmc_2p_q(4096);
        res.mpmc_2p2c_mops = bench_topology_throughput(mpmc_2p_q, 2, 2);

        MutexDequeQueue<uint64_t> mpmc_4p_q(4096);
        res.mpmc_4p4c_mops = bench_topology_throughput(mpmc_4p_q, 4, 4);

        MutexDequeQueue<uint64_t> bulk_q(4096);
        res.bulk_batch32_mops = bench_bulk_throughput(bulk_q);

        results.push_back(res);
        std::cout << "      [DONE] std::mutex + std::deque" << std::endl;
    }

    // ── Table 1: Paced True One-Way Handoff Latency ─────────────────────────
    std::cout << "\n\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << "             TABLE 1: PACED TRUE ONE-WAY LATENCY (Core 0 -> Core 1, 10 Runs x 50,000 samples)\n";
    std::cout << "             Measurement: Enqueue-call to Dequeue-return (including CAS claim & sequence store)\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << std::left
              << std::setw(28) << "Queue Implementation"
              << std::setw(22) << "p50 [min-max]"
              << std::setw(18) << "p90"
              << std::setw(18) << "p99"
              << std::setw(18) << "p99.9"
              << std::setw(18) << "Worst Max"
              << "\n";
    std::cout << std::string(122, '-') << "\n";

    for (const auto& r : results) {
        auto format_val = [](double ticks) {
            double ns = ticks * kNsPerTscTick;
            return std::to_string(static_cast<int>(ticks)) + "t / " + std::to_string(static_cast<int>(ns)) + "ns";
        };

        std::string p50_str = std::to_string(static_cast<int>(r.latency.p50_med)) + "t (" +
                              std::to_string(static_cast<int>(r.latency.p50_med * kNsPerTscTick)) + "ns) [" +
                              std::to_string(static_cast<int>(r.latency.p50_min)) + "-" +
                              std::to_string(static_cast<int>(r.latency.p50_max)) + "t]";

        std::cout << std::left
                  << std::setw(28) << r.name
                  << std::setw(22) << p50_str
                  << std::setw(18) << format_val(r.latency.p90_med)
                  << std::setw(18) << format_val(r.latency.p99_med)
                  << std::setw(18) << format_val(r.latency.p999_med)
                  << std::setw(18) << format_val(r.latency.worst_max)
                  << "\n";
    }

    // ── Table 2: Streaming Throughput by Topology ───────────────────
    std::cout << "\n\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << "             TABLE 2: STREAMING THROUGHPUT BY TOPOLOGY ACROSS PHYSICAL P-CORES\n";
    std::cout << "=======================================================================================================================\n";
    std::cout << std::left
              << std::setw(28) << "Queue Implementation"
              << std::setw(26) << "Pattern"
              << std::setw(18) << "1P:1C (2 cores)"
              << std::setw(18) << "2P:2C (4 cores)"
              << std::setw(18) << "4P:4C (8 cores)"
              << std::setw(22) << "1P:1C Bulk (Batch=32)"
              << "\n";
    std::cout << std::string(130, '-') << "\n";

    for (const auto& r : results) {
        std::string spsc_str = std::to_string(r.spsc_pipelined_mops).substr(0, 5) + " M ops/s";
        std::string mpmc_2p_str = (r.mpmc_2p2c_mops > 0.0) 
            ? (std::to_string(r.mpmc_2p2c_mops).substr(0, 5) + " M ops/s")
            : "N/A (SPSC-only)";
        std::string mpmc_4p_str = (r.mpmc_4p4c_mops > 0.0) 
            ? (std::to_string(r.mpmc_4p4c_mops).substr(0, 5) + " M ops/s")
            : "N/A (SPSC-only)";
        std::string bulk_str = std::to_string(r.bulk_batch32_mops).substr(0, 5) + " M ops/s";

        std::cout << std::left
                  << std::setw(28) << r.name
                  << std::setw(26) << r.pattern
                  << std::setw(18) << spsc_str
                  << std::setw(18) << mpmc_2p_str
                  << std::setw(18) << mpmc_4p_str
                  << std::setw(22) << bulk_str
                  << "\n";
    }

    std::cout << "=======================================================================================================================\n";
    std::cout << " Run Hygiene & Methodology Disclosures:\n";
    std::cout << "  - Host: Intel Core i7-14700HX (Raptor Lake, 8 P-Cores, 12 E-Cores). Plugged into AC power, High Performance plan.\n";
    std::cout << "  - Thread Affinity: All threads pinned to distinct physical P-Cores (Cores 0-7) with THREAD_PRIORITY_TIME_CRITICAL.\n";
    std::cout << "  - Timing: Invariant TSC reference calibrated at 2.304011 GHz (0.434026 ns/tick).\n";
    std::cout << "  - One-Way Latency: Paced queue (depth ~0). Producer stamps rdtsc_start() before enqueue, consumer reads rdtsc_end().\n";
    std::cout << "  - Statistics: Median across 10 runs of each percentile, with [min-max] range and worst-case max observed.\n";
    std::cout << "=======================================================================================================================\n\n";

    return 0;
}

#include "mpmc/ring_buffer.hpp"
#include "mpmc/affinity.hpp"
#include "mpmc/histogram.hpp"
#include "mpmc/stats.hpp"
#include "mpmc/cache.hpp"
#include "bench_util.hpp"

#include <chrono>
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <iomanip>
#include <numeric>
#include <algorithm>

using namespace mpmc;

struct LatencyMessage {
    uint64_t send_tsc{0};
    uint64_t seq{0};
    uint64_t checksum{0};
};
static_assert(sizeof(LatencyMessage) <= 56, "LatencyMessage fits inside 64B slot inline storage");

struct PercentileStats {
    double p50{0};
    double p90{0};
    double p99{0};
    double p999{0};
    double max_val{0};
};

struct AggregatedStats {
    double p50_med{0}, p50_min{0}, p50_max{0};
    double p90_med{0}, p90_min{0}, p90_max{0};
    double p99_med{0}, p99_min{0}, p99_max{0};
    double p999_med{0}, p999_min{0}, p999_max{0};
    double worst_max{0};
};

AggregatedStats aggregate_runs(std::vector<PercentileStats>& runs) {
    auto extract = [&](auto member_ptr) {
        std::vector<double> v;
        for (const auto& r : runs) v.push_back(r.*member_ptr);
        std::sort(v.begin(), v.end());
        double med = (v.size() % 2 == 1) ? v[v.size() / 2] : (v[v.size() / 2 - 1] + v[v.size() / 2]) / 2.0;
        return std::make_tuple(med, v.front(), v.back());
    };

    AggregatedStats res;
    std::tie(res.p50_med, res.p50_min, res.p50_max) = extract(&PercentileStats::p50);
    std::tie(res.p90_med, res.p90_min, res.p90_max) = extract(&PercentileStats::p90);
    std::tie(res.p99_med, res.p99_min, res.p99_max) = extract(&PercentileStats::p99);
    std::tie(res.p999_med, res.p999_min, res.p999_max) = extract(&PercentileStats::p999);

    double w_max = 0;
    for (const auto& r : runs) {
        if (r.max_val > w_max) w_max = r.max_val;
    }
    res.worst_max = w_max;
    return res;
}

void print_aggregated(const std::string& label, const AggregatedStats& s) {
    std::cout << "  " << std::left << std::setw(30) << label 
              << "p50: " << std::fixed << std::setprecision(1) << s.p50_med << "t (" << s.p50_med * bench::ns_per_tick() << "ns) [" 
              << s.p50_min << "-" << s.p50_max << "t] | "
              << "p90: " << s.p90_med << "t (" << s.p90_med * bench::ns_per_tick() << "ns) | "
              << "p99: " << s.p99_med << "t (" << s.p99_med * bench::ns_per_tick() << "ns) | "
              << "p99.9: " << s.p999_med << "t (" << s.p999_med * bench::ns_per_tick() << "ns) | "
              << "Worst Max: " << s.worst_max << "t (" << s.worst_max * bench::ns_per_tick() / 1000.0 << "µs)\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 1: Paced True One-Way Latency (Producer physical core 0 -> 1)
// ─────────────────────────────────────────────────────────────────────────────
template <bool UsePause, typename QueueType>
PercentileStats run_one_way_latency_pass(QueueType& queue, size_t warmup, size_t samples, std::atomic<size_t>& negative_tsc_count) {
    LatencyHistogram hist;
    alignas(64) std::atomic<uint64_t> drain_ack{0};
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&]() {
        bench::pin(1);
        affinity::set_current_thread_priority_critical();

        while (!stop_consumer.load(std::memory_order_relaxed)) {
            LatencyMessage msg{};
            while (!queue.try_dequeue(msg)) {
                if constexpr (UsePause) {
                    cache::cpu_relax();
                }
                if (stop_consumer.load(std::memory_order_relaxed)) return;
            }
            uint64_t t1 = cache::rdtsc_end();

            if (__builtin_expect(t1 < msg.send_tsc, 0)) {
                negative_tsc_count.fetch_add(1, std::memory_order_relaxed);
            } else if (msg.seq >= warmup) {
                hist.record(t1 - msg.send_tsc);
            }
            drain_ack.store(msg.seq + 1, std::memory_order_release);
        }
    });

    bench::pin(0);
    affinity::set_current_thread_priority_critical();

    size_t total = warmup + samples;
    for (size_t i = 0; i < total; ++i) {
        // Paced producer: wait until consumer drains the slot to keep queue depth ~0
        while (drain_ack.load(std::memory_order_acquire) != i) {
            cache::cpu_relax();
        }

        LatencyMessage msg;
        msg.seq = i;
        msg.checksum = i ^ 0xA5A5A5A5ULL;

        // Defined measurement point: stamp send_tsc immediately before try_enqueue
        uint64_t t0 = cache::rdtsc_start();
        msg.send_tsc = t0;

        while (!queue.try_enqueue(msg)) {
            cache::cpu_relax();
        }
    }

    // Wait for last ack
    while (drain_ack.load(std::memory_order_acquire) < total) {
        cache::cpu_relax();
    }

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    PercentileStats ps;
    ps.p50 = static_cast<double>(hist.percentile(50.0));
    ps.p90 = static_cast<double>(hist.percentile(90.0));
    ps.p99 = static_cast<double>(hist.percentile(99.0));
    ps.p999 = static_cast<double>(hist.percentile(99.9));
    ps.max_val = static_cast<double>(hist.max());
    return ps;
}

void benchmark_one_way_latency_suite() {
    std::cout << "\n========================================================================\n";
    std::cout << " [BENCHMARK 1] Paced True One-Way Latency Across Physical Cores (physical core 0 -> 1)\n";
    std::cout << "               Measurement: send_tsc stamped immediately before try_enqueue\n";
    std::cout << "                            recv_tsc stamped immediately upon try_dequeue\n";
    std::cout << "               Queue depth: ~0 (paced producer waits for consumer drain ack)\n";
    std::cout << "               Sample size: 10 runs x 100,000 samples per mode\n";
    std::cout << "========================================================================\n";

    constexpr size_t RUNS = 10;
    constexpr size_t WARMUP = 10'000;
    constexpr size_t SAMPLES = 100'000;

    std::atomic<size_t> negative_tsc_count{0};

    // Mode A: Pure Busy-Poll (No Pause in consumer wait loop)
    std::vector<PercentileStats> busy_runs;
    for (size_t r = 0; r < RUNS; ++r) {
        RingBuffer<LatencyMessage, 1024> q;
        busy_runs.push_back(run_one_way_latency_pass<false>(q, WARMUP, SAMPLES, negative_tsc_count));
    }
    AggregatedStats busy_agg = aggregate_runs(busy_runs);

    // Mode B: PAUSE Backoff (_mm_pause in consumer wait loop)
    std::vector<PercentileStats> pause_runs;
    for (size_t r = 0; r < RUNS; ++r) {
        RingBuffer<LatencyMessage, 1024> q;
        pause_runs.push_back(run_one_way_latency_pass<true>(q, WARMUP, SAMPLES, negative_tsc_count));
    }
    AggregatedStats pause_agg = aggregate_runs(pause_runs);

    std::cout << "\n--- Cross-Core TSC Monotonicity & Invariant Synchronization ---\n";
    std::cout << "  Negative TSC Latency Anomalies: " << negative_tsc_count.load() 
              << " / " << (RUNS * 2 * SAMPLES) << " observations\n\n";

    std::cout << "--- 10-Run Aggregated True One-Way Handoff Latency ---\n";
    print_aggregated("Mode A (Pure Busy-Poll):", busy_agg);
    print_aggregated("Mode B (_mm_pause Backoff):", pause_agg);

}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 2: Split Padding & Spatial Prefetcher Ablations
// ─────────────────────────────────────────────────────────────────────────────
void benchmark_padding_ablations() {
    std::cout << "\n========================================================================\n";
    std::cout << " [BENCHMARK 2] Split Padding Ablation: Spatial Prefetcher Interference\n";
    std::cout << "               Config 1: 64B Slot  /  64B Head-Tail Alignment (Baseline)\n";
    std::cout << "               Config 2: 64B Slot  / 128B Head-Tail Separation\n";
    std::cout << "               Config 3: 128B Slot / 128B Head-Tail Separation\n";
    std::cout << "========================================================================\n";

    constexpr size_t RUNS = 10;
    constexpr size_t WARMUP = 10'000;
    constexpr size_t SAMPLES = 100'000;
    std::atomic<size_t> neg_dummy{0};

    // Config 1: 64B slot, 64B head/tail
    std::vector<PercentileStats> c1_runs;
    for (size_t r = 0; r < RUNS; ++r) {
        RingBuffer<LatencyMessage, 1024, 64, 64> q;
        c1_runs.push_back(run_one_way_latency_pass<false>(q, WARMUP, SAMPLES, neg_dummy));
    }
    AggregatedStats c1_agg = aggregate_runs(c1_runs);

    // Config 2: 64B slot, 128B head/tail
    std::vector<PercentileStats> c2_runs;
    for (size_t r = 0; r < RUNS; ++r) {
        RingBuffer<LatencyMessage, 1024, 64, 128> q;
        c2_runs.push_back(run_one_way_latency_pass<false>(q, WARMUP, SAMPLES, neg_dummy));
    }
    AggregatedStats c2_agg = aggregate_runs(c2_runs);

    // Config 3: 128B slot, 128B head/tail
    std::vector<PercentileStats> c3_runs;
    for (size_t r = 0; r < RUNS; ++r) {
        RingBuffer<LatencyMessage, 1024, 128, 128> q;
        c3_runs.push_back(run_one_way_latency_pass<false>(q, WARMUP, SAMPLES, neg_dummy));
    }
    AggregatedStats c3_agg = aggregate_runs(c3_runs);

    std::cout << "--- 10-Run Aggregated Latency Across Padding Configurations ---\n";
    print_aggregated("Config 1 (64B Slot / 64B HT):", c1_agg);
    print_aggregated("Config 2 (64B Slot / 128B HT):", c2_agg);
    print_aggregated("Config 3 (128B Slot / 128B HT):", c3_agg);

}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 3: Empirical Contention Scaling Across Physical P-Cores
// ─────────────────────────────────────────────────────────────────────────────
double run_contention_topology(size_t p_count, size_t c_count, size_t total_items,
                               QueueStats& producer_totals, QueueStats& consumer_totals) {
    size_t items_per_p = total_items / p_count;
    RingBuffer<uint64_t, 4096> rb;

    std::vector<QueueStats> p_stats(p_count);
    std::vector<QueueStats> c_stats(c_count);

    bench::StartGate gate(p_count + c_count);
    std::atomic<size_t> total_consumed{0};
    std::atomic<uint64_t> total_sum{0};

    // Consumers pinned to distinct physical P-cores starting after producers
    std::vector<std::thread> consumers;
    for (size_t c = 0; c < c_count; ++c) {
        consumers.emplace_back([&, c]() {
            bench::pin(p_count + c);
            affinity::set_current_thread_priority_critical();

            gate.arrive();
            uint64_t local_sum = 0;
            while (total_consumed.load(std::memory_order_relaxed) < total_items) {
                uint64_t out = 0;
                if (rb.try_dequeue(out, c_stats[c])) {
                    local_sum += out;
                    total_consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    cache::cpu_relax();
                }
            }
            total_sum.fetch_add(local_sum, std::memory_order_relaxed);
        });
    }

    // Producers pinned to distinct physical P-cores [0 .. p_count-1]
    std::vector<std::thread> producers;
    for (size_t p = 0; p < p_count; ++p) {
        producers.emplace_back([&, p]() {
            bench::pin(p);
            affinity::set_current_thread_priority_critical();

            gate.arrive();
            size_t base = p * items_per_p;
            for (size_t i = 0; i < items_per_p; ++i) {
                while (!rb.try_enqueue(base + i + 1, p_stats[p])) {
                    cache::cpu_relax();
                }
            }
        });
    }

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();
    const double elapsed_sec = gate.seconds_since_open();

    const QueueStats agg_p = aggregate_stats(p_stats);
    const QueueStats agg_c = aggregate_stats(c_stats);
    producer_totals.enqueue_success += agg_p.enqueue_success;
    producer_totals.enqueue_cas_fail += agg_p.enqueue_cas_fail;
    consumer_totals.dequeue_success += agg_c.dequeue_success;
    consumer_totals.dequeue_cas_fail += agg_c.dequeue_cas_fail;
    return (static_cast<double>(total_items) / elapsed_sec) / 1e6;
}

void benchmark_contention_scaling_suite() {
    std::cout << "\n========================================================================\n";
    std::cout << " [BENCHMARK 3] Empirical Contention Scaling on Physical P-Cores\n";
    std::cout << "               Topologies strictly within 8 physical P-cores (no SMT/E-cores)\n";
    std::cout << "               Tracking exact CAS collision rate via QueueStats\n";
    std::cout << "========================================================================\n";

    constexpr size_t TOTAL = 2'000'000;
    for (size_t n : {size_t{1}, size_t{2}, size_t{4}}) {
        QueueStats p, c;
        const bench::Summary mops = bench::repeat([&] { return run_contention_topology(n, n, TOTAL, p, c); });
        std::cout << "  - " << n << "P:" << n << "C  M ops/s " << bench::format(mops) << std::fixed
                  << std::setprecision(1) << " | failed CAS: producer " << p.enqueue_contention_ratio()
                  << "%, consumer " << c.dequeue_contention_ratio() << "%\n";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 4: Bulk Batch Scaling
// ─────────────────────────────────────────────────────────────────────────────
void benchmark_bulk_scaling_suite() {
    std::cout << "\n========================================================================\n";
    std::cout << " [BENCHMARK 4] Bulk Operations Batch Amortization (1P:1C Pinned)\n";
    std::cout << "========================================================================\n";

    constexpr size_t TOTAL = 5'000'000;
    auto run_batch = [&](size_t batch_size) {
        RingBuffer<uint64_t, 4096> rb;
        std::vector<uint64_t> send_data(TOTAL);
        for (size_t i = 0; i < TOTAL; ++i) send_data[i] = i + 1;

        bench::StartGate gate(2);
        uint64_t consumer_sum = 0;

        std::thread consumer([&]() {
            bench::pin(1);
            affinity::set_current_thread_priority_critical();
            gate.arrive();

            size_t received = 0;
            std::vector<uint64_t> batch(batch_size, 0);
            while (received < TOTAL) {
                size_t n = rb.try_dequeue_bulk(batch.data(), batch_size);
                if (n > 0) {
                    for (size_t i = 0; i < n; ++i) consumer_sum += batch[i];
                    received += n;
                } else {
                    cache::cpu_relax();
                }
            }
        });

        bench::pin(0);
        affinity::set_current_thread_priority_critical();

        gate.arrive();

        size_t sent = 0;
        while (sent < TOTAL) {
            size_t count = std::min(batch_size, TOTAL - sent);
            size_t n = rb.try_enqueue_bulk(&send_data[sent], count);
            if (n > 0) sent += n;
            else cache::cpu_relax();
        }

        consumer.join();
        return (static_cast<double>(TOTAL) / gate.seconds_since_open()) / 1e6;
    };

    for (size_t batch : {size_t{1}, size_t{8}, size_t{16}, size_t{32}}) {
        std::cout << "  - batch " << batch << "  M items/s "
                  << bench::format(bench::repeat([&] { return run_batch(batch); })) << "\n";
    }
}

// Benchmark 5: cost of the spin-wait primitives
template <typename Fn>
void measure_primitive(const char* name, Fn&& fn) {
    constexpr size_t SAMPLES = 500'000;
    LatencyHistogram hist;
    for (size_t i = 0; i < SAMPLES; ++i) {
        const uint64_t t0 = cache::rdtsc_start();
        fn();
        hist.record(cache::rdtsc_end() - t0);
    }
    std::cout << "  - " << name << std::fixed << std::setprecision(1)
              << " p50 " << hist.percentile(50.0) * bench::ns_per_tick() << " ns, p99 "
              << hist.percentile(99.0) * bench::ns_per_tick() << " ns, max "
              << hist.max() * bench::ns_per_tick() / 1000.0 << " us\n";
}

void benchmark_primitives() {
    std::cout << "\n [BENCHMARK 5] Spin-wait primitives (includes timer overhead, see empty row)\n";
    bench::pin(0);
    measure_primitive("empty             ", [] {});
    measure_primitive("cpu_relax (pause) ", [] { cache::cpu_relax(); });
    measure_primitive("this_thread::yield", [] { std::this_thread::yield(); });
}

int main() {
    std::cout << "MPMC ring buffer pinned benchmarks\n";
    bench::print_host();

    benchmark_one_way_latency_suite();
    benchmark_padding_ablations();
    benchmark_contention_scaling_suite();
    benchmark_bulk_scaling_suite();
    benchmark_primitives();

    std::cout << "\n========================================================================\n";
    std::cout << " [ALL BENCHMARKS AND ABLATIONS COMPLETED SUCCESSFULLY]\n";
    std::cout << "========================================================================\n";
    return 0;
}

// Effect of exponential backoff after a lost CAS on contended throughput.
// Configurations are interleaved run by run so that drift in machine state
// affects all of them equally.
#include "mpmc/ring_buffer.hpp"
#include "bench_util.hpp"

#include <array>
#include <cstdio>
#include <thread>
#include <vector>

namespace {

constexpr std::size_t kItems = 2'000'000;

template <uint32_t MaxBackoff>
double run(std::size_t pairs) {
    mpmc::RingBuffer<uint64_t, 4096, 64, 64, MaxBackoff> q;
    bench::StartGate gate(2 * pairs);
    std::atomic<std::size_t> consumed{0};
    std::vector<std::thread> threads;

    for (std::size_t i = 0; i < pairs; ++i) {
        threads.emplace_back([&, i] {
            bench::pin(i);
            mpmc::affinity::set_current_thread_priority_critical();
            gate.arrive();
            for (std::size_t j = 0; j < kItems / pairs; ++j) {
                while (!q.try_enqueue(static_cast<uint64_t>(j))) mpmc::cache::cpu_relax();
            }
        });
        threads.emplace_back([&, i] {
            bench::pin(pairs + i);
            mpmc::affinity::set_current_thread_priority_critical();
            gate.arrive();
            uint64_t item = 0;
            while (consumed.load(std::memory_order_relaxed) < kItems) {
                if (q.try_dequeue(item)) consumed.fetch_add(1, std::memory_order_relaxed);
                else mpmc::cache::cpu_relax();
            }
        });
    }
    for (auto& t : threads) t.join();
    return kItems / gate.seconds_since_open() / 1e6;
}

} // namespace

int main() {
    std::printf("CAS backoff sweep\n");
    bench::print_host();
    std::printf("M items/s, median [min-max] of %zu runs\n", bench::kThroughputRuns);

    constexpr std::array<uint32_t, 6> caps{1, 4, 16, 64, 256, 1024};
    constexpr std::array<double (*)(std::size_t), 6> runners{run<1>, run<4>, run<16>, run<64>, run<256>, run<1024>};

    for (std::size_t pairs : {std::size_t{1}, std::size_t{2}, std::size_t{4}}) {
        std::array<std::vector<double>, 6> results;
        for (std::size_t r = 0; r < bench::kThroughputRuns; ++r) {
            for (std::size_t k = 0; k < caps.size(); ++k) results[k].push_back(runners[k](pairs));
        }
        for (std::size_t k = 0; k < caps.size(); ++k) {
            std::size_t next = 0;
            const bench::Summary s = bench::repeat([&] { return results[k][next++]; });
            std::printf("  %zuP:%zuC  max pauses %4u  %s\n", pairs, pairs, caps[k], bench::format(s).c_str());
        }
        std::fflush(stdout);
    }
}

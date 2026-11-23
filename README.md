# Low-Latency Bounded MPMC Ring Buffer

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows%20(x86--64%20%7C%20AArch64)-lightgrey.svg)]()

A header-only C++20 implementation of Dmitry Vyukov's bounded Multi-Producer Multi-Consumer (MPMC) queue, extended with **56-byte inline payload storage**, **CRTP compile-time policy dispatch**, and **contiguous bulk CAS amortization**.

> **Progress Guarantees & Preemption Semantics**:
> Like all queues in the Dmitry Vyukov bounded family, this design is **blocking under preemption**:
> * Index reservation via atomic CAS on `head_` and `tail_` is **lock-free**.
> * Slot publication (the atomic release store on `sequence`) is **blocking**. If a thread is preempted or descheduled after claiming a slot ticket but before publishing its sequence counter, that slot (and any subsequent lap through that slot index) stalls until the thread resumes execution.
> * In low-latency deployment environments, this vulnerability is mitigated by pinning threads to dedicated, isolated CPU cores (`isolcpus`, `nohz_full`, real-time FIFO scheduling), eliminating involuntary OS preemption on the hot path.

---

## Performance Highlights

Measured on physical P-Cores pinned with high priority (**Intel® Core™ i7-14700HX, Raptor Lake**, calibrated Invariant TSC: 2.304 GHz, 0.434 ns/tick, plugged into AC power):

| Metric | Measurement | Description |
|---|---|---|
| **Single-Item One-Way Latency (p50, Busy-Poll)** | **45.1 ns** (104 TSC ticks) | Paced true one-way handoff (Core 0 $\to$ Core 1) in pure busy-poll mode |
| **Single-Item One-Way Latency (p50, PAUSE)** | **50.3 ns** (116 TSC ticks) | Paced true one-way handoff with `_mm_pause()` in consumer wait loop |
| **Single-Item Pipelined Streaming (1P:1C)** | **101.9 Million ops/sec** | ~9.81 ns per unbatched item across physical cores with payload checksummed |
| **Multi-Core Streaming (2P:2C)** | **15.93 Million ops/sec** | 4 physical P-cores under contention (40.0% producer CAS collisions) |
| **Multi-Core Streaming (4P:4C)** | **11.53 Million ops/sec** | Symmetrically contended streaming across 8 physical P-cores (63.4% CAS collisions) |
| **Batched Bulk Streaming (Batch = 32)** | **571.1 - 628.1 Million ops/sec** | ~1.59 - 1.75 ns per item when amortizing atomic CAS claims across 32 items |
| **Zero Memory Allocation** | **0 bytes** | Zero calls to `malloc`/`new` on hot enqueue/dequeue paths |

> *Note: All latency figures report median percentiles across 10 independent runs of 50,000–100,000 samples each. See [Industry Baseline Comparison](#industry-baseline-comparison) for full baseline tables and [Ablation Studies](#ablation-studies) for backoff and padding benchmarks.*

---

## Industry Baseline Comparison

To objectively evaluate performance and architectural trade-offs, `mpmc::RingBuffer` was benchmarked under identical hardware, compiler flags (`-O3 -march=native -fno-rtti`), and core-pinning conditions against four industry reference queues:
1. **mpmc::RingBuffer (Ours)**: Dmitry Vyukov ticket array with inline by-value storage ($T \le 56$B) and contiguous bulk CAS amortization.
2. **rigtorp::MPMCQueue**: Erik Rigtorp's industry-standard bounded turn-based MPMC queue.
3. **rigtorp::SPSCQueue**: Erik Rigtorp's dedicated single-producer single-consumer queue (load/store synchronization without atomic CAS).
4. **moodycamel::ConcurrentQueue**: Cameron Desrochers' unbounded block-token concurrent queue.
5. **std::mutex + std::deque**: Standard thread-safe mutex and condition-variable baseline.

### Table 1: Paced True One-Way Latency (Core 0 $\to$ Core 1, 10 Runs $\times$ 50,000 samples)
*Measurement point: Producer stamps `rdtsc_start()` (`_mm_lfence; rdtsc; _mm_lfence`) immediately before `try_enqueue`; Consumer records `rdtsc_end()` (`rdtscp; _mm_lfence`) immediately upon `try_dequeue`. Includes the full CAS claim, payload copy, and sequence publication. Paced producer waits for consumer drain ack so queue depth is $\approx 0$. TSC cross-core synchronization confirmed empirically (0 negative latency anomalies across 2,000,000 samples).*

| Queue Implementation | Architecture Pattern | p50 [min-max] | p90 | p99 | p99.9 | Worst Max |
|---|---|---|---|---|---|---|
| **mpmc::RingBuffer (Ours)** | Bounded Ticket Array (In-line) | **126t (54ns)** [116-128t] | 152t / 65ns | 168t / 72ns | 208t / 90ns | 86,536t / 37.5µs |
| **rigtorp::MPMCQueue** | Bounded Turn Array | 128t (55ns) [128-136t] | 152t / 65ns | **160t / 69ns** | **168t / 72ns** | **32,880t / 14.2µs** |
| **rigtorp::SPSCQueue** | Dedicated SPSC (Load/Store) | *92t (39ns)* [92-96t] | *112t / 48ns* | *116t / 50ns* | *140t / 60ns* | 80,958t / 35.1µs |
| **moodycamel::ConcurrentQueue** | Unbounded Block Token Queue | 122t (52ns) [116-128t] | 152t / 65ns | 184t / 79ns | 224t / 97ns | 64,276t / 27.8µs |
| **std::mutex + std::deque** | Mutex + Condvar + Deque | 126t (54ns) [124-136t] | 736t / 319ns | 864t / 374ns | 2,016t / 874ns | 149,044t / 64.6µs |

### Table 2: Streaming Throughput by Topology Across Physical P-Cores
*Streaming throughput with consumers actively reading and checksumming payload data. Topologies are strictly confined to physical P-Cores on a single socket (no SMT hyperthreads, no E-cores).*

| Queue Implementation | Architecture Pattern | 1P:1C (2 cores) | 2P:2C (4 cores) | 4P:4C (8 cores) | 1P:1C Bulk (Batch=32) |
|---|---|---|---|---|---|
| **mpmc::RingBuffer (Ours)** | Bounded Ticket Array (In-line) | **101.9 M ops/s** | 15.93 M ops/s | 11.53 M ops/s | **571.1 M ops/s** |
| **rigtorp::MPMCQueue** | Bounded Turn Array | 75.68 M ops/s | 18.14 M ops/s | 12.19 M ops/s | 76.00 M ops/s\* |
| **rigtorp::SPSCQueue** | Dedicated SPSC (Load/Store) | *628.1 M ops/s* | *N/A (SPSC-only)* | *N/A (SPSC-only)* | *629.3 M ops/s* |
| **moodycamel::ConcurrentQueue** | Unbounded Block Token Queue | 63.26 M ops/s | **43.33 M ops/s** | **19.28 M ops/s** | 402.3 M ops/s |
| **std::mutex + std::deque** | Mutex + Condvar + Deque | 12.86 M ops/s | 16.17 M ops/s | 10.55 M ops/s | 118.2 M ops/s |

*\*Note: `rigtorp::MPMCQueue` does not expose a native bulk CAS API; bulk throughput was measured using a batched single-item loop.*

### Architectural Analysis & Trade-offs

1. **The Cost of MPMC Synchronization (`rigtorp::SPSCQueue` vs `mpmc::RingBuffer`)**:
   * A dedicated SPSC queue requires no atomic read-modify-write (CAS) instructions—only atomic loads and stores with acquire/release semantics. This yields an ultra-fast **39 ns** one-way handoff and **628.1M ops/s** throughput.
   * `mpmc::RingBuffer` pays the hardware cost of atomic `compare_exchange_weak` on head/tail indices to safely arbitrate multiple producers and consumers, delivering **54 ns** median one-way handoff and **101.9M ops/s** in unbatched 1P:1C mode.
2. **Contended Multi-Producer Scaling (4P:4C Contention Analysis)**:
   * Under heavy multi-producer contention (4P:4C on 8 physical P-cores), throughput drops to **11.53M ops/s**, trailing `rigtorp::MPMCQueue` (12.19M ops/s) and `moodycamel::ConcurrentQueue` (19.28M ops/s).
   * **Root Cause (Single-CAS Serialization)**: In a single circular ring, every producer must execute a `LOCK CMPXCHG` on the shared `head_` cache line, forcing continuous cache line invalidations and bus locking across cores (empirically measured at **63.4% CAS collisions** at 4P:4C). Moodycamel bypasses this by allocating thread-local sub-queues (at the trade-off of losing global FIFO ordering and incurring dynamic chunk allocations).
   * **Next Design Steps**: To scale past 4 producers on a single ring, next-step architectures would employ flat-combining (where one core claims a batch on behalf of all producers) or partitioned sub-rings.
3. **Contiguous Bulk Amortization (`try_enqueue_bulk` / `try_dequeue_bulk`)**:
   * By acquiring contiguous slot spans in a single atomic CAS and publishing via release stores, `mpmc::RingBuffer` processes **571.1 Million items/sec** (~1.75 ns/item). This achieves a **7.5× speedup compared to a batched single-item loop on `rigtorp::MPMCQueue`** (which does not expose a native contiguous bulk CAS API), while maintaining strict bounded zero-allocation guarantees.
4. **Tail Latency & Predictability**:
   * Both bounded circular array designs (`mpmc::RingBuffer` and `rigtorp::MPMCQueue`) maintain tight tail distributions through $p99.9$ (<90 ns) due to deterministic cache footprint, zero heap allocation, and contiguous memory access. Mutex-based queues suffer severe tail spikes ($p99.9$ of 874 ns, max of 64.6 µs) due to OS kernel context switches.

---

## Ablation Studies

Empirical micro-architectural ablations conducted on physical P-Cores of the Intel Core i7-14700HX:

### 1. Backoff Policy Ablation: Pure Busy-Poll vs. `_mm_pause`
*Evaluates the effect of consumer spin-wait backoff on true one-way handoff latency (10 runs $\times$ 100,000 samples, paced queue depth $\approx 0$):*

| Backoff Mode | p50 | p90 | p99 | p99.9 | Worst Max | Trade-off / Deployment Context |
|---|---|---|---|---|---|---|
| **Mode A: Pure Busy-Poll** (`while (!try_dequeue) {}`) | **45.1 ns** (104t) [104-120t] | **50.3 ns** (116t) | **53.0 ns** (122t) | **57.3 ns** (132t) | **21.2 µs** | Optimal on dedicated isolated cores (`isolcpus`); eliminates pipeline stall. |
| **Mode B: `_mm_pause` Backoff** (`cpu_relax()`) | **50.3 ns** (116t) [116-116t] | **59.0 ns** (136t) | **59.0 ns** (136t) | **66.0 ns** (152t) | **78.3 µs** | Essential on shared/SMT cores; cuts power and avoids hyperthread pipeline starvation. |

* **Finding**: `_mm_pause()` on Raptor Lake has an empirical latency of ~42.5 ns. Inserting it into the consumer polling loop adds approximately **5.2 ns** to median handoff latency ($p50$) and **8.7 ns** to tail latency ($p99$) when a producer publishes right after a pause begins.

### 2. Split Padding Ablation: Spatial Prefetcher Interference
*Tests whether Intel's L2 Adjacent Cache Line Prefetcher (which fetches pairs of 64-byte lines as 128-byte sectors) causes cross-core interference on `head_`/`tail_` or slot boundaries:*

| Configuration | Slot Size | Head/Tail Separation | p50 Latency | p90 Latency | p99 Latency | Conclusion |
|---|---|---|---|---|---|---|
| **Config 1 (Baseline)** | 64 bytes | 64 bytes | **45.1 ns** (104t) | 50.3 ns (116t) | 53.8 ns (124t) | Reference design (1 cache line per slot). |
| **Config 2 (128B Head/Tail)** | 64 bytes | 128 bytes | **45.1 ns** (104t) | **48.6 ns** (112t) | **52.1 ns** (120t) | **Null result**: 128B index separation yields negligible change. |
| **Config 3 (128B Slot & Indices)**| 128 bytes | 128 bytes | 50.3 ns (116t) | 53.0 ns (122t) | 55.6 ns (128t) | Latency slightly increases (+5 ns) due to larger cache footprint. |

### 3. Empirical Contention Scaling Telemetry
*Measures actual CAS collision ratios and throughput degradation across strictly physical P-core topologies:*

| Topology | Physical Cores | Streaming Throughput | Latency per Item | Producer CAS Collision Rate | Consumer CAS Collision Rate |
|---|---|---|---|---|---|
| **1P:1C** | 2 cores (Core 0 $\to$ Core 1) | **60.49 M ops/s** | 16.5 ns/op | **0.00%** (0 fails) | **0.00%** (0 fails) |
| **2P:2C** | 4 cores (Cores 0-1 $\to$ 2-3) | **19.63 M ops/s** | 51.0 ns/op | **40.00%** (1,333,470 fails) | **16.40%** (392,362 fails) |
| **4P:4C** | 8 cores (Cores 0-3 $\to$ 4-7) | **11.33 M ops/s** | 88.3 ns/op | **63.39%** (3,463,038 fails) | **59.69%** (2,961,690 fails) |

---

## Architectural Principles

```
  Producer Core 0  ───┐                                ┌───  Consumer Core 4
  Producer Core 1  ───┼──> [ Lock-Free MPMC Ring ] ───┼──>  Consumer Core 5
  Producer Core 2  ───┘       (Power-of-2 Sized)      └───  Consumer Core 6
```

### 1. 64-Byte Cache Line Isolation & Inline Payload Storage
On modern x86-64 and ARM64 architectures, the L1 data cache operates in 64-byte lines. If multiple cores write to distinct variables located on the same 64-byte line, hardware coherency protocols (MESI/MOESI) invalidate the line back and forth across cores (false sharing ping-pong).
* **Dual-Mode Slot Architecture (`slot.hpp`)**:
  * **By-Value Inline Mode ($T \le 56$ bytes)**: Payloads up to 56 bytes (such as financial orders, tick events, or 64-bit timestamps) are stored directly inside the slot padding. This eliminates the second cache miss entirely:
    ```
    ┌─────────────────────────────────────────────────────────────┐
    │  Byte 0–7   │  sequence  │  atomic<size_t>   (8 bytes)      │
    │  Byte 8–N   │  value     │  T                (sizeof(T) B)  │
    │  Byte N–63  │  padding   │  char[56 - sizeof(T)]            │
    └─────────────────────────────────────────────────────────────┘
                  Total: 8 + sizeof(T) + Pad = exactly 64 bytes
    ```
  * **Pointer + Pool Mode ($T > 56$ bytes)**: For large, variable, or polymorphic objects, the queue stores `T*` pointers paired with `ObjectPool<T>` pre-allocated slab memory.
* **Independent `head_` and `tail_`**: Producers contend on `head_` and consumers contend on `tail_`. They reside on distinct 64-byte cache lines separated by trailing padding (`char pad[56]`) to eliminate false sharing between enqueue and dequeue workers.

### 2. Dmitry Vyukov Sequence Ticket Handshake
Instead of locking a global mutex or relying on a centralized counter, synchronization is governed by an atomic `sequence` ticket embedded in every slot:
* **Initial State**: Slot $i$ holds `sequence = i`.
* **Producer Handshake**: Producer checks if `slot->sequence == pos`. Upon atomic CAS claim of `head_`, it writes `slot->value` and executes a `release` store on `sequence = pos + 1` (synchronizes-with consumer acquire load).
* **Consumer Handshake**: Consumer checks if `slot->sequence == pos + 1`. Upon atomic CAS claim of `tail_`, it reads `slot->value` and executes a `release` store on `sequence = pos + Capacity` (synchronizes-with subsequent lap producer acquire load).
* **Why `pos + Capacity` instead of `pos + 1`?**: Incrementing by `pos + Capacity` acts as a generation sentinel. It ensures that producers on generation $N+1$ cannot write into a slot until generation $N$ consumers have completely finished reading and released it. 64-bit monotonic sequences prevent ABA wrap-around under all practical operational horizons.

### 3. User-Space Spin-Wait & Hardware Pause Backoff
* **Empirical Cost of OS Yielding**: Calling `std::this_thread::yield()` or `sched_yield` deschedules the thread into the operating system kernel. On our benchmark host, measuring 500,000 invocations reveals:
  * `std::this_thread::yield()`: **p50 = 162 ticks (70.3 ns)**, **Max = 15,284 ticks (6.63 µs)**.
  * In contrast, `_mm_pause()`: **p50 = 98 ticks (42.5 ns)**, **p99 = 100 ticks (43.4 ns)**, executed entirely in user space.
* The backoff engine operates strictly in user space via architecture-specific pause hints:
  * **x86-64**: `_mm_pause` (`rep nop`, opcode `F3 90`). As specified in Intel SDM Vol. 2B, `PAUSE` introduces a finite pipeline delay, prevents speculative memory order mispredictions on loop exit, and reduces core power consumption.
  * **AArch64**: `yield` instruction (ARM DDI 0487), providing a hardware spin hint to the CPU core.
  * **RISC-V**: Standardized `nop` fallback.

### 4. Static Polymorphism (CRTP) Over Virtual Functions
Zero virtual functions, zero vtable pointers, and zero runtime branch indirections. Queue overflow/underflow policies are resolved at compile time:
* `RingBuffer<T, Cap>`: **Drop policy** (non-blocking, returns `false` immediately on full/empty).
* `SpinningRingBuffer<T, Cap>`: **Spin-wait policy** (spins with `cpu_relax()` backoff until space/item appears).
* *(Note: An overwrite-oldest policy was evaluated and intentionally removed because evicting the queue tail from concurrent producers introduces an unresolvable multi-producer race condition against concurrent consumers).*

### 5. Slab Object Pool Architecture (`pool.hpp`)
When transferring large payloads ($>56$ bytes), heap allocations (`new Order()`) in trading loops introduce allocator lock contention. `ObjectPool<T, Capacity>` pre-allocates contiguous memory at initialization and manages available slots through an internal lock-free ring buffer of pointers (`RingBuffer<T*, Capacity>`), achieving zero allocations on the hot path.

---

## Code Examples

### 1. Basic Enqueue and Dequeue (Inline By-Value Storage)
```cpp
#include "mpmc/ring_buffer.hpp"
#include <iostream>

struct MarketOrder {
    uint64_t order_id;
    double price;
    uint32_t qty;
    uint32_t venue_id;
};
static_assert(sizeof(MarketOrder) <= 56, "MarketOrder fits inline in 64B slot");

// Queue holds MarketOrder by value directly inside the 64-byte slots.
mpmc::RingBuffer<MarketOrder, 1024> queue;

MarketOrder order{1001, 150.25, 50, 1};

// Non-blocking try_enqueue (returns false immediately if queue is full)
if (queue.try_enqueue(order)) {
    // Successfully published (copied directly into slot inline storage)
}

// Non-blocking try_dequeue (returns false immediately if queue is empty)
MarketOrder received{};
if (queue.try_dequeue(received)) {
    std::cout << "Received Order ID: " << received.order_id 
              << " @ Price: " << received.price << "\n";
}
```

### 2. Zero-Allocation Pipeline with ObjectPool (Large Payloads)
```cpp
#include "mpmc/ring_buffer.hpp"
#include "mpmc/pool.hpp"

struct MassivePayload {
    char payload[256];
    uint64_t sequence;
};

// Pre-allocate 4096 objects in contiguous slab memory at startup
mpmc::ObjectPool<MassivePayload, 4096> pool;
mpmc::SpinningRingBuffer<MassivePayload*, 4096> queue;

// Producer Thread (Zero Malloc)
MassivePayload* obj = pool.acquire(); // Lock-free pop from pool slab
if (obj) {
    obj->sequence = 42;
    queue.enqueue(obj);                // Spin-wait publish pointer (SpinningRingBuffer policy)
}

// Consumer Thread (Zero Free)
MassivePayload* processed = queue.dequeue(); // Spin-wait dequeue
// ... process trade ...
pool.release(processed);               // Return to slab free-list for immediate reuse
```

### 3. High-Throughput Bulk Operations
```cpp
// Claim and publish up to 32 slots in a SINGLE atomic CAS
MarketOrder batch[32];
// ... populate batch ...
size_t enqueued = queue.try_enqueue_bulk(batch, 32);

// Pull up to 32 slots in a SINGLE atomic CAS
MarketOrder out_batch[32];
size_t dequeued = queue.try_dequeue_bulk(out_batch, 32);
```

### 4. Hardware Core Pinning & Contention Telemetry
```cpp
#include "mpmc/affinity.hpp"
#include "mpmc/stats.hpp"
#include <iostream>

// Pin current thread to physical Core 2 with real-time priority
mpmc::affinity::pin_current_thread_to_core(2);
mpmc::affinity::set_current_thread_priority_critical();

// Track thread-local CAS collision ratios without cross-thread cache thrashing
mpmc::QueueStats my_stats;
queue.try_enqueue(order, my_stats);

std::cout << "CAS Contention Ratio: " << my_stats.enqueue_contention_ratio() << "%\n";
```

---

## Benchmark Environment & Data Accounting

To ensure scientific honesty and reproducibility, this section documents the exact benchmarking environment, data definitions, and hardware behavior:

### 1. Hardware & Platform Details
* **CPU**: Intel® Core™ i7-14700HX (8 Performance Cores, 12 Efficient Cores, 28 Threads).
* **OS**: Windows 11 (MSYS2 / UCRT64 toolchain, GCC 13.1.0).
* **Compiler Flags**: `-O3 -march=native -Wall -Wextra -Wpedantic -fno-rtti`.
* **Thread Affinity**: Threads are explicitly pinned to distinct physical P-Cores using `SetThreadAffinityMask` with `THREAD_PRIORITY_TIME_CRITICAL`.

> **Environment Transparency & Linux Deployment**:
> * All primary benchmark figures in this repository were measured on **Windows 11 with Time-Critical priority on physical P-Cores**.
> * **WSL2**: While WSL2 is available on the test machine, it runs inside a virtualized Hyper-V container where virtual CPU affinity does not translate to physical core isolation. WSL2 results are therefore excluded from the primary headline tables to prevent misleading figures.
> * **Bare-Metal Linux**: Low-latency production trading systems operate on bare-metal Linux with `isolcpus`, `nohz_full`, and the `performance` CPU frequency governor. Bare-metal Linux benchmarking remains the reference deployment target.

### 2. Invariant TSC Calibration & Timing Methodology
* Modern x86-64 processors feature an Invariant TSC (`invtsc`), which increments at a fixed nominal reference frequency independent of core P-state or turbo frequency.
* In our benchmarks, the exact invariant TSC frequency is calibrated empirically at startup against `std::chrono::steady_clock` over a $10^8$ cycle sampling interval:
  $$\text{Calibrated Frequency} = 2.304011\text{ GHz} \implies 0.434026\text{ ns per TSC tick}$$
* All reported nanosecond values are computed as $\text{ticks} \times 0.434026\text{ ns}$.
* **One-Way Latency Note**: The preliminary Table 1 latency measurements report one-way latency approximated as $\text{RTT} / 2$ across 500,000 round-trip ping-pong exchanges. An upgraded benchmark measuring true one-way latency via producer-serialized TSC timestamps (`lfence; rdtsc` to `rdtscp`) under a paced non-backlogged queue across 10-run distributions is currently in progress.

### 3. Log-Linear (HDR-Style) Latency Histogram Bucketing
* The latency histogram uses 64 octaves with 16 linear sub-buckets per octave ($O(1)$ bucketing via `__builtin_clzll`), providing constant relative precision across dynamic ranges from 1 tick to $>10^6$ ticks.
* **Octave Resolution Math**:
  * In octave $[64, 128)$, each sub-bucket spans $\frac{128 - 64}{16} = 4\text{ ticks}$. A reading of **96 ticks** represents $64 + 8 \times 4$ (resolution $\pm 4\text{ ticks}$).
  * In octave $[128, 256)$, each sub-bucket spans $\frac{256 - 128}{16} = 8\text{ ticks}$. A reading of **184 ticks** represents $128 + 7 \times 8$ (resolution $\pm 8\text{ ticks}$).

### 4. Operation Counting & Payload Accounting
* **What is an "op"?** In all benchmarks, each op represents a complete transmission of a payload value (`sizeof(T) = 8` to `40` bytes) from producer to consumer.
* **Payload Verification**: In both latency and throughput benchmarks, the consumer actively reads and accumulates a 64-bit checksum of all payload bytes, ensuring the payload line is pulled into the consumer's L1 cache and cache-to-cache transfer costs are fully accounted for.

### 5. Verification & Correctness Disclosures
* **ThreadSanitizer (TSan)**: Executed in CI (`-fsanitize=thread`). TSan found **zero data races** across all evaluation interleavings.
* **AArch64 Cross-Compilation**: Verified in CI using `aarch64-linux-gnu-g++` (`-march=armv8-a`). While memory barriers and the `yield` hint compile and link cleanly, runtime evaluation on physical ARM64 hardware (e.g. AWS Graviton, Apple Silicon) is required to observe weak-memory hardware reorderings in practice.

---

## File Structure

```
mpmc-ring-buffer/
├── CMakeLists.txt              # Build configuration & FetchContent for baselines
├── README.md                   # Technical documentation & benchmarks
├── .github/
│   └── workflows/
│       └── ci.yml              # CI: GCC, Clang, ThreadSanitizer (TSan), ARM64 cross-compile
├── include/
│   └── mpmc/
│       ├── ring_buffer.hpp     # Dmitry Vyukov lock-free core + CRTP policies
│       ├── pool.hpp            # Lock-free slab object pool (zero dynamic allocation)
│       ├── slot.hpp            # 64-byte isolated slot layout (by-value inline & pointer)
│       ├── cache.hpp           # Hardware intrinsics (_mm_pause, yield, prefetch, RDTSC)
│       ├── backoff.hpp         # Pure user-space spin-wait backoff
│       ├── affinity.hpp        # Core pinning & real-time thread priority helpers
│       ├── stats.hpp           # Thread-isolated CAS collision metrics
│       └── histogram.hpp       # O(1) lock-free log-linear latency histogram
├── benchmarks/
│   ├── bench_pinned.cpp        # Physical core-pinned RTT & throughput benchmark
│   └── bench_comparison.cpp    # Baseline comparison (Rigtorp MPMC/SPSC, Moodycamel, Mutex)
└── tests/
    ├── CMakeLists.txt          # Test executable definitions & TSan target
    ├── test_unit.cpp           # 14-phase comprehensive verification suite
    └── test_stress.cpp         # 62,500-lap wrap-around & asymmetric chaos suite
```

---

## Building and Running

### Prerequisites
* C++20 compliant compiler: **GCC $\ge$ 11**, **Clang $\ge$ 13**, or **MSVC 2022**.
* CMake 3.20 or newer.

### Build Instructions
```bash
# Clone the repository
git clone https://github.com/Janay05/Lock-Free-MPMC-Ring-Buffer.git
cd Lock-Free-MPMC-Ring-Buffer

# Configure and compile with native host optimizations
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

### Running Tests and Benchmarks
```bash
# Run the 14-test verification suite
./build/tests/unit_tests

# Run the multi-threaded chaos & wrap-around stress tests
./build/tests/stress_tests

# Run the core-pinned physical latency benchmark
./build/bench_pinned

# Run the comparative baseline benchmark suite
./build/bench_comparison
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

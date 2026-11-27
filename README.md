# Bounded MPMC Ring Buffer

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A header-only C++20 implementation and performance study of Dmitry Vyukov's bounded multi-producer multi-consumer queue, with inline payload storage (up to 56 bytes), CRTP full/empty policies and batch operations that claim many slots with one CAS.

**Progress guarantee:** the queue uses no locks but is not strictly lock-free. A thread preempted between claiming a slot and publishing it stalls the threads that need that slot. See [DESIGN.md](DESIGN.md).

## Results

Intel Core i7-14700HX, Windows 11 (Balanced power plan, AC power), GCC 13.1, `-O3 -march=native`. Each thread is pinned to a different physical P-core at time-critical priority. Raw output of two full sessions is in [`benchmarks/results/`](benchmarks/results/).

### One-way latency, one producer to one consumer

Queue kept near empty; 10 runs of 50,000 samples. Median of the per-run percentiles in ns, shown as session 1 / session 2. Figures include about 16 ns of timer overhead.

| Queue | p50 | p99 | p99.9 |
|---|---|---|---|
| **this queue** | 118 / 118 | 138 / 138 | 597 / 145 |
| rigtorp::MPMCQueue | 105 / 131 | 145 / 180 | 215 / 229 |
| rigtorp::SPSCQueue | 114 / 118 | 173 / 145 | 180 / 166 |
| moodycamel::ConcurrentQueue | 277 / 263 | 583 / 527 | 666 / 625 |
| std::mutex + std::deque (polled) | 694 / 694 | 2555 / 861 | 5666 / 3500 |

This queue, rigtorp MPMC and rigtorp SPSC are not clearly separable here: their differences are about as large as the variation between runs. Every queue also saw worst-case outliers of 0.5-0.8 ms on this non-isolated system.

### Throughput

Millions of items per second, median of 11 runs, session 1 / session 2. 8-byte payload.

| Queue | 1P:1C | 2P:2C | 4P:4C | 1P:1C, batch 32 |
|---|---|---|---|---|
| **this queue** | 112 / 26 | 9.9 / 9.3 | 5.8 / 5.9 | 262 / 279 |
| rigtorp::MPMCQueue | 60 / 61 | 9.4 / 8.4 | 8.8 / 10.0 | 58 / 57 (no batch API) |
| rigtorp::SPSCQueue | 231 / 69 | n/a | n/a | 55 / 57 (no batch API) |
| moodycamel::ConcurrentQueue | 41 / 40 | 24.5 / 26.2 | 23.0 / 22.1 | 204 / 190 |
| std::mutex + std::deque | 4.7 / 5.7 | 15.8 / 16.7 | 15.0 / 15.0 | 69 / 66 |

* **Under contention this queue is slow.** At 4P:4C it trails rigtorp and is roughly 4x behind moodycamel. At 2P:2C and 4P:4C it is also behind a mutex.
* **Contention on the shared indices is part of the cause.** All producers contend on one `head_` and all consumers on one `tail_`, and 65-71% of CAS attempts fail at 4P:4C. Backoff recovers only part of the gap (below).
* **1P:1C is unstable.** Runs of this queue range from 23 to 119 M/s, so no single figure is representative. The cause is not yet established.
* **Batching helps.** One CAS per 32 items gives about 260-280 M/s, somewhat above moodycamel's bulk API. The rigtorp batch figures are loops of single-item calls.

### Exponential backoff after a failed CAS

With about two thirds of CAS attempts failing at 4P:4C, the obvious fix is to back off exponentially. The last template parameter caps the number of pauses between retries (default 1). Throughput at 4P:4C in M items/s, median of 11 interleaved runs, session 1 / session 2:

| Max pauses | 1 | 4 | 16 | 64 | 256 | 1024 |
|---|---|---|---|---|---|---|
| 4P:4C | 6.0 / 7.1 | 6.7 / 7.9 | 7.1 / 8.4 | 7.4 / 8.6 | 7.5 / 8.7 | 7.6 / 8.7 |

A cap of 64 gains about 20% at 4P:4C, with no clear change at 1P:1C or 2P:2C. That is far short of closing the gap to moodycamel, so retry pressure explains only part of the cost. The rest is still unexplained. Removing the next-slot prefetch was also tried and gave no consistent gain. The effect of backoff on tail latency and fairness has not been measured.

```cpp
mpmc::RingBuffer<uint64_t, 4096, 64, 64, /*MaxBackoff=*/64> queue;
```

Reproduce with `./build/bench_backoff`.

### Ablations (this queue, p50 one-way, session 1 / session 2)

| Variant | p50 ns |
|---|---|
| Consumer busy-polls | 118 / 111 |
| Consumer executes `pause` while polling | 94 / 76 |
| 64-byte slots, 64-byte index separation | 111 / 111 |
| 64-byte slots, 128-byte index separation | 111 / 111 |
| 128-byte slots, 128-byte index separation | 83 / 80 |

* Polling with `pause` had a lower median than pure busy-polling in both sessions, though individual runs overlap.
* Separating `head_` and `tail_` by 128 bytes made no measurable difference to the median.
* 128-byte slots lowered the median by about 30 ns. Adjacent-line prefetching between neighbouring slots is a possible explanation, not yet verified with hardware counters. The default remains 64 bytes.

Measured primitive costs (p50, including timer overhead): `pause` 59 ns, `std::this_thread::yield()` 73-94 ns.

### Limitations

* Windows only, with no core isolation. There are no bare-metal Linux measurements yet.
* One machine. AArch64 is cross-compiled in CI but has not been run on ARM hardware.
* Latency is measured with paced traffic; it says nothing about latency under load.
* The latency histogram has coarse (6-12%) buckets and reports each bucket's lower bound.
* In the rigtorp SPSC latency pass the end timestamp is taken before the payload is copied out, which slightly favours it.

---

## Architectural Principles

```
  Producer Core 0  ───┐                                ┌───  Consumer Core 4
  Producer Core 1  ───┼──> [  Bounded MPMC Ring  ] ───┼──>  Consumer Core 5
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

### 3. Spin-Wait Hints
Waiting loops stay in user space and issue `_mm_pause` on x86-64, `yield` on AArch64 and `nop` on RISC-V.

### 4. Static Polymorphism (CRTP) Over Virtual Functions
Zero virtual functions, zero vtable pointers, and zero runtime branch indirections. Queue overflow/underflow policies are resolved at compile time:
* `RingBuffer<T, Cap>`: **Drop policy** (non-blocking, returns `false` immediately on full/empty).
* `SpinningRingBuffer<T, Cap>`: **Spin-wait policy** (spins with `cpu_relax()` backoff until space/item appears).
* *(Note: An overwrite-oldest policy was evaluated and intentionally removed because evicting the queue tail from concurrent producers introduces an unresolvable multi-producer race condition against concurrent consumers).*

### 5. Slab Object Pool Architecture (`pool.hpp`)
When transferring large payloads ($>56$ bytes), heap allocations (`new Order()`) in trading loops introduce allocator lock contention. `ObjectPool<T, Capacity>` pre-allocates contiguous memory at initialization and manages available slots through an internal ring buffer of pointers (`RingBuffer<T*, Capacity>`), achieving zero allocations on the hot path.

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
MassivePayload* obj = pool.acquire(); // Non-locking pop from pool slab
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

## Verification

* Multi-threaded tests check that items arrive exactly once. The checks stay active in Release builds (`tests/check.hpp`).
* On Linux, CMake also builds both test suites with ThreadSanitizer. TSan only sees the interleavings that occur, so a clean run is not a proof of correctness.
* On some recent Linux kernels TSan aborts with `unexpected memory mapping`; run it as `setarch -R ctest`.

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
│       ├── ring_buffer.hpp     # Vyukov core, batch operations, CRTP policies
│       ├── pool.hpp            # Slab object pool (zero dynamic allocation)
│       ├── slot.hpp            # 64-byte isolated slot layout (by-value inline & pointer)
│       ├── cache.hpp           # Hardware intrinsics (_mm_pause, yield, prefetch, RDTSC)
│       ├── backoff.hpp         # Pure user-space spin-wait backoff
│       ├── affinity.hpp        # Core pinning & real-time thread priority helpers
│       ├── stats.hpp           # Thread-isolated CAS collision metrics
│       └── histogram.hpp       # O(1) atomic log-linear latency histogram
├── benchmarks/
│   ├── bench_pinned.cpp        # Latency, ablations, contention, batching
│   ├── bench_comparison.cpp    # Baseline comparison (rigtorp, moodycamel, mutex)
│   ├── bench_backoff.cpp       # CAS backoff sweep
│   ├── bench_util.hpp          # TSC calibration, core pinning, repeated runs
│   └── results/                # Raw output behind the tables above
└── tests/
    ├── CMakeLists.txt          # Test executable definitions & TSan targets
    ├── check.hpp               # Assertion that survives -DNDEBUG
    ├── test_unit.cpp           # Unit and concurrency tests
    └── test_stress.cpp         # Wrap-around and timing-jitter stress tests
```

---

## Building and Running

### Prerequisites
* A C++20 compiler: GCC or Clang (CI uses GCC 12 and Clang 15). MSVC is not supported.
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
# Run the unit tests
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

# Lock-Free MPMC Ring Buffer

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows%20(x86--64%20%7C%20AArch64)-lightgrey.svg)]()

A header-only, zero-syscall, lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer queue engineered in **C++20** for ultra-low latency execution environments and high-frequency trading (HFT) data feeds.

Built from first principles around **cache-line isolation**, **bounded atomic sequence counters**, **zero dynamic allocation on the hot path**, and **CRTP static polymorphism**.

---

## Performance Highlights

Measured on physical P-Cores pinned with high priority (**Intel® Core™ i7-14700HX, Raptor Lake**):

| Metric | Measurement | Description |
|---|---|---|
| **Single-Item Cross-Core Latency (p50)** | **~21 ns** (96 TSC cycles) | Hardware cross-core cache transfer RTT / 2 over CPU ring interconnect |
| **Single-Item Cross-Core Latency (p99)** | **~40 ns** (184 TSC cycles) | Tail latency under continuous single-item transmission |
| **Single-Item SPSC Throughput** | **~64.35 Million ops/sec** | ~15.5 ns per unbatched item transmission across cores |
| **Batched Bulk Throughput (Batch = 32)** | **541.14 Million ops/sec** | ~1.84 ns per item when amortizing atomic CAS across batches |
| **Contention Storm Reduction** | **64.4% $\to$ 1.15%** | Pause backoff + bulk CAS dramatically suppresses bus locks |
| **Zero Memory Allocation** | **0 bytes** | Zero calls to `malloc`/`new` on hot enqueue/dequeue paths |

> *Note: For full measurement details, invariant TSC derivations, and data accounting disclosures, see [Benchmark Environment & Data Accounting](#benchmark-environment--data-accounting).*

---

## Industry Baseline Comparison

To evaluate performance against industry standards, we benchmarked mpmc::RingBuffer under identical hardware, compiler flags (-O3 -march=native), and core-pinning conditions against:
1. **
igtorp::MPMCQueue** (Erik Rigtorp\'s industry reference bounded Vyukov MPMC queue).
2. **moodycamel::ConcurrentQueue** (Cameron Desrochers\' lock-free unbounded queue with thread-local tokens).
3. **std::mutex + std::deque** (Standard thread-safe mutex and condition-variable baseline).

### Comparative Benchmark Results

Executed on physical P-Cores pinned with THREAD_PRIORITY_TIME_CRITICAL (**Intel® Core™ i7-14700HX, Raptor Lake**):

| Queue Implementation | Architecture Pattern | SPSC RTT (p50) | SPSC RTT (p99) | SPSC Unbatched (1P:1C) | MPMC Contention (4P:4C) | Bulk Throughput (Batch=32) |
|---|---|---|---|---|---|---|
| **mpmc::RingBuffer (Ours)** | Bounded Ticket Array + CRTP + Pause | **144 cyc (~31 ns)** | **224 cyc (~48 ns)** | **22.42 M ops/s** | 12.62 M ops/s | **589.2 M ops/s** |
| **
igtorp::MPMCQueue** | Bounded Ticket Array | 176 cyc (~38 ns) | 200 cyc (~43 ns) | 19.77 M ops/s | 16.62 M ops/s | 75.29 M ops/s* |
| **moodycamel::ConcurrentQueue** | Unbounded Block Token Queue | 152 cyc (~33 ns) | 320 cyc (~69 ns) | 20.09 M ops/s | **19.46 M ops/s** | **617.7 M ops/s** |
| **std::mutex + std::deque** | Mutex + Condvar + Deque | 1216 cyc (~264 ns) | 4864 cyc (~1057 ns) | 3.26 M ops/s | 11.49 M ops/s | 105.9 M ops/s |

*\*Note: 
igtorp::MPMCQueue does not expose a native bulk CAS API; bulk throughput was measured using a batched single-item loop.*

### Architectural Analysis & Trade-offs

1. **Lowest Cross-Core Round-Trip Latency (SPSC)**:
   * mpmc::RingBuffer delivers the lowest median round-trip time (**144 cycles**, 18% faster than Rigtorp, 5% faster than Moodycamel, and 8.4× faster than a mutex). Our cache-line prefetching (prefetch_read / prefetch_write) and pure user-space cpu_relax() backoff minimize latency on single-item handover.
2. **Bulk Amortization (	ry_enqueue_bulk / 	ry_dequeue_bulk)**:
   * Amortizing the atomic CAS over 32 slots achieves **589.2 Million ops/sec** (~1.7 ns/item) — a **7.8× speedup over Rigtorp\'s batched loop** (75.3M ops/s), approaching Moodycamel\'s specialized multi-item block design (617.7M ops/s).
3. **Contended Multi-Producer Multi-Consumer (4P:4C)**:
   * Under heavy multi-producer contention, moodycamel::ConcurrentQueue achieves higher throughput (19.46M ops/s) because its architecture routes each producer to a dedicated thread-local block sub-queue, avoiding contention on a shared circular ring head.
   * mpmc::RingBuffer achieves 12.62M ops/s under symmetric 4P:4C contention, maintaining sub-microsecond item transfer while avoiding the dynamic memory allocations and higher tail latency ($ of 320 cycles) inherent to block-chaining architectures.

---

## Architectural Principles

```
  Producer Core 0  ───┐                                ┌───  Consumer Core 4
  Producer Core 1  ───┼──> [ Lock-Free MPMC Ring ] ───┼──>  Consumer Core 5
  Producer Core 2  ───┘       (Power-of-2 Sized)      └───  Consumer Core 6
```

### 1. 64-Byte Cache Line Isolation (False Sharing Mitigation)
On modern x86-64 and ARM64 architectures, the L1 data cache operates in 64-byte lines. If multiple cores write to distinct variables located on the same 64-byte line, hardware coherency protocols (MESI/MOESI) invalidate the line back and forth across cores (cache ping-pong).
* **Each Slot is isolated to 64 bytes**:
  ```
  ┌─────────────────────────────────────────────────────────────┐
  │  Byte 0–7   │  sequence  │  atomic<size_t>   (8 bytes)      │
  │  Byte 8–15  │  data      │  T*               (8 bytes)      │
  │  Byte 16–63 │  padding   │  char[48]         (48 bytes)     │
  └─────────────────────────────────────────────────────────────┘
                Total: 8 + 8 + 48 = 64 bytes = 1 Cache Line
  ```
  Adjacent slots never share a cache line, eliminating slot-to-slot false sharing.
* **Independent `head_` and `tail_`**: Producers contend on `head_` and consumers contend on `tail_`. They reside on distinct 64-byte cache lines separated by trailing padding to prevent cross-producer/consumer false sharing.

### 2. Dmitry Vyukov Sequence Ticket Handshake
Instead of locking a global mutex or relying on a centralized counter, synchronization is governed by an atomic `sequence` ticket embedded in every slot:
* **Initial State**: Slot $i$ holds `sequence = i`.
* **Producer Handshake**: Producer checks if `slot->sequence == pos`. Upon atomic CAS claim of `head_`, it writes `slot->data` and executes a `release` store on `sequence = pos + 1`.
* **Consumer Handshake**: Consumer checks if `slot->sequence == pos + 1`. Upon atomic CAS claim of `tail_`, it reads `slot->data` and executes a `release` store on `sequence = pos + Capacity`.
* **Generational Wrapping**: Incrementing by `pos + Capacity` (rather than `pos + 1`) acts as a generation sentinel, ensuring producers on wrap-around generation $N+1$ never mistake an unread generation $N$ slot as free. 64-bit tickets eliminate ABA in practice.

### 3. User-Space Spin-Wait & Hardware Pause Backoff
* Yielding via `sched_yield` or sleep primitives transitions the thread into the OS scheduler, introducing involuntary descheduling and latency spikes ranging from hundreds of nanoseconds to several microseconds under scheduler queue depth.
* The backoff engine operates strictly in user space via architecture-specific pause hints:
  * **x86-64**: `_mm_pause` (`rep nop`, opcode `F3 90`). As specified in Intel SDM Vol. 2B, `PAUSE` introduces a finite delay loop serialization, prevents speculative memory order mispredictions on pipeline exit from tight spin loops, and reduces core power consumption.
  * **AArch64**: `yield` instruction (ARM DDI 0487), providing a hardware spin hint to the CPU core.
  * **RISC-V**: Standardized `nop` fallback.

### 4. Static Polymorphism (CRTP) Over Virtual Functions
Zero virtual functions, zero vtable pointers, and zero runtime branch indirections. Queue overflow/underflow policies are resolved at compile time:
* `RingBuffer<T, Cap>`: **Drop policy** (non-blocking, returns `false` on full/empty).
* `SpinningRingBuffer<T, Cap>`: **Spin-wait policy** (spins with `cpu_relax()` until space/item appears).
* `OverwriteRingBuffer<T, Cap>`: **Overwrite-oldest policy** (advances tail to evict oldest unread item; specialized for loss-tolerant market data feeds).

### 5. Slab Object Pool Architecture (`pool.hpp`)
Transferring 8-byte pointers avoids copying large payloads (such as 64-byte market orders) through the ring buffer. However, heap allocations (`new Order()`) in trading loops introduce allocator lock contention. `ObjectPool<T, Capacity>` pre-allocates contiguous memory at initialization and manages available slots through an internal lock-free ring buffer, achieving zero allocations on the hot path.

---

## Code Examples

### 1. Basic Enqueue and Dequeue (Safe Memory Model)
```cpp
#include "mpmc/ring_buffer.hpp"
#include <iostream>

// Queue holds pointers to uint64_t items. Capacity must be a power of 2.
mpmc::RingBuffer<uint64_t, 1024> queue;

// Enqueued items should have stable storage (e.g. pool, static, or heap).
static uint64_t g_order_id = 987654321;

// Non-blocking try_enqueue (returns false immediately if queue is full)
if (queue.try_enqueue(&g_order_id)) {
    // Successfully published
}

// Non-blocking try_dequeue (returns false immediately if queue is empty)
uint64_t* received = nullptr;
if (queue.try_dequeue(received)) {
    std::cout << "Received order ID: " << *received << "\n";
}
```

### 2. Zero-Allocation Pipeline with ObjectPool
```cpp
#include "mpmc/ring_buffer.hpp"
#include "mpmc/pool.hpp"

struct Order {
    uint64_t id;
    double price;
    uint32_t qty;
};

// Pre-allocate 4096 orders in contiguous cache-aligned memory at startup
mpmc::ObjectPool<Order, 4096> pool;
mpmc::RingBuffer<Order, 4096> queue;

// Producer Thread (Zero Malloc)
Order* o = pool.acquire(); // Lock-free pop from pool
if (o) {
    o->id = 1001;
    o->price = 150.25;
    o->qty = 50;
    queue.enqueue(o);      // Guaranteed publish with spin-wait
}

// Consumer Thread (Zero Free)
Order* processed = queue.dequeue();
// ... process trade ...
pool.release(processed);   // Returned to pool free-list for immediate reuse
```

### 3. High-Throughput Bulk Operations
```cpp
// Claim and publish up to 32 slots in a SINGLE atomic CAS
uint64_t* batch[32];
// ... populate batch pointers ...
size_t enqueued = queue.try_enqueue_bulk(batch, 32);

// Pull up to 32 slots in a SINGLE atomic CAS
uint64_t* out_batch[32];
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
queue.try_enqueue(o, my_stats);

std::cout << "CAS Contention Ratio: " << my_stats.enqueue_contention_ratio() << "%\n";
```

---

## Benchmark Environment & Data Accounting

To ensure scientific honesty and reproducibility, this section documents the exact benchmarking environment, data definitions, and hardware behavior:

### 1. Hardware & Platform Details
* **CPU**: Intel® Core™ i7-14700HX (8 Performance Cores, 12 Efficient Cores, 28 Threads).
* **OS**: Windows 11 (MSYS2 / UCRT64 toolchain, GCC 13.1.0).
* **Compiler Flags**: `-O3 -march=native -Wall -Wextra -Wpedantic -fno-rtti`.
* **Thread Priority**: Threads are pinned to physical P-Cores using `SetThreadAffinityMask` with `THREAD_PRIORITY_TIME_CRITICAL`.

> **HFT Deployment Note**: While laptop benchmarking under Windows with high priority verifies lock-free algorithms and relative scaling, production low-latency trading systems operate on bare-metal Linux instances configured with `isolcpus`, `nohz_full`, CPU frequency governor pinned to `performance`, and C-states disabled to prevent P-state frequency governor and scheduler jitter.

### 2. Operation Counting & Payload Accounting
* **What is an "op"?** In throughput measurements, each op represents a completed transmission of one 8-byte pointer from a producer to a consumer.
* **Payload Access**: Benchmarks measure queue transport overhead. The consumer receives the pointer without dereferencing or dirtying cold payload bytes. In production, pairing with `ObjectPool` ensures payloads reside in cache during active pipelines.
* **Bulk vs. Single-Item**:
  * **Single-Item (1P:1C)**: Measures unbatched single CAS operations per item (~64.35M ops/sec, ~15.5 ns/op).
  * **Bulk (Batch = 32)**: Measures amortized CAS claims across 32 items simultaneously (541.14M ops/sec, ~1.84 ns/op).

### 3. Timing & Histogram Resolution
* **Invariant TSC**: On modern x86-64 hardware, the Time Stamp Counter (read via `RDTSC`/`RDTSCP`) is invariant (`invtsc`). It increments at a fixed nominal reference frequency regardless of CPU turbo states or instantaneous clock frequency.
* **Histogram Quantization**: The lock-free latency histogram buckets values logarithmically using 64 octaves with 16 linear sub-buckets per octave ($O(1)$ bucketing via `__builtin_clzll`). Reported percentiles return the **lower bound** of each sub-bucket. This is why percentiles align to discrete power-of-two multiples (e.g., 96 cycles = $64 + 2 \times 16$; 184 cycles = $128 + 3.5 \times 16$).

### 4. Pointer-Only Slots vs. Value In-line Slots
Each slot holds an 8-byte pointer (`T*`) and 48 bytes of padding to fill 64 bytes.
* **Trade-off Justification**: Storing payloads by value inside the ring buffer causes multi-cache-line slots for structs $>56$ bytes, leading to multi-line bus invalidations per operation. Our pointer design pairs with `ObjectPool` so that payloads are managed in pre-warmed slab memory. For payloads that are tiny ($\le 8$ bytes) or when payloads are cold, an in-line value queue avoids the second dereference miss.

---

## File Structure

```
mpmc-ring-buffer/
├── CMakeLists.txt              # Build configuration (-O3, -march=native, -fno-rtti)
├── README.md                   # Technical documentation & benchmarks
├── include/
│   └── mpmc/
│       ├── ring_buffer.hpp     # Dmitry Vyukov lock-free core + CRTP policies
│       ├── pool.hpp            # Lock-free slab object pool (zero dynamic allocation)
│       ├── slot.hpp            # 64-byte isolated slot layout & static assertions
│       ├── cache.hpp           # Hardware intrinsics (_mm_pause, yield, prefetch, RDTSC)
│       ├── backoff.hpp         # Pure user-space spin-wait backoff
│       ├── affinity.hpp        # Core pinning & real-time thread priority helpers
│       ├── stats.hpp           # Thread-isolated CAS collision metrics
│       └── histogram.hpp       # O(1) lock-free log2 latency histogram
├── benchmarks/
│   └── bench_pinned.cpp        # Physical core-pinned RTT & throughput benchmark
└── tests/
    ├── CMakeLists.txt          # Test executable definitions
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
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

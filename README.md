# Lock-Free MPMC Ring Buffer

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows%20(x86--64)-lightgrey.svg)]()

A header-only, zero-syscall, lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer queue engineered in **C++20** for ultra-low latency execution environments and high-frequency trading (HFT) data feeds.

Built from first principles around **cache-line isolation**, **bounded atomic sequence counters**, **zero dynamic allocation on the hot path**, and **CRTP static polymorphism**.

---

## Performance Highlights

Benchmarked natively on an **Intel® Core™ i7-14700HX (Raptor Lake, P-Cores pinned at Time-Critical Priority)**:

| Metric | Measurement | Description |
|---|---|---|
| **Bulk Throughput (Batch = 32)** | **541.14 Million ops/sec** | ~1.84 ns per item across pinned physical cores |
| **SPSC Cross-Core Latency (p50)** | **~21 ns** (96 CPU cycles) | Hardware cache-transfer RTT / 2 over CPU interconnect |
| **SPSC Cross-Core Latency (p99)** | **~40 ns** (184 CPU cycles) | Tail latency under continuous transmission |
| **Contention Storm Reduction** | **64.4% $\to$ 1.15%** | Amortizing CAS across batches virtually eliminates bus locks |
| **Zero Memory Allocation** | **0 bytes** | Zero calls to `malloc`/`new` on hot enqueue/dequeue paths |

---

## Architectural Principles

```
  Producer Core 0  ──┐                               ┌──  Consumer Core 4
  Producer Core 1  ──┼──> [ Lock-Free MPMC Ring ] ──┼──>  Consumer Core 5
  Producer Core 2  ──┘       (Power-of-2 Sized)      └──  Consumer Core 6
```

### 1. 64-Byte Cache Line Isolation (Zero False Sharing)
On modern x86-64 hardware, the L1 data cache operates in 64-byte lines. If multiple threads write to distinct variables located on the same 64-byte line, CPU hardware generates constant cache-coherency invalidations (the MESI protocol ping-pong effect).
* **Each Slot is exactly 64 bytes**:
  ```
  ┌─────────────────────────────────────────────────────────────┐
  │  Byte 0–7   │  sequence  │  atomic<size_t>   (8 bytes)      │
  │  Byte 8–15  │  data      │  T*               (8 bytes)      │
  │  Byte 16–63 │  padding   │  char[48]         (48 bytes)     │
  └─────────────────────────────────────────────────────────────┘
                Total: 8 + 8 + 48 = 64 bytes = 1 Cache Line
  ```
* **Independent `head_` and `tail_`**: Producers hammer `head_` and consumers hammer `tail_`. They reside on distinct 64-byte cache lines separated by trailing padding to ensure zero false sharing.

### 2. Dmitry Vyukov Sequence Handshake
Instead of locking a global index or contending directly on head/tail, synchronization is governed by an atomic `sequence` ticket embedded in every slot:
* **Initial State**: Slot $i$ holds `sequence = i`.
* **Producer Step**: Producer checks if `slot->sequence == pos`. On atomic CAS claim of `head_`, it writes `slot->data` and releases `sequence = pos + 1`.
* **Consumer Step**: Consumer checks if `slot->sequence == pos + 1`. On atomic CAS claim of `tail_`, it reads `slot->data` and releases `sequence = pos + Capacity`.
* **ABA Safety**: Monotonically increasing 64-bit tickets only wrap after $2^{64}$ operations, making ABA impossible in practice.

### 3. Zero Kernel Syscalls & Pure Hardware Pause
* Any invocation of `std::this_thread::yield()` or `sleep()` triggers a kernel context switch, costing **1,000 to 5,000+ nanoseconds**.
* The backoff engine operates strictly in user space via `_mm_pause` (`rep nop`, opcode `F3 90`), de-pipelining the CPU speculative execution unit and reducing core power without yielding the core time-slice.

### 4. Static Polymorphism (CRTP) Over Virtual Functions
Zero virtual functions and zero runtime vtables. All policy dispatching is resolved at compile time:
* `RingBuffer<T, Cap>`: Default **Drop policy** (non-blocking, returns `false` on full).
* `SpinningRingBuffer<T, Cap>`: **Spin-wait policy** (spins with `_mm_pause` until slot is freed).
* `OverwriteRingBuffer<T, Cap>`: **Overwrite policy** (force-evicts oldest item, ideal for top-of-book market feeds).

### 5. Lock-Free Slab Object Pool (`pool.hpp`)
Moving 8-byte pointers avoids copying payloads, but dynamic heap allocation (`new Order()`) in trading loops introduces allocator mutex contention. `ObjectPool<T, Capacity>` pre-allocates contiguous memory at initialization and manages free instances through an internal ring buffer with zero dynamic allocations on `acquire()` / `release()`.

---

## Code Examples

### 1. Basic Enqueue and Dequeue
```cpp
#include "mpmc/ring_buffer.hpp"

// Capacity must be a power of 2
mpmc::RingBuffer<uint64_t, 1024> queue;

uint64_t order_id = 987654321;

// Non-blocking try_enqueue (returns false if queue is full)
if (queue.try_enqueue(&order_id)) {
    // Successfully published
}

// Non-blocking try_dequeue (returns false if queue is empty)
uint64_t* received = nullptr;
if (queue.try_dequeue(received)) {
    // Process *received
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

// Producer (Zero Malloc)
Order* o = pool.acquire(); // Lock-free pop from pool
if (o) {
    o->id = 1001;
    o->price = 150.25;
    o->qty = 50;
    queue.enqueue(o);
}

// Consumer (Zero Free)
Order* processed = queue.dequeue();
// ... process trade ...
pool.release(processed); // Returned to pool for reuse
```

### 3. High-Throughput Bulk Operations
```cpp
// Claim up to 32 slots in a SINGLE atomic CAS
uint64_t* batch[32];
// ... populate batch ...
size_t enqueued = queue.try_enqueue_bulk(batch, 32);

// Pull up to 32 slots in a SINGLE atomic CAS
uint64_t* out_batch[32];
size_t dequeued = queue.try_dequeue_bulk(out_batch, 32);
```

### 4. Hardware Core Pinning & Contention Telemetry
```cpp
#include "mpmc/affinity.hpp"
#include "mpmc/stats.hpp"

// Pin current thread to physical Core 2 with real-time priority
mpmc::affinity::pin_current_thread_to_core(2);
mpmc::affinity::set_current_thread_priority_critical();

// Track thread-local CAS collision ratios without cross-thread false sharing
mpmc::QueueStats my_stats;
queue.try_enqueue(o, my_stats);

std::cout << "CAS Contention Ratio: " << my_stats.enqueue_contention_ratio() << "%\n";
```

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
│       ├── cache.hpp           # Hardware intrinsics (_mm_pause, prefetch, RDTSC)
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

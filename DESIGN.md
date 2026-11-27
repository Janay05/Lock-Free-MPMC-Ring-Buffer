# Architectural Design & Memory Model Specification

This document provides a formal breakdown of the architectural design decisions, atomic synchronization semantics, progress guarantees, and hardware trade-offs governing the bounded Multi-Producer Multi-Consumer (MPMC) ring buffer queue.

---

## 1. Architectural Overview & Slot Layout

The queue is a bounded circular array derived from Dmitry Vyukov's algorithm, extended to support inline payload storage and contiguous bulk atomic operations.

### Dual-Mode Slot Architecture

Storing an 8-byte pointer to a heap-allocated struct induces a second cache-line miss on the consumer core.

To eliminate this, `Slot<T>` provides **dual-mode storage** within a fixed 64-byte boundary:

```
┌─────────────────────────────────────────────────────────────┐
│  Byte 0–7   │  sequence  │  atomic<size_t>   (8 bytes)      │
│  Byte 8–N   │  value     │  T                (sizeof(T) B)  │
│  Byte N–63  │  padding   │  char[56 - sizeof(T)]            │
└─────────────────────────────────────────────────────────────┘
              Total: 8 + sizeof(T) + Pad = exactly 64 bytes
```

* **Inline By-Value Mode (`sizeof(T) <= 56`)**: Payloads (such as market orders, execution reports, or hardware tick timestamps) are embedded directly inside the 64-byte slot alongside the sequence ticket. Enqueue and dequeue operations touch a single cache line per item.
* **Pointer + Pool Mode (`sizeof(T) > 56`)**: When payloads exceed 56 bytes, storing them by value would force slots to span multiple cache lines, causing multiple MESI invalidations per transaction. In this regime, the queue transfers `T*` pointers backed by `ObjectPool<T>` contiguous slab memory.

---

## 2. Formal Memory Ordering & Synchronization Proof

The queue synchronizes concurrently executing threads without mutexes using a sequence ticket embedded in each slot.

### Complete Atomic Operation Matrix

| Location | Atomic Variable | Operation | `memory_order` | Synchronizes-With Pair | Justification / Failure if Weakened |
|---|---|---|---|---|---|
| `try_enqueue` | `head_` | `load` | `relaxed` | N/A | Heuristic starting index for CAS attempt. |
| `try_enqueue` | `head_` | `compare_exchange_weak` | `relaxed` | N/A | Ticket reservation only. Slot availability is governed separately by the slot's sequence ticket. |
| `try_enqueue` | `slot->sequence` | `load` | `acquire` | `try_dequeue` release store (`pos + Capacity`) | **Critical**: Synchronizes with previous generation consumer. Prevents producer from writing into `slot->value` before the consumer has finished reading it on the prior lap. |
| `try_enqueue` | `slot->sequence` | `store` | `release` | `try_dequeue` acquire load (`pos + 1`) | **Critical**: Establishes *happens-before* relationship for payload. Guarantees that the producer's write to `slot->value` is globally visible to the consumer core before the sequence ticket becomes `pos + 1`. |
| `try_dequeue` | `tail_` | `load` | `relaxed` | N/A | Heuristic starting index for CAS attempt. |
| `try_dequeue` | `tail_` | `compare_exchange_weak` | `relaxed` | N/A | Consumer ticket reservation. |
| `try_dequeue` | `slot->sequence` | `load` | `acquire` | `try_enqueue` release store (`pos + 1`) | **Critical**: Synchronizes with producer's release store. Guarantees the consumer does not read stale/uninitialized data from `slot->value` before producer finishes writing. |
| `try_dequeue` | `slot->sequence` | `store` | `release` | `try_enqueue` acquire load (`pos + Capacity`) | **Critical**: Guarantees consumer finishes copying `slot->value` out before releasing the slot for wrap-around reuse by generation $N+1$ producers. |
| `SpinningRingBuffer` | `head_` / `tail_` | `load` | `relaxed` | N/A | Refreshes the position hint after a full or empty result; the slot sequence provides the ordering. |

### Generational Monotonic Sequences & ABA Immunity

Instead of toggling a 1-bit full/empty flag, each slot's sequence ticket transitions through monotonic generational values:

$$\text{Initial: } \text{seq} = i$$
$$\text{After Enqueue: } \text{seq} = \text{pos} + 1$$
$$\text{After Dequeue: } \text{seq} = \text{pos} + \text{Capacity}$$

**Why `pos + Capacity`?**
The next producer to use this slot holds position `pos + Capacity` and waits for `sequence == pos + Capacity`. Storing that value hands the slot to exactly that lap. Sequences are 64-bit, so wrapping them would take centuries at a billion operations per second.

### Batch Operations

A batch operation scans forward from the current index, counting consecutive ready slots, then claims that run with one CAS. A scanned slot can change only after the index moves past it, so a successful CAS proves the whole run is still ready. No batch call waits for another thread; it returns a partial count instead. The acquire load of each slot during the scan provides the happens-before edge for that slot's payload.

---

## 3. Progress Guarantees & Preemption Semantics

A common misconception in concurrent programming is labeling all bounded array queues as "lock-free". 

### Formal Classification

1. **CAS Arbitration (`head_` / `tail_`)**: **Lock-free**.
   * If $N$ threads concurrently attempt `compare_exchange_weak`, at least one thread is guaranteed to succeed in a finite number of system steps.
2. **Slot Publication (`sequence` release store)**: **Blocking under preemption**.
   * If Thread $P$ succeeds in CAS-claiming index `pos`, but is preempted by the OS scheduler before executing `slot->sequence.store(pos + 1, std::memory_order_release)`, then:
     * Consumers waiting for index `pos` will spin indefinitely.
     * Subsequent producers on the next buffer generation ($pos + \text{Capacity}$) will spin indefinitely.
   * Because progress of other threads depends on the execution of a specific preempted thread, **the algorithm is not strictly lock-free under arbitrary scheduling**.

### Production Mitigation in Low-Latency Infrastructure

In high-frequency trading and ultra-low latency telemetry:
* Worker threads are pinned to dedicated physical CPU cores (`pthread_setaffinity_np` / `SetThreadAffinityMask`).
* Cores are isolated from the OS scheduler via Linux boot parameters (`isolcpus`, `nohz_full`, `rcu_nocbs`).
* Threads run under real-time FIFO scheduling policies (`SCHED_FIFO`) with hardware interrupts routed to dedicated housekeeping cores.

Under these conditions, involuntary kernel preemption on the hot path is practically eliminated, allowing Vyukov's bounded design to operate with near-zero latency jitter.

---

## 4. Micro-Architectural Trade-Offs

Measured values are in the [README](README.md#results); this section covers the reasoning.

### False sharing and adjacent-line prefetching

`head_`, `tail_` and each slot sit on separate 64-byte lines. Some Intel prefetchers fetch lines in 128-byte pairs, so template parameters allow 128-byte alignment. On the test machine, wider index separation had no measurable effect on the median, while 128-byte slots lowered median handoff latency at twice the memory cost. The mechanism has not been confirmed with hardware counters.

### Busy-polling and `pause`

`pause` reduces power use and pressure on an SMT sibling, at the possible cost of delaying the next poll. On the test machine the paced one-way median was lower with `pause` than without, with overlapping run ranges.

### Contention

Every producer performs a CAS on `head_` and every consumer on `tail_`, so those cache lines move between cores on each claim. moodycamel avoids a single shared producer index with per-producer sub-queues, giving up global FIFO order and allocating memory as it grows. Truncated exponential backoff (the `MaxBackoff` parameter) gained about 20% at 4P:4C, which leaves most of the gap unexplained. Next steps are profiling cache-line transfers with hardware counters, and trying a `fetch_add`-based claim (which changes the full/empty semantics).

---

## 5. Verification

1. **Exactly-once delivery:** multi-threaded tests sort what was received and compare it with what was sent.
2. **Wrap-around stress:** one million messages through a 16-slot ring (62,500 laps), plus a run with randomized timing jitter.
3. **ThreadSanitizer:** both suites on Linux. It checks only the interleavings that occur and is not a proof.
4. **Timing:** benchmarks measure the TSC period against `steady_clock` at startup.

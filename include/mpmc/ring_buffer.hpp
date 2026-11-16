#pragma once

#include "cache.hpp"
#include "slot.hpp"
#include "backoff.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <type_traits>

#if defined(_WIN32)
    #include <malloc.h>
#endif

namespace mpmc {

namespace detail {
    inline void* aligned_alloc_64(std::size_t bytes) noexcept {
#if defined(_WIN32)
        return _aligned_malloc(bytes, kCacheLineSize);
#else
        void* ptr = nullptr;
        if (posix_memalign(&ptr, kCacheLineSize, bytes) != 0) {
            return nullptr;
        }
        return ptr;
#endif
    }

    inline void aligned_free_64(void* ptr) noexcept {
#if defined(_WIN32)
        _aligned_free(ptr);
#else
        free(ptr);
#endif
    }
} // namespace detail

/**
 * Lock-Free Multi-Producer Multi-Consumer (MPMC) Ring Buffer.
 *
 * Key Latency & Throughput Optimizations:
 * 1. False Sharing Elimination:
 *    - Each Slot is 64 bytes (1 cache line).
 *    - head_ and tail_ are separated onto their own distinct cache lines.
 *    - Read-mostly control data is isolated from mutable atomics.
 * 2. Pure User-Space Lock-Free:
 *    - No kernel synchronization primitives (no mutex, no futex, no semaphores).
 *    - CAS loops with pure hardware _mm_pause hints, zero OS preemption or syscalls.
 * 3. Power-of-2 Bitmasking:
 *    - pos & mask_ replaces CPU integer division (idiv is ~40-80 cycles, and is 1 cycle).
 * 4. Hardware Memory Prefetching:
 *    - Prefetches next slot into L1 cache ahead of time to overlap memory latency.
 * 5. Minimal Cache-Coherency Invalidation:
 *    - CAS on sequence handshake rather than contending on a single global lock.
 */
template <typename T = void, std::size_t Capacity = 1024>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(Capacity >= 2, "Capacity must be at least 2");

public:
    using value_type = T*;

    explicit RingBuffer() {
        std::size_t alloc_size = sizeof(Slot<T>) * Capacity;
        slots_ = static_cast<Slot<T>*>(detail::aligned_alloc_64(alloc_size));
        if (!slots_) {
            throw std::bad_alloc();
        }

        // Initialize slots: sequence for slot i is set to i.
        // This ensures the producer for turn 0 sees seq == pos.
        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&slots_[i]) Slot<T>();
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }

        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    ~RingBuffer() noexcept {
        if (slots_) {
            for (std::size_t i = 0; i < Capacity; ++i) {
                slots_[i].~Slot<T>();
            }
            detail::aligned_free_64(slots_);
            slots_ = nullptr;
        }
    }

    // Non-copyable, non-movable to ensure cache-pinned addresses
    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    /**
     * Non-blocking attempt to enqueue an item.
     * Returns true if successfully enqueued, false if queue was full or contended.
     */
    [[nodiscard]] bool try_enqueue(T* item) noexcept {
        std::size_t pos = head_.load(std::memory_order_relaxed);

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (__builtin_expect(diff == 0, 1)) {
                // Slot is ready for our turn. Attempt to claim head.
                if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    // Prefetch the NEXT slot into L1 for writing
                    cache::prefetch_write(&slots_[(pos + 1) & kMask]);

                    slot->data = item;
                    // Publish: release ensures slot->data write is visible before sequence updates
                    slot->sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
                // CAS failed: pos was updated by compare_exchange_weak with current head value.
            } else if (diff < 0) {
                // Buffer is full (or pos was stale). Verify if head has moved.
                std::size_t current_head = head_.load(std::memory_order_relaxed);
                if (current_head != pos) {
                    pos = current_head;
                } else {
                    return false; // Queue is full
                }
            } else {
                // Another producer won this slot; re-read head
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

    /**
     * Pure spin-wait enqueue. Zero OS scheduler calls.
     * Spins strictly in user-space with hardware _mm_pause until a slot is claimed.
     */
    void enqueue(T* item) noexcept {
        SpinWait backoff;
        while (!try_enqueue(item)) {
            backoff.spin();
        }
    }

    /**
     * Bulk enqueue: Claim up to `max_count` slots in a single atomic CAS operation.
     * Amortizes the CAS instruction and cache invalidation over `to_claim` items.
     * Returns the actual number of items successfully enqueued.
     */
    [[nodiscard]] std::size_t try_enqueue_bulk(T* const* items, std::size_t max_count) noexcept {
        if (__builtin_expect(max_count == 0, 0)) {
            return 0;
        }

        std::size_t pos = head_.load(std::memory_order_relaxed);

        for (;;) {
            std::size_t current_tail = tail_.load(std::memory_order_relaxed);
            std::size_t occupied = pos - current_tail;
            if (occupied >= Capacity) {
                return 0; // Buffer is completely full
            }

            std::size_t available = Capacity - occupied;
            std::size_t to_claim = (max_count < available) ? max_count : available;
            if (to_claim == 0) {
                return 0;
            }

            if (head_.compare_exchange_weak(pos, pos + to_claim, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < to_claim; ++i) {
                    std::size_t slot_idx = (pos + i) & kMask;
                    Slot<T>* slot = &slots_[slot_idx];

                    SpinWait backoff;
                    while (slot->sequence.load(std::memory_order_acquire) != (pos + i)) {
                        backoff.spin();
                    }

                    slot->data = items[i];
                    slot->sequence.store(pos + i + 1, std::memory_order_release);
                }
                return to_claim;
            }
        }
    }

    /**
     * Non-blocking attempt to dequeue an item.
     * Returns true and sets out_item if successful, false if queue is empty.
     */
    [[nodiscard]] bool try_dequeue(T*& out_item) noexcept {
        std::size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (__builtin_expect(diff == 0, 1)) {
                // Slot has data ready for our turn. Attempt to claim tail.
                if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    // Prefetch the NEXT slot into L1 for reading
                    cache::prefetch_read(&slots_[(pos + 1) & kMask]);

                    out_item = slot->data;
                    // Release slot for next cycle (pos + Capacity)
                    slot->sequence.store(pos + kMask + 1, std::memory_order_release);
                    return true;
                }
                // CAS failed: pos was updated with current tail value.
            } else if (diff < 0) {
                // Buffer is empty (or pos was stale). Verify if tail has moved.
                std::size_t current_tail = tail_.load(std::memory_order_relaxed);
                if (current_tail != pos) {
                    pos = current_tail;
                } else {
                    return false; // Queue is empty
                }
            } else {
                // Another consumer won this slot; re-read tail
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    /**
     * Bulk dequeue: Claim up to `max_count` slots in a single atomic CAS operation.
     * Returns the actual number of items successfully dequeued.
     */
    [[nodiscard]] std::size_t try_dequeue_bulk(T** out_items, std::size_t max_count) noexcept {
        if (__builtin_expect(max_count == 0, 0)) {
            return 0;
        }

        std::size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            std::size_t current_head = head_.load(std::memory_order_relaxed);
            if (pos >= current_head) {
                return 0; // Buffer is empty
            }

            std::size_t available = current_head - pos;
            std::size_t to_claim = (max_count < available) ? max_count : available;
            if (to_claim == 0) {
                return 0;
            }

            if (tail_.compare_exchange_weak(pos, pos + to_claim, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < to_claim; ++i) {
                    std::size_t slot_idx = (pos + i) & kMask;
                    Slot<T>* slot = &slots_[slot_idx];

                    SpinWait backoff;
                    while (slot->sequence.load(std::memory_order_acquire) != (pos + i + 1)) {
                        backoff.spin();
                    }

                    out_items[i] = slot->data;
                    slot->sequence.store(pos + i + kMask + 1, std::memory_order_release);
                }
                return to_claim;
            }
        }
    }

    /**
     * Pure spin-wait dequeue. Zero OS scheduler calls.
     * Spins strictly in user-space with hardware _mm_pause until an item is available.
     */
    [[nodiscard]] T* dequeue() noexcept {
        T* item = nullptr;
        SpinWait backoff;
        while (!try_dequeue(item)) {
            backoff.spin();
        }
        return item;
    }

    /**
     * Approximate queue size for telemetry/monitoring.
     * Relaxed atomic reads - no barrier overhead.
     */
    [[nodiscard]] std::size_t size_approx() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        return (head >= tail) ? (head - tail) : 0;
    }

    [[nodiscard]] bool empty_approx() const noexcept {
        return size_approx() == 0;
    }

    [[nodiscard]] constexpr std::size_t capacity() const noexcept {
        return Capacity;
    }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    // Cache line 0: Pointer to dynamically allocated slots array. Read-mostly.
    alignas(kCacheLineSize) Slot<T>* slots_{nullptr};

    // Cache line 1: Producer head index. Heavily modified by producers.
    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};

    // Cache line 2: Consumer tail index. Heavily modified by consumers.
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};

    // Trailing padding to prevent adjacent allocations from sharing the tail_ cache line
    char _pad[kCacheLineSize - sizeof(std::atomic<std::size_t>)]{};
};

} // namespace mpmc

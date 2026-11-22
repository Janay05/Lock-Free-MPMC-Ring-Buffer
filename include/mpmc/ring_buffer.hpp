#pragma once

#include "cache.hpp"
#include "slot.hpp"
#include "backoff.hpp"
#include "stats.hpp"

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

// ─────────────────────────────────────────────────────────────────────────────
// Platform-portable aligned allocation helpers
// ─────────────────────────────────────────────────────────────────────────────
namespace detail {
    inline void* aligned_alloc_64(std::size_t bytes) noexcept {
#if defined(_WIN32)
        return _aligned_malloc(bytes, kCacheLineSize);
#else
        void* ptr = nullptr;
        if (posix_memalign(&ptr, kCacheLineSize, bytes) != 0) return nullptr;
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


// ─────────────────────────────────────────────────────────────────────────────
// RingBufferBase — CRTP base containing all shared CAS mechanics
//
// Derived classes implement two policy hooks via static dispatch (no vtable):
//
//   bool on_full(T* item)   — called when try_enqueue finds the queue full.
//                             Return true  → retry (Derived handled it).
//                             Return false → caller receives false immediately.
//
//   bool on_empty(T*& out)  — called when try_dequeue finds the queue empty.
//                             Return true  → retry (Derived handled it).
//                             Return false → caller receives false immediately.
//
// All Derived::on_full / on_empty calls are inlined at compile time.
// Zero virtual dispatch, zero vtable pointer in the object.
// ─────────────────────────────────────────────────────────────────────────────
template <typename Derived, typename T, std::size_t Capacity>
class RingBufferBase {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(Capacity >= 2,                    "Capacity must be at least 2");

public:
    using value_type = T*;

    explicit RingBufferBase() {
        slots_ = static_cast<Slot<T>*>(
            detail::aligned_alloc_64(sizeof(Slot<T>) * Capacity));
        if (!slots_) throw std::bad_alloc();

        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&slots_[i]) Slot<T>();
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    ~RingBufferBase() noexcept {
        if (slots_) {
            for (std::size_t i = 0; i < Capacity; ++i) slots_[i].~Slot<T>();
            detail::aligned_free_64(slots_);
            slots_ = nullptr;
        }
    }

    RingBufferBase(const RingBufferBase&)            = delete;
    RingBufferBase& operator=(const RingBufferBase&) = delete;
    RingBufferBase(RingBufferBase&&)                 = delete;
    RingBufferBase& operator=(RingBufferBase&&)      = delete;

    // ── Core enqueue ─────────────────────────────────────────────────────────
        /**
     * Non-blocking single-item enqueue.
     *
     * When the queue is full, calls Derived::on_full(item):
     *   - Returns false immediately (DropPolicy)
     *   - Spins with cpu_relax() (SpinningPolicy)
     *   - Advances tail to overwrite oldest item (OverwritePolicy)
     *
     * ── Memory ordering rationale ─────────────────────────────────────────────────
     *
     * pos = head_.load(relaxed)
     *   Only a hint; any stale value is safe because the subsequent CAS
     *   corrects pos on failure.  No ordering required at this read.
     *
     * seq = slot->sequence.load(acquire)
     *   ACQUIRE pairs with the RELEASE store from the last consumer that
     *   freed this slot: slot->sequence.store(pos + kMask + 1, release).
     *   We must observe all writes the consumer made to slot->data before
     *   we overwrite it.  Without acquire we could speculatively read a
     *   stale sequence value on weakly-ordered ISAs (AArch64, RISC-V).
     *
     * head_.compare_exchange_weak(pos, pos+1, relaxed, relaxed)
     *   The CAS itself does not need acquire/release because the ordering
     *   guarantee is provided by the slot->sequence handshake: the acquire
     *   load above and the release store below create a synchronizes-with
     *   edge between this producer and the consumer.  Relaxed CAS avoids a
     *   redundant fence.  On x86, LOCK CMPXCHG already implies a full
     *   barrier; on AArch64/RISC-V the acquire/release on sequence suffice.
     *   On CAS failure, pos is updated to the actual current head_.
     *
     * slot->sequence.store(pos + 1, release)
     *   RELEASE publishes slot->data to any consumer that acquire-loads
     *   seq == pos+1.  This is the synchronizes-with edge the consumer
     *   pairs with.  Without release, a consumer could observe the sequence
     *   update before slot->data is globally visible on AArch64/RISC-V.
     * ───────────────────────────────────────────────────────────────────────────
     */
    [[nodiscard]] bool try_enqueue(T* item) noexcept {
        std::size_t pos = head_.load(std::memory_order_relaxed); // hint only; CAS corrects stale reads

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
            // acquire: sync with consumer's release that reset this slot to write-ready state
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (__builtin_expect(diff == 0, 1)) {
                // Slot is write-ready (seq == pos). Race all producers via CAS on head_.
                // Relaxed CAS: ordering provided by acquire/release on slot->sequence.
                if (head_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_write(&slots_[(pos + 1) & kMask]); // RFO for next slot
                    slot->data = item;
                    // release: publish slot->data; consumer's acquire on seq == pos+1 sees it
                    slot->sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
                // CAS lost (pos updated); pause before retrying to reduce bus-lock pressure
                cache::cpu_relax();
            } else if (diff < 0) {
                // seq < pos: consumer has not reset this slot from the previous wrap-around.
                // Queue is full. Delegate to policy (drop / spin / overwrite).
                if (derived().on_full(item, pos)) {
                    pos = head_.load(std::memory_order_relaxed);
                    continue;
                }
                return false;
            } else {
                // diff > 0: another producer already claimed pos. Reload and retry.
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }



    /**
     * Instrumented non-blocking single-item enqueue.
     * Records CAS collisions and queue fullness into caller's thread-local QueueStats.
     */
    [[nodiscard]] bool try_enqueue(T* item, QueueStats& stats) noexcept {
        std::size_t pos = head_.load(std::memory_order_relaxed);

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (__builtin_expect(diff == 0, 1)) {
                if (head_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_write(&slots_[(pos + 1) & kMask]);
                    slot->data = item;
                    slot->sequence.store(pos + 1, std::memory_order_release);
                    ++stats.enqueue_success;
                    return true;
                }
                ++stats.enqueue_cas_fail;
                cache::cpu_relax();
            } else if (diff < 0) {
                if (derived().on_full(item, pos)) {
                    pos = head_.load(std::memory_order_relaxed);
                    continue;
                }
                ++stats.enqueue_full;
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

    // ── Core dequeue ─────────────────────────────────────────────────────────
    /**
     * Non-blocking single-item dequeue.
     *
     * When the queue is empty, calls Derived::on_empty(out):
     *   - Returns false immediately (DropPolicy / OverwritePolicy)
     *   - Spins with cpu_relax() (SpinningPolicy)
     *
     * ── Memory ordering rationale ─────────────────────────────────────────────────
     *
     * pos = tail_.load(relaxed)
     *   Hint only. The subsequent CAS corrects stale reads.
     *
     * seq = slot->sequence.load(acquire)
     *   ACQUIRE pairs with the producer's RELEASE store that published
     *   slot->data: slot->sequence.store(pos + 1, release).
     *   Without acquire, slot->data could be read before the producer's
     *   store is globally visible on AArch64/RISC-V.
     *
     * tail_.compare_exchange_weak(pos, pos+1, relaxed, relaxed)
     *   Relaxed: ordering provided by acquire/release on slot->sequence.
     *   On CAS failure, pos is updated with the current tail_.
     *
     * slot->sequence.store(pos + kMask + 1, release)
     *   RELEASE resets the slot to write-ready for the NEXT wrap-around.
     *   pos + kMask + 1 == pos + Capacity (since kMask == Capacity - 1).
     *   The next producer to claim this slot's position will acquire-load
     *   this value as seq == next_pos, confirming the slot is free.
     *   Why pos + Capacity (not pos + 1)? The slot passes through Capacity
     *   positions before returning to this index. The sentinel increments by
     *   Capacity each wrap to distinguish generation N from generation N+1.
     * ───────────────────────────────────────────────────────────────────────────
     */
    [[nodiscard]] bool try_dequeue(T*& out_item) noexcept {
        std::size_t pos = tail_.load(std::memory_order_relaxed); // hint only

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
            // acquire: sync with producer's release that published slot->data
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (__builtin_expect(diff == 0, 1)) {
                // Slot is read-ready (seq == pos+1). Race all consumers via CAS on tail_.
                // Relaxed CAS: ordering provided by acquire/release on slot->sequence.
                if (tail_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_read(&slots_[(pos + 1) & kMask]); // warm next slot
                    out_item = slot->data;
                    // release: reset slot to write-ready for the next wrap-around generation
                    // pos + kMask + 1 == pos + Capacity (kMask == Capacity - 1)
                    slot->sequence.store(pos + kMask + 1, std::memory_order_release);
                    return true;
                }
                // CAS lost; pause before retrying
                cache::cpu_relax();
            } else if (diff < 0) {
                // seq < pos+1: producer hasn't published this slot yet. Queue is empty.
                if (derived().on_empty(out_item, pos)) {
                    pos = tail_.load(std::memory_order_relaxed);
                    continue;
                }
                return false;
            } else {
                // diff > 0: another consumer already claimed pos. Reload and retry.
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    /**
     * Instrumented non-blocking single-item dequeue.
     * Records CAS collisions and queue emptiness into caller's thread-local QueueStats.
     */
    [[nodiscard]] bool try_dequeue(T*& out_item, QueueStats& stats) noexcept {
        std::size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (__builtin_expect(diff == 0, 1)) {
                if (tail_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_read(&slots_[(pos + 1) & kMask]);
                    out_item = slot->data;
                    slot->sequence.store(pos + kMask + 1, std::memory_order_release);
                    ++stats.dequeue_success;
                    return true;
                }
                ++stats.dequeue_cas_fail;
                cache::cpu_relax();
            } else if (diff < 0) {
                if (derived().on_empty(out_item, pos)) {
                    pos = tail_.load(std::memory_order_relaxed);
                    continue;
                }
                ++stats.dequeue_empty;
                return false;
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    // ── Bulk enqueue ─────────────────────────────────────────────────────────
    /**
     * Claim up to max_count slots with a single head_ CAS.
     * Amortises bus-lock cost across the batch.
     */
    [[nodiscard]] std::size_t try_enqueue_bulk(T* const* items,
                                                std::size_t max_count) noexcept {
        if (__builtin_expect(max_count == 0, 0)) return 0;

        std::size_t pos = head_.load(std::memory_order_relaxed);

        for (;;) {
            std::size_t current_tail = tail_.load(std::memory_order_relaxed);
            std::size_t occupied     = pos - current_tail;
            if (occupied >= Capacity) return 0;

            std::size_t available = Capacity - occupied;
            std::size_t to_claim  = (max_count < available) ? max_count : available;
            if (to_claim == 0) return 0;

            if (head_.compare_exchange_weak(pos, pos + to_claim,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < to_claim; ++i) {
                    Slot<T>* slot = &slots_[(pos + i) & kMask];
                    SpinWait bk;
                    while (slot->sequence.load(std::memory_order_acquire) != (pos + i))
                        bk.spin();
                    slot->data = items[i];
                    slot->sequence.store(pos + i + 1, std::memory_order_release);
                }
                return to_claim;
            }
            cache::cpu_relax();
        }
    }

    [[nodiscard]] std::size_t try_enqueue_bulk(T* const* items,
                                                std::size_t max_count,
                                                QueueStats& stats) noexcept {
        if (__builtin_expect(max_count == 0, 0)) return 0;

        std::size_t pos = head_.load(std::memory_order_relaxed);

        for (;;) {
            std::size_t current_tail = tail_.load(std::memory_order_relaxed);
            std::size_t occupied     = pos - current_tail;
            if (occupied >= Capacity) {
                ++stats.enqueue_full;
                return 0;
            }

            std::size_t available = Capacity - occupied;
            std::size_t to_claim  = (max_count < available) ? max_count : available;
            if (to_claim == 0) return 0;

            if (head_.compare_exchange_weak(pos, pos + to_claim,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < to_claim; ++i) {
                    Slot<T>* slot = &slots_[(pos + i) & kMask];
                    SpinWait bk;
                    while (slot->sequence.load(std::memory_order_acquire) != (pos + i))
                        bk.spin();
                    slot->data = items[i];
                    slot->sequence.store(pos + i + 1, std::memory_order_release);
                }
                stats.enqueue_success += to_claim;
                return to_claim;
            }
            ++stats.enqueue_cas_fail;
            cache::cpu_relax();
        }
    }

    // ── Bulk dequeue ─────────────────────────────────────────────────────────
    [[nodiscard]] std::size_t try_dequeue_bulk(T** out_items,
                                                std::size_t max_count) noexcept {
        if (__builtin_expect(max_count == 0, 0)) return 0;

        std::size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            std::size_t current_head = head_.load(std::memory_order_relaxed);
            if (pos >= current_head) return 0;

            std::size_t available = current_head - pos;
            std::size_t to_claim  = (max_count < available) ? max_count : available;
            if (to_claim == 0) return 0;

            if (tail_.compare_exchange_weak(pos, pos + to_claim,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < to_claim; ++i) {
                    Slot<T>* slot = &slots_[(pos + i) & kMask];
                    SpinWait bk;
                    while (slot->sequence.load(std::memory_order_acquire) != (pos + i + 1))
                        bk.spin();
                    out_items[i] = slot->data;
                    slot->sequence.store(pos + i + kMask + 1, std::memory_order_release);
                }
                return to_claim;
            }
            cache::cpu_relax();
        }
    }

    [[nodiscard]] std::size_t try_dequeue_bulk(T** out_items,
                                                std::size_t max_count,
                                                QueueStats& stats) noexcept {
        if (__builtin_expect(max_count == 0, 0)) return 0;

        std::size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            std::size_t current_head = head_.load(std::memory_order_relaxed);
            if (pos >= current_head) {
                ++stats.dequeue_empty;
                return 0;
            }

            std::size_t available = current_head - pos;
            std::size_t to_claim  = (max_count < available) ? max_count : available;
            if (to_claim == 0) return 0;

            if (tail_.compare_exchange_weak(pos, pos + to_claim,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < to_claim; ++i) {
                    Slot<T>* slot = &slots_[(pos + i) & kMask];
                    SpinWait bk;
                    while (slot->sequence.load(std::memory_order_acquire) != (pos + i + 1))
                        bk.spin();
                    out_items[i] = slot->data;
                    slot->sequence.store(pos + i + kMask + 1, std::memory_order_release);
                }
                stats.dequeue_success += to_claim;
                return to_claim;
            }
            ++stats.dequeue_cas_fail;
            cache::cpu_relax();
        }
    }

    // ── Convenience wrappers ─────────────────────────────────────────────────

    // Spin-wait enqueue — calls try_enqueue in a pure _mm_pause loop.
    // Zero OS scheduler involvement.
    void enqueue(T* item) noexcept {
        SpinWait bk;
        while (!try_enqueue(item)) bk.spin();
    }

    // Spin-wait dequeue — calls try_dequeue in a pure _mm_pause loop.
    [[nodiscard]] T* dequeue() noexcept {
        T* item = nullptr;
        SpinWait bk;
        while (!try_dequeue(item)) bk.spin();
        return item;
    }

    // ── Telemetry (relaxed — monitoring only) ────────────────────────────────
    [[nodiscard]] std::size_t size_approx() const noexcept {
        std::size_t h = head_.load(std::memory_order_relaxed);
        std::size_t t = tail_.load(std::memory_order_relaxed);
        return (h >= t) ? (h - t) : 0;
    }

    [[nodiscard]] bool empty_approx() const noexcept { return size_approx() == 0; }
    [[nodiscard]] constexpr std::size_t capacity() const noexcept { return Capacity; }

protected:
    // Expose head_/tail_ to OverwritePolicy which must advance tail on its own.
    std::atomic<std::size_t>& head_ref() noexcept { return head_; }
    std::atomic<std::size_t>& tail_ref() noexcept { return tail_; }
    Slot<T>* slots_ptr() noexcept { return slots_; }

    static constexpr std::size_t kMask = Capacity - 1;

private:
    // CRTP accessor — resolves to derived class at compile time, zero overhead.
    Derived& derived() noexcept { return *static_cast<Derived*>(this); }

    // ── Memory layout (3 isolated cache lines) ───────────────────────────────
    // CL 0: slots_ pointer — read-mostly after construction
    alignas(kCacheLineSize) Slot<T>* slots_{nullptr};

    // CL 1: head_ — written exclusively by producers
    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};

    // CL 2: tail_ — written exclusively by consumers
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};

    // Trailing pad: prevents the allocation after RingBuffer from sharing tail_'s cache line
    char _pad[kCacheLineSize - sizeof(std::atomic<std::size_t>)]{};
};


// ─────────────────────────────────────────────────────────────────────────────
// Policy 1 — RingBuffer (Drop Policy)
//
// Default HFT ring buffer.
// on_full  → returns false immediately (drop / signal backpressure to caller)
// on_empty → returns false immediately (caller decides whether to retry)
//
// Use when: producers must never block; dropped items are acceptable
// (e.g. market data feed where stale tick is useless anyway).
// ─────────────────────────────────────────────────────────────────────────────
template <typename T = void, std::size_t Capacity = 1024>
class RingBuffer : public RingBufferBase<RingBuffer<T, Capacity>, T, Capacity> {
    friend class RingBufferBase<RingBuffer<T, Capacity>, T, Capacity>;
public:
    using RingBufferBase<RingBuffer<T, Capacity>, T, Capacity>::RingBufferBase;

private:
    // Queue full: drop the item, return false to caller.
    [[nodiscard]] bool on_full(T*, std::size_t&) noexcept { return false; }
    // Queue empty: signal caller immediately.
    [[nodiscard]] bool on_empty(T*&, std::size_t&) noexcept { return false; }
};


// ─────────────────────────────────────────────────────────────────────────────
// Policy 2 — SpinningRingBuffer (Spin-Wait Policy)
//
// on_full  → spins with _mm_pause until a slot is free (no OS yield)
// on_empty → spins with _mm_pause until an item is available
//
// Use when: producers/consumers must never lose items and can tolerate
// busy-waiting (e.g. order book update pipeline on a dedicated core).
// ─────────────────────────────────────────────────────────────────────────────
template <typename T = void, std::size_t Capacity = 1024>
class SpinningRingBuffer
    : public RingBufferBase<SpinningRingBuffer<T, Capacity>, T, Capacity> {
    friend class RingBufferBase<SpinningRingBuffer<T, Capacity>, T, Capacity>;
public:
    using RingBufferBase<SpinningRingBuffer<T, Capacity>, T, Capacity>::RingBufferBase;

private:
    // Spin until head advances (slot freed by a consumer); signal base to retry.
    [[nodiscard]] bool on_full(T*, std::size_t& pos) noexcept {
        cache::cpu_relax();
        pos = this->head_ref().load(std::memory_order_relaxed);
        return true;  // tell base: retry the outer loop
    }

    // Spin until tail advances (item published by a producer); signal base to retry.
    [[nodiscard]] bool on_empty(T*&, std::size_t& pos) noexcept {
        cache::cpu_relax();
        pos = this->tail_ref().load(std::memory_order_relaxed);
        return true;
    }
};


// ─────────────────────────────────────────────────────────────────────────────
// Policy 3 — OverwriteRingBuffer (Overwrite-Oldest Policy)
//
// on_full  → forcibly advances tail by 1 (evicts oldest unconsumed item),
//            then tells the base to retry the enqueue.
// on_empty → returns false (nothing to read).
//
// Use when: newest data always wins, oldest data is expendable
// (e.g. real-time sensor feed, top-of-book price updates).
//
// WARNING: The evicted item pointer is simply lost — the caller is responsible
// for not leaking payload memory. Pair with ObjectPool::release() in a
// destructor/guard if necessary.
// ─────────────────────────────────────────────────────────────────────────────
template <typename T = void, std::size_t Capacity = 1024>
class OverwriteRingBuffer
    : public RingBufferBase<OverwriteRingBuffer<T, Capacity>, T, Capacity> {
    friend class RingBufferBase<OverwriteRingBuffer<T, Capacity>, T, Capacity>;
public:
    using RingBufferBase<OverwriteRingBuffer<T, Capacity>, T, Capacity>::RingBufferBase;

    // Public accessor: retrieve the pointer that was evicted during last overwrite.
    // Returns nullptr if no eviction has occurred or the evicted slot is not yet visible.
    [[nodiscard]] T* last_evicted() const noexcept {
        return evicted_.load(std::memory_order_acquire);
    }

private:
    // Force-advance tail by 1, exposing the slot for reuse.
    // The slot's sequence is reset so the next producer can overwrite it.
    [[nodiscard]] bool on_full(T*, std::size_t& pos) noexcept {
        std::size_t tail_pos = this->tail_ref().load(std::memory_order_relaxed);
        Slot<T>* evict_slot  = &this->slots_ptr()[tail_pos & this->kMask];

        // Store evicted pointer for caller inspection (optional pool.release()).
        evicted_.store(evict_slot->data, std::memory_order_relaxed);

        // Advance tail — same CAS pattern as try_dequeue but we discard the value.
        std::size_t expected_seq = tail_pos + 1;
        std::size_t actual_seq   = evict_slot->sequence.load(std::memory_order_acquire);

        if (actual_seq == expected_seq) {
            if (this->tail_ref().compare_exchange_strong(
                    tail_pos, tail_pos + 1,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                // Release the slot back to the write-ready state for head to claim.
                evict_slot->sequence.store(tail_pos + this->kMask + 1,
                                           std::memory_order_release);
            }
        }
        // Reload head to reflect the now-freed slot.
        pos = this->head_ref().load(std::memory_order_relaxed);
        return true; // tell base: retry enqueue
    }

    [[nodiscard]] bool on_empty(T*&, std::size_t&) noexcept { return false; }

    // Stores the pointer of the most recently evicted (overwritten) item.
    // Callers may inspect this to return it to an ObjectPool.
    alignas(kCacheLineSize) std::atomic<T*> evicted_{nullptr};
};

} // namespace mpmc

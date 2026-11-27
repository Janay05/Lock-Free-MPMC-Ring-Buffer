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
#include <utility>
#include <type_traits>

#if defined(_WIN32)
    #include <malloc.h>
#endif

namespace mpmc {

// ─────────────────────────────────────────────────────────────────────────────
// Platform-portable aligned allocation helpers
// ─────────────────────────────────────────────────────────────────────────────
namespace detail {
    inline void* aligned_alloc(std::size_t bytes, std::size_t alignment) noexcept {
#if defined(_WIN32)
        return _aligned_malloc(bytes, alignment);
#else
        void* ptr = nullptr;
        if (posix_memalign(&ptr, alignment, bytes) != 0) return nullptr;
        return ptr;
#endif
    }

    inline void aligned_free(void* ptr) noexcept {
#if defined(_WIN32)
        _aligned_free(ptr);
#else
        free(ptr);
#endif
    }

    inline void* aligned_alloc_64(std::size_t bytes) noexcept { return aligned_alloc(bytes, kCacheLineSize); }
    inline void aligned_free_64(void* ptr) noexcept { aligned_free(ptr); }
} // namespace detail


// ─────────────────────────────────────────────────────────────────────────────
// RingBufferBase — CRTP base containing Vyukov sequence CAS mechanics
//
// Derived classes implement two compile-time policy hooks via static dispatch:
//   bool on_full(size_t& pos)  — invoked when try_enqueue finds the buffer full
//   bool on_empty(size_t& pos) — invoked when try_dequeue finds the buffer empty
//
// SlotAlignment:  Alignment & size of each slot (64B standard, 128B for prefetcher ablation)
// IndexAlignment: Alignment & isolation of head_ and tail_ (64B standard, 128B for prefetcher ablation)
// MaxBackoff:     Cap on pauses between CAS retries (see CasBackoff)
// Zero virtual dispatch, zero vtable pointers.
// ─────────────────────────────────────────────────────────────────────────────
template <typename Derived, typename T, std::size_t Capacity,
          std::size_t SlotAlignment = kCacheLineSize,
          std::size_t IndexAlignment = kCacheLineSize,
          uint32_t MaxBackoff = 1>
class RingBufferBase {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(Capacity >= 2,                    "Capacity must be at least 2");

public:
    using value_type = T;
    using SlotType   = Slot<T, SlotAlignment>;

    explicit RingBufferBase() {
        slots_ = static_cast<SlotType*>(
            detail::aligned_alloc(sizeof(SlotType) * Capacity, SlotAlignment));
        if (!slots_) throw std::bad_alloc();

        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&slots_[i]) SlotType();
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    ~RingBufferBase() noexcept {
        if (slots_) {
            for (std::size_t i = 0; i < Capacity; ++i) slots_[i].~SlotType();
            detail::aligned_free(slots_);
            slots_ = nullptr;
        }
    }

    RingBufferBase(const RingBufferBase&)            = delete;
    RingBufferBase& operator=(const RingBufferBase&) = delete;
    RingBufferBase(RingBufferBase&&)                 = delete;
    RingBufferBase& operator=(RingBufferBase&&)      = delete;

    // ── Single-Item Enqueue ──────────────────────────────────────────────────
    template <typename U = T>
    [[nodiscard]] bool try_enqueue(U&& item) noexcept {
        std::size_t pos = head_.load(std::memory_order_relaxed); // Hint only; CAS corrects stale reads
        CasBackoff<MaxBackoff> backoff;

        for (;;) {
            SlotType* slot = &slots_[pos & kMask];
            // Synchronizes with consumer's release store below (seq == pos + Capacity)
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (__builtin_expect(diff == 0, 1)) {
                // Slot write-ready: race to claim head_ ticket
                if (head_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_write(&slots_[(pos + 1) & kMask]);
                    slot->data() = std::forward<U>(item);
                    // Synchronizes with consumer's acquire load (seq == pos + 1)
                    slot->sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
                backoff.pause();
            } else if (diff < 0) {
                // Buffer is full (seq < pos); consumer hasn't drained previous turn
                if (derived().on_full(pos)) {
                    continue;
                }
                return false;
            } else {
                // Another producer won this slot; reload head and retry
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

    template <typename U = T>
    [[nodiscard]] bool try_enqueue(U&& item, QueueStats& stats) noexcept {
        std::size_t pos = head_.load(std::memory_order_relaxed);
        CasBackoff<MaxBackoff> backoff;

        for (;;) {
            SlotType* slot = &slots_[pos & kMask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (__builtin_expect(diff == 0, 1)) {
                if (head_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_write(&slots_[(pos + 1) & kMask]);
                    slot->data() = std::forward<U>(item);
                    slot->sequence.store(pos + 1, std::memory_order_release);
                    ++stats.enqueue_success;
                    return true;
                }
                ++stats.enqueue_cas_fail;
                backoff.pause();
            } else if (diff < 0) {
                if (derived().on_full(pos)) {
                    continue;
                }
                ++stats.enqueue_full;
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

    // ── Single-Item Dequeue ──────────────────────────────────────────────────
    [[nodiscard]] bool try_dequeue(T& out_item) noexcept {
        std::size_t pos = tail_.load(std::memory_order_relaxed); // Hint only; CAS corrects stale reads
        CasBackoff<MaxBackoff> backoff;

        for (;;) {
            SlotType* slot = &slots_[pos & kMask];
            // Synchronizes with producer's release store above (seq == pos + 1)
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (__builtin_expect(diff == 0, 1)) {
                // Slot read-ready: race to claim tail_ ticket
                if (tail_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_read(&slots_[(pos + 1) & kMask]);
                    out_item = std::move(slot->data());
                    // Synchronizes with producer's acquire load for next wrap generation
                    slot->sequence.store(pos + kMask + 1, std::memory_order_release);
                    return true;
                }
                backoff.pause();
            } else if (diff < 0) {
                // Buffer is empty (seq < pos + 1); producer has not published this slot yet
                if (derived().on_empty(pos)) {
                    continue;
                }
                return false;
            } else {
                // Another consumer won this slot; reload tail and retry
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] bool try_dequeue(T& out_item, QueueStats& stats) noexcept {
        std::size_t pos = tail_.load(std::memory_order_relaxed);
        CasBackoff<MaxBackoff> backoff;

        for (;;) {
            SlotType* slot = &slots_[pos & kMask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff   = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (__builtin_expect(diff == 0, 1)) {
                if (tail_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    cache::prefetch_read(&slots_[(pos + 1) & kMask]);
                    out_item = std::move(slot->data());
                    slot->sequence.store(pos + kMask + 1, std::memory_order_release);
                    ++stats.dequeue_success;
                    return true;
                }
                ++stats.dequeue_cas_fail;
                backoff.pause();
            } else if (diff < 0) {
                if (derived().on_empty(pos)) {
                    continue;
                }
                ++stats.dequeue_empty;
                return false;
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    // ── Bulk Operations (one CAS per batch) ──
    // Each claims the longest run of already-ready slots (up to max_count) with a
    // single CAS. Neither waits on another thread: a slot held by a stalled peer
    // ends the run. Returns the number of items transferred, possibly 0.
    [[nodiscard]] std::size_t try_enqueue_bulk(const T* items, std::size_t max_count) noexcept {
        QueueStats unused;
        return try_enqueue_bulk(items, max_count, unused);
    }

    [[nodiscard]] std::size_t try_enqueue_bulk(const T* items, std::size_t max_count, QueueStats& stats) noexcept {
        if (__builtin_expect(max_count == 0, 0)) return 0;

        std::size_t pos = head_.load(std::memory_order_relaxed);
        CasBackoff<MaxBackoff> backoff;

        for (;;) {
            std::size_t count = 0;
            intptr_t diff     = 0;
            while (count < max_count) {
                // Acquire pairs with the consumer's release of the previous lap
                const std::size_t seq = slots_[(pos + count) & kMask].sequence.load(std::memory_order_acquire);
                diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + count);
                if (diff != 0) break;
                ++count;
            }

            if (count == 0) {
                if (diff < 0) {
                    ++stats.enqueue_full;
                    return 0;
                }
                pos = head_.load(std::memory_order_relaxed); // pos was stale
                continue;
            }

            // A scanned slot can only change after head_ passes it, so a successful
            // CAS proves every slot in [pos, pos + count) is still ours to use.
            if (head_.compare_exchange_weak(pos, pos + count,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < count; ++i) {
                    SlotType* slot = &slots_[(pos + i) & kMask];
                    slot->data() = items[i];
                    slot->sequence.store(pos + i + 1, std::memory_order_release);
                }
                stats.enqueue_success += count;
                return count;
            }
            ++stats.enqueue_cas_fail;
            backoff.pause();
        }
    }

    [[nodiscard]] std::size_t try_dequeue_bulk(T* out_items, std::size_t max_count) noexcept {
        QueueStats unused;
        return try_dequeue_bulk(out_items, max_count, unused);
    }

    [[nodiscard]] std::size_t try_dequeue_bulk(T* out_items, std::size_t max_count, QueueStats& stats) noexcept {
        if (__builtin_expect(max_count == 0, 0)) return 0;

        std::size_t pos = tail_.load(std::memory_order_relaxed);
        CasBackoff<MaxBackoff> backoff;

        for (;;) {
            std::size_t count = 0;
            intptr_t diff     = 0;
            while (count < max_count) {
                // Acquire pairs with the producer's release publication
                const std::size_t seq = slots_[(pos + count) & kMask].sequence.load(std::memory_order_acquire);
                diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + count + 1);
                if (diff != 0) break;
                ++count;
            }

            if (count == 0) {
                if (diff < 0) {
                    ++stats.dequeue_empty;
                    return 0;
                }
                pos = tail_.load(std::memory_order_relaxed); // pos was stale
                continue;
            }

            // A scanned slot can only change after tail_ passes it, so a successful
            // CAS proves every slot in [pos, pos + count) is still ours to use.
            if (tail_.compare_exchange_weak(pos, pos + count,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
                for (std::size_t i = 0; i < count; ++i) {
                    SlotType* slot = &slots_[(pos + i) & kMask];
                    out_items[i] = std::move(slot->data());
                    slot->sequence.store(pos + i + kMask + 1, std::memory_order_release);
                }
                stats.dequeue_success += count;
                return count;
            }
            ++stats.dequeue_cas_fail;
            backoff.pause();
        }
    }

    // ── Spin-Wait Blocking Wrappers ──────────────────────────────────────────
    template <typename U = T>
    void enqueue(U&& item) noexcept {
        SpinWait bk;
        while (!try_enqueue(std::forward<U>(item))) bk.spin();
    }

    [[nodiscard]] T dequeue() noexcept {
        T item{};
        SpinWait bk;
        while (!try_dequeue(item)) bk.spin();
        return item;
    }

    // ── Monitoring Telemetry ─────────────────────────────────────────────────
    [[nodiscard]] std::size_t size_approx() const noexcept {
        std::size_t h = head_.load(std::memory_order_relaxed);
        std::size_t t = tail_.load(std::memory_order_relaxed);
        return (h >= t) ? (h - t) : 0;
    }

    [[nodiscard]] bool empty_approx() const noexcept { return size_approx() == 0; }
    [[nodiscard]] constexpr std::size_t capacity() const noexcept { return Capacity; }

protected:
    std::atomic<std::size_t>& head_ref() noexcept { return head_; }
    std::atomic<std::size_t>& tail_ref() noexcept { return tail_; }
    SlotType* slots_ptr() noexcept { return slots_; }

    static constexpr std::size_t kMask = Capacity - 1;

private:
    Derived& derived() noexcept { return *static_cast<Derived*>(this); }

    // CL 0: slots_ pointer (read-mostly)
    alignas(IndexAlignment) SlotType* slots_{nullptr};

    // CL 1: head_ index (producers exclusive)
    alignas(IndexAlignment) std::atomic<std::size_t> head_{0};

    // CL 2: tail_ index (consumers exclusive)
    alignas(IndexAlignment) std::atomic<std::size_t> tail_{0};

    // Trailing padding to isolate tail_'s cache line
    char _pad[IndexAlignment - sizeof(std::atomic<std::size_t>)]{};
};


// ─────────────────────────────────────────────────────────────────────────────
// Policy 1: RingBuffer (Drop Policy)
//
// on_full  → returns false immediately (backpressure signal)
// on_empty → returns false immediately
// ─────────────────────────────────────────────────────────────────────────────
template <typename T = void*, std::size_t Capacity = 1024,
          std::size_t SlotAlignment = kCacheLineSize,
          std::size_t IndexAlignment = kCacheLineSize,
          uint32_t MaxBackoff = 1>
class RingBuffer : public RingBufferBase<RingBuffer<T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>, T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff> {
    friend class RingBufferBase<RingBuffer<T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>, T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>;
public:
    using RingBufferBase<RingBuffer<T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>, T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>::RingBufferBase;

private:
    [[nodiscard]] bool on_full(std::size_t&) noexcept { return false; }
    [[nodiscard]] bool on_empty(std::size_t&) noexcept { return false; }
};


// ─────────────────────────────────────────────────────────────────────────────
// Policy 2: SpinningRingBuffer (Spin-Wait Policy)
//
// on_full  → spins with cpu_relax() until space is available
// on_empty → spins with cpu_relax() until an item is available
// ─────────────────────────────────────────────────────────────────────────────
template <typename T = void*, std::size_t Capacity = 1024,
          std::size_t SlotAlignment = kCacheLineSize,
          std::size_t IndexAlignment = kCacheLineSize,
          uint32_t MaxBackoff = 1>
class SpinningRingBuffer
    : public RingBufferBase<SpinningRingBuffer<T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>, T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff> {
    friend class RingBufferBase<SpinningRingBuffer<T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>, T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>;
public:
    using RingBufferBase<SpinningRingBuffer<T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>, T, Capacity, SlotAlignment, IndexAlignment, MaxBackoff>::RingBufferBase;

private:
    [[nodiscard]] bool on_full(std::size_t& pos) noexcept {
        cache::cpu_relax();
        pos = this->head_ref().load(std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] bool on_empty(std::size_t& pos) noexcept {
        cache::cpu_relax();
        pos = this->tail_ref().load(std::memory_order_relaxed);
        return true;
    }
};

} // namespace mpmc

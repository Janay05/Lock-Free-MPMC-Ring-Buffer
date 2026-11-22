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
// Platform-portable aligned allocation helpers (64-byte cache line alignment)
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
// RingBufferBase — CRTP base containing lock-free Vyukov sequence CAS mechanics
//
// Derived classes implement two compile-time policy hooks via static dispatch:
//   bool on_full(size_t& pos)  — invoked when try_enqueue finds the buffer full
//   bool on_empty(size_t& pos) — invoked when try_dequeue finds the buffer empty
//
// Zero virtual dispatch, zero vtable pointers.
// ─────────────────────────────────────────────────────────────────────────────
template <typename Derived, typename T, std::size_t Capacity>
class RingBufferBase {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(Capacity >= 2,                    "Capacity must be at least 2");

public:
    using value_type = T;

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

    // ── Single-Item Enqueue ──────────────────────────────────────────────────
    template <typename U = T>
    [[nodiscard]] bool try_enqueue(U&& item) noexcept {
        std::size_t pos = head_.load(std::memory_order_relaxed); // Hint only; CAS corrects stale reads

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
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
                cache::cpu_relax(); // Backoff on CAS collision
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

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
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
                cache::cpu_relax();
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

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
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
                cache::cpu_relax(); // Backoff on CAS collision
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

        for (;;) {
            Slot<T>* slot = &slots_[pos & kMask];
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
                cache::cpu_relax();
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

    // ── Bulk Enqueue (Amortized CAS) ─────────────────────────────────────────
    [[nodiscard]] std::size_t try_enqueue_bulk(const T* items, std::size_t max_count) noexcept {
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
                    slot->data() = items[i];
                    slot->sequence.store(pos + i + 1, std::memory_order_release);
                }
                return to_claim;
            }
            cache::cpu_relax();
        }
    }

    [[nodiscard]] std::size_t try_enqueue_bulk(const T* items, std::size_t max_count, QueueStats& stats) noexcept {
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
                    slot->data() = items[i];
                    slot->sequence.store(pos + i + 1, std::memory_order_release);
                }
                stats.enqueue_success += to_claim;
                return to_claim;
            }
            ++stats.enqueue_cas_fail;
            cache::cpu_relax();
        }
    }

    // ── Bulk Dequeue (Amortized CAS) ─────────────────────────────────────────
    [[nodiscard]] std::size_t try_dequeue_bulk(T* out_items, std::size_t max_count) noexcept {
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
                    out_items[i] = std::move(slot->data());
                    slot->sequence.store(pos + i + kMask + 1, std::memory_order_release);
                }
                return to_claim;
            }
            cache::cpu_relax();
        }
    }

    [[nodiscard]] std::size_t try_dequeue_bulk(T* out_items, std::size_t max_count, QueueStats& stats) noexcept {
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
                    out_items[i] = std::move(slot->data());
                    slot->sequence.store(pos + i + kMask + 1, std::memory_order_release);
                }
                stats.dequeue_success += to_claim;
                return to_claim;
            }
            ++stats.dequeue_cas_fail;
            cache::cpu_relax();
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
    Slot<T>* slots_ptr() noexcept { return slots_; }

    static constexpr std::size_t kMask = Capacity - 1;

private:
    Derived& derived() noexcept { return *static_cast<Derived*>(this); }

    // CL 0: slots_ pointer (read-mostly)
    alignas(kCacheLineSize) Slot<T>* slots_{nullptr};

    // CL 1: head_ index (producers exclusive)
    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};

    // CL 2: tail_ index (consumers exclusive)
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};

    // Trailing padding to isolate tail_'s cache line
    char _pad[kCacheLineSize - sizeof(std::atomic<std::size_t>)]{};
};


// ─────────────────────────────────────────────────────────────────────────────
// Policy 1: RingBuffer (Drop Policy)
//
// on_full  → returns false immediately (backpressure signal)
// on_empty → returns false immediately
// ─────────────────────────────────────────────────────────────────────────────
template <typename T = void*, std::size_t Capacity = 1024>
class RingBuffer : public RingBufferBase<RingBuffer<T, Capacity>, T, Capacity> {
    friend class RingBufferBase<RingBuffer<T, Capacity>, T, Capacity>;
public:
    using RingBufferBase<RingBuffer<T, Capacity>, T, Capacity>::RingBufferBase;

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
template <typename T = void*, std::size_t Capacity = 1024>
class SpinningRingBuffer
    : public RingBufferBase<SpinningRingBuffer<T, Capacity>, T, Capacity> {
    friend class RingBufferBase<SpinningRingBuffer<T, Capacity>, T, Capacity>;
public:
    using RingBufferBase<SpinningRingBuffer<T, Capacity>, T, Capacity>::RingBufferBase;

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

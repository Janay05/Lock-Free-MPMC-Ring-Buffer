#pragma once

#include "cache.hpp"
#include "ring_buffer.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <type_traits>

namespace mpmc {

/**
 * Lock-Free Slab Object Pool
 *
 * Pre-allocates exactly `Capacity` objects of type `T` at construction time
 * and manages them via an internal MPMC ring buffer of free pointers.
 *
 * Hot-path operations:
 *   acquire() → one lock-free try_dequeue from free_list_
 *   release() → one lock-free try_enqueue  into free_list_
 *
 * Zero dynamic allocation after construction. Zero locks. Zero syscalls.
 *
 * Memory layout:
 *   storage_[]   : alignas(64) flat array of T — contiguous, cache-friendly
 *   free_list_   : RingBuffer<T, Capacity> holding T* to each slot
 *
 * Design constraints:
 *   - T must be default-constructible (pool pre-constructs all objects)
 *   - Capacity must be a power of 2 (enforced by RingBuffer)
 *   - Pool is non-copyable, non-movable (address-stable for pointer safety)
 *
 * Typical HFT usage:
 *   ObjectPool<Order, 4096> pool;    // at startup
 *   Order* o = pool.acquire();       // hot path: zero malloc
 *   // fill o, enqueue pointer into ring buffer
 *   pool.release(o);                 // hot path: zero free
 */
template <typename T, std::size_t Capacity>
class ObjectPool {
    static_assert(std::is_default_constructible_v<T>,
        "ObjectPool<T>: T must be default-constructible");
    static_assert((Capacity & (Capacity - 1)) == 0,
        "ObjectPool<T>: Capacity must be a power of 2");
    static_assert(Capacity >= 2,
        "ObjectPool<T>: Capacity must be at least 2");

public:
    ObjectPool() {
        // Allocate flat storage for all T objects, cache-line aligned.
        // Using aligned_alloc to guarantee kCacheLineSize boundary regardless of sizeof(T).
        storage_ = static_cast<T*>(detail::aligned_alloc_64(sizeof(T) * Capacity));
        if (!storage_) {
            throw std::bad_alloc();
        }

        // Placement-construct every object and push its address into the free list.
        // After construction, free_list_ is full — all slots are available.
        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&storage_[i]) T();         // default-construct in-place
            bool ok = free_list_.try_enqueue(&storage_[i]);
            // During construction the ring is empty so this must always succeed.
            // If it fails the pool is fatally misconfigured.
            if (!ok) {
                // Destruct already-constructed objects before throwing
                for (std::size_t j = 0; j <= i; ++j) {
                    storage_[j].~T();
                }
                detail::aligned_free_64(storage_);
                storage_ = nullptr;
                throw std::runtime_error("ObjectPool: free_list_ enqueue failed during init");
            }
        }
    }

    ~ObjectPool() noexcept {
        if (storage_) {
            for (std::size_t i = 0; i < Capacity; ++i) {
                storage_[i].~T();
            }
            detail::aligned_free_64(storage_);
            storage_ = nullptr;
        }
    }

    // Non-copyable, non-movable: pointers into storage_ must remain stable
    ObjectPool(const ObjectPool&)            = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;
    ObjectPool(ObjectPool&&)                 = delete;
    ObjectPool& operator=(ObjectPool&&)      = delete;

    /**
     * Acquire a pre-allocated object from the pool.
     *
     * Lock-free: internally calls try_dequeue on the free list ring buffer.
     * Returns nullptr if the pool is exhausted (all Capacity objects in use).
     *
     * Hot path cost: 1 atomic load (acquire) + 1 CAS + 1 atomic store (release)
     * — identical to a single ring buffer dequeue.
     */
    [[nodiscard]] T* acquire() noexcept {
        T* ptr = nullptr;
        [[maybe_unused]] bool ok = free_list_.try_dequeue(ptr);
        return ptr;   // nullptr if pool exhausted
    }

    /**
     * Release an object back to the pool.
     *
     * Lock-free: internally calls try_enqueue on the free list ring buffer.
     * The caller MUST NOT access `ptr` after calling release().
     *
     * Precondition: ptr must have been obtained from this pool's acquire().
     * Releasing a foreign pointer or releasing twice is undefined behaviour.
     *
     * Hot path cost: 1 atomic load (acquire) + 1 CAS + 1 atomic store (release)
     */
    void release(T* ptr) noexcept {
        // Enqueue back to the free list. Spins with _mm_pause until the slot is claimed.
        // Since at most Capacity items can be in use, the free list is guaranteed to have space.
        free_list_.enqueue(ptr);
    }

    /**
     * Approximate count of currently available (free) slots.
     * Relaxed reads — suitable for telemetry/monitoring only, not for control flow.
     */
    [[nodiscard]] std::size_t available_approx() const noexcept {
        return free_list_.size_approx();
    }

    [[nodiscard]] std::size_t in_use_approx() const noexcept {
        return Capacity - available_approx();
    }

    [[nodiscard]] constexpr std::size_t capacity() const noexcept {
        return Capacity;
    }

private:
    // Flat pre-allocated storage for all T objects.
    // Cache-line aligned to prevent the first object straddling a cache boundary.
    alignas(kCacheLineSize) T* storage_{nullptr};

    // Lock-free ring buffer acting as the free-slot list.
    // Holds T* pointers to slots within storage_[].
    // At full capacity: Capacity pointers in queue → all slots free.
    // At zero:          0 pointers in queue       → all slots in use.
    RingBuffer<T*, Capacity> free_list_;
};

} // namespace mpmc

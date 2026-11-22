#pragma once

#include "cache.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace mpmc {

namespace detail {
    // Helper template to pack payload and trailing padding into a slot.
    // Specialization for PadSize == 0 prevents zero-length array compiler warnings.
    template <typename T, std::size_t PadSize>
    struct SlotPayload {
        T value;
        char _pad[PadSize]{};
    };

    template <typename T>
    struct SlotPayload<T, 0> {
        T value;
    };
} // namespace detail

/**
 * Cache-line aligned slot for the lock-free MPMC ring buffer.
 *
 * Each slot occupies EXACTLY 64 bytes (1 L1 Data cache line) and is aligned to a 64-byte boundary.
 *
 * Layout (64 bytes total):
 *   - Byte 0..7   : atomic<size_t> sequence (monotonic generation ticket)
 *   - Byte 8..63  : T value (up to 56 bytes) + padding to 64 bytes
 *
 * Payload Modes:
 *   1. Inline by-value mode (sizeof(T) <= 56):
 *      Data payload is stored directly in the slot's cache line.
 *      The consumer acquires both the sequence ticket and the message payload in a
 *      SINGLE cache-line transfer (64 bytes), completely eliminating secondary cache misses.
 *   2. Pointer mode (T*):
 *      For large payloads (>56 bytes), store a pointer T* (8 bytes) paired with an ObjectPool<T>.
 */
template <typename T>
struct alignas(kCacheLineSize) Slot {
    static_assert(sizeof(T) <= (kCacheLineSize - sizeof(std::atomic<std::size_t>)),
        "T must be <= 56 bytes to fit inside a single 64-byte cache-line slot. "
        "For payloads > 56 bytes, instantiate RingBuffer<T*, Capacity> paired with ObjectPool<T>.");

    static constexpr std::size_t kPadSize =
        kCacheLineSize - sizeof(std::atomic<std::size_t>) - sizeof(T);

    // 8 bytes: Ticket counter for Dmitry Vyukov sequence handshake
    std::atomic<std::size_t> sequence;

    // Up to 56 bytes: Payload storage + trailing cache line fill
    detail::SlotPayload<T, kPadSize> payload;

    Slot() noexcept : sequence(0), payload{} {}

    [[nodiscard]] T& data() noexcept { return payload.value; }
    [[nodiscard]] const T& data() const noexcept { return payload.value; }
};

// Strict compile-time validation for 64-bit architecture and cache alignment
static_assert(sizeof(void*) == 8, "MPMC Queue requires a 64-bit architecture (sizeof(void*) == 8)");
static_assert(sizeof(Slot<uint64_t>) == kCacheLineSize, "Slot<uint64_t> must be exactly 64 bytes (1 cache line)");
static_assert(alignof(Slot<uint64_t>) == kCacheLineSize, "Slot<uint64_t> must be aligned to 64 bytes");
static_assert(sizeof(Slot<void*>) == kCacheLineSize, "Slot<void*> must be exactly 64 bytes (1 cache line)");
static_assert(alignof(Slot<void*>) == kCacheLineSize, "Slot<void*> must be aligned to 64 bytes");

} // namespace mpmc

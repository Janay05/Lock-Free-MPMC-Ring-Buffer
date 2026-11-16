#pragma once

#include "cache.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mpmc {

// Cache line aligned slot for the lock-free MPMC ring buffer.
// In low latency architectures, each slot represents an independent synchronization point.
// By isolating each slot into exactly 64 bytes (1 cache line), writes to slot N never
// invalidate the L1/L2 cache line of slot N-1 or slot N+1 (false sharing elimination).
template <typename T = void>
struct alignas(kCacheLineSize) Slot {
    // 8 bytes: Sequence counter used for ticket/handshake synchronization
    // Transitions:
    // Initial: pos
    // Written: pos + 1
    // Read:    pos + capacity
    std::atomic<std::size_t> sequence;

    // 8 bytes: Pointer to the actual data payload.
    // Fixed at pointer width (8 bytes on 64-bit systems) to avoid copying large payloads
    // through the ring buffer. Payloads are allocated in caller-managed arenas / pools.
    T* data;

    // 48 bytes: Cache line padding to fill the remainder of the 64-byte line.
    // 64 - 8 (sequence) - 8 (data) = 48 bytes.
    char _pad[kCacheLineSize - sizeof(std::atomic<std::size_t>) - sizeof(T*)];

    Slot() noexcept : sequence(0), data(nullptr), _pad{} {}
};

// Strict compile-time validation for 64-bit architectures
static_assert(sizeof(void*) == 8, "MPMC Queue requires a 64-bit architecture (sizeof(void*) == 8)");
static_assert(sizeof(Slot<void>) == kCacheLineSize, "Slot must be exactly 64 bytes (1 cache line)");
static_assert(alignof(Slot<void>) == kCacheLineSize, "Slot must be aligned to 64-byte cache line boundary");

} // namespace mpmc

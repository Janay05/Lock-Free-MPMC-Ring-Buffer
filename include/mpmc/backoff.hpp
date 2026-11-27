#pragma once

#include "cache.hpp"
#include <cstdint>

namespace mpmc {

// Pure user-space spin strategy with zero OS scheduler intervention.
// Under low-latency HFT constraints, any invocation of std::this_thread::yield() or sleep()
// triggers a kernel transition (syscall) and thread preemption, costing 1,000 to 10,000+ ns.
// Instead, we issue hardware `pause` instructions (_mm_pause) directly to the CPU core.
class SpinWait {
public:
    constexpr SpinWait() noexcept = default;

    // Execute one spin-wait cycle using x86 hardware pause.
    // Informs the processor speculative execution pipeline that it is inside a spin loop,
    // avoiding false memory order violations on loop exit and throttling pipeline power.
    inline void spin() noexcept {
        cache::cpu_relax();
        ++count_;
    }

    // Reset the spin counter when progress is made
    inline void reset() noexcept {
        count_ = 0;
    }

    [[nodiscard]] inline uint32_t count() const noexcept {
        return count_;
    }

private:
    uint32_t count_{0};
};

// Truncated exponential backoff after a lost CAS: 1, 2, 4, ... pauses, capped at
// MaxPauses. MaxPauses = 1 is a single pause per failure. State is local to one
// queue call, so an uncontended operation pays nothing.
template <uint32_t MaxPauses>
class CasBackoff {
    static_assert(MaxPauses >= 1, "MaxPauses must be at least 1");

public:
    inline void pause() noexcept {
        for (uint32_t i = 0; i < next_; ++i) cache::cpu_relax();
        if (next_ < MaxPauses) next_ *= 2;
    }

private:
    uint32_t next_{1};
};

} // namespace mpmc

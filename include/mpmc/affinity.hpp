#pragma once

#include <cstdint>
#include <thread>

#if defined(_WIN32)
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <windows.h>
#else
    #include <pthread.h>
    #include <sched.h>
#endif

namespace mpmc {

namespace affinity {

    // Pin current executing thread to a specific CPU logical core.
    // In HFT, thread migration between CPU cores causes:
    // 1. Complete L1/L2 cache invalidation (cache misses jump to 100%).
    // 2. Inter-core communication penalties across NUMA nodes.
    // 3. Scheduler preemption jitter.
    inline bool pin_current_thread_to_core(uint32_t core_id) noexcept {
#if defined(_WIN32)
        HANDLE thread = GetCurrentThread();
        DWORD_PTR mask = static_cast<DWORD_PTR>(1ULL << (core_id % 64));
        DWORD_PTR result = SetThreadAffinityMask(thread, mask);
        return result != 0;
#else
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
#endif
    }

    // Set real-time / time-critical priority to prevent OS scheduler preemption
    inline bool set_current_thread_priority_critical() noexcept {
#if defined(_WIN32)
        return SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL) != 0;
#else
        struct sched_param param;
        param.sched_priority = sched_get_priority_max(SCHED_FIFO);
        return pthread_setschedparam(pthread_self(), SCHED_FIFO, &param) == 0;
#endif
    }

} // namespace affinity

} // namespace mpmc

#pragma once

#include <cstdint>
#include <thread>
#include <vector>

#if defined(_WIN32)
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <windows.h>
#else
    #include <pthread.h>
    #include <sched.h>
    #include <fstream>
    #include <string>
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

    // One logical CPU id per physical core, in OS order. Adjacent logical ids are
    // often SMT siblings of one core, so benchmarks must not treat them as cores.
    // On hybrid CPUs only performance cores are returned.
    // Windows: limited to the first processor group (64 logical CPUs).
    inline std::vector<uint32_t> physical_core_cpus() {
        std::vector<uint32_t> cpus;
#if defined(_WIN32)
        DWORD len = 0;
        GetLogicalProcessorInformationEx(RelationProcessorCore, nullptr, &len);
        if (len == 0) return cpus;
        std::vector<char> buf(len);
        auto* first = reinterpret_cast<PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>(buf.data());
        if (!GetLogicalProcessorInformationEx(RelationProcessorCore, first, &len)) return cpus;

        BYTE top_class = 0; // higher EfficiencyClass = faster core type
        for (int pass = 0; pass < 2; ++pass) {
            for (DWORD off = 0; off < len;) {
                auto* info = reinterpret_cast<PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>(buf.data() + off);
                // EfficiencyClass is byte 1; read by offset because older MinGW headers omit the name.
                const BYTE cls = reinterpret_cast<const BYTE*>(&info->Processor)[1];
                if (pass == 0) {
                    if (cls > top_class) top_class = cls;
                } else if (cls == top_class && info->Processor.GroupMask[0].Group == 0) {
                    KAFFINITY mask = info->Processor.GroupMask[0].Mask;
                    uint32_t cpu = 0;
                    while (mask && !(mask & 1)) { mask >>= 1; ++cpu; }
                    cpus.push_back(cpu);
                }
                off += info->Size;
            }
        }
#else
        // Intel hybrid parts list P-core CPUs here; the file is absent elsewhere.
        std::string p_cores;
        std::ifstream("/sys/devices/cpu_core/cpus") >> p_cores;
        auto in_p_cores = [&](uint32_t cpu) {
            if (p_cores.empty()) return true;
            std::size_t i = 0;
            while (i < p_cores.size()) {
                std::size_t used = 0;
                const unsigned long lo = std::stoul(p_cores.substr(i), &used);
                i += used;
                unsigned long hi = lo;
                if (i < p_cores.size() && p_cores[i] == '-') {
                    ++i;
                    hi = std::stoul(p_cores.substr(i), &used);
                    i += used;
                }
                if (cpu >= lo && cpu <= hi) return true;
                ++i; // skip ','
            }
            return false;
        };

        for (uint32_t cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            std::ifstream f("/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/thread_siblings_list");
            if (!f) break;
            uint32_t lowest_sibling = 0;
            f >> lowest_sibling;
            if (lowest_sibling == cpu && in_p_cores(cpu)) cpus.push_back(cpu);
        }
#endif
        return cpus;
    }

    // Pin to the index-th physical core. Returns false if the host has fewer cores
    // or the OS rejects the request; the thread is then left unpinned.
    inline bool pin_current_thread_to_physical_core(uint32_t index) {
        static const std::vector<uint32_t> cpus = physical_core_cpus();
        return index < cpus.size() && pin_current_thread_to_core(cpus[index]);
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

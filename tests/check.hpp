#pragma once

#include <cstdio>
#include <cstdlib>

// Unlike assert(), stays active in Release (-DNDEBUG) builds.
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", #cond,     \
                         __FILE__, __LINE__);                             \
            std::abort();                                                 \
        }                                                                 \
    } while (0)

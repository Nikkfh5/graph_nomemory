#pragma once

#include <pthread.h>
#include <time.h>

namespace tbank::parallel::detail {

// Injectable clock calls for deterministic executor tests.
struct FixedExecutorClockFunctions {
    void* context{};
    int (*get_time)(void*, clockid_t, timespec*) noexcept{};
    int (*get_resolution)(void*, clockid_t, timespec*) noexcept{};
    int (*get_worker_clock_id)(
        void*, pthread_t, clockid_t*
    ) noexcept{};
};

}  // namespace tbank::parallel::detail

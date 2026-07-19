#pragma once

#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace tbank::pagerank::detail {

using TelemetryClock = std::chrono::steady_clock;
using TelemetryTimePoint = TelemetryClock::time_point;
static_assert(TelemetryClock::is_steady);

[[nodiscard]] inline std::uint64_t elapsed_nanoseconds(
    const TelemetryTimePoint started,
    const TelemetryTimePoint finished
) {
    if (finished < started) {
        throw std::logic_error("monotonic telemetry clock moved backwards");
    }

    const TelemetryClock::duration elapsed = finished - started;
    using WideNanoseconds = std::chrono::duration<long double, std::nano>;
    const long double value = WideNanoseconds(elapsed).count();
    const long double uint64_exclusive_upper_bound = std::ldexp(
        1.0L, std::numeric_limits<std::uint64_t>::digits
    );
    if (!std::isfinite(value) || value < 0.0L
        || value >= uint64_exclusive_upper_bound) {
        throw std::overflow_error(
            "monotonic telemetry duration is outside uint64 nanoseconds"
        );
    }
    using UnsignedNanoseconds =
        std::chrono::duration<std::uint64_t, std::nano>;
    return std::chrono::duration_cast<UnsignedNanoseconds>(elapsed).count();
}

inline void add_telemetry_value(
    std::uint64_t& destination,
    const std::uint64_t increment
) {
    if (increment > std::numeric_limits<std::uint64_t>::max() - destination) {
        throw std::overflow_error("telemetry counter overflow");
    }
    destination += increment;
}

inline void increment_telemetry_count(std::uint64_t& destination) {
    add_telemetry_value(destination, 1U);
}

}  // namespace tbank::pagerank::detail

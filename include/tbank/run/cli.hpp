#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace tbank::cli {

inline constexpr int kExitSuccess = 0;
inline constexpr int kExitUsage = 2;
inline constexpr int kExitData = 3;
inline constexpr int kExitResource = 4;
inline constexpr int kExitNonConverged = 5;
inline constexpr int kExitNumericalFailure = 6;
inline constexpr int kExitPublication = 7;
inline constexpr int kExitDurabilityUncertain = 8;
inline constexpr int kExitSystem = 9;
inline constexpr int kExitInternal = 10;

class UsageError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

[[nodiscard]] bool is_help_request(int argument_count, char* arguments[]);

// Converts a closed stdout/stderr pipe into a checked EPIPE result instead of
// asynchronous SIGPIPE process termination.
void initialize_result_channels();

[[nodiscard]] std::uint64_t parse_u64(
    std::string_view option,
    std::string_view value
);

[[nodiscard]] std::size_t parse_size(
    std::string_view option,
    std::string_view value
);

[[nodiscard]] double parse_finite_double(
    std::string_view option,
    std::string_view value
);

void require_valid_utf8(
    std::string_view option,
    std::string_view value
);

void write_stdout(std::string_view payload);
void write_stderr(std::string_view payload) noexcept;

}  // namespace tbank::cli

#pragma once

#include "tbank/pagerank/config.hpp"
#include "tbank/preprocess/prepare.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string_view>

namespace tbank::run {

inline constexpr std::size_t kMaximumRunConfigBytes = 64U * 1024U;
inline constexpr std::string_view kRunConfigSchema =
    "tbank-run-config-v1";

class RunConfigError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct RunConfig {
    preprocess::CountedGraphPreprocessorConfig preprocess{};
    pagerank::PageRankConfig pagerank{};

    friend bool operator==(const RunConfig& left, const RunConfig& right) {
        return left.preprocess == right.preprocess
            && left.pagerank.alpha == right.pagerank.alpha
            && left.pagerank.eta == right.pagerank.eta
            && left.pagerank.max_iterations == right.pagerank.max_iterations
            && left.pagerank.tau_mass == right.pagerank.tau_mass
            && left.pagerank.epsilon_verify == right.pagerank.epsilon_verify
            && left.pagerank.resources == right.pagerank.resources;
    }
};

// main_stack_bytes is observed by the executable, not configurable.
[[nodiscard]] RunConfig default_run_config(
    std::uint64_t observed_main_stack_bytes
);

// Parses strict v1 key=value input before workspace or output creation.
[[nodiscard]] RunConfig parse_run_config(
    std::string_view bytes,
    std::uint64_t observed_main_stack_bytes
);

// Reads one stable, bounded, non-symlink regular file; config never supplies pipeline paths.
[[nodiscard]] RunConfig load_run_config(
    const std::filesystem::path& path,
    std::uint64_t observed_main_stack_bytes
);

// ENOENT selects defaults; every other failure is reported.
[[nodiscard]] RunConfig load_run_config_or_default(
    const std::filesystem::path& path,
    std::uint64_t observed_main_stack_bytes
);

// Static validation uses the shortest valid CSV; runtime admits actual input size and observed
// main stack separately.
void validate_run_config(const RunConfig& config);

}  // namespace tbank::run

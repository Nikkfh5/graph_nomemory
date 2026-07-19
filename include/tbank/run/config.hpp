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

// Returns the accepted resource profile. The main-stack inventory is observed
// by the executable and deliberately cannot be overridden by a config file.
[[nodiscard]] RunConfig default_run_config(
    std::uint64_t observed_main_stack_bytes
);

// Applies the strict v1 key=value document to default_run_config(). Parsing and
// complete static validation finish before the caller can create any workspace
// or output artifact.
[[nodiscard]] RunConfig parse_run_config(
    std::string_view bytes,
    std::uint64_t observed_main_stack_bytes
);

// Opens one non-symlink regular file without blocking, reads its stable
// contents into a bounded heap buffer, then delegates to the same strict
// parser. The path is configuration input only; paths used by the graph
// pipeline are never part of RunConfig.
[[nodiscard]] RunConfig load_run_config(
    const std::filesystem::path& path,
    std::uint64_t observed_main_stack_bytes
);

// Validates a resolved typed configuration. csv_byte_limit is derived from the
// input file at runtime, so the static preprocessing check uses the shortest
// valid CSV length without mutating the supplied object. main_stack_bytes is a
// host observation; its actual memory admission remains a separate resource
// check in the executable.
void validate_run_config(const RunConfig& config);

}  // namespace tbank::run

#pragma once

#include "tbank/resources/page_rank.hpp"

#include <cstdint>

namespace tbank::pagerank {

// EXP-SUM accepted the hexadecimal tolerances for the verified single-thread domain.
inline constexpr double kPageRankDefaultAlpha = 0.85;
inline constexpr double kPageRankDefaultEta = 1.0e-8;
inline constexpr std::uint64_t kPageRankDefaultMaxIterations = 200U;
inline constexpr double kPageRankDefaultTauMass = 0x1p-30;
inline constexpr double kPageRankDefaultEpsilonVerify = 0x1p-30;
inline constexpr std::uint32_t kPageRankVerifiedMaximumVertexCount =
    1'000'000U;

struct PageRankConfig {
    double alpha = kPageRankDefaultAlpha;
    double eta = kPageRankDefaultEta;
    std::uint64_t max_iterations = kPageRankDefaultMaxIterations;

    double tau_mass = kPageRankDefaultTauMass;
    double epsilon_verify = kPageRankDefaultEpsilonVerify;

    resources::PageRankResourcePolicy resources{};
};

// Graph-dependent admission runs after manifest read, before allocation or edge traversal.
void validate_page_rank_config(const PageRankConfig& config);

}  // namespace tbank::pagerank

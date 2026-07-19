#pragma once

#include "tbank/pagerank/pagerank.hpp"

#include <cstdint>
#include <span>

namespace tbank::pagerank::detail {

struct TransformResult {
    PageRankNumericalFailure failure = PageRankNumericalFailure::none;
    double dangling_mass = 0.0;
};

struct CandidateMetrics {
    PageRankNumericalFailure failure = PageRankNumericalFailure::none;
    double raw_mass = 0.0;
    double mass_error = 0.0;
    double delta_l1 = 0.0;
};

[[nodiscard]] TransformResult transform_current_in_place(
    std::span<double> current,
    std::span<const std::uint32_t> out_degree
) noexcept;

// Pre-normalization failure leaves the candidate unnormalized, preserving mass violations.
[[nodiscard]] CandidateMetrics normalize_and_measure_candidate(
    std::span<double> candidate,
    std::span<const double> transformed_current,
    std::span<const std::uint32_t> out_degree,
    double tau_mass
) noexcept;

[[nodiscard]] bool delta_prefilter_passes(
    double delta_l1,
    double alpha,
    double eta
) noexcept;

[[nodiscard]] bool residual_condition_passes(
    double true_residual_l1,
    double epsilon_verify,
    double alpha,
    double eta
) noexcept;

}  // namespace tbank::pagerank::detail

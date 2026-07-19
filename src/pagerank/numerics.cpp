#include "numerics.hpp"

#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>

namespace tbank::pagerank::detail {
namespace {

static_assert(sizeof(double) == sizeof(std::uint64_t));
static_assert(std::numeric_limits<double>::is_iec559);

// Binary64 products use 2^-2148 units and at most bit 2047; 66 limbs suffice.
class ExactNonnegativeBinary final {
public:
    void add_double(const double value) noexcept {
        const Binary64 decoded = decode(value);
        add_significand(
            decoded.significand,
            decoded.exponent - kBaseExponent
        );
    }

    void add_product(const double left, const double right) noexcept {
        const Binary64 first = decode(left);
        const Binary64 second = decode(right);
        if (first.significand == 0U || second.significand == 0U) {
            return;
        }
        const int product_offset = first.exponent + second.exponent
            - kBaseExponent;
        for (unsigned int bit = 0U; bit < kSignificandBits; ++bit) {
            if ((second.significand & (std::uint64_t{1U} << bit)) != 0U) {
                add_significand(
                    first.significand,
                    product_offset + static_cast<int>(bit)
                );
            }
        }
    }

    [[nodiscard]] int compare(
        const ExactNonnegativeBinary& other
    ) const noexcept {
        for (std::size_t offset = 0U; offset < limbs_.size(); ++offset) {
            const std::size_t index = limbs_.size() - 1U - offset;
            if (limbs_[index] < other.limbs_[index]) {
                return -1;
            }
            if (limbs_[index] > other.limbs_[index]) {
                return 1;
            }
        }
        return 0;
    }

private:
    struct Binary64 {
        std::uint64_t significand = 0U;
        int exponent = 0;
    };

    static constexpr int kBaseExponent = -2'148;
    static constexpr unsigned int kFractionBits = 52U;
    static constexpr unsigned int kSignificandBits = 53U;
    static constexpr std::size_t kLimbCount = 66U;
    static constexpr std::uint64_t kFractionMask =
        (std::uint64_t{1U} << kFractionBits) - 1U;
    static constexpr std::uint64_t kHiddenBit =
        std::uint64_t{1U} << kFractionBits;

    [[nodiscard]] static Binary64 decode(const double value) noexcept {
        const std::uint64_t bits = std::bit_cast<std::uint64_t>(value);
        const std::uint64_t fraction = bits & kFractionMask;
        const unsigned int stored_exponent = static_cast<unsigned int>(
            (bits >> kFractionBits) & 0x7ffU
        );
        if (stored_exponent == 0U) {
            return Binary64{
                .significand = fraction,
                .exponent = -1'074,
            };
        }
        return Binary64{
            .significand = kHiddenBit | fraction,
            .exponent = static_cast<int>(stored_exponent) - 1'023 - 52,
        };
    }

    void add_significand(
        const std::uint64_t significand,
        const int bit_offset
    ) noexcept {
        for (unsigned int bit = 0U; bit < kSignificandBits; ++bit) {
            if ((significand & (std::uint64_t{1U} << bit)) != 0U) {
                add_power_of_two(bit_offset + static_cast<int>(bit));
            }
        }
    }

    void add_power_of_two(const int bit_index) noexcept {
        if (bit_index < 0) {
            std::terminate();
        }
        std::size_t limb = static_cast<std::size_t>(bit_index) / 64U;
        const unsigned int offset = static_cast<unsigned int>(bit_index) % 64U;
        if (limb >= limbs_.size()) {
            std::terminate();
        }

        const std::uint64_t addend = std::uint64_t{1U} << offset;
        const std::uint64_t previous = limbs_[limb];
        limbs_[limb] += addend;
        if (limbs_[limb] >= previous) {
            return;
        }
        ++limb;
        while (limb < limbs_.size()) {
            ++limbs_[limb];
            if (limbs_[limb] != 0U) {
                return;
            }
            ++limb;
        }
        std::terminate();
    }

    std::array<std::uint64_t, kLimbCount> limbs_{};
};

[[nodiscard]] bool valid_threshold_inputs(
    const double alpha,
    const double eta
) noexcept {
    return std::isfinite(alpha) && alpha > 0.0 && alpha < 1.0
        && std::isfinite(eta) && eta > 0.0;
}

}  // namespace

TransformResult transform_current_in_place(
    const std::span<double> current,
    const std::span<const std::uint32_t> out_degree
) noexcept {
    if (current.size() != out_degree.size()) {
        return TransformResult{
            .failure = PageRankNumericalFailure::invalid_current_rank,
            .dangling_mass = 0.0,
        };
    }

    double dangling_mass = 0.0;
    for (std::size_t vertex = 0U; vertex < current.size(); ++vertex) {
        const double rank = current[vertex];
        if (!std::isfinite(rank) || rank < 0.0) {
            return TransformResult{
                .failure = PageRankNumericalFailure::invalid_current_rank,
                .dangling_mass = dangling_mass,
            };
        }
        if (out_degree[vertex] == 0U) {
            dangling_mass += rank;
            if (!std::isfinite(dangling_mass)) {
                return TransformResult{
                    .failure =
                        PageRankNumericalFailure::invalid_dangling_mass,
                    .dangling_mass = dangling_mass,
                };
            }
            continue;
        }

        current[vertex] = rank / static_cast<double>(out_degree[vertex]);
        if (!std::isfinite(current[vertex]) || current[vertex] < 0.0) {
            return TransformResult{
                .failure = PageRankNumericalFailure::invalid_transformed_rank,
                .dangling_mass = dangling_mass,
            };
        }
    }

    return TransformResult{
        .failure = PageRankNumericalFailure::none,
        .dangling_mass = dangling_mass,
    };
}

CandidateMetrics normalize_and_measure_candidate(
    const std::span<double> candidate,
    const std::span<const double> transformed_current,
    const std::span<const std::uint32_t> out_degree,
    const double tau_mass
) noexcept {
    if (candidate.size() != transformed_current.size()
        || candidate.size() != out_degree.size()) {
        return CandidateMetrics{
            .failure = PageRankNumericalFailure::invalid_candidate_rank,
        };
    }

    double raw_mass = 0.0;
    for (const double rank : candidate) {
        if (!std::isfinite(rank) || rank < 0.0) {
            return CandidateMetrics{
                .failure = PageRankNumericalFailure::invalid_candidate_rank,
                .raw_mass = raw_mass,
            };
        }
        raw_mass += rank;
        if (!std::isfinite(raw_mass)) {
            return CandidateMetrics{
                .failure = PageRankNumericalFailure::invalid_raw_mass,
                .raw_mass = raw_mass,
            };
        }
    }
    if (raw_mass <= 0.0) {
        return CandidateMetrics{
            .failure = PageRankNumericalFailure::invalid_raw_mass,
            .raw_mass = raw_mass,
        };
    }

    const double mass_error = std::abs(raw_mass - 1.0);
    if (!std::isfinite(mass_error)) {
        return CandidateMetrics{
            .failure = PageRankNumericalFailure::invalid_raw_mass,
            .raw_mass = raw_mass,
            .mass_error = mass_error,
        };
    }
    if (mass_error > tau_mass) {
        return CandidateMetrics{
            .failure = PageRankNumericalFailure::mass_tolerance_exceeded,
            .raw_mass = raw_mass,
            .mass_error = mass_error,
        };
    }

    double delta_l1 = 0.0;
    for (std::size_t vertex = 0U; vertex < candidate.size(); ++vertex) {
        candidate[vertex] /= raw_mass;
        if (!std::isfinite(candidate[vertex]) || candidate[vertex] < 0.0) {
            return CandidateMetrics{
                .failure =
                    PageRankNumericalFailure::invalid_normalized_rank,
                .raw_mass = raw_mass,
                .mass_error = mass_error,
                .delta_l1 = delta_l1,
            };
        }

        const double old_rank = out_degree[vertex] == 0U
            ? transformed_current[vertex]
            : transformed_current[vertex]
                * static_cast<double>(out_degree[vertex]);
        if (!std::isfinite(old_rank) || old_rank < 0.0) {
            return CandidateMetrics{
                .failure = PageRankNumericalFailure::invalid_current_rank,
                .raw_mass = raw_mass,
                .mass_error = mass_error,
                .delta_l1 = delta_l1,
            };
        }
        delta_l1 += std::abs(candidate[vertex] - old_rank);
        if (!std::isfinite(delta_l1)) {
            return CandidateMetrics{
                .failure = PageRankNumericalFailure::invalid_delta,
                .raw_mass = raw_mass,
                .mass_error = mass_error,
                .delta_l1 = delta_l1,
            };
        }
    }

    return CandidateMetrics{
        .failure = PageRankNumericalFailure::none,
        .raw_mass = raw_mass,
        .mass_error = mass_error,
        .delta_l1 = delta_l1,
    };
}

bool delta_prefilter_passes(
    const double delta_l1,
    const double alpha,
    const double eta
) noexcept {
    if (!std::isfinite(delta_l1) || delta_l1 < 0.0) {
        return false;
    }
    if (!valid_threshold_inputs(alpha, eta)) {
        return false;
    }
    ExactNonnegativeBinary left;
    left.add_product(delta_l1, alpha);
    left.add_product(eta, alpha);
    ExactNonnegativeBinary right;
    right.add_double(eta);
    return left.compare(right) <= 0;
}

bool residual_condition_passes(
    const double true_residual_l1,
    const double epsilon_verify,
    const double alpha,
    const double eta
) noexcept {
    if (!std::isfinite(true_residual_l1) || true_residual_l1 < 0.0
        || !std::isfinite(epsilon_verify) || epsilon_verify < 0.0) {
        return false;
    }
    if (!valid_threshold_inputs(alpha, eta)) {
        return false;
    }
    ExactNonnegativeBinary guarded_residual;
    guarded_residual.add_double(true_residual_l1);
    guarded_residual.add_double(epsilon_verify);
    guarded_residual.add_product(alpha, eta);
    ExactNonnegativeBinary threshold;
    threshold.add_double(eta);
    return guarded_residual.compare(threshold) <= 0;
}

}  // namespace tbank::pagerank::detail

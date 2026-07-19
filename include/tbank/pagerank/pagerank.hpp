#pragma once

#include "tbank/pagerank/config.hpp"
#include "tbank/resources/page_rank.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>

namespace tbank::pagerank {

namespace detail {
class PageRankResultFactory;
}

enum class PageRankStatus {
    converged,
    numerical_failure,
    non_converged,
};

enum class PageRankNumericalFailure {
    none,
    invalid_current_rank,
    invalid_transformed_rank,
    invalid_dangling_mass,
    invalid_candidate_rank,
    invalid_raw_mass,
    mass_tolerance_exceeded,
    invalid_normalized_rank,
    invalid_delta,
    invalid_residual,
};

struct PageRankReport {
    PageRankStatus status = PageRankStatus::non_converged;
    PageRankNumericalFailure numerical_failure =
        PageRankNumericalFailure::none;
    std::uint64_t iterations_attempted = 0U;
    std::uint64_t residual_checks = 0U;
    std::optional<double> last_raw_mass{};
    double max_mass_error = 0.0;
    std::optional<double> last_delta_l1{};
    std::optional<double> theoretical_delta_error_estimate_l1{};
    std::optional<double> true_residual_l1{};
    std::optional<double> theoretical_residual_error_estimate_l1{};
    resources::PageRankMemoryPlan memory_plan{};

    friend bool operator==(const PageRankReport&, const PageRankReport&) =
        default;
};

struct PageRankIterationView {
    std::uint64_t iteration = 0U;
    std::span<const double> candidate{};
    double raw_mass = 0.0;
    double delta_l1 = 0.0;
    std::optional<double> true_residual_l1{};
};

// Callback-scoped normalized candidate; production never retains it.
using PageRankIterationObserver =
    std::function<void(const PageRankIterationView&)>;

class PageRankExecutionError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class PageRankVertexCountLimitError final : public std::runtime_error {
public:
    explicit PageRankVertexCountLimitError(std::uint32_t vertex_count);

    [[nodiscard]] std::uint32_t vertex_count() const noexcept;
    [[nodiscard]] std::uint32_t maximum_vertex_count() const noexcept;

private:
    std::uint32_t vertex_count_;
};

class PageRankResult final {
public:
    PageRankResult(const PageRankResult&) = delete;
    PageRankResult& operator=(const PageRankResult&) = delete;
    PageRankResult(PageRankResult&&) noexcept = default;
    PageRankResult& operator=(PageRankResult&&) noexcept = default;
    ~PageRankResult() = default;

    [[nodiscard]] const PageRankReport& report() const noexcept;
    [[nodiscard]] bool has_verified_ranks() const noexcept;

    // Only verified convergence owns ranks: the verified scratch array, without swap or full-vector
    // copy.
    [[nodiscard]] std::span<const double> verified_ranks() const;

private:
    friend class detail::PageRankResultFactory;
    friend PageRankResult run_page_rank_single_thread(
        const std::filesystem::path&,
        const PageRankConfig&,
        const PageRankIterationObserver&
    );

    PageRankResult(
        PageRankReport report,
        std::unique_ptr<double[]> verified_ranks,
        std::uint32_t vertex_count
    ) noexcept;

    PageRankReport report_{};
    std::unique_ptr<double[]> verified_ranks_{};
    std::uint32_t vertex_count_ = 0U;
};

// Enforces config, numerical domain and memory admission before graph scans.
[[nodiscard]] PageRankResult run_page_rank_single_thread(
    const std::filesystem::path& graph_directory,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer = {}
);

// Uses bounded persistent pthreads without changing task or reduction order.
[[nodiscard]] PageRankResult run_page_rank_parallel(
    const std::filesystem::path& graph_directory,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer = {}
);

}  // namespace tbank::pagerank

#pragma once

#include "tbank/pagerank/pagerank.hpp"
#include "tbank/parallel/fixed_executor.hpp"
#include "tbank/storage/graph.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>

namespace tbank::platform {
class PublicationBackend;
}

namespace tbank::pagerank {
struct AnalyzeResult;
}

namespace tbank::pagerank::detail {

struct OpenedPageRankGraph {
    storage::ValidatedGraph graph;
    resources::PageRankMemoryPlan memory_plan;
};

struct PageRankTraversalTelemetry {
    std::uint64_t candidate_destination_traversal_ns = 0U;
    std::uint64_t first_candidate_destination_traversal_ns = 0U;
    std::uint64_t true_residual_destination_traversal_ns = 0U;
    std::uint64_t candidate_destination_traversal_count = 0U;
    std::uint64_t true_residual_destination_traversal_count = 0U;
    parallel::FixedExecutorConcurrencyEvidence internal_concurrency{
        .status = parallel::FixedExecutorConcurrencyStatus::not_applicable,
        .failure = parallel::FixedExecutorConcurrencyFailure::none,
    };
};

// Bounded synchronous traversal: one writer per destination; rank arrays stay coordinator-owned.
class DestinationTraversal {
public:
    DestinationTraversal() = default;
    DestinationTraversal(const DestinationTraversal&) = delete;
    DestinationTraversal& operator=(const DestinationTraversal&) = delete;
    virtual ~DestinationTraversal() = default;

    virtual void write_candidate(
        std::span<const std::uint32_t> out_degree,
        std::span<const double> transformed_current,
        double base,
        double alpha,
        std::span<double> candidate
    ) = 0;

    [[nodiscard]] virtual std::optional<double> compute_true_residual(
        std::span<const std::uint32_t> out_degree,
        std::span<const double> candidate,
        std::span<double> reusable_workspace,
        const PageRankConfig& config
    ) = 0;
};

// Validate manifest, resource budget, and numerical domain before opening payloads.
[[nodiscard]] OpenedPageRankGraph open_page_rank_graph(
    const std::filesystem::path& graph_directory,
    const PageRankConfig& config
);

// Keep one validated generation open through analytics and result emission.
[[nodiscard]] PageRankResult run_page_rank_on_opened_graph(
    const OpenedPageRankGraph& opened,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer = {},
    PageRankTraversalTelemetry* telemetry = nullptr
);

[[nodiscard]] PageRankResult run_page_rank_on_opened_graph_parallel(
    const OpenedPageRankGraph& opened,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer = {},
    PageRankTraversalTelemetry* telemetry = nullptr
);

// Test hook preserving graph binding and resource admission.
[[nodiscard]] AnalyzeResult analyze_page_rank_to_csv_with_after_open_hook(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config,
    platform::PublicationBackend& publication_backend,
    const std::function<void()>& after_open
);

// One state machine keeps convergence, ownership, observer, and publication semantics identical
// across modes.
[[nodiscard]] PageRankResult run_page_rank_with_traversal(
    const OpenedPageRankGraph& opened,
    const PageRankConfig& config,
    DestinationTraversal& traversal,
    const PageRankIterationObserver& observer = {},
    PageRankTraversalTelemetry* telemetry = nullptr
);

class PageRankResultFactory final {
public:
    [[nodiscard]] static PageRankResult make(
        PageRankReport report,
        std::unique_ptr<double[]> verified_ranks,
        std::uint32_t vertex_count
    ) noexcept;
};

}  // namespace tbank::pagerank::detail

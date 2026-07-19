#pragma once

#include "tbank/io/rank_csv.hpp"
#include "tbank/pagerank/config.hpp"
#include "tbank/pagerank/pagerank.hpp"
#include "tbank/parallel/fixed_executor.hpp"
#include "tbank/platform/publication.hpp"

#include <cstdint>
#include <filesystem>
#include <optional>

namespace tbank::pagerank {

enum class AnalyzeStatus {
    published,
    numerical_failure,
    non_converged,
    publication_failed,
    durability_uncertain,
};

// Nondeterministic steady-clock observations for one analyze invocation.
// These measurements are deliberately separate from PageRankReport: they do
// not participate in numerical equality or affect the deterministic CSV.
struct AnalyzeTelemetry {
    std::uint64_t graph_validation_open_preflight_ns = 0U;
    std::uint64_t pagerank_engine_ns = 0U;
    std::uint64_t candidate_destination_traversal_ns = 0U;
    // The first candidate traversal is separate because the parallel path
    // lazily constructs its bounded worker pool inside this interval.
    std::uint64_t first_candidate_destination_traversal_ns = 0U;
    // Covers the complete verified-residual pass: its coordinator vertex
    // scans/reduction and its destination traversal are one numerical unit.
    std::uint64_t true_residual_destination_traversal_ns = 0U;
    // Exactly candidate_destination_traversal_ns plus the residual-pass value.
    std::uint64_t core_destination_traversal_ns = 0U;
    // Exactly pagerank_engine_ns minus core_destination_traversal_ns; includes
    // engine setup, rank-vector math, checks and coordinator overhead.
    std::uint64_t pagerank_engine_other_ns = 0U;
    // Null when numerical failure or non-convergence skips publication.
    std::optional<std::uint64_t> csv_publication_ns{};
    // Whole library call from graph-open start through outcome classification;
    // CLI argument parsing and JSON receipt emission are outside this value.
    std::uint64_t total_ns = 0U;
    std::uint64_t candidate_destination_traversal_count = 0U;
    std::uint64_t true_residual_destination_traversal_count = 0U;
    parallel::FixedExecutorConcurrencyEvidence internal_concurrency{
        .status = parallel::FixedExecutorConcurrencyStatus::not_applicable,
        .failure = parallel::FixedExecutorConcurrencyFailure::none,
    };
};

struct AnalyzeResult {
    AnalyzeStatus status = AnalyzeStatus::non_converged;
    PageRankReport pagerank_report{};
    std::optional<io::RankCsvSummary> csv_summary{};
    platform::PublicationResult publication{};
    AnalyzeTelemetry telemetry{};
};

// Computes and publishes ranks from one open immutable graph generation.
// Numerical failures do not publish; output must be outside the graph directory.
[[nodiscard]] AnalyzeResult analyze_page_rank_to_csv(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config
);

// Deterministic publication seam for failure-injection tests. It has the same
// state and ownership contract as the POSIX overload.
[[nodiscard]] AnalyzeResult analyze_page_rank_to_csv(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config,
    platform::PublicationBackend& publication_backend
);

}  // namespace tbank::pagerank

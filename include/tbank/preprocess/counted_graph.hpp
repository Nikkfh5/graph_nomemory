#pragma once

#include "tbank/platform/publication.hpp"
#include "tbank/preprocess/compact_edge_merge.hpp"
#include "tbank/storage/manifest.hpp"
#include "tbank/tasks/partitioner.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <system_error>

namespace tbank::preprocess {

inline constexpr std::size_t kDefaultCountedGraphEdgeBatchRecords = 8'192U;
inline constexpr std::size_t kDefaultCountedGraphValidationChunkBytes =
    64U * 1024U;

struct CountedGraphConfig {
    CompactEdgeMergeConfig merge{};
    tasks::TaskConfig tasks{};
    std::size_t edge_batch_records = kDefaultCountedGraphEdgeBatchRecords;
    std::size_t validation_io_chunk_bytes =
        kDefaultCountedGraphValidationChunkBytes;

    friend bool operator==(
        const CountedGraphConfig&,
        const CountedGraphConfig&
    ) = default;
};

// Peak managed memory is the maximum of exclusive phases, not their sum.
// Logical disk peak includes staging graph and both predecessor generations.
struct CountedGraphResourcePlan {
    std::uint64_t vertex_count = 0U;
    std::uint64_t frontier_run_count = 0U;
    std::uint64_t frontier_record_count = 0U;
    std::uint64_t edge_count_upper_bound = 0U;
    std::uint64_t task_count_upper_bound = 0U;

    std::uint64_t out_degree_bytes = 0U;
    std::uint64_t edge_batch_bytes = 0U;
    std::uint64_t source_batch_bytes = 0U;
    std::uint64_t cursor_reader_buffers_bytes = 0U;
    std::uint64_t cursor_objects_bytes = 0U;
    std::uint64_t cursor_heap_arrays_bytes = 0U;
    std::uint64_t output_writer_buffers_bytes = 0U;
    std::uint64_t merge_phase_upper_bound_bytes = 0U;
    std::uint64_t out_degree_write_phase_upper_bound_bytes = 0U;
    std::uint64_t vertex_copy_phase_upper_bound_bytes = 0U;
    std::uint64_t validation_phase_upper_bound_bytes = 0U;
    std::uint64_t cursor_policy_floor_bytes = 0U;
    std::uint64_t managed_bulk_upper_bound_bytes = 0U;
    std::uint64_t max_open_files = 0U;

    std::uint64_t predecessor_frontier_logical_bytes = 0U;
    std::uint64_t predecessor_vertex_ids_logical_bytes = 0U;
    std::uint64_t staging_logical_upper_bound_bytes = 0U;
    std::uint64_t logical_peak_upper_bound_bytes = 0U;

    friend bool operator==(
        const CountedGraphResourcePlan&,
        const CountedGraphResourcePlan&
    ) = default;
};

[[nodiscard]] CountedGraphResourcePlan counted_graph_resource_plan(
    CompactEdgeMergeFrontier frontier,
    const CountedGraphConfig& config
);

enum class PredecessorRetirementState {
    not_attempted,
    retired,
    failed,
};

struct CountedGraphBuildResult {
    storage::GraphManifest manifest{};
    // Global accounting: input_record_count is frontier.initial_record_count,
    // so duplicate_record_count includes records removed by prior merge passes.
    CompactEdgeUniqueSummary edges{};
    tasks::TaskPartitionSummary tasks{};
    CountedGraphResourcePlan resources{};
    platform::PublicationResult publication{};
    PredecessorRetirementState frontier_retirement =
        PredecessorRetirementState::not_attempted;
    std::error_code frontier_retirement_error{};
    PredecessorRetirementState vertex_ids_retirement =
        PredecessorRetirementState::not_attempted;
    std::error_code vertex_ids_retirement_error{};

    // Verified staging survives pre-rename failure; only published proves durability.
    std::filesystem::path staging_path{};
};

// Validates staging before publication. Pre-rename failures preserve recovery state; after rename,
// the target is authoritative and cleanup is retryable. durability_uncertain leaves the target
// visible; target parent and workspace must stay trusted.
[[nodiscard]] CountedGraphBuildResult build_and_publish_counted_graph(
    const std::filesystem::path& preprocess_workspace,
    CompactEdgeMergeFrontier frontier,
    const std::filesystem::path& target,
    CountedGraphConfig config
);

[[nodiscard]] CountedGraphBuildResult build_and_publish_counted_graph(
    const std::filesystem::path& preprocess_workspace,
    CompactEdgeMergeFrontier frontier,
    const std::filesystem::path& target,
    CountedGraphConfig config,
    platform::PublicationBackend& backend
);

}  // namespace tbank::preprocess

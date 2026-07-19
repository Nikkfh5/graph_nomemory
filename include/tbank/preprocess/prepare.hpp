#pragma once

#include "tbank/preprocess/counted_graph.hpp"
#include "tbank/preprocess/ingest.hpp"
#include "tbank/preprocess/vertex_id_merge.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>
#include <system_error>

namespace tbank::preprocess {

struct PreprocessResourcePolicy {
    // Mandatory reserve for caller buffers, stacks, allocator and runtime state.
    std::uint64_t non_bulk_reserve_bytes = 0U;

    // Caller chunk bound; remaining reserve covers parser/runtime, allocator metadata, and stacks.
    std::uint64_t maximum_input_chunk_bytes = 0U;

    // Excludes caller input and process-wide baseline descriptors.
    std::uint64_t phase_fd_budget = 0U;

    // Disk headroom beyond logical data and filesystem accounting allowances.
    std::uint64_t minimum_free_space_reserve_bytes = 0U;

    friend bool operator==(
        const PreprocessResourcePolicy&,
        const PreprocessResourcePolicy&
    ) = default;
};

struct CountedGraphPreprocessorConfig {
    InitialRunConfig initial_runs{};
    VertexIdMergeConfig vertex_ids{};
    CompactEdgeRunConfig compact_edges{};
    CountedGraphConfig counted_graph{};
    PreprocessResourcePolicy resources{};

    // Required input-size bound for disk admission before workspace creation.
    std::uint64_t csv_byte_limit = 0U;

    friend bool operator==(
        const CountedGraphPreprocessorConfig&,
        const CountedGraphPreprocessorConfig&
    ) = default;
};

struct InitialRunDiskPlan {
    std::uint64_t csv_byte_limit = 0U;
    std::uint64_t raw_edge_count_upper_bound = 0U;
    std::uint64_t endpoint_run_count_upper_bound = 0U;
    std::uint64_t raw_edge_file_bytes = 0U;
    std::uint64_t endpoint_run_files_bytes_upper_bound = 0U;
    std::uint64_t successor_logical_upper_bound_bytes = 0U;
    std::uint64_t successor_file_count_upper_bound = 0U;
    std::uint64_t successor_entry_count_upper_bound = 0U;

    friend bool operator==(const InitialRunDiskPlan&, const InitialRunDiskPlan&)
        = default;
};

struct VertexIdMergeDiskPlan {
    std::uint64_t raw_edge_count = 0U;
    std::uint64_t input_run_count = 0U;
    std::uint64_t input_record_count = 0U;
    std::uint64_t successor_run_count_upper_bound = 0U;
    std::uint64_t raw_edge_file_bytes = 0U;
    std::uint64_t predecessor_run_files_bytes = 0U;
    std::uint64_t successor_logical_upper_bound_bytes = 0U;
    std::uint64_t logical_peak_upper_bound_bytes = 0U;
    std::uint64_t successor_file_count_upper_bound = 0U;
    std::uint64_t successor_entry_count_upper_bound = 0U;

    friend bool operator==(
        const VertexIdMergeDiskPlan&,
        const VertexIdMergeDiskPlan&
    ) = default;
};

struct PreprocessStaticResourcePlan {
    std::uint64_t hard_memory_budget_bytes =
        kCompactEdgeHardMemoryBudgetBytes;
    std::uint64_t non_bulk_reserve_bytes = 0U;
    std::uint64_t maximum_input_chunk_bytes = 0U;
    std::uint64_t runtime_reserve_after_input_chunk_bytes = 0U;
    std::uint64_t managed_bulk_limit_bytes = 0U;
    InitialRunMemoryPlan initial_run_memory{};
    VertexIdMergeMemoryPlan vertex_id_merge_memory{};
    CompactEdgeRunMemoryPlan minimum_compact_edge_memory{};
    CompactEdgeMergeMemoryPlan compact_edge_merge_memory{};
    CountedGraphResourcePlan full_fan_in_counted_graph_floor{};
    InitialRunDiskPlan initial_run_disk{};
    std::uint64_t managed_bulk_floor_bytes = 0U;
    std::uint64_t component_fd_floor = 0U;

    friend bool operator==(
        const PreprocessStaticResourcePlan&,
        const PreprocessStaticResourcePlan&
    ) = default;
};

// Computes static minimum resource bounds; exact bounds follow discovered counts.
[[nodiscard]] PreprocessStaticResourcePlan preprocess_static_resource_plan(
    const CountedGraphPreprocessorConfig& config
);

[[nodiscard]] VertexIdMergeDiskPlan vertex_id_merge_disk_plan(
    const InitialRunSummary& initial_runs,
    const VertexIdMergeConfig& config
);

enum class PreprocessDiskPhase {
    initial_runs,
    vertex_id_merge,
};

struct PreprocessDiskAdmission {
    PreprocessDiskPhase phase = PreprocessDiskPhase::initial_runs;
    std::uint64_t logical_successor_bytes = 0U;
    std::uint64_t rounding_slack_bytes = 0U;
    std::uint64_t directory_allowance_bytes = 0U;
    std::uint64_t reserve_bytes = 0U;
    std::uint64_t required_available_bytes = 0U;
    std::uint64_t observed_available_bytes = 0U;
    std::uint64_t required_inodes = 0U;
    std::uint64_t observed_available_inodes = 0U;
    std::uint64_t filesystem_fragment_bytes = 0U;

    friend bool operator==(
        const PreprocessDiskAdmission&,
        const PreprocessDiskAdmission&
    ) = default;
};

class PreprocessInputLimitError final : public std::runtime_error {
public:
    PreprocessInputLimitError(
        std::uint64_t attempted_bytes,
        std::uint64_t limit_bytes
    );

    [[nodiscard]] std::uint64_t attempted_bytes() const noexcept;
    [[nodiscard]] std::uint64_t limit_bytes() const noexcept;

private:
    std::uint64_t attempted_bytes_;
    std::uint64_t limit_bytes_;
};

class PreprocessInputChunkLimitError final : public std::runtime_error {
public:
    PreprocessInputChunkLimitError(
        std::uint64_t chunk_bytes,
        std::uint64_t limit_bytes
    );

    [[nodiscard]] std::uint64_t chunk_bytes() const noexcept;
    [[nodiscard]] std::uint64_t limit_bytes() const noexcept;

private:
    std::uint64_t chunk_bytes_;
    std::uint64_t limit_bytes_;
};

class PreprocessDiskSpaceError final : public std::runtime_error {
public:
    PreprocessDiskSpaceError(
        PreprocessDiskPhase phase,
        std::uint64_t required_bytes,
        std::uint64_t available_bytes
    );

    [[nodiscard]] PreprocessDiskPhase phase() const noexcept;
    [[nodiscard]] std::uint64_t required_bytes() const noexcept;
    [[nodiscard]] std::uint64_t available_bytes() const noexcept;

private:
    PreprocessDiskPhase phase_;
    std::uint64_t required_bytes_;
    std::uint64_t available_bytes_;
};

class PreprocessInodeLimitError final : public std::runtime_error {
public:
    PreprocessInodeLimitError(
        PreprocessDiskPhase phase,
        std::uint64_t required_inodes,
        std::uint64_t available_inodes
    );

    [[nodiscard]] PreprocessDiskPhase phase() const noexcept;
    [[nodiscard]] std::uint64_t required_inodes() const noexcept;
    [[nodiscard]] std::uint64_t available_inodes() const noexcept;

private:
    PreprocessDiskPhase phase_;
    std::uint64_t required_inodes_;
    std::uint64_t available_inodes_;
};

struct CountedGraphPreprocessResources {
    PreprocessStaticResourcePlan static_plan{};
    VertexIdMergeDiskPlan vertex_id_merge_disk{};
    PreprocessDiskAdmission initial_disk_admission{};
    PreprocessDiskAdmission vertex_id_disk_admission{};
    std::uint64_t managed_bulk_upper_bound_bytes = 0U;
    std::uint64_t component_max_open_files = 0U;

    friend bool operator==(
        const CountedGraphPreprocessResources&,
        const CountedGraphPreprocessResources&
    ) = default;
};

enum class WorkspaceRetirementState {
    not_attempted,
    retired,
    failed,
};

struct CountedGraphPreprocessResult {
    std::uint64_t consumed_csv_bytes = 0U;
    InitialRunResult initial_runs{};
    VertexIdMergeResult vertex_ids{};
    CompactEdgeRunResult compact_edges{};
    CompactEdgeReduceResult reduction{};
    CountedGraphBuildResult graph{};
    CountedGraphPreprocessResources resources{};
    WorkspaceRetirementState workspace_retirement =
        WorkspaceRetirementState::not_attempted;
    std::error_code workspace_retirement_error{};
};

// One-shot bounded orchestration. Failures preserve the newest verified recovery state; workspace
// and target parents must stay private and stable.
class CountedGraphPreprocessor final {
public:
    [[nodiscard]] static CountedGraphPreprocessor create(
        const std::filesystem::path& workspace,
        const std::filesystem::path& target,
        CountedGraphPreprocessorConfig config
    );

    CountedGraphPreprocessor(const CountedGraphPreprocessor&) = delete;
    CountedGraphPreprocessor& operator=(const CountedGraphPreprocessor&) =
        delete;
    CountedGraphPreprocessor(CountedGraphPreprocessor&&) noexcept;
    CountedGraphPreprocessor& operator=(CountedGraphPreprocessor&&) noexcept;
    ~CountedGraphPreprocessor();

    void consume(std::span<const char> csv_bytes);
    [[nodiscard]] CountedGraphPreprocessResult finish();
    [[nodiscard]] CountedGraphPreprocessResult finish(
        platform::PublicationBackend& backend
    );

private:
    class Impl;
    explicit CountedGraphPreprocessor(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

}  // namespace tbank::preprocess

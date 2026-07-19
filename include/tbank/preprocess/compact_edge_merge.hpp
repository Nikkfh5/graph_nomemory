#pragma once

#include "tbank/preprocess/compact_edge_runs.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>

namespace tbank::preprocess {

inline constexpr std::size_t kDefaultCompactEdgeMergeFanIn = 32U;
inline constexpr std::size_t kDefaultCompactEdgeMergeReaderBufferBytes =
    64U * 1024U;
inline constexpr std::size_t kDefaultCompactEdgeMergeWriterBufferBytes =
    64U * 1024U;
inline constexpr std::size_t kDefaultCompactEdgeMergeCrcChunkBytes =
    64U * 1024U;

struct CompactEdgeMergeConfig {
    std::size_t fan_in = kDefaultCompactEdgeMergeFanIn;
    std::size_t reader_buffer_bytes =
        kDefaultCompactEdgeMergeReaderBufferBytes;
    std::size_t writer_buffer_bytes =
        kDefaultCompactEdgeMergeWriterBufferBytes;
    std::size_t crc_chunk_bytes = kDefaultCompactEdgeMergeCrcChunkBytes;

    // Limits exclude the caller reserve; reduction needs fan_in readers and one writer.
    std::uint64_t managed_bulk_limit_bytes = 0U;
    std::uint64_t phase_fd_budget = 0U;
    std::uint64_t minimum_free_space_reserve_bytes = 0U;

    friend bool operator==(
        const CompactEdgeMergeConfig&,
        const CompactEdgeMergeConfig&
    ) = default;
};

// Explicit managed storage for a full fan-in group, including the contiguous
// BufferedEdgeCursor object array as well as its separately allocated buffers.
struct CompactEdgeMergeMemoryPlan {
    std::uint64_t reader_buffers_bytes = 0U;
    std::uint64_t writer_buffer_bytes = 0U;
    std::uint64_t cursor_object_bytes = 0U;
    std::uint64_t cursor_objects_bytes = 0U;
    std::uint64_t heap_arrays_bytes = 0U;
    std::uint64_t crc_buffer_bytes = 0U;
    std::uint64_t managed_bulk_upper_bound_bytes = 0U;
    std::uint64_t reduction_max_open_files = 0U;
    std::uint64_t cursor_max_open_files = 0U;

    friend bool operator==(
        const CompactEdgeMergeMemoryPlan&,
        const CompactEdgeMergeMemoryPlan&
    ) = default;
};

[[nodiscard]] CompactEdgeMergeMemoryPlan compact_edge_merge_memory_plan(
    const CompactEdgeMergeConfig& config
);

struct CompactEdgeMergeDiskPlan {
    std::uint64_t input_run_count = 0U;
    std::uint64_t output_run_count = 0U;
    std::uint64_t input_record_count = 0U;
    std::uint64_t vertex_ids_file_bytes = 0U;
    std::uint64_t predecessor_logical_bytes = 0U;
    std::uint64_t successor_logical_upper_bound_bytes = 0U;
    std::uint64_t logical_peak_upper_bound_bytes = 0U;

    friend bool operator==(
        const CompactEdgeMergeDiskPlan&,
        const CompactEdgeMergeDiskPlan&
    ) = default;
};

[[nodiscard]] CompactEdgeMergeDiskPlan compact_edge_merge_disk_plan(
    std::uint64_t vertex_count,
    std::uint64_t input_run_count,
    std::uint64_t input_record_count,
    const CompactEdgeMergeConfig& config
);

struct CompactEdgeMergeFrontier {
    std::uint64_t generation_index = kCompactEdgeInitialGeneration;
    std::uint64_t run_count = 0U;
    std::uint64_t record_count = 0U;
    std::uint64_t initial_record_count = 0U;
    std::uint32_t vertex_count = 0U;

    friend bool operator==(
        const CompactEdgeMergeFrontier&,
        const CompactEdgeMergeFrontier&
    ) = default;
};

class CompactEdgeReductionProgressError final : public std::runtime_error {
public:
    CompactEdgeReductionProgressError(
        CompactEdgeMergeFrontier recovery_frontier,
        std::exception_ptr cause
    );

    [[nodiscard]] CompactEdgeMergeFrontier recovery_frontier() const noexcept;
    [[noreturn]] void rethrow_cause() const;

private:
    CompactEdgeMergeFrontier recovery_frontier_{};
    std::exception_ptr cause_{};
};

// Counters cover this call only; global deduplication comes from frontier totals.
struct CompactEdgeReduceTelemetry {
    std::uint64_t merge_pass_count = 0U;
    std::uint64_t intermediate_run_count = 0U;
    std::uint64_t intermediate_duplicates_removed = 0U;
    std::uint64_t max_successor_logical_upper_bound_bytes = 0U;
    std::size_t peak_open_input_runs = 0U;
    std::size_t peak_heap_items = 0U;
    std::size_t max_writer_peak_buffered_bytes = 0U;

    friend bool operator==(
        const CompactEdgeReduceTelemetry&,
        const CompactEdgeReduceTelemetry&
    ) = default;
};

struct CompactEdgeReduceResult {
    CompactEdgeMergeFrontier frontier{};
    CompactEdgeReduceTelemetry telemetry{};
    CompactEdgeMergeMemoryPlan memory_plan{};

    friend bool operator==(
        const CompactEdgeReduceResult&,
        const CompactEdgeReduceResult&
    ) = default;
};

// Deterministic bounded fan-in reduction. Verifies successors before retiring predecessors;
// failures expose the latest safe frontier. Workspace must remain private.
[[nodiscard]] CompactEdgeReduceResult reduce_compact_edge_runs(
    const std::filesystem::path& workspace,
    std::uint64_t initial_run_count,
    std::uint64_t expected_initial_record_count,
    std::uint64_t expected_vertex_count,
    CompactEdgeMergeConfig config
);

// Reopens and validates the frontier before creating a successor.
[[nodiscard]] CompactEdgeReduceResult reduce_compact_edge_runs(
    const std::filesystem::path& workspace,
    CompactEdgeMergeFrontier starting_frontier,
    CompactEdgeMergeConfig config
);

struct CompactEdgeUniqueSummary {
    std::uint64_t input_record_count = 0U;
    std::uint64_t unique_record_count = 0U;
    std::uint64_t duplicate_record_count = 0U;

    friend bool operator==(
        const CompactEdgeUniqueSummary&,
        const CompactEdgeUniqueSummary&
    ) = default;
};

// Bounded final merge emits unique (destination,source) records in order.
// It never removes its frontier; the graph builder owns retirement.
class UniqueCompactEdgeCursor final {
public:
    [[nodiscard]] static UniqueCompactEdgeCursor open(
        const std::filesystem::path& workspace,
        CompactEdgeMergeFrontier frontier,
        CompactEdgeMergeConfig config
    );

    UniqueCompactEdgeCursor(const UniqueCompactEdgeCursor&) = delete;
    UniqueCompactEdgeCursor& operator=(const UniqueCompactEdgeCursor&) = delete;
    UniqueCompactEdgeCursor(UniqueCompactEdgeCursor&&) noexcept;
    UniqueCompactEdgeCursor& operator=(UniqueCompactEdgeCursor&&) noexcept;
    ~UniqueCompactEdgeCursor();

    [[nodiscard]] std::size_t read(
        std::span<CompactEdgeRecord> destination
    );
    [[nodiscard]] CompactEdgeUniqueSummary summary() const;

private:
    class Impl;
    explicit UniqueCompactEdgeCursor(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

}  // namespace tbank::preprocess

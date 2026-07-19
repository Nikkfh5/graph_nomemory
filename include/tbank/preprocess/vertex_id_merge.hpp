#pragma once

#include "tbank/preprocess/run_file.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace tbank::preprocess {

inline constexpr std::size_t kDefaultVertexIdMergeFanIn = 32U;
inline constexpr std::size_t kDefaultVertexIdMergeBufferBytes = 64U * 1024U;
inline constexpr std::size_t kDefaultVertexIdMergeCrcChunkBytes = 64U * 1024U;

struct VertexIdMergeConfig {
    std::size_t fan_in = kDefaultVertexIdMergeFanIn;
    std::size_t reader_buffer_bytes = kDefaultVertexIdMergeBufferBytes;
    std::size_t writer_buffer_bytes = kDefaultVertexIdMergeBufferBytes;
    std::size_t crc_chunk_bytes = kDefaultVertexIdMergeCrcChunkBytes;

    friend bool operator==(
        const VertexIdMergeConfig&,
        const VertexIdMergeConfig&
    ) = default;
};

// Excludes paths, allocator metadata, and runtime overhead.
struct VertexIdMergeMemoryPlan {
    std::uint64_t reader_buffers_bytes = 0U;
    std::uint64_t writer_buffer_bytes = 0U;
    std::uint64_t cursor_object_bytes = 0U;
    std::uint64_t cursor_objects_bytes = 0U;
    std::uint64_t heap_arrays_bytes = 0U;
    std::uint64_t crc_buffer_bytes = 0U;
    std::uint64_t managed_bulk_upper_bound_bytes = 0U;
    std::uint64_t max_open_files = 0U;

    friend bool operator==(
        const VertexIdMergeMemoryPlan&,
        const VertexIdMergeMemoryPlan&
    ) = default;
};

[[nodiscard]] VertexIdMergeMemoryPlan vertex_id_merge_memory_plan(
    const VertexIdMergeConfig& config
);

class VertexCountLimitError final : public std::runtime_error {
public:
    explicit VertexCountLimitError(std::uint64_t vertex_count);

    [[nodiscard]] std::uint64_t vertex_count() const noexcept;

private:
    std::uint64_t vertex_count_;
};

// Compact IDs are uint32; larger vertex counts are rejected before narrowing.
[[nodiscard]] std::uint32_t checked_compact_vertex_count(
    std::uint64_t vertex_count
);

struct VertexIdMergeTelemetry {
    std::uint64_t merge_pass_count = 0U;
    std::uint64_t intermediate_run_count = 0U;
    std::size_t peak_open_input_runs = 0U;
    std::size_t peak_heap_items = 0U;
    std::size_t max_writer_peak_buffered_bytes = 0U;

    friend bool operator==(
        const VertexIdMergeTelemetry&,
        const VertexIdMergeTelemetry&
    ) = default;
};

struct VertexIdMergeResult {
    std::uint32_t vertex_count = 0U;
    RunFileInfo vertex_ids{};
    VertexIdMergeTelemetry telemetry{};

    friend bool operator==(
        const VertexIdMergeResult&,
        const VertexIdMergeResult&
    ) = default;
};

[[nodiscard]] std::string vertex_id_merge_generation_name(
    std::uint64_t pass_index
);

[[nodiscard]] std::filesystem::path vertex_id_merge_generation_path(
    const std::filesystem::path& workspace,
    std::uint64_t pass_index
);

[[nodiscard]] std::filesystem::path vertex_id_merge_run_path(
    const std::filesystem::path& workspace,
    std::uint64_t pass_index,
    std::uint64_t run_index
);

[[nodiscard]] std::filesystem::path vertex_ids_path(
    const std::filesystem::path& workspace
);

// Produces sorted unique vertex_ids.bin. Verifies successors before retiring known predecessors;
// failures preserve the latest valid generation. Workspace must remain private; counts above
// UINT32_MAX are rejected.
[[nodiscard]] VertexIdMergeResult merge_endpoint_id_runs(
    const std::filesystem::path& workspace,
    std::uint64_t input_run_count,
    VertexIdMergeConfig config = {}
);

}  // namespace tbank::preprocess

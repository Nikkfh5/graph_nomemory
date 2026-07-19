#pragma once

#include "tbank/preprocess/run_file.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <stdexcept>
#include <string>

namespace tbank::preprocess {

inline constexpr storage::BinaryMagic kCompactEdgeRunMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x43}, std::byte{0x45},
    std::byte{0x52}, std::byte{0x30}, std::byte{0x30}, std::byte{0x31},
};

inline constexpr std::uint32_t kCompactEdgeRunRecordBytes = 8U;
inline constexpr std::uint64_t kCompactEdgeInitialGeneration = 0U;
inline constexpr std::size_t kDefaultRawEdgesPerCompactRun = 1U << 20U;
inline constexpr std::size_t kDefaultCompactEdgeReaderBufferBytes =
    64U * 1024U;
inline constexpr std::size_t kDefaultCompactEdgeWriterBufferBytes =
    64U * 1024U;
inline constexpr std::size_t kDefaultCompactEdgeCrcChunkBytes =
    64U * 1024U;
inline constexpr std::uint64_t kCompactEdgeHardMemoryBudgetBytes =
    128U * 1024U * 1024U;

// Temporary records are canonical little-endian (destination,source) pairs.
struct CompactEdgeRecord {
    std::uint32_t destination = 0U;
    std::uint32_t source = 0U;

    friend bool operator==(
        const CompactEdgeRecord&,
        const CompactEdgeRecord&
    ) = default;
};

[[nodiscard]] std::array<std::byte, kCompactEdgeRunRecordBytes>
encode_compact_edge_record(const CompactEdgeRecord& record);

[[nodiscard]] CompactEdgeRecord decode_compact_edge_record(
    std::span<const std::byte> bytes
);

struct CompactEdgeRunConfig {
    std::size_t raw_edges_per_run = kDefaultRawEdgesPerCompactRun;
    std::size_t reader_buffer_bytes =
        kDefaultCompactEdgeReaderBufferBytes;
    std::size_t writer_buffer_bytes =
        kDefaultCompactEdgeWriterBufferBytes;
    std::size_t crc_chunk_bytes = kDefaultCompactEdgeCrcChunkBytes;

    // The explicit limit excludes non-bulk reserve; zero is planning-only.
    std::uint64_t managed_bulk_limit_bytes = 0U;

    std::uint64_t minimum_free_space_reserve_bytes = 0U;

    friend bool operator==(
        const CompactEdgeRunConfig&,
        const CompactEdgeRunConfig&
    ) = default;
};

struct CompactEdgeRunMemoryPlan {
    std::uint64_t dictionary_bytes = 0U;
    std::uint64_t edge_chunk_bytes = 0U;
    std::uint64_t reader_buffer_bytes = 0U;
    std::uint64_t writer_buffer_bytes = 0U;
    std::uint64_t crc_buffer_bytes = 0U;
    std::uint64_t managed_bulk_upper_bound_bytes = 0U;
    std::uint64_t max_open_files = 0U;

    friend bool operator==(
        const CompactEdgeRunMemoryPlan&,
        const CompactEdgeRunMemoryPlan&
    ) = default;
};

[[nodiscard]] CompactEdgeRunMemoryPlan compact_edge_run_memory_plan(
    std::uint64_t vertex_count,
    std::uint64_t raw_edge_count,
    const CompactEdgeRunConfig& config
);

struct CompactEdgeRunDiskPlan {
    std::uint64_t run_count = 0U;
    std::uint64_t raw_input_file_bytes = 0U;
    std::uint64_t vertex_ids_file_bytes = 0U;
    std::uint64_t successor_logical_upper_bound_bytes = 0U;
    std::uint64_t logical_peak_upper_bound_bytes = 0U;

    friend bool operator==(
        const CompactEdgeRunDiskPlan&,
        const CompactEdgeRunDiskPlan&
    ) = default;
};

// Logical file lengths before filesystem block rounding. The successor bound
// assumes no local duplicates and therefore remains safe before remap begins.
[[nodiscard]] CompactEdgeRunDiskPlan compact_edge_run_disk_plan(
    std::uint64_t vertex_count,
    std::uint64_t raw_edge_count,
    const CompactEdgeRunConfig& config
);

class CompactEdgeMemoryLimitError final : public std::runtime_error {
public:
    CompactEdgeMemoryLimitError(
        std::uint64_t required_bytes,
        std::uint64_t limit_bytes
    );

    [[nodiscard]] std::uint64_t required_bytes() const noexcept;
    [[nodiscard]] std::uint64_t limit_bytes() const noexcept;

private:
    std::uint64_t required_bytes_;
    std::uint64_t limit_bytes_;
};

class CompactEdgeDiskSpaceError final : public std::runtime_error {
public:
    CompactEdgeDiskSpaceError(
        std::uint64_t required_bytes,
        std::uint64_t available_bytes
    );

    [[nodiscard]] std::uint64_t required_bytes() const noexcept;
    [[nodiscard]] std::uint64_t available_bytes() const noexcept;

private:
    std::uint64_t required_bytes_;
    std::uint64_t available_bytes_;
};

struct CompactEdgeRunSummary {
    std::uint64_t raw_edge_count = 0U;
    std::uint32_t vertex_count = 0U;
    std::uint64_t run_count = 0U;
    std::uint64_t locally_unique_edge_count = 0U;
    std::uint64_t local_duplicates_removed = 0U;

    friend bool operator==(
        const CompactEdgeRunSummary&,
        const CompactEdgeRunSummary&
    ) = default;
};

struct CompactEdgeRunTelemetry {
    std::size_t peak_chunk_edges = 0U;
    std::size_t max_writer_peak_buffered_bytes = 0U;
    std::uint64_t binary_search_lookups = 0U;

    friend bool operator==(
        const CompactEdgeRunTelemetry&,
        const CompactEdgeRunTelemetry&
    ) = default;
};

struct CompactEdgeRunResult {
    CompactEdgeRunSummary summary{};
    CompactEdgeRunTelemetry telemetry{};
    CompactEdgeRunMemoryPlan memory_plan{};
    CompactEdgeRunDiskPlan disk_plan{};

    friend bool operator==(
        const CompactEdgeRunResult&,
        const CompactEdgeRunResult&
    ) = default;
};

[[nodiscard]] std::string compact_edge_generation_name(
    std::uint64_t generation_index
);

[[nodiscard]] std::filesystem::path compact_edge_generation_path(
    const std::filesystem::path& workspace,
    std::uint64_t generation_index
);

[[nodiscard]] std::string compact_edge_run_name(std::uint64_t run_index);

[[nodiscard]] std::filesystem::path compact_edge_run_path(
    const std::filesystem::path& workspace,
    std::uint64_t generation_index,
    std::uint64_t run_index
);

// Admits memory before loading IDs and leaves cross-run duplicates for final merge. Verifies
// successors before retiring raw input; workspace must remain private.
[[nodiscard]] CompactEdgeRunResult build_compact_edge_runs(
    const std::filesystem::path& workspace,
    std::uint64_t expected_raw_edge_count,
    std::uint64_t expected_vertex_count,
    CompactEdgeRunConfig config
);

}  // namespace tbank::preprocess

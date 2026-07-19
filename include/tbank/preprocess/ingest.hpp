#pragma once

#include "tbank/storage/binary.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>

namespace tbank::preprocess {

inline constexpr storage::BinaryMagic kRawEdgeRunMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x52}, std::byte{0x41},
    std::byte{0x57}, std::byte{0x30}, std::byte{0x30}, std::byte{0x31},
};
inline constexpr storage::BinaryMagic kEndpointIdRunMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x49}, std::byte{0x44},
    std::byte{0x52}, std::byte{0x30}, std::byte{0x30}, std::byte{0x31},
};

inline constexpr std::uint32_t kRawEdgeRunRecordBytes = 8U;
inline constexpr std::uint32_t kEndpointIdRunRecordBytes = 4U;
inline constexpr std::size_t kDefaultEndpointIdsPerRun = 1U << 20U;
inline constexpr std::size_t kDefaultRunWriterBufferBytes = 64U * 1024U;

struct RawEdgeRecord {
    std::int32_t source = 0;
    std::int32_t destination = 0;

    friend bool operator==(const RawEdgeRecord&, const RawEdgeRecord&) = default;
};

[[nodiscard]] std::array<std::byte, kRawEdgeRunRecordBytes>
encode_raw_edge_record(const RawEdgeRecord& record);

[[nodiscard]] RawEdgeRecord decode_raw_edge_record(
    std::span<const std::byte> bytes
);

struct InitialRunConfig {
    std::size_t endpoint_ids_per_run = kDefaultEndpointIdsPerRun;
    std::size_t writer_buffer_bytes = kDefaultRunWriterBufferBytes;

    friend bool operator==(const InitialRunConfig&, const InitialRunConfig&) =
        default;
};

// Counts only endpoint and writer buffers; other RSS belongs to full admission.
struct InitialRunMemoryPlan {
    std::uint64_t endpoint_buffer_bytes = 0U;
    std::uint64_t writer_buffers_bytes = 0U;
    std::uint64_t managed_peak_bytes = 0U;

    friend bool operator==(
        const InitialRunMemoryPlan&,
        const InitialRunMemoryPlan&
    ) = default;
};

[[nodiscard]] InitialRunMemoryPlan initial_run_memory_plan(
    const InitialRunConfig& config
);

struct InitialRunSummary {
    std::uint64_t raw_edge_count = 0U;
    std::uint64_t endpoint_ids_seen = 0U;
    std::uint64_t endpoint_run_count = 0U;
    std::uint64_t endpoint_run_records = 0U;

    friend bool operator==(const InitialRunSummary&, const InitialRunSummary&) =
        default;
};

struct InitialRunTelemetry {
    std::size_t peak_endpoint_ids = 0U;
    std::size_t raw_writer_peak_buffered_bytes = 0U;
    std::size_t max_endpoint_writer_peak_buffered_bytes = 0U;

    friend bool operator==(
        const InitialRunTelemetry&,
        const InitialRunTelemetry&
    ) = default;
};

struct InitialRunResult {
    InitialRunSummary summary{};
    InitialRunTelemetry telemetry{};

    friend bool operator==(const InitialRunResult&, const InitialRunResult&) =
        default;
};

[[nodiscard]] std::filesystem::path raw_edge_run_path(
    const std::filesystem::path& workspace
);

[[nodiscard]] std::string endpoint_id_run_name(std::uint64_t run_index);

[[nodiscard]] std::filesystem::path endpoint_id_run_path(
    const std::filesystem::path& workspace,
    std::uint64_t run_index
);

// Streams strict CSV into bounded private run storage. Sticky failures clean owned incomplete
// files; success keeps the workspace. Its parent must remain trusted.
class InitialRunIngestor final {
public:
    [[nodiscard]] static InitialRunIngestor create(
        const std::filesystem::path& workspace,
        InitialRunConfig config = {}
    );

    InitialRunIngestor(const InitialRunIngestor&) = delete;
    InitialRunIngestor& operator=(const InitialRunIngestor&) = delete;
    InitialRunIngestor(InitialRunIngestor&&) noexcept;
    InitialRunIngestor& operator=(InitialRunIngestor&&) noexcept;
    ~InitialRunIngestor();

    void consume(std::span<const char> csv_bytes);
    [[nodiscard]] InitialRunResult finish();

private:
    class Impl;
    explicit InitialRunIngestor(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

}  // namespace tbank::preprocess

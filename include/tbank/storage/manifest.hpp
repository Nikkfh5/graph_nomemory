#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace tbank::storage {

// A canonical v1 manifest is currently below 2 KiB even when every integer is
// printed at its maximum width. The larger public limit leaves room for clear
// diagnostics while bounding parser work before schema/version validation.
inline constexpr std::size_t kMaxManifestBytes = 16U * 1024U;
inline constexpr std::uint32_t kManifestSchemaVersion = 1U;
inline constexpr std::uint64_t kManifestBinaryHeaderBytes = 48U;
inline constexpr std::size_t kManifestFileCount = 5U;

class ManifestError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

enum class ManifestFileKind : std::uint8_t {
    vertex_ids,
    incoming_sources,
    incoming_counts,
    out_degree,
    tasks,
};

[[nodiscard]] std::string_view manifest_file_name(
    ManifestFileKind kind
) noexcept;
[[nodiscard]] std::string_view manifest_file_magic(
    ManifestFileKind kind
) noexcept;
[[nodiscard]] std::uint32_t manifest_record_bytes(
    ManifestFileKind kind
) noexcept;

struct ManifestFileDescriptor {
    ManifestFileKind kind = ManifestFileKind::vertex_ids;
    std::uint32_t record_bytes = 0U;
    std::uint64_t record_count = 0U;
    std::uint64_t payload_bytes = 0U;
    std::uint64_t file_bytes = 0U;
    std::uint64_t crc64 = 0U;

    friend bool operator==(
        const ManifestFileDescriptor&,
        const ManifestFileDescriptor&
    ) = default;
};

struct GraphManifest {
    std::uint32_t schema_version = kManifestSchemaVersion;
    std::uint32_t vertex_count = 0U;
    std::uint64_t edge_count = 0U;
    std::uint64_t edge_slice_size = 0U;
    std::uint64_t max_task_edges = 0U;
    std::uint32_t max_task_vertices = 0U;
    std::array<ManifestFileDescriptor, kManifestFileCount> files{};

    friend bool operator==(const GraphManifest&, const GraphManifest&) = default;
};

// Validates typed manifest invariants but does not inspect files or recompute CRC.
void validate_manifest(const GraphManifest& manifest);

// Produces exactly one ASCII JSON line with no insignificant whitespace and a
// final LF. Invalid typed values are rejected rather than normalized.
[[nodiscard]] std::string encode_manifest(const GraphManifest& manifest);

// Accepts only the byte-for-byte canonical v1 grammar. Unknown, missing,
// duplicate, reordered, escaped, or non-canonical fields are rejected. Input
// larger than kMaxManifestBytes is rejected before parsing.
[[nodiscard]] GraphManifest parse_manifest(std::string_view bytes);

}  // namespace tbank::storage

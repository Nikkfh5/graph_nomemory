#pragma once

#include "tbank/storage/file_reader.hpp"
#include "tbank/storage/manifest.hpp"

#include <cstddef>
#include <filesystem>
#include <functional>
#include <stdexcept>

namespace tbank::storage {

class GraphValidationError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct GraphValidationOptions {
    // Each sequential cursor uses one bounded buffer and assembles split records.
    std::size_t io_chunk_bytes = kDefaultCrcChunkBytes;

    friend bool operator==(
        const GraphValidationOptions&,
        const GraphValidationOptions&
    ) = default;
};

// Early admission callback runs after manifest validation but before payload scans.
using GraphManifestPreflight = std::function<void(const GraphManifest&)>;

// Opens and validates the complete five-file graph without retaining O(V), O(E) or O(Q).
// The published directory must remain immutable for all reader lifetimes.
// Validation proves global degree totals and task coverage, not per-source reconstruction.
class ValidatedGraph final {
public:
    [[nodiscard]] static ValidatedGraph open(
        const std::filesystem::path& directory,
        GraphValidationOptions options = {},
        const GraphManifestPreflight& manifest_preflight = {}
    );

    ValidatedGraph(const ValidatedGraph&) = delete;
    ValidatedGraph& operator=(const ValidatedGraph&) = delete;
    ValidatedGraph(ValidatedGraph&&) noexcept = default;
    ValidatedGraph& operator=(ValidatedGraph&&) noexcept = default;
    ~ValidatedGraph() = default;

    [[nodiscard]] const GraphManifest& manifest() const noexcept;
    [[nodiscard]] const ValidatedBinaryFileReader& vertex_ids() const noexcept;
    [[nodiscard]] const ValidatedBinaryFileReader& incoming_sources() const noexcept;
    [[nodiscard]] const ValidatedBinaryFileReader& incoming_counts() const noexcept;
    [[nodiscard]] const ValidatedBinaryFileReader& out_degree() const noexcept;
    [[nodiscard]] const ValidatedBinaryFileReader& tasks() const noexcept;

private:
    ValidatedGraph(
        GraphManifest manifest,
        ValidatedBinaryFileReader vertex_ids,
        ValidatedBinaryFileReader incoming_sources,
        ValidatedBinaryFileReader incoming_counts,
        ValidatedBinaryFileReader out_degree,
        ValidatedBinaryFileReader tasks
    );

    GraphManifest manifest_;
    ValidatedBinaryFileReader vertex_ids_;
    ValidatedBinaryFileReader incoming_sources_;
    ValidatedBinaryFileReader incoming_counts_;
    ValidatedBinaryFileReader out_degree_;
    ValidatedBinaryFileReader tasks_;
};

}  // namespace tbank::storage

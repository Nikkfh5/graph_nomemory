#include "cli_common.hpp"
#include "sha256.hpp"

#include "tbank/io/edge_csv.hpp"
#include "tbank/platform/checked_io.hpp"
#include "tbank/platform/publication.hpp"
#include "tbank/preprocess/prepare.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <locale>
#include <new>
#include <optional>
#include <sstream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

using tbank::cli::ParsedOptions;
using tbank::cli::UsageError;
using tbank::platform::PublicationState;
using tbank::platform::PublicationStep;
using tbank::preprocess::CountedGraphPreprocessResult;
using tbank::preprocess::CountedGraphPreprocessor;
using tbank::preprocess::CountedGraphPreprocessorConfig;
using tbank::preprocess::PredecessorRetirementState;
using tbank::preprocess::WorkspaceRetirementState;

constexpr std::array<std::string_view, 26U> kAllowedOptions{
    "input-csv",
    "input-sha256",
    "workspace",
    "graph-dir",
    "non-bulk-reserve-bytes",
    "input-chunk-bytes",
    "phase-fd-budget",
    "disk-reserve-bytes",
    "edge-slice-size",
    "max-task-edges",
    "max-task-vertices",
    "endpoint-ids-per-run",
    "run-writer-buffer-bytes",
    "vertex-merge-fan-in",
    "vertex-merge-reader-buffer-bytes",
    "vertex-merge-writer-buffer-bytes",
    "vertex-merge-crc-chunk-bytes",
    "raw-edges-per-run",
    "compact-reader-buffer-bytes",
    "compact-writer-buffer-bytes",
    "compact-crc-chunk-bytes",
    "edge-merge-fan-in",
    "edge-merge-reader-buffer-bytes",
    "edge-merge-writer-buffer-bytes",
    "edge-merge-crc-chunk-bytes",
    "edge-batch-records",
};

// The graph validation chunk is deliberately separate from the merge CRC
// chunks. Keep it outside the fixed array above only to make an accidental
// option-count change a compile error at the declaration site below.
constexpr std::string_view kGraphValidationOption =
    "graph-validation-chunk-bytes";

constexpr std::string_view kHelp =
    "Usage: tbank-prepare [options]\n"
    "\n"
    "Required:\n"
    "  --input-csv PATH\n"
    "  --input-sha256 LOWERCASE_HEX\n"
    "  --workspace PATH\n"
    "  --graph-dir PATH\n"
    "  --non-bulk-reserve-bytes UINT\n"
    "  --input-chunk-bytes UINT\n"
    "  --phase-fd-budget UINT\n"
    "  --disk-reserve-bytes UINT\n"
    "  --edge-slice-size UINT\n"
    "  --max-task-edges UINT\n"
    "  --max-task-vertices UINT\n"
    "\n"
    "Optional bounded preprocessing overrides:\n"
    "  --endpoint-ids-per-run UINT\n"
    "  --run-writer-buffer-bytes UINT\n"
    "  --vertex-merge-fan-in UINT\n"
    "  --vertex-merge-reader-buffer-bytes UINT\n"
    "  --vertex-merge-writer-buffer-bytes UINT\n"
    "  --vertex-merge-crc-chunk-bytes UINT\n"
    "  --raw-edges-per-run UINT\n"
    "  --compact-reader-buffer-bytes UINT\n"
    "  --compact-writer-buffer-bytes UINT\n"
    "  --compact-crc-chunk-bytes UINT\n"
    "  --edge-merge-fan-in UINT\n"
    "  --edge-merge-reader-buffer-bytes UINT\n"
    "  --edge-merge-writer-buffer-bytes UINT\n"
    "  --edge-merge-crc-chunk-bytes UINT\n"
    "  --edge-batch-records UINT\n"
    "  --graph-validation-chunk-bytes UINT\n"
    "\n"
    "All UINT values use canonical unsigned decimal spelling. SHA-256 is\n"
    "exactly 64 lowercase hex characters. The input must be one stable regular\n"
    "file and is opened without following a symlink.\n";

class InputDigestMismatchError final : public std::runtime_error {
public:
    InputDigestMismatchError(std::string expected, std::string observed)
        : std::runtime_error(
              "prepare input SHA-256 mismatch: expected " + expected
              + ", observed " + observed
          ) {}
};

[[noreturn]] void throw_posix_error(
    const int error_number,
    const char* const operation
) {
    throw std::system_error(
        error_number, std::generic_category(), operation
    );
}

class FileDescriptor final {
public:
    explicit FileDescriptor(const std::filesystem::path& path) {
        do {
            errno = 0;
            descriptor_ = ::open(
                path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW
            );
        } while (descriptor_ == -1 && errno == EINTR);
        if (descriptor_ == -1) {
            throw_posix_error(errno, "open prepare input");
        }
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    ~FileDescriptor() noexcept {
        if (descriptor_ != -1) {
            static_cast<void>(::close(descriptor_));
        }
    }

    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }

private:
    int descriptor_ = -1;
};

[[nodiscard]] bool path_entry_exists(
    const std::filesystem::path& path
) {
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(path.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == 0) {
        return true;
    }
    if (errno == ENOENT) {
        return false;
    }
    throw_posix_error(errno, "inspect prepare graph target");
}

[[nodiscard]] tbank::checksum::Sha256Digest parse_input_digest(
    const ParsedOptions& options
) {
    try {
        return tbank::checksum::parse_sha256_lower_hex(
            options.require("input-sha256")
        );
    } catch (const std::invalid_argument& error) {
        throw UsageError(
            std::string("option --input-sha256: ") + error.what()
        );
    }
}

[[nodiscard]] std::filesystem::path path_option(
    const ParsedOptions& options,
    const std::string_view name
) {
    const std::string_view value = options.require(name);
    tbank::cli::require_valid_utf8(name, value);
    return std::filesystem::path(std::string(value));
}

[[nodiscard]] std::filesystem::path resolved_child_path(
    const std::filesystem::path& path
) {
    const std::filesystem::path absolute =
        std::filesystem::absolute(path).lexically_normal();
    if (absolute.filename().empty()) {
        throw std::invalid_argument("CLI path has an empty final component");
    }
    std::filesystem::path resolved =
        std::filesystem::canonical(absolute.parent_path())
        / absolute.filename();
    tbank::cli::require_valid_utf8("resolved-path", resolved.native());
    return resolved;
}

[[nodiscard]] struct stat inspect_input(const int descriptor) {
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::fstat(descriptor, &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        throw_posix_error(errno, "fstat prepare input");
    }
    if (!S_ISREG(status.st_mode)) {
        throw std::invalid_argument("prepare input must be a regular file");
    }
    if (status.st_size < 0) {
        throw std::runtime_error("prepare input has a negative file size");
    }
    return status;
}

[[nodiscard]] bool same_input_snapshot(
    const struct stat& before,
    const struct stat& after
) noexcept {
    return before.st_dev == after.st_dev
        && before.st_ino == after.st_ino
        && before.st_mode == after.st_mode
        && before.st_nlink == after.st_nlink
        && before.st_size == after.st_size
        && before.st_mtim.tv_sec == after.st_mtim.tv_sec
        && before.st_mtim.tv_nsec == after.st_mtim.tv_nsec
        && before.st_ctim.tv_sec == after.st_ctim.tv_sec
        && before.st_ctim.tv_nsec == after.st_ctim.tv_nsec;
}

void require_no_input_growth(
    const int descriptor,
    const std::uint64_t expected_size
) {
    std::byte probe{};
    ssize_t result = -1;
    do {
        errno = 0;
        result = ::pread(
            descriptor,
            &probe,
            1U,
            tbank::platform::checked_u64_to_off_t(expected_size)
        );
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        throw_posix_error(errno, "probe prepare input EOF");
    }
    if (result != 0) {
        throw std::runtime_error(
            "prepare input grew while it was being consumed"
        );
    }
}

[[nodiscard]] std::uint32_t parse_u32_option(
    const std::string_view name,
    const std::string_view value
) {
    const std::uint64_t parsed = tbank::cli::parse_u64(name, value);
    if (parsed > std::numeric_limits<std::uint32_t>::max()) {
        throw UsageError(
            "option --" + std::string(name) + ": value is outside uint32 range"
        );
    }
    return static_cast<std::uint32_t>(parsed);
}

template <class Destination>
void assign_optional_size(
    const ParsedOptions& options,
    const std::string_view name,
    Destination& destination
) {
    const std::optional<std::string_view> value = options.optional(name);
    if (value.has_value()) {
        destination = static_cast<Destination>(
            tbank::cli::parse_size(name, *value)
        );
    }
}

[[nodiscard]] CountedGraphPreprocessorConfig make_config(
    const ParsedOptions& options
) {
    CountedGraphPreprocessorConfig config{};
    config.resources.non_bulk_reserve_bytes = tbank::cli::parse_u64(
        "non-bulk-reserve-bytes",
        options.require("non-bulk-reserve-bytes")
    );
    config.resources.maximum_input_chunk_bytes = tbank::cli::parse_u64(
        "input-chunk-bytes", options.require("input-chunk-bytes")
    );
    config.resources.phase_fd_budget = tbank::cli::parse_u64(
        "phase-fd-budget", options.require("phase-fd-budget")
    );
    config.resources.minimum_free_space_reserve_bytes = tbank::cli::parse_u64(
        "disk-reserve-bytes", options.require("disk-reserve-bytes")
    );
    config.counted_graph.tasks.edge_slice_size = tbank::cli::parse_u64(
        "edge-slice-size", options.require("edge-slice-size")
    );
    config.counted_graph.tasks.max_task_edges = tbank::cli::parse_u64(
        "max-task-edges", options.require("max-task-edges")
    );
    config.counted_graph.tasks.max_task_vertices = parse_u32_option(
        "max-task-vertices", options.require("max-task-vertices")
    );

    assign_optional_size(
        options,
        "endpoint-ids-per-run",
        config.initial_runs.endpoint_ids_per_run
    );
    assign_optional_size(
        options,
        "run-writer-buffer-bytes",
        config.initial_runs.writer_buffer_bytes
    );
    assign_optional_size(
        options, "vertex-merge-fan-in", config.vertex_ids.fan_in
    );
    assign_optional_size(
        options,
        "vertex-merge-reader-buffer-bytes",
        config.vertex_ids.reader_buffer_bytes
    );
    assign_optional_size(
        options,
        "vertex-merge-writer-buffer-bytes",
        config.vertex_ids.writer_buffer_bytes
    );
    assign_optional_size(
        options,
        "vertex-merge-crc-chunk-bytes",
        config.vertex_ids.crc_chunk_bytes
    );
    assign_optional_size(
        options,
        "raw-edges-per-run",
        config.compact_edges.raw_edges_per_run
    );
    assign_optional_size(
        options,
        "compact-reader-buffer-bytes",
        config.compact_edges.reader_buffer_bytes
    );
    assign_optional_size(
        options,
        "compact-writer-buffer-bytes",
        config.compact_edges.writer_buffer_bytes
    );
    assign_optional_size(
        options,
        "compact-crc-chunk-bytes",
        config.compact_edges.crc_chunk_bytes
    );
    assign_optional_size(
        options,
        "edge-merge-fan-in",
        config.counted_graph.merge.fan_in
    );
    assign_optional_size(
        options,
        "edge-merge-reader-buffer-bytes",
        config.counted_graph.merge.reader_buffer_bytes
    );
    assign_optional_size(
        options,
        "edge-merge-writer-buffer-bytes",
        config.counted_graph.merge.writer_buffer_bytes
    );
    assign_optional_size(
        options,
        "edge-merge-crc-chunk-bytes",
        config.counted_graph.merge.crc_chunk_bytes
    );
    assign_optional_size(
        options,
        "edge-batch-records",
        config.counted_graph.edge_batch_records
    );
    assign_optional_size(
        options,
        kGraphValidationOption,
        config.counted_graph.validation_io_chunk_bytes
    );

    // Validate every caller-selected shape before creating a workspace. The
    // shortest possible CSV limit is sufficient for this static admission;
    // the exact regular-file length is installed immediately before create().
    config.csv_byte_limit = 11U;
    try {
        static_cast<void>(
            tbank::preprocess::preprocess_static_resource_plan(config)
        );
    } catch (const std::exception& error) {
        throw UsageError(
            std::string("invalid prepare configuration: ") + error.what()
        );
    }
    return config;
}

[[nodiscard]] std::string_view publication_state_name(
    const PublicationState state
) noexcept {
    switch (state) {
        case PublicationState::published:
            return "published";
        case PublicationState::not_published:
            return "not_published";
        case PublicationState::target_exists:
            return "target_exists";
        case PublicationState::no_replace_unsupported:
            return "no_replace_unsupported";
        case PublicationState::durability_uncertain:
            return "durability_uncertain";
    }
    return "unknown";
}

[[nodiscard]] std::string_view publication_step_name(
    const PublicationStep step
) noexcept {
    switch (step) {
        case PublicationStep::none:
            return "none";
        case PublicationStep::validate_paths:
            return "validate_paths";
        case PublicationStep::open_parent_directory:
            return "open_parent_directory";
        case PublicationStep::create_temporary_file:
            return "create_temporary_file";
        case PublicationStep::write_temporary_file:
            return "write_temporary_file";
        case PublicationStep::sync_temporary_file:
            return "sync_temporary_file";
        case PublicationStep::close_temporary_file:
            return "close_temporary_file";
        case PublicationStep::open_staging_directory:
            return "open_staging_directory";
        case PublicationStep::sync_staging_directory:
            return "sync_staging_directory";
        case PublicationStep::close_staging_directory:
            return "close_staging_directory";
        case PublicationStep::rename_no_replace:
            return "rename_no_replace";
        case PublicationStep::sync_parent_directory:
            return "sync_parent_directory";
    }
    return "unknown";
}

[[nodiscard]] std::string_view workspace_state_name(
    const WorkspaceRetirementState state
) noexcept {
    switch (state) {
        case WorkspaceRetirementState::not_attempted:
            return "not_attempted";
        case WorkspaceRetirementState::retired:
            return "retired";
        case WorkspaceRetirementState::failed:
            return "failed";
    }
    return "unknown";
}

[[nodiscard]] std::string_view predecessor_state_name(
    const PredecessorRetirementState state
) noexcept {
    switch (state) {
        case PredecessorRetirementState::not_attempted:
            return "not_attempted";
        case PredecessorRetirementState::retired:
            return "retired";
        case PredecessorRetirementState::failed:
            return "failed";
    }
    return "unknown";
}

[[nodiscard]] bool cleanup_complete(
    const CountedGraphPreprocessResult& result
) noexcept {
    return result.graph.frontier_retirement
            == PredecessorRetirementState::retired
        && result.graph.vertex_ids_retirement
            == PredecessorRetirementState::retired
        && result.workspace_retirement == WorkspaceRetirementState::retired;
}

void write_result_json(
    const CountedGraphPreprocessResult& result,
    const CountedGraphPreprocessorConfig& config,
    const std::filesystem::path& requested_workspace,
    const std::filesystem::path& requested_target,
    const std::filesystem::path& resolved_workspace,
    const std::filesystem::path& resolved_target,
    const std::string_view input_sha256
) {
    const auto& publication = result.graph.publication;
    if (result.initial_runs.summary.raw_edge_count
        < result.graph.edges.unique_record_count) {
        throw std::logic_error(
            "prepare result has more unique edges than raw rows"
        );
    }
    const std::uint64_t total_duplicates =
        result.initial_runs.summary.raw_edge_count
        - result.graph.edges.unique_record_count;
    const std::uint64_t local_duplicates =
        result.compact_edges.summary.local_duplicates_removed;
    const std::uint64_t cross_run_duplicates =
        result.graph.edges.duplicate_record_count;
    if (local_duplicates > total_duplicates
        || cross_run_duplicates != total_duplicates - local_duplicates) {
        throw std::logic_error(
            "prepare duplicate accounting does not reconcile"
        );
    }

    const auto write_error_code = [](std::ostream& output,
                                     const std::error_code& error) {
        output
            << "{\"value\":" << error.value()
            << ",\"category\":"
            << tbank::cli::json_quote(error.category().name())
            << ",\"message\":"
            << tbank::cli::json_quote(error.message()) << '}';
    };
    const auto write_initial_memory = [](std::ostream& output,
                                         const auto& plan) {
        output
            << "{\"endpoint_buffer_bytes\":" << plan.endpoint_buffer_bytes
            << ",\"writer_buffers_bytes\":" << plan.writer_buffers_bytes
            << ",\"managed_peak_bytes\":" << plan.managed_peak_bytes << '}';
    };
    const auto write_vertex_memory = [](std::ostream& output,
                                        const auto& plan) {
        output
            << "{\"reader_buffers_bytes\":" << plan.reader_buffers_bytes
            << ",\"writer_buffer_bytes\":" << plan.writer_buffer_bytes
            << ",\"cursor_object_bytes\":" << plan.cursor_object_bytes
            << ",\"cursor_objects_bytes\":" << plan.cursor_objects_bytes
            << ",\"heap_arrays_bytes\":" << plan.heap_arrays_bytes
            << ",\"crc_buffer_bytes\":" << plan.crc_buffer_bytes
            << ",\"managed_bulk_upper_bound_bytes\":"
            << plan.managed_bulk_upper_bound_bytes
            << ",\"max_open_files\":" << plan.max_open_files << '}';
    };
    const auto write_compact_memory = [](std::ostream& output,
                                         const auto& plan) {
        output
            << "{\"dictionary_bytes\":" << plan.dictionary_bytes
            << ",\"edge_chunk_bytes\":" << plan.edge_chunk_bytes
            << ",\"reader_buffer_bytes\":" << plan.reader_buffer_bytes
            << ",\"writer_buffer_bytes\":" << plan.writer_buffer_bytes
            << ",\"crc_buffer_bytes\":" << plan.crc_buffer_bytes
            << ",\"managed_bulk_upper_bound_bytes\":"
            << plan.managed_bulk_upper_bound_bytes
            << ",\"max_open_files\":" << plan.max_open_files << '}';
    };
    const auto write_merge_memory = [](std::ostream& output,
                                       const auto& plan) {
        output
            << "{\"reader_buffers_bytes\":" << plan.reader_buffers_bytes
            << ",\"writer_buffer_bytes\":" << plan.writer_buffer_bytes
            << ",\"cursor_object_bytes\":" << plan.cursor_object_bytes
            << ",\"cursor_objects_bytes\":" << plan.cursor_objects_bytes
            << ",\"heap_arrays_bytes\":" << plan.heap_arrays_bytes
            << ",\"crc_buffer_bytes\":" << plan.crc_buffer_bytes
            << ",\"managed_bulk_upper_bound_bytes\":"
            << plan.managed_bulk_upper_bound_bytes
            << ",\"reduction_max_open_files\":"
            << plan.reduction_max_open_files
            << ",\"cursor_max_open_files\":"
            << plan.cursor_max_open_files << '}';
    };
    const auto write_counted_resources = [](std::ostream& output,
                                            const auto& plan) {
        output
            << "{\"vertex_count\":" << plan.vertex_count
            << ",\"frontier_run_count\":" << plan.frontier_run_count
            << ",\"frontier_record_count\":" << plan.frontier_record_count
            << ",\"edge_count_upper_bound\":"
            << plan.edge_count_upper_bound
            << ",\"task_count_upper_bound\":"
            << plan.task_count_upper_bound
            << ",\"out_degree_bytes\":" << plan.out_degree_bytes
            << ",\"edge_batch_bytes\":" << plan.edge_batch_bytes
            << ",\"source_batch_bytes\":" << plan.source_batch_bytes
            << ",\"cursor_reader_buffers_bytes\":"
            << plan.cursor_reader_buffers_bytes
            << ",\"cursor_objects_bytes\":" << plan.cursor_objects_bytes
            << ",\"cursor_heap_arrays_bytes\":"
            << plan.cursor_heap_arrays_bytes
            << ",\"output_writer_buffers_bytes\":"
            << plan.output_writer_buffers_bytes
            << ",\"merge_phase_upper_bound_bytes\":"
            << plan.merge_phase_upper_bound_bytes
            << ",\"out_degree_write_phase_upper_bound_bytes\":"
            << plan.out_degree_write_phase_upper_bound_bytes
            << ",\"vertex_copy_phase_upper_bound_bytes\":"
            << plan.vertex_copy_phase_upper_bound_bytes
            << ",\"validation_phase_upper_bound_bytes\":"
            << plan.validation_phase_upper_bound_bytes
            << ",\"cursor_policy_floor_bytes\":"
            << plan.cursor_policy_floor_bytes
            << ",\"managed_bulk_upper_bound_bytes\":"
            << plan.managed_bulk_upper_bound_bytes
            << ",\"max_open_files\":" << plan.max_open_files
            << ",\"predecessor_frontier_logical_bytes\":"
            << plan.predecessor_frontier_logical_bytes
            << ",\"predecessor_vertex_ids_logical_bytes\":"
            << plan.predecessor_vertex_ids_logical_bytes
            << ",\"staging_logical_upper_bound_bytes\":"
            << plan.staging_logical_upper_bound_bytes
            << ",\"logical_peak_upper_bound_bytes\":"
            << plan.logical_peak_upper_bound_bytes << '}';
    };
    const auto write_initial_disk = [](std::ostream& output,
                                       const auto& plan) {
        output
            << "{\"csv_byte_limit\":" << plan.csv_byte_limit
            << ",\"raw_edge_count_upper_bound\":"
            << plan.raw_edge_count_upper_bound
            << ",\"endpoint_run_count_upper_bound\":"
            << plan.endpoint_run_count_upper_bound
            << ",\"raw_edge_file_bytes\":" << plan.raw_edge_file_bytes
            << ",\"endpoint_run_files_bytes_upper_bound\":"
            << plan.endpoint_run_files_bytes_upper_bound
            << ",\"successor_logical_upper_bound_bytes\":"
            << plan.successor_logical_upper_bound_bytes
            << ",\"successor_file_count_upper_bound\":"
            << plan.successor_file_count_upper_bound
            << ",\"successor_entry_count_upper_bound\":"
            << plan.successor_entry_count_upper_bound << '}';
    };
    const auto write_vertex_disk = [](std::ostream& output,
                                      const auto& plan) {
        output
            << "{\"raw_edge_count\":" << plan.raw_edge_count
            << ",\"input_run_count\":" << plan.input_run_count
            << ",\"input_record_count\":" << plan.input_record_count
            << ",\"successor_run_count_upper_bound\":"
            << plan.successor_run_count_upper_bound
            << ",\"raw_edge_file_bytes\":" << plan.raw_edge_file_bytes
            << ",\"predecessor_run_files_bytes\":"
            << plan.predecessor_run_files_bytes
            << ",\"successor_logical_upper_bound_bytes\":"
            << plan.successor_logical_upper_bound_bytes
            << ",\"logical_peak_upper_bound_bytes\":"
            << plan.logical_peak_upper_bound_bytes
            << ",\"successor_file_count_upper_bound\":"
            << plan.successor_file_count_upper_bound
            << ",\"successor_entry_count_upper_bound\":"
            << plan.successor_entry_count_upper_bound << '}';
    };
    const auto write_admission = [](std::ostream& output, const auto& plan) {
        const std::string_view phase = plan.phase
                == tbank::preprocess::PreprocessDiskPhase::initial_runs
            ? "initial_runs"
            : "vertex_id_merge";
        output
            << "{\"phase\":" << tbank::cli::json_quote(phase)
            << ",\"logical_successor_bytes\":"
            << plan.logical_successor_bytes
            << ",\"rounding_slack_bytes\":" << plan.rounding_slack_bytes
            << ",\"directory_allowance_bytes\":"
            << plan.directory_allowance_bytes
            << ",\"reserve_bytes\":" << plan.reserve_bytes
            << ",\"required_available_bytes\":"
            << plan.required_available_bytes
            << ",\"observed_available_bytes\":"
            << plan.observed_available_bytes
            << ",\"required_inodes\":" << plan.required_inodes
            << ",\"observed_available_inodes\":"
            << plan.observed_available_inodes
            << ",\"filesystem_fragment_bytes\":"
            << plan.filesystem_fragment_bytes << '}';
    };
    const auto write_compact_disk = [](std::ostream& output,
                                       const auto& plan) {
        output
            << "{\"run_count\":" << plan.run_count
            << ",\"raw_input_file_bytes\":" << plan.raw_input_file_bytes
            << ",\"vertex_ids_file_bytes\":"
            << plan.vertex_ids_file_bytes
            << ",\"successor_logical_upper_bound_bytes\":"
            << plan.successor_logical_upper_bound_bytes
            << ",\"logical_peak_upper_bound_bytes\":"
            << plan.logical_peak_upper_bound_bytes << '}';
    };
    const auto write_reduction_telemetry = [](std::ostream& output,
                                              const auto& telemetry) {
        output
            << "{\"merge_pass_count\":" << telemetry.merge_pass_count
            << ",\"intermediate_run_count\":"
            << telemetry.intermediate_run_count
            << ",\"intermediate_duplicates_removed\":"
            << telemetry.intermediate_duplicates_removed
            << ",\"max_successor_logical_upper_bound_bytes\":"
            << telemetry.max_successor_logical_upper_bound_bytes
            << ",\"peak_open_input_runs\":"
            << telemetry.peak_open_input_runs
            << ",\"peak_heap_items\":" << telemetry.peak_heap_items
            << ",\"max_writer_peak_buffered_bytes\":"
            << telemetry.max_writer_peak_buffered_bytes << '}';
    };

    const bool cleaned = cleanup_complete(result);
    const std::string_view status =
        publication.state == PublicationState::published && !cleaned
        ? "published_cleanup_incomplete"
        : publication_state_name(publication.state);
    const auto& static_plan = result.resources.static_plan;
    std::ostringstream output;
    output.imbue(std::locale::classic());
    output
        << "{\"schema\":\"TBANK_PREPARE_RESULT_V1\""
        << ",\"status\":" << tbank::cli::json_quote(status)
        << ",\"publication_state\":"
        << tbank::cli::json_quote(publication_state_name(publication.state))
        << ",\"publication_failed_step\":"
        << tbank::cli::json_quote(publication_step_name(publication.failed_step))
        << ",\"publication_error\":" << publication.error.value()
        << ",\"publication_cleanup_error\":"
        << publication.cleanup_error.value()
        << ",\"cleanup_complete\":" << (cleaned ? "true" : "false")
        << ",\"workspace_retirement\":"
        << tbank::cli::json_quote(
               workspace_state_name(result.workspace_retirement)
           )
        << ",\"frontier_retirement\":"
        << tbank::cli::json_quote(
               predecessor_state_name(result.graph.frontier_retirement)
           )
        << ",\"vertex_ids_retirement\":"
        << tbank::cli::json_quote(
               predecessor_state_name(result.graph.vertex_ids_retirement)
           )
        << ",\"workspace_retirement_error\":"
        << result.workspace_retirement_error.value()
        << ",\"frontier_retirement_error\":"
        << result.graph.frontier_retirement_error.value()
        << ",\"vertex_ids_retirement_error\":"
        << result.graph.vertex_ids_retirement_error.value()
        << ",\"input_sha256\":" << tbank::cli::json_quote(input_sha256)
        << ",\"consumed_csv_bytes\":" << result.consumed_csv_bytes
        << ",\"raw_edge_count\":" << result.initial_runs.summary.raw_edge_count
        << ",\"vertex_count\":" << result.graph.manifest.vertex_count
        << ",\"edge_count\":" << result.graph.manifest.edge_count
        << ",\"unique_edge_count\":"
        << result.graph.edges.unique_record_count
        << ",\"duplicate_edge_count\":" << total_duplicates
        << ",\"local_duplicate_edge_count\":"
        << result.compact_edges.summary.local_duplicates_removed
        << ",\"cross_run_duplicate_edge_count\":"
        << result.graph.edges.duplicate_record_count
        << ",\"task_count\":" << result.graph.tasks.task_count
        << ",\"managed_bulk_upper_bound_bytes\":"
        << result.resources.managed_bulk_upper_bound_bytes
        << ",\"component_max_open_files\":"
        << result.resources.component_max_open_files
        << ",\"config\":{"
        << "\"csv_byte_limit\":" << config.csv_byte_limit
        << ",\"non_bulk_reserve_bytes\":"
        << config.resources.non_bulk_reserve_bytes
        << ",\"input_chunk_bytes\":"
        << config.resources.maximum_input_chunk_bytes
        << ",\"phase_fd_budget\":" << config.resources.phase_fd_budget
        << ",\"disk_reserve_bytes\":"
        << config.resources.minimum_free_space_reserve_bytes
        << ",\"edge_slice_size\":"
        << config.counted_graph.tasks.edge_slice_size
        << ",\"max_task_edges\":"
        << config.counted_graph.tasks.max_task_edges
        << ",\"max_task_vertices\":"
        << config.counted_graph.tasks.max_task_vertices
        << ",\"endpoint_ids_per_run\":"
        << config.initial_runs.endpoint_ids_per_run
        << ",\"run_writer_buffer_bytes\":"
        << config.initial_runs.writer_buffer_bytes
        << ",\"vertex_merge_fan_in\":" << config.vertex_ids.fan_in
        << ",\"vertex_merge_reader_buffer_bytes\":"
        << config.vertex_ids.reader_buffer_bytes
        << ",\"vertex_merge_writer_buffer_bytes\":"
        << config.vertex_ids.writer_buffer_bytes
        << ",\"vertex_merge_crc_chunk_bytes\":"
        << config.vertex_ids.crc_chunk_bytes
        << ",\"raw_edges_per_run\":"
        << config.compact_edges.raw_edges_per_run
        << ",\"compact_reader_buffer_bytes\":"
        << config.compact_edges.reader_buffer_bytes
        << ",\"compact_writer_buffer_bytes\":"
        << config.compact_edges.writer_buffer_bytes
        << ",\"compact_crc_chunk_bytes\":"
        << config.compact_edges.crc_chunk_bytes
        << ",\"edge_merge_fan_in\":"
        << config.counted_graph.merge.fan_in
        << ",\"edge_merge_reader_buffer_bytes\":"
        << config.counted_graph.merge.reader_buffer_bytes
        << ",\"edge_merge_writer_buffer_bytes\":"
        << config.counted_graph.merge.writer_buffer_bytes
        << ",\"edge_merge_crc_chunk_bytes\":"
        << config.counted_graph.merge.crc_chunk_bytes
        << ",\"edge_batch_records\":"
        << config.counted_graph.edge_batch_records
        << ",\"graph_validation_chunk_bytes\":"
        << config.counted_graph.validation_io_chunk_bytes
        << ",\"compact_managed_bulk_limit_bytes\":"
        << static_plan.managed_bulk_limit_bytes
        << ",\"compact_disk_reserve_bytes\":"
        << config.resources.minimum_free_space_reserve_bytes
        << ",\"edge_merge_managed_bulk_limit_bytes\":"
        << static_plan.managed_bulk_limit_bytes
        << ",\"edge_merge_phase_fd_budget\":"
        << config.resources.phase_fd_budget
        << ",\"edge_merge_disk_reserve_bytes\":"
        << config.resources.minimum_free_space_reserve_bytes
        << "},\"paths\":{\"workspace\":"
        << tbank::cli::json_quote(resolved_workspace.native())
        << ",\"target\":"
        << tbank::cli::json_quote(resolved_target.native())
        << ",\"requested_workspace\":"
        << tbank::cli::json_quote(
               std::filesystem::absolute(requested_workspace)
                   .lexically_normal()
                   .native()
           )
        << ",\"requested_target\":"
        << tbank::cli::json_quote(
               std::filesystem::absolute(requested_target)
                   .lexically_normal()
                   .native()
           )
        << ",\"staging\":"
        << tbank::cli::json_quote(result.graph.staging_path.native())
        << "},\"errors\":{\"publication\":";
    write_error_code(output, publication.error);
    output << ",\"publication_cleanup\":";
    write_error_code(output, publication.cleanup_error);
    output << ",\"workspace_retirement\":";
    write_error_code(output, result.workspace_retirement_error);
    output << ",\"frontier_retirement\":";
    write_error_code(output, result.graph.frontier_retirement_error);
    output << ",\"vertex_ids_retirement\":";
    write_error_code(output, result.graph.vertex_ids_retirement_error);
    output
        << "},\"resources\":{\"managed_bulk_upper_bound_bytes\":"
        << result.resources.managed_bulk_upper_bound_bytes
        << ",\"component_max_open_files\":"
        << result.resources.component_max_open_files
        << ",\"static_plan\":{\"hard_memory_budget_bytes\":"
        << static_plan.hard_memory_budget_bytes
        << ",\"non_bulk_reserve_bytes\":"
        << static_plan.non_bulk_reserve_bytes
        << ",\"maximum_input_chunk_bytes\":"
        << static_plan.maximum_input_chunk_bytes
        << ",\"runtime_reserve_after_input_chunk_bytes\":"
        << static_plan.runtime_reserve_after_input_chunk_bytes
        << ",\"managed_bulk_limit_bytes\":"
        << static_plan.managed_bulk_limit_bytes
        << ",\"managed_bulk_floor_bytes\":"
        << static_plan.managed_bulk_floor_bytes
        << ",\"component_fd_floor\":" << static_plan.component_fd_floor
        << ",\"initial_run_memory\":";
    write_initial_memory(output, static_plan.initial_run_memory);
    output << ",\"vertex_id_merge_memory\":";
    write_vertex_memory(output, static_plan.vertex_id_merge_memory);
    output << ",\"minimum_compact_edge_memory\":";
    write_compact_memory(output, static_plan.minimum_compact_edge_memory);
    output << ",\"compact_edge_merge_memory\":";
    write_merge_memory(output, static_plan.compact_edge_merge_memory);
    output << ",\"full_fan_in_counted_graph_floor\":";
    write_counted_resources(
        output, static_plan.full_fan_in_counted_graph_floor
    );
    output << ",\"initial_run_disk\":";
    write_initial_disk(output, static_plan.initial_run_disk);
    output << "},\"vertex_id_merge_disk\":";
    write_vertex_disk(output, result.resources.vertex_id_merge_disk);
    output << ",\"initial_disk_admission\":";
    write_admission(output, result.resources.initial_disk_admission);
    output << ",\"vertex_id_disk_admission\":";
    write_admission(output, result.resources.vertex_id_disk_admission);
    output << ",\"actual_compact_edge_memory\":";
    write_compact_memory(output, result.compact_edges.memory_plan);
    output << ",\"actual_compact_edge_disk\":";
    write_compact_disk(output, result.compact_edges.disk_plan);
    output << ",\"actual_reduction_memory\":";
    write_merge_memory(output, result.reduction.memory_plan);
    output << ",\"actual_reduction_telemetry\":";
    write_reduction_telemetry(output, result.reduction.telemetry);
    output << ",\"actual_counted_graph\":";
    write_counted_resources(output, result.graph.resources);
    output << "}}\n";
    tbank::cli::write_stdout(output.str());
}

[[nodiscard]] int report_reduction_progress_error(
    const tbank::preprocess::CompactEdgeReductionProgressError& error,
    const std::filesystem::path& workspace
) noexcept;

[[nodiscard]] int run(const ParsedOptions& options) {
    const std::filesystem::path input = path_option(options, "input-csv");
    const std::filesystem::path workspace = path_option(options, "workspace");
    const std::filesystem::path graph = path_option(options, "graph-dir");
    CountedGraphPreprocessorConfig config = make_config(options);
    const tbank::checksum::Sha256Digest expected_digest =
        parse_input_digest(options);
    const std::filesystem::path resolved_workspace =
        resolved_child_path(workspace);
    const std::filesystem::path resolved_graph = resolved_child_path(graph);

    // Advisory fail-fast only. The final publication still uses atomic
    // no-replace because a competing creator can win after this check.
    if (path_entry_exists(resolved_graph)) {
        tbank::cli::write_error_json(
            "prepare",
            "publication",
            "target_exists_preflight",
            "prepare graph target already exists before preprocessing"
        );
        return tbank::cli::kExitPublication;
    }

    FileDescriptor input_file(input);
    const struct stat before = inspect_input(input_file.get());
    const auto input_bytes = static_cast<std::uint64_t>(before.st_size);
    config.csv_byte_limit = input_bytes;

    const std::size_t chunk_bytes = tbank::cli::parse_size(
        "input-chunk-bytes", options.require("input-chunk-bytes")
    );
    std::vector<char> buffer(chunk_bytes);
    tbank::checksum::Sha256Hasher input_hasher;
    // Freeze the caller-visible identity once. Passing the raw paths here
    // would resolve symlink ancestors a second time and could make the
    // publication target disagree with the structured receipt.
    CountedGraphPreprocessor preprocessor = CountedGraphPreprocessor::create(
        resolved_workspace, resolved_graph, config
    );

    std::uint64_t offset = 0U;
    while (offset < input_bytes) {
        const std::uint64_t remaining = input_bytes - offset;
        const std::size_t selected = static_cast<std::size_t>(std::min(
            remaining, static_cast<std::uint64_t>(buffer.size())
        ));
        std::span<char> chars(buffer.data(), selected);
        tbank::platform::pread_exact(
            input_file.get(), std::as_writable_bytes(chars), offset
        );
        input_hasher.update(std::as_bytes(chars));
        preprocessor.consume(chars);
        offset += static_cast<std::uint64_t>(selected);
    }

    require_no_input_growth(input_file.get(), input_bytes);
    const struct stat after = inspect_input(input_file.get());
    if (!same_input_snapshot(before, after)) {
        throw std::runtime_error(
            "prepare input metadata changed while it was being consumed"
        );
    }

    const tbank::checksum::Sha256Digest observed_digest =
        input_hasher.finish();
    const std::string observed_sha256 =
        tbank::checksum::sha256_lower_hex(observed_digest);
    if (observed_digest != expected_digest) {
        throw InputDigestMismatchError(
            tbank::checksum::sha256_lower_hex(expected_digest),
            observed_sha256
        );
    }

    try {
        const CountedGraphPreprocessResult result = preprocessor.finish();
        write_result_json(
            result,
            config,
            workspace,
            graph,
            resolved_workspace,
            resolved_graph,
            observed_sha256
        );
        if (result.graph.publication.state == PublicationState::published) {
            return tbank::cli::kExitSuccess;
        }
        if (result.graph.publication.state
            == PublicationState::durability_uncertain) {
            return tbank::cli::kExitDurabilityUncertain;
        }
        return tbank::cli::kExitPublication;
    } catch (
        const tbank::preprocess::CompactEdgeReductionProgressError& error
    ) {
        return report_reduction_progress_error(error, resolved_workspace);
    }
}

[[nodiscard]] int report_reduction_progress_error(
    const tbank::preprocess::CompactEdgeReductionProgressError& error,
    const std::filesystem::path& workspace
) noexcept {
    std::string_view kind = "data";
    std::string_view code = "reduction_progress_data_failure";
    int exit_code = tbank::cli::kExitData;
    std::string cause_message = "unknown reduction failure";
    try {
        error.rethrow_cause();
    } catch (const tbank::preprocess::CompactEdgeMemoryLimitError& cause) {
        kind = "resource";
        code = "memory_preflight_rejected";
        exit_code = tbank::cli::kExitResource;
        cause_message = cause.what();
    } catch (const tbank::preprocess::CompactEdgeDiskSpaceError& cause) {
        kind = "resource";
        code = "disk_preflight_rejected";
        exit_code = tbank::cli::kExitResource;
        cause_message = cause.what();
    } catch (const tbank::preprocess::PreprocessDiskSpaceError& cause) {
        kind = "resource";
        code = "disk_preflight_rejected";
        exit_code = tbank::cli::kExitResource;
        cause_message = cause.what();
    } catch (const tbank::preprocess::PreprocessInodeLimitError& cause) {
        kind = "resource";
        code = "inode_preflight_rejected";
        exit_code = tbank::cli::kExitResource;
        cause_message = cause.what();
    } catch (const std::bad_alloc& cause) {
        kind = "resource";
        code = "allocation_failed";
        exit_code = tbank::cli::kExitResource;
        cause_message = cause.what();
    } catch (const std::system_error& cause) {
        kind = "system";
        code = "reduction_progress_system_failure";
        exit_code = tbank::cli::kExitSystem;
        cause_message = cause.what();
    } catch (const std::invalid_argument& cause) {
        cause_message = cause.what();
    } catch (const std::logic_error& cause) {
        kind = "internal";
        code = "reduction_progress_invariant_failure";
        exit_code = tbank::cli::kExitInternal;
        cause_message = cause.what();
    } catch (const std::exception& cause) {
        cause_message = cause.what();
    } catch (...) {
        kind = "internal";
        code = "reduction_progress_unknown_failure";
        exit_code = tbank::cli::kExitInternal;
    }

    try {
        const auto frontier = error.recovery_frontier();
        std::ostringstream output;
        output.imbue(std::locale::classic());
        output
            << "{\"schema\":\"TBANK_CLI_ERROR_V1\",\"command\":\"prepare\""
            << ",\"kind\":" << tbank::cli::json_quote(kind)
            << ",\"code\":" << tbank::cli::json_quote(code)
            << ",\"message\":" << tbank::cli::json_quote(cause_message)
            << ",\"workspace\":"
            << (workspace.empty()
                    ? "null"
                    : tbank::cli::json_quote(workspace.native()))
            << ",\"recovery_frontier\":{\"generation_index\":"
            << frontier.generation_index
            << ",\"run_count\":" << frontier.run_count
            << ",\"record_count\":" << frontier.record_count
            << ",\"initial_record_count\":"
            << frontier.initial_record_count
            << ",\"vertex_count\":" << frontier.vertex_count << "}}\n";
        tbank::cli::write_stderr(output.str());
    } catch (...) {
        tbank::cli::write_error_json(
            "prepare", kind, code, cause_message
        );
    }
    return exit_code;
}

}  // namespace

int main(const int argument_count, char* arguments[]) {
    try {
        tbank::cli::initialize_result_channels();
        if (tbank::cli::is_help_request(argument_count, arguments)) {
            tbank::cli::write_stdout(kHelp);
            return tbank::cli::kExitSuccess;
        }
        std::array<std::string_view, kAllowedOptions.size() + 1U> allowed{};
        std::copy(kAllowedOptions.begin(), kAllowedOptions.end(), allowed.begin());
        allowed.back() = kGraphValidationOption;
        const ParsedOptions options = ParsedOptions::parse(
            argument_count, arguments, allowed
        );
        return run(options);
    } catch (const UsageError& error) {
        tbank::cli::write_error_json(
            "prepare", "usage", "invalid_arguments", error.what()
        );
        return tbank::cli::kExitUsage;
    } catch (const InputDigestMismatchError& error) {
        tbank::cli::write_error_json(
            "prepare", "data", "input_sha256_mismatch", error.what()
        );
        return tbank::cli::kExitData;
    } catch (
        const tbank::preprocess::CompactEdgeReductionProgressError& error
    ) {
        return report_reduction_progress_error(error, {});
    } catch (const tbank::preprocess::CompactEdgeMemoryLimitError& error) {
        tbank::cli::write_error_json(
            "prepare", "resource", "memory_preflight_rejected", error.what()
        );
        return tbank::cli::kExitResource;
    } catch (const tbank::preprocess::CompactEdgeDiskSpaceError& error) {
        tbank::cli::write_error_json(
            "prepare", "resource", "disk_preflight_rejected", error.what()
        );
        return tbank::cli::kExitResource;
    } catch (const tbank::preprocess::PreprocessDiskSpaceError& error) {
        tbank::cli::write_error_json(
            "prepare", "resource", "disk_preflight_rejected", error.what()
        );
        return tbank::cli::kExitResource;
    } catch (const tbank::preprocess::PreprocessInodeLimitError& error) {
        tbank::cli::write_error_json(
            "prepare", "resource", "inode_preflight_rejected", error.what()
        );
        return tbank::cli::kExitResource;
    } catch (const std::bad_alloc& error) {
        tbank::cli::write_error_json(
            "prepare", "resource", "allocation_failed", error.what()
        );
        return tbank::cli::kExitResource;
    } catch (const tbank::checksum::Sha256Error& error) {
        tbank::cli::write_error_json(
            "prepare", "internal", "sha256_provider_failure", error.what()
        );
        return tbank::cli::kExitInternal;
    } catch (const tbank::io::EdgeCsvError& error) {
        tbank::cli::write_error_json(
            "prepare", "data", "invalid_csv", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const std::system_error& error) {
        tbank::cli::write_error_json(
            "prepare", "system", "system_error", error.what()
        );
        return tbank::cli::kExitSystem;
    } catch (const std::invalid_argument& error) {
        tbank::cli::write_error_json(
            "prepare", "data", "invalid_data", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const std::logic_error& error) {
        tbank::cli::write_error_json(
            "prepare", "internal", "invariant_failure", error.what()
        );
        return tbank::cli::kExitInternal;
    } catch (const std::exception& error) {
        tbank::cli::write_error_json(
            "prepare", "internal", "unexpected_exception", error.what()
        );
        return tbank::cli::kExitInternal;
    } catch (...) {
        tbank::cli::write_error_json(
            "prepare",
            "internal",
            "unknown_exception",
            "unknown non-standard exception"
        );
        return tbank::cli::kExitInternal;
    }
}

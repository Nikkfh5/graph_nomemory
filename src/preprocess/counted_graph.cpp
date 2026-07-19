#include "tbank/preprocess/counted_graph.hpp"

#include "tbank/platform/checked_io.hpp"
#include "tbank/preprocess/compact_edge_runs.hpp"
#include "tbank/preprocess/run_file.hpp"
#include "tbank/preprocess/vertex_id_merge.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/file_reader.hpp"
#include "tbank/storage/graph.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

namespace tbank::preprocess {
namespace {

static_assert(
    std::is_nothrow_move_assignable_v<platform::PublicationResult>
);
static_assert(
    std::is_nothrow_move_constructible_v<CountedGraphBuildResult>
);

constexpr std::size_t kMaximumStagingNameAttempts = 128U;
constexpr std::uint64_t kGraphBinaryFileCount = 5U;
constexpr std::uint64_t kGraphDirectoryFileCount = 6U;
constexpr std::uint64_t kGraphCreatedInodeCount = 7U;
constexpr std::string_view kManifestName = "metadata.json";

constexpr std::array<storage::ManifestFileKind, storage::kManifestFileCount>
    kManifestKinds{
        storage::ManifestFileKind::vertex_ids,
        storage::ManifestFileKind::incoming_sources,
        storage::ManifestFileKind::incoming_counts,
        storage::ManifestFileKind::out_degree,
        storage::ManifestFileKind::tasks,
    };

constexpr std::array<std::string_view, storage::kManifestFileCount + 1U>
    kOwnedStagingEntries{
        "vertex_ids.bin",
        "incoming_sources.bin",
        "incoming_counts.bin",
        "out_degree.bin",
        "tasks.bin",
        kManifestName,
    };

std::atomic<std::uint64_t> staging_name_sequence{0U};

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) noexcept {
    static_assert(
        std::numeric_limits<std::size_t>::digits
            <= std::numeric_limits<std::uint64_t>::digits
    );
    return static_cast<std::uint64_t>(value);
}

[[nodiscard]] bool contains_embedded_nul(
    const std::filesystem::path& path
) {
    const auto& native = path.native();
    return native.find(std::filesystem::path::value_type{})
        != std::filesystem::path::string_type::npos;
}

[[noreturn]] void throw_posix_error(
    const int error_number,
    const char* const operation
) {
    throw std::system_error(
        error_number == 0 ? EIO : error_number,
        std::generic_category(),
        operation
    );
}

void validate_frontier_and_config(
    const CompactEdgeMergeFrontier frontier,
    const CountedGraphConfig& config
) {
    // Reuse merge checks; runtime allowances are validated by the entrypoint.
    static_cast<void>(compact_edge_merge_memory_plan(config.merge));
    tasks::validate_task_config(config.tasks);

    if (frontier.run_count == 0U
        || frontier.run_count > size_to_u64(config.merge.fan_in)) {
        throw std::invalid_argument(
            "counted graph frontier run count is outside merge fan_in"
        );
    }
    if (frontier.record_count == 0U
        || frontier.run_count > frontier.record_count
        || frontier.initial_record_count < frontier.record_count) {
        throw std::invalid_argument(
            "counted graph frontier record counts are invalid"
        );
    }
    if (frontier.vertex_count == 0U) {
        throw std::invalid_argument(
            "counted graph frontier vertex count is zero"
        );
    }
    if (config.merge.writer_buffer_bytes < storage::kTaskRecordBytes) {
        throw std::invalid_argument(
            "counted graph writer buffer must hold one task record"
        );
    }
    if (config.edge_batch_records == 0U) {
        throw std::invalid_argument(
            "counted graph edge batch must contain at least one record"
        );
    }
    if (config.edge_batch_records
        > std::numeric_limits<std::size_t>::max()
            / sizeof(CompactEdgeRecord)) {
        throw std::overflow_error(
            "counted graph edge batch byte size overflows size_t"
        );
    }
    if (config.validation_io_chunk_bytes == 0U
        || config.validation_io_chunk_bytes
            > storage::kMaximumCrcChunkBytes) {
        throw std::invalid_argument(
            "counted graph validation chunk must be in [1, 1 MiB]"
        );
    }
}

[[nodiscard]] std::uint64_t add_many(
    std::initializer_list<std::uint64_t> values
) {
    std::uint64_t result = 0U;
    for (const std::uint64_t value : values) {
        result = platform::checked_add(result, value);
    }
    return result;
}

void validate_file_upper_bounds(
    const std::uint64_t vertex_count,
    const std::uint64_t edge_count,
    const std::uint64_t task_count
) {
    const std::uint64_t vertex_payload = platform::checked_multiply(
        vertex_count, storage::kScalarRecordBytes
    );
    const std::uint64_t source_payload = platform::checked_multiply(
        edge_count, storage::kScalarRecordBytes
    );
    const std::uint64_t task_payload = platform::checked_multiply(
        task_count, storage::kTaskRecordBytes
    );
    for (const std::uint64_t payload :
         {vertex_payload, source_payload, task_payload}) {
        static_cast<void>(platform::checked_u64_to_off_t(
            platform::checked_add(storage::kBinaryHeaderBytes, payload)
        ));
    }
    static_cast<void>(platform::checked_u64_to_off_t(storage::kMaxManifestBytes));
}

[[nodiscard]] std::filesystem::path absolute_target_path(
    const std::filesystem::path& target
) {
    if (target.empty() || contains_embedded_nul(target)
        || !target.has_filename() || target.filename() == "."
        || target.filename() == "..") {
        throw std::invalid_argument("counted graph target path is invalid");
    }

    std::filesystem::path absolute = std::filesystem::absolute(target);
    const std::filesystem::path parent = absolute.parent_path();
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(parent.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        throw_posix_error(errno, "inspect counted graph target parent");
    }
    if (!S_ISDIR(status.st_mode)) {
        throw std::invalid_argument(
            "counted graph target parent must be a real directory"
        );
    }
    return absolute;
}

[[nodiscard]] std::filesystem::path absolute_workspace_path(
    const std::filesystem::path& workspace
) {
    if (workspace.empty() || contains_embedded_nul(workspace)) {
        throw std::invalid_argument(
            "counted graph preprocessing workspace is invalid"
        );
    }
    std::filesystem::path absolute = std::filesystem::absolute(workspace);
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(absolute.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        throw_posix_error(errno, "inspect counted graph preprocessing workspace");
    }
    if (!S_ISDIR(status.st_mode)) {
        throw std::invalid_argument(
            "counted graph preprocessing workspace must be a real directory"
        );
    }
    return absolute;
}

[[nodiscard]] bool path_is_same_or_descendant(
    const std::filesystem::path& candidate,
    const std::filesystem::path& ancestor
) noexcept {
    auto candidate_part = candidate.begin();
    for (auto ancestor_part = ancestor.begin();
         ancestor_part != ancestor.end();
         ++ancestor_part, ++candidate_part) {
        if (candidate_part == candidate.end()
            || *candidate_part != *ancestor_part) {
            return false;
        }
    }
    return true;
}

void reject_target_inside_frontier(
    const std::filesystem::path& workspace,
    const CompactEdgeMergeFrontier frontier,
    const std::filesystem::path& target
) {
    // Resolve aliases before checking target containment.
    const std::filesystem::path generation =
        std::filesystem::weakly_canonical(compact_edge_generation_path(
            workspace, frontier.generation_index
        ));
    const std::filesystem::path resolved_target =
        std::filesystem::weakly_canonical(target);
    if (path_is_same_or_descendant(resolved_target, generation)) {
        throw std::invalid_argument(
            "counted graph target must not be inside the compact frontier"
        );
    }
}

void preflight_target_filesystem(
    const std::filesystem::path& parent,
    const CountedGraphResourcePlan& plan,
    const CountedGraphConfig& config
) {
    struct statvfs status {};
    int result = -1;
    do {
        errno = 0;
        result = ::statvfs(parent.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        throw_posix_error(errno, "inspect counted graph target filesystem");
    }

    const std::uint64_t fragment_bytes = status.f_frsize != 0U
        ? static_cast<std::uint64_t>(status.f_frsize)
        : static_cast<std::uint64_t>(status.f_bsize);
    if (fragment_bytes == 0U) {
        throw std::runtime_error(
            "counted graph filesystem reports zero allocation unit"
        );
    }
    const std::uint64_t available_bytes = platform::checked_multiply(
        static_cast<std::uint64_t>(status.f_bavail), fragment_bytes
    );
    const std::uint64_t rounding_slack = platform::checked_multiply(
        kGraphDirectoryFileCount, fragment_bytes - 1U
    );
    const std::uint64_t directory_allowance = platform::checked_multiply(
        kGraphCreatedInodeCount, fragment_bytes
    );
    const std::uint64_t required_bytes = add_many({
        plan.staging_logical_upper_bound_bytes,
        rounding_slack,
        directory_allowance,
        config.merge.minimum_free_space_reserve_bytes,
    });
    if (required_bytes > available_bytes) {
        throw CompactEdgeDiskSpaceError(required_bytes, available_bytes);
    }
    if (status.f_files != 0U
        && kGraphCreatedInodeCount
            > static_cast<std::uint64_t>(status.f_favail)) {
        throw std::runtime_error(
            "counted graph filesystem has too few available inodes"
        );
    }
}

[[nodiscard]] std::filesystem::path create_unique_staging_directory(
    const std::filesystem::path& target
) {
    const std::filesystem::path parent = target.parent_path();
    for (std::size_t attempt = 0U;
         attempt < kMaximumStagingNameAttempts;
         ++attempt) {
        const std::uint64_t sequence = staging_name_sequence.fetch_add(
            1U, std::memory_order_relaxed
        );
        const std::filesystem::path staging = parent /
            (".tbank-graph-" + std::to_string(::getpid()) + "-"
             + std::to_string(sequence));
        if (staging == target) {
            continue;
        }
        errno = 0;
        if (::mkdir(staging.c_str(), 0700) == 0) {
            return staging;
        }
        if (errno != EEXIST) {
            throw_posix_error(errno, "create counted graph staging directory");
        }
    }
    throw_posix_error(
        EEXIST, "exhaust counted graph staging directory names"
    );
}

class StagingDirectoryGuard final {
public:
    explicit StagingDirectoryGuard(std::filesystem::path path)
        : path_(std::move(path)) {}

    StagingDirectoryGuard(const StagingDirectoryGuard&) = delete;
    StagingDirectoryGuard& operator=(const StagingDirectoryGuard&) = delete;

    ~StagingDirectoryGuard() noexcept {
        if (!active_) {
            return;
        }
        for (const std::string_view name : kOwnedStagingEntries) {
            const std::filesystem::path entry = path_ / name;
            if (::unlink(entry.c_str()) == -1 && errno != ENOENT) {
                // Continue cleanup.
            }
        }
        static_cast<void>(::rmdir(path_.c_str()));
    }

    void release() noexcept {
        active_ = false;
    }

private:
    std::filesystem::path path_;
    bool active_ = true;
};

[[nodiscard]] storage::ManifestFileDescriptor descriptor_from_info(
    const storage::ManifestFileKind kind,
    const RunFileInfo& info
) {
    return storage::ManifestFileDescriptor{
        .kind = kind,
        .record_bytes = info.header.record_bytes,
        .record_count = info.header.record_count,
        .payload_bytes = info.header.payload_bytes,
        .file_bytes = info.file_bytes,
        .crc64 = info.header.payload_crc64,
    };
}

[[nodiscard]] std::filesystem::path graph_file_path(
    const std::filesystem::path& staging,
    const storage::ManifestFileKind kind
) {
    return staging / storage::manifest_file_name(kind);
}

[[nodiscard]] RunFileInfo copy_vertex_ids(
    const std::filesystem::path& workspace,
    const std::filesystem::path& staging,
    const CompactEdgeMergeFrontier frontier,
    const CountedGraphConfig& config
) {
    storage::ValidatedBinaryFileReader reader =
        storage::ValidatedBinaryFileReader::open(
            vertex_ids_path(workspace),
            storage::kVertexIdsMagic,
            storage::kScalarRecordBytes,
            config.merge.crc_chunk_bytes
        );
    const storage::BinaryHeader input_header = reader.header();
    if (input_header.record_count != frontier.vertex_count) {
        throw storage::BinaryError(
            "vertex_ids count differs from counted graph frontier"
        );
    }

    RunFileWriter writer = RunFileWriter::create(
        graph_file_path(staging, storage::ManifestFileKind::vertex_ids),
        storage::kVertexIdsMagic,
        storage::kScalarRecordBytes,
        config.merge.writer_buffer_bytes
    );
    std::vector<std::byte> buffer(config.merge.reader_buffer_bytes);
    const std::uint64_t records_per_chunk = size_to_u64(
        buffer.size() / storage::kScalarRecordBytes
    );
    std::uint64_t next_record = 0U;
    while (next_record < input_header.record_count) {
        const std::uint64_t record_count = std::min(
            records_per_chunk, input_header.record_count - next_record
        );
        const std::size_t byte_count = static_cast<std::size_t>(record_count)
            * storage::kScalarRecordBytes;
        const std::span<std::byte> chunk(buffer.data(), byte_count);
        reader.read_records(next_record, chunk);
        writer.append_records(chunk);
        next_record = platform::checked_add(next_record, record_count);
    }
    RunFileInfo output = writer.finish_and_sync();
    if (output.header.record_count != input_header.record_count
        || output.header.payload_bytes != input_header.payload_bytes
        || output.header.payload_crc64 != input_header.payload_crc64) {
        throw std::logic_error("bounded vertex_ids copy changed payload");
    }
    return output;
}

[[nodiscard]] storage::TaskRecord physical_task_record(
    const tasks::Task& task
) {
    if (const auto* const ordinary = std::get_if<tasks::OrdinaryTask>(&task)) {
        return storage::make_ordinary_task_record(
            ordinary->dst_begin,
            ordinary->dst_count,
            ordinary->edge_begin,
            ordinary->edge_count
        );
    }
    if (const auto* const hub = std::get_if<tasks::HubSlice>(&task)) {
        return storage::make_hub_slice_task_record(
            hub->dst,
            hub->slice_index,
            hub->slice_count,
            hub->edge_begin,
            hub->edge_count
        );
    }
    throw std::logic_error("task variant is valueless");
}

struct MergeOutputs {
    RunFileInfo incoming_sources{};
    RunFileInfo incoming_counts{};
    RunFileInfo tasks{};
    CompactEdgeUniqueSummary edge_summary{};
    tasks::TaskPartitionSummary task_summary{};
};

[[nodiscard]] MergeOutputs build_edge_dependent_files(
    const std::filesystem::path& workspace,
    const std::filesystem::path& staging,
    const CompactEdgeMergeFrontier frontier,
    const CountedGraphConfig& config,
    std::vector<std::uint32_t>& out_degree
) {
    UniqueCompactEdgeCursor cursor = UniqueCompactEdgeCursor::open(
        workspace, frontier, config.merge
    );
    RunFileWriter source_writer = RunFileWriter::create(
        graph_file_path(staging, storage::ManifestFileKind::incoming_sources),
        storage::kIncomingSourcesMagic,
        storage::kScalarRecordBytes,
        config.merge.writer_buffer_bytes
    );
    RunFileWriter count_writer = RunFileWriter::create(
        graph_file_path(staging, storage::ManifestFileKind::incoming_counts),
        storage::kIncomingCountsMagic,
        storage::kScalarRecordBytes,
        config.merge.writer_buffer_bytes
    );
    RunFileWriter task_writer = RunFileWriter::create(
        graph_file_path(staging, storage::ManifestFileKind::tasks),
        storage::kTasksMagic,
        storage::kTaskRecordBytes,
        config.merge.writer_buffer_bytes
    );

    tasks::TaskPartitionBuilder partitioner(
        config.tasks,
        [&](tasks::Task task) {
            const storage::TaskRecord record = physical_task_record(task);
            task_writer.append_records(storage::encode_task_record(record));
        }
    );

    std::uint64_t next_destination = 0U;
    std::optional<std::uint32_t> current_destination;
    std::uint64_t current_incoming_count = 0U;
    std::uint64_t emitted_edge_count = 0U;
    std::optional<CompactEdgeRecord> previous_edge;
    std::vector<CompactEdgeRecord> edge_batch(config.edge_batch_records);
    std::vector<std::byte> source_batch(
        config.edge_batch_records * storage::kScalarRecordBytes
    );

    const auto emit_count = [&](const std::uint64_t count) {
        if (next_destination >= frontier.vertex_count
            || count > frontier.vertex_count
            || count > std::numeric_limits<std::uint32_t>::max()) {
            throw std::logic_error(
                "counted graph incoming count is outside compact range"
            );
        }
        std::array<std::byte, storage::kScalarRecordBytes> encoded{};
        const auto narrowed = static_cast<std::uint32_t>(count);
        storage::encode_u32_le(narrowed, encoded);
        count_writer.append_records(encoded);
        partitioner.consume(narrowed);
        ++next_destination;
    };

    for (;;) {
        const std::size_t count = cursor.read(edge_batch);
        if (count == 0U) {
            break;
        }
        for (std::size_t index = 0U; index < count; ++index) {
            const CompactEdgeRecord edge = edge_batch[index];
            if (previous_edge.has_value()
                && !(previous_edge->destination < edge.destination
                     || (previous_edge->destination == edge.destination
                         && previous_edge->source < edge.source))) {
                throw std::logic_error(
                    "unique compact cursor emitted non-increasing edges"
                );
            }
            previous_edge = edge;

            if (!current_destination.has_value()
                || edge.destination != *current_destination) {
                if (current_destination.has_value()) {
                    emit_count(current_incoming_count);
                }
                while (next_destination < edge.destination) {
                    emit_count(0U);
                }
                current_destination = edge.destination;
                current_incoming_count = 0U;
            }

            storage::encode_u32_le(
                edge.source,
                source_batch,
                index * storage::kScalarRecordBytes
            );

            if (out_degree[edge.source]
                == std::numeric_limits<std::uint32_t>::max()) {
                throw std::overflow_error("counted graph out degree overflow");
            }
            ++out_degree[edge.source];
            current_incoming_count = platform::checked_add(
                current_incoming_count, 1U
            );
            emitted_edge_count = platform::checked_add(emitted_edge_count, 1U);
        }
        source_writer.append_records(
            std::span<const std::byte>(source_batch).first(
                count * storage::kScalarRecordBytes
            )
        );
    }
    if (current_destination.has_value()) {
        emit_count(current_incoming_count);
    }
    while (next_destination < frontier.vertex_count) {
        emit_count(0U);
    }

    const CompactEdgeUniqueSummary cursor_summary = cursor.summary();
    const tasks::TaskPartitionSummary task_summary = partitioner.finish();
    if (cursor_summary.input_record_count != frontier.record_count
        || cursor_summary.unique_record_count != emitted_edge_count
        || task_summary.destination_count != frontier.vertex_count
        || task_summary.edge_count != emitted_edge_count) {
        throw std::logic_error("counted graph stream accounting diverged");
    }
    const CompactEdgeUniqueSummary edge_summary{
        .input_record_count = frontier.initial_record_count,
        .unique_record_count = cursor_summary.unique_record_count,
        .duplicate_record_count =
            frontier.initial_record_count - cursor_summary.unique_record_count,
    };

    std::uint64_t degree_sum = 0U;
    for (const std::uint32_t degree : out_degree) {
        if (degree > frontier.vertex_count) {
            throw std::logic_error("counted graph out degree exceeds V");
        }
        degree_sum = platform::checked_add(degree_sum, degree);
    }
    if (degree_sum != emitted_edge_count) {
        throw std::logic_error("counted graph out degree sum differs from E");
    }

    MergeOutputs output{
        .incoming_sources = source_writer.finish_and_sync(),
        .incoming_counts = count_writer.finish_and_sync(),
        .tasks = task_writer.finish_and_sync(),
        .edge_summary = edge_summary,
        .task_summary = task_summary,
    };
    if (output.incoming_sources.header.record_count != emitted_edge_count
        || output.incoming_counts.header.record_count != frontier.vertex_count
        || output.tasks.header.record_count != task_summary.task_count) {
        throw std::logic_error("counted graph writer counts diverged");
    }
    return output;
}

[[nodiscard]] RunFileInfo write_out_degree(
    const std::filesystem::path& staging,
    const std::span<const std::uint32_t> out_degree,
    const CountedGraphConfig& config
) {
    RunFileWriter writer = RunFileWriter::create(
        graph_file_path(staging, storage::ManifestFileKind::out_degree),
        storage::kOutDegreeMagic,
        storage::kScalarRecordBytes,
        config.merge.writer_buffer_bytes
    );
    for (const std::uint32_t degree : out_degree) {
        std::array<std::byte, storage::kScalarRecordBytes> encoded{};
        storage::encode_u32_le(degree, encoded);
        writer.append_records(encoded);
    }
    return writer.finish_and_sync();
}

[[nodiscard]] storage::GraphManifest make_manifest(
    const CompactEdgeMergeFrontier frontier,
    const CountedGraphConfig& config,
    const RunFileInfo& vertex_ids,
    const MergeOutputs& merge,
    const RunFileInfo& out_degree
) {
    storage::GraphManifest manifest{
        .schema_version = storage::kManifestSchemaVersion,
        .vertex_count = frontier.vertex_count,
        .edge_count = merge.edge_summary.unique_record_count,
        .edge_slice_size = config.tasks.edge_slice_size,
        .max_task_edges = config.tasks.max_task_edges,
        .max_task_vertices = config.tasks.max_task_vertices,
        .files = {
            descriptor_from_info(kManifestKinds[0], vertex_ids),
            descriptor_from_info(kManifestKinds[1], merge.incoming_sources),
            descriptor_from_info(kManifestKinds[2], merge.incoming_counts),
            descriptor_from_info(kManifestKinds[3], out_degree),
            descriptor_from_info(kManifestKinds[4], merge.tasks),
        },
    };
    storage::validate_manifest(manifest);
    return manifest;
}

[[nodiscard]] platform::PublicationResult publish_manifest(
    const std::filesystem::path& staging,
    const std::string& encoded_manifest,
    platform::PublicationBackend* const backend
) {
    const std::span<const std::byte> bytes = std::as_bytes(
        std::span<const char>(encoded_manifest.data(), encoded_manifest.size())
    );
    if (backend == nullptr) {
        return platform::publish_file_no_replace(
            staging / kManifestName, bytes, 0600
        );
    }
    return platform::publish_file_no_replace(
        staging / kManifestName, bytes, *backend, 0600
    );
}

void require_manifest_publication(
    const platform::PublicationResult& result
) {
    if (result.state == platform::PublicationState::published) {
        return;
    }
    const std::error_code error = result.error
        ? result.error
        : std::error_code(EIO, std::generic_category());
    throw std::system_error(error, "publish counted graph metadata.json");
}

[[nodiscard]] std::error_code unlink_checked_noexcept(
    const std::filesystem::path& path
) noexcept {
    int result = -1;
    do {
        errno = 0;
        result = ::unlink(path.c_str());
    } while (result == -1 && errno == EINTR);
    return result == 0
        ? std::error_code{}
        : std::error_code(errno == 0 ? EIO : errno, std::generic_category());
}

[[nodiscard]] std::error_code rmdir_checked_noexcept(
    const std::filesystem::path& path
) noexcept {
    int result = -1;
    do {
        errno = 0;
        result = ::rmdir(path.c_str());
    } while (result == -1 && errno == EINTR);
    return result == 0
        ? std::error_code{}
        : std::error_code(errno == 0 ? EIO : errno, std::generic_category());
}

[[nodiscard]] std::error_code retire_frontier(
    const std::filesystem::path& workspace,
    const CompactEdgeMergeFrontier frontier
) noexcept {
    try {
        for (std::uint64_t run = 0U; run < frontier.run_count; ++run) {
            const std::error_code error = unlink_checked_noexcept(
                compact_edge_run_path(
                    workspace, frontier.generation_index, run
                )
            );
            if (error) {
                return error;
            }
        }
        return rmdir_checked_noexcept(compact_edge_generation_path(
            workspace, frontier.generation_index
        ));
    } catch (...) {
        return {EIO, std::generic_category()};
    }
}

[[nodiscard]] std::error_code retire_vertex_ids(
    const std::filesystem::path& workspace
) noexcept {
    try {
        return unlink_checked_noexcept(vertex_ids_path(workspace));
    } catch (...) {
        return {EIO, std::generic_category()};
    }
}

[[nodiscard]] CountedGraphBuildResult build_impl(
    const std::filesystem::path& preprocess_workspace,
    const CompactEdgeMergeFrontier frontier,
    const std::filesystem::path& target,
    const CountedGraphConfig& config,
    platform::PublicationBackend* const backend
) {
    const CountedGraphResourcePlan resources = counted_graph_resource_plan(
        frontier, config
    );
    if (config.merge.managed_bulk_limit_bytes == 0U) {
        throw std::invalid_argument(
            "counted graph managed bulk limit must be explicitly supplied"
        );
    }
    if (config.merge.managed_bulk_limit_bytes
        > kCompactEdgeHardMemoryBudgetBytes) {
        throw std::invalid_argument(
            "counted graph managed bulk limit exceeds hard budget"
        );
    }
    if (resources.managed_bulk_upper_bound_bytes
        > config.merge.managed_bulk_limit_bytes) {
        throw CompactEdgeMemoryLimitError(
            resources.managed_bulk_upper_bound_bytes,
            config.merge.managed_bulk_limit_bytes
        );
    }
    if (config.merge.phase_fd_budget < resources.max_open_files) {
        throw std::invalid_argument(
            "counted graph phase FD budget is below required maximum"
        );
    }

    const std::filesystem::path workspace =
        absolute_workspace_path(preprocess_workspace);
    const std::filesystem::path absolute_target = absolute_target_path(target);
    reject_target_inside_frontier(workspace, frontier, absolute_target);
    preflight_target_filesystem(
        absolute_target.parent_path(), resources, config
    );

    const std::filesystem::path staging =
        create_unique_staging_directory(absolute_target);
    StagingDirectoryGuard staging_guard(staging);

    const RunFileInfo vertex_ids = copy_vertex_ids(
        workspace, staging, frontier, config
    );
    std::vector<std::uint32_t> out_degree(frontier.vertex_count, 0U);
    const MergeOutputs merge = build_edge_dependent_files(
        workspace, staging, frontier, config, out_degree
    );
    const RunFileInfo out_degree_info = write_out_degree(
        staging, out_degree, config
    );
    std::vector<std::uint32_t>{}.swap(out_degree);

    const storage::GraphManifest manifest = make_manifest(
        frontier, config, vertex_ids, merge, out_degree_info
    );
    {
        const std::string encoded_manifest = storage::encode_manifest(manifest);
        require_manifest_publication(
            publish_manifest(staging, encoded_manifest, backend)
        );
    }

    {
        const storage::ValidatedGraph graph = storage::ValidatedGraph::open(
            staging,
            storage::GraphValidationOptions{
                .io_chunk_bytes = config.validation_io_chunk_bytes,
            }
        );
        if (graph.manifest() != manifest) {
            throw std::logic_error(
                "validated counted graph manifest changed after writing"
            );
        }
    }

    CountedGraphBuildResult result{
        .manifest = manifest,
        .edges = merge.edge_summary,
        .tasks = merge.task_summary,
        .resources = resources,
        .publication = {},
        .frontier_retirement = PredecessorRetirementState::not_attempted,
        .frontier_retirement_error = {},
        .vertex_ids_retirement = PredecessorRetirementState::not_attempted,
        .vertex_ids_retirement_error = {},
        .staging_path = staging,
    };
    result.publication = backend == nullptr
        ? platform::publish_staging_directory_no_replace(
              staging, absolute_target
          )
        : platform::publish_staging_directory_no_replace(
              staging, absolute_target, *backend
          );

    // Preserve recoverable staging unless rename already consumed it.
    staging_guard.release();

    if (result.publication.state == platform::PublicationState::published) {
        result.frontier_retirement_error = retire_frontier(workspace, frontier);
        result.frontier_retirement = result.frontier_retirement_error
            ? PredecessorRetirementState::failed
            : PredecessorRetirementState::retired;
        result.vertex_ids_retirement_error = retire_vertex_ids(workspace);
        result.vertex_ids_retirement = result.vertex_ids_retirement_error
            ? PredecessorRetirementState::failed
            : PredecessorRetirementState::retired;
    }
    return result;
}

}  // namespace

CountedGraphResourcePlan counted_graph_resource_plan(
    const CompactEdgeMergeFrontier frontier,
    const CountedGraphConfig& config
) {
    validate_frontier_and_config(frontier, config);

    const CompactEdgeMergeMemoryPlan cursor_plan =
        compact_edge_merge_memory_plan(config.merge);
    const std::uint64_t vertex_count = frontier.vertex_count;
    const std::uint64_t maximum_simple_edges = platform::checked_multiply(
        vertex_count, vertex_count
    );
    const std::uint64_t edge_count_upper_bound = std::min(
        frontier.record_count, maximum_simple_edges
    );
    const std::uint64_t task_count_upper_bound = platform::checked_add(
        vertex_count,
        edge_count_upper_bound / config.tasks.edge_slice_size
    );
    validate_file_upper_bounds(
        vertex_count, edge_count_upper_bound, task_count_upper_bound
    );

    const std::uint64_t out_degree_bytes = platform::checked_multiply(
        vertex_count, storage::kScalarRecordBytes
    );
    if (out_degree_bytes > std::numeric_limits<std::size_t>::max()) {
        throw std::overflow_error(
            "counted graph out_degree allocation exceeds size_t"
        );
    }
    const std::uint64_t edge_batch_bytes = platform::checked_multiply(
        size_to_u64(config.edge_batch_records), sizeof(CompactEdgeRecord)
    );
    const std::uint64_t source_batch_bytes = platform::checked_multiply(
        size_to_u64(config.edge_batch_records), storage::kScalarRecordBytes
    );
    const std::uint64_t cursor_reader_buffers = platform::checked_multiply(
        frontier.run_count, size_to_u64(config.merge.reader_buffer_bytes)
    );
    const std::uint64_t cursor_objects = platform::checked_multiply(
        frontier.run_count, cursor_plan.cursor_object_bytes
    );
    const std::uint64_t heap_entry_bytes = platform::checked_add(
        sizeof(CompactEdgeRecord), sizeof(std::size_t)
    );
    const std::uint64_t cursor_heap_arrays = platform::checked_multiply(
        frontier.run_count, heap_entry_bytes
    );
    const std::uint64_t output_writer_buffers = platform::checked_multiply(
        3U, size_to_u64(config.merge.writer_buffer_bytes)
    );
    const std::uint64_t merge_phase = add_many({
        out_degree_bytes,
        edge_batch_bytes,
        source_batch_bytes,
        cursor_reader_buffers,
        cursor_objects,
        cursor_heap_arrays,
        size_to_u64(config.merge.crc_chunk_bytes),
        output_writer_buffers,
    });
    const std::uint64_t out_degree_write_phase = platform::checked_add(
        out_degree_bytes, size_to_u64(config.merge.writer_buffer_bytes)
    );
    const std::uint64_t vertex_copy_phase = add_many({
        size_to_u64(config.merge.reader_buffer_bytes),
        size_to_u64(config.merge.writer_buffer_bytes),
        size_to_u64(config.merge.crc_chunk_bytes),
    });
    const std::uint64_t validation_phase = add_many({
        platform::checked_multiply(
            2U, size_to_u64(config.validation_io_chunk_bytes)
        ),
        storage::kMaxManifestBytes,
    });
    const std::uint64_t cursor_policy_floor =
        cursor_plan.managed_bulk_upper_bound_bytes;
    const std::uint64_t managed_bulk = std::max({
        merge_phase,
        out_degree_write_phase,
        vertex_copy_phase,
        validation_phase,
        cursor_policy_floor,
    });
    const std::uint64_t max_open_files = std::max(
        platform::checked_add(frontier.run_count, 3U), std::uint64_t{6U}
    );

    const std::uint64_t predecessor_frontier = add_many({
        platform::checked_multiply(
            frontier.run_count, storage::kBinaryHeaderBytes
        ),
        platform::checked_multiply(
            frontier.record_count, kCompactEdgeRunRecordBytes
        ),
    });
    const std::uint64_t predecessor_vertex_ids = platform::checked_add(
        storage::kBinaryHeaderBytes, out_degree_bytes
    );
    const std::uint64_t staging = add_many({
        platform::checked_multiply(
            kGraphBinaryFileCount, storage::kBinaryHeaderBytes
        ),
        platform::checked_multiply(
            edge_count_upper_bound, storage::kScalarRecordBytes
        ),
        platform::checked_multiply(
            platform::checked_multiply(3U, vertex_count),
            storage::kScalarRecordBytes
        ),
        platform::checked_multiply(
            task_count_upper_bound, storage::kTaskRecordBytes
        ),
        storage::kMaxManifestBytes,
    });

    return CountedGraphResourcePlan{
        .vertex_count = vertex_count,
        .frontier_run_count = frontier.run_count,
        .frontier_record_count = frontier.record_count,
        .edge_count_upper_bound = edge_count_upper_bound,
        .task_count_upper_bound = task_count_upper_bound,
        .out_degree_bytes = out_degree_bytes,
        .edge_batch_bytes = edge_batch_bytes,
        .source_batch_bytes = source_batch_bytes,
        .cursor_reader_buffers_bytes = cursor_reader_buffers,
        .cursor_objects_bytes = cursor_objects,
        .cursor_heap_arrays_bytes = cursor_heap_arrays,
        .output_writer_buffers_bytes = output_writer_buffers,
        .merge_phase_upper_bound_bytes = merge_phase,
        .out_degree_write_phase_upper_bound_bytes = out_degree_write_phase,
        .vertex_copy_phase_upper_bound_bytes = vertex_copy_phase,
        .validation_phase_upper_bound_bytes = validation_phase,
        .cursor_policy_floor_bytes = cursor_policy_floor,
        .managed_bulk_upper_bound_bytes = managed_bulk,
        .max_open_files = max_open_files,
        .predecessor_frontier_logical_bytes = predecessor_frontier,
        .predecessor_vertex_ids_logical_bytes = predecessor_vertex_ids,
        .staging_logical_upper_bound_bytes = staging,
        .logical_peak_upper_bound_bytes = add_many({
            predecessor_frontier, predecessor_vertex_ids, staging,
        }),
    };
}

CountedGraphBuildResult build_and_publish_counted_graph(
    const std::filesystem::path& preprocess_workspace,
    const CompactEdgeMergeFrontier frontier,
    const std::filesystem::path& target,
    const CountedGraphConfig config
) {
    return build_impl(
        preprocess_workspace, frontier, target, config, nullptr
    );
}

CountedGraphBuildResult build_and_publish_counted_graph(
    const std::filesystem::path& preprocess_workspace,
    const CompactEdgeMergeFrontier frontier,
    const std::filesystem::path& target,
    const CountedGraphConfig config,
    platform::PublicationBackend& backend
) {
    return build_impl(
        preprocess_workspace, frontier, target, config, &backend
    );
}

}  // namespace tbank::preprocess

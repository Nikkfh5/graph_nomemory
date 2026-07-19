#include "tbank/preprocess/compact_edge_merge.hpp"

#include "tbank/platform/checked_io.hpp"
#include "tbank/preprocess/run_file.hpp"
#include "tbank/preprocess/vertex_id_merge.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/file_reader.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

namespace tbank::preprocess {
namespace {

static_assert(sizeof(CompactEdgeRecord) == kCompactEdgeRunRecordBytes);
static_assert(
    std::numeric_limits<std::size_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);
static_assert(
    std::numeric_limits<fsblkcnt_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);
static_assert(
    std::numeric_limits<fsfilcnt_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) noexcept {
    return static_cast<std::uint64_t>(value);
}

[[nodiscard]] bool edge_less(
    const CompactEdgeRecord& left,
    const CompactEdgeRecord& right
) noexcept {
    return left.destination < right.destination
        || (left.destination == right.destination
            && left.source < right.source);
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
        error_number,
        std::generic_category(),
        operation
    );
}

void validate_plan_config(const CompactEdgeMergeConfig& config) {
    if (config.fan_in < 2U) {
        throw std::invalid_argument(
            "compact edge merge fan_in must be at least 2"
        );
    }
    if (config.reader_buffer_bytes < kCompactEdgeRunRecordBytes
        || config.reader_buffer_bytes % kCompactEdgeRunRecordBytes != 0U) {
        throw std::invalid_argument(
            "compact edge merge reader buffer must be a positive multiple of 8"
        );
    }
    if (config.writer_buffer_bytes < kCompactEdgeRunRecordBytes) {
        throw std::invalid_argument(
            "compact edge merge writer buffer must hold one edge record"
        );
    }
    if (config.crc_chunk_bytes == 0U
        || config.crc_chunk_bytes > storage::kMaximumCrcChunkBytes) {
        throw std::invalid_argument(
            "compact edge merge CRC chunk must be in [1, 1 MiB]"
        );
    }
}

void validate_memory_allowance(const CompactEdgeMergeConfig& config) {
    if (config.managed_bulk_limit_bytes == 0U) {
        throw std::invalid_argument(
            "compact edge merge managed bulk limit must be explicitly supplied"
        );
    }
    if (config.managed_bulk_limit_bytes
        > kCompactEdgeHardMemoryBudgetBytes) {
        throw std::invalid_argument(
            "compact edge merge managed bulk limit exceeds hard budget"
        );
    }
}

[[nodiscard]] std::filesystem::path validate_workspace(
    const std::filesystem::path& workspace
) {
    if (workspace.empty()) {
        throw std::invalid_argument("compact edge merge workspace is empty");
    }
    if (contains_embedded_nul(workspace)) {
        throw std::invalid_argument(
            "compact edge merge workspace contains an embedded NUL"
        );
    }

    std::filesystem::path absolute_workspace =
        std::filesystem::absolute(workspace);
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(absolute_workspace.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "inspect compact edge merge workspace");
    }
    if (!S_ISDIR(status.st_mode)) {
        throw std::invalid_argument(
            "compact edge merge workspace must be a real directory"
        );
    }
    return absolute_workspace;
}

void require_absent(
    const std::filesystem::path& path,
    const char* const operation
) {
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(path.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == 0) {
        throw_posix_error(EEXIST, operation);
    }
    if (errno != ENOENT) {
        const int error_number = errno;
        throw_posix_error(error_number, operation);
    }
}

void unlink_checked(
    const std::filesystem::path& path,
    const char* const operation
) {
    int result = -1;
    do {
        errno = 0;
        result = ::unlink(path.c_str());
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, operation);
    }
}

void rmdir_checked(
    const std::filesystem::path& path,
    const char* const operation
) {
    int result = -1;
    do {
        errno = 0;
        result = ::rmdir(path.c_str());
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, operation);
    }
}

void create_private_directory(const std::filesystem::path& path) {
    errno = 0;
    if (::mkdir(path.c_str(), 0700) == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "create compact edge merge generation");
    }
}

[[nodiscard]] std::uint64_t successor_run_count(
    const std::uint64_t input_run_count,
    const std::size_t fan_in
) noexcept {
    const std::uint64_t divisor = size_to_u64(fan_in);
    return input_run_count / divisor
        + static_cast<std::uint64_t>(input_run_count % divisor != 0U);
}

[[nodiscard]] std::size_t group_size(
    const std::uint64_t remaining,
    const std::size_t fan_in
) noexcept {
    return static_cast<std::size_t>(
        std::min(remaining, size_to_u64(fan_in))
    );
}

struct CompactRunSet {
    std::filesystem::path workspace;
    std::uint64_t generation_index = 0U;
    std::uint64_t run_count = 0U;
};

[[nodiscard]] std::filesystem::path run_set_path(
    const CompactRunSet& set,
    const std::uint64_t run_index
) {
    if (run_index >= set.run_count) {
        throw std::logic_error("compact edge run index is out of range");
    }
    return compact_edge_run_path(
        set.workspace, set.generation_index, run_index
    );
}

void cleanup_run_set_noexcept(
    const CompactRunSet& set,
    const std::uint64_t created_count
) noexcept {
    for (std::uint64_t index = 0U; index < created_count; ++index) {
        try {
            static_cast<void>(::unlink(run_set_path(set, index).c_str()));
        } catch (...) {
            // Continue cleanup.
        }
    }
    try {
        static_cast<void>(::rmdir(
            compact_edge_generation_path(
                set.workspace, set.generation_index
            ).c_str()
        ));
    } catch (...) {
        // Preserve the primary failure.
    }
}

void remove_run_set_checked(const CompactRunSet& set) {
    for (std::uint64_t index = 0U; index < set.run_count; ++index) {
        unlink_checked(
            run_set_path(set, index),
            "remove retired compact edge run"
        );
    }
    rmdir_checked(
        compact_edge_generation_path(set.workspace, set.generation_index),
        "remove retired compact edge generation"
    );
}

class CreatedRunSetGuard final {
public:
    explicit CreatedRunSetGuard(CompactRunSet set)
        : set_(std::move(set)) {}

    CreatedRunSetGuard(const CreatedRunSetGuard&) = delete;
    CreatedRunSetGuard& operator=(const CreatedRunSetGuard&) = delete;

    ~CreatedRunSetGuard() noexcept {
        if (active_) {
            cleanup_run_set_noexcept(set_, created_count_);
        }
    }

    void arm() noexcept {
        active_ = true;
    }

    void note_created() noexcept {
        ++created_count_;
    }

    void commit() noexcept {
        active_ = false;
    }

private:
    CompactRunSet set_;
    std::uint64_t created_count_ = 0U;
    bool active_ = false;
};

void preflight_successor_names(
    const std::filesystem::path& workspace,
    std::uint64_t generation_index,
    std::uint64_t run_count,
    const std::size_t fan_in
) {
    const std::uint64_t fan_in_u64 = size_to_u64(fan_in);
    while (run_count > fan_in_u64) {
        generation_index = platform::checked_add(generation_index, 1U);
        require_absent(
            compact_edge_generation_path(workspace, generation_index),
            "compact edge merge successor generation already exists"
        );
        run_count = successor_run_count(run_count, fan_in);
    }
}

void preflight_disk_space(
    const std::filesystem::path& workspace,
    const CompactEdgeMergeDiskPlan& plan,
    const CompactEdgeMergeConfig& config
) {
    struct statvfs status {};
    int result = -1;
    do {
        errno = 0;
        result = ::statvfs(workspace.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "inspect compact edge merge filesystem");
    }

    const std::uint64_t fragment_bytes = status.f_frsize != 0U
        ? static_cast<std::uint64_t>(status.f_frsize)
        : static_cast<std::uint64_t>(status.f_bsize);
    if (fragment_bytes == 0U) {
        throw std::runtime_error(
            "compact edge merge filesystem reports zero allocation unit"
        );
    }
    const std::uint64_t available_bytes = platform::checked_multiply(
        static_cast<std::uint64_t>(status.f_bavail), fragment_bytes
    );
    const std::uint64_t rounding_slack = platform::checked_multiply(
        plan.output_run_count, fragment_bytes - 1U
    );
    const std::uint64_t successor_entries = platform::checked_add(
        plan.output_run_count, 1U
    );
    const std::uint64_t directory_allowance = platform::checked_multiply(
        successor_entries, fragment_bytes
    );
    std::uint64_t required_bytes = platform::checked_add(
        plan.successor_logical_upper_bound_bytes, rounding_slack
    );
    required_bytes = platform::checked_add(
        required_bytes, directory_allowance
    );
    required_bytes = platform::checked_add(
        required_bytes, config.minimum_free_space_reserve_bytes
    );
    if (required_bytes > available_bytes) {
        throw CompactEdgeDiskSpaceError(required_bytes, available_bytes);
    }
    if (status.f_files != 0U
        && successor_entries
            > static_cast<std::uint64_t>(status.f_favail)) {
        throw std::runtime_error(
            "compact edge merge filesystem has too few available inodes"
        );
    }
}

class BufferedEdgeCursor final {
public:
    BufferedEdgeCursor(
        const std::filesystem::path& path,
        const std::uint32_t vertex_count,
        const CompactEdgeMergeConfig& config
    )
        : reader_(storage::ValidatedBinaryFileReader::open(
              path,
              kCompactEdgeRunMagic,
              kCompactEdgeRunRecordBytes,
              config.crc_chunk_bytes
          )),
          buffer_(config.reader_buffer_bytes),
          record_count_(reader_.header().record_count),
          vertex_count_(vertex_count) {
        if (record_count_ == 0U) {
            throw storage::BinaryError("compact edge merge input run is empty");
        }
    }

    BufferedEdgeCursor(const BufferedEdgeCursor&) = delete;
    BufferedEdgeCursor& operator=(const BufferedEdgeCursor&) = delete;
    BufferedEdgeCursor(BufferedEdgeCursor&&) noexcept = default;
    BufferedEdgeCursor& operator=(BufferedEdgeCursor&&) noexcept = default;

    [[nodiscard]] std::optional<CompactEdgeRecord> next() {
        if (buffer_index_ == buffered_records_) {
            refill();
        }
        if (buffer_index_ == buffered_records_) {
            return std::nullopt;
        }

        const std::size_t byte_offset =
            buffer_index_ * kCompactEdgeRunRecordBytes;
        const CompactEdgeRecord edge = decode_compact_edge_record(
            std::span<const std::byte>(buffer_).subspan(
                byte_offset, kCompactEdgeRunRecordBytes
            )
        );
        ++buffer_index_;
        const std::uint64_t record_index = records_seen_;
        records_seen_ = platform::checked_add(records_seen_, 1U);
        if (edge.destination >= vertex_count_) {
            throw storage::BinaryError(
                "compact edge destination is outside vertex count at record "
                + std::to_string(record_index)
            );
        }
        if (edge.source >= vertex_count_) {
            throw storage::BinaryError(
                "compact edge source is outside vertex count at record "
                + std::to_string(record_index)
            );
        }
        if (previous_.has_value() && !edge_less(*previous_, edge)) {
            throw storage::BinaryError(
                "compact edge input run is not strictly ordered at record "
                + std::to_string(record_index)
            );
        }
        previous_ = edge;
        return edge;
    }

    [[nodiscard]] std::uint64_t record_count() const noexcept {
        return record_count_;
    }

private:
    void refill() {
        if (next_record_ == record_count_) {
            buffer_index_ = 0U;
            buffered_records_ = 0U;
            return;
        }
        const std::size_t capacity_records =
            buffer_.size() / kCompactEdgeRunRecordBytes;
        const std::uint64_t remaining = record_count_ - next_record_;
        const std::size_t requested_records = static_cast<std::size_t>(
            std::min(remaining, size_to_u64(capacity_records))
        );
        const std::size_t requested_bytes =
            requested_records * kCompactEdgeRunRecordBytes;
        reader_.read_records(
            next_record_,
            std::span<std::byte>(buffer_).first(requested_bytes)
        );
        next_record_ = platform::checked_add(
            next_record_, size_to_u64(requested_records)
        );
        buffer_index_ = 0U;
        buffered_records_ = requested_records;
    }

    storage::ValidatedBinaryFileReader reader_;
    std::vector<std::byte> buffer_;
    std::uint64_t record_count_ = 0U;
    std::uint64_t next_record_ = 0U;
    std::uint64_t records_seen_ = 0U;
    std::uint32_t vertex_count_ = 0U;
    std::size_t buffer_index_ = 0U;
    std::size_t buffered_records_ = 0U;
    std::optional<CompactEdgeRecord> previous_{};
};

struct HeapNode {
    CompactEdgeRecord edge{};
    std::size_t cursor_index = 0U;
};

class FixedEdgeMinHeap final {
public:
    explicit FixedEdgeMinHeap(const std::size_t capacity)
        : edges_(std::make_unique<CompactEdgeRecord[]>(capacity)),
          cursor_indices_(std::make_unique<std::size_t[]>(capacity)),
          capacity_(capacity) {}

    void push(const HeapNode node) {
        if (size_ >= capacity_) {
            throw std::logic_error("compact edge heap capacity exceeded");
        }
        std::size_t position = size_;
        edges_[position] = node.edge;
        cursor_indices_[position] = node.cursor_index;
        ++size_;
        peak_size_ = std::max(peak_size_, size_);
        while (position > 0U) {
            const std::size_t parent = (position - 1U) / 2U;
            if (!less(position, parent)) {
                break;
            }
            swap_nodes(position, parent);
            position = parent;
        }
    }

    [[nodiscard]] HeapNode pop() {
        if (size_ == 0U) {
            throw std::logic_error("cannot pop an empty compact edge heap");
        }
        const HeapNode result{
            .edge = edges_[0],
            .cursor_index = cursor_indices_[0],
        };
        --size_;
        if (size_ == 0U) {
            return result;
        }
        edges_[0] = edges_[size_];
        cursor_indices_[0] = cursor_indices_[size_];
        std::size_t position = 0U;
        while (true) {
            if (size_ <= 1U || position > (size_ - 2U) / 2U) {
                break;
            }
            const std::size_t left = position * 2U + 1U;
            const std::size_t right = left + 1U;
            std::size_t selected = left;
            if (right < size_ && less(right, left)) {
                selected = right;
            }
            if (!less(selected, position)) {
                break;
            }
            swap_nodes(position, selected);
            position = selected;
        }
        return result;
    }

    [[nodiscard]] bool empty() const noexcept {
        return size_ == 0U;
    }

    [[nodiscard]] std::size_t peak_size() const noexcept {
        return peak_size_;
    }

private:
    [[nodiscard]] bool less(
        const std::size_t left,
        const std::size_t right
    ) const noexcept {
        return edge_less(edges_[left], edges_[right])
            || (edges_[left] == edges_[right]
                && cursor_indices_[left] < cursor_indices_[right]);
    }

    void swap_nodes(const std::size_t left, const std::size_t right) noexcept {
        std::swap(edges_[left], edges_[right]);
        std::swap(cursor_indices_[left], cursor_indices_[right]);
    }

    std::unique_ptr<CompactEdgeRecord[]> edges_;
    std::unique_ptr<std::size_t[]> cursor_indices_;
    std::size_t capacity_ = 0U;
    std::size_t size_ = 0U;
    std::size_t peak_size_ = 0U;
};

struct GroupMergeResult {
    RunFileInfo output{};
    std::uint64_t input_record_count = 0U;
};

[[nodiscard]] GroupMergeResult merge_group(
    const CompactRunSet& inputs,
    const std::uint64_t input_begin,
    const std::size_t input_count,
    const std::filesystem::path& output_path,
    const std::uint32_t vertex_count,
    const CompactEdgeMergeConfig& config,
    CompactEdgeReduceTelemetry& telemetry
) {
    std::vector<BufferedEdgeCursor> cursors;
    cursors.reserve(input_count);
    std::uint64_t input_records = 0U;
    for (std::size_t local_index = 0U;
         local_index < input_count;
         ++local_index) {
        const std::uint64_t run_index = platform::checked_add(
            input_begin, size_to_u64(local_index)
        );
        cursors.emplace_back(
            run_set_path(inputs, run_index), vertex_count, config
        );
        input_records = platform::checked_add(
            input_records, cursors.back().record_count()
        );
    }
    telemetry.peak_open_input_runs = std::max(
        telemetry.peak_open_input_runs, cursors.size()
    );

    FixedEdgeMinHeap heap(cursors.size());
    for (std::size_t index = 0U; index < cursors.size(); ++index) {
        const std::optional<CompactEdgeRecord> edge = cursors[index].next();
        if (!edge.has_value()) {
            throw storage::BinaryError("compact edge merge input run is empty");
        }
        heap.push(HeapNode{.edge = *edge, .cursor_index = index});
    }

    bool output_created = false;
    try {
        RunFileWriter writer = RunFileWriter::create(
            output_path,
            kCompactEdgeRunMagic,
            kCompactEdgeRunRecordBytes,
            config.writer_buffer_bytes
        );
        output_created = true;
        std::optional<CompactEdgeRecord> last_emitted;
        while (!heap.empty()) {
            const HeapNode node = heap.pop();
            if (!last_emitted.has_value() || node.edge != *last_emitted) {
                writer.append_records(encode_compact_edge_record(node.edge));
                last_emitted = node.edge;
            }
            const std::optional<CompactEdgeRecord> next =
                cursors[node.cursor_index].next();
            if (next.has_value()) {
                heap.push(HeapNode{
                    .edge = *next,
                    .cursor_index = node.cursor_index,
                });
            }
        }

        const RunFileInfo info = writer.finish();
        if (info.header.record_count == 0U) {
            throw storage::BinaryError(
                "compact edge merge produced empty successor run"
            );
        }
        telemetry.peak_heap_items = std::max(
            telemetry.peak_heap_items, heap.peak_size()
        );
        telemetry.max_writer_peak_buffered_bytes = std::max(
            telemetry.max_writer_peak_buffered_bytes,
            info.peak_buffered_bytes
        );
        return GroupMergeResult{
            .output = info,
            .input_record_count = input_records,
        };
    } catch (...) {
        if (output_created) {
            static_cast<void>(::unlink(output_path.c_str()));
        }
        throw;
    }
}

[[nodiscard]] std::uint64_t verify_generation(
    const CompactRunSet& set,
    const std::uint32_t vertex_count,
    const CompactEdgeMergeConfig& config
) {
    std::uint64_t total_records = 0U;
    for (std::uint64_t run_index = 0U;
         run_index < set.run_count;
         ++run_index) {
        BufferedEdgeCursor cursor(
            run_set_path(set, run_index), vertex_count, config
        );
        std::uint64_t seen = 0U;
        while (cursor.next().has_value()) {
            seen = platform::checked_add(seen, 1U);
        }
        if (seen != cursor.record_count()) {
            throw storage::BinaryError(
                "compact edge verification count diverged"
            );
        }
        total_records = platform::checked_add(total_records, seen);
    }
    return total_records;
}

}  // namespace

CompactEdgeMergeMemoryPlan compact_edge_merge_memory_plan(
    const CompactEdgeMergeConfig& config
) {
    validate_plan_config(config);
    const std::uint64_t fan_in = size_to_u64(config.fan_in);
    const std::uint64_t reader_buffers = platform::checked_multiply(
        fan_in, size_to_u64(config.reader_buffer_bytes)
    );
    const std::uint64_t cursor_object_bytes = sizeof(BufferedEdgeCursor);
    const std::uint64_t cursor_objects = platform::checked_multiply(
        fan_in, cursor_object_bytes
    );
    const std::uint64_t heap_entry_bytes = platform::checked_add(
        sizeof(CompactEdgeRecord), sizeof(std::size_t)
    );
    const std::uint64_t heap_arrays = platform::checked_multiply(
        fan_in, heap_entry_bytes
    );
    std::uint64_t managed = platform::checked_add(
        reader_buffers, size_to_u64(config.writer_buffer_bytes)
    );
    managed = platform::checked_add(managed, cursor_objects);
    managed = platform::checked_add(managed, heap_arrays);
    managed = platform::checked_add(
        managed, size_to_u64(config.crc_chunk_bytes)
    );
    return CompactEdgeMergeMemoryPlan{
        .reader_buffers_bytes = reader_buffers,
        .writer_buffer_bytes = size_to_u64(config.writer_buffer_bytes),
        .cursor_object_bytes = cursor_object_bytes,
        .cursor_objects_bytes = cursor_objects,
        .heap_arrays_bytes = heap_arrays,
        .crc_buffer_bytes = size_to_u64(config.crc_chunk_bytes),
        .managed_bulk_upper_bound_bytes = managed,
        .reduction_max_open_files = platform::checked_add(fan_in, 1U),
        .cursor_max_open_files = fan_in,
    };
}

CompactEdgeMergeDiskPlan compact_edge_merge_disk_plan(
    const std::uint64_t vertex_count,
    const std::uint64_t input_run_count,
    const std::uint64_t input_record_count,
    const CompactEdgeMergeConfig& config
) {
    validate_plan_config(config);
    if (vertex_count == 0U) {
        throw std::invalid_argument("compact edge merge vertex count is zero");
    }
    if (input_run_count == 0U) {
        throw std::invalid_argument("compact edge merge run count is zero");
    }
    if (input_record_count == 0U) {
        throw std::invalid_argument("compact edge merge record count is zero");
    }
    const std::uint64_t output_run_count = successor_run_count(
        input_run_count, config.fan_in
    );
    const std::uint64_t payload_bytes = platform::checked_multiply(
        input_record_count, kCompactEdgeRunRecordBytes
    );
    const std::uint64_t maximum_output_file_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes, payload_bytes
    );
    static_cast<void>(
        platform::checked_u64_to_off_t(maximum_output_file_bytes)
    );
    const std::uint64_t vertex_payload_bytes = platform::checked_multiply(
        vertex_count, storage::kScalarRecordBytes
    );
    const std::uint64_t vertex_ids_file_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes, vertex_payload_bytes
    );
    const std::uint64_t predecessor_headers = platform::checked_multiply(
        input_run_count, storage::kBinaryHeaderBytes
    );
    const std::uint64_t successor_headers = platform::checked_multiply(
        output_run_count, storage::kBinaryHeaderBytes
    );
    const std::uint64_t predecessor_logical = platform::checked_add(
        predecessor_headers, payload_bytes
    );
    const std::uint64_t successor_logical = platform::checked_add(
        successor_headers, payload_bytes
    );
    std::uint64_t logical_peak = platform::checked_add(
        vertex_ids_file_bytes, predecessor_logical
    );
    logical_peak = platform::checked_add(logical_peak, successor_logical);
    return CompactEdgeMergeDiskPlan{
        .input_run_count = input_run_count,
        .output_run_count = output_run_count,
        .input_record_count = input_record_count,
        .vertex_ids_file_bytes = vertex_ids_file_bytes,
        .predecessor_logical_bytes = predecessor_logical,
        .successor_logical_upper_bound_bytes = successor_logical,
        .logical_peak_upper_bound_bytes = logical_peak,
    };
}

CompactEdgeReductionProgressError::CompactEdgeReductionProgressError(
    const CompactEdgeMergeFrontier recovery_frontier,
    std::exception_ptr cause
)
    : std::runtime_error(
          "compact edge reduction stopped after verified progress at generation "
          + std::to_string(recovery_frontier.generation_index)
      ),
      recovery_frontier_(recovery_frontier),
      cause_(std::move(cause)) {
    if (!cause_) {
        throw std::invalid_argument(
            "compact edge reduction progress error requires a cause"
        );
    }
}

CompactEdgeMergeFrontier
CompactEdgeReductionProgressError::recovery_frontier() const noexcept {
    return recovery_frontier_;
}

void CompactEdgeReductionProgressError::rethrow_cause() const {
    std::rethrow_exception(cause_);
}

CompactEdgeReduceResult reduce_compact_edge_runs(
    const std::filesystem::path& workspace,
    const std::uint64_t initial_run_count,
    const std::uint64_t expected_initial_record_count,
    const std::uint64_t expected_vertex_count,
    const CompactEdgeMergeConfig config
) {
    if (initial_run_count == 0U) {
        throw std::invalid_argument(
            "compact edge reduction requires at least one run"
        );
    }
    if (expected_initial_record_count == 0U) {
        throw std::invalid_argument(
            "compact edge reduction requires at least one record"
        );
    }
    return reduce_compact_edge_runs(
        workspace,
        CompactEdgeMergeFrontier{
            .generation_index = kCompactEdgeInitialGeneration,
            .run_count = initial_run_count,
            .record_count = expected_initial_record_count,
            .initial_record_count = expected_initial_record_count,
            .vertex_count = checked_compact_vertex_count(expected_vertex_count),
        },
        config
    );
}

CompactEdgeReduceResult reduce_compact_edge_runs(
    const std::filesystem::path& workspace,
    const CompactEdgeMergeFrontier starting_frontier,
    const CompactEdgeMergeConfig config
) {
    validate_plan_config(config);
    validate_memory_allowance(config);
    if (starting_frontier.run_count == 0U) {
        throw std::invalid_argument(
            "compact edge reduction requires at least one run"
        );
    }
    if (starting_frontier.record_count == 0U
        || starting_frontier.initial_record_count
            < starting_frontier.record_count) {
        throw std::invalid_argument(
            "compact edge reduction frontier record counts are invalid"
        );
    }
    if (starting_frontier.vertex_count == 0U) {
        throw std::invalid_argument(
            "compact edge reduction frontier vertex count is zero"
        );
    }
    const std::uint32_t vertex_count = starting_frontier.vertex_count;
    const CompactEdgeMergeMemoryPlan memory_plan =
        compact_edge_merge_memory_plan(config);
    if (memory_plan.managed_bulk_upper_bound_bytes
        > config.managed_bulk_limit_bytes) {
        throw CompactEdgeMemoryLimitError(
            memory_plan.managed_bulk_upper_bound_bytes,
            config.managed_bulk_limit_bytes
        );
    }
    if (config.phase_fd_budget < memory_plan.reduction_max_open_files) {
        throw std::invalid_argument(
            "compact edge merge phase FD budget is below fan_in + 1"
        );
    }

    const std::filesystem::path absolute_workspace =
        validate_workspace(workspace);
    preflight_successor_names(
        absolute_workspace,
        starting_frontier.generation_index,
        starting_frontier.run_count,
        config.fan_in
    );

    CompactRunSet current{
        .workspace = absolute_workspace,
        .generation_index = starting_frontier.generation_index,
        .run_count = starting_frontier.run_count,
    };
    std::uint64_t current_record_count = starting_frontier.record_count;
    const std::uint64_t starting_record_count = current_record_count;
    CompactEdgeReduceTelemetry telemetry{};
    const std::uint64_t fan_in = size_to_u64(config.fan_in);

    const std::uint64_t verified_start = verify_generation(
        current, vertex_count, config
    );
    if (verified_start != current_record_count) {
        throw storage::BinaryError(
            "compact edge frontier count differs from expected count"
        );
    }

    CompactEdgeMergeFrontier recovery_frontier = starting_frontier;
    bool made_verified_progress = false;
    try {
        while (current.run_count > fan_in) {
            const CompactEdgeMergeDiskPlan disk_plan =
                compact_edge_merge_disk_plan(
                    vertex_count,
                    current.run_count,
                    current_record_count,
                    config
                );
            preflight_disk_space(absolute_workspace, disk_plan, config);
            telemetry.max_successor_logical_upper_bound_bytes = std::max(
                telemetry.max_successor_logical_upper_bound_bytes,
                disk_plan.successor_logical_upper_bound_bytes
            );

            const std::uint64_t next_generation = platform::checked_add(
                current.generation_index, 1U
            );
            CompactRunSet successor{
                .workspace = absolute_workspace,
                .generation_index = next_generation,
                .run_count = disk_plan.output_run_count,
            };
            CreatedRunSetGuard guard(successor);
            create_private_directory(compact_edge_generation_path(
                absolute_workspace, next_generation
            ));
            guard.arm();

            std::uint64_t input_begin = 0U;
            std::uint64_t merged_input_records = 0U;
            std::uint64_t output_records = 0U;
            for (std::uint64_t output_index = 0U;
                 output_index < successor.run_count;
                 ++output_index) {
                const std::size_t count = group_size(
                    current.run_count - input_begin, config.fan_in
                );
                const GroupMergeResult group = merge_group(
                    current,
                    input_begin,
                    count,
                    run_set_path(successor, output_index),
                    vertex_count,
                    config,
                    telemetry
                );
                guard.note_created();
                merged_input_records = platform::checked_add(
                    merged_input_records, group.input_record_count
                );
                output_records = platform::checked_add(
                    output_records, group.output.header.record_count
                );
                input_begin = platform::checked_add(
                    input_begin, size_to_u64(count)
                );
            }
            if (input_begin != current.run_count) {
                throw std::logic_error(
                    "compact edge pass did not consume every input run"
                );
            }
            if (merged_input_records != current_record_count) {
                throw storage::BinaryError(
                    "compact edge pass input count differs from expected count"
                );
            }
            const std::uint64_t verified_output_records = verify_generation(
                successor, vertex_count, config
            );
            if (verified_output_records != output_records) {
                throw storage::BinaryError(
                    "compact edge successor verification count diverged"
                );
            }

            telemetry.merge_pass_count = platform::checked_add(
                telemetry.merge_pass_count, 1U
            );
            telemetry.intermediate_run_count = platform::checked_add(
                telemetry.intermediate_run_count, successor.run_count
            );
            guard.commit();
            recovery_frontier = CompactEdgeMergeFrontier{
                .generation_index = successor.generation_index,
                .run_count = successor.run_count,
                .record_count = output_records,
                .initial_record_count =
                    starting_frontier.initial_record_count,
                .vertex_count = vertex_count,
            };
            made_verified_progress = true;
            remove_run_set_checked(current);
            current = std::move(successor);
            current_record_count = output_records;
        }
    } catch (...) {
        if (!made_verified_progress) {
            throw;
        }
        throw CompactEdgeReductionProgressError(
            recovery_frontier, std::current_exception()
        );
    }

    if (current_record_count > starting_record_count) {
        throw std::logic_error(
            "compact edge reduction increased the record count"
        );
    }
    telemetry.intermediate_duplicates_removed =
        starting_record_count - current_record_count;
    return CompactEdgeReduceResult{
        .frontier = CompactEdgeMergeFrontier{
            .generation_index = current.generation_index,
            .run_count = current.run_count,
            .record_count = current_record_count,
            .initial_record_count = starting_frontier.initial_record_count,
            .vertex_count = vertex_count,
        },
        .telemetry = telemetry,
        .memory_plan = memory_plan,
    };
}

class UniqueCompactEdgeCursor::Impl final {
public:
    Impl(
        const std::filesystem::path& workspace,
        const CompactEdgeMergeFrontier frontier,
        const CompactEdgeMergeConfig& config
    )
        : frontier_(frontier),
          heap_(static_cast<std::size_t>(frontier.run_count)) {
        cursors_.reserve(static_cast<std::size_t>(frontier.run_count));
        std::uint64_t header_records = 0U;
        for (std::uint64_t run_index = 0U;
             run_index < frontier.run_count;
             ++run_index) {
            cursors_.emplace_back(
                compact_edge_run_path(
                    workspace, frontier.generation_index, run_index
                ),
                frontier.vertex_count,
                config
            );
            header_records = platform::checked_add(
                header_records, cursors_.back().record_count()
            );
        }
        if (header_records != frontier.record_count) {
            throw storage::BinaryError(
                "compact edge cursor frontier count differs from headers"
            );
        }
        for (std::size_t index = 0U; index < cursors_.size(); ++index) {
            const std::optional<CompactEdgeRecord> edge = cursors_[index].next();
            if (!edge.has_value()) {
                throw storage::BinaryError(
                    "compact edge cursor input run is empty"
                );
            }
            heap_.push(HeapNode{.edge = *edge, .cursor_index = index});
        }
    }

    [[nodiscard]] std::size_t read(
        const std::span<CompactEdgeRecord> destination
    ) {
        if (destination.empty()) {
            throw std::invalid_argument(
                "compact edge cursor destination must be nonempty"
            );
        }
        ensure_usable();
        if (drained_) {
            eof_observed_ = true;
            return 0U;
        }
        try {
            std::size_t emitted = 0U;
            while (emitted < destination.size() && !heap_.empty()) {
                const HeapNode node = heap_.pop();
                consumed_records_ = platform::checked_add(
                    consumed_records_, 1U
                );
                const std::optional<CompactEdgeRecord> next =
                    cursors_[node.cursor_index].next();
                if (next.has_value()) {
                    heap_.push(HeapNode{
                        .edge = *next,
                        .cursor_index = node.cursor_index,
                    });
                }
                if (!last_emitted_.has_value()
                    || node.edge != *last_emitted_) {
                    destination[emitted] = node.edge;
                    ++emitted;
                    unique_records_ = platform::checked_add(
                        unique_records_, 1U
                    );
                    last_emitted_ = node.edge;
                }
            }
            if (heap_.empty()) {
                if (consumed_records_ != frontier_.record_count) {
                    throw storage::BinaryError(
                        "compact edge cursor consumed count diverged"
                    );
                }
                drained_ = true;
                if (emitted == 0U) {
                    eof_observed_ = true;
                }
            }
            return emitted;
        } catch (...) {
            if (!failure_) {
                failure_ = std::current_exception();
            }
            std::rethrow_exception(failure_);
        }
    }

    [[nodiscard]] CompactEdgeUniqueSummary summary() const {
        ensure_usable();
        if (!eof_observed_) {
            throw std::logic_error(
                "compact edge cursor summary requires observed EOF"
            );
        }
        return CompactEdgeUniqueSummary{
            .input_record_count = frontier_.record_count,
            .unique_record_count = unique_records_,
            .duplicate_record_count =
                frontier_.record_count - unique_records_,
        };
    }

private:
    void ensure_usable() const {
        if (failure_) {
            std::rethrow_exception(failure_);
        }
    }

    CompactEdgeMergeFrontier frontier_{};
    std::vector<BufferedEdgeCursor> cursors_{};
    FixedEdgeMinHeap heap_;
    std::optional<CompactEdgeRecord> last_emitted_{};
    std::uint64_t consumed_records_ = 0U;
    std::uint64_t unique_records_ = 0U;
    bool drained_ = false;
    bool eof_observed_ = false;
    std::exception_ptr failure_{};
};

UniqueCompactEdgeCursor UniqueCompactEdgeCursor::open(
    const std::filesystem::path& workspace,
    const CompactEdgeMergeFrontier frontier,
    const CompactEdgeMergeConfig config
) {
    validate_plan_config(config);
    validate_memory_allowance(config);
    if (frontier.run_count == 0U
        || frontier.run_count > size_to_u64(config.fan_in)) {
        throw std::invalid_argument(
            "compact edge cursor frontier run count is outside fan_in"
        );
    }
    if (frontier.record_count == 0U
        || frontier.initial_record_count < frontier.record_count) {
        throw std::invalid_argument(
            "compact edge cursor frontier record counts are invalid"
        );
    }
    if (frontier.vertex_count == 0U) {
        throw std::invalid_argument(
            "compact edge cursor vertex count is zero"
        );
    }
    const CompactEdgeMergeMemoryPlan memory_plan =
        compact_edge_merge_memory_plan(config);
    if (memory_plan.managed_bulk_upper_bound_bytes
        > config.managed_bulk_limit_bytes) {
        throw CompactEdgeMemoryLimitError(
            memory_plan.managed_bulk_upper_bound_bytes,
            config.managed_bulk_limit_bytes
        );
    }
    if (config.phase_fd_budget < frontier.run_count) {
        throw std::invalid_argument(
            "compact edge cursor phase FD budget is below frontier run count"
        );
    }
    const std::filesystem::path absolute_workspace =
        validate_workspace(workspace);
    return UniqueCompactEdgeCursor(std::make_unique<Impl>(
        absolute_workspace, frontier, config
    ));
}

UniqueCompactEdgeCursor::UniqueCompactEdgeCursor(
    std::unique_ptr<Impl> impl
) noexcept
    : impl_(std::move(impl)) {}

UniqueCompactEdgeCursor::UniqueCompactEdgeCursor(
    UniqueCompactEdgeCursor&&
) noexcept = default;

UniqueCompactEdgeCursor& UniqueCompactEdgeCursor::operator=(
    UniqueCompactEdgeCursor&&
) noexcept = default;

UniqueCompactEdgeCursor::~UniqueCompactEdgeCursor() = default;

std::size_t UniqueCompactEdgeCursor::read(
    const std::span<CompactEdgeRecord> destination
) {
    if (impl_ == nullptr) {
        throw std::logic_error("compact edge cursor was moved from");
    }
    return impl_->read(destination);
}

CompactEdgeUniqueSummary UniqueCompactEdgeCursor::summary() const {
    if (impl_ == nullptr) {
        throw std::logic_error("compact edge cursor was moved from");
    }
    return impl_->summary();
}

}  // namespace tbank::preprocess

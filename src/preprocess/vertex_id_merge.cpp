#include "tbank/preprocess/vertex_id_merge.hpp"

#include "tbank/platform/checked_io.hpp"
#include "tbank/preprocess/ingest.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/file_reader.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

namespace tbank::preprocess {
namespace {

constexpr std::string_view kGenerationPrefix = "id_merge.pass.";
constexpr std::string_view kVertexIdsName = "vertex_ids.bin";
constexpr std::size_t kIndexDigits = 20U;

static_assert(
    std::numeric_limits<std::size_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);
static_assert(
    kEndpointIdRunRecordBytes == storage::kScalarRecordBytes,
    "endpoint and persistent vertex-ID records must both be int32"
);

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) noexcept {
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
        error_number,
        std::generic_category(),
        operation
    );
}

[[nodiscard]] std::string format_index(
    const std::string_view prefix,
    const std::uint64_t index
) {
    std::array<char, kIndexDigits> digits{};
    std::array<char, kIndexDigits> unpadded{};
    const auto conversion = std::to_chars(
        unpadded.data(),
        unpadded.data() + unpadded.size(),
        index
    );
    if (conversion.ec != std::errc{}) {
        throw std::runtime_error("failed to format merge index");
    }
    const std::size_t digit_count = static_cast<std::size_t>(
        conversion.ptr - unpadded.data()
    );
    if (digit_count > digits.size()) {
        throw std::overflow_error("merge index exceeds filename width");
    }

    const std::size_t padding = digits.size() - digit_count;
    std::fill_n(digits.data(), padding, '0');
    std::copy_n(unpadded.data(), digit_count, digits.data() + padding);

    std::string name;
    name.reserve(prefix.size() + digits.size());
    name.append(prefix);
    name.append(digits.data(), digits.size());
    return name;
}

void validate_config(const VertexIdMergeConfig& config) {
    if (config.fan_in < 2U) {
        throw std::invalid_argument("vertex-ID merge fan_in must be at least 2");
    }
    if (config.reader_buffer_bytes < kEndpointIdRunRecordBytes
        || config.reader_buffer_bytes % kEndpointIdRunRecordBytes != 0U) {
        throw std::invalid_argument(
            "vertex-ID merge reader buffer must be a positive multiple of 4"
        );
    }
    if (config.writer_buffer_bytes < kEndpointIdRunRecordBytes) {
        throw std::invalid_argument(
            "vertex-ID merge writer buffer must hold one int32 record"
        );
    }
    if (config.crc_chunk_bytes == 0U
        || config.crc_chunk_bytes > storage::kMaximumCrcChunkBytes) {
        throw std::invalid_argument(
            "vertex-ID merge CRC chunk must be in [1, 1 MiB]"
        );
    }
}

[[nodiscard]] std::filesystem::path validate_workspace(
    const std::filesystem::path& workspace
) {
    if (workspace.empty()) {
        throw std::invalid_argument("vertex-ID merge workspace is empty");
    }
    if (contains_embedded_nul(workspace)) {
        throw std::invalid_argument(
            "vertex-ID merge workspace contains an embedded NUL"
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
        throw_posix_error(error_number, "inspect vertex-ID merge workspace");
    }
    if (!S_ISDIR(status.st_mode)) {
        throw std::invalid_argument(
            "vertex-ID merge workspace must be a real directory"
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

enum class RunSetKind {
    initial,
    intermediate,
    final_vertex_ids,
};

struct RunSet {
    std::filesystem::path workspace;
    RunSetKind kind = RunSetKind::initial;
    std::uint64_t pass_index = 0U;
    std::uint64_t run_count = 0U;
};

[[nodiscard]] std::filesystem::path run_set_path(
    const RunSet& set,
    const std::uint64_t run_index
) {
    if (run_index >= set.run_count) {
        throw std::logic_error("vertex-ID merge run index is out of range");
    }
    switch (set.kind) {
        case RunSetKind::initial:
            return endpoint_id_run_path(set.workspace, run_index);
        case RunSetKind::intermediate:
            return vertex_id_merge_run_path(
                set.workspace, set.pass_index, run_index
            );
        case RunSetKind::final_vertex_ids:
            if (run_index != 0U) {
                throw std::logic_error(
                    "final vertex-ID set contains exactly one file"
                );
            }
            return vertex_ids_path(set.workspace);
    }
    throw std::logic_error("unknown vertex-ID merge run-set kind");
}

void cleanup_run_set_noexcept(
    const RunSet& set,
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
        if (set.kind == RunSetKind::intermediate) {
            static_cast<void>(::rmdir(
                vertex_id_merge_generation_path(
                    set.workspace, set.pass_index
                ).c_str()
            ));
        }
    } catch (...) {
        // Preserve the primary failure.
    }
}

void remove_run_set_checked(const RunSet& set) {
    if (set.kind == RunSetKind::final_vertex_ids) {
        throw std::logic_error("cannot retire the final vertex-ID run set");
    }
    for (std::uint64_t index = 0U; index < set.run_count; ++index) {
        unlink_checked(
            run_set_path(set, index),
            "remove retired vertex-ID merge run"
        );
    }
    if (set.kind == RunSetKind::intermediate) {
        rmdir_checked(
            vertex_id_merge_generation_path(set.workspace, set.pass_index),
            "remove retired vertex-ID merge generation"
        );
    }
}

class CreatedRunSetGuard final {
public:
    explicit CreatedRunSetGuard(RunSet set) : set_(std::move(set)) {}

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
    RunSet set_;
    std::uint64_t created_count_ = 0U;
    bool active_ = false;
};

class BufferedIdCursor final {
public:
    BufferedIdCursor(
        const std::filesystem::path& path,
        const storage::BinaryMagic& magic,
        const std::size_t reader_buffer_bytes,
        const std::size_t crc_chunk_bytes
    )
        : reader_(storage::ValidatedBinaryFileReader::open(
              path,
              magic,
              kEndpointIdRunRecordBytes,
              crc_chunk_bytes
          )),
          buffer_(reader_buffer_bytes),
          record_count_(reader_.header().record_count) {
        if (record_count_ == 0U) {
            throw storage::BinaryError("vertex-ID merge input run is empty");
        }
    }

    BufferedIdCursor(const BufferedIdCursor&) = delete;
    BufferedIdCursor& operator=(const BufferedIdCursor&) = delete;
    BufferedIdCursor(BufferedIdCursor&&) noexcept = default;
    BufferedIdCursor& operator=(BufferedIdCursor&&) noexcept = default;

    [[nodiscard]] std::optional<std::int32_t> next() {
        if (buffer_index_ == buffered_records_) {
            refill();
        }
        if (buffer_index_ == buffered_records_) {
            return std::nullopt;
        }

        const std::size_t byte_offset =
            buffer_index_ * kEndpointIdRunRecordBytes;
        const std::int32_t value = storage::decode_i32_le(
            buffer_, byte_offset
        );
        ++buffer_index_;
        if (previous_.has_value() && value <= *previous_) {
            throw storage::BinaryError(
                "vertex-ID merge input is not strictly signed-ascending"
            );
        }
        previous_ = value;
        return value;
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
            buffer_.size() / kEndpointIdRunRecordBytes;
        const std::uint64_t remaining = record_count_ - next_record_;
        const std::size_t requested_records = static_cast<std::size_t>(
            std::min(remaining, size_to_u64(capacity_records))
        );
        const std::size_t requested_bytes =
            requested_records * kEndpointIdRunRecordBytes;
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
    std::size_t buffer_index_ = 0U;
    std::size_t buffered_records_ = 0U;
    std::optional<std::int32_t> previous_{};
};

struct HeapNode {
    std::int32_t value = 0;
    std::size_t cursor_index = 0U;
};

class FixedMinHeap final {
public:
    explicit FixedMinHeap(const std::size_t capacity)
        : values_(std::make_unique<std::int32_t[]>(capacity)),
          cursor_indices_(std::make_unique<std::size_t[]>(capacity)),
          capacity_(capacity) {}

    void push(const HeapNode node) {
        if (size_ >= capacity_) {
            throw std::logic_error("vertex-ID merge heap capacity exceeded");
        }
        std::size_t position = size_;
        values_[position] = node.value;
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
            throw std::logic_error("cannot pop an empty vertex-ID merge heap");
        }
        const HeapNode result{
            .value = values_[0],
            .cursor_index = cursor_indices_[0],
        };
        --size_;
        if (size_ == 0U) {
            return result;
        }

        values_[0] = values_[size_];
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
        return values_[left] < values_[right]
            || (values_[left] == values_[right]
                && cursor_indices_[left] < cursor_indices_[right]);
    }

    void swap_nodes(const std::size_t left, const std::size_t right) noexcept {
        std::swap(values_[left], values_[right]);
        std::swap(cursor_indices_[left], cursor_indices_[right]);
    }

    std::unique_ptr<std::int32_t[]> values_;
    std::unique_ptr<std::size_t[]> cursor_indices_;
    std::size_t capacity_ = 0U;
    std::size_t size_ = 0U;
    std::size_t peak_size_ = 0U;
};

[[nodiscard]] std::uint64_t successor_run_count(
    const std::uint64_t input_count,
    const std::size_t fan_in
) {
    const std::uint64_t divisor = size_to_u64(fan_in);
    return input_count / divisor
        + static_cast<std::uint64_t>(input_count % divisor != 0U);
}

void preflight_successor_names(
    const std::filesystem::path& workspace,
    std::uint64_t run_count,
    const std::size_t fan_in
) {
    std::uint64_t pass_index = 0U;
    const std::uint64_t fan_in_u64 = size_to_u64(fan_in);
    while (run_count > fan_in_u64) {
        require_absent(
            vertex_id_merge_generation_path(workspace, pass_index),
            "vertex-ID merge generation already exists"
        );
        run_count = successor_run_count(run_count, fan_in);
        pass_index = platform::checked_add(pass_index, 1U);
    }
    require_absent(
        vertex_ids_path(workspace),
        "vertex_ids.bin already exists"
    );
}

[[nodiscard]] std::size_t group_size(
    const std::uint64_t remaining,
    const std::size_t fan_in
) {
    return static_cast<std::size_t>(
        std::min(remaining, size_to_u64(fan_in))
    );
}

[[nodiscard]] RunFileInfo merge_group(
    const RunSet& inputs,
    const std::uint64_t input_begin,
    const std::size_t input_count,
    const std::filesystem::path& output_path,
    const storage::BinaryMagic& output_magic,
    const VertexIdMergeConfig& config,
    VertexIdMergeTelemetry& telemetry
) {
    std::vector<BufferedIdCursor> cursors;
    cursors.reserve(input_count);
    for (std::size_t local_index = 0U;
         local_index < input_count;
         ++local_index) {
        const std::uint64_t run_index = platform::checked_add(
            input_begin, size_to_u64(local_index)
        );
        cursors.emplace_back(
            run_set_path(inputs, run_index),
            kEndpointIdRunMagic,
            config.reader_buffer_bytes,
            config.crc_chunk_bytes
        );
    }
    telemetry.peak_open_input_runs = std::max(
        telemetry.peak_open_input_runs,
        cursors.size()
    );

    FixedMinHeap heap(cursors.size());
    for (std::size_t index = 0U; index < cursors.size(); ++index) {
        const std::optional<std::int32_t> value = cursors[index].next();
        if (!value.has_value()) {
            throw storage::BinaryError("vertex-ID merge input run is empty");
        }
        heap.push(HeapNode{.value = *value, .cursor_index = index});
    }

    bool output_created = false;
    try {
        RunFileWriter writer = RunFileWriter::create(
            output_path,
            output_magic,
            kEndpointIdRunRecordBytes,
            config.writer_buffer_bytes
        );
        output_created = true;
        std::optional<std::int32_t> last_emitted;
        while (!heap.empty()) {
            const HeapNode node = heap.pop();
            if (!last_emitted.has_value() || node.value != *last_emitted) {
                std::array<std::byte, kEndpointIdRunRecordBytes> encoded{};
                storage::encode_i32_le(node.value, encoded);
                writer.append_records(encoded);
                last_emitted = node.value;
            }

            const std::optional<std::int32_t> next =
                cursors[node.cursor_index].next();
            if (next.has_value()) {
                heap.push(HeapNode{
                    .value = *next,
                    .cursor_index = node.cursor_index,
                });
            }
        }

        const RunFileInfo info = writer.finish();
        if (info.header.record_count == 0U) {
            throw storage::BinaryError(
                "vertex-ID merge produced an empty successor run"
            );
        }
        telemetry.peak_heap_items = std::max(
            telemetry.peak_heap_items,
            heap.peak_size()
        );
        telemetry.max_writer_peak_buffered_bytes = std::max(
            telemetry.max_writer_peak_buffered_bytes,
            info.peak_buffered_bytes
        );
        return info;
    } catch (...) {
        if (output_created) {
            static_cast<void>(::unlink(output_path.c_str()));
        }
        throw;
    }
}

[[nodiscard]] std::uint64_t verify_sorted_run(
    const std::filesystem::path& path,
    const storage::BinaryMagic& magic,
    const VertexIdMergeConfig& config
) {
    BufferedIdCursor cursor(
        path,
        magic,
        config.reader_buffer_bytes,
        config.crc_chunk_bytes
    );
    std::uint64_t seen = 0U;
    while (cursor.next().has_value()) {
        seen = platform::checked_add(seen, 1U);
    }
    if (seen != cursor.record_count()) {
        throw storage::BinaryError(
            "vertex-ID merge verification count diverged"
        );
    }
    return seen;
}

void create_private_directory(const std::filesystem::path& path) {
    errno = 0;
    if (::mkdir(path.c_str(), 0700) == -1) {
        const int error_number = errno;
        throw_posix_error(
            error_number, "create vertex-ID merge generation"
        );
    }
}

}  // namespace

VertexIdMergeMemoryPlan vertex_id_merge_memory_plan(
    const VertexIdMergeConfig& config
) {
    validate_config(config);
    const std::uint64_t fan_in = size_to_u64(config.fan_in);
    const std::uint64_t reader_buffers = platform::checked_multiply(
        fan_in, size_to_u64(config.reader_buffer_bytes)
    );
    const std::uint64_t cursor_object_bytes = sizeof(BufferedIdCursor);
    const std::uint64_t cursor_objects = platform::checked_multiply(
        fan_in, cursor_object_bytes
    );
    const std::uint64_t heap_entry_bytes = platform::checked_add(
        sizeof(std::int32_t), sizeof(std::size_t)
    );
    const std::uint64_t heap_arrays = platform::checked_multiply(
        fan_in, heap_entry_bytes
    );
    const std::uint64_t with_writer = platform::checked_add(
        reader_buffers, size_to_u64(config.writer_buffer_bytes)
    );
    const std::uint64_t with_cursors = platform::checked_add(
        with_writer, cursor_objects
    );
    const std::uint64_t with_heap = platform::checked_add(
        with_cursors, heap_arrays
    );
    const std::uint64_t managed_upper_bound = platform::checked_add(
        with_heap, size_to_u64(config.crc_chunk_bytes)
    );
    return VertexIdMergeMemoryPlan{
        .reader_buffers_bytes = reader_buffers,
        .writer_buffer_bytes = size_to_u64(config.writer_buffer_bytes),
        .cursor_object_bytes = cursor_object_bytes,
        .cursor_objects_bytes = cursor_objects,
        .heap_arrays_bytes = heap_arrays,
        .crc_buffer_bytes = size_to_u64(config.crc_chunk_bytes),
        .managed_bulk_upper_bound_bytes = managed_upper_bound,
        .max_open_files = platform::checked_add(fan_in, 1U),
    };
}

VertexCountLimitError::VertexCountLimitError(
    const std::uint64_t vertex_count
)
    : std::runtime_error(
          "unique vertex count exceeds UINT32_MAX: "
          + std::to_string(vertex_count)
      ),
      vertex_count_(vertex_count) {}

std::uint64_t VertexCountLimitError::vertex_count() const noexcept {
    return vertex_count_;
}

std::uint32_t checked_compact_vertex_count(
    const std::uint64_t vertex_count
) {
    if (vertex_count == 0U) {
        throw std::invalid_argument("unique vertex count must be positive");
    }
    if (vertex_count > std::numeric_limits<std::uint32_t>::max()) {
        throw VertexCountLimitError(vertex_count);
    }
    return static_cast<std::uint32_t>(vertex_count);
}

std::string vertex_id_merge_generation_name(
    const std::uint64_t pass_index
) {
    return format_index(kGenerationPrefix, pass_index);
}

std::filesystem::path vertex_id_merge_generation_path(
    const std::filesystem::path& workspace,
    const std::uint64_t pass_index
) {
    return workspace / vertex_id_merge_generation_name(pass_index);
}

std::filesystem::path vertex_id_merge_run_path(
    const std::filesystem::path& workspace,
    const std::uint64_t pass_index,
    const std::uint64_t run_index
) {
    return vertex_id_merge_generation_path(workspace, pass_index)
        / endpoint_id_run_name(run_index);
}

std::filesystem::path vertex_ids_path(
    const std::filesystem::path& workspace
) {
    return workspace / kVertexIdsName;
}

VertexIdMergeResult merge_endpoint_id_runs(
    const std::filesystem::path& workspace,
    const std::uint64_t input_run_count,
    const VertexIdMergeConfig config
) {
    static_cast<void>(vertex_id_merge_memory_plan(config));
    if (input_run_count == 0U) {
        throw std::invalid_argument(
            "vertex-ID merge requires at least one endpoint run"
        );
    }
    const std::filesystem::path owned_workspace =
        validate_workspace(workspace);
    preflight_successor_names(
        owned_workspace, input_run_count, config.fan_in
    );

    RunSet current{
        .workspace = owned_workspace,
        .kind = RunSetKind::initial,
        .pass_index = 0U,
        .run_count = input_run_count,
    };
    VertexIdMergeTelemetry telemetry{};
    std::uint64_t pass_index = 0U;
    const std::uint64_t fan_in = size_to_u64(config.fan_in);

    while (current.run_count > fan_in) {
        const std::uint64_t output_count = successor_run_count(
            current.run_count, config.fan_in
        );
        RunSet successor{
            .workspace = owned_workspace,
            .kind = RunSetKind::intermediate,
            .pass_index = pass_index,
            .run_count = output_count,
        };
        CreatedRunSetGuard guard(successor);
        create_private_directory(vertex_id_merge_generation_path(
            owned_workspace, pass_index
        ));
        guard.arm();

        std::uint64_t input_begin = 0U;
        for (std::uint64_t output_index = 0U;
             output_index < output_count;
             ++output_index) {
            const std::size_t count = group_size(
                current.run_count - input_begin,
                config.fan_in
            );
            static_cast<void>(merge_group(
                current,
                input_begin,
                count,
                run_set_path(successor, output_index),
                kEndpointIdRunMagic,
                config,
                telemetry
            ));
            guard.note_created();
            input_begin = platform::checked_add(
                input_begin, size_to_u64(count)
            );
        }
        if (input_begin != current.run_count) {
            throw std::logic_error(
                "vertex-ID merge generation did not consume every input"
            );
        }

        for (std::uint64_t index = 0U; index < output_count; ++index) {
            static_cast<void>(verify_sorted_run(
                run_set_path(successor, index),
                kEndpointIdRunMagic,
                config
            ));
        }

        telemetry.merge_pass_count = platform::checked_add(
            telemetry.merge_pass_count, 1U
        );
        telemetry.intermediate_run_count = platform::checked_add(
            telemetry.intermediate_run_count, output_count
        );
        guard.commit();
        remove_run_set_checked(current);
        current = std::move(successor);
        pass_index = platform::checked_add(pass_index, 1U);
    }

    const RunSet final_set{
        .workspace = owned_workspace,
        .kind = RunSetKind::final_vertex_ids,
        .pass_index = 0U,
        .run_count = 1U,
    };
    CreatedRunSetGuard final_guard(final_set);
    const RunFileInfo final_info = merge_group(
        current,
        0U,
        static_cast<std::size_t>(current.run_count),
        run_set_path(final_set, 0U),
        storage::kVertexIdsMagic,
        config,
        telemetry
    );
    final_guard.note_created();
    final_guard.arm();
    const std::uint64_t verified_vertex_count = verify_sorted_run(
        run_set_path(final_set, 0U),
        storage::kVertexIdsMagic,
        config
    );
    if (verified_vertex_count != final_info.header.record_count) {
        throw storage::BinaryError(
            "final vertex-ID verification count diverged from header"
        );
    }
    const std::uint32_t compact_vertex_count =
        checked_compact_vertex_count(verified_vertex_count);

    telemetry.merge_pass_count = platform::checked_add(
        telemetry.merge_pass_count, 1U
    );
    final_guard.commit();
    remove_run_set_checked(current);
    return VertexIdMergeResult{
        .vertex_count = compact_vertex_count,
        .vertex_ids = final_info,
        .telemetry = telemetry,
    };
}

}  // namespace tbank::preprocess

#include "tbank/preprocess/compact_edge_runs.hpp"

#include "tbank/platform/checked_io.hpp"
#include "tbank/preprocess/ingest.hpp"
#include "tbank/preprocess/vertex_id_merge.hpp"
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
#include <sys/statvfs.h>
#include <unistd.h>

namespace tbank::preprocess {
namespace {

constexpr std::string_view kGenerationPrefix = "compact_edges.pass.";
constexpr std::string_view kRunPrefix = "compact_edges.";
constexpr std::string_view kRunSuffix = ".bin";
constexpr std::size_t kIndexDigits = 20U;

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

[[nodiscard]] std::size_t u64_to_size(const std::uint64_t value) {
    if (value > size_to_u64(std::numeric_limits<std::size_t>::max())) {
        throw std::overflow_error("compact edge count does not fit size_t");
    }
    return static_cast<std::size_t>(value);
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

void validate_plan_config(const CompactEdgeRunConfig& config) {
    if (config.raw_edges_per_run == 0U) {
        throw std::invalid_argument(
            "compact edge raw_edges_per_run must be positive"
        );
    }
    if (config.reader_buffer_bytes < kCompactEdgeRunRecordBytes
        || config.reader_buffer_bytes % kCompactEdgeRunRecordBytes != 0U) {
        throw std::invalid_argument(
            "compact edge reader buffer must be a positive multiple of 8"
        );
    }
    if (config.writer_buffer_bytes < kCompactEdgeRunRecordBytes) {
        throw std::invalid_argument(
            "compact edge writer buffer must hold one edge record"
        );
    }
    if (config.crc_chunk_bytes == 0U
        || config.crc_chunk_bytes > storage::kMaximumCrcChunkBytes) {
        throw std::invalid_argument(
            "compact edge CRC chunk must be in [1, 1 MiB]"
        );
    }
}

void validate_execution_config(const CompactEdgeRunConfig& config) {
    validate_plan_config(config);
    if (config.managed_bulk_limit_bytes == 0U) {
        throw std::invalid_argument(
            "compact edge managed bulk limit must be explicitly supplied"
        );
    }
    if (config.managed_bulk_limit_bytes
        > kCompactEdgeHardMemoryBudgetBytes) {
        throw std::invalid_argument(
            "compact edge managed bulk limit exceeds the hard memory budget"
        );
    }
}

[[nodiscard]] std::filesystem::path validate_workspace(
    const std::filesystem::path& workspace
) {
    if (workspace.empty()) {
        throw std::invalid_argument("compact edge workspace is empty");
    }
    if (contains_embedded_nul(workspace)) {
        throw std::invalid_argument(
            "compact edge workspace contains an embedded NUL"
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
        throw_posix_error(error_number, "inspect compact edge workspace");
    }
    if (!S_ISDIR(status.st_mode)) {
        throw std::invalid_argument(
            "compact edge workspace must be a real directory"
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

[[nodiscard]] std::string format_index(
    const std::string_view prefix,
    const std::string_view suffix,
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
        throw std::runtime_error("failed to format compact edge index");
    }
    const std::size_t digit_count = static_cast<std::size_t>(
        conversion.ptr - unpadded.data()
    );
    if (digit_count > digits.size()) {
        throw std::overflow_error("compact edge index exceeds filename width");
    }
    const std::size_t padding = digits.size() - digit_count;
    std::fill_n(digits.data(), padding, '0');
    std::copy_n(unpadded.data(), digit_count, digits.data() + padding);

    std::string name;
    name.reserve(prefix.size() + digits.size() + suffix.size());
    name.append(prefix);
    name.append(digits.data(), digits.size());
    name.append(suffix);
    return name;
}

[[nodiscard]] std::uint64_t run_count_for(
    const std::uint64_t raw_edge_count,
    const std::size_t raw_edges_per_run
) {
    if (raw_edge_count == 0U) {
        throw std::invalid_argument("compact edge raw edge count is zero");
    }
    const std::uint64_t chunk = size_to_u64(raw_edges_per_run);
    return 1U + (raw_edge_count - 1U) / chunk;
}

class SuccessorGenerationGuard final {
public:
    SuccessorGenerationGuard(
        std::filesystem::path workspace,
        const std::uint64_t generation_index
    )
        : workspace_(std::move(workspace)),
          generation_index_(generation_index) {}

    SuccessorGenerationGuard(const SuccessorGenerationGuard&) = delete;
    SuccessorGenerationGuard& operator=(
        const SuccessorGenerationGuard&
    ) = delete;

    ~SuccessorGenerationGuard() noexcept {
        if (!active_) {
            return;
        }
        for (std::uint64_t index = 0U; index < created_run_count_; ++index) {
            try {
                static_cast<void>(::unlink(
                    compact_edge_run_path(
                        workspace_, generation_index_, index
                    ).c_str()
                ));
            } catch (...) {
                // Continue cleanup.
            }
        }
        try {
            static_cast<void>(::rmdir(
                compact_edge_generation_path(
                    workspace_, generation_index_
                ).c_str()
            ));
        } catch (...) {
            // Preserve the primary failure.
        }
    }

    void arm() noexcept {
        active_ = true;
    }

    void note_created_run() noexcept {
        ++created_run_count_;
    }

    void commit() noexcept {
        active_ = false;
    }

private:
    std::filesystem::path workspace_;
    std::uint64_t generation_index_ = 0U;
    std::uint64_t created_run_count_ = 0U;
    bool active_ = false;
};

void preflight_disk_space(
    const std::filesystem::path& workspace,
    const CompactEdgeRunDiskPlan& disk_plan,
    const CompactEdgeRunConfig& config
) {
    struct statvfs status {};
    int result = -1;
    do {
        errno = 0;
        result = ::statvfs(workspace.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "inspect compact edge filesystem");
    }

    const std::uint64_t fragment_bytes = status.f_frsize != 0U
        ? static_cast<std::uint64_t>(status.f_frsize)
        : static_cast<std::uint64_t>(status.f_bsize);
    if (fragment_bytes == 0U) {
        throw std::runtime_error(
            "compact edge filesystem reports a zero allocation unit"
        );
    }
    const std::uint64_t available_bytes = platform::checked_multiply(
        static_cast<std::uint64_t>(status.f_bavail), fragment_bytes
    );

    // Reserve one block of filesystem slack per run and generation directory.
    const std::uint64_t rounding_slack = platform::checked_multiply(
        disk_plan.run_count, fragment_bytes - 1U
    );
    const std::uint64_t successor_entries = platform::checked_add(
        disk_plan.run_count, 1U
    );
    const std::uint64_t directory_allowance = platform::checked_multiply(
        successor_entries, fragment_bytes
    );
    std::uint64_t required_bytes = platform::checked_add(
        disk_plan.successor_logical_upper_bound_bytes, rounding_slack
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
            "compact edge filesystem has too few available inodes"
        );
    }
}

[[nodiscard]] std::uint32_t find_compact_id(
    const std::span<const std::int32_t> dictionary,
    const std::int32_t original_id,
    const std::uint64_t raw_record_index,
    const char* const endpoint_name
) {
    const auto found = std::lower_bound(
        dictionary.begin(), dictionary.end(), original_id
    );
    if (found == dictionary.end() || *found != original_id) {
        throw storage::BinaryError(
            std::string("raw edge ") + endpoint_name
            + " is absent from vertex_ids at record "
            + std::to_string(raw_record_index)
        );
    }
    const auto compact = static_cast<std::uint64_t>(
        found - dictionary.begin()
    );
    if (compact > std::numeric_limits<std::uint32_t>::max()) {
        throw std::logic_error("compact edge dictionary index overflowed");
    }
    return static_cast<std::uint32_t>(compact);
}

[[nodiscard]] std::vector<std::int32_t> load_vertex_dictionary(
    const std::filesystem::path& path,
    const std::uint32_t expected_vertex_count,
    const CompactEdgeRunConfig& config,
    std::vector<std::byte>& io_buffer
) {
    auto reader = storage::ValidatedBinaryFileReader::open(
        path,
        storage::kVertexIdsMagic,
        storage::kScalarRecordBytes,
        config.crc_chunk_bytes
    );
    if (reader.header().record_count != expected_vertex_count) {
        throw storage::BinaryError(
            "vertex_ids count differs from the expected merge result"
        );
    }

    std::vector<std::int32_t> dictionary(expected_vertex_count);
    std::uint64_t next_record = 0U;
    std::size_t destination_index = 0U;
    while (next_record < expected_vertex_count) {
        const std::uint64_t remaining = expected_vertex_count - next_record;
        const std::size_t request_records = static_cast<std::size_t>(
            std::min(
                remaining,
                size_to_u64(io_buffer.size() / storage::kScalarRecordBytes)
            )
        );
        const std::size_t request_bytes =
            request_records * storage::kScalarRecordBytes;
        reader.read_records(
            next_record,
            std::span<std::byte>(io_buffer.data(), request_bytes)
        );
        for (std::size_t index = 0U; index < request_records; ++index) {
            const std::int32_t value = storage::decode_i32_le(
                io_buffer, index * storage::kScalarRecordBytes
            );
            if (destination_index > 0U
                && value <= dictionary[destination_index - 1U]) {
                throw storage::BinaryError(
                    "vertex_ids is not strictly signed-ascending"
                );
            }
            dictionary[destination_index] = value;
            ++destination_index;
        }
        next_record = platform::checked_add(
            next_record, size_to_u64(request_records)
        );
    }
    if (destination_index != dictionary.size()) {
        throw std::logic_error("vertex dictionary decode count diverged");
    }
    return dictionary;
}

void append_compact_edges(
    RunFileWriter& writer,
    const std::span<const CompactEdgeRecord> edges,
    std::vector<std::byte>& io_buffer
) {
    const std::size_t capacity_records =
        io_buffer.size() / kCompactEdgeRunRecordBytes;
    std::size_t source_index = 0U;
    while (source_index < edges.size()) {
        const std::size_t batch_records = std::min(
            capacity_records, edges.size() - source_index
        );
        for (std::size_t index = 0U; index < batch_records; ++index) {
            const auto encoded = encode_compact_edge_record(
                edges[source_index + index]
            );
            std::copy(
                encoded.begin(),
                encoded.end(),
                io_buffer.begin()
                    + static_cast<std::ptrdiff_t>(
                        index * kCompactEdgeRunRecordBytes
                    )
            );
        }
        const std::size_t batch_bytes =
            batch_records * kCompactEdgeRunRecordBytes;
        writer.append_records(
            std::span<const std::byte>(io_buffer.data(), batch_bytes)
        );
        source_index += batch_records;
    }
}

[[nodiscard]] std::uint64_t verify_compact_generation(
    const std::filesystem::path& workspace,
    const std::uint64_t generation_index,
    const std::uint64_t run_count,
    const std::uint32_t vertex_count,
    const CompactEdgeRunConfig& config,
    std::vector<std::byte>& io_buffer
) {
    std::uint64_t verified_records = 0U;
    for (std::uint64_t run_index = 0U;
         run_index < run_count;
         ++run_index) {
        auto reader = storage::ValidatedBinaryFileReader::open(
            compact_edge_run_path(workspace, generation_index, run_index),
            kCompactEdgeRunMagic,
            kCompactEdgeRunRecordBytes,
            config.crc_chunk_bytes
        );
        const std::uint64_t record_count = reader.header().record_count;
        if (record_count == 0U) {
            throw storage::BinaryError("compact edge run is empty");
        }

        bool has_previous = false;
        CompactEdgeRecord previous{};
        std::uint64_t next_record = 0U;
        while (next_record < record_count) {
            const std::uint64_t remaining = record_count - next_record;
            const std::size_t request_records = static_cast<std::size_t>(
                std::min(
                    remaining,
                    size_to_u64(
                        io_buffer.size() / kCompactEdgeRunRecordBytes
                    )
                )
            );
            const std::size_t request_bytes =
                request_records * kCompactEdgeRunRecordBytes;
            reader.read_records(
                next_record,
                std::span<std::byte>(io_buffer.data(), request_bytes)
            );
            for (std::size_t index = 0U; index < request_records; ++index) {
                const CompactEdgeRecord edge = decode_compact_edge_record(
                    std::span<const std::byte>(
                        io_buffer.data()
                            + index * kCompactEdgeRunRecordBytes,
                        kCompactEdgeRunRecordBytes
                    )
                );
                if (edge.destination >= vertex_count
                    || edge.source >= vertex_count) {
                    throw storage::BinaryError(
                        "compact edge endpoint is outside vertex count"
                    );
                }
                if (has_previous
                    && !(previous.destination < edge.destination
                         || (previous.destination == edge.destination
                             && previous.source < edge.source))) {
                    throw storage::BinaryError(
                        "compact edge run is not strictly ordered"
                    );
                }
                previous = edge;
                has_previous = true;
            }
            next_record = platform::checked_add(
                next_record, size_to_u64(request_records)
            );
        }
        verified_records = platform::checked_add(
            verified_records, record_count
        );
    }
    return verified_records;
}

}  // namespace

std::array<std::byte, kCompactEdgeRunRecordBytes>
encode_compact_edge_record(const CompactEdgeRecord& record) {
    std::array<std::byte, kCompactEdgeRunRecordBytes> encoded{};
    storage::encode_u32_le(record.destination, encoded, 0U);
    storage::encode_u32_le(record.source, encoded, 4U);
    return encoded;
}

CompactEdgeRecord decode_compact_edge_record(
    const std::span<const std::byte> bytes
) {
    if (bytes.size() != kCompactEdgeRunRecordBytes) {
        throw storage::BinaryError(
            "compact edge record must contain exactly 8 bytes"
        );
    }
    return CompactEdgeRecord{
        .destination = storage::decode_u32_le(bytes, 0U),
        .source = storage::decode_u32_le(bytes, 4U),
    };
}

CompactEdgeRunMemoryPlan compact_edge_run_memory_plan(
    const std::uint64_t vertex_count,
    const std::uint64_t raw_edge_count,
    const CompactEdgeRunConfig& config
) {
    validate_plan_config(config);
    if (vertex_count == 0U) {
        throw std::invalid_argument("compact edge vertex count is zero");
    }
    if (raw_edge_count == 0U) {
        throw std::invalid_argument("compact edge raw edge count is zero");
    }

    const std::uint64_t effective_chunk_edges = std::min(
        raw_edge_count, size_to_u64(config.raw_edges_per_run)
    );
    const std::uint64_t dictionary_bytes = platform::checked_multiply(
        vertex_count, storage::kScalarRecordBytes
    );
    const std::uint64_t edge_chunk_bytes = platform::checked_multiply(
        effective_chunk_edges, kCompactEdgeRunRecordBytes
    );
    const std::uint64_t maximum_run_file_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes, edge_chunk_bytes
    );
    static_cast<void>(
        platform::checked_u64_to_off_t(maximum_run_file_bytes)
    );
    const std::uint64_t reader_buffer_bytes = size_to_u64(
        config.reader_buffer_bytes
    );
    const std::uint64_t writer_buffer_bytes = size_to_u64(
        config.writer_buffer_bytes
    );
    const std::uint64_t crc_buffer_bytes = size_to_u64(
        config.crc_chunk_bytes
    );
    std::uint64_t managed = platform::checked_add(
        dictionary_bytes, edge_chunk_bytes
    );
    managed = platform::checked_add(managed, reader_buffer_bytes);
    managed = platform::checked_add(managed, writer_buffer_bytes);
    managed = platform::checked_add(managed, crc_buffer_bytes);

    return CompactEdgeRunMemoryPlan{
        .dictionary_bytes = dictionary_bytes,
        .edge_chunk_bytes = edge_chunk_bytes,
        .reader_buffer_bytes = reader_buffer_bytes,
        .writer_buffer_bytes = writer_buffer_bytes,
        .crc_buffer_bytes = crc_buffer_bytes,
        .managed_bulk_upper_bound_bytes = managed,
        .max_open_files = 2U,
    };
}

CompactEdgeRunDiskPlan compact_edge_run_disk_plan(
    const std::uint64_t vertex_count,
    const std::uint64_t raw_edge_count,
    const CompactEdgeRunConfig& config
) {
    validate_plan_config(config);
    if (vertex_count == 0U) {
        throw std::invalid_argument("compact edge vertex count is zero");
    }
    if (raw_edge_count == 0U) {
        throw std::invalid_argument("compact edge raw edge count is zero");
    }

    const std::uint64_t run_count = run_count_for(
        raw_edge_count, config.raw_edges_per_run
    );
    const std::uint64_t raw_payload_bytes = platform::checked_multiply(
        raw_edge_count, kRawEdgeRunRecordBytes
    );
    const std::uint64_t vertex_payload_bytes = platform::checked_multiply(
        vertex_count, storage::kScalarRecordBytes
    );
    const std::uint64_t raw_input_file_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes, raw_payload_bytes
    );
    const std::uint64_t vertex_ids_file_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes, vertex_payload_bytes
    );
    const std::uint64_t successor_headers_bytes = platform::checked_multiply(
        run_count, storage::kBinaryHeaderBytes
    );
    const std::uint64_t successor_logical_upper_bound_bytes =
        platform::checked_add(raw_payload_bytes, successor_headers_bytes);
    const std::uint64_t predecessor_bytes = platform::checked_add(
        raw_input_file_bytes, vertex_ids_file_bytes
    );
    const std::uint64_t logical_peak_upper_bound_bytes =
        platform::checked_add(
            predecessor_bytes, successor_logical_upper_bound_bytes
        );

    return CompactEdgeRunDiskPlan{
        .run_count = run_count,
        .raw_input_file_bytes = raw_input_file_bytes,
        .vertex_ids_file_bytes = vertex_ids_file_bytes,
        .successor_logical_upper_bound_bytes =
            successor_logical_upper_bound_bytes,
        .logical_peak_upper_bound_bytes = logical_peak_upper_bound_bytes,
    };
}

CompactEdgeMemoryLimitError::CompactEdgeMemoryLimitError(
    const std::uint64_t required_bytes,
    const std::uint64_t limit_bytes
)
    : std::runtime_error(
          "compact edge managed bulk preflight requires "
          + std::to_string(required_bytes) + " bytes but limit is "
          + std::to_string(limit_bytes)
      ),
      required_bytes_(required_bytes),
      limit_bytes_(limit_bytes) {}

std::uint64_t CompactEdgeMemoryLimitError::required_bytes() const noexcept {
    return required_bytes_;
}

std::uint64_t CompactEdgeMemoryLimitError::limit_bytes() const noexcept {
    return limit_bytes_;
}

CompactEdgeDiskSpaceError::CompactEdgeDiskSpaceError(
    const std::uint64_t required_bytes,
    const std::uint64_t available_bytes
)
    : std::runtime_error(
          "compact edge disk preflight requires "
          + std::to_string(required_bytes) + " available bytes but found "
          + std::to_string(available_bytes)
      ),
      required_bytes_(required_bytes),
      available_bytes_(available_bytes) {}

std::uint64_t CompactEdgeDiskSpaceError::required_bytes() const noexcept {
    return required_bytes_;
}

std::uint64_t CompactEdgeDiskSpaceError::available_bytes() const noexcept {
    return available_bytes_;
}

std::string compact_edge_generation_name(
    const std::uint64_t generation_index
) {
    return format_index(kGenerationPrefix, {}, generation_index);
}

std::filesystem::path compact_edge_generation_path(
    const std::filesystem::path& workspace,
    const std::uint64_t generation_index
) {
    return workspace / compact_edge_generation_name(generation_index);
}

std::string compact_edge_run_name(const std::uint64_t run_index) {
    return format_index(kRunPrefix, kRunSuffix, run_index);
}

std::filesystem::path compact_edge_run_path(
    const std::filesystem::path& workspace,
    const std::uint64_t generation_index,
    const std::uint64_t run_index
) {
    return compact_edge_generation_path(workspace, generation_index)
        / compact_edge_run_name(run_index);
}

CompactEdgeRunResult build_compact_edge_runs(
    const std::filesystem::path& workspace,
    const std::uint64_t expected_raw_edge_count,
    const std::uint64_t expected_vertex_count,
    const CompactEdgeRunConfig config
) {
    validate_execution_config(config);
    const std::uint32_t vertex_count = checked_compact_vertex_count(
        expected_vertex_count
    );
    const CompactEdgeRunMemoryPlan memory_plan =
        compact_edge_run_memory_plan(
            expected_vertex_count, expected_raw_edge_count, config
        );
    const CompactEdgeRunDiskPlan disk_plan = compact_edge_run_disk_plan(
        expected_vertex_count, expected_raw_edge_count, config
    );
    if (memory_plan.managed_bulk_upper_bound_bytes
        > config.managed_bulk_limit_bytes) {
        throw CompactEdgeMemoryLimitError(
            memory_plan.managed_bulk_upper_bound_bytes,
            config.managed_bulk_limit_bytes
        );
    }

    const std::uint64_t run_count = run_count_for(
        expected_raw_edge_count, config.raw_edges_per_run
    );
    const std::uint64_t binary_search_lookups =
        platform::checked_multiply(expected_raw_edge_count, 2U);
    const std::filesystem::path absolute_workspace =
        validate_workspace(workspace);
    const std::filesystem::path generation_path =
        compact_edge_generation_path(
            absolute_workspace, kCompactEdgeInitialGeneration
        );
    require_absent(
        generation_path, "compact edge successor generation already exists"
    );
    preflight_disk_space(absolute_workspace, disk_plan, config);

    // Allocate caller-owned bulk memory before output; writer failures roll back.
    std::vector<std::byte> io_buffer(config.reader_buffer_bytes);
    std::vector<std::int32_t> dictionary = load_vertex_dictionary(
        vertex_ids_path(absolute_workspace),
        vertex_count,
        config,
        io_buffer
    );
    const std::uint64_t effective_chunk_edges = std::min(
        expected_raw_edge_count, size_to_u64(config.raw_edges_per_run)
    );
    const std::size_t chunk_capacity = u64_to_size(effective_chunk_edges);
    auto chunk = std::make_unique<CompactEdgeRecord[]>(chunk_capacity);

    std::optional<storage::ValidatedBinaryFileReader> raw_reader;
    raw_reader.emplace(storage::ValidatedBinaryFileReader::open(
        raw_edge_run_path(absolute_workspace),
        kRawEdgeRunMagic,
        kRawEdgeRunRecordBytes,
        config.crc_chunk_bytes
    ));
    if (raw_reader->header().record_count != expected_raw_edge_count) {
        throw storage::BinaryError(
            "raw edge count differs from the expected ingest result"
        );
    }

    SuccessorGenerationGuard generation_guard(
        absolute_workspace, kCompactEdgeInitialGeneration
    );
    errno = 0;
    if (::mkdir(generation_path.c_str(), 0700) == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "create compact edge generation");
    }
    generation_guard.arm();

    CompactEdgeRunTelemetry telemetry{};
    telemetry.binary_search_lookups = binary_search_lookups;
    std::uint64_t next_raw_record = 0U;
    std::uint64_t locally_unique_edges = 0U;
    for (std::uint64_t run_index = 0U;
         run_index < run_count;
         ++run_index) {
        std::size_t chunk_size = 0U;
        const std::uint64_t remaining_total =
            expected_raw_edge_count - next_raw_record;
        const std::size_t target_chunk_records = u64_to_size(
            std::min(
                remaining_total,
                size_to_u64(config.raw_edges_per_run)
            )
        );
        while (chunk_size < target_chunk_records) {
            const std::size_t request_records = std::min(
                io_buffer.size() / kRawEdgeRunRecordBytes,
                target_chunk_records - chunk_size
            );
            const std::size_t request_bytes =
                request_records * kRawEdgeRunRecordBytes;
            raw_reader->read_records(
                next_raw_record,
                std::span<std::byte>(io_buffer.data(), request_bytes)
            );
            for (std::size_t index = 0U; index < request_records; ++index) {
                const RawEdgeRecord raw = decode_raw_edge_record(
                    std::span<const std::byte>(
                        io_buffer.data() + index * kRawEdgeRunRecordBytes,
                        kRawEdgeRunRecordBytes
                    )
                );
                const std::uint64_t record_index = platform::checked_add(
                    next_raw_record, size_to_u64(index)
                );
                const std::uint32_t source = find_compact_id(
                    dictionary, raw.source, record_index, "source"
                );
                const std::uint32_t destination = find_compact_id(
                    dictionary,
                    raw.destination,
                    record_index,
                    "destination"
                );
                chunk[chunk_size + index] = CompactEdgeRecord{
                    .destination = destination,
                    .source = source,
                };
            }
            chunk_size += request_records;
            next_raw_record = platform::checked_add(
                next_raw_record, size_to_u64(request_records)
            );
        }
        telemetry.peak_chunk_edges = std::max(
            telemetry.peak_chunk_edges, chunk_size
        );

        std::sort(
            chunk.get(),
            chunk.get() + chunk_size,
            [](const CompactEdgeRecord& left,
               const CompactEdgeRecord& right) {
                return left.destination < right.destination
                    || (left.destination == right.destination
                        && left.source < right.source);
            }
        );
        const auto unique_end = std::unique(
            chunk.get(), chunk.get() + chunk_size
        );
        const std::size_t unique_count = static_cast<std::size_t>(
            unique_end - chunk.get()
        );
        if (unique_count == 0U) {
            throw std::logic_error("nonempty raw chunk became empty");
        }

        RunFileWriter writer = RunFileWriter::create(
            compact_edge_run_path(
                absolute_workspace,
                kCompactEdgeInitialGeneration,
                run_index
            ),
            kCompactEdgeRunMagic,
            kCompactEdgeRunRecordBytes,
            config.writer_buffer_bytes
        );
        append_compact_edges(
            writer,
            std::span<const CompactEdgeRecord>(chunk.get(), unique_count),
            io_buffer
        );
        const RunFileInfo info = writer.finish();
        generation_guard.note_created_run();
        if (info.header.record_count != size_to_u64(unique_count)) {
            throw std::logic_error(
                "compact edge run count diverged from local unique count"
            );
        }
        locally_unique_edges = platform::checked_add(
            locally_unique_edges, info.header.record_count
        );
        telemetry.max_writer_peak_buffered_bytes = std::max(
            telemetry.max_writer_peak_buffered_bytes,
            info.peak_buffered_bytes
        );
    }
    if (next_raw_record != expected_raw_edge_count) {
        throw std::logic_error("compact edge raw input count diverged");
    }

    // Close the validated input before output verification and retirement.
    raw_reader.reset();

    const std::uint64_t verified_records = verify_compact_generation(
        absolute_workspace,
        kCompactEdgeInitialGeneration,
        run_count,
        vertex_count,
        config,
        io_buffer
    );
    if (verified_records != locally_unique_edges) {
        throw std::logic_error(
            "verified compact generation count diverged from producer count"
        );
    }

    generation_guard.commit();
    unlink_checked(
        raw_edge_run_path(absolute_workspace),
        "retire verified raw edge predecessor"
    );

    return CompactEdgeRunResult{
        .summary = CompactEdgeRunSummary{
            .raw_edge_count = expected_raw_edge_count,
            .vertex_count = vertex_count,
            .run_count = run_count,
            .locally_unique_edge_count = locally_unique_edges,
            .local_duplicates_removed =
                expected_raw_edge_count - locally_unique_edges,
        },
        .telemetry = telemetry,
        .memory_plan = memory_plan,
        .disk_plan = disk_plan,
    };
}

}  // namespace tbank::preprocess

#include "tbank/storage/graph.hpp"

#include "tbank/platform/checked_io.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/tasks/partitioner.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tbank::storage {
namespace {

constexpr std::array<std::string_view, kManifestFileCount + 1U>
    kDirectoryEntries{
        "metadata.json",
        "vertex_ids.bin",
        "incoming_sources.bin",
        "incoming_counts.bin",
        "out_degree.bin",
        "tasks.bin",
    };

constexpr std::array<ManifestFileKind, kManifestFileCount> kFileKinds{
    ManifestFileKind::vertex_ids,
    ManifestFileKind::incoming_sources,
    ManifestFileKind::incoming_counts,
    ManifestFileKind::out_degree,
    ManifestFileKind::tasks,
};

class UniqueFileDescriptor final {
public:
    explicit UniqueFileDescriptor(const int descriptor = -1) noexcept
        : descriptor_(descriptor) {}

    UniqueFileDescriptor(const UniqueFileDescriptor&) = delete;
    UniqueFileDescriptor& operator=(const UniqueFileDescriptor&) = delete;

    UniqueFileDescriptor(UniqueFileDescriptor&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)) {}

    UniqueFileDescriptor& operator=(UniqueFileDescriptor&& other) noexcept {
        if (this != &other) {
            close_once();
            descriptor_ = std::exchange(other.descriptor_, -1);
        }
        return *this;
    }

    ~UniqueFileDescriptor() noexcept {
        close_once();
    }

    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }

    [[nodiscard]] int release() noexcept {
        return std::exchange(descriptor_, -1);
    }

private:
    void close_once() noexcept {
        if (descriptor_ >= 0) {
            const int descriptor = std::exchange(descriptor_, -1);
            static_cast<void>(::close(descriptor));
        }
    }

    int descriptor_;
};

class UniqueDirectory final {
public:
    explicit UniqueDirectory(DIR* const directory) noexcept
        : directory_(directory) {}

    UniqueDirectory(const UniqueDirectory&) = delete;
    UniqueDirectory& operator=(const UniqueDirectory&) = delete;

    ~UniqueDirectory() noexcept {
        if (directory_ != nullptr) {
            static_cast<void>(::closedir(directory_));
        }
    }

    [[nodiscard]] DIR* get() const noexcept {
        return directory_;
    }

    [[nodiscard]] int close_once() noexcept {
        DIR* const directory = std::exchange(directory_, nullptr);
        return directory == nullptr ? 0 : ::closedir(directory);
    }

private:
    DIR* directory_;
};

[[noreturn]] void fail(const std::string& detail) {
    throw GraphValidationError("graph directory: " + detail);
}

[[noreturn]] void fail_posix(
    const std::string_view operation,
    const int error_number
) {
    fail(
        std::string(operation) + ": " +
        std::generic_category().message(error_number)
    );
}

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) {
    static_assert(
        std::numeric_limits<std::size_t>::digits
            <= std::numeric_limits<std::uint64_t>::digits
    );
    return static_cast<std::uint64_t>(value);
}

void validate_options(const GraphValidationOptions& options) {
    if (options.io_chunk_bytes == 0U) {
        throw std::invalid_argument("graph I/O chunk size must be positive");
    }
    if (options.io_chunk_bytes > kMaximumCrcChunkBytes) {
        throw std::invalid_argument(
            "graph I/O chunk size exceeds the 1 MiB limit"
        );
    }
}

[[nodiscard]] UniqueFileDescriptor open_directory(
    const std::filesystem::path& directory
) {
    std::filesystem::path::string_type native = directory.native();
    if (native.find('\0') != std::filesystem::path::string_type::npos) {
        fail("graph directory path contains an embedded NUL byte");
    }
    // Strip trailing slash or "/." so O_NOFOLLOW still checks the final component.
    for (;;) {
        while (native.size() > 1U && native.back() == '/') {
            native.pop_back();
        }
        if (native.size() >= 2U && native.back() == '.' &&
            native[native.size() - 2U] == '/') {
            native.pop_back();
            continue;
        }
        break;
    }
    const int descriptor = ::open(
        native.c_str(),
        O_RDONLY | O_CLOEXEC | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK |
            O_NOCTTY
    );
    if (descriptor == -1) {
        fail_posix("open graph directory", errno);
    }

    UniqueFileDescriptor result(descriptor);
    struct stat status {};
    if (::fstat(result.get(), &status) == -1) {
        fail_posix("fstat graph directory", errno);
    }
    if (!S_ISDIR(status.st_mode)) {
        fail("graph path is not a directory");
    }
    return result;
}

[[nodiscard]] std::optional<std::size_t> expected_entry_index(
    const std::string_view name
) noexcept {
    for (std::size_t index = 0U; index < kDirectoryEntries.size(); ++index) {
        if (name == kDirectoryEntries[index]) {
            return index;
        }
    }
    return std::nullopt;
}

void validate_exact_directory_entries(const int directory_descriptor) {
    const int raw_enumeration_descriptor = ::openat(
        directory_descriptor,
        ".",
        O_RDONLY | O_CLOEXEC | O_DIRECTORY | O_NOFOLLOW
    );
    if (raw_enumeration_descriptor == -1) {
        fail_posix("open graph directory for enumeration", errno);
    }
    UniqueFileDescriptor enumeration_descriptor(raw_enumeration_descriptor);

    DIR* const raw_directory = ::fdopendir(enumeration_descriptor.get());
    if (raw_directory == nullptr) {
        fail_posix("fdopendir graph directory", errno);
    }
    static_cast<void>(enumeration_descriptor.release());
    UniqueDirectory directory(raw_directory);

    std::array<bool, kDirectoryEntries.size()> found{};
    errno = 0;
    while (dirent* const entry = ::readdir(directory.get())) {
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            errno = 0;
            continue;
        }
        const std::optional<std::size_t> index = expected_entry_index(name);
        if (!index.has_value()) {
            fail("unexpected directory entry '" + std::string(name) + "'");
        }
        if (found[*index]) {
            fail("duplicate directory entry '" + std::string(name) + "'");
        }
        found[*index] = true;
        errno = 0;
    }
    const int read_error = errno;
    if (directory.close_once() == -1 && read_error == 0) {
        fail_posix("closedir graph directory", errno);
    }
    if (read_error != 0) {
        fail_posix("readdir graph directory", read_error);
    }

    for (std::size_t index = 0U; index < found.size(); ++index) {
        if (!found[index]) {
            fail(
                "missing required entry '" +
                std::string(kDirectoryEntries[index]) + "'"
            );
        }
    }
}

[[nodiscard]] std::string read_manifest_bytes(
    const int directory_descriptor
) {
    const int raw_descriptor = ::openat(
        directory_descriptor,
        "metadata.json",
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY
    );
    if (raw_descriptor == -1) {
        fail_posix("open metadata.json", errno);
    }
    UniqueFileDescriptor descriptor(raw_descriptor);

    struct stat status {};
    if (::fstat(descriptor.get(), &status) == -1) {
        fail_posix("fstat metadata.json", errno);
    }
    if (!S_ISREG(status.st_mode)) {
        fail("metadata.json is not a regular file");
    }
    if (status.st_size < 0) {
        fail("metadata.json has a negative length");
    }
    const std::uint64_t byte_count = static_cast<std::uint64_t>(status.st_size);
    if (byte_count > kMaxManifestBytes) {
        fail("metadata.json exceeds kMaxManifestBytes");
    }
    if (byte_count > std::numeric_limits<std::size_t>::max()) {
        fail("metadata.json length is not representable in memory");
    }

    std::string bytes(static_cast<std::size_t>(byte_count), '\0');
    if (!bytes.empty()) {
        try {
            tbank::platform::pread_exact(
                descriptor.get(),
                std::as_writable_bytes(std::span<char>(bytes)),
                0U
            );
        } catch (const std::exception& error) {
            fail(std::string("read metadata.json: ") + error.what());
        }
    }
    return bytes;
}

[[nodiscard]] std::filesystem::path descriptor_relative_path(
    const int directory_descriptor,
    const std::string_view name
) {
    // Resolve through the no-follow directory fd, not a caller path that may be renamed.
    return std::filesystem::path("/proc/self/fd") /
        std::to_string(directory_descriptor) / std::string(name);
}

[[nodiscard]] ValidatedBinaryFileReader open_binary(
    const int directory_descriptor,
    const ManifestFileKind kind,
    const BinaryMagic& magic,
    const std::size_t io_chunk_bytes
) {
    const std::string_view name = manifest_file_name(kind);
    try {
        return ValidatedBinaryFileReader::open(
            descriptor_relative_path(directory_descriptor, name),
            magic,
            manifest_record_bytes(kind),
            io_chunk_bytes
        );
    } catch (const std::exception& error) {
        fail(std::string(name) + ": " + error.what());
    }
}

void compare_descriptor(
    const ManifestFileDescriptor& descriptor,
    const ValidatedBinaryFileReader& file
) {
    const std::string name(manifest_file_name(descriptor.kind));
    const ValidatedBinaryFileInfo info = file.info();
    const BinaryHeader& header = info.header;

    if (descriptor.record_bytes != header.record_bytes) {
        fail(name + ": manifest/header record_bytes mismatch");
    }
    if (descriptor.record_count != header.record_count) {
        fail(name + ": manifest/header record_count mismatch");
    }
    if (descriptor.payload_bytes != header.payload_bytes) {
        fail(name + ": manifest/header payload_bytes mismatch");
    }
    if (descriptor.file_bytes != info.file_bytes) {
        fail(name + ": manifest/actual file_bytes mismatch");
    }
    if (descriptor.crc64 != header.payload_crc64) {
        fail(name + ": manifest/header CRC-64 mismatch");
    }
}

class SequentialRecordCursor final {
public:
    SequentialRecordCursor(
        const ValidatedBinaryFileReader& reader,
        const std::size_t io_chunk_bytes,
        std::string_view diagnostic_name
    )
        : reader_(reader),
          header_(reader.header()),
          buffer_(io_chunk_bytes),
          diagnostic_name_(diagnostic_name) {}

    void next(const std::span<std::byte> record) {
        if (record.size() != header_.record_bytes) {
            fail(
                diagnostic_name_ +
                ": internal cursor requested a non-record-sized value"
            );
        }
        if (records_read_ == header_.record_count) {
            fail(diagnostic_name_ + ": record stream ended unexpectedly");
        }

        std::size_t copied = 0U;
        while (copied < record.size()) {
            if (buffer_position_ == buffer_size_) {
                refill();
            }
            const std::size_t available = buffer_size_ - buffer_position_;
            const std::size_t count = std::min(
                available, record.size() - copied
            );
            std::copy_n(
                buffer_.data() + buffer_position_,
                count,
                record.data() + copied
            );
            buffer_position_ += count;
            copied += count;
        }
        ++records_read_;
    }

    [[nodiscard]] std::uint64_t records_read() const noexcept {
        return records_read_;
    }

    [[nodiscard]] std::uint64_t record_count() const noexcept {
        return header_.record_count;
    }

    void require_exhausted() const {
        if (records_read_ != header_.record_count) {
            fail(diagnostic_name_ + ": record stream was not consumed exactly");
        }
    }

private:
    void refill() {
        if (payload_offset_ == header_.payload_bytes) {
            fail(diagnostic_name_ + ": payload ended inside a record");
        }
        const std::uint64_t remaining = header_.payload_bytes - payload_offset_;
        const std::size_t request = static_cast<std::size_t>(std::min(
            remaining, size_to_u64(buffer_.size())
        ));
        try {
            reader_.read_payload(
                payload_offset_,
                std::span<std::byte>(buffer_.data(), request)
            );
        } catch (const std::exception& error) {
            fail(diagnostic_name_ + ": payload read failed: " + error.what());
        }
        payload_offset_ += size_to_u64(request);
        buffer_position_ = 0U;
        buffer_size_ = request;
    }

    const ValidatedBinaryFileReader& reader_;
    BinaryHeader header_;
    std::vector<std::byte> buffer_;
    std::string diagnostic_name_;
    std::uint64_t payload_offset_ = 0U;
    std::size_t buffer_position_ = 0U;
    std::size_t buffer_size_ = 0U;
    std::uint64_t records_read_ = 0U;
};

class U32Cursor final {
public:
    U32Cursor(
        const ValidatedBinaryFileReader& reader,
        const std::size_t io_chunk_bytes,
        const std::string_view diagnostic_name
    )
        : cursor_(reader, io_chunk_bytes, diagnostic_name) {}

    [[nodiscard]] std::uint32_t next() {
        std::array<std::byte, kScalarRecordBytes> bytes{};
        cursor_.next(bytes);
        return decode_u32_le(bytes);
    }

    [[nodiscard]] std::uint64_t records_read() const noexcept {
        return cursor_.records_read();
    }

    void require_exhausted() const {
        cursor_.require_exhausted();
    }

private:
    SequentialRecordCursor cursor_;
};

[[nodiscard]] std::uint64_t checked_sum(
    const std::uint64_t sum,
    const std::uint32_t value,
    const std::string_view diagnostic_name
) {
    if (static_cast<std::uint64_t>(value)
        > std::numeric_limits<std::uint64_t>::max() - sum) {
        fail(std::string(diagnostic_name) + ": uint64 sum overflow");
    }
    return sum + value;
}

void validate_vertex_ids(
    const GraphManifest& manifest,
    const ValidatedBinaryFileReader& reader,
    const std::size_t io_chunk_bytes
) {
    SequentialRecordCursor cursor(reader, io_chunk_bytes, "vertex_ids.bin");
    std::optional<std::int32_t> previous;
    for (std::uint64_t index = 0U; index < manifest.vertex_count; ++index) {
        std::array<std::byte, kScalarRecordBytes> bytes{};
        cursor.next(bytes);
        const std::int32_t value = decode_i32_le(bytes);
        if (previous.has_value() && value <= *previous) {
            fail("vertex_ids.bin: IDs must be strictly ascending signed int32");
        }
        previous = value;
    }
    cursor.require_exhausted();
}

void validate_count_sum(
    const ValidatedBinaryFileReader& reader,
    const std::size_t io_chunk_bytes,
    const std::uint32_t vertex_count,
    const std::uint64_t expected_edge_count,
    const std::string_view diagnostic_name,
    const std::string_view bound_diagnostic
) {
    U32Cursor cursor(reader, io_chunk_bytes, diagnostic_name);
    std::uint64_t sum = 0U;
    for (std::uint64_t index = 0U; index < vertex_count; ++index) {
        const std::uint32_t value = cursor.next();
        if (value > vertex_count) {
            fail(
                std::string(diagnostic_name) + ": " +
                std::string(bound_diagnostic)
            );
        }
        sum = checked_sum(sum, value, diagnostic_name);
    }
    cursor.require_exhausted();
    if (sum != expected_edge_count) {
        fail(
            std::string(diagnostic_name) +
            ": count sum does not equal manifest edge_count"
        );
    }
}

void validate_sources(
    const GraphManifest& manifest,
    const ValidatedBinaryFileReader& count_reader,
    const ValidatedBinaryFileReader& source_reader,
    const std::size_t io_chunk_bytes
) {
    U32Cursor counts(count_reader, io_chunk_bytes, "incoming_counts.bin");
    U32Cursor sources(source_reader, io_chunk_bytes, "incoming_sources.bin");
    std::uint64_t sources_seen = 0U;

    for (std::uint64_t destination = 0U;
         destination < manifest.vertex_count;
         ++destination) {
        const std::uint32_t count = counts.next();
        std::optional<std::uint32_t> previous;
        for (std::uint64_t edge = 0U; edge < count; ++edge) {
            const std::uint32_t source = sources.next();
            if (source >= manifest.vertex_count) {
                fail("incoming_sources.bin: source ID is outside compact range");
            }
            if (previous.has_value() && source <= *previous) {
                fail(
                    "incoming_sources.bin: sources must be strictly ascending "
                    "within each destination"
                );
            }
            previous = source;
            if (sources_seen == std::numeric_limits<std::uint64_t>::max()) {
                fail("incoming_sources.bin: source count overflows uint64");
            }
            ++sources_seen;
        }
    }
    counts.require_exhausted();
    sources.require_exhausted();
    if (sources_seen != manifest.edge_count) {
        fail(
            "incoming_sources.bin: source stream does not contain exactly "
            "manifest edge_count records"
        );
    }
}

[[nodiscard]] tbank::tasks::Task decode_logical_task(
    const std::span<const std::byte, kTaskRecordBytes> bytes
) {
    const TaskRecord record{
        .tag = static_cast<TaskTag>(decode_u32_le(bytes, 0U)),
        .a = decode_u32_le(bytes, 4U),
        .b = decode_u32_le(bytes, 8U),
        .c = decode_u32_le(bytes, 12U),
        .edge_begin = decode_u64_le(bytes, 16U),
        .edge_count = decode_u64_le(bytes, 24U),
    };
    try {
        validate_task_record(record);
    } catch (const std::exception& error) {
        fail(std::string("tasks.bin: invalid physical task record: ") + error.what());
    }

    switch (record.tag) {
        case TaskTag::ordinary:
            return tbank::tasks::OrdinaryTask{
                .dst_begin = record.a,
                .dst_count = record.b,
                .edge_begin = record.edge_begin,
                .edge_count = record.edge_count,
            };
        case TaskTag::hub_slice:
            return tbank::tasks::HubSlice{
                .dst = record.a,
                .slice_index = record.b,
                .slice_count = record.c,
                .edge_begin = record.edge_begin,
                .edge_count = record.edge_count,
            };
    }
    fail("tasks.bin: unknown physical task tag");
}

void validate_task_directory(
    const GraphManifest& manifest,
    const ValidatedBinaryFileReader& count_reader,
    const ValidatedBinaryFileReader& task_reader,
    const std::size_t io_chunk_bytes
) {
    U32Cursor counts(count_reader, io_chunk_bytes, "incoming_counts.bin");
    std::uint32_t next_count_destination = 0U;

    const tbank::tasks::TaskConfig config{
        .edge_slice_size = manifest.edge_slice_size,
        .max_task_edges = manifest.max_task_edges,
        .max_task_vertices = manifest.max_task_vertices,
    };
    tbank::tasks::TaskValidator validator(
        config,
        manifest.vertex_count,
        [&](const std::uint32_t destination) {
            if (destination != next_count_destination) {
                fail(
                    "tasks.bin: validator requested incoming counts out of "
                    "sequential order"
                );
            }
            const std::uint32_t result = counts.next();
            ++next_count_destination;
            return result;
        }
    );

    SequentialRecordCursor tasks(task_reader, io_chunk_bytes, "tasks.bin");
    try {
        for (std::uint64_t task_index = 0U;
             task_index < task_reader.header().record_count;
             ++task_index) {
            std::array<std::byte, kTaskRecordBytes> bytes{};
            tasks.next(bytes);
            validator.consume(decode_logical_task(bytes));
        }
        tasks.require_exhausted();
        const tbank::tasks::TaskPartitionSummary summary = validator.finish();
        counts.require_exhausted();
        if (summary.destination_count != manifest.vertex_count) {
            fail("tasks.bin: validated destination count mismatch");
        }
        if (summary.edge_count != manifest.edge_count) {
            fail("tasks.bin: validated edge coverage mismatch");
        }
        if (summary.task_count != task_reader.header().record_count) {
            fail("tasks.bin: validated task count mismatch");
        }
    } catch (const GraphValidationError&) {
        throw;
    } catch (const tbank::tasks::TaskValidationError& error) {
        fail(
            "tasks.bin: task " + std::to_string(error.task_index()) +
            " violates canonical partition: " + error.what()
        );
    } catch (const std::exception& error) {
        fail(std::string("tasks.bin: validation failed: ") + error.what());
    }
}

}  // namespace

ValidatedGraph ValidatedGraph::open(
    const std::filesystem::path& directory,
    const GraphValidationOptions options,
    const GraphManifestPreflight& manifest_preflight
) {
    validate_options(options);
    UniqueFileDescriptor directory_descriptor = open_directory(directory);
    validate_exact_directory_entries(directory_descriptor.get());

    GraphManifest manifest;
    try {
        manifest = parse_manifest(read_manifest_bytes(directory_descriptor.get()));
    } catch (const GraphValidationError&) {
        throw;
    } catch (const ManifestError& error) {
        fail(error.what());
    } catch (const std::exception& error) {
        fail(std::string("metadata.json: ") + error.what());
    }

    if (manifest_preflight) {
        manifest_preflight(manifest);
    }

    ValidatedBinaryFileReader vertex_ids = open_binary(
        directory_descriptor.get(),
        ManifestFileKind::vertex_ids,
        kVertexIdsMagic,
        options.io_chunk_bytes
    );
    ValidatedBinaryFileReader incoming_sources = open_binary(
        directory_descriptor.get(),
        ManifestFileKind::incoming_sources,
        kIncomingSourcesMagic,
        options.io_chunk_bytes
    );
    ValidatedBinaryFileReader incoming_counts = open_binary(
        directory_descriptor.get(),
        ManifestFileKind::incoming_counts,
        kIncomingCountsMagic,
        options.io_chunk_bytes
    );
    ValidatedBinaryFileReader out_degree = open_binary(
        directory_descriptor.get(),
        ManifestFileKind::out_degree,
        kOutDegreeMagic,
        options.io_chunk_bytes
    );
    ValidatedBinaryFileReader tasks = open_binary(
        directory_descriptor.get(),
        ManifestFileKind::tasks,
        kTasksMagic,
        options.io_chunk_bytes
    );

    const std::array<const ValidatedBinaryFileReader*, kManifestFileCount> files{
        &vertex_ids,
        &incoming_sources,
        &incoming_counts,
        &out_degree,
        &tasks,
    };
    for (std::size_t index = 0U; index < files.size(); ++index) {
        if (manifest.files[index].kind != kFileKinds[index]) {
            fail("metadata.json descriptor order changed after parsing");
        }
        compare_descriptor(manifest.files[index], *files[index]);
    }

    validate_vertex_ids(manifest, vertex_ids, options.io_chunk_bytes);
    validate_count_sum(
        incoming_counts,
        options.io_chunk_bytes,
        manifest.vertex_count,
        manifest.edge_count,
        "incoming_counts.bin",
        "incoming count exceeds vertex_count"
    );
    validate_count_sum(
        out_degree,
        options.io_chunk_bytes,
        manifest.vertex_count,
        manifest.edge_count,
        "out_degree.bin",
        "out degree exceeds vertex_count"
    );
    validate_sources(
        manifest,
        incoming_counts,
        incoming_sources,
        options.io_chunk_bytes
    );
    validate_task_directory(
        manifest,
        incoming_counts,
        tasks,
        options.io_chunk_bytes
    );

    return ValidatedGraph(
        std::move(manifest),
        std::move(vertex_ids),
        std::move(incoming_sources),
        std::move(incoming_counts),
        std::move(out_degree),
        std::move(tasks)
    );
}

ValidatedGraph::ValidatedGraph(
    GraphManifest manifest,
    ValidatedBinaryFileReader vertex_ids,
    ValidatedBinaryFileReader incoming_sources,
    ValidatedBinaryFileReader incoming_counts,
    ValidatedBinaryFileReader out_degree,
    ValidatedBinaryFileReader tasks
)
    : manifest_(std::move(manifest)),
      vertex_ids_(std::move(vertex_ids)),
      incoming_sources_(std::move(incoming_sources)),
      incoming_counts_(std::move(incoming_counts)),
      out_degree_(std::move(out_degree)),
      tasks_(std::move(tasks)) {}

const GraphManifest& ValidatedGraph::manifest() const noexcept {
    return manifest_;
}

const ValidatedBinaryFileReader& ValidatedGraph::vertex_ids() const noexcept {
    return vertex_ids_;
}

const ValidatedBinaryFileReader& ValidatedGraph::incoming_sources() const noexcept {
    return incoming_sources_;
}

const ValidatedBinaryFileReader& ValidatedGraph::incoming_counts() const noexcept {
    return incoming_counts_;
}

const ValidatedBinaryFileReader& ValidatedGraph::out_degree() const noexcept {
    return out_degree_;
}

const ValidatedBinaryFileReader& ValidatedGraph::tasks() const noexcept {
    return tasks_;
}

}  // namespace tbank::storage

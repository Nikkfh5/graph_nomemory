#include "tbank/storage/file_reader.hpp"

#include "tbank/platform/checked_io.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tbank::storage {
namespace {

class UniqueFileDescriptor final {
public:
    explicit UniqueFileDescriptor(const int file_descriptor) noexcept
        : file_descriptor_(file_descriptor) {}

    UniqueFileDescriptor(const UniqueFileDescriptor&) = delete;
    UniqueFileDescriptor& operator=(const UniqueFileDescriptor&) = delete;

    ~UniqueFileDescriptor() noexcept {
        if (file_descriptor_ >= 0) {
            static_cast<void>(::close(file_descriptor_));
        }
    }

    [[nodiscard]] int get() const noexcept {
        return file_descriptor_;
    }

    [[nodiscard]] int release() noexcept {
        return std::exchange(file_descriptor_, -1);
    }

private:
    int file_descriptor_;
};

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

std::uint64_t size_to_u64(const std::size_t value) {
    static_assert(
        std::numeric_limits<std::size_t>::digits
            <= std::numeric_limits<std::uint64_t>::digits,
        "validated file reader requires size_t to fit uint64_t"
    );
    return static_cast<std::uint64_t>(value);
}

void validate_crc_chunk_size(const std::size_t crc_chunk_bytes) {
    if (crc_chunk_bytes == 0U) {
        throw std::invalid_argument("CRC chunk size must be positive");
    }
    if (crc_chunk_bytes > kMaximumCrcChunkBytes) {
        throw std::invalid_argument("CRC chunk size exceeds the 1 MiB limit");
    }
}

void validate_path(const std::filesystem::path& path) {
    const std::filesystem::path::string_type& native = path.native();
    if (std::find(native.begin(), native.end(), '\0') != native.end()) {
        throw std::invalid_argument("binary input path contains an embedded NUL");
    }
}

void validate_streaming_crc(
    const int file_descriptor,
    const BinaryHeader& header,
    const std::size_t crc_chunk_bytes
) {
    Crc64Ecma checksum;
    std::vector<std::byte> buffer(crc_chunk_bytes);
    std::uint64_t payload_offset = 0U;

    while (payload_offset < header.payload_bytes) {
        const std::uint64_t remaining = header.payload_bytes - payload_offset;
        const std::size_t request = static_cast<std::size_t>(
            std::min(remaining, size_to_u64(buffer.size()))
        );
        std::span<std::byte> window(buffer.data(), request);
        const std::uint64_t file_offset = tbank::platform::checked_add(
            kBinaryHeaderBytes,
            payload_offset
        );
        tbank::platform::pread_exact(file_descriptor, window, file_offset);
        checksum.update(window);
        payload_offset = tbank::platform::checked_add(
            payload_offset,
            size_to_u64(request)
        );
    }

    if (checksum.value() != header.payload_crc64) {
        throw BinaryError("binary payload CRC-64 mismatch");
    }
}

}  // namespace

ValidatedBinaryFileReader ValidatedBinaryFileReader::open(
    const std::filesystem::path& path,
    const BinaryMagic& expected_magic,
    const std::uint32_t expected_record_bytes,
    const std::size_t crc_chunk_bytes
) {
    validate_crc_chunk_size(crc_chunk_bytes);
    if (expected_record_bytes == 0U) {
        throw BinaryError("expected record_bytes must be positive");
    }
    validate_path(path);

    const int raw_descriptor = ::open(
        path.c_str(),
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY
    );
    if (raw_descriptor == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "open binary file");
    }
    UniqueFileDescriptor descriptor(raw_descriptor);

    const FileFingerprint before = inspect_regular_file(descriptor.get());

    std::array<std::byte, kBinaryHeaderBytes> encoded_header{};
    tbank::platform::pread_exact(descriptor.get(), encoded_header, 0U);
    const BinaryHeader decoded_header = decode_binary_header(encoded_header);
    const std::uint64_t expected_file_bytes = validate_binary_header(
        decoded_header,
        expected_magic,
        expected_record_bytes
    );

    // Full length must fit off_t and equal st_size; checking only the last byte is insufficient.
    static_cast<void>(
        tbank::platform::checked_u64_to_off_t(expected_file_bytes)
    );
    if (before.file_bytes != expected_file_bytes) {
        throw BinaryError("binary file length is not exact");
    }

    validate_streaming_crc(
        descriptor.get(),
        decoded_header,
        crc_chunk_bytes
    );

    const FileFingerprint after = inspect_regular_file(descriptor.get());
    if (after.file_bytes != expected_file_bytes) {
        throw BinaryError("binary file length changed during validation");
    }
    if (!same_fingerprint(before, after)) {
        throw BinaryError("binary file changed during validation");
    }

    return ValidatedBinaryFileReader(
        descriptor.release(),
        decoded_header,
        after
    );
}

ValidatedBinaryFileReader::ValidatedBinaryFileReader(
    const int file_descriptor,
    BinaryHeader header,
    FileFingerprint fingerprint
) noexcept
    : file_descriptor_(file_descriptor),
      header_(std::move(header)),
      fingerprint_(fingerprint) {}

ValidatedBinaryFileReader::ValidatedBinaryFileReader(
    ValidatedBinaryFileReader&& other
) noexcept
    : file_descriptor_(std::exchange(other.file_descriptor_, -1)),
      header_(other.header_),
      fingerprint_(other.fingerprint_) {}

ValidatedBinaryFileReader& ValidatedBinaryFileReader::operator=(
    ValidatedBinaryFileReader&& other
) noexcept {
    if (this != &other) {
        close_once();
        file_descriptor_ = std::exchange(other.file_descriptor_, -1);
        header_ = other.header_;
        fingerprint_ = other.fingerprint_;
    }
    return *this;
}

ValidatedBinaryFileReader::~ValidatedBinaryFileReader() noexcept {
    close_once();
}

bool ValidatedBinaryFileReader::is_open() const noexcept {
    return file_descriptor_ >= 0;
}

BinaryHeader ValidatedBinaryFileReader::header() const {
    ensure_open();
    return header_;
}

ValidatedBinaryFileInfo ValidatedBinaryFileReader::info() const {
    ensure_open();
    return ValidatedBinaryFileInfo{
        .header = header_,
        .file_bytes = fingerprint_.file_bytes,
    };
}

std::uint64_t ValidatedBinaryFileReader::file_size() const {
    ensure_open();
    return fingerprint_.file_bytes;
}

void ValidatedBinaryFileReader::read_payload(
    const std::uint64_t offset,
    const std::span<std::byte> destination
) const {
    ensure_open();
    ensure_unchanged();
    const std::uint64_t byte_count = size_to_u64(destination.size());
    if (offset > header_.payload_bytes
        || byte_count > header_.payload_bytes - offset) {
        throw BinaryError("payload read is outside the validated file");
    }
    if (destination.empty()) {
        return;
    }

    const std::uint64_t file_offset = tbank::platform::checked_add(
        kBinaryHeaderBytes,
        offset
    );
    tbank::platform::pread_exact(
        file_descriptor_,
        destination,
        file_offset
    );
}

ValidatedBinaryFileReader::FileFingerprint
ValidatedBinaryFileReader::inspect_regular_file(const int file_descriptor) {
    struct stat status {};
    if (::fstat(file_descriptor, &status) == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "fstat binary file");
    }
    if (!S_ISREG(status.st_mode)) {
        throw BinaryError("binary input is not a regular file");
    }
    if (status.st_size < 0) {
        throw BinaryError("binary file has a negative length");
    }

    return FileFingerprint{
        .device = status.st_dev,
        .inode = status.st_ino,
        .mode = status.st_mode,
        .file_bytes = static_cast<std::uint64_t>(status.st_size),
        .modification_time = status.st_mtim,
        .status_change_time = status.st_ctim,
    };
}

bool ValidatedBinaryFileReader::same_fingerprint(
    const FileFingerprint& left,
    const FileFingerprint& right
) noexcept {
    const auto same_time = [](const timespec& first,
                              const timespec& second) noexcept {
        return first.tv_sec == second.tv_sec
            && first.tv_nsec == second.tv_nsec;
    };
    return left.device == right.device
        && left.inode == right.inode
        && left.mode == right.mode
        && left.file_bytes == right.file_bytes
        && same_time(left.modification_time, right.modification_time)
        && same_time(left.status_change_time, right.status_change_time);
}

void ValidatedBinaryFileReader::read_records(
    const std::uint64_t record_begin,
    const std::span<std::byte> destination
) const {
    ensure_open();
    const std::size_t record_bytes = header_.record_bytes;
    if (destination.size() % record_bytes != 0U) {
        throw BinaryError("record read destination is not record-aligned");
    }

    const std::uint64_t requested_records = size_to_u64(
        destination.size() / record_bytes
    );
    if (record_begin > header_.record_count
        || requested_records > header_.record_count - record_begin) {
        throw BinaryError("record read is outside the validated file");
    }

    const std::uint64_t payload_offset = tbank::platform::checked_multiply(
        record_begin,
        header_.record_bytes
    );
    read_payload(payload_offset, destination);
}

void ValidatedBinaryFileReader::ensure_open() const {
    if (!is_open()) {
        throw std::logic_error("validated binary file reader is not open");
    }
}

void ValidatedBinaryFileReader::ensure_unchanged() const {
    const FileFingerprint current = inspect_regular_file(file_descriptor_);
    if (!same_fingerprint(fingerprint_, current)) {
        throw BinaryError(
            "binary file changed after validation; immutable generation required"
        );
    }
}

void ValidatedBinaryFileReader::close_once() noexcept {
    if (file_descriptor_ >= 0) {
        const int descriptor = std::exchange(file_descriptor_, -1);
        static_cast<void>(::close(descriptor));
    }
}

}  // namespace tbank::storage

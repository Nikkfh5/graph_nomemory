#include "tbank/preprocess/run_file.hpp"

#include "tbank/platform/checked_io.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tbank::preprocess {
namespace {

static_assert(
    storage::kBinaryHeaderBytes == 48U,
    "run files require the canonical 48-byte binary header"
);
static_assert(
    std::numeric_limits<std::size_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits,
    "run files require size_t to fit in uint64_t"
);

bool contains_embedded_nul(const std::filesystem::path& path) {
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

std::uint64_t size_to_u64(const std::size_t value) noexcept {
    return static_cast<std::uint64_t>(value);
}

}  // namespace

RunFileWriter RunFileWriter::create(
    const std::filesystem::path& path,
    const storage::BinaryMagic& magic,
    const std::uint32_t record_bytes,
    const std::size_t buffer_bytes
) {
    if (contains_embedded_nul(path)) {
        throw std::invalid_argument("run file path contains an embedded NUL");
    }
    if (record_bytes == 0U) {
        throw std::invalid_argument("run file record_bytes must be positive");
    }
    if (buffer_bytes == 0U || buffer_bytes < record_bytes) {
        throw std::invalid_argument(
            "run file buffer must hold at least one complete record"
        );
    }

    // Allocate before file creation; anchor cleanup with an absolute path.
    std::filesystem::path owned_path = std::filesystem::absolute(path);
    std::vector<std::byte> buffer(buffer_bytes);
    const storage::BinaryHeader header{
        .magic = magic,
        .schema_version = storage::kBinarySchemaVersion,
        .header_bytes = storage::kBinaryHeaderBytes,
        .record_bytes = record_bytes,
        .flags = 0U,
        .record_count = 0U,
        .payload_bytes = 0U,
        .payload_crc64 = 0U,
    };

    errno = 0;
    const int file_descriptor = ::open(
        owned_path.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
        0600
    );
    if (file_descriptor == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "create run file");
    }

    RunFileWriter writer(
        std::move(owned_path),
        file_descriptor,
        header,
        std::move(buffer)
    );
    writer.write_placeholder();
    return writer;
}

RunFileWriter::RunFileWriter(
    std::filesystem::path path,
    const int file_descriptor,
    storage::BinaryHeader header,
    std::vector<std::byte> buffer
) noexcept
    : path_(std::move(path)),
      file_descriptor_(file_descriptor),
      header_(header),
      buffer_(std::move(buffer)),
      state_(State::active),
      owns_incomplete_path_(true) {}

RunFileWriter::RunFileWriter(RunFileWriter&& other) noexcept
    : path_(std::move(other.path_)),
      file_descriptor_(std::exchange(other.file_descriptor_, -1)),
      header_(other.header_),
      buffer_(std::move(other.buffer_)),
      buffered_bytes_(other.buffered_bytes_),
      peak_buffered_bytes_(other.peak_buffered_bytes_),
      payload_crc_(other.payload_crc_),
      state_(other.state_),
      failure_(std::move(other.failure_)),
      owns_incomplete_path_(
          std::exchange(other.owns_incomplete_path_, false)
      ) {
    other.buffered_bytes_ = 0U;
    other.peak_buffered_bytes_ = 0U;
    other.state_ = State::moved_from;
}

RunFileWriter& RunFileWriter::operator=(RunFileWriter&& other) noexcept {
    if (this != &other) {
        cleanup_incomplete();
        path_ = std::move(other.path_);
        file_descriptor_ = std::exchange(other.file_descriptor_, -1);
        header_ = other.header_;
        buffer_ = std::move(other.buffer_);
        buffered_bytes_ = other.buffered_bytes_;
        peak_buffered_bytes_ = other.peak_buffered_bytes_;
        payload_crc_ = other.payload_crc_;
        state_ = other.state_;
        failure_ = std::move(other.failure_);
        owns_incomplete_path_ = std::exchange(
            other.owns_incomplete_path_, false
        );

        other.buffered_bytes_ = 0U;
        other.peak_buffered_bytes_ = 0U;
        other.state_ = State::moved_from;
    }
    return *this;
}

RunFileWriter::~RunFileWriter() noexcept {
    cleanup_incomplete();
}

void RunFileWriter::append_records(
    const std::span<const std::byte> records
) {
    ensure_active();
    try {
        if (records.size() % header_.record_bytes != 0U) {
            throw std::invalid_argument(
                "run file append is not a whole number of records"
            );
        }

        const std::uint64_t appended_bytes = size_to_u64(records.size());
        const std::uint64_t appended_records = appended_bytes
            / header_.record_bytes;
        const std::uint64_t next_record_count =
            platform::checked_add(header_.record_count, appended_records);
        const std::uint64_t next_payload_bytes =
            platform::checked_add(header_.payload_bytes, appended_bytes);
        const std::uint64_t count_derived_bytes =
            platform::checked_multiply(
                next_record_count,
                header_.record_bytes
            );
        if (next_payload_bytes != count_derived_bytes) {
            throw std::logic_error("run file record accounting diverged");
        }
        const std::uint64_t next_file_bytes = platform::checked_add(
            header_.header_bytes,
            next_payload_bytes
        );
        static_cast<void>(
            platform::checked_u64_to_off_t(next_file_bytes)
        );

        payload_crc_.update(records);
        header_.record_count = next_record_count;
        header_.payload_bytes = next_payload_bytes;
        header_.payload_crc64 = payload_crc_.value();

        std::size_t source_offset = 0U;
        while (source_offset < records.size()) {
            if (buffered_bytes_ == buffer_.size()) {
                flush_buffer();
            }
            const std::size_t available = buffer_.size() - buffered_bytes_;
            const std::size_t copied = std::min(
                available,
                records.size() - source_offset
            );
            std::copy_n(
                records.data() + source_offset,
                copied,
                buffer_.data() + buffered_bytes_
            );
            source_offset += copied;
            buffered_bytes_ += copied;
            peak_buffered_bytes_ = std::max(
                peak_buffered_bytes_,
                buffered_bytes_
            );
        }
    } catch (...) {
        poison_and_cleanup(std::current_exception());
        throw;
    }
}

RunFileInfo RunFileWriter::finish() {
    return finish_impl(false);
}

RunFileInfo RunFileWriter::finish_and_sync() {
    return finish_impl(true);
}

RunFileInfo RunFileWriter::finish_impl(const bool synchronize) {
    ensure_active();
    try {
        flush_buffer();

        const std::uint64_t expected_file_bytes = platform::checked_add(
            header_.header_bytes,
            header_.payload_bytes
        );
        const off_t expected_file_size =
            platform::checked_u64_to_off_t(expected_file_bytes);

        struct stat status {};
        int fstat_result = -1;
        do {
            errno = 0;
            fstat_result = ::fstat(file_descriptor_, &status);
        } while (fstat_result == -1 && errno == EINTR);
        if (fstat_result == -1) {
            const int error_number = errno;
            throw_posix_error(error_number, "fstat run file");
        }
        if (!S_ISREG(status.st_mode)) {
            throw std::runtime_error("run output is not a regular file");
        }
        if (status.st_size != expected_file_size) {
            throw std::runtime_error("run file length is not exact");
        }

        const auto encoded_header = storage::encode_binary_header(header_);
        platform::pwrite_all(file_descriptor_, encoded_header, 0U);
        if (synchronize) {
            sync_checked();
        }
        close_checked();

        state_ = State::finished;
        owns_incomplete_path_ = false;
        return RunFileInfo{
            .header = header_,
            .file_bytes = expected_file_bytes,
            .peak_buffered_bytes = peak_buffered_bytes_,
        };
    } catch (...) {
        poison_and_cleanup(std::current_exception());
        throw;
    }
}

void RunFileWriter::sync_checked() {
    int result = -1;
    do {
        errno = 0;
        result = ::fdatasync(file_descriptor_);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "fdatasync run file");
    }
}

void RunFileWriter::write_placeholder() {
    ensure_active();
    try {
        const std::array<std::byte, storage::kBinaryHeaderBytes> placeholder{};
        platform::write_all(file_descriptor_, placeholder);
    } catch (...) {
        poison_and_cleanup(std::current_exception());
        throw;
    }
}

void RunFileWriter::flush_buffer() {
    if (buffered_bytes_ == 0U) {
        return;
    }
    platform::write_all(
        file_descriptor_,
        std::span<const std::byte>(buffer_.data(), buffered_bytes_)
    );
    buffered_bytes_ = 0U;
}

void RunFileWriter::ensure_active() const {
    if (state_ == State::failed && failure_) {
        std::rethrow_exception(failure_);
    }
    if (state_ == State::finished) {
        throw std::logic_error("run file writer is already finished");
    }
    if (state_ == State::moved_from) {
        throw std::logic_error("run file writer was moved from");
    }
    if (state_ != State::active || file_descriptor_ < 0
        || !owns_incomplete_path_) {
        throw std::logic_error("run file writer has invalid active state");
    }
}

void RunFileWriter::poison_and_cleanup(
    std::exception_ptr failure
) noexcept {
    if (state_ != State::failed) {
        failure_ = std::move(failure);
        state_ = State::failed;
    }
    cleanup_incomplete();
}

void RunFileWriter::cleanup_incomplete() noexcept {
    if (!owns_incomplete_path_) {
        return;
    }

    if (file_descriptor_ >= 0) {
        const int descriptor = std::exchange(file_descriptor_, -1);
        static_cast<void>(::close(descriptor));
    }
    if (!path_.empty()) {
        static_cast<void>(::unlink(path_.c_str()));
    }
    owns_incomplete_path_ = false;
}

void RunFileWriter::close_checked() {
    const int descriptor = std::exchange(file_descriptor_, -1);
    errno = 0;
    if (::close(descriptor) == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "close run file");
    }
}

}  // namespace tbank::preprocess

#include "tbank/platform/checked_io.hpp"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <string>
#include <system_error>

#include <fcntl.h>
#include <unistd.h>

namespace tbank::platform {
namespace {

static_assert(
    std::numeric_limits<std::size_t>::digits <=
        std::numeric_limits<std::uint64_t>::digits,
    "checked I/O requires size_t to fit in uint64_t"
);

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

void validate_positional_range(
    const std::uint64_t offset,
    const std::size_t byte_count
) {
    if (byte_count == 0U) {
        return;
    }

    const std::uint64_t last_offset = checked_add(
        offset,
        static_cast<std::uint64_t>(byte_count - 1U)
    );
    static_cast<void>(checked_u64_to_off_t(last_offset));
}

}  // namespace

std::uint64_t checked_add(
    const std::uint64_t left,
    const std::uint64_t right
) {
    if (right > std::numeric_limits<std::uint64_t>::max() - left) {
        throw std::overflow_error("uint64 addition overflow");
    }
    return left + right;
}

std::uint64_t checked_multiply(
    const std::uint64_t left,
    const std::uint64_t right
) {
    if (right != 0U
        && left > std::numeric_limits<std::uint64_t>::max() / right) {
        throw std::overflow_error("uint64 multiplication overflow");
    }
    return left * right;
}

off_t checked_u64_to_off_t(const std::uint64_t value) {
    static_assert(
        std::numeric_limits<off_t>::is_signed,
        "checked I/O requires a signed POSIX off_t"
    );
    constexpr off_t kOffTMaximum = std::numeric_limits<off_t>::max();
    const auto maximum = static_cast<std::uint64_t>(kOffTMaximum);
    if (value > maximum) {
        throw std::overflow_error("uint64 value is outside off_t range");
    }
    return static_cast<off_t>(value);
}

std::size_t bounded_io_request(const std::uint64_t remaining) noexcept {
    constexpr auto kSsizeMaximum =
        static_cast<std::uint64_t>(std::numeric_limits<ssize_t>::max());
    constexpr auto kSizeMaximum =
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max());
    constexpr std::uint64_t kRequestMaximum =
        std::min(kSsizeMaximum, kSizeMaximum);
    return static_cast<std::size_t>(std::min(remaining, kRequestMaximum));
}

UnexpectedEofError::UnexpectedEofError(
    const std::size_t requested,
    const std::size_t transferred
)
    : std::runtime_error(
          "unexpected EOF after " + std::to_string(transferred)
          + " of " + std::to_string(requested) + " bytes"
      ),
      requested_(requested),
      transferred_(transferred) {}

std::size_t UnexpectedEofError::requested() const noexcept {
    return requested_;
}

std::size_t UnexpectedEofError::transferred() const noexcept {
    return transferred_;
}

void pread_exact(
    const int file_descriptor,
    const std::span<std::byte> destination,
    const std::uint64_t offset
) {
    pread_exact(
        file_descriptor,
        destination,
        offset,
        [](const int fd,
           void* const buffer,
           const std::size_t byte_count,
           const off_t read_offset) {
            return ::pread(fd, buffer, byte_count, read_offset);
        }
    );
}

void pread_exact(
    const int file_descriptor,
    const std::span<std::byte> destination,
    const std::uint64_t offset,
    const PreadOperation& operation
) {
    if (destination.empty()) {
        return;
    }
    if (!operation) {
        throw std::invalid_argument("pread operation must not be empty");
    }
    validate_positional_range(offset, destination.size());

    std::size_t transferred = 0U;
    while (transferred < destination.size()) {
        const std::size_t remaining = destination.size() - transferred;
        const std::size_t request = bounded_io_request(
            static_cast<std::uint64_t>(remaining)
        );
        const std::uint64_t call_offset = checked_add(
            offset,
            static_cast<std::uint64_t>(transferred)
        );

        errno = 0;
        const ssize_t result = operation(
            file_descriptor,
            destination.data() + transferred,
            request,
            checked_u64_to_off_t(call_offset)
        );
        if (result > 0) {
            const auto completed = static_cast<std::size_t>(result);
            if (completed > request) {
                throw std::runtime_error(
                    "pread returned more bytes than requested"
                );
            }
            transferred += completed;
            continue;
        }
        if (result == 0) {
            throw UnexpectedEofError(destination.size(), transferred);
        }
        if (result != -1) {
            throw std::runtime_error("pread returned an invalid result");
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }
        throw_posix_error(error_number, "pread");
    }
}

void pwrite_all(
    const int file_descriptor,
    const std::span<const std::byte> source,
    const std::uint64_t offset
) {
    if (source.empty()) {
        return;
    }

    errno = 0;
    const int descriptor_flags = ::fcntl(file_descriptor, F_GETFL);
    if (descriptor_flags == -1) {
        const int error_number = errno;
        throw_posix_error(error_number, "fcntl(F_GETFL) before pwrite");
    }
    if ((descriptor_flags & O_APPEND) != 0) {
        throw std::invalid_argument(
            "pwrite requires a descriptor without O_APPEND"
        );
    }

    pwrite_all(
        file_descriptor,
        source,
        offset,
        [](const int fd,
           const void* const buffer,
           const std::size_t byte_count,
           const off_t write_offset) {
            return ::pwrite(fd, buffer, byte_count, write_offset);
        }
    );
}

void pwrite_all(
    const int file_descriptor,
    const std::span<const std::byte> source,
    const std::uint64_t offset,
    const PwriteOperation& operation
) {
    if (source.empty()) {
        return;
    }
    if (!operation) {
        throw std::invalid_argument("pwrite operation must not be empty");
    }
    validate_positional_range(offset, source.size());

    std::size_t transferred = 0U;
    while (transferred < source.size()) {
        const std::size_t remaining = source.size() - transferred;
        const std::size_t request = bounded_io_request(
            static_cast<std::uint64_t>(remaining)
        );
        const std::uint64_t call_offset = checked_add(
            offset,
            static_cast<std::uint64_t>(transferred)
        );

        errno = 0;
        const ssize_t result = operation(
            file_descriptor,
            source.data() + transferred,
            request,
            checked_u64_to_off_t(call_offset)
        );
        if (result > 0) {
            const auto completed = static_cast<std::size_t>(result);
            if (completed > request) {
                throw std::runtime_error(
                    "pwrite returned more bytes than requested"
                );
            }
            transferred += completed;
            continue;
        }
        if (result == 0) {
            throw_posix_error(EIO, "pwrite made no progress");
        }
        if (result != -1) {
            throw std::runtime_error("pwrite returned an invalid result");
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }
        throw_posix_error(error_number, "pwrite");
    }
}

void write_all(
    const int file_descriptor,
    const std::span<const std::byte> source
) {
    write_all(
        file_descriptor,
        source,
        [](const int fd,
           const void* const buffer,
           const std::size_t byte_count) {
            return ::write(fd, buffer, byte_count);
        }
    );
}

void write_all(
    const int file_descriptor,
    const std::span<const std::byte> source,
    const WriteOperation& operation
) {
    if (source.empty()) {
        return;
    }
    if (!operation) {
        throw std::invalid_argument("write operation must not be empty");
    }

    std::size_t transferred = 0U;
    while (transferred < source.size()) {
        const std::size_t remaining = source.size() - transferred;
        const std::size_t request = bounded_io_request(
            static_cast<std::uint64_t>(remaining)
        );

        errno = 0;
        const ssize_t result = operation(
            file_descriptor,
            source.data() + transferred,
            request
        );
        if (result > 0) {
            const auto completed = static_cast<std::size_t>(result);
            if (completed > request) {
                throw std::runtime_error(
                    "write returned more bytes than requested"
                );
            }
            transferred += completed;
            continue;
        }
        if (result == 0) {
            throw_posix_error(EIO, "write made no progress");
        }
        if (result != -1) {
            throw std::runtime_error("write returned an invalid result");
        }

        const int error_number = errno;
        if (error_number == EINTR) {
            continue;
        }
        throw_posix_error(error_number, "write");
    }
}

}  // namespace tbank::platform

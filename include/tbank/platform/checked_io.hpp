#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <stdexcept>

#include <sys/types.h>

namespace tbank::platform {

// The arithmetic helpers throw std::overflow_error instead of allowing a
// byte count or file offset to wrap. checked_u64_to_off_t also rejects values
// that the host POSIX API cannot represent.
[[nodiscard]] std::uint64_t checked_add(
    std::uint64_t left,
    std::uint64_t right
);
[[nodiscard]] std::uint64_t checked_multiply(
    std::uint64_t left,
    std::uint64_t right
);
[[nodiscard]] off_t checked_u64_to_off_t(std::uint64_t value);

// Returns the largest legal request for one POSIX read/write operation. This
// is public so request-size boundary behavior can be tested without creating
// an impossibly large in-memory span.
[[nodiscard]] std::size_t bounded_io_request(
    std::uint64_t remaining
) noexcept;

class UnexpectedEofError final : public std::runtime_error {
public:
    UnexpectedEofError(std::size_t requested, std::size_t transferred);

    [[nodiscard]] std::size_t requested() const noexcept;
    [[nodiscard]] std::size_t transferred() const noexcept;

private:
    std::size_t requested_;
    std::size_t transferred_;
};

using PreadOperation = std::function<ssize_t(
    int,
    void*,
    std::size_t,
    off_t
)>;
using PwriteOperation = std::function<ssize_t(
    int,
    const void*,
    std::size_t,
    off_t
)>;
using WriteOperation = std::function<ssize_t(
    int,
    const void*,
    std::size_t
)>;

// Reads the full span at an absolute offset, retrying EINTR and short reads.
// EOF and system failures are reported explicitly.
void pread_exact(
    int file_descriptor,
    std::span<std::byte> destination,
    std::uint64_t offset
);

// Injectable POSIX-compatible pread for deterministic I/O-failure tests.
void pread_exact(
    int file_descriptor,
    std::span<std::byte> destination,
    std::uint64_t offset,
    const PreadOperation& operation
);

// Writes the full span at an absolute offset, retrying EINTR and short writes.
// O_APPEND descriptors are rejected because Linux applies it to pwrite.
// This function does not provide durability.
void pwrite_all(
    int file_descriptor,
    std::span<const std::byte> source,
    std::uint64_t offset
);

// Injectable POSIX-compatible pwrite for deterministic I/O-failure tests.
void pwrite_all(
    int file_descriptor,
    std::span<const std::byte> source,
    std::uint64_t offset,
    const PwriteOperation& operation
);

// Writes the full span, retrying EINTR and short writes.
// This function does not provide durability.
void write_all(
    int file_descriptor,
    std::span<const std::byte> source
);

// Injectable POSIX-compatible write for deterministic I/O-failure tests.
void write_all(
    int file_descriptor,
    std::span<const std::byte> source,
    const WriteOperation& operation
);

}  // namespace tbank::platform

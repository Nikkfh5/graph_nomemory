#pragma once

#include "tbank/storage/binary.hpp"

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <span>

#include <sys/types.h>

namespace tbank::storage {

inline constexpr std::size_t kDefaultCrcChunkBytes = 64U * 1024U;
inline constexpr std::size_t kMaximumCrcChunkBytes = 1024U * 1024U;

struct ValidatedBinaryFileInfo {
    BinaryHeader header;
    std::uint64_t file_bytes = 0U;

    friend bool operator==(
        const ValidatedBinaryFileInfo&,
        const ValidatedBinaryFileInfo&
    ) = default;
};

// Bounded read-only view that validates framing, length and payload CRC on open.
// It owns one descriptor; callers keep the underlying generation immutable.
// Fingerprints detect observed mutation but are not an atomic snapshot or security proof.
// O_NOFOLLOW protects only the final component, so ancestor directories must be trusted.
class ValidatedBinaryFileReader final {
public:
    [[nodiscard]] static ValidatedBinaryFileReader open(
        const std::filesystem::path& path,
        const BinaryMagic& expected_magic,
        std::uint32_t expected_record_bytes,
        std::size_t crc_chunk_bytes = kDefaultCrcChunkBytes
    );

    ValidatedBinaryFileReader(const ValidatedBinaryFileReader&) = delete;
    ValidatedBinaryFileReader& operator=(
        const ValidatedBinaryFileReader&
    ) = delete;

    ValidatedBinaryFileReader(ValidatedBinaryFileReader&& other) noexcept;
    ValidatedBinaryFileReader& operator=(
        ValidatedBinaryFileReader&& other
    ) noexcept;
    ~ValidatedBinaryFileReader() noexcept;

    [[nodiscard]] bool is_open() const noexcept;
    [[nodiscard]] BinaryHeader header() const;
    [[nodiscard]] ValidatedBinaryFileInfo info() const;
    [[nodiscard]] std::uint64_t file_size() const;

    // offset is relative to the payload. Empty reads are allowed at any
    // offset in the closed interval [0, payload_bytes], and still validate
    // that offset; non-empty reads must fit completely in the payload.
    void read_payload(
        std::uint64_t offset,
        std::span<std::byte> destination
    ) const;

    // destination must contain a whole number of physical records. Empty
    // reads are allowed for record_begin in [0, record_count].
    void read_records(
        std::uint64_t record_begin,
        std::span<std::byte> destination
    ) const;

private:
    struct FileFingerprint {
        dev_t device = 0;
        ino_t inode = 0;
        mode_t mode = 0;
        std::uint64_t file_bytes = 0U;
        timespec modification_time{};
        timespec status_change_time{};
    };

    ValidatedBinaryFileReader(
        int file_descriptor,
        BinaryHeader header,
        FileFingerprint fingerprint
    ) noexcept;

    [[nodiscard]] static FileFingerprint inspect_regular_file(
        int file_descriptor
    );
    [[nodiscard]] static bool same_fingerprint(
        const FileFingerprint& left,
        const FileFingerprint& right
    ) noexcept;

    void ensure_open() const;
    void ensure_unchanged() const;
    void close_once() noexcept;

    int file_descriptor_ = -1;
    BinaryHeader header_{};
    FileFingerprint fingerprint_{};
};

}  // namespace tbank::storage

#pragma once

#include "tbank/storage/binary.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <span>
#include <vector>

namespace tbank::preprocess {

struct RunFileInfo {
    storage::BinaryHeader header{};
    std::uint64_t file_bytes = 0U;
    std::size_t peak_buffered_bytes = 0U;

    friend bool operator==(const RunFileInfo&, const RunFileInfo&) = default;
};

// Exclusive bounded writer for canonical temporary runs. Failures are sticky and clean owned
// incomplete files; the parent directory must remain trusted.
class RunFileWriter final {
public:
    [[nodiscard]] static RunFileWriter create(
        const std::filesystem::path& path,
        const storage::BinaryMagic& magic,
        std::uint32_t record_bytes,
        std::size_t buffer_bytes
    );

    RunFileWriter(const RunFileWriter&) = delete;
    RunFileWriter& operator=(const RunFileWriter&) = delete;

    RunFileWriter(RunFileWriter&& other) noexcept;
    RunFileWriter& operator=(RunFileWriter&& other) noexcept;
    ~RunFileWriter() noexcept;

    // Empty input is allowed; non-empty input must contain whole records.
    void append_records(std::span<const std::byte> records);

    // Writes the canonical header, verifies length, closes once, and returns metadata without
    // borrowed views.
    [[nodiscard]] RunFileInfo finish();

    // finish() with EINTR-safe fdatasync; sync/close failure is sticky and removes the incomplete
    // path best-effort.
    [[nodiscard]] RunFileInfo finish_and_sync();

private:
    enum class State {
        active,
        failed,
        finished,
        moved_from,
    };

    RunFileWriter(
        std::filesystem::path path,
        int file_descriptor,
        storage::BinaryHeader header,
        std::vector<std::byte> buffer
    ) noexcept;

    void write_placeholder();
    void flush_buffer();
    [[nodiscard]] RunFileInfo finish_impl(bool synchronize);
    void sync_checked();
    void ensure_active() const;
    void poison_and_cleanup(std::exception_ptr failure) noexcept;
    void cleanup_incomplete() noexcept;
    void close_checked();

    std::filesystem::path path_{};
    int file_descriptor_ = -1;
    storage::BinaryHeader header_{};
    std::vector<std::byte> buffer_{};
    std::size_t buffered_bytes_ = 0U;
    std::size_t peak_buffered_bytes_ = 0U;
    storage::Crc64Ecma payload_crc_{};
    State state_ = State::moved_from;
    std::exception_ptr failure_{};
    bool owns_incomplete_path_ = false;
};

}  // namespace tbank::preprocess

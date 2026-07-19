#pragma once

#include <cstddef>
#include <filesystem>
#include <functional>
#include <span>
#include <string>
#include <system_error>

#include <sys/types.h>

namespace tbank::platform {

// The namespace outcome is explicit because a failure after rename has
// different recovery semantics from every failure before rename.
enum class PublicationState {
    published,
    not_published,
    target_exists,
    no_replace_unsupported,
    durability_uncertain,
};

enum class PublicationStep {
    none,
    validate_paths,
    open_parent_directory,
    create_temporary_file,
    write_temporary_file,
    sync_temporary_file,
    close_temporary_file,
    open_staging_directory,
    sync_staging_directory,
    close_staging_directory,
    rename_no_replace,
    sync_parent_directory,
};

struct PublicationResult {
    PublicationState state{PublicationState::not_published};
    PublicationStep failed_step{PublicationStep::none};
    std::error_code error{};

    // Cleanup never replaces the primary result. For example, a write error
    // remains the primary failure even if closing or unlinking the temporary
    // file also fails.
    std::error_code cleanup_error{};

    // Set for file publication after a temporary name has been selected. It
    // names the removed temp after a pre-rename failure and the consumed temp
    // after a successful rename.
    std::filesystem::path temporary_path{};

    [[nodiscard]] bool succeeded() const noexcept {
        return state == PublicationState::published;
    }
};

// POSIX-like boundary used both by the Linux implementation and deterministic
// failure-injection tests. Implementations set errno on failure and otherwise
// follow the corresponding syscall's return contract. They must not throw.
class PublicationBackend {
public:
    virtual ~PublicationBackend() = default;

    virtual int open_directory(const std::filesystem::path& path) noexcept = 0;
    virtual int open_file_at(
        int directory_descriptor,
        const std::string& name,
        int flags,
        mode_t mode
    ) noexcept = 0;
    virtual ssize_t write(
        int file_descriptor,
        const void* buffer,
        std::size_t byte_count
    ) noexcept = 0;
    virtual int sync(int file_descriptor) noexcept = 0;
    virtual int close_descriptor(int file_descriptor) noexcept = 0;
    virtual int rename_no_replace_at(
        int source_directory_descriptor,
        const std::string& source_name,
        int target_directory_descriptor,
        const std::string& target_name
    ) noexcept = 0;
    virtual int unlink_file_at(
        int directory_descriptor,
        const std::string& name
    ) noexcept = 0;
};

using PublicationByteSink = std::function<void(std::span<const std::byte>)>;
using PublicationContentWriter = std::function<void(
    const PublicationByteSink&
)>;

// Publishes a synced sibling file atomically without replacing an existing name.
// A failed parent fsync yields durability_uncertain; the parent must be trusted.
[[nodiscard]] PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    std::span<const std::byte> payload,
    mode_t mode = 0644
);

// Streaming publication keeps output off heap; sink failures are sticky.
// Pre-rename failures remove the owned temporary file when possible.
[[nodiscard]] PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    const PublicationContentWriter& writer,
    mode_t mode = 0644
);

[[nodiscard]] PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    const PublicationContentWriter& writer,
    PublicationBackend& backend,
    mode_t mode = 0644
);

[[nodiscard]] PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    std::span<const std::byte> payload,
    PublicationBackend& backend,
    mode_t mode = 0644
);

// Syncs and publishes a sibling staging directory without replacement.
// Pre-rename failure keeps staging; no rollback follows a successful rename.
[[nodiscard]] PublicationResult publish_staging_directory_no_replace(
    const std::filesystem::path& staging,
    const std::filesystem::path& target
);

[[nodiscard]] PublicationResult publish_staging_directory_no_replace(
    const std::filesystem::path& staging,
    const std::filesystem::path& target,
    PublicationBackend& backend
);

}  // namespace tbank::platform

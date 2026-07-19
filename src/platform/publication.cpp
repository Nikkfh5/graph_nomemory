#include "tbank/platform/publication.hpp"

#include "tbank/platform/checked_io.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#include <sys/syscall.h>
#include <unistd.h>

namespace tbank::platform {
namespace {

constexpr unsigned int kRenameNoReplace = 1U;
constexpr std::size_t kMaximumTemporaryNameAttempts = 128U;

std::atomic<std::uint64_t> temporary_name_sequence{0U};

class PosixPublicationBackend final : public PublicationBackend {
public:
    int open_directory(const std::filesystem::path& path) noexcept override {
        return ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    }

    int open_file_at(
        const int directory_descriptor,
        const std::string& name,
        const int flags,
        const mode_t mode
    ) noexcept override {
        return ::openat(directory_descriptor, name.c_str(), flags, mode);
    }

    ssize_t write(
        const int file_descriptor,
        const void* const buffer,
        const std::size_t byte_count
    ) noexcept override {
        return ::write(file_descriptor, buffer, byte_count);
    }

    int sync(const int file_descriptor) noexcept override {
        return ::fsync(file_descriptor);
    }

    int close_descriptor(const int file_descriptor) noexcept override {
        return ::close(file_descriptor);
    }

    int rename_no_replace_at(
        const int source_directory_descriptor,
        const std::string& source_name,
        const int target_directory_descriptor,
        const std::string& target_name
    ) noexcept override {
#if defined(SYS_renameat2)
        return static_cast<int>(::syscall(
            SYS_renameat2,
            source_directory_descriptor,
            source_name.c_str(),
            target_directory_descriptor,
            target_name.c_str(),
            kRenameNoReplace
        ));
#else
        static_cast<void>(source_directory_descriptor);
        static_cast<void>(source_name);
        static_cast<void>(target_directory_descriptor);
        static_cast<void>(target_name);
        errno = ENOSYS;
        return -1;
#endif
    }

    int unlink_file_at(
        const int directory_descriptor,
        const std::string& name
    ) noexcept override {
        return ::unlinkat(directory_descriptor, name.c_str(), 0);
    }
};

PosixPublicationBackend& posix_backend() {
    static PosixPublicationBackend backend;
    return backend;
}

std::error_code captured_errno() {
    return {
        errno == 0 ? EIO : errno,
        std::generic_category(),
    };
}

std::filesystem::path parent_as_given(
    const std::filesystem::path& path
) {
    std::filesystem::path parent = path.parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    return parent;
}

bool contains_embedded_nul(const std::filesystem::path& path) {
    const auto& native = path.native();
    return native.find(std::filesystem::path::value_type{})
        != std::filesystem::path::string_type::npos;
}

bool valid_leaf_name(const std::filesystem::path& path) {
    if (path.empty() || contains_embedded_nul(path) || !path.has_filename()) {
        return false;
    }
    const std::filesystem::path filename = path.filename();
    return filename != "." && filename != "..";
}

std::string next_temporary_name() {
    const std::uint64_t sequence = temporary_name_sequence.fetch_add(
        1U,
        std::memory_order_relaxed
    );
    return ".tbank-publish-" + std::to_string(::getpid()) + "-"
        + std::to_string(sequence);
}

std::error_code close_once(
    PublicationBackend& backend,
    int& descriptor
) {
    if (descriptor < 0) {
        return {};
    }

    // close(EINTR) may consume the fd; invalidate first and never retry.
    const int descriptor_to_close = std::exchange(descriptor, -1);
    errno = 0;
    if (backend.close_descriptor(descriptor_to_close) == 0) {
        return {};
    }
    return captured_errno();
}

class DescriptorGuard final {
public:
    DescriptorGuard(
        PublicationBackend& backend,
        int& descriptor
    ) noexcept
        : backend_(backend), descriptor_(descriptor) {}

    ~DescriptorGuard() {
        static_cast<void>(close_once(backend_, descriptor_));
    }

    DescriptorGuard(const DescriptorGuard&) = delete;
    DescriptorGuard& operator=(const DescriptorGuard&) = delete;

private:
    PublicationBackend& backend_;
    int& descriptor_;
};

class TemporaryFileGuard final {
public:
    TemporaryFileGuard(
        PublicationBackend& backend,
        int& parent_descriptor,
        const std::string& name,
        bool& present
    ) noexcept
        : backend_(backend),
          parent_descriptor_(parent_descriptor),
          name_(name),
          present_(present) {}

    ~TemporaryFileGuard() {
        if (!present_ || parent_descriptor_ < 0) {
            return;
        }
        errno = 0;
        static_cast<void>(backend_.unlink_file_at(parent_descriptor_, name_));
        present_ = false;
    }

    TemporaryFileGuard(const TemporaryFileGuard&) = delete;
    TemporaryFileGuard& operator=(const TemporaryFileGuard&) = delete;

private:
    PublicationBackend& backend_;
    int& parent_descriptor_;
    const std::string& name_;
    bool& present_;
};

class PublicationSinkState final {
public:
    PublicationSinkState(
        PublicationBackend& backend,
        const int file_descriptor
    ) noexcept
        : backend_(&backend), file_descriptor_(file_descriptor) {}

    void write(const std::span<const std::byte> chunk) {
        std::lock_guard lock(mutex_);
        if (!active_) {
            throw std::logic_error("publication byte sink is no longer active");
        }
        if (primary_error_) {
            throw std::system_error(
                primary_error_,
                "publication byte sink is poisoned"
            );
        }

        try {
            write_all(
                file_descriptor_,
                chunk,
                [&](const int descriptor,
                    const void* const buffer,
                    const std::size_t byte_count) {
                    return backend_->write(descriptor, buffer, byte_count);
                }
            );
        } catch (const std::system_error& error) {
            primary_error_ = error.code();
            throw;
        } catch (...) {
            primary_error_ = {EIO, std::generic_category()};
            throw;
        }
    }

    void terminalize() {
        std::lock_guard lock(mutex_);
        active_ = false;
        backend_ = nullptr;
        file_descriptor_ = -1;
    }

    [[nodiscard]] std::error_code primary_error() const {
        std::lock_guard lock(mutex_);
        return primary_error_;
    }

private:
    mutable std::mutex mutex_;
    PublicationBackend* backend_;
    int file_descriptor_;
    bool active_{true};
    std::error_code primary_error_{};
};

void remember_cleanup_error(
    PublicationResult& result,
    const std::error_code& error
) {
    if (error && !result.cleanup_error) {
        result.cleanup_error = error;
    }
}

PublicationResult basic_result(
    const PublicationState state,
    const PublicationStep step,
    const std::error_code& error
) {
    return PublicationResult{
        .state = state,
        .failed_step = step,
        .error = error,
        .cleanup_error = {},
        .temporary_path = {},
    };
}

PublicationState rename_failure_state(const int error_number) {
    if (error_number == EEXIST) {
        return PublicationState::target_exists;
    }
    if (error_number == ENOSYS || error_number == EOPNOTSUPP
        || error_number == EINVAL) {
        return PublicationState::no_replace_unsupported;
    }
    return PublicationState::not_published;
}

void close_parent(
    PublicationResult& result,
    PublicationBackend& backend,
    int& parent_descriptor
) {
    remember_cleanup_error(
        result,
        close_once(backend, parent_descriptor)
    );
}

PublicationResult fail_file_before_rename(
    const PublicationState state,
    const PublicationStep step,
    const std::error_code& error,
    PublicationBackend& backend,
    int& file_descriptor,
    int& parent_descriptor,
    const std::filesystem::path& temporary_path,
    const std::string& temporary_name,
    bool& temporary_created
) {
    PublicationResult result = basic_result(state, step, error);
    if (temporary_created && !temporary_path.empty()) {
        result.temporary_path = temporary_path;
    }

    remember_cleanup_error(result, close_once(backend, file_descriptor));
    if (temporary_created) {
        errno = 0;
        if (backend.unlink_file_at(parent_descriptor, temporary_name) != 0) {
            remember_cleanup_error(result, captured_errno());
        }
        temporary_created = false;
    }
    close_parent(result, backend, parent_descriptor);
    return result;
}

PublicationResult fail_directory_before_rename(
    const PublicationState state,
    const PublicationStep step,
    const std::error_code& error,
    PublicationBackend& backend,
    int& staging_descriptor,
    int& parent_descriptor
) {
    PublicationResult result = basic_result(state, step, error);
    remember_cleanup_error(result, close_once(backend, staging_descriptor));
    close_parent(result, backend, parent_descriptor);
    return result;
}

}  // namespace

PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    const std::span<const std::byte> payload,
    const mode_t mode
) {
    return publish_file_no_replace(
        target,
        PublicationContentWriter(
            [payload](const PublicationByteSink& sink) { sink(payload); }
        ),
        posix_backend(),
        mode
    );
}

PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    const std::span<const std::byte> payload,
    PublicationBackend& backend,
    const mode_t mode
) {
    return publish_file_no_replace(
        target,
        PublicationContentWriter(
            [payload](const PublicationByteSink& sink) { sink(payload); }
        ),
        backend,
        mode
    );
}

PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    const PublicationContentWriter& writer,
    const mode_t mode
) {
    return publish_file_no_replace(target, writer, posix_backend(), mode);
}

PublicationResult publish_file_no_replace(
    const std::filesystem::path& target,
    const PublicationContentWriter& writer,
    PublicationBackend& backend,
    const mode_t mode
) {
    if (!valid_leaf_name(target) || !writer) {
        return basic_result(
            PublicationState::not_published,
            PublicationStep::validate_paths,
            {EINVAL, std::generic_category()}
        );
    }

    const std::filesystem::path parent = parent_as_given(target);
    const std::string target_name = target.filename().string();

    int parent_descriptor = -1;
    DescriptorGuard parent_guard(backend, parent_descriptor);
    errno = 0;
    parent_descriptor = backend.open_directory(parent);
    if (parent_descriptor < 0) {
        return basic_result(
            PublicationState::not_published,
            PublicationStep::open_parent_directory,
            captured_errno()
        );
    }

    std::string temporary_name;
    std::filesystem::path temporary_path;
    bool temporary_created = false;
    TemporaryFileGuard temporary_guard(
        backend,
        parent_descriptor,
        temporary_name,
        temporary_created
    );
    int file_descriptor = -1;
    DescriptorGuard file_guard(backend, file_descriptor);
    for (std::size_t attempt = 0U;
         attempt < kMaximumTemporaryNameAttempts;
         ++attempt) {
        temporary_name = next_temporary_name();
        if (temporary_name == target_name) {
            continue;
        }
        temporary_path = parent / temporary_name;
        errno = 0;
        file_descriptor = backend.open_file_at(
            parent_descriptor,
            temporary_name,
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            mode
        );
        if (file_descriptor >= 0) {
            temporary_created = true;
            break;
        }
        if (errno != EEXIST) {
            return fail_file_before_rename(
                PublicationState::not_published,
                PublicationStep::create_temporary_file,
                captured_errno(),
                backend,
                file_descriptor,
                parent_descriptor,
                temporary_path,
                temporary_name,
                temporary_created
            );
        }
    }
    if (file_descriptor < 0) {
        return fail_file_before_rename(
            PublicationState::not_published,
            PublicationStep::create_temporary_file,
            {EEXIST, std::generic_category()},
            backend,
            file_descriptor,
            parent_descriptor,
            temporary_path,
            temporary_name,
            temporary_created
        );
    }

    std::shared_ptr<PublicationSinkState> sink_state;
    try {
        sink_state = std::make_shared<PublicationSinkState>(
            backend,
            file_descriptor
        );
        const PublicationByteSink sink = [sink_state](
            const std::span<const std::byte> chunk
        ) {
            sink_state->write(chunk);
        };
        writer(sink);
        sink_state->terminalize();
        const std::error_code sink_error = sink_state->primary_error();
        if (sink_error) {
            return fail_file_before_rename(
                PublicationState::not_published,
                PublicationStep::write_temporary_file,
                sink_error,
                backend,
                file_descriptor,
                parent_descriptor,
                temporary_path,
                temporary_name,
                temporary_created
            );
        }
    } catch (const std::system_error& error) {
        std::error_code primary_error = error.code();
        if (sink_state) {
            sink_state->terminalize();
            const std::error_code sink_error = sink_state->primary_error();
            if (sink_error) {
                primary_error = sink_error;
            }
        }
        return fail_file_before_rename(
            PublicationState::not_published,
            PublicationStep::write_temporary_file,
            primary_error,
            backend,
            file_descriptor,
            parent_descriptor,
            temporary_path,
            temporary_name,
            temporary_created
        );
    } catch (const std::runtime_error&) {
        std::error_code primary_error{EIO, std::generic_category()};
        if (sink_state) {
            sink_state->terminalize();
            const std::error_code sink_error = sink_state->primary_error();
            if (sink_error) {
                primary_error = sink_error;
            }
        }
        return fail_file_before_rename(
            PublicationState::not_published,
            PublicationStep::write_temporary_file,
            primary_error,
            backend,
            file_descriptor,
            parent_descriptor,
            temporary_path,
            temporary_name,
            temporary_created
        );
    } catch (...) {
        std::error_code primary_error{EIO, std::generic_category()};
        if (sink_state) {
            sink_state->terminalize();
            const std::error_code sink_error = sink_state->primary_error();
            if (sink_error) {
                primary_error = sink_error;
            }
        }
        return fail_file_before_rename(
            PublicationState::not_published,
            PublicationStep::write_temporary_file,
            primary_error,
            backend,
            file_descriptor,
            parent_descriptor,
            temporary_path,
            temporary_name,
            temporary_created
        );
    }

    errno = 0;
    if (backend.sync(file_descriptor) != 0) {
        return fail_file_before_rename(
            PublicationState::not_published,
            PublicationStep::sync_temporary_file,
            captured_errno(),
            backend,
            file_descriptor,
            parent_descriptor,
            temporary_path,
            temporary_name,
            temporary_created
        );
    }

    const std::error_code file_close_error = close_once(
        backend,
        file_descriptor
    );
    if (file_close_error) {
        return fail_file_before_rename(
            PublicationState::not_published,
            PublicationStep::close_temporary_file,
            file_close_error,
            backend,
            file_descriptor,
            parent_descriptor,
            temporary_path,
            temporary_name,
            temporary_created
        );
    }

    // Allocate result state before rename so guards can remove the temp on failure.
    PublicationResult result = basic_result(
        PublicationState::published,
        PublicationStep::none,
        {}
    );
    result.temporary_path = temporary_path;

    errno = 0;
    if (backend.rename_no_replace_at(
            parent_descriptor,
            temporary_name,
            parent_descriptor,
            target_name
        ) != 0) {
        const int error_number = errno == 0 ? EIO : errno;
        return fail_file_before_rename(
            rename_failure_state(error_number),
            PublicationStep::rename_no_replace,
            {error_number, std::generic_category()},
            backend,
            file_descriptor,
            parent_descriptor,
            temporary_path,
            temporary_name,
            temporary_created
        );
    }
    temporary_created = false;

    errno = 0;
    if (backend.sync(parent_descriptor) != 0) {
        result.state = PublicationState::durability_uncertain;
        result.failed_step = PublicationStep::sync_parent_directory;
        result.error = captured_errno();
    }

    // Post-publication close failure is cleanup-only; parent-fsync failure remains authoritative.
    close_parent(result, backend, parent_descriptor);
    return result;
}

PublicationResult publish_staging_directory_no_replace(
    const std::filesystem::path& staging,
    const std::filesystem::path& target
) {
    return publish_staging_directory_no_replace(
        staging,
        target,
        posix_backend()
    );
}

PublicationResult publish_staging_directory_no_replace(
    const std::filesystem::path& staging,
    const std::filesystem::path& target,
    PublicationBackend& backend
) {
    if (!valid_leaf_name(staging) || !valid_leaf_name(target)
        || parent_as_given(staging) != parent_as_given(target)
        || staging.filename() == target.filename()) {
        return basic_result(
            PublicationState::not_published,
            PublicationStep::validate_paths,
            {EINVAL, std::generic_category()}
        );
    }

    const std::filesystem::path parent = parent_as_given(target);
    const std::string staging_name = staging.filename().string();
    const std::string target_name = target.filename().string();

    int parent_descriptor = -1;
    DescriptorGuard parent_guard(backend, parent_descriptor);
    errno = 0;
    parent_descriptor = backend.open_directory(parent);
    if (parent_descriptor < 0) {
        return basic_result(
            PublicationState::not_published,
            PublicationStep::open_parent_directory,
            captured_errno()
        );
    }

    int staging_descriptor = -1;
    DescriptorGuard staging_guard(backend, staging_descriptor);
    errno = 0;
    staging_descriptor = backend.open_file_at(
        parent_descriptor,
        staging_name,
        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW,
        0
    );
    if (staging_descriptor < 0) {
        return fail_directory_before_rename(
            PublicationState::not_published,
            PublicationStep::open_staging_directory,
            captured_errno(),
            backend,
            staging_descriptor,
            parent_descriptor
        );
    }

    PublicationResult result = basic_result(
        PublicationState::published,
        PublicationStep::none,
        {}
    );

    errno = 0;
    if (backend.sync(staging_descriptor) != 0) {
        return fail_directory_before_rename(
            PublicationState::not_published,
            PublicationStep::sync_staging_directory,
            captured_errno(),
            backend,
            staging_descriptor,
            parent_descriptor
        );
    }

    const std::error_code staging_close_error = close_once(
        backend,
        staging_descriptor
    );
    if (staging_close_error) {
        return fail_directory_before_rename(
            PublicationState::not_published,
            PublicationStep::close_staging_directory,
            staging_close_error,
            backend,
            staging_descriptor,
            parent_descriptor
        );
    }

    errno = 0;
    if (backend.rename_no_replace_at(
            parent_descriptor,
            staging_name,
            parent_descriptor,
            target_name
        ) != 0) {
        const int error_number = errno == 0 ? EIO : errno;
        return fail_directory_before_rename(
            rename_failure_state(error_number),
            PublicationStep::rename_no_replace,
            {error_number, std::generic_category()},
            backend,
            staging_descriptor,
            parent_descriptor
        );
    }

    errno = 0;
    if (backend.sync(parent_descriptor) != 0) {
        result.state = PublicationState::durability_uncertain;
        result.failed_step = PublicationStep::sync_parent_directory;
        result.error = captured_errno();
    }
    close_parent(result, backend, parent_descriptor);
    return result;
}

}  // namespace tbank::platform

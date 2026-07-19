#include "tbank/run/cli.hpp"

#include "tbank/io/edge_csv.hpp"
#include "tbank/pagerank/analyze.hpp"
#include "tbank/pagerank/pagerank.hpp"
#include "tbank/platform/checked_io.hpp"
#include "tbank/platform/publication.hpp"
#include "tbank/preprocess/prepare.hpp"
#include "tbank/resources/page_rank.hpp"
#include "tbank/run/config.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/graph.hpp"
#include "tbank/storage/manifest.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

using tbank::pagerank::AnalyzeResult;
using tbank::pagerank::AnalyzeStatus;
using tbank::platform::PublicationState;
using tbank::preprocess::CountedGraphPreprocessResult;
using tbank::preprocess::CountedGraphPreprocessor;

constexpr std::string_view kHelp =
    "Usage: main INPUT.csv OUTPUT.csv [CONFIG]\n"
    "\n"
    "Configuration: "
    "https://github.com/Nikkfh5/graph_nomemory/blob/main/"
    "docs/configuration.md\n";

class OutputExistsError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class InputChangedError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class StackLimitError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

[[noreturn]] void throw_posix_error(
    const int error_number,
    const char* const operation
) {
    throw std::system_error(
        error_number == 0 ? EIO : error_number,
        std::generic_category(),
        operation
    );
}

void report_error(
    const std::string_view kind,
    const std::string_view message
) noexcept {
    try {
        std::string line = "main: ";
        line.append(kind);
        line.append(": ");
        line.append(message);
        line.push_back('\n');
        tbank::cli::write_stderr(line);
    } catch (...) {
        tbank::cli::write_stderr("main: error reporting failed\n");
    }
}

struct Invocation {
    std::filesystem::path input;
    std::filesystem::path output;
    std::optional<std::filesystem::path> config;
};

[[nodiscard]] Invocation parse_invocation(
    const int argument_count,
    char* arguments[]
) {
    if (arguments == nullptr || (argument_count != 3 && argument_count != 4)) {
        throw tbank::cli::UsageError(
            "expected INPUT.csv OUTPUT.csv and at most one CONFIG path"
        );
    }
    if (arguments[0] == nullptr || arguments[1] == nullptr
        || arguments[2] == nullptr
        || (argument_count == 4 && arguments[3] == nullptr)) {
        throw tbank::cli::UsageError("invalid process argument vector");
    }

    const std::string_view input(arguments[1]);
    const std::string_view output(arguments[2]);
    if (input.empty() || output.empty()) {
        throw tbank::cli::UsageError("input and output paths must not be empty");
    }
    tbank::cli::require_valid_utf8("input path", input);
    tbank::cli::require_valid_utf8("output path", output);

    Invocation invocation{
        .input = std::filesystem::path(std::string(input)),
        .output = std::filesystem::path(std::string(output)),
        .config = std::nullopt,
    };
    if (argument_count == 4) {
        const std::string_view config(arguments[3]);
        if (config.empty()) {
            throw tbank::cli::UsageError("config path must not be empty");
        }
        tbank::cli::require_valid_utf8("config path", config);
        invocation.config = std::filesystem::path(std::string(config));
    }
    return invocation;
}

[[nodiscard]] std::uint64_t observed_main_stack_bytes() {
    struct rlimit limit {};
    errno = 0;
    if (::getrlimit(RLIMIT_STACK, &limit) == -1) {
        throw_posix_error(errno, "read main stack limit");
    }
    if (limit.rlim_cur == RLIM_INFINITY) {
        throw StackLimitError(
            "soft RLIMIT_STACK is unlimited; install a finite limit before "
            "running main"
        );
    }
    if (limit.rlim_cur == 0U) {
        throw StackLimitError("soft RLIMIT_STACK must be positive");
    }
    if constexpr (sizeof(rlim_t) > sizeof(std::uint64_t)) {
        if (limit.rlim_cur > std::numeric_limits<std::uint64_t>::max()) {
            throw StackLimitError(
                "soft RLIMIT_STACK is outside the supported uint64 range"
            );
        }
    }
    return static_cast<std::uint64_t>(limit.rlim_cur);
}

void require_real_directory(
    const std::filesystem::path& path,
    const char* const operation
) {
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(path.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        throw_posix_error(errno, operation);
    }
    if (!S_ISDIR(status.st_mode)) {
        throw std::invalid_argument(
            std::string(operation) + ": path is not a real directory"
        );
    }
}

void require_absent_output(const std::filesystem::path& output) {
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(output.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == 0) {
        throw OutputExistsError(
            "output path already exists; main never replaces output"
        );
    }
    if (errno != ENOENT) {
        throw_posix_error(errno, "inspect output target");
    }
}

[[nodiscard]] std::filesystem::path resolve_output_path(
    const std::filesystem::path& output
) {
    const std::filesystem::path absolute =
        std::filesystem::absolute(output).lexically_normal();
    if (absolute.filename().empty()) {
        throw std::invalid_argument("output path has an empty final component");
    }
    require_real_directory(absolute.parent_path(), "inspect output parent");
    const std::filesystem::path resolved =
        std::filesystem::canonical(absolute.parent_path()) / absolute.filename();
    tbank::cli::require_valid_utf8("resolved output path", resolved.native());
    require_absent_output(resolved);
    return resolved;
}

class InputFile final {
public:
    explicit InputFile(const std::filesystem::path& path) {
        do {
            errno = 0;
            descriptor_ = ::open(
                path.c_str(),
                O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK
            );
        } while (descriptor_ == -1 && errno == EINTR);
        if (descriptor_ == -1) {
            throw_posix_error(errno, "open input CSV");
        }
        try {
            before_ = inspect();
        } catch (...) {
            static_cast<void>(::close(descriptor_));
            descriptor_ = -1;
            throw;
        }
    }

    InputFile(const InputFile&) = delete;
    InputFile& operator=(const InputFile&) = delete;

    InputFile(InputFile&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)),
          before_(other.before_) {}

    InputFile& operator=(InputFile&&) = delete;

    ~InputFile() noexcept {
        if (descriptor_ != -1) {
            static_cast<void>(::close(descriptor_));
        }
    }

    [[nodiscard]] std::uint64_t size_bytes() const noexcept {
        return static_cast<std::uint64_t>(before_.st_size);
    }

    void read_exact(
        const std::span<char> destination,
        const std::uint64_t offset
    ) const {
        tbank::platform::pread_exact(
            descriptor_, std::as_writable_bytes(destination), offset
        );
    }

    void verify_unchanged() const {
        require_no_growth();
        const struct stat after = inspect();
        if (!same_snapshot(before_, after)) {
            throw InputChangedError(
                "input CSV metadata changed while it was being consumed"
            );
        }
    }

private:
    [[nodiscard]] struct stat inspect() const {
        struct stat status {};
        int result = -1;
        do {
            errno = 0;
            result = ::fstat(descriptor_, &status);
        } while (result == -1 && errno == EINTR);
        if (result == -1) {
            throw_posix_error(errno, "inspect input CSV");
        }
        if (!S_ISREG(status.st_mode)) {
            throw std::invalid_argument("input CSV must be a regular file");
        }
        if (status.st_size < 0) {
            throw std::runtime_error("input CSV has a negative file size");
        }
        return status;
    }

    void require_no_growth() const {
        std::byte probe{};
        ssize_t result = -1;
        do {
            errno = 0;
            result = ::pread(
                descriptor_,
                &probe,
                1U,
                tbank::platform::checked_u64_to_off_t(size_bytes())
            );
        } while (result == -1 && errno == EINTR);
        if (result == -1) {
            throw_posix_error(errno, "probe input CSV end-of-file");
        }
        if (result != 0) {
            throw InputChangedError(
                "input CSV grew while it was being consumed"
            );
        }
    }

    [[nodiscard]] static bool same_snapshot(
        const struct stat& before,
        const struct stat& after
    ) noexcept {
        return before.st_dev == after.st_dev
            && before.st_ino == after.st_ino
            && before.st_mode == after.st_mode
            && before.st_nlink == after.st_nlink
            && before.st_size == after.st_size
            && before.st_mtim.tv_sec == after.st_mtim.tv_sec
            && before.st_mtim.tv_nsec == after.st_mtim.tv_nsec
            && before.st_ctim.tv_sec == after.st_ctim.tv_sec
            && before.st_ctim.tv_nsec == after.st_ctim.tv_nsec;
    }

    int descriptor_ = -1;
    struct stat before_ {};
};

class UniqueRunDirectory final {
public:
    [[nodiscard]] static UniqueRunDirectory create(
        const std::filesystem::path& parent
    ) {
        const std::filesystem::path pattern_path = parent / ".tbank-run-XXXXXX";
        const std::string pattern = pattern_path.native();
        std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
        mutable_pattern.push_back('\0');
        char* const created = ::mkdtemp(mutable_pattern.data());
        if (created == nullptr) {
            throw_posix_error(errno, "create private main run directory");
        }
        return UniqueRunDirectory(std::filesystem::path(created));
    }

    UniqueRunDirectory(const UniqueRunDirectory&) = delete;
    UniqueRunDirectory& operator=(const UniqueRunDirectory&) = delete;

    ~UniqueRunDirectory() noexcept {
        cleanup_noexcept();
    }

    [[nodiscard]] std::filesystem::path workspace() const {
        return path_ / "workspace";
    }

    [[nodiscard]] std::filesystem::path graph() const {
        return path_ / "graph";
    }

    void cleanup_checked() {
        if (!active_) {
            return;
        }
        std::error_code error;
        static_cast<void>(std::filesystem::remove_all(path_, error));
        if (error) {
            throw std::system_error(error, "remove private main run directory");
        }
        active_ = false;
    }

private:
    explicit UniqueRunDirectory(std::filesystem::path path)
        : path_(std::move(path)) {}

    void cleanup_noexcept() noexcept {
        if (!active_) {
            return;
        }
        try {
            std::error_code ignored;
            static_cast<void>(std::filesystem::remove_all(path_, ignored));
        } catch (...) {
            // Best effort only: cleanup must not replace the primary failure.
        }
    }

    std::filesystem::path path_;
    bool active_ = true;
};

[[nodiscard]] int prepare_outcome(
    const CountedGraphPreprocessResult& result
) noexcept {
    if (result.graph.publication.state == PublicationState::published) {
        return tbank::cli::kExitSuccess;
    }
    if (result.graph.publication.state
        == PublicationState::durability_uncertain) {
        report_error(
            "durability",
            "intermediate graph publication has uncertain durability"
        );
        return tbank::cli::kExitDurabilityUncertain;
    }
    report_error("publication", "could not publish the intermediate graph");
    return tbank::cli::kExitPublication;
}

[[nodiscard]] int analyze_outcome(const AnalyzeResult& result) noexcept {
    switch (result.status) {
        case AnalyzeStatus::published:
            return tbank::cli::kExitSuccess;
        case AnalyzeStatus::non_converged:
            report_error(
                "non-convergence",
                "PageRank did not converge within the configured iteration cap"
            );
            return tbank::cli::kExitNonConverged;
        case AnalyzeStatus::numerical_failure:
            report_error("numerical", "PageRank numerical verification failed");
            return tbank::cli::kExitNumericalFailure;
        case AnalyzeStatus::publication_failed:
            report_error("publication", "could not publish output CSV");
            return tbank::cli::kExitPublication;
        case AnalyzeStatus::durability_uncertain:
            report_error(
                "durability",
                "output CSV is visible but its crash durability is uncertain"
            );
            return tbank::cli::kExitDurabilityUncertain;
    }
    report_error("internal", "unknown PageRank outcome");
    return tbank::cli::kExitInternal;
}

[[nodiscard]] int run_pipeline(
    const Invocation& invocation,
    tbank::run::RunConfig config
) {
    const std::filesystem::path output = resolve_output_path(invocation.output);
    InputFile input(invocation.input);
    config.preprocess.csv_byte_limit = input.size_bytes();
    static_cast<void>(
        tbank::preprocess::preprocess_static_resource_plan(config.preprocess)
    );

    const std::uint64_t chunk_bytes_u64 =
        config.preprocess.resources.maximum_input_chunk_bytes;
    if (chunk_bytes_u64 > std::numeric_limits<std::size_t>::max()) {
        throw std::overflow_error("input chunk is outside size_t range");
    }
    std::vector<char> buffer(static_cast<std::size_t>(chunk_bytes_u64));

    UniqueRunDirectory run_directory = UniqueRunDirectory::create(
        output.parent_path()
    );
    const CountedGraphPreprocessResult prepared = [&] {
        // Move every preprocessing-only owner into this phase boundary. The
        // input descriptor and configurable input buffer are destroyed before
        // PageRank admission/allocation, so the two planners compose by phase
        // maximum rather than an accidental sum of live allocations.
        InputFile phase_input(std::move(input));
        std::vector<char> phase_buffer(std::move(buffer));
        CountedGraphPreprocessor preprocessor = CountedGraphPreprocessor::create(
            run_directory.workspace(), run_directory.graph(), config.preprocess
        );
        std::uint64_t offset = 0U;
        while (offset < phase_input.size_bytes()) {
            const std::uint64_t remaining = phase_input.size_bytes() - offset;
            const std::size_t selected = static_cast<std::size_t>(std::min(
                remaining, static_cast<std::uint64_t>(phase_buffer.size())
            ));
            std::span<char> chunk(phase_buffer.data(), selected);
            phase_input.read_exact(chunk, offset);
            preprocessor.consume(chunk);
            offset += static_cast<std::uint64_t>(selected);
        }
        phase_input.verify_unchanged();
        return preprocessor.finish();
    }();

    int outcome = prepare_outcome(prepared);
    if (outcome == tbank::cli::kExitSuccess) {
        const AnalyzeResult analyzed = tbank::pagerank::analyze_page_rank_to_csv(
            run_directory.graph(), output, config.pagerank
        );
        outcome = analyze_outcome(analyzed);
    }

    // The output uses independent no-replace publication. Never roll it back if
    // cleanup fails after publication; report cleanup as a system failure.
    run_directory.cleanup_checked();
    return outcome;
}

void admit_observed_main_stack(const tbank::run::RunConfig& config) {
    try {
        static_cast<void>(tbank::resources::page_rank_memory_plan(
            1U, config.pagerank.resources
        ));
    } catch (const tbank::resources::PageRankMemoryLimitError&) {
        throw;
    } catch (const std::overflow_error& error) {
        throw StackLimitError(
            std::string("soft RLIMIT_STACK cannot be inventoried: ")
            + error.what()
        );
    }
}

[[nodiscard]] int report_reduction_progress(
    const tbank::preprocess::CompactEdgeReductionProgressError& error
) noexcept {
    try {
        error.rethrow_cause();
    } catch (const tbank::preprocess::CompactEdgeMemoryLimitError& cause) {
        report_error("resource", cause.what());
        return tbank::cli::kExitResource;
    } catch (const tbank::preprocess::CompactEdgeDiskSpaceError& cause) {
        report_error("resource", cause.what());
        return tbank::cli::kExitResource;
    } catch (const std::system_error& cause) {
        report_error("system", cause.what());
        return tbank::cli::kExitSystem;
    } catch (const std::invalid_argument& cause) {
        report_error("data", cause.what());
        return tbank::cli::kExitData;
    } catch (const std::exception& cause) {
        report_error("internal", cause.what());
        return tbank::cli::kExitInternal;
    } catch (...) {
        report_error("internal", "unknown compact-edge reduction failure");
        return tbank::cli::kExitInternal;
    }
}

}  // namespace

int main(const int argument_count, char* arguments[]) {
    try {
        tbank::cli::initialize_result_channels();
        if (tbank::cli::is_help_request(argument_count, arguments)) {
            tbank::cli::write_stdout(kHelp);
            return tbank::cli::kExitSuccess;
        }

        const Invocation invocation = parse_invocation(argument_count, arguments);
        const std::uint64_t main_stack_bytes = observed_main_stack_bytes();
        tbank::run::RunConfig config = invocation.config.has_value()
            ? tbank::run::load_run_config(*invocation.config, main_stack_bytes)
            : tbank::run::default_run_config(main_stack_bytes);
        tbank::run::validate_run_config(config);
        admit_observed_main_stack(config);
        return run_pipeline(invocation, std::move(config));
    } catch (const tbank::cli::UsageError& error) {
        report_error("usage", error.what());
        return tbank::cli::kExitUsage;
    } catch (const tbank::run::RunConfigError& error) {
        report_error("configuration", error.what());
        return tbank::cli::kExitUsage;
    } catch (const OutputExistsError& error) {
        report_error("publication", error.what());
        return tbank::cli::kExitPublication;
    } catch (const InputChangedError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const StackLimitError& error) {
        report_error("resource", error.what());
        return tbank::cli::kExitResource;
    } catch (const tbank::io::EdgeCsvError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const tbank::platform::UnexpectedEofError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const tbank::storage::GraphValidationError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const tbank::storage::ManifestError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const tbank::storage::BinaryError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const tbank::preprocess::VertexCountLimitError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const tbank::pagerank::PageRankVertexCountLimitError& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (
        const tbank::preprocess::CompactEdgeReductionProgressError& error
    ) {
        return report_reduction_progress(error);
    } catch (const tbank::preprocess::CompactEdgeMemoryLimitError& error) {
        report_error("resource", error.what());
        return tbank::cli::kExitResource;
    } catch (const tbank::preprocess::CompactEdgeDiskSpaceError& error) {
        report_error("resource", error.what());
        return tbank::cli::kExitResource;
    } catch (const tbank::preprocess::PreprocessDiskSpaceError& error) {
        report_error("resource", error.what());
        return tbank::cli::kExitResource;
    } catch (const tbank::preprocess::PreprocessInodeLimitError& error) {
        report_error("resource", error.what());
        return tbank::cli::kExitResource;
    } catch (const tbank::resources::PageRankMemoryLimitError& error) {
        report_error("resource", error.what());
        return tbank::cli::kExitResource;
    } catch (const std::bad_alloc& error) {
        report_error("resource", error.what());
        return tbank::cli::kExitResource;
    } catch (const std::system_error& error) {
        report_error("system", error.what());
        return tbank::cli::kExitSystem;
    } catch (const std::overflow_error& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const std::invalid_argument& error) {
        report_error("data", error.what());
        return tbank::cli::kExitData;
    } catch (const std::logic_error& error) {
        report_error("internal", error.what());
        return tbank::cli::kExitInternal;
    } catch (const std::exception& error) {
        report_error("internal", error.what());
        return tbank::cli::kExitInternal;
    } catch (...) {
        report_error("internal", "unknown non-standard exception");
        return tbank::cli::kExitInternal;
    }
}

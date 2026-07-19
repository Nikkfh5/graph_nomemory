#include "tbank/run/config.hpp"

#include "tbank/run/cli.hpp"

#include "tbank/resources/page_rank.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tbank::run {
namespace {

inline constexpr std::uint64_t kMebibyte = 1024U * 1024U;
inline constexpr std::uint64_t kGibibyte = 1024U * kMebibyte;
inline constexpr std::string_view kSchemaLine =
    "schema=tbank-run-config-v1";
inline constexpr std::uint64_t kStaticCsvByteLimit = 11U;

[[noreturn]] void config_error(const std::string& detail) {
    throw RunConfigError("run config: " + detail);
}

[[noreturn]] void config_posix_error(
    const char* const operation,
    const std::filesystem::path& path,
    const int error_number
) {
    const std::error_code error(
        error_number == 0 ? EIO : error_number,
        std::generic_category()
    );
    config_error(
        std::string(operation) + " " + path.string() + ": " + error.message()
    );
}

class ConfigFile final {
public:
    explicit ConfigFile(const std::filesystem::path& path) : path_(path) {
        do {
            errno = 0;
            descriptor_ = ::open(
                path.c_str(),
                O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK
            );
        } while (descriptor_ == -1 && errno == EINTR);
        if (descriptor_ == -1) {
            config_posix_error("cannot open", path_, errno);
        }
    }

    ConfigFile(const ConfigFile&) = delete;
    ConfigFile& operator=(const ConfigFile&) = delete;

    ~ConfigFile() noexcept {
        if (descriptor_ != -1) {
            static_cast<void>(::close(descriptor_));
        }
    }

    [[nodiscard]] struct stat inspect() const {
        struct stat status {};
        int result = -1;
        do {
            errno = 0;
            result = ::fstat(descriptor_, &status);
        } while (result == -1 && errno == EINTR);
        if (result == -1) {
            config_posix_error("cannot inspect", path_, errno);
        }
        if (!S_ISREG(status.st_mode)) {
            config_error("file must be a regular file: " + path_.string());
        }
        if (status.st_size < 0) {
            config_error("file has a negative size: " + path_.string());
        }
        return status;
    }

    [[nodiscard]] std::vector<char> read_exact(
        const struct stat& before
    ) const {
        const auto byte_count = static_cast<std::uint64_t>(before.st_size);
        if (byte_count > kMaximumRunConfigBytes) {
            config_error(
                "document exceeds "
                + std::to_string(kMaximumRunConfigBytes) + " bytes"
            );
        }
        std::vector<char> bytes(static_cast<std::size_t>(byte_count));
        std::size_t offset = 0U;
        while (offset < bytes.size()) {
            ssize_t result = -1;
            do {
                errno = 0;
                result = ::pread(
                    descriptor_,
                    bytes.data() + offset,
                    bytes.size() - offset,
                    static_cast<off_t>(offset)
                );
            } while (result == -1 && errno == EINTR);
            if (result == -1) {
                config_posix_error("cannot read", path_, errno);
            }
            if (result == 0) {
                config_error("file changed while being read: " + path_.string());
            }
            offset += static_cast<std::size_t>(result);
        }

        char probe = '\0';
        ssize_t probe_result = -1;
        do {
            errno = 0;
            probe_result = ::pread(
                descriptor_,
                &probe,
                1U,
                static_cast<off_t>(bytes.size())
            );
        } while (probe_result == -1 && errno == EINTR);
        if (probe_result == -1) {
            config_posix_error("cannot probe", path_, errno);
        }
        if (probe_result != 0) {
            config_error("file changed while being read: " + path_.string());
        }
        return bytes;
    }

private:
    std::filesystem::path path_;
    int descriptor_ = -1;
};

[[nodiscard]] bool same_snapshot(
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

void require_observed_main_stack(const std::uint64_t bytes) {
    if (bytes == 0U) {
        config_error("observed main stack must be positive");
    }
}

[[nodiscard]] RunConfig make_defaults_unvalidated(
    const std::uint64_t observed_main_stack_bytes
) {
    require_observed_main_stack(observed_main_stack_bytes);

    RunConfig config{};
    config.preprocess.resources.non_bulk_reserve_bytes = 32U * kMebibyte;
    config.preprocess.resources.maximum_input_chunk_bytes = 1U * kMebibyte;
    config.preprocess.resources.phase_fd_budget = 64U;
    config.preprocess.resources.minimum_free_space_reserve_bytes = kGibibyte;
    config.preprocess.counted_graph.tasks = {
        .edge_slice_size = 8'192U,
        .max_task_edges = 262'144U,
        .max_task_vertices = 4'096U,
    };

    auto& resources = config.pagerank.resources;
    resources.record_batch_records = 8'192U;
    resources.validation_io_chunk_bytes = 65'536U;
    resources.main_stack_bytes = observed_main_stack_bytes;
    resources.runtime_reserve_bytes = 32U * kMebibyte;
    resources.worker_count = 64U;
    resources.worker_stack_bytes = 1U * kMebibyte;
    resources.worker_guard_bytes = 4'096U;
    resources.worker_count_batch_records = 8'192U;
    resources.worker_source_batch_records = 8'192U;
    resources.scheduler_window_records = 64U;
    return config;
}

[[nodiscard]] std::uint64_t parse_u64_value(
    const std::string_view key,
    const std::string_view value
) {
    try {
        return cli::parse_u64(key, value);
    } catch (const cli::UsageError& error) {
        config_error(error.what());
    }
}

[[nodiscard]] std::size_t parse_size_value(
    const std::string_view key,
    const std::string_view value
) {
    try {
        return cli::parse_size(key, value);
    } catch (const cli::UsageError& error) {
        config_error(error.what());
    }
}

[[nodiscard]] double parse_double_value(
    const std::string_view key,
    const std::string_view value
) {
    try {
        return cli::parse_finite_double(key, value);
    } catch (const cli::UsageError& error) {
        config_error(error.what());
    }
}

[[nodiscard]] std::uint32_t parse_u32_value(
    const std::string_view key,
    const std::string_view value
) {
    const std::uint64_t parsed = parse_u64_value(key, value);
    if (parsed > std::numeric_limits<std::uint32_t>::max()) {
        config_error(std::string(key) + " is outside uint32 range");
    }
    return static_cast<std::uint32_t>(parsed);
}

void apply_preprocess_override(
    RunConfig& config,
    const std::string_view key,
    const std::string_view value
) {
    auto& preprocess = config.preprocess;
    if (key == "preprocess.non_bulk_reserve_bytes") {
        preprocess.resources.non_bulk_reserve_bytes = parse_u64_value(key, value);
    } else if (key == "preprocess.input_chunk_bytes") {
        preprocess.resources.maximum_input_chunk_bytes = parse_u64_value(key, value);
    } else if (key == "preprocess.phase_fd_budget") {
        preprocess.resources.phase_fd_budget = parse_u64_value(key, value);
    } else if (key == "preprocess.disk_reserve_bytes") {
        preprocess.resources.minimum_free_space_reserve_bytes =
            parse_u64_value(key, value);
    } else if (key == "preprocess.edge_slice_size") {
        preprocess.counted_graph.tasks.edge_slice_size =
            parse_u64_value(key, value);
    } else if (key == "preprocess.max_task_edges") {
        preprocess.counted_graph.tasks.max_task_edges =
            parse_u64_value(key, value);
    } else if (key == "preprocess.max_task_vertices") {
        preprocess.counted_graph.tasks.max_task_vertices =
            parse_u32_value(key, value);
    } else if (key == "preprocess.endpoint_ids_per_run") {
        preprocess.initial_runs.endpoint_ids_per_run =
            parse_size_value(key, value);
    } else if (key == "preprocess.run_writer_buffer_bytes") {
        preprocess.initial_runs.writer_buffer_bytes =
            parse_size_value(key, value);
    } else if (key == "preprocess.vertex_merge_fan_in") {
        preprocess.vertex_ids.fan_in = parse_size_value(key, value);
    } else if (key == "preprocess.vertex_merge_reader_buffer_bytes") {
        preprocess.vertex_ids.reader_buffer_bytes = parse_size_value(key, value);
    } else if (key == "preprocess.vertex_merge_writer_buffer_bytes") {
        preprocess.vertex_ids.writer_buffer_bytes = parse_size_value(key, value);
    } else if (key == "preprocess.vertex_merge_crc_chunk_bytes") {
        preprocess.vertex_ids.crc_chunk_bytes = parse_size_value(key, value);
    } else if (key == "preprocess.raw_edges_per_run") {
        preprocess.compact_edges.raw_edges_per_run = parse_size_value(key, value);
    } else if (key == "preprocess.compact_reader_buffer_bytes") {
        preprocess.compact_edges.reader_buffer_bytes = parse_size_value(key, value);
    } else if (key == "preprocess.compact_writer_buffer_bytes") {
        preprocess.compact_edges.writer_buffer_bytes = parse_size_value(key, value);
    } else if (key == "preprocess.compact_crc_chunk_bytes") {
        preprocess.compact_edges.crc_chunk_bytes = parse_size_value(key, value);
    } else if (key == "preprocess.edge_merge_fan_in") {
        preprocess.counted_graph.merge.fan_in = parse_size_value(key, value);
    } else if (key == "preprocess.edge_merge_reader_buffer_bytes") {
        preprocess.counted_graph.merge.reader_buffer_bytes =
            parse_size_value(key, value);
    } else if (key == "preprocess.edge_merge_writer_buffer_bytes") {
        preprocess.counted_graph.merge.writer_buffer_bytes =
            parse_size_value(key, value);
    } else if (key == "preprocess.edge_merge_crc_chunk_bytes") {
        preprocess.counted_graph.merge.crc_chunk_bytes =
            parse_size_value(key, value);
    } else if (key == "preprocess.edge_batch_records") {
        preprocess.counted_graph.edge_batch_records = parse_size_value(key, value);
    } else if (key == "preprocess.graph_validation_chunk_bytes") {
        preprocess.counted_graph.validation_io_chunk_bytes =
            parse_size_value(key, value);
    } else {
        config_error("unknown key " + std::string(key));
    }
}

void apply_override(
    RunConfig& config,
    const std::string_view key,
    const std::string_view value
) {
    if (key.starts_with("preprocess.")) {
        apply_preprocess_override(config, key, value);
        return;
    }

    auto& resources = config.pagerank.resources;
    if (key == "analyze.record_batch_records") {
        resources.record_batch_records = parse_size_value(key, value);
    } else if (key == "analyze.validation_chunk_bytes") {
        resources.validation_io_chunk_bytes = parse_size_value(key, value);
    } else if (key == "analyze.runtime_reserve_bytes") {
        resources.runtime_reserve_bytes = parse_u64_value(key, value);
    } else if (key == "parallel.worker_count") {
        resources.worker_count = parse_u64_value(key, value);
    } else if (key == "parallel.worker_stack_bytes") {
        resources.worker_stack_bytes = parse_u64_value(key, value);
    } else if (key == "parallel.worker_guard_bytes") {
        resources.worker_guard_bytes = parse_u64_value(key, value);
    } else if (key == "parallel.worker_count_batch_records") {
        resources.worker_count_batch_records = parse_size_value(key, value);
    } else if (key == "parallel.worker_source_batch_records") {
        resources.worker_source_batch_records = parse_size_value(key, value);
    } else if (key == "parallel.scheduler_window_records") {
        resources.scheduler_window_records = parse_size_value(key, value);
    } else if (key == "pagerank.alpha") {
        config.pagerank.alpha = parse_double_value(key, value);
    } else if (key == "pagerank.eta") {
        config.pagerank.eta = parse_double_value(key, value);
    } else if (key == "pagerank.max_iterations") {
        config.pagerank.max_iterations = parse_u64_value(key, value);
    } else {
        config_error("unknown key " + std::string(key));
    }
}

void validate_document_bytes(const std::string_view bytes) {
    if (bytes.empty()) {
        config_error("document is empty");
    }
    if (bytes.size() > kMaximumRunConfigBytes) {
        config_error(
            "document exceeds " + std::to_string(kMaximumRunConfigBytes)
            + " bytes"
        );
    }
    if (bytes.back() != '\n') {
        config_error("document must end with LF");
    }
    for (std::size_t offset = 0U; offset < bytes.size(); ++offset) {
        const auto byte = static_cast<unsigned char>(bytes[offset]);
        if (byte == static_cast<unsigned char>('\n')) {
            continue;
        }
        if (byte < 0x20U || byte > 0x7eU) {
            config_error(
                "document contains a non-printable or non-ASCII byte at byte "
                + std::to_string(offset)
            );
        }
    }
}

}  // namespace

RunConfig default_run_config(const std::uint64_t observed_main_stack_bytes) {
    RunConfig config = make_defaults_unvalidated(observed_main_stack_bytes);
    validate_run_config(config);
    return config;
}

RunConfig parse_run_config(
    const std::string_view bytes,
    const std::uint64_t observed_main_stack_bytes
) {
    validate_document_bytes(bytes);
    RunConfig config = make_defaults_unvalidated(observed_main_stack_bytes);
    std::set<std::string_view> seen_keys;
    bool schema_seen = false;

    std::size_t line_begin = 0U;
    while (line_begin < bytes.size()) {
        const std::size_t line_end = bytes.find('\n', line_begin);
        if (line_end == std::string_view::npos) {
            config_error("internal line framing failure");
        }
        const std::string_view line = bytes.substr(
            line_begin, line_end - line_begin
        );
        line_begin = line_end + 1U;
        if (line.empty() || line.front() == '#') {
            continue;
        }

        if (!schema_seen) {
            if (line != kSchemaLine) {
                config_error(
                    "first semantic line must be " + std::string(kSchemaLine)
                );
            }
            schema_seen = true;
            continue;
        }

        const std::size_t equals = line.find('=');
        if (equals == std::string_view::npos || equals == 0U
            || equals + 1U == line.size()
            || line.find('=', equals + 1U) != std::string_view::npos) {
            config_error("expected one nonempty key=value pair");
        }
        const std::string_view key = line.substr(0U, equals);
        const std::string_view value = line.substr(equals + 1U);
        if (key == "schema") {
            config_error("schema is repeated");
        }
        const auto [iterator, inserted] = seen_keys.insert(key);
        static_cast<void>(iterator);
        if (!inserted) {
            config_error("key is repeated: " + std::string(key));
        }
        apply_override(config, key, value);
    }

    if (!schema_seen) {
        config_error("schema line is missing");
    }
    validate_run_config(config);
    return config;
}

RunConfig load_run_config(
    const std::filesystem::path& path,
    const std::uint64_t observed_main_stack_bytes
) {
    const ConfigFile input(path);
    const struct stat before = input.inspect();
    const std::vector<char> bytes = input.read_exact(before);
    const struct stat after = input.inspect();
    if (!same_snapshot(before, after)) {
        config_error("file changed while being read: " + path.string());
    }
    const std::string_view document = bytes.empty()
        ? std::string_view{}
        : std::string_view(bytes.data(), bytes.size());
    return parse_run_config(
        document, observed_main_stack_bytes
    );
}

void validate_run_config(const RunConfig& config) {
    if (config.pagerank.resources.main_stack_bytes == 0U) {
        config_error("observed main stack must be positive");
    }

    preprocess::CountedGraphPreprocessorConfig preprocess = config.preprocess;
    preprocess.csv_byte_limit = kStaticCsvByteLimit;
    try {
        static_cast<void>(preprocess::preprocess_static_resource_plan(preprocess));
    } catch (const std::exception& error) {
        config_error(
            std::string("invalid preprocessing configuration: ") + error.what()
        );
    }

    try {
        pagerank::validate_page_rank_config(config.pagerank);
        resources::PageRankResourcePolicy configured_resources =
            config.pagerank.resources;
        // main_stack_bytes is observed from the host rather than configured.
        // Validate every config-controlled term with the smallest positive
        // inventory here; the executable performs actual host admission as a
        // resource check before creating any filesystem artifact.
        configured_resources.main_stack_bytes = 1U;
        static_cast<void>(resources::page_rank_memory_plan(
            1U, configured_resources
        ));
    } catch (const std::exception& error) {
        config_error(
            std::string("invalid PageRank configuration: ") + error.what()
        );
    }
}

}  // namespace tbank::run

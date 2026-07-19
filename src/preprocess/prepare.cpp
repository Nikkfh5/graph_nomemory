#include "tbank/preprocess/prepare.hpp"

#include "tbank/platform/checked_io.hpp"
#include "tbank/storage/binary.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

namespace tbank::preprocess {
namespace {

constexpr std::uint64_t kMinimumCanonicalEdgeCsvBytes = 11U;
constexpr std::uint64_t kMinimumCanonicalCsvFixedBytes = 7U;
constexpr std::uint64_t kInitialRunMaxOpenFiles = 2U;

static_assert(
    std::numeric_limits<std::size_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);
static_assert(
    std::numeric_limits<fsblkcnt_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);
static_assert(
    std::numeric_limits<fsfilcnt_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);
static_assert(
    std::is_nothrow_move_constructible_v<CountedGraphPreprocessResult>
);

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) noexcept {
    return static_cast<std::uint64_t>(value);
}

[[nodiscard]] bool contains_embedded_nul(
    const std::filesystem::path& path
) {
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

[[nodiscard]] std::uint64_t ceil_div_positive(
    const std::uint64_t value,
    const std::uint64_t divisor
) {
    if (value == 0U || divisor == 0U) {
        throw std::invalid_argument(
            "positive ceiling division requires nonzero operands"
        );
    }
    return 1U + (value - 1U) / divisor;
}

[[nodiscard]] std::uint64_t max_value(
    const std::initializer_list<std::uint64_t> values
) noexcept {
    return *std::max_element(values.begin(), values.end());
}

[[nodiscard]] const char* phase_name(
    const PreprocessDiskPhase phase
) noexcept {
    switch (phase) {
        case PreprocessDiskPhase::initial_runs:
            return "initial runs";
        case PreprocessDiskPhase::vertex_id_merge:
            return "vertex-ID merge";
    }
    return "unknown preprocessing phase";
}

void validate_top_level_resource_policy(
    const CountedGraphPreprocessorConfig& config
) {
    if (config.compact_edges.managed_bulk_limit_bytes != 0U
        || config.compact_edges.minimum_free_space_reserve_bytes != 0U
        || config.counted_graph.merge.managed_bulk_limit_bytes != 0U
        || config.counted_graph.merge.phase_fd_budget != 0U
        || config.counted_graph.merge.minimum_free_space_reserve_bytes != 0U) {
        throw std::invalid_argument(
            "nested preprocessing resource fields must remain zero"
        );
    }
    if (config.resources.non_bulk_reserve_bytes == 0U
        || config.resources.non_bulk_reserve_bytes
            >= kCompactEdgeHardMemoryBudgetBytes) {
        throw std::invalid_argument(
            "non-bulk reserve must be in (0, 128 MiB)"
        );
    }
    if (config.resources.maximum_input_chunk_bytes == 0U
        || config.resources.maximum_input_chunk_bytes
            >= config.resources.non_bulk_reserve_bytes) {
        throw std::invalid_argument(
            "maximum input chunk must leave a nonzero runtime reserve"
        );
    }
    if (config.resources.phase_fd_budget == 0U) {
        throw std::invalid_argument(
            "preprocessor phase FD budget must be explicitly supplied"
        );
    }
    if (config.csv_byte_limit < kMinimumCanonicalEdgeCsvBytes) {
        throw std::invalid_argument(
            "CSV byte limit is below the shortest valid edge CSV"
        );
    }
}

[[nodiscard]] CountedGraphPreprocessorConfig derive_execution_config(
    CountedGraphPreprocessorConfig config
) {
    validate_top_level_resource_policy(config);
    const std::uint64_t managed_limit = kCompactEdgeHardMemoryBudgetBytes
        - config.resources.non_bulk_reserve_bytes;
    config.compact_edges.managed_bulk_limit_bytes = managed_limit;
    config.compact_edges.minimum_free_space_reserve_bytes =
        config.resources.minimum_free_space_reserve_bytes;
    config.counted_graph.merge.managed_bulk_limit_bytes = managed_limit;
    config.counted_graph.merge.phase_fd_budget =
        config.resources.phase_fd_budget;
    config.counted_graph.merge.minimum_free_space_reserve_bytes =
        config.resources.minimum_free_space_reserve_bytes;
    return config;
}

[[nodiscard]] InitialRunDiskPlan make_initial_run_disk_plan(
    const CountedGraphPreprocessorConfig& config
) {
    const std::uint64_t raw_edge_count =
        (config.csv_byte_limit - kMinimumCanonicalCsvFixedBytes) / 4U;
    const std::uint64_t endpoint_count = platform::checked_multiply(
        raw_edge_count, 2U
    );
    const std::uint64_t endpoint_capacity = size_to_u64(
        config.initial_runs.endpoint_ids_per_run
    );
    const std::uint64_t endpoint_run_count = ceil_div_positive(
        endpoint_count, endpoint_capacity
    );
    const std::uint64_t raw_payload_bytes = platform::checked_multiply(
        raw_edge_count, kRawEdgeRunRecordBytes
    );
    const std::uint64_t endpoint_payload_bytes = platform::checked_multiply(
        endpoint_count, kEndpointIdRunRecordBytes
    );
    const std::uint64_t raw_file_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes, raw_payload_bytes
    );
    const std::uint64_t endpoint_headers = platform::checked_multiply(
        endpoint_run_count, storage::kBinaryHeaderBytes
    );
    const std::uint64_t endpoint_files_bytes = platform::checked_add(
        endpoint_headers, endpoint_payload_bytes
    );
    const std::uint64_t file_count = platform::checked_add(
        endpoint_run_count, 1U
    );
    const std::uint64_t entry_count = platform::checked_add(file_count, 1U);

    static_cast<void>(platform::checked_u64_to_off_t(raw_file_bytes));
    const std::uint64_t maximum_endpoint_run_records = std::min(
        endpoint_count, endpoint_capacity
    );
    const std::uint64_t maximum_endpoint_run_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes,
        platform::checked_multiply(
            maximum_endpoint_run_records, kEndpointIdRunRecordBytes
        )
    );
    static_cast<void>(
        platform::checked_u64_to_off_t(maximum_endpoint_run_bytes)
    );

    return InitialRunDiskPlan{
        .csv_byte_limit = config.csv_byte_limit,
        .raw_edge_count_upper_bound = raw_edge_count,
        .endpoint_run_count_upper_bound = endpoint_run_count,
        .raw_edge_file_bytes = raw_file_bytes,
        .endpoint_run_files_bytes_upper_bound = endpoint_files_bytes,
        .successor_logical_upper_bound_bytes = platform::checked_add(
            raw_file_bytes, endpoint_files_bytes
        ),
        .successor_file_count_upper_bound = file_count,
        .successor_entry_count_upper_bound = entry_count,
    };
}

void validate_execution_resources(
    const CountedGraphPreprocessorConfig& config,
    const PreprocessStaticResourcePlan& plan
) {
    const std::uint64_t managed_limit = plan.managed_bulk_limit_bytes;
    if (plan.managed_bulk_floor_bytes > managed_limit) {
        throw CompactEdgeMemoryLimitError(
            plan.managed_bulk_floor_bytes, managed_limit
        );
    }
    if (config.resources.phase_fd_budget < plan.component_fd_floor) {
        throw std::invalid_argument(
            "preprocessor phase FD budget is below the static floor"
        );
    }
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

void require_absent(
    const std::filesystem::path& path,
    const char* const operation
) {
    struct stat status {};
    int result = -1;
    do {
        errno = 0;
        result = ::lstat(path.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == 0) {
        throw_posix_error(EEXIST, operation);
    }
    if (errno != ENOENT) {
        throw_posix_error(errno, operation);
    }
}

[[nodiscard]] bool path_is_within(
    const std::filesystem::path& candidate,
    const std::filesystem::path& ancestor
) {
    auto candidate_part = candidate.begin();
    auto ancestor_part = ancestor.begin();
    while (ancestor_part != ancestor.end()) {
        if (candidate_part == candidate.end()
            || *candidate_part != *ancestor_part) {
            return false;
        }
        ++candidate_part;
        ++ancestor_part;
    }
    return true;
}

[[nodiscard]] std::filesystem::path resolve_absent_child(
    const std::filesystem::path& path,
    const char* const empty_message,
    const char* const nul_message,
    const char* const parent_operation
) {
    if (path.empty()) {
        throw std::invalid_argument(empty_message);
    }
    if (contains_embedded_nul(path)) {
        throw std::invalid_argument(nul_message);
    }
    const std::filesystem::path absolute =
        std::filesystem::absolute(path).lexically_normal();
    if (absolute.filename().empty()) {
        throw std::invalid_argument(empty_message);
    }
    require_real_directory(absolute.parent_path(), parent_operation);
    const std::filesystem::path canonical_parent =
        std::filesystem::canonical(absolute.parent_path());
    return canonical_parent / absolute.filename();
}

struct ResolvedPaths {
    std::filesystem::path workspace;
    std::filesystem::path target;
};

[[nodiscard]] ResolvedPaths resolve_and_validate_paths(
    const std::filesystem::path& workspace,
    const std::filesystem::path& target
) {
    ResolvedPaths paths{
        .workspace = resolve_absent_child(
            workspace,
            "preprocessor workspace path is empty",
            "preprocessor workspace path contains an embedded NUL",
            "inspect preprocessor workspace parent"
        ),
        .target = resolve_absent_child(
            target,
            "preprocessor target path is empty",
            "preprocessor target path contains an embedded NUL",
            "inspect preprocessor target parent"
        ),
    };
    require_absent(paths.workspace, "require absent preprocessor workspace");
    if (path_is_within(paths.target, paths.workspace)
        || path_is_within(paths.workspace, paths.target)) {
        throw std::invalid_argument(
            "preprocessor workspace and graph target must be disjoint"
        );
    }
    return paths;
}

[[nodiscard]] PreprocessDiskAdmission preflight_disk_space(
    const std::filesystem::path& filesystem_path,
    const PreprocessDiskPhase phase,
    const std::uint64_t logical_successor_bytes,
    const std::uint64_t successor_file_count,
    const std::uint64_t successor_entry_count,
    const std::uint64_t reserve_bytes
) {
    struct statvfs status {};
    int result = -1;
    do {
        errno = 0;
        result = ::statvfs(filesystem_path.c_str(), &status);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        throw_posix_error(errno, "inspect preprocessing filesystem");
    }

    const std::uint64_t fragment_bytes = status.f_frsize != 0U
        ? static_cast<std::uint64_t>(status.f_frsize)
        : static_cast<std::uint64_t>(status.f_bsize);
    if (fragment_bytes == 0U) {
        throw std::runtime_error(
            "preprocessing filesystem reports a zero allocation unit"
        );
    }
    const std::uint64_t available_bytes = platform::checked_multiply(
        static_cast<std::uint64_t>(status.f_bavail), fragment_bytes
    );
    const std::uint64_t rounding_slack = platform::checked_multiply(
        successor_file_count, fragment_bytes - 1U
    );
    const std::uint64_t directory_allowance = platform::checked_multiply(
        successor_entry_count, fragment_bytes
    );
    std::uint64_t required_bytes = platform::checked_add(
        logical_successor_bytes, rounding_slack
    );
    required_bytes = platform::checked_add(
        required_bytes, directory_allowance
    );
    required_bytes = platform::checked_add(required_bytes, reserve_bytes);
    if (required_bytes > available_bytes) {
        throw PreprocessDiskSpaceError(
            phase, required_bytes, available_bytes
        );
    }

    const bool inode_count_reported = status.f_files != 0U;
    const std::uint64_t available_inodes = inode_count_reported
        ? static_cast<std::uint64_t>(status.f_favail)
        : std::numeric_limits<std::uint64_t>::max();
    if (inode_count_reported && successor_entry_count > available_inodes) {
        throw PreprocessInodeLimitError(
            phase, successor_entry_count, available_inodes
        );
    }

    return PreprocessDiskAdmission{
        .phase = phase,
        .logical_successor_bytes = logical_successor_bytes,
        .rounding_slack_bytes = rounding_slack,
        .directory_allowance_bytes = directory_allowance,
        .reserve_bytes = reserve_bytes,
        .required_available_bytes = required_bytes,
        .observed_available_bytes = available_bytes,
        .required_inodes = successor_entry_count,
        .observed_available_inodes = available_inodes,
        .filesystem_fragment_bytes = fragment_bytes,
    };
}

[[nodiscard]] std::error_code retire_workspace_noexcept(
    const std::filesystem::path& workspace
) noexcept {
    int result = -1;
    do {
        errno = 0;
        result = ::rmdir(workspace.c_str());
    } while (result == -1 && errno == EINTR);
    return result == 0
        ? std::error_code{}
        : std::error_code(
              errno == 0 ? EIO : errno,
              std::generic_category()
          );
}

[[nodiscard]] CountedGraphPreprocessResources make_resource_summary(
    const PreprocessStaticResourcePlan& static_plan,
    const VertexIdMergeDiskPlan& vertex_disk,
    const PreprocessDiskAdmission& initial_admission,
    const PreprocessDiskAdmission& vertex_admission,
    const CompactEdgeRunResult& compact_edges,
    const CompactEdgeReduceResult& reduction,
    const CountedGraphResourcePlan& counted_graph
) noexcept {
    return CountedGraphPreprocessResources{
        .static_plan = static_plan,
        .vertex_id_merge_disk = vertex_disk,
        .initial_disk_admission = initial_admission,
        .vertex_id_disk_admission = vertex_admission,
        .managed_bulk_upper_bound_bytes = max_value({
            static_plan.initial_run_memory.managed_peak_bytes,
            static_plan.vertex_id_merge_memory
                .managed_bulk_upper_bound_bytes,
            compact_edges.memory_plan.managed_bulk_upper_bound_bytes,
            reduction.memory_plan.managed_bulk_upper_bound_bytes,
            counted_graph.managed_bulk_upper_bound_bytes,
        }),
        .component_max_open_files = max_value({
            kInitialRunMaxOpenFiles,
            static_plan.vertex_id_merge_memory.max_open_files,
            compact_edges.memory_plan.max_open_files,
            reduction.memory_plan.reduction_max_open_files,
            counted_graph.max_open_files,
        }),
    };
}

}  // namespace

PreprocessStaticResourcePlan preprocess_static_resource_plan(
    const CountedGraphPreprocessorConfig& config
) {
    const CountedGraphPreprocessorConfig execution_config =
        derive_execution_config(config);
    const InitialRunMemoryPlan initial_memory = initial_run_memory_plan(
        execution_config.initial_runs
    );
    const VertexIdMergeMemoryPlan vertex_memory =
        vertex_id_merge_memory_plan(execution_config.vertex_ids);
    const CompactEdgeRunMemoryPlan compact_minimum =
        compact_edge_run_memory_plan(1U, 1U, execution_config.compact_edges);
    const CompactEdgeMergeMemoryPlan merge_memory =
        compact_edge_merge_memory_plan(execution_config.counted_graph.merge);
    const std::uint64_t merge_fan_in = size_to_u64(
        execution_config.counted_graph.merge.fan_in
    );
    const CountedGraphResourcePlan counted_floor =
        counted_graph_resource_plan(
            CompactEdgeMergeFrontier{
                .generation_index = kCompactEdgeInitialGeneration,
                .run_count = merge_fan_in,
                .record_count = merge_fan_in,
                .initial_record_count = merge_fan_in,
                .vertex_count = 1U,
            },
            execution_config.counted_graph
        );
    return PreprocessStaticResourcePlan{
        .hard_memory_budget_bytes = kCompactEdgeHardMemoryBudgetBytes,
        .non_bulk_reserve_bytes = config.resources.non_bulk_reserve_bytes,
        .maximum_input_chunk_bytes =
            config.resources.maximum_input_chunk_bytes,
        .runtime_reserve_after_input_chunk_bytes =
            config.resources.non_bulk_reserve_bytes
            - config.resources.maximum_input_chunk_bytes,
        .managed_bulk_limit_bytes = kCompactEdgeHardMemoryBudgetBytes
            - config.resources.non_bulk_reserve_bytes,
        .initial_run_memory = initial_memory,
        .vertex_id_merge_memory = vertex_memory,
        .minimum_compact_edge_memory = compact_minimum,
        .compact_edge_merge_memory = merge_memory,
        .full_fan_in_counted_graph_floor = counted_floor,
        .initial_run_disk = make_initial_run_disk_plan(config),
        .managed_bulk_floor_bytes = max_value({
            initial_memory.managed_peak_bytes,
            vertex_memory.managed_bulk_upper_bound_bytes,
            compact_minimum.managed_bulk_upper_bound_bytes,
            merge_memory.managed_bulk_upper_bound_bytes,
            counted_floor.managed_bulk_upper_bound_bytes,
        }),
        .component_fd_floor = max_value({
            kInitialRunMaxOpenFiles,
            vertex_memory.max_open_files,
            compact_minimum.max_open_files,
            merge_memory.reduction_max_open_files,
            counted_floor.max_open_files,
        }),
    };
}

VertexIdMergeDiskPlan vertex_id_merge_disk_plan(
    const InitialRunSummary& initial_runs,
    const VertexIdMergeConfig& config
) {
    static_cast<void>(vertex_id_merge_memory_plan(config));
    if (initial_runs.raw_edge_count == 0U
        || initial_runs.endpoint_run_count == 0U
        || initial_runs.endpoint_run_records == 0U) {
        throw std::invalid_argument(
            "vertex-ID disk plan requires nonempty initial runs"
        );
    }
    const std::uint64_t expected_endpoints = platform::checked_multiply(
        initial_runs.raw_edge_count, 2U
    );
    if (initial_runs.endpoint_ids_seen != expected_endpoints
        || initial_runs.endpoint_run_records > expected_endpoints
        || initial_runs.endpoint_run_count
            > initial_runs.endpoint_run_records) {
        throw std::invalid_argument(
            "vertex-ID disk plan received inconsistent ingest counts"
        );
    }

    const std::uint64_t fan_in = size_to_u64(config.fan_in);
    const bool creates_generation = initial_runs.endpoint_run_count > fan_in;
    const std::uint64_t successor_run_count = creates_generation
        ? ceil_div_positive(initial_runs.endpoint_run_count, fan_in)
        : 1U;
    const std::uint64_t raw_file_bytes = platform::checked_add(
        storage::kBinaryHeaderBytes,
        platform::checked_multiply(
            initial_runs.raw_edge_count, kRawEdgeRunRecordBytes
        )
    );
    const std::uint64_t predecessor_bytes = platform::checked_add(
        platform::checked_multiply(
            initial_runs.endpoint_run_count, storage::kBinaryHeaderBytes
        ),
        platform::checked_multiply(
            initial_runs.endpoint_run_records, kEndpointIdRunRecordBytes
        )
    );
    const std::uint64_t successor_bytes = platform::checked_add(
        platform::checked_multiply(
            successor_run_count, storage::kBinaryHeaderBytes
        ),
        platform::checked_multiply(
            initial_runs.endpoint_run_records, kEndpointIdRunRecordBytes
        )
    );
    const std::uint64_t successor_entry_count = platform::checked_add(
        successor_run_count, creates_generation ? 1U : 0U
    );
    static_cast<void>(platform::checked_u64_to_off_t(
        platform::checked_add(
            storage::kBinaryHeaderBytes,
            platform::checked_multiply(
                initial_runs.endpoint_run_records,
                kEndpointIdRunRecordBytes
            )
        )
    ));

    return VertexIdMergeDiskPlan{
        .raw_edge_count = initial_runs.raw_edge_count,
        .input_run_count = initial_runs.endpoint_run_count,
        .input_record_count = initial_runs.endpoint_run_records,
        .successor_run_count_upper_bound = successor_run_count,
        .raw_edge_file_bytes = raw_file_bytes,
        .predecessor_run_files_bytes = predecessor_bytes,
        .successor_logical_upper_bound_bytes = successor_bytes,
        .logical_peak_upper_bound_bytes = platform::checked_add(
            platform::checked_add(raw_file_bytes, predecessor_bytes),
            successor_bytes
        ),
        .successor_file_count_upper_bound = successor_run_count,
        .successor_entry_count_upper_bound = successor_entry_count,
    };
}

PreprocessInputLimitError::PreprocessInputLimitError(
    const std::uint64_t attempted_bytes,
    const std::uint64_t limit_bytes
)
    : std::runtime_error(
          "preprocessor input would consume " + std::to_string(attempted_bytes)
          + " CSV bytes but limit is " + std::to_string(limit_bytes)
      ),
      attempted_bytes_(attempted_bytes),
      limit_bytes_(limit_bytes) {}

std::uint64_t PreprocessInputLimitError::attempted_bytes() const noexcept {
    return attempted_bytes_;
}

std::uint64_t PreprocessInputLimitError::limit_bytes() const noexcept {
    return limit_bytes_;
}

PreprocessInputChunkLimitError::PreprocessInputChunkLimitError(
    const std::uint64_t chunk_bytes,
    const std::uint64_t limit_bytes
)
    : std::runtime_error(
          "preprocessor input chunk contains " + std::to_string(chunk_bytes)
          + " bytes but limit is " + std::to_string(limit_bytes)
      ),
      chunk_bytes_(chunk_bytes),
      limit_bytes_(limit_bytes) {}

std::uint64_t PreprocessInputChunkLimitError::chunk_bytes() const noexcept {
    return chunk_bytes_;
}

std::uint64_t PreprocessInputChunkLimitError::limit_bytes() const noexcept {
    return limit_bytes_;
}

PreprocessDiskSpaceError::PreprocessDiskSpaceError(
    const PreprocessDiskPhase phase,
    const std::uint64_t required_bytes,
    const std::uint64_t available_bytes
)
    : std::runtime_error(
          std::string("preprocessor ") + phase_name(phase) + " requires "
          + std::to_string(required_bytes) + " available bytes but found "
          + std::to_string(available_bytes)
      ),
      phase_(phase),
      required_bytes_(required_bytes),
      available_bytes_(available_bytes) {}

PreprocessDiskPhase PreprocessDiskSpaceError::phase() const noexcept {
    return phase_;
}

std::uint64_t PreprocessDiskSpaceError::required_bytes() const noexcept {
    return required_bytes_;
}

std::uint64_t PreprocessDiskSpaceError::available_bytes() const noexcept {
    return available_bytes_;
}

PreprocessInodeLimitError::PreprocessInodeLimitError(
    const PreprocessDiskPhase phase,
    const std::uint64_t required_inodes,
    const std::uint64_t available_inodes
)
    : std::runtime_error(
          std::string("preprocessor ") + phase_name(phase) + " requires "
          + std::to_string(required_inodes) + " free inodes but found "
          + std::to_string(available_inodes)
      ),
      phase_(phase),
      required_inodes_(required_inodes),
      available_inodes_(available_inodes) {}

PreprocessDiskPhase PreprocessInodeLimitError::phase() const noexcept {
    return phase_;
}

std::uint64_t PreprocessInodeLimitError::required_inodes() const noexcept {
    return required_inodes_;
}

std::uint64_t PreprocessInodeLimitError::available_inodes() const noexcept {
    return available_inodes_;
}

class CountedGraphPreprocessor::Impl final {
public:
    Impl(
        ResolvedPaths paths,
        CountedGraphPreprocessorConfig config,
        PreprocessStaticResourcePlan static_plan,
        PreprocessDiskAdmission initial_disk_admission
    )
        : paths_(std::move(paths)),
          config_(std::move(config)),
          static_plan_(std::move(static_plan)),
          initial_disk_admission_(initial_disk_admission),
          ingestor_(std::make_unique<InitialRunIngestor>(
              InitialRunIngestor::create(
                  paths_.workspace, config_.initial_runs
              )
          )) {}

    void consume(const std::span<const char> csv_bytes) {
        ensure_active();
        try {
            const std::uint64_t chunk_bytes = size_to_u64(csv_bytes.size());
            if (chunk_bytes > config_.resources.maximum_input_chunk_bytes) {
                throw PreprocessInputChunkLimitError(
                    chunk_bytes,
                    config_.resources.maximum_input_chunk_bytes
                );
            }
            const std::uint64_t attempted = platform::checked_add(
                consumed_csv_bytes_, chunk_bytes
            );
            if (attempted > config_.csv_byte_limit) {
                throw PreprocessInputLimitError(
                    attempted, config_.csv_byte_limit
                );
            }
            ingestor_->consume(csv_bytes);
            consumed_csv_bytes_ = attempted;
        } catch (...) {
            fail(std::current_exception());
            throw;
        }
    }

    [[nodiscard]] CountedGraphPreprocessResult finish(
        platform::PublicationBackend* const backend
    ) {
        ensure_active();
        try {
            InitialRunResult initial_runs = ingestor_->finish();
            ingestor_.reset();

            const VertexIdMergeDiskPlan vertex_disk =
                vertex_id_merge_disk_plan(
                    initial_runs.summary, config_.vertex_ids
                );
            const PreprocessDiskAdmission vertex_admission =
                preflight_disk_space(
                    paths_.workspace,
                    PreprocessDiskPhase::vertex_id_merge,
                    vertex_disk.successor_logical_upper_bound_bytes,
                    vertex_disk.successor_file_count_upper_bound,
                    vertex_disk.successor_entry_count_upper_bound,
                    config_.resources.minimum_free_space_reserve_bytes
                );
            VertexIdMergeResult vertex_ids = merge_endpoint_id_runs(
                paths_.workspace,
                initial_runs.summary.endpoint_run_count,
                config_.vertex_ids
            );
            CompactEdgeRunResult compact_edges = build_compact_edge_runs(
                paths_.workspace,
                initial_runs.summary.raw_edge_count,
                vertex_ids.vertex_count,
                config_.compact_edges
            );
            CompactEdgeReduceResult reduction = reduce_compact_edge_runs(
                paths_.workspace,
                compact_edges.summary.run_count,
                compact_edges.summary.locally_unique_edge_count,
                compact_edges.summary.vertex_count,
                config_.counted_graph.merge
            );
            const CountedGraphResourcePlan counted_resources =
                counted_graph_resource_plan(
                    reduction.frontier, config_.counted_graph
                );
            CountedGraphPreprocessResources resources = make_resource_summary(
                static_plan_,
                vertex_disk,
                initial_disk_admission_,
                vertex_admission,
                compact_edges,
                reduction,
                counted_resources
            );
            CountedGraphBuildResult graph = backend == nullptr
                ? build_and_publish_counted_graph(
                      paths_.workspace,
                      reduction.frontier,
                      paths_.target,
                      config_.counted_graph
                  )
                : build_and_publish_counted_graph(
                      paths_.workspace,
                      reduction.frontier,
                      paths_.target,
                      config_.counted_graph,
                      *backend
                  );

            CountedGraphPreprocessResult result{
                .consumed_csv_bytes = consumed_csv_bytes_,
                .initial_runs = std::move(initial_runs),
                .vertex_ids = std::move(vertex_ids),
                .compact_edges = std::move(compact_edges),
                .reduction = std::move(reduction),
                .graph = std::move(graph),
                .resources = std::move(resources),
                .workspace_retirement =
                    WorkspaceRetirementState::not_attempted,
                .workspace_retirement_error = {},
            };
            if (result.graph.publication.state
                    == platform::PublicationState::published
                && result.graph.frontier_retirement
                    == PredecessorRetirementState::retired
                && result.graph.vertex_ids_retirement
                    == PredecessorRetirementState::retired) {
                result.workspace_retirement_error =
                    retire_workspace_noexcept(paths_.workspace);
                result.workspace_retirement =
                    result.workspace_retirement_error
                    ? WorkspaceRetirementState::failed
                    : WorkspaceRetirementState::retired;
            }
            state_ = State::finished;
            return result;
        } catch (...) {
            fail(std::current_exception());
            throw;
        }
    }

private:
    enum class State {
        active,
        failed,
        finished,
    };

    void ensure_active() const {
        if (state_ == State::failed && failure_) {
            std::rethrow_exception(failure_);
        }
        if (state_ == State::finished) {
            throw std::logic_error("preprocessor is already finished");
        }
        if (state_ != State::active || ingestor_ == nullptr) {
            throw std::logic_error("preprocessor has invalid active state");
        }
    }

    void fail(std::exception_ptr failure) noexcept {
        if (state_ != State::failed) {
            failure_ = std::move(failure);
            state_ = State::failed;
        }
        ingestor_.reset();
    }

    ResolvedPaths paths_;
    CountedGraphPreprocessorConfig config_{};
    PreprocessStaticResourcePlan static_plan_{};
    PreprocessDiskAdmission initial_disk_admission_{};
    std::unique_ptr<InitialRunIngestor> ingestor_{};
    std::uint64_t consumed_csv_bytes_ = 0U;
    State state_ = State::active;
    std::exception_ptr failure_{};
};

CountedGraphPreprocessor CountedGraphPreprocessor::create(
    const std::filesystem::path& workspace,
    const std::filesystem::path& target,
    CountedGraphPreprocessorConfig config
) {
    const PreprocessStaticResourcePlan static_plan =
        preprocess_static_resource_plan(config);
    CountedGraphPreprocessorConfig execution_config =
        derive_execution_config(std::move(config));
    validate_execution_resources(execution_config, static_plan);
    ResolvedPaths paths = resolve_and_validate_paths(workspace, target);
    const PreprocessDiskAdmission initial_admission = preflight_disk_space(
        paths.workspace.parent_path(),
        PreprocessDiskPhase::initial_runs,
        static_plan.initial_run_disk.successor_logical_upper_bound_bytes,
        static_plan.initial_run_disk.successor_file_count_upper_bound,
        static_plan.initial_run_disk.successor_entry_count_upper_bound,
        execution_config.resources.minimum_free_space_reserve_bytes
    );
    return CountedGraphPreprocessor(std::make_unique<Impl>(
        std::move(paths),
        std::move(execution_config),
        static_plan,
        initial_admission
    ));
}

CountedGraphPreprocessor::CountedGraphPreprocessor(
    std::unique_ptr<Impl> impl
) noexcept
    : impl_(std::move(impl)) {}

CountedGraphPreprocessor::CountedGraphPreprocessor(
    CountedGraphPreprocessor&&
) noexcept = default;

CountedGraphPreprocessor& CountedGraphPreprocessor::operator=(
    CountedGraphPreprocessor&&
) noexcept = default;

CountedGraphPreprocessor::~CountedGraphPreprocessor() = default;

void CountedGraphPreprocessor::consume(
    const std::span<const char> csv_bytes
) {
    if (impl_ == nullptr) {
        throw std::logic_error("preprocessor was moved from");
    }
    impl_->consume(csv_bytes);
}

CountedGraphPreprocessResult CountedGraphPreprocessor::finish() {
    if (impl_ == nullptr) {
        throw std::logic_error("preprocessor was moved from");
    }
    return impl_->finish(nullptr);
}

CountedGraphPreprocessResult CountedGraphPreprocessor::finish(
    platform::PublicationBackend& backend
) {
    if (impl_ == nullptr) {
        throw std::logic_error("preprocessor was moved from");
    }
    return impl_->finish(&backend);
}

}  // namespace tbank::preprocess

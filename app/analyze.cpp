#include "cli_common.hpp"

#include "tbank/pagerank/analyze.hpp"
#include "tbank/pagerank/config.hpp"
#include "tbank/pagerank/pagerank.hpp"
#include "tbank/platform/publication.hpp"
#include "tbank/resources/page_rank.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/graph.hpp"
#include "tbank/storage/manifest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <locale>
#include <new>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include <cerrno>
#include <pthread.h>
#include <sys/resource.h>
#include <unistd.h>

namespace {

using tbank::cli::ParsedOptions;
using tbank::cli::UsageError;
using tbank::pagerank::AnalyzeResult;
using tbank::pagerank::AnalyzeStatus;
using tbank::pagerank::PageRankConfig;
using tbank::pagerank::PageRankNumericalFailure;
using tbank::pagerank::PageRankStatus;
using tbank::platform::PublicationState;
using tbank::platform::PublicationStep;
using tbank::resources::PageRankMemoryPlan;

constexpr std::array<std::string_view, 15U> kAllowedOptions{
    "graph-dir",
    "output-csv",
    "worker-count",
    "record-batch-records",
    "validation-chunk-bytes",
    "main-stack-bytes",
    "runtime-reserve-bytes",
    "worker-stack-bytes",
    "worker-guard-bytes",
    "worker-count-batch-records",
    "worker-source-batch-records",
    "scheduler-window-records",
    "alpha",
    "eta",
    "max-iterations",
};

constexpr std::array<std::string_view, 5U> kParallelOptions{
    "worker-stack-bytes",
    "worker-guard-bytes",
    "worker-count-batch-records",
    "worker-source-batch-records",
    "scheduler-window-records",
};

constexpr std::string_view kHelp =
    "Usage: tbank-analyze [options]\n"
    "\n"
    "Required:\n"
    "  --graph-dir PATH\n"
    "  --output-csv PATH\n"
    "  --worker-count UINT\n"
    "  --record-batch-records UINT\n"
    "  --validation-chunk-bytes UINT\n"
    "  --main-stack-bytes UINT\n"
    "  --runtime-reserve-bytes UINT\n"
    "\n"
    "Required when --worker-count is positive; forbidden when it is 0:\n"
    "  --worker-stack-bytes UINT\n"
    "  --worker-guard-bytes UINT\n"
    "  --worker-count-batch-records UINT\n"
    "  --worker-source-batch-records UINT\n"
    "  --scheduler-window-records UINT\n"
    "\n"
    "Optional numerical overrides outside the frozen default profile:\n"
    "  --alpha FLOAT\n"
    "  --eta FLOAT\n"
    "  --max-iterations UINT\n"
    "\n"
    "All UINT values use canonical unsigned decimal spelling. worker-count=0\n"
    "uses the sequential engine; N>=1 creates exactly N worker threads. The\n"
    "launcher must install the exact soft RLIMIT_STACK before exec; the CLI\n"
    "re-reads it and fails closed before graph validation.\n";

[[nodiscard]] std::string_view analyze_status_name(
    const AnalyzeStatus status
) noexcept {
    switch (status) {
        case AnalyzeStatus::published:
            return "published";
        case AnalyzeStatus::numerical_failure:
            return "numerical_failure";
        case AnalyzeStatus::non_converged:
            return "non_converged";
        case AnalyzeStatus::publication_failed:
            return "publication_failed";
        case AnalyzeStatus::durability_uncertain:
            return "durability_uncertain";
    }
    return "unknown";
}

[[nodiscard]] std::string_view pagerank_status_name(
    const PageRankStatus status
) noexcept {
    switch (status) {
        case PageRankStatus::converged:
            return "converged";
        case PageRankStatus::numerical_failure:
            return "numerical_failure";
        case PageRankStatus::non_converged:
            return "non_converged";
    }
    return "unknown";
}

[[nodiscard]] std::string_view numerical_failure_name(
    const PageRankNumericalFailure failure
) noexcept {
    switch (failure) {
        case PageRankNumericalFailure::none:
            return "none";
        case PageRankNumericalFailure::invalid_current_rank:
            return "invalid_current_rank";
        case PageRankNumericalFailure::invalid_transformed_rank:
            return "invalid_transformed_rank";
        case PageRankNumericalFailure::invalid_dangling_mass:
            return "invalid_dangling_mass";
        case PageRankNumericalFailure::invalid_candidate_rank:
            return "invalid_candidate_rank";
        case PageRankNumericalFailure::invalid_raw_mass:
            return "invalid_raw_mass";
        case PageRankNumericalFailure::mass_tolerance_exceeded:
            return "mass_tolerance_exceeded";
        case PageRankNumericalFailure::invalid_normalized_rank:
            return "invalid_normalized_rank";
        case PageRankNumericalFailure::invalid_delta:
            return "invalid_delta";
        case PageRankNumericalFailure::invalid_residual:
            return "invalid_residual";
    }
    return "unknown";
}

[[nodiscard]] std::string_view concurrency_status_name(
    const tbank::parallel::FixedExecutorConcurrencyStatus status
) noexcept {
    using Status = tbank::parallel::FixedExecutorConcurrencyStatus;
    switch (status) {
        case Status::valid:
            return "valid";
        case Status::invalid:
            return "invalid";
        case Status::not_applicable:
            return "not_applicable";
    }
    return "unknown";
}

[[nodiscard]] std::string_view concurrency_failure_name(
    const tbank::parallel::FixedExecutorConcurrencyFailure failure
) noexcept {
    using Failure = tbank::parallel::FixedExecutorConcurrencyFailure;
    switch (failure) {
        case Failure::none:
            return "none";
        case Failure::wall_clock_unavailable:
            return "wall_clock_unavailable";
        case Failure::worker_clock_id_unavailable:
            return "worker_clock_id_unavailable";
        case Failure::worker_cpu_clock_unavailable:
            return "worker_cpu_clock_unavailable";
        case Failure::clock_resolution_unsupported:
            return "clock_resolution_unsupported";
        case Failure::clock_regression:
            return "clock_regression";
        case Failure::clock_model_violation:
            return "clock_model_violation";
        case Failure::counter_overflow:
            return "counter_overflow";
        case Failure::snapshot_incomplete:
            return "snapshot_incomplete";
    }
    return "unknown";
}

[[nodiscard]] std::string_view publication_state_name(
    const PublicationState state
) noexcept {
    switch (state) {
        case PublicationState::published:
            return "published";
        case PublicationState::not_published:
            return "not_published";
        case PublicationState::target_exists:
            return "target_exists";
        case PublicationState::no_replace_unsupported:
            return "no_replace_unsupported";
        case PublicationState::durability_uncertain:
            return "durability_uncertain";
    }
    return "unknown";
}

[[nodiscard]] std::string_view publication_step_name(
    const PublicationStep step
) noexcept {
    switch (step) {
        case PublicationStep::none:
            return "none";
        case PublicationStep::validate_paths:
            return "validate_paths";
        case PublicationStep::open_parent_directory:
            return "open_parent_directory";
        case PublicationStep::create_temporary_file:
            return "create_temporary_file";
        case PublicationStep::write_temporary_file:
            return "write_temporary_file";
        case PublicationStep::sync_temporary_file:
            return "sync_temporary_file";
        case PublicationStep::close_temporary_file:
            return "close_temporary_file";
        case PublicationStep::open_staging_directory:
            return "open_staging_directory";
        case PublicationStep::sync_staging_directory:
            return "sync_staging_directory";
        case PublicationStep::close_staging_directory:
            return "close_staging_directory";
        case PublicationStep::rename_no_replace:
            return "rename_no_replace";
        case PublicationStep::sync_parent_directory:
            return "sync_parent_directory";
    }
    return "unknown";
}

[[nodiscard]] std::string optional_hex(
    const std::optional<double>& value
) {
    return value.has_value() ? tbank::cli::json_hex_double(*value) : "null";
}

[[nodiscard]] std::uint64_t require_positive(
    const ParsedOptions& options,
    const std::string_view name
) {
    const std::uint64_t value = tbank::cli::parse_u64(
        name, options.require(name)
    );
    if (value == 0U) {
        throw UsageError(
            "option --" + std::string(name) + ": value must be positive"
        );
    }
    return value;
}

[[nodiscard]] std::filesystem::path path_option(
    const ParsedOptions& options,
    const std::string_view name
) {
    const std::string_view value = options.require(name);
    tbank::cli::require_valid_utf8(name, value);
    return std::filesystem::path(std::string(value));
}

[[nodiscard]] std::size_t require_positive_size(
    const ParsedOptions& options,
    const std::string_view name
) {
    const std::size_t value = tbank::cli::parse_size(
        name, options.require(name)
    );
    if (value == 0U) {
        throw UsageError(
            "option --" + std::string(name) + ": value must be positive"
        );
    }
    return value;
}

struct ResolvedConfig {
    PageRankConfig pagerank{};
    std::uint64_t observed_main_stack_bytes = 0U;
};

class StackLimitMismatchError final : public std::runtime_error {
public:
    StackLimitMismatchError(
        const std::uint64_t declared_bytes,
        const std::uint64_t observed_bytes
    )
        : std::runtime_error(
              "declared main stack is " + std::to_string(declared_bytes)
              + " bytes but inherited soft RLIMIT_STACK is "
              + std::to_string(observed_bytes) + " bytes"
          ),
          declared_bytes_(declared_bytes),
          observed_bytes_(observed_bytes) {}

    [[nodiscard]] std::uint64_t declared_bytes() const noexcept {
        return declared_bytes_;
    }

    [[nodiscard]] std::uint64_t observed_bytes() const noexcept {
        return observed_bytes_;
    }

private:
    std::uint64_t declared_bytes_;
    std::uint64_t observed_bytes_;
};

[[nodiscard]] ResolvedConfig make_config(const ParsedOptions& options) {
    ResolvedConfig resolved{};
    PageRankConfig& config = resolved.pagerank;

    config.resources.worker_count = tbank::cli::parse_u64(
        "worker-count", options.require("worker-count")
    );
    config.resources.record_batch_records = require_positive_size(
        options, "record-batch-records"
    );
    config.resources.validation_io_chunk_bytes = require_positive_size(
        options, "validation-chunk-bytes"
    );
    config.resources.main_stack_bytes = require_positive(
        options, "main-stack-bytes"
    );
    config.resources.runtime_reserve_bytes = require_positive(
        options, "runtime-reserve-bytes"
    );

    if (const auto value = options.optional("alpha"); value.has_value()) {
        config.alpha = tbank::cli::parse_finite_double("alpha", *value);
    }
    if (const auto value = options.optional("eta"); value.has_value()) {
        config.eta = tbank::cli::parse_finite_double("eta", *value);
    }
    if (const auto value = options.optional("max-iterations");
        value.has_value()) {
        config.max_iterations = tbank::cli::parse_u64(
            "max-iterations", *value
        );
    }

    if (config.resources.worker_count == 0U) {
        for (const std::string_view name : kParallelOptions) {
            if (options.contains(name)) {
                throw UsageError(
                    "option --" + std::string(name)
                    + " is forbidden when --worker-count is 0"
                );
            }
        }
    } else {
        config.resources.worker_stack_bytes = require_positive(
            options, "worker-stack-bytes"
        );
        config.resources.worker_guard_bytes = require_positive(
            options, "worker-guard-bytes"
        );
        config.resources.worker_count_batch_records = require_positive_size(
            options, "worker-count-batch-records"
        );
        config.resources.worker_source_batch_records = require_positive_size(
            options, "worker-source-batch-records"
        );
        config.resources.scheduler_window_records = require_positive_size(
            options, "scheduler-window-records"
        );
    }

    try {
        tbank::pagerank::validate_page_rank_config(config);
    } catch (const std::invalid_argument& error) {
        throw UsageError(
            std::string("invalid analyze configuration: ") + error.what()
        );
    }
    try {
        static_cast<void>(tbank::resources::page_rank_memory_plan(
            1U, config.resources
        ));
    } catch (const std::invalid_argument& error) {
        throw UsageError(
            std::string("invalid analyze configuration: ") + error.what()
        );
    } catch (const std::overflow_error& error) {
        throw UsageError(
            std::string("invalid analyze configuration: ") + error.what()
        );
    }
    return resolved;
}

[[nodiscard]] std::uint64_t verify_main_stack_limit(
    const std::uint64_t requested_bytes
) {
    errno = 0;
    const long page_size_result = ::sysconf(_SC_PAGESIZE);
    if (page_size_result <= 0L) {
        throw std::system_error(
            errno == 0 ? EIO : errno,
            std::generic_category(),
            "read host page size for main stack"
        );
    }
    const auto page_size = static_cast<std::uint64_t>(page_size_result);
    if (requested_bytes < static_cast<std::uint64_t>(PTHREAD_STACK_MIN)) {
        throw UsageError(
            "option --main-stack-bytes: value is below PTHREAD_STACK_MIN"
        );
    }
    if (requested_bytes % page_size != 0U) {
        throw UsageError(
            "option --main-stack-bytes: value must be host-page-aligned"
        );
    }
    if constexpr (sizeof(rlim_t) < sizeof(std::uint64_t)) {
        if (requested_bytes > std::numeric_limits<rlim_t>::max()) {
            throw UsageError(
                "option --main-stack-bytes: value is outside rlim_t range"
            );
        }
    }
    const rlim_t requested = static_cast<rlim_t>(requested_bytes);
    if (requested == RLIM_INFINITY) {
        throw UsageError(
            "option --main-stack-bytes: an infinite stack is forbidden"
        );
    }

    struct rlimit limit {};
    errno = 0;
    if (::getrlimit(RLIMIT_STACK, &limit) == -1) {
        throw std::system_error(
            errno == 0 ? EIO : errno,
            std::generic_category(),
            "read main stack limit"
        );
    }
    if (limit.rlim_cur == RLIM_INFINITY) {
        throw StackLimitMismatchError(
            requested_bytes,
            std::numeric_limits<std::uint64_t>::max()
        );
    }
    const auto observed = static_cast<std::uint64_t>(limit.rlim_cur);
    if (observed != requested_bytes) {
        throw StackLimitMismatchError(requested_bytes, observed);
    }
    return observed;
}

void write_memory_plan(std::ostream& output, const PageRankMemoryPlan& plan) {
    output
        << "{\"vertex_count\":" << plan.vertex_count
        << ",\"current_rank_bytes\":" << plan.current_rank_bytes
        << ",\"scratch_rank_bytes\":" << plan.scratch_rank_bytes
        << ",\"out_degree_bytes\":" << plan.out_degree_bytes
        << ",\"hot_vertex_payload_bytes\":"
        << plan.hot_vertex_payload_bytes
        << ",\"validation_io_peak_bytes\":"
        << plan.validation_io_peak_bytes
        << ",\"iteration_io_peak_bytes\":"
        << plan.iteration_io_peak_bytes
        << ",\"single_thread_io_peak_bytes\":"
        << plan.single_thread_io_peak_bytes
        << ",\"worker_stack_bytes\":" << plan.worker_stack_bytes
        << ",\"worker_guard_bytes\":" << plan.worker_guard_bytes
        << ",\"worker_control_bytes\":" << plan.worker_control_bytes
        << ",\"worker_count_buffer_bytes\":"
        << plan.worker_count_buffer_bytes
        << ",\"worker_source_buffer_bytes\":"
        << plan.worker_source_buffer_bytes
        << ",\"worker_io_buffer_bytes\":" << plan.worker_io_buffer_bytes
        << ",\"worker_memory_bytes\":" << plan.worker_memory_bytes
        << ",\"scheduler_window_bytes\":"
        << plan.scheduler_window_bytes
        << ",\"active_partials_bytes\":" << plan.active_partials_bytes
        << ",\"parallel_iteration_peak_bytes\":"
        << plan.parallel_iteration_peak_bytes
        << ",\"managed_phase_peak_bytes\":"
        << plan.managed_phase_peak_bytes
        << ",\"main_stack_bytes\":" << plan.main_stack_bytes
        << ",\"runtime_reserve_bytes\":" << plan.runtime_reserve_bytes
        << ",\"required_bytes\":" << plan.required_bytes
        << ",\"hard_memory_budget_bytes\":"
        << plan.hard_memory_budget_bytes << '}';
}

void write_internal_concurrency(
    std::ostream& output,
    const tbank::parallel::FixedExecutorConcurrencyEvidence& evidence
) {
    output
        << "{\"schema\":\"tbank-internal-concurrency-v1\""
        << ",\"status\":"
        << tbank::cli::json_quote(concurrency_status_name(evidence.status))
        << ",\"failure\":"
        << tbank::cli::json_quote(concurrency_failure_name(evidence.failure))
        << ",\"measurement_scope\":\"fixed-executor-worker-waves\""
        << ",\"wall_clock\":\"CLOCK_MONOTONIC\""
        << ",\"worker_cpu_clock\":"
           "\"pthread_getcpuclockid+clock_gettime\""
        << ",\"clock_model\":"
           "\"linux-thread-cpu-bounded-resolution-v1\""
        << ",\"wall_clock_resolution_ns\":"
        << evidence.wall_clock_resolution_ns
        << ",\"worker_cpu_clock_resolution_ns\":"
        << evidence.worker_cpu_clock_resolution_ns
        << ",\"configured_worker_count\":"
        << evidence.configured_worker_count
        << ",\"wave_count\":" << evidence.wave_count
        << ",\"claimed_job_count\":" << evidence.claimed_job_count
        << ",\"participating_worker_count\":"
        << evidence.participating_worker_count
        << ",\"positive_worker_cpu_count\":"
        << evidence.positive_worker_cpu_count
        << ",\"accounted_worker_cpu_ns\":"
        << evidence.accounted_worker_cpu_ns
        << ",\"executor_wall_ns\":" << evidence.executor_wall_ns
        << ",\"quantization_guard_ns\":"
        << evidence.quantization_guard_ns << '}';
}

void write_result_json(
    const AnalyzeResult& result,
    const ResolvedConfig& resolved
) {
    const auto& report = result.pagerank_report;
    const auto& resources = resolved.pagerank.resources;
    const bool verified_default_profile =
        resolved.pagerank.alpha == tbank::pagerank::kPageRankDefaultAlpha
        && resolved.pagerank.eta == tbank::pagerank::kPageRankDefaultEta
        && resolved.pagerank.max_iterations
            == tbank::pagerank::kPageRankDefaultMaxIterations
        && resolved.pagerank.tau_mass
            == tbank::pagerank::kPageRankDefaultTauMass
        && resolved.pagerank.epsilon_verify
            == tbank::pagerank::kPageRankDefaultEpsilonVerify;
    std::ostringstream output;
    output.imbue(std::locale::classic());
    output
        << "{\"schema\":\"TBANK_ANALYZE_RESULT_V2\""
        << ",\"status\":"
        << tbank::cli::json_quote(analyze_status_name(result.status))
        << ",\"verified_default_profile\":"
        << (verified_default_profile ? "true" : "false")
        << ",\"telemetry\":{\"clock\":\"steady_monotonic\""
        << ",\"durations_ns\":{"
        << "\"graph_validation_open_preflight\":"
        << result.telemetry.graph_validation_open_preflight_ns
        << ",\"pagerank_engine\":"
        << result.telemetry.pagerank_engine_ns
        << ",\"candidate_destination_traversal\":"
        << result.telemetry.candidate_destination_traversal_ns
        << ",\"first_candidate_destination_traversal\":"
        << result.telemetry.first_candidate_destination_traversal_ns
        << ",\"true_residual_destination_traversal\":"
        << result.telemetry.true_residual_destination_traversal_ns
        << ",\"core_destination_traversal\":"
        << result.telemetry.core_destination_traversal_ns
        << ",\"pagerank_engine_other\":"
        << result.telemetry.pagerank_engine_other_ns
        << ",\"csv_publication\":";
    if (result.telemetry.csv_publication_ns.has_value()) {
        output << *result.telemetry.csv_publication_ns;
    } else {
        output << "null";
    }
    output
        << ",\"total\":" << result.telemetry.total_ns
        << "},\"counts\":{"
        << "\"candidate_destination_traversals\":"
        << result.telemetry.candidate_destination_traversal_count
        << ",\"true_residual_destination_traversals\":"
        << result.telemetry.true_residual_destination_traversal_count
        << "},\"internal_concurrency\":";
    write_internal_concurrency(
        output, result.telemetry.internal_concurrency
    );
    output
        << '}'
        << ",\"config\":{"
        << "\"alpha_hex\":"
        << tbank::cli::json_hex_double(resolved.pagerank.alpha)
        << ",\"eta_hex\":"
        << tbank::cli::json_hex_double(resolved.pagerank.eta)
        << ",\"max_iterations\":" << resolved.pagerank.max_iterations
        << ",\"tau_mass_hex\":"
        << tbank::cli::json_hex_double(resolved.pagerank.tau_mass)
        << ",\"epsilon_verify_hex\":"
        << tbank::cli::json_hex_double(resolved.pagerank.epsilon_verify)
        << ",\"execution_mode\":"
        << tbank::cli::json_quote(
               resources.worker_count == 0U ? "sequential" : "parallel"
           )
        << ",\"worker_count\":" << resources.worker_count
        << ",\"record_batch_records\":" << resources.record_batch_records
        << ",\"validation_chunk_bytes\":"
        << resources.validation_io_chunk_bytes
        << ",\"main_stack_bytes\":" << resources.main_stack_bytes
        << ",\"observed_main_stack_bytes\":"
        << resolved.observed_main_stack_bytes
        << ",\"runtime_reserve_bytes\":"
        << resources.runtime_reserve_bytes
        << ",\"worker_stack_bytes\":" << resources.worker_stack_bytes
        << ",\"worker_guard_bytes\":" << resources.worker_guard_bytes
        << ",\"worker_count_batch_records\":"
        << resources.worker_count_batch_records
        << ",\"worker_source_batch_records\":"
        << resources.worker_source_batch_records
        << ",\"scheduler_window_records\":"
        << resources.scheduler_window_records
        << "},\"report\":{"
        << "\"status\":"
        << tbank::cli::json_quote(pagerank_status_name(report.status))
        << ",\"numerical_failure\":"
        << tbank::cli::json_quote(
               numerical_failure_name(report.numerical_failure)
           )
        << ",\"iterations_attempted\":" << report.iterations_attempted
        << ",\"residual_checks\":" << report.residual_checks
        << ",\"last_raw_mass_hex\":"
        << optional_hex(report.last_raw_mass)
        << ",\"max_mass_error_hex\":"
        << tbank::cli::json_hex_double(report.max_mass_error)
        << ",\"last_delta_l1_hex\":"
        << optional_hex(report.last_delta_l1)
        << ",\"theoretical_delta_error_estimate_l1_hex\":"
        << optional_hex(report.theoretical_delta_error_estimate_l1)
        << ",\"true_residual_l1_hex\":"
        << optional_hex(report.true_residual_l1)
        << ",\"theoretical_residual_error_estimate_l1_hex\":"
        << optional_hex(report.theoretical_residual_error_estimate_l1)
        << ",\"memory_plan\":";
    write_memory_plan(output, report.memory_plan);
    output
        << "},\"csv\":{\"rows\":";
    if (result.csv_summary.has_value()) {
        output << result.csv_summary->rows;
    } else {
        output << "null";
    }
    output << ",\"bytes\":";
    if (result.csv_summary.has_value()) {
        output << result.csv_summary->bytes;
    } else {
        output << "null";
    }
    output
        << "},\"publication\":{\"state\":"
        << tbank::cli::json_quote(
               publication_state_name(result.publication.state)
           )
        << ",\"failed_step\":"
        << tbank::cli::json_quote(
               publication_step_name(result.publication.failed_step)
           )
        << ",\"error\":" << result.publication.error.value()
        << ",\"cleanup_error\":"
        << result.publication.cleanup_error.value()
        << ",\"temporary_path\":"
        << tbank::cli::json_quote(result.publication.temporary_path.native())
        << "}}\n";
    tbank::cli::write_stdout(output.str());
}

[[nodiscard]] int outcome_exit(const AnalyzeStatus status) noexcept {
    switch (status) {
        case AnalyzeStatus::published:
            return tbank::cli::kExitSuccess;
        case AnalyzeStatus::non_converged:
            return tbank::cli::kExitNonConverged;
        case AnalyzeStatus::numerical_failure:
            return tbank::cli::kExitNumericalFailure;
        case AnalyzeStatus::publication_failed:
            return tbank::cli::kExitPublication;
        case AnalyzeStatus::durability_uncertain:
            return tbank::cli::kExitDurabilityUncertain;
    }
    return tbank::cli::kExitInternal;
}

[[nodiscard]] int run(const ParsedOptions& options) {
    const std::filesystem::path graph = path_option(options, "graph-dir");
    const std::filesystem::path output = path_option(options, "output-csv");
    ResolvedConfig config = make_config(options);
    config.observed_main_stack_bytes = verify_main_stack_limit(
        config.pagerank.resources.main_stack_bytes
    );
    const AnalyzeResult result = tbank::pagerank::analyze_page_rank_to_csv(
        graph, output, config.pagerank
    );
    write_result_json(result, config);
    return outcome_exit(result.status);
}

void report_memory_limit(
    const tbank::resources::PageRankMemoryLimitError& error
) noexcept {
    try {
        std::ostringstream output;
        output.imbue(std::locale::classic());
        output
            << "{\"schema\":\"TBANK_CLI_ERROR_V1\",\"command\":\"analyze\""
            << ",\"kind\":\"resource\""
            << ",\"code\":\"memory_preflight_rejected\""
            << ",\"phase\":"
            << tbank::cli::json_quote(
                   error.vertex_count() == 1U
                       ? "configuration_shape_preflight"
                       : "graph_manifest_preflight"
               )
            << ",\"message\":" << tbank::cli::json_quote(error.what())
            << ",\"vertex_count\":" << error.vertex_count()
            << ",\"required_bytes\":" << error.required_bytes()
            << ",\"budget_bytes\":" << error.budget_bytes() << "}\n";
        tbank::cli::write_stderr(output.str());
    } catch (...) {
        tbank::cli::write_error_json(
            "analyze",
            "resource",
            "memory_preflight_rejected",
            error.what()
        );
    }
}

void report_stack_limit(const StackLimitMismatchError& error) noexcept {
    try {
        std::ostringstream output;
        output.imbue(std::locale::classic());
        output
            << "{\"schema\":\"TBANK_CLI_ERROR_V1\",\"command\":\"analyze\""
            << ",\"kind\":\"resource\""
            << ",\"code\":\"stack_limit_mismatch\""
            << ",\"phase\":\"startup_preflight\""
            << ",\"message\":" << tbank::cli::json_quote(error.what())
            << ",\"declared_bytes\":" << error.declared_bytes()
            << ",\"observed_bytes\":" << error.observed_bytes() << "}\n";
        tbank::cli::write_stderr(output.str());
    } catch (...) {
        tbank::cli::write_error_json(
            "analyze", "resource", "stack_limit_mismatch", error.what()
        );
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
        const ParsedOptions options = ParsedOptions::parse(
            argument_count, arguments, kAllowedOptions
        );
        return run(options);
    } catch (const UsageError& error) {
        tbank::cli::write_error_json(
            "analyze", "usage", "invalid_arguments", error.what()
        );
        return tbank::cli::kExitUsage;
    } catch (const StackLimitMismatchError& error) {
        report_stack_limit(error);
        return tbank::cli::kExitResource;
    } catch (const tbank::resources::PageRankMemoryLimitError& error) {
        report_memory_limit(error);
        return tbank::cli::kExitResource;
    } catch (const std::bad_alloc& error) {
        tbank::cli::write_error_json(
            "analyze", "resource", "allocation_failed", error.what()
        );
        return tbank::cli::kExitResource;
    } catch (const tbank::storage::GraphValidationError& error) {
        tbank::cli::write_error_json(
            "analyze", "data", "invalid_graph", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const tbank::storage::ManifestError& error) {
        tbank::cli::write_error_json(
            "analyze", "data", "invalid_graph", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const tbank::storage::BinaryError& error) {
        tbank::cli::write_error_json(
            "analyze", "data", "invalid_graph", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const std::system_error& error) {
        tbank::cli::write_error_json(
            "analyze", "system", "system_error", error.what()
        );
        return tbank::cli::kExitSystem;
    } catch (const std::invalid_argument& error) {
        tbank::cli::write_error_json(
            "analyze", "data", "invalid_data", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const std::logic_error& error) {
        tbank::cli::write_error_json(
            "analyze", "internal", "invariant_failure", error.what()
        );
        return tbank::cli::kExitInternal;
    } catch (const std::exception& error) {
        tbank::cli::write_error_json(
            "analyze", "internal", "unexpected_exception", error.what()
        );
        return tbank::cli::kExitInternal;
    } catch (...) {
        tbank::cli::write_error_json(
            "analyze",
            "internal",
            "unknown_exception",
            "unknown non-standard exception"
        );
        return tbank::cli::kExitInternal;
    }
}

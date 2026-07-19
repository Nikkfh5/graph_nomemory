#include "engine.hpp"

#include "numerics.hpp"
#include "timing.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/file_reader.hpp"
#include "tbank/storage/graph.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tbank::pagerank {
namespace {

using storage::TaskRecord;
using storage::TaskTag;
using storage::ValidatedBinaryFileReader;
using storage::ValidatedGraph;

[[noreturn]] void fail(const std::string& detail) {
    throw PageRankExecutionError("PageRank execution: " + detail);
}

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) noexcept {
    static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));
    return static_cast<std::uint64_t>(value);
}

class BatchedRecordCursor final {
public:
    BatchedRecordCursor(
        const ValidatedBinaryFileReader& reader,
        const std::uint32_t expected_record_bytes,
        const std::size_t batch_records,
        std::string diagnostic_name
    )
        : reader_(reader),
          header_(reader.header()),
          batch_records_(batch_records),
          diagnostic_name_(std::move(diagnostic_name)) {
        if (header_.record_bytes != expected_record_bytes) {
            fail(diagnostic_name_ + ": unexpected physical record size");
        }
        if (batch_records_ == 0U
            || batch_records_
                > std::numeric_limits<std::size_t>::max()
                    / header_.record_bytes) {
            fail(diagnostic_name_ + ": invalid record batch size");
        }
        buffer_.resize(batch_records_ * header_.record_bytes);
    }

    [[nodiscard]] std::span<const std::byte> next() {
        if (records_read_ == header_.record_count) {
            fail(diagnostic_name_ + ": record stream ended unexpectedly");
        }
        if (batch_position_ == batch_size_) {
            refill();
        }

        const std::size_t offset = batch_position_ * header_.record_bytes;
        ++batch_position_;
        ++records_read_;
        return std::span<const std::byte>(buffer_).subspan(
            offset, header_.record_bytes
        );
    }

    void require_exhausted() const {
        if (records_read_ != header_.record_count) {
            fail(diagnostic_name_ + ": record stream was not consumed exactly");
        }
    }

private:
    void refill() {
        const std::uint64_t remaining = header_.record_count - records_read_;
        const std::uint64_t selected_u64 = std::min(
            remaining, size_to_u64(batch_records_)
        );
        const std::size_t selected = static_cast<std::size_t>(selected_u64);
        const std::size_t selected_bytes = selected * header_.record_bytes;
        reader_.read_records(
            records_read_,
            std::span<std::byte>(buffer_.data(), selected_bytes)
        );
        batch_position_ = 0U;
        batch_size_ = selected;
    }

    const ValidatedBinaryFileReader& reader_;
    storage::BinaryHeader header_{};
    std::size_t batch_records_ = 0U;
    std::string diagnostic_name_;
    std::vector<std::byte> buffer_;
    std::size_t batch_position_ = 0U;
    std::size_t batch_size_ = 0U;
    std::uint64_t records_read_ = 0U;
};

[[nodiscard]] std::uint32_t next_u32(BatchedRecordCursor& cursor) {
    return storage::decode_u32_le(cursor.next());
}

[[nodiscard]] TaskRecord next_task(BatchedRecordCursor& cursor) {
    return storage::decode_task_record(cursor.next());
}

[[nodiscard]] std::unique_ptr<std::uint32_t[]> load_verified_out_degree(
    const ValidatedGraph& graph,
    const std::size_t batch_records
) {
    const std::uint32_t vertex_count = graph.manifest().vertex_count;
    auto out_degree = std::make_unique<std::uint32_t[]>(vertex_count);

    {
        BatchedRecordCursor sources(
            graph.incoming_sources(),
            storage::kScalarRecordBytes,
            batch_records,
            "incoming_sources.bin"
        );
        for (std::uint64_t edge = 0U;
             edge < graph.manifest().edge_count;
             ++edge) {
            const std::uint32_t source = next_u32(sources);
            if (source >= vertex_count) {
                fail("incoming source is outside compact vertex range");
            }
            if (out_degree[source]
                == std::numeric_limits<std::uint32_t>::max()) {
                fail("recomputed source out-degree overflows uint32");
            }
            ++out_degree[source];
        }
        sources.require_exhausted();
    }

    BatchedRecordCursor stored(
        graph.out_degree(),
        storage::kScalarRecordBytes,
        batch_records,
        "out_degree.bin"
    );
    for (std::uint32_t source = 0U; source < vertex_count; ++source) {
        if (next_u32(stored) != out_degree[source]) {
            fail(
                "out_degree.bin source histogram mismatch at compact source "
                + std::to_string(source)
            );
        }
    }
    stored.require_exhausted();
    return out_degree;
}

struct ActiveHub {
    std::uint32_t destination = 0U;
    std::uint32_t slice_count = 0U;
    std::uint32_t next_slice = 0U;
    std::uint64_t expected_edges = 0U;
    std::uint64_t covered_edges = 0U;
    double incoming_sum = 0.0;
};

template <class Contribution>
[[nodiscard]] double consume_sources(
    BatchedRecordCursor& sources,
    const std::span<const std::uint32_t> out_degree,
    const std::uint64_t count,
    const std::uint64_t edge_count,
    std::uint64_t& next_edge,
    Contribution&& contribution
) {
    if (next_edge > edge_count || count > edge_count - next_edge) {
        fail("task edge range exceeds manifest edge_count");
    }

    double sum = 0.0;
    for (std::uint64_t index = 0U; index < count; ++index) {
        const std::uint32_t source = next_u32(sources);
        if (source >= out_degree.size()) {
            fail("incoming source is outside resident vertex state");
        }
        if (out_degree[source] == 0U) {
            fail("edge source has zero verified out-degree");
        }
        sum += contribution(source);
        ++next_edge;
    }
    return sum;
}

template <class Contribution, class DestinationConsumer>
void traverse_destinations(
    const ValidatedGraph& graph,
    const std::span<const std::uint32_t> out_degree,
    const std::size_t batch_records,
    Contribution&& contribution,
    DestinationConsumer&& destination_consumer
) {
    BatchedRecordCursor counts(
        graph.incoming_counts(),
        storage::kScalarRecordBytes,
        batch_records,
        "incoming_counts.bin"
    );
    BatchedRecordCursor sources(
        graph.incoming_sources(),
        storage::kScalarRecordBytes,
        batch_records,
        "incoming_sources.bin"
    );
    BatchedRecordCursor tasks(
        graph.tasks(),
        storage::kTaskRecordBytes,
        batch_records,
        "tasks.bin"
    );

    std::uint64_t next_destination = 0U;
    std::uint64_t next_edge = 0U;
    ActiveHub hub{};
    bool hub_active = false;
    const std::uint64_t task_count = graph.tasks().header().record_count;

    for (std::uint64_t task_index = 0U;
         task_index < task_count;
         ++task_index) {
        const TaskRecord task = next_task(tasks);
        if (task.edge_begin != next_edge) {
            fail("tasks.bin edge ranges are not traversed in order");
        }

        if (task.tag == TaskTag::ordinary) {
            if (hub_active) {
                fail("ordinary task interrupts hub slices");
            }
            if (task.a != next_destination) {
                fail("ordinary destinations are not traversed in order");
            }
            const std::uint64_t destination_end =
                static_cast<std::uint64_t>(task.a) + task.b;
            if (destination_end > graph.manifest().vertex_count) {
                fail("ordinary destination range exceeds graph");
            }

            const std::uint64_t task_edge_begin = next_edge;
            for (std::uint64_t destination = task.a;
                 destination < destination_end;
                 ++destination) {
                const std::uint32_t incoming_count = next_u32(counts);
                const double incoming_sum = consume_sources(
                    sources,
                    out_degree,
                    incoming_count,
                    graph.manifest().edge_count,
                    next_edge,
                    contribution
                );
                destination_consumer(
                    static_cast<std::uint32_t>(destination), incoming_sum
                );
            }
            if (task.edge_count != next_edge - task_edge_begin) {
                fail("ordinary task edge_count disagrees with counts");
            }
            next_destination = destination_end;
            continue;
        }

        if (task.tag != TaskTag::hub_slice) {
            fail("tasks.bin contains an unknown task tag");
        }
        if (!hub_active) {
            if (task.b != 0U || task.a != next_destination) {
                fail("first hub slice is not canonical");
            }
            hub = ActiveHub{
                .destination = task.a,
                .slice_count = task.c,
                .next_slice = 0U,
                .expected_edges = next_u32(counts),
                .covered_edges = 0U,
                .incoming_sum = 0.0,
            };
            hub_active = true;
        }
        if (task.a != hub.destination
            || task.c != hub.slice_count
            || task.b != hub.next_slice) {
            fail("hub slices are not one ordered group");
        }
        if (hub.covered_edges > hub.expected_edges
            || task.edge_count
                > hub.expected_edges - hub.covered_edges) {
            fail("hub slices exceed destination incoming count");
        }

        hub.incoming_sum += consume_sources(
            sources,
            out_degree,
            task.edge_count,
            graph.manifest().edge_count,
            next_edge,
            contribution
        );
        hub.covered_edges += task.edge_count;
        ++hub.next_slice;
        if (hub.next_slice == hub.slice_count) {
            if (hub.covered_edges != hub.expected_edges) {
                fail("hub slices do not cover destination incoming edges");
            }
            destination_consumer(hub.destination, hub.incoming_sum);
            ++next_destination;
            hub_active = false;
        }
    }

    tasks.require_exhausted();
    counts.require_exhausted();
    sources.require_exhausted();
    if (hub_active) {
        fail("task stream ended inside a hub destination");
    }
    if (next_destination != graph.manifest().vertex_count) {
        fail("task stream did not write every destination");
    }
    if (next_edge != graph.manifest().edge_count) {
        fail("task stream did not consume every edge");
    }
}

[[nodiscard]] std::optional<double> compute_true_residual_single_thread(
    const ValidatedGraph& graph,
    const std::span<const std::uint32_t> out_degree,
    const std::span<const double> candidate,
    const PageRankConfig& config
) {
    double dangling_mass = 0.0;
    for (std::size_t vertex = 0U; vertex < candidate.size(); ++vertex) {
        if (!std::isfinite(candidate[vertex]) || candidate[vertex] < 0.0) {
            return std::nullopt;
        }
        if (out_degree[vertex] == 0U) {
            dangling_mass += candidate[vertex];
            if (!std::isfinite(dangling_mass)) {
                return std::nullopt;
            }
        }
    }

    const double vertex_count = static_cast<double>(candidate.size());
    const double base = (1.0 - config.alpha) / vertex_count
        + config.alpha * dangling_mass / vertex_count;
    if (!std::isfinite(base) || base < 0.0) {
        return std::nullopt;
    }

    double residual_l1 = 0.0;
    bool valid = true;
    traverse_destinations(
        graph,
        out_degree,
        config.resources.record_batch_records,
        [&](const std::uint32_t source) {
            return candidate[source] / static_cast<double>(out_degree[source]);
        },
        [&](const std::uint32_t destination, const double incoming_sum) {
            const double mapped = base + config.alpha * incoming_sum;
            if (!std::isfinite(mapped) || mapped < 0.0) {
                valid = false;
                return;
            }
            residual_l1 += std::abs(mapped - candidate[destination]);
            if (!std::isfinite(residual_l1)) {
                valid = false;
            }
        }
    );
    return valid ? std::optional<double>(residual_l1) : std::nullopt;
}

class SingleThreadDestinationTraversal final
    : public detail::DestinationTraversal {
public:
    SingleThreadDestinationTraversal(
        const ValidatedGraph& graph,
        const std::size_t batch_records
    ) noexcept
        : graph_(graph), batch_records_(batch_records) {}

    void write_candidate(
        const std::span<const std::uint32_t> out_degree,
        const std::span<const double> transformed_current,
        const double base,
        const double alpha,
        const std::span<double> candidate
    ) override {
        traverse_destinations(
            graph_,
            out_degree,
            batch_records_,
            [&](const std::uint32_t source) {
                return transformed_current[source];
            },
            [&](const std::uint32_t destination, const double incoming_sum) {
                candidate[destination] = base + alpha * incoming_sum;
            }
        );
    }

    [[nodiscard]] std::optional<double> compute_true_residual(
        const std::span<const std::uint32_t> out_degree,
        const std::span<const double> candidate,
        const std::span<double> reusable_workspace,
        const PageRankConfig& config
    ) override {
        static_cast<void>(reusable_workspace);
        return compute_true_residual_single_thread(
            graph_, out_degree, candidate, config
        );
    }

private:
    const ValidatedGraph& graph_;
    std::size_t batch_records_;
};

void write_candidate_with_telemetry(
    detail::DestinationTraversal& traversal,
    const std::span<const std::uint32_t> out_degree,
    const std::span<const double> transformed_current,
    const double base,
    const double alpha,
    const std::span<double> candidate,
    detail::PageRankTraversalTelemetry* const telemetry
) {
    if (telemetry == nullptr) {
        traversal.write_candidate(
            out_degree, transformed_current, base, alpha, candidate
        );
        return;
    }

    // Measure destination traversal only; exclude transform and normalization.
    const detail::TelemetryTimePoint started = detail::TelemetryClock::now();
    traversal.write_candidate(
        out_degree, transformed_current, base, alpha, candidate
    );
    const detail::TelemetryTimePoint finished = detail::TelemetryClock::now();
    const std::uint64_t elapsed =
        detail::elapsed_nanoseconds(started, finished);
    if (telemetry->candidate_destination_traversal_count == 0U) {
        telemetry->first_candidate_destination_traversal_ns = elapsed;
    }
    detail::add_telemetry_value(
        telemetry->candidate_destination_traversal_ns, elapsed
    );
    detail::increment_telemetry_count(
        telemetry->candidate_destination_traversal_count
    );
}

[[nodiscard]] std::optional<double> compute_true_residual_with_telemetry(
    detail::DestinationTraversal& traversal,
    const std::span<const std::uint32_t> out_degree,
    const std::span<const double> candidate,
    const std::span<double> reusable_workspace,
    const PageRankConfig& config,
    detail::PageRankTraversalTelemetry* const telemetry
) {
    if (telemetry == nullptr) {
        return traversal.compute_true_residual(
            out_degree, candidate, reusable_workspace, config
        );
    }

    const detail::TelemetryTimePoint started = detail::TelemetryClock::now();
    std::optional<double> residual = traversal.compute_true_residual(
        out_degree, candidate, reusable_workspace, config
    );
    const detail::TelemetryTimePoint finished = detail::TelemetryClock::now();
    detail::add_telemetry_value(
        telemetry->true_residual_destination_traversal_ns,
        detail::elapsed_nanoseconds(started, finished)
    );
    detail::increment_telemetry_count(
        telemetry->true_residual_destination_traversal_count
    );
    return residual;
}

}  // namespace

PageRankResult::PageRankResult(
    PageRankReport report,
    std::unique_ptr<double[]> verified_ranks,
    const std::uint32_t vertex_count
) noexcept
    : report_(std::move(report)),
      verified_ranks_(std::move(verified_ranks)),
      vertex_count_(vertex_count) {}

PageRankVertexCountLimitError::PageRankVertexCountLimitError(
    const std::uint32_t vertex_count
)
    : std::runtime_error(
          "PageRank vertex_count " + std::to_string(vertex_count)
          + " exceeds verified numerical domain maximum "
          + std::to_string(kPageRankVerifiedMaximumVertexCount)
      ),
      vertex_count_(vertex_count) {}

std::uint32_t PageRankVertexCountLimitError::vertex_count() const noexcept {
    return vertex_count_;
}

std::uint32_t
PageRankVertexCountLimitError::maximum_vertex_count() const noexcept {
    return kPageRankVerifiedMaximumVertexCount;
}

PageRankResult detail::PageRankResultFactory::make(
    PageRankReport report,
    std::unique_ptr<double[]> verified_ranks,
    const std::uint32_t vertex_count
) noexcept {
    return PageRankResult(
        std::move(report), std::move(verified_ranks), vertex_count
    );
}

const PageRankReport& PageRankResult::report() const noexcept {
    return report_;
}

bool PageRankResult::has_verified_ranks() const noexcept {
    return report_.status == PageRankStatus::converged
        && verified_ranks_ != nullptr;
}

std::span<const double> PageRankResult::verified_ranks() const {
    if (!has_verified_ranks()) {
        throw std::logic_error("PageRank result has no verified ranks");
    }
    return std::span<const double>(verified_ranks_.get(), vertex_count_);
}

detail::OpenedPageRankGraph detail::open_page_rank_graph(
    const std::filesystem::path& graph_directory,
    const PageRankConfig& config
) {
    validate_page_rank_config(config);
    resources::PageRankMemoryPlan memory_plan{};
    ValidatedGraph graph = ValidatedGraph::open(
        graph_directory,
        storage::GraphValidationOptions{
            .io_chunk_bytes = config.resources.validation_io_chunk_bytes,
        },
        [&](const storage::GraphManifest& manifest) {
            memory_plan = resources::page_rank_memory_plan(
                manifest.vertex_count, config.resources
            );
            if (manifest.vertex_count
                > kPageRankVerifiedMaximumVertexCount) {
                throw PageRankVertexCountLimitError(manifest.vertex_count);
            }
        }
    );
    if (memory_plan.vertex_count != graph.manifest().vertex_count) {
        fail("manifest preflight did not bind the validated graph");
    }
    return detail::OpenedPageRankGraph{
        .graph = std::move(graph),
        .memory_plan = memory_plan,
    };
}

PageRankResult detail::run_page_rank_with_traversal(
    const detail::OpenedPageRankGraph& opened,
    const PageRankConfig& config,
    detail::DestinationTraversal& traversal,
    const PageRankIterationObserver& observer,
    detail::PageRankTraversalTelemetry* const telemetry
) {
    const ValidatedGraph& graph = opened.graph;
    const resources::PageRankMemoryPlan& memory_plan = opened.memory_plan;

    const std::uint32_t vertex_count = graph.manifest().vertex_count;
    std::unique_ptr<std::uint32_t[]> out_degree = load_verified_out_degree(
        graph, config.resources.record_batch_records
    );
    std::unique_ptr<double[]> current(new double[vertex_count]);
    std::unique_ptr<double[]> scratch(new double[vertex_count]);
    const double initial_rank = 1.0 / static_cast<double>(vertex_count);
    std::fill_n(current.get(), vertex_count, initial_rank);

    PageRankReport report{
        .status = PageRankStatus::non_converged,
        .numerical_failure = PageRankNumericalFailure::none,
        .iterations_attempted = 0U,
        .residual_checks = 0U,
        .last_raw_mass = std::nullopt,
        .max_mass_error = 0.0,
        .last_delta_l1 = std::nullopt,
        .theoretical_delta_error_estimate_l1 = std::nullopt,
        .true_residual_l1 = std::nullopt,
        .theoretical_residual_error_estimate_l1 = std::nullopt,
        .memory_plan = memory_plan,
    };
    const std::span<const std::uint32_t> degrees(
        out_degree.get(), vertex_count
    );
    const auto numerical_failure = [&](const PageRankNumericalFailure failure) {
        report.status = PageRankStatus::numerical_failure;
        report.numerical_failure = failure;
        return detail::PageRankResultFactory::make(
            std::move(report), {}, vertex_count
        );
    };

    std::uint64_t iteration = 1U;
    while (true) {
        report.iterations_attempted = iteration;

        const std::span<double> current_view(current.get(), vertex_count);
        const detail::TransformResult transform =
            detail::transform_current_in_place(current_view, degrees);
        if (transform.failure != PageRankNumericalFailure::none) {
            return numerical_failure(transform.failure);
        }

        const double n = static_cast<double>(vertex_count);
        const double base = (1.0 - config.alpha) / n
            + config.alpha * transform.dangling_mass / n;
        if (!std::isfinite(base) || base < 0.0) {
            return numerical_failure(
                PageRankNumericalFailure::invalid_dangling_mass
            );
        }

        const std::span<double> candidate(scratch.get(), vertex_count);
        write_candidate_with_telemetry(
            traversal,
            degrees,
            current_view,
            base,
            config.alpha,
            candidate,
            telemetry
        );

        const detail::CandidateMetrics metrics =
            detail::normalize_and_measure_candidate(
                candidate, current_view, degrees, config.tau_mass
            );
        report.last_raw_mass = metrics.raw_mass;
        if (std::isfinite(metrics.mass_error)) {
            report.max_mass_error = std::max(
                report.max_mass_error, metrics.mass_error
            );
        }
        if (metrics.failure != PageRankNumericalFailure::none) {
            return numerical_failure(metrics.failure);
        }

        report.last_delta_l1 = metrics.delta_l1;
        report.theoretical_delta_error_estimate_l1 =
            config.alpha / (1.0 - config.alpha) * metrics.delta_l1;
        report.true_residual_l1.reset();
        report.theoretical_residual_error_estimate_l1.reset();

        // Delta only gates the independent full-graph residual check required before acceptance.
        if (detail::delta_prefilter_passes(
                metrics.delta_l1, config.alpha, config.eta
            )) {
            ++report.residual_checks;
            const std::optional<double> residual =
                compute_true_residual_with_telemetry(
                    traversal,
                    degrees,
                    candidate,
                    current_view,
                    config,
                    telemetry
                );
            if (!residual.has_value()) {
                return numerical_failure(
                    PageRankNumericalFailure::invalid_residual
                );
            }
            report.true_residual_l1 = *residual;
            report.theoretical_residual_error_estimate_l1 =
                *residual / (1.0 - config.alpha);
        }

        if (observer) {
            observer(PageRankIterationView{
                .iteration = iteration,
                .candidate = candidate,
                .raw_mass = metrics.raw_mass,
                .delta_l1 = metrics.delta_l1,
                .true_residual_l1 = report.true_residual_l1,
            });
        }

        // Return verified scratch; otherwise rotate the two resident buffers.
        if (report.true_residual_l1.has_value()
            && detail::residual_condition_passes(
                *report.true_residual_l1,
                config.epsilon_verify,
                config.alpha,
                config.eta
            )) {
            report.status = PageRankStatus::converged;
            return detail::PageRankResultFactory::make(
                std::move(report), std::move(scratch), vertex_count
            );
        }

        if (iteration == config.max_iterations) {
            break;
        }
        current.swap(scratch);
        ++iteration;
    }

    report.status = PageRankStatus::non_converged;
    return detail::PageRankResultFactory::make(
        std::move(report), {}, vertex_count
    );
}

PageRankResult detail::run_page_rank_on_opened_graph(
    const detail::OpenedPageRankGraph& opened,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer,
    detail::PageRankTraversalTelemetry* const telemetry
) {
    if (telemetry != nullptr) {
        telemetry->internal_concurrency =
            parallel::FixedExecutorConcurrencyEvidence{
                .status = parallel::FixedExecutorConcurrencyStatus::
                    not_applicable,
                .failure = parallel::FixedExecutorConcurrencyFailure::none,
            };
    }
    SingleThreadDestinationTraversal traversal(
        opened.graph, config.resources.record_batch_records
    );
    return detail::run_page_rank_with_traversal(
        opened, config, traversal, observer, telemetry
    );
}

PageRankResult run_page_rank_single_thread(
    const std::filesystem::path& graph_directory,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer
) {
    if (config.resources.worker_count != 0U) {
        throw std::invalid_argument(
            "single-thread PageRank requires a zero-worker resource policy"
        );
    }
    const detail::OpenedPageRankGraph opened = detail::open_page_rank_graph(
        graph_directory, config
    );
    return detail::run_page_rank_on_opened_graph(opened, config, observer);
}

}  // namespace tbank::pagerank

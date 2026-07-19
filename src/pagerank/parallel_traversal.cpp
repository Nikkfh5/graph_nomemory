#include "engine.hpp"

#include "tbank/parallel/fixed_executor.hpp"
#include "tbank/platform/checked_io.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/file_reader.hpp"
#include "tbank/storage/graph.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace tbank::pagerank {
namespace {

using storage::TaskRecord;
using storage::TaskTag;
using storage::ValidatedBinaryFileReader;
using storage::ValidatedGraph;

[[noreturn]] void fail(const std::string& detail) {
    throw PageRankExecutionError("PageRank parallel traversal: " + detail);
}

[[nodiscard]] std::size_t checked_size(
    const std::uint64_t value,
    const char* const description
) {
    if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
        if (value > std::numeric_limits<std::size_t>::max()) {
            throw std::overflow_error(
                std::string(description) + " is outside size_t range"
            );
        }
    }
    return static_cast<std::size_t>(value);
}

[[nodiscard]] std::size_t checked_allocation_size(
    const std::uint64_t count,
    const std::uint64_t width,
    const char* const description
) {
    return checked_size(
        platform::checked_multiply(count, width), description
    );
}

class PositionalU32Cursor final {
public:
    PositionalU32Cursor(
        const ValidatedBinaryFileReader& reader,
        const std::uint64_t record_begin,
        const std::uint64_t record_count,
        const std::span<std::byte> buffer,
        std::string diagnostic_name
    )
        : reader_(reader),
          record_begin_(record_begin),
          record_count_(record_count),
          buffer_(buffer),
          diagnostic_name_(std::move(diagnostic_name)) {
        if (reader_.header().record_bytes != storage::kScalarRecordBytes) {
            fail(diagnostic_name_ + ": unexpected physical record size");
        }
        if (buffer_.empty()
            || buffer_.size() % storage::kScalarRecordBytes != 0U) {
            fail(diagnostic_name_ + ": invalid worker record buffer");
        }
    }

    [[nodiscard]] std::uint32_t next() {
        if (position_ == record_count_) {
            fail(diagnostic_name_ + ": range ended unexpectedly");
        }
        if (buffer_position_ == buffer_records_) {
            refill();
        }
        const std::size_t offset =
            buffer_position_ * storage::kScalarRecordBytes;
        ++buffer_position_;
        ++position_;
        return storage::decode_u32_le(
            std::span<const std::byte>(buffer_).subspan(
                offset, storage::kScalarRecordBytes
            )
        );
    }

    void require_exhausted() const {
        if (position_ != record_count_) {
            fail(diagnostic_name_ + ": range was not consumed exactly");
        }
    }

private:
    void refill() {
        const std::uint64_t remaining = record_count_ - position_;
        const std::size_t capacity =
            buffer_.size() / storage::kScalarRecordBytes;
        const std::uint64_t selected_u64 = std::min(
            remaining, static_cast<std::uint64_t>(capacity)
        );
        const std::size_t selected = static_cast<std::size_t>(selected_u64);
        const std::size_t selected_bytes =
            selected * storage::kScalarRecordBytes;
        reader_.read_records(
            platform::checked_add(record_begin_, position_),
            buffer_.first(selected_bytes)
        );
        buffer_position_ = 0U;
        buffer_records_ = selected;
    }

    const ValidatedBinaryFileReader& reader_;
    std::uint64_t record_begin_ = 0U;
    std::uint64_t record_count_ = 0U;
    std::span<std::byte> buffer_{};
    std::string diagnostic_name_;
    std::uint64_t position_ = 0U;
    std::size_t buffer_position_ = 0U;
    std::size_t buffer_records_ = 0U;
};

struct StructuralHub {
    std::uint32_t destination = 0U;
    std::uint32_t slice_count = 0U;
    std::uint32_t next_slice = 0U;
};

struct StructuralCursor {
    std::uint64_t next_destination = 0U;
    std::uint64_t next_edge = 0U;
    StructuralHub hub{};
    bool hub_active = false;
};

struct ReductionHub {
    std::uint32_t destination = 0U;
    std::uint32_t slice_count = 0U;
    std::uint32_t next_slice = 0U;
    std::uint64_t expected_edges = 0U;
    std::uint64_t covered_edges = 0U;
    double incoming_sum = 0.0;
};

enum class PullPass {
    candidate,
    residual,
};

struct PullContext {
    PullPass pass = PullPass::candidate;
    std::span<const std::uint32_t> out_degree{};
    std::span<const double> source_values{};
    std::span<const double> candidate{};
    std::span<double> output{};
    double base = 0.0;
    double alpha = 0.0;
};

class ParallelTraversalWorkspace final {
public:
    ParallelTraversalWorkspace(
        const ValidatedGraph& graph,
        const resources::PageRankResourcePolicy& policy
    )
        : graph_(graph),
          worker_count_(checked_size(policy.worker_count, "worker count")),
          count_buffer_bytes_per_worker_(checked_allocation_size(
              policy.worker_count_batch_records,
              storage::kScalarRecordBytes,
              "per-worker count buffer"
          )),
          source_buffer_bytes_per_worker_(checked_allocation_size(
              policy.worker_source_batch_records,
              storage::kScalarRecordBytes,
              "per-worker source buffer"
          )),
          window_records_(policy.scheduler_window_records),
          task_window_bytes_(checked_allocation_size(
              policy.scheduler_window_records,
              storage::kTaskRecordBytes,
              "task window"
          )),
          count_buffers_bytes_(checked_allocation_size(
              policy.worker_count,
              count_buffer_bytes_per_worker_,
              "aggregate worker count buffers"
          )),
          source_buffers_bytes_(checked_allocation_size(
              policy.worker_count,
              source_buffer_bytes_per_worker_,
              "aggregate worker source buffers"
          )),
          task_window_(std::make_unique<std::byte[]>(task_window_bytes_)),
          partials_(std::make_unique<double[]>(window_records_)),
          count_buffers_(
              std::make_unique<std::byte[]>(count_buffers_bytes_)
          ),
          source_buffers_(
              std::make_unique<std::byte[]>(source_buffers_bytes_)
          ),
          executor_(parallel::FixedExecutorConfig{
              .worker_count = worker_count_,
              .usable_stack_bytes = checked_size(
                  policy.worker_stack_bytes, "worker stack"
              ),
              .guard_bytes = checked_size(
                  policy.worker_guard_bytes, "worker guard"
              ),
          }) {
        if (worker_count_ == 0U || window_records_ == 0U) {
            fail("parallel workspace requires workers and a task window");
        }
    }

    void write_candidate(
        const std::span<const std::uint32_t> out_degree,
        const std::span<const double> transformed_current,
        const double base,
        const double alpha,
        const std::span<double> candidate
    ) {
        run_pass(PullContext{
            .pass = PullPass::candidate,
            .out_degree = out_degree,
            .source_values = transformed_current,
            .candidate = {},
            .output = candidate,
            .base = base,
            .alpha = alpha,
        });
    }

    void write_residual_terms(
        const std::span<const std::uint32_t> out_degree,
        const std::span<const double> candidate,
        const double base,
        const double alpha,
        const std::span<double> workspace
    ) {
        run_pass(PullContext{
            .pass = PullPass::residual,
            .out_degree = out_degree,
            .source_values = candidate,
            .candidate = candidate,
            .output = workspace,
            .base = base,
            .alpha = alpha,
        });
    }

    [[nodiscard]] parallel::FixedExecutorConcurrencyEvidence
    concurrency_evidence() const {
        return executor_.concurrency_evidence();
    }

private:
    [[nodiscard]] TaskRecord task_at(const std::size_t slot) const {
        const std::size_t offset = slot * storage::kTaskRecordBytes;
        return storage::decode_task_record(
            std::span<const std::byte>(
                task_window_.get(), task_window_bytes_
            ).subspan(
                offset, storage::kTaskRecordBytes
            )
        );
    }

    [[nodiscard]] std::span<std::byte> count_buffer(
        const std::size_t worker_index
    ) {
        return std::span<std::byte>(
            count_buffers_.get(), count_buffers_bytes_
        ).subspan(
            worker_index * count_buffer_bytes_per_worker_,
            count_buffer_bytes_per_worker_
        );
    }

    [[nodiscard]] std::span<std::byte> source_buffer(
        const std::size_t worker_index
    ) {
        return std::span<std::byte>(
            source_buffers_.get(), source_buffers_bytes_
        ).subspan(
            worker_index * source_buffer_bytes_per_worker_,
            source_buffer_bytes_per_worker_
        );
    }

    void validate_task(
        const TaskRecord& task,
        StructuralCursor& cursor
    ) const {
        if (task.edge_begin != cursor.next_edge) {
            fail("task edge ranges are not in canonical order");
        }
        if (cursor.next_edge > graph_.manifest().edge_count
            || task.edge_count
                > graph_.manifest().edge_count - cursor.next_edge) {
            fail("task edge range exceeds manifest edge_count");
        }

        if (task.tag == TaskTag::ordinary) {
            if (cursor.hub_active) {
                fail("ordinary task interrupts a hub group");
            }
            if (task.a != cursor.next_destination) {
                fail("ordinary task destination range is not canonical");
            }
            const std::uint64_t destination_end =
                static_cast<std::uint64_t>(task.a) + task.b;
            if (destination_end > graph_.manifest().vertex_count) {
                fail("ordinary task destination range exceeds graph");
            }
            cursor.next_destination = destination_end;
            cursor.next_edge += task.edge_count;
            return;
        }

        if (task.tag != TaskTag::hub_slice) {
            fail("task stream contains an unknown tag");
        }
        if (!cursor.hub_active) {
            if (task.b != 0U || task.a != cursor.next_destination) {
                fail("first hub slice is not canonical");
            }
            cursor.hub = StructuralHub{
                .destination = task.a,
                .slice_count = task.c,
                .next_slice = 0U,
            };
            cursor.hub_active = true;
        }
        if (task.a != cursor.hub.destination
            || task.c != cursor.hub.slice_count
            || task.b != cursor.hub.next_slice) {
            fail("hub slice group is not canonical");
        }
        cursor.next_edge += task.edge_count;
        ++cursor.hub.next_slice;
        if (cursor.hub.next_slice == cursor.hub.slice_count) {
            ++cursor.next_destination;
            cursor.hub_active = false;
        }
    }

    [[nodiscard]] double consume_sources(
        PositionalU32Cursor& sources,
        const std::uint64_t count,
        const PullContext& context
    ) const {
        double sum = 0.0;
        for (std::uint64_t index = 0U; index < count; ++index) {
            const std::uint32_t source = sources.next();
            if (source >= context.out_degree.size()) {
                fail("incoming source is outside resident vertex state");
            }
            if (context.out_degree[source] == 0U) {
                fail("edge source has zero verified out-degree");
            }
            const double contribution = context.pass == PullPass::candidate
                ? context.source_values[source]
                : context.source_values[source]
                    / static_cast<double>(context.out_degree[source]);
            sum += contribution;
        }
        return sum;
    }

    void process_ordinary(
        const std::size_t worker_index,
        const TaskRecord& task,
        const PullContext& context
    ) {
        PositionalU32Cursor counts(
            graph_.incoming_counts(),
            task.a,
            task.b,
            count_buffer(worker_index),
            "incoming_counts.bin"
        );
        PositionalU32Cursor sources(
            graph_.incoming_sources(),
            task.edge_begin,
            task.edge_count,
            source_buffer(worker_index),
            "incoming_sources.bin"
        );

        std::uint64_t consumed_edges = 0U;
        const std::uint64_t destination_end =
            static_cast<std::uint64_t>(task.a) + task.b;
        for (std::uint64_t destination = task.a;
             destination < destination_end;
             ++destination) {
            const std::uint32_t incoming_count = counts.next();
            if (consumed_edges > task.edge_count
                || incoming_count > task.edge_count - consumed_edges) {
                fail("ordinary counts exceed task edge range");
            }
            const double incoming_sum = consume_sources(
                sources, incoming_count, context
            );
            consumed_edges += incoming_count;
            const double mapped = context.base + context.alpha * incoming_sum;
            const std::size_t destination_index =
                static_cast<std::size_t>(destination);
            if (context.pass == PullPass::candidate) {
                context.output[destination_index] = mapped;
            } else if (!std::isfinite(mapped) || mapped < 0.0) {
                context.output[destination_index] =
                    std::numeric_limits<double>::quiet_NaN();
            } else {
                context.output[destination_index] = std::abs(
                    mapped - context.candidate[destination_index]
                );
            }
        }
        counts.require_exhausted();
        sources.require_exhausted();
        if (consumed_edges != task.edge_count) {
            fail("ordinary counts do not cover task edge range");
        }
    }

    void process_task(
        const std::size_t worker_index,
        const std::size_t slot,
        const PullContext& context
    ) {
        const TaskRecord task = task_at(slot);
        if (task.tag == TaskTag::ordinary) {
            process_ordinary(worker_index, task, context);
            return;
        }

        PositionalU32Cursor sources(
            graph_.incoming_sources(),
            task.edge_begin,
            task.edge_count,
            source_buffer(worker_index),
            "incoming_sources.bin"
        );
        partials_[slot] = consume_sources(
            sources, task.edge_count, context
        );
        sources.require_exhausted();
    }

    [[nodiscard]] std::uint32_t read_incoming_count(
        const std::uint32_t destination
    ) const {
        std::array<std::byte, storage::kScalarRecordBytes> encoded{};
        graph_.incoming_counts().read_records(destination, encoded);
        return storage::decode_u32_le(encoded);
    }

    void reduce_hub_slice(
        const TaskRecord& task,
        const double partial,
        std::optional<ReductionHub>& active,
        const PullContext& context
    ) const {
        if (!active.has_value()) {
            if (task.b != 0U) {
                fail("hub reduction did not begin at slice zero");
            }
            active = ReductionHub{
                .destination = task.a,
                .slice_count = task.c,
                .next_slice = 0U,
                .expected_edges = read_incoming_count(task.a),
                .covered_edges = 0U,
                .incoming_sum = 0.0,
            };
        }
        ReductionHub& hub = *active;
        if (task.a != hub.destination
            || task.c != hub.slice_count
            || task.b != hub.next_slice) {
            fail("hub reduction order differs from task order");
        }
        if (hub.covered_edges > hub.expected_edges
            || task.edge_count > hub.expected_edges - hub.covered_edges) {
            fail("hub slices exceed destination incoming count");
        }
        hub.incoming_sum += partial;
        hub.covered_edges += task.edge_count;
        ++hub.next_slice;
        if (hub.next_slice != hub.slice_count) {
            return;
        }
        if (hub.covered_edges != hub.expected_edges) {
            fail("hub slices do not cover destination incoming count");
        }

        const double mapped = context.base
            + context.alpha * hub.incoming_sum;
        if (context.pass == PullPass::candidate) {
            context.output[hub.destination] = mapped;
        } else if (!std::isfinite(mapped) || mapped < 0.0) {
            context.output[hub.destination] =
                std::numeric_limits<double>::quiet_NaN();
        } else {
            context.output[hub.destination] = std::abs(
                mapped - context.candidate[hub.destination]
            );
        }
        active.reset();
    }

    void run_pass(const PullContext& context) {
        const std::size_t vertex_count = graph_.manifest().vertex_count;
        if (context.out_degree.size() != vertex_count
            || context.source_values.size() != vertex_count
            || context.output.size() != vertex_count
            || (context.pass == PullPass::residual
                && context.candidate.size() != vertex_count)) {
            fail("parallel pull received mismatched vertex spans");
        }

        StructuralCursor structure{};
        std::optional<ReductionHub> reduction;
        const std::uint64_t task_count = graph_.tasks().header().record_count;
        std::uint64_t task_index = 0U;
        while (task_index < task_count) {
            const std::uint64_t selected_u64 = std::min(
                task_count - task_index,
                static_cast<std::uint64_t>(window_records_)
            );
            const std::size_t selected =
                static_cast<std::size_t>(selected_u64);
            graph_.tasks().read_records(
                task_index,
                std::span<std::byte>(
                    task_window_.get(), task_window_bytes_
                ).first(
                    selected * storage::kTaskRecordBytes
                )
            );

            for (std::size_t slot = 0U; slot < selected; ++slot) {
                validate_task(task_at(slot), structure);
            }

            executor_.run_indexed(
                selected,
                [&](const std::size_t worker_index,
                    const std::size_t slot) {
                    process_task(worker_index, slot, context);
                }
            );

            for (std::size_t slot = 0U; slot < selected; ++slot) {
                const TaskRecord task = task_at(slot);
                if (task.tag == TaskTag::hub_slice) {
                    reduce_hub_slice(
                        task, partials_[slot], reduction, context
                    );
                }
            }
            task_index += selected_u64;
        }

        if (structure.hub_active || reduction.has_value()) {
            fail("task stream ended inside a hub group");
        }
        if (structure.next_destination != graph_.manifest().vertex_count) {
            fail("task stream did not cover every destination");
        }
        if (structure.next_edge != graph_.manifest().edge_count) {
            fail("task stream did not cover every edge");
        }
    }

    const ValidatedGraph& graph_;
    std::size_t worker_count_ = 0U;
    std::size_t count_buffer_bytes_per_worker_ = 0U;
    std::size_t source_buffer_bytes_per_worker_ = 0U;
    std::size_t window_records_ = 0U;
    std::size_t task_window_bytes_ = 0U;
    std::size_t count_buffers_bytes_ = 0U;
    std::size_t source_buffers_bytes_ = 0U;
    std::unique_ptr<std::byte[]> task_window_;
    std::unique_ptr<double[]> partials_;
    std::unique_ptr<std::byte[]> count_buffers_;
    std::unique_ptr<std::byte[]> source_buffers_;
    parallel::FixedExecutor executor_;
};

class ParallelDestinationTraversal final
    : public detail::DestinationTraversal {
public:
    ParallelDestinationTraversal(
        const ValidatedGraph& graph,
        const resources::PageRankResourcePolicy& policy
    ) noexcept
        : graph_(graph), policy_(policy) {}

    void write_candidate(
        const std::span<const std::uint32_t> out_degree,
        const std::span<const double> transformed_current,
        const double base,
        const double alpha,
        const std::span<double> candidate
    ) override {
        workspace().write_candidate(
            out_degree, transformed_current, base, alpha, candidate
        );
    }

    [[nodiscard]] std::optional<double> compute_true_residual(
        const std::span<const std::uint32_t> out_degree,
        const std::span<const double> candidate,
        const std::span<double> reusable_workspace,
        const PageRankConfig& config
    ) override {
        if (candidate.size() != out_degree.size()
            || reusable_workspace.size() != candidate.size()) {
            fail("residual received mismatched vertex spans");
        }

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

        workspace().write_residual_terms(
            out_degree,
            candidate,
            base,
            config.alpha,
            reusable_workspace
        );
        double residual_l1 = 0.0;
        for (const double term : reusable_workspace) {
            if (!std::isfinite(term) || term < 0.0) {
                return std::nullopt;
            }
            residual_l1 += term;
            if (!std::isfinite(residual_l1)) {
                return std::nullopt;
            }
        }
        return residual_l1;
    }

    [[nodiscard]] parallel::FixedExecutorConcurrencyEvidence
    concurrency_evidence() const {
        if (workspace_) {
            return workspace_->concurrency_evidence();
        }
        parallel::FixedExecutorConcurrencyEvidence evidence{};
        evidence.configured_worker_count = policy_.worker_count;
        return evidence;
    }

private:
    [[nodiscard]] ParallelTraversalWorkspace& workspace() {
        // Create worker memory after validation and out-degree loading; retain it across traversals.
        if (!workspace_) {
            workspace_ = std::make_unique<ParallelTraversalWorkspace>(
                graph_, policy_
            );
        }
        return *workspace_;
    }

    const ValidatedGraph& graph_;
    resources::PageRankResourcePolicy policy_;
    std::unique_ptr<ParallelTraversalWorkspace> workspace_{};
};

}  // namespace

PageRankResult detail::run_page_rank_on_opened_graph_parallel(
    const detail::OpenedPageRankGraph& opened,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer,
    detail::PageRankTraversalTelemetry* const telemetry
) {
    if (config.resources.worker_count == 0U) {
        throw std::invalid_argument(
            "parallel PageRank requires a positive worker count"
        );
    }
    ParallelDestinationTraversal traversal(opened.graph, config.resources);
    PageRankResult result = detail::run_page_rank_with_traversal(
        opened, config, traversal, observer, telemetry
    );
    if (telemetry != nullptr) {
        telemetry->internal_concurrency = traversal.concurrency_evidence();
    }
    return result;
}

PageRankResult run_page_rank_parallel(
    const std::filesystem::path& graph_directory,
    const PageRankConfig& config,
    const PageRankIterationObserver& observer
) {
    if (config.resources.worker_count == 0U) {
        throw std::invalid_argument(
            "parallel PageRank requires a positive worker count"
        );
    }
    const detail::OpenedPageRankGraph opened = detail::open_page_rank_graph(
        graph_directory, config
    );
    return detail::run_page_rank_on_opened_graph_parallel(
        opened, config, observer
    );
}

}  // namespace tbank::pagerank

#include "tbank/tasks/partitioner.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

namespace tbank::tasks {
namespace {

template <class Integer>
Integer checked_add(
    const Integer left,
    const Integer right,
    const char* const message
) {
    if (right > std::numeric_limits<Integer>::max() - left) {
        throw std::overflow_error(message);
    }
    return static_cast<Integer>(left + right);
}

std::uint32_t hub_slice_count(
    const std::uint32_t incoming_count,
    const std::uint64_t edge_slice_size
) {
    const std::uint64_t count = incoming_count;
    const std::uint64_t slices = count / edge_slice_size
        + static_cast<std::uint64_t>(count % edge_slice_size != 0U);
    if (slices == 0U
        || slices > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error("hub slice count is outside uint32 range");
    }
    return static_cast<std::uint32_t>(slices);
}

}  // namespace

void validate_task_config(const TaskConfig& config) {
    if (config.edge_slice_size == 0U) {
        throw std::invalid_argument("edge_slice_size must be positive");
    }
    if (config.max_task_edges == 0U) {
        throw std::invalid_argument("max_task_edges must be positive");
    }
    if (config.max_task_vertices == 0U) {
        throw std::invalid_argument("max_task_vertices must be positive");
    }
    if (config.edge_slice_size > config.max_task_edges) {
        throw std::invalid_argument(
            "edge_slice_size must not exceed max_task_edges"
        );
    }
}

class TaskPartitionBuilder::Impl final {
public:
    Impl(TaskConfig config, TaskConsumer consumer)
        : config_(config), consumer_(std::move(consumer)) {
        validate_task_config(config_);
        if (!consumer_) {
            throw std::invalid_argument("task consumer must not be empty");
        }
    }

    void consume(const std::uint32_t incoming_count) {
        ensure_usable();
        try {
            consume_one(incoming_count);
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

    void consume(const std::span<const std::uint32_t> incoming_counts) {
        ensure_usable();
        try {
            for (const std::uint32_t incoming_count : incoming_counts) {
                consume_one(incoming_count);
            }
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

    TaskPartitionSummary finish() {
        ensure_usable();
        try {
            flush_ordinary();
            state_ = State::finished;
            return summary();
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

private:
    enum class State {
        active,
        finished,
        failed,
    };

    void ensure_usable() {
        if (in_consumer_) {
            state_ = State::failed;
            throw std::logic_error("reentrant task partition builder call");
        }
        if (state_ == State::finished) {
            throw std::logic_error("task partition builder is already finished");
        }
        if (state_ == State::failed) {
            throw std::logic_error("task partition builder is poisoned");
        }
    }

    void consume_one(const std::uint32_t incoming_count) {
        if (destination_count_ == std::numeric_limits<std::uint32_t>::max()) {
            throw std::overflow_error(
                "destination count exceeds representable uint32 range"
            );
        }

        const std::uint64_t new_edge_count = checked_add(
            edge_count_,
            static_cast<std::uint64_t>(incoming_count),
            "total incoming edge count overflows uint64"
        );

        if (static_cast<std::uint64_t>(incoming_count)
            > config_.edge_slice_size) {
            flush_ordinary();
            emit_hub(incoming_count);
        } else {
            add_ordinary_destination(incoming_count);
        }

        edge_count_ = new_edge_count;
        ++destination_count_;
    }

    void add_ordinary_destination(const std::uint32_t incoming_count) {
        const std::uint64_t count = incoming_count;
        if (ordinary_.has_value()) {
            const bool vertex_limit_reached =
                ordinary_->dst_count >= config_.max_task_vertices;
            const bool edge_limit_exceeded =
                count > config_.max_task_edges - ordinary_->edge_count;
            if (vertex_limit_reached || edge_limit_exceeded) {
                flush_ordinary();
            }
        }

        if (!ordinary_.has_value()) {
            ordinary_ = OrdinaryTask{
                .dst_begin = destination_count_,
                .dst_count = 0U,
                .edge_begin = edge_count_,
                .edge_count = 0U,
            };
        }

        ordinary_->dst_count = checked_add(
            ordinary_->dst_count,
            1U,
            "ordinary destination count overflows uint32"
        );
        ordinary_->edge_count = checked_add(
            ordinary_->edge_count,
            count,
            "ordinary edge count overflows uint64"
        );
    }

    void emit_hub(const std::uint32_t incoming_count) {
        const std::uint32_t slice_count = hub_slice_count(
            incoming_count, config_.edge_slice_size
        );
        std::uint64_t emitted_edges = 0U;
        for (std::uint32_t slice_index = 0U;
             slice_index < slice_count;
             ++slice_index) {
            const std::uint64_t remaining =
                static_cast<std::uint64_t>(incoming_count) - emitted_edges;
            const std::uint64_t slice_edges =
                std::min(config_.edge_slice_size, remaining);
            emit(Task{HubSlice{
                .dst = destination_count_,
                .slice_index = slice_index,
                .slice_count = slice_count,
                .edge_begin = checked_add(
                    edge_count_,
                    emitted_edges,
                    "hub slice edge offset overflows uint64"
                ),
                .edge_count = slice_edges,
            }});
            emitted_edges = checked_add(
                emitted_edges,
                slice_edges,
                "hub slice coverage overflows uint64"
            );
        }
        if (emitted_edges != incoming_count) {
            throw std::logic_error("internal hub slice coverage mismatch");
        }
    }

    void flush_ordinary() {
        if (!ordinary_.has_value()) {
            return;
        }
        emit(Task{*ordinary_});
        ordinary_.reset();
    }

    void emit(Task task) {
        if (task_count_ == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error("task count overflows uint64");
        }

        in_consumer_ = true;
        try {
            consumer_(std::move(task));
        } catch (...) {
            in_consumer_ = false;
            state_ = State::failed;
            throw;
        }
        in_consumer_ = false;

        if (state_ != State::active) {
            throw std::logic_error(
                "task consumer made a reentrant builder call"
            );
        }
        ++task_count_;
    }

    [[nodiscard]] TaskPartitionSummary summary() const noexcept {
        return TaskPartitionSummary{
            .destination_count = destination_count_,
            .edge_count = edge_count_,
            .task_count = task_count_,
        };
    }

    TaskConfig config_;
    TaskConsumer consumer_;
    State state_{State::active};
    bool in_consumer_{false};
    std::optional<OrdinaryTask> ordinary_;
    std::uint32_t destination_count_{0U};
    std::uint64_t edge_count_{0U};
    std::uint64_t task_count_{0U};
};

TaskPartitionBuilder::TaskPartitionBuilder(
    const TaskConfig config,
    TaskConsumer consumer
)
    : impl_(std::make_unique<Impl>(config, std::move(consumer))) {}

TaskPartitionBuilder::~TaskPartitionBuilder() = default;

void TaskPartitionBuilder::consume(const std::uint32_t incoming_count) {
    impl_->consume(incoming_count);
}

void TaskPartitionBuilder::consume(
    const std::span<const std::uint32_t> incoming_counts
) {
    impl_->consume(incoming_counts);
}

TaskPartitionSummary TaskPartitionBuilder::finish() {
    return impl_->finish();
}

std::vector<Task> partition_tasks(
    const std::span<const std::uint32_t> incoming_counts,
    const TaskConfig config
) {
    if (incoming_counts.size()
        > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error(
            "destination count exceeds representable uint32 range"
        );
    }

    std::vector<Task> tasks;
    TaskPartitionBuilder builder(config, [&](Task task) {
        tasks.push_back(std::move(task));
    });
    builder.consume(incoming_counts);
    static_cast<void>(builder.finish());
    return tasks;
}

TaskValidationError::TaskValidationError(
    const std::uint64_t task_index,
    const char* const message
)
    : std::runtime_error(message), task_index_(task_index) {}

std::uint64_t TaskValidationError::task_index() const noexcept {
    return task_index_;
}

class TaskValidator::Impl final {
public:
    Impl(
        TaskConfig config,
        const std::uint32_t destination_count,
        IncomingCountReader count_reader
    )
        : config_(config),
          destination_count_(destination_count),
          count_reader_(std::move(count_reader)) {
        validate_task_config(config_);
        if (!count_reader_ && destination_count_ != 0U) {
            throw std::invalid_argument(
                "incoming-count reader must not be empty"
            );
        }
    }

    void consume(const Task& task) {
        ensure_usable();
        try {
            consume_one(task);
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

    void consume(const std::span<const Task> tasks) {
        ensure_usable();
        try {
            for (const Task& task : tasks) {
                consume_one(task);
            }
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

    TaskPartitionSummary finish() {
        ensure_usable();
        try {
            if (hub_.has_value()) {
                fail("hub destination is missing one or more slices");
            }
            if (next_destination_ != destination_count_) {
                fail("task directory does not cover every destination");
            }
            if (lookahead_.has_value()) {
                fail("internal count lookahead was not consumed");
            }
            state_ = State::finished;
            return TaskPartitionSummary{
                .destination_count = destination_count_,
                .edge_count = next_edge_,
                .task_count = task_count_,
            };
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

private:
    enum class State {
        active,
        finished,
        failed,
    };

    struct CountLookahead {
        std::uint32_t destination;
        std::uint32_t count;
    };

    struct ActiveHub {
        std::uint32_t destination;
        std::uint32_t slice_count;
        std::uint32_t next_slice;
        std::uint64_t total_edges;
        std::uint64_t covered_edges;
    };

    [[noreturn]] void fail(const char* const message) {
        state_ = State::failed;
        throw TaskValidationError(task_count_, message);
    }

    void ensure_usable() {
        if (in_count_reader_) {
            state_ = State::failed;
            throw std::logic_error("reentrant task validator call");
        }
        if (state_ == State::finished) {
            throw std::logic_error("task validator is already finished");
        }
        if (state_ == State::failed) {
            throw std::logic_error("task validator is poisoned");
        }
    }

    std::uint32_t read_count(const std::uint32_t destination) {
        if (destination >= destination_count_) {
            fail("task references destination outside graph");
        }

        in_count_reader_ = true;
        std::uint32_t count = 0U;
        try {
            count = count_reader_(destination);
        } catch (...) {
            in_count_reader_ = false;
            state_ = State::failed;
            throw;
        }
        in_count_reader_ = false;
        if (state_ != State::active) {
            throw std::logic_error(
                "incoming-count reader made a reentrant validator call"
            );
        }
        return count;
    }

    std::uint32_t take_count(const std::uint32_t destination) {
        if (lookahead_.has_value()) {
            if (lookahead_->destination != destination) {
                fail("incoming-count stream is not in destination order");
            }
            const std::uint32_t count = lookahead_->count;
            lookahead_.reset();
            return count;
        }
        return read_count(destination);
    }

    std::uint32_t peek_count(const std::uint32_t destination) {
        if (!lookahead_.has_value()) {
            lookahead_ = CountLookahead{
                .destination = destination,
                .count = read_count(destination),
            };
        } else if (lookahead_->destination != destination) {
            fail("incoming-count lookahead is not in destination order");
        }
        return lookahead_->count;
    }

    void consume_one(const Task& task) {
        if (task_count_ == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error("task count overflows uint64");
        }
        if (task.valueless_by_exception()) {
            fail("task record has no logical value");
        }

        if (const auto* const ordinary = std::get_if<OrdinaryTask>(&task)) {
            validate_ordinary(*ordinary);
        } else if (const auto* const hub = std::get_if<HubSlice>(&task)) {
            validate_hub(*hub);
        } else {
            fail("task record has an unknown logical kind");
        }
        ++task_count_;
    }

    void validate_ordinary(const OrdinaryTask& task) {
        if (hub_.has_value()) {
            fail("ordinary task interrupts ordered hub slices");
        }
        if (task.dst_count == 0U) {
            fail("ordinary task must cover at least one destination");
        }
        if (task.dst_begin != next_destination_) {
            fail("ordinary task destination range is out of order");
        }
        if (task.dst_count > config_.max_task_vertices) {
            fail("ordinary task exceeds max_task_vertices");
        }

        const std::uint64_t destination_end = checked_add(
            static_cast<std::uint64_t>(task.dst_begin),
            static_cast<std::uint64_t>(task.dst_count),
            "ordinary destination range overflows uint64"
        );
        if (destination_end > destination_count_) {
            fail("ordinary task destination range exceeds graph");
        }
        if (task.edge_begin != next_edge_) {
            fail("ordinary task edge range is out of order");
        }

        std::uint64_t expected_edges = 0U;
        for (std::uint64_t destination = task.dst_begin;
             destination < destination_end;
             ++destination) {
            const std::uint32_t count =
                take_count(static_cast<std::uint32_t>(destination));
            if (static_cast<std::uint64_t>(count)
                > config_.edge_slice_size) {
                fail("ordinary task contains a hub destination");
            }
            expected_edges = checked_add(
                expected_edges,
                static_cast<std::uint64_t>(count),
                "ordinary edge coverage overflows uint64"
            );
        }

        if (task.edge_count != expected_edges) {
            fail("ordinary edge range does not match incoming counts");
        }
        if (task.edge_count > config_.max_task_edges) {
            fail("ordinary task exceeds max_task_edges");
        }

        if (destination_end < destination_count_) {
            const std::uint32_t next_count = peek_count(
                static_cast<std::uint32_t>(destination_end)
            );
            const bool next_is_ordinary =
                static_cast<std::uint64_t>(next_count)
                <= config_.edge_slice_size;
            const bool vertex_has_room =
                task.dst_count < config_.max_task_vertices;
            const bool edge_has_room =
                static_cast<std::uint64_t>(next_count)
                <= config_.max_task_edges - expected_edges;
            if (next_is_ordinary && vertex_has_room && edge_has_room) {
                fail("ordinary task ended before canonical greedy boundary");
            }
        }

        next_destination_ = static_cast<std::uint32_t>(destination_end);
        next_edge_ = checked_add(
            next_edge_,
            expected_edges,
            "validated edge coverage overflows uint64"
        );
    }

    void validate_hub(const HubSlice& task) {
        if (!hub_.has_value()) {
            begin_hub(task);
        }

        if (task.dst != hub_->destination) {
            fail("hub slices are not contiguous for one destination");
        }
        if (task.slice_count != hub_->slice_count) {
            fail("hub slices disagree on slice_count");
        }
        if (task.slice_index != hub_->next_slice) {
            fail("hub slice indices are not consecutive");
        }

        const std::uint64_t expected_begin = checked_add(
            next_edge_,
            hub_->covered_edges,
            "hub slice edge offset overflows uint64"
        );
        if (task.edge_begin != expected_begin) {
            fail("hub slice edge range is out of order");
        }
        const std::uint64_t remaining =
            hub_->total_edges - hub_->covered_edges;
        const std::uint64_t expected_count =
            std::min(config_.edge_slice_size, remaining);
        if (task.edge_count != expected_count) {
            fail("hub slice edge_count is not the canonical slice size");
        }

        hub_->covered_edges = checked_add(
            hub_->covered_edges,
            task.edge_count,
            "hub slice coverage overflows uint64"
        );
        ++hub_->next_slice;

        if (hub_->next_slice == hub_->slice_count) {
            if (hub_->covered_edges != hub_->total_edges) {
                fail("hub slices do not exactly cover incoming edges");
            }
            next_edge_ = checked_add(
                next_edge_,
                hub_->total_edges,
                "validated edge coverage overflows uint64"
            );
            ++next_destination_;
            hub_.reset();
        }
    }

    void begin_hub(const HubSlice& task) {
        if (next_destination_ >= destination_count_) {
            fail("hub slice references destination outside graph");
        }
        if (task.dst != next_destination_) {
            fail("hub destination is out of order");
        }
        if (task.slice_index != 0U) {
            fail("first hub slice must have index zero");
        }

        const std::uint32_t incoming_count = take_count(next_destination_);
        if (static_cast<std::uint64_t>(incoming_count)
            <= config_.edge_slice_size) {
            fail("non-hub destination is represented by hub slices");
        }
        const std::uint32_t expected_slice_count = hub_slice_count(
            incoming_count, config_.edge_slice_size
        );
        if (task.slice_count != expected_slice_count) {
            fail("hub slice_count does not match incoming count");
        }

        hub_ = ActiveHub{
            .destination = task.dst,
            .slice_count = expected_slice_count,
            .next_slice = 0U,
            .total_edges = incoming_count,
            .covered_edges = 0U,
        };
    }

    TaskConfig config_;
    std::uint32_t destination_count_;
    IncomingCountReader count_reader_;
    State state_{State::active};
    bool in_count_reader_{false};
    std::optional<CountLookahead> lookahead_;
    std::optional<ActiveHub> hub_;
    std::uint32_t next_destination_{0U};
    std::uint64_t next_edge_{0U};
    std::uint64_t task_count_{0U};
};

TaskValidator::TaskValidator(
    const TaskConfig config,
    const std::uint32_t destination_count,
    IncomingCountReader count_reader
)
    : impl_(std::make_unique<Impl>(
          config, destination_count, std::move(count_reader)
      )) {}

TaskValidator::~TaskValidator() = default;

void TaskValidator::consume(const Task& task) {
    impl_->consume(task);
}

void TaskValidator::consume(const std::span<const Task> tasks) {
    impl_->consume(tasks);
}

TaskPartitionSummary TaskValidator::finish() {
    return impl_->finish();
}

TaskPartitionSummary validate_tasks(
    const std::span<const std::uint32_t> incoming_counts,
    const std::span<const Task> tasks,
    const TaskConfig config
) {
    if (incoming_counts.size()
        > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error(
            "destination count exceeds representable uint32 range"
        );
    }

    TaskValidator validator(
        config,
        static_cast<std::uint32_t>(incoming_counts.size()),
        [incoming_counts](const std::uint32_t destination) {
            return incoming_counts[destination];
        }
    );
    validator.consume(tasks);
    return validator.finish();
}

}  // namespace tbank::tasks

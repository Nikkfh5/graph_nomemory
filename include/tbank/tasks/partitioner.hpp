#pragma once

#include "tbank/tasks/task.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

namespace tbank::tasks {

// Throws std::invalid_argument when a value is zero or a slice cannot fit in
// an ordinary task's edge limit.
void validate_task_config(const TaskConfig& config);

struct TaskPartitionSummary {
    std::uint32_t destination_count;
    std::uint64_t edge_count;
    std::uint64_t task_count;

    friend bool operator==(
        const TaskPartitionSummary&,
        const TaskPartitionSummary&
    ) = default;
};

using TaskConsumer = std::function<void(Task)>;

// Streams deterministic ordinary tasks and ordered hub slices from incoming counts.
// Only the current task is retained; callback delivery is synchronous.
// Partition or callback failure permanently invalidates the builder.
class TaskPartitionBuilder final {
public:
    TaskPartitionBuilder(TaskConfig config, TaskConsumer consumer);
    ~TaskPartitionBuilder();

    TaskPartitionBuilder(TaskPartitionBuilder&&) = delete;
    TaskPartitionBuilder& operator=(TaskPartitionBuilder&&) = delete;
    TaskPartitionBuilder(const TaskPartitionBuilder&) = delete;
    TaskPartitionBuilder& operator=(const TaskPartitionBuilder&) = delete;

    void consume(std::uint32_t incoming_count);
    void consume(std::span<const std::uint32_t> incoming_counts);

    // Flushes the last ordinary task and makes the builder terminal. Empty
    // input is allowed and produces an all-zero summary.
    [[nodiscard]] TaskPartitionSummary finish();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::vector<Task> partition_tasks(
    std::span<const std::uint32_t> incoming_counts,
    TaskConfig config
);

class TaskValidationError final : public std::runtime_error {
public:
    TaskValidationError(std::uint64_t task_index, const char* message);

    // Zero-based index of the task being checked. For finish-time failures it
    // equals the number of tasks consumed.
    [[nodiscard]] std::uint64_t task_index() const noexcept;

private:
    std::uint64_t task_index_;
};

using IncomingCountReader =
    std::function<std::uint32_t(std::uint32_t destination)>;

// Validates exact coverage and canonical partitioning with bounded state.
class TaskValidator final {
public:
    TaskValidator(
        TaskConfig config,
        std::uint32_t destination_count,
        IncomingCountReader count_reader
    );
    ~TaskValidator();

    TaskValidator(TaskValidator&&) = delete;
    TaskValidator& operator=(TaskValidator&&) = delete;
    TaskValidator(const TaskValidator&) = delete;
    TaskValidator& operator=(const TaskValidator&) = delete;

    void consume(const Task& task);
    void consume(std::span<const Task> tasks);
    [[nodiscard]] TaskPartitionSummary finish();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] TaskPartitionSummary validate_tasks(
    std::span<const std::uint32_t> incoming_counts,
    std::span<const Task> tasks,
    TaskConfig config
);

}  // namespace tbank::tasks

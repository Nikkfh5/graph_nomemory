#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace tbank::resources {

inline constexpr std::uint64_t kPageRankHardMemoryBudgetBytes =
    128U * 1024U * 1024U;
inline constexpr std::uint64_t kPageRankRankBytesPerVertex = 8U;
inline constexpr std::uint64_t kPageRankDegreeBytesPerVertex = 4U;
inline constexpr std::uint64_t kPageRankHotBytesPerVertex = 20U;

static_assert(sizeof(double) == kPageRankRankBytesPerVertex);
static_assert(sizeof(std::uint32_t) == kPageRankDegreeBytesPerVertex);

// Stack and runtime reserves coexist with every phase.
struct PageRankResourcePolicy {
    std::size_t record_batch_records = 0U;
    std::size_t validation_io_chunk_bytes = 0U;
    std::uint64_t main_stack_bytes = 0U;
    std::uint64_t runtime_reserve_bytes = 0U;

    // Parallel fields are either all zero or all positive; stack sizes obey host pthread limits.
    std::uint64_t worker_count = 0U;
    std::uint64_t worker_stack_bytes = 0U;
    std::uint64_t worker_guard_bytes = 0U;
    std::size_t worker_count_batch_records = 0U;
    std::size_t worker_source_batch_records = 0U;
    std::size_t scheduler_window_records = 0U;

    friend bool operator==(
        const PageRankResourcePolicy&,
        const PageRankResourcePolicy&
    ) = default;
};

struct PageRankMemoryPlan {
    std::uint32_t vertex_count = 0U;
    std::uint64_t current_rank_bytes = 0U;
    std::uint64_t scratch_rank_bytes = 0U;
    std::uint64_t out_degree_bytes = 0U;
    std::uint64_t hot_vertex_payload_bytes = 0U;
    std::uint64_t validation_io_peak_bytes = 0U;
    std::uint64_t iteration_io_peak_bytes = 0U;
    std::uint64_t single_thread_io_peak_bytes = 0U;
    // Worker fields are totals across all workers.
    std::uint64_t worker_stack_bytes = 0U;
    std::uint64_t worker_guard_bytes = 0U;
    std::uint64_t worker_control_bytes = 0U;
    std::uint64_t worker_count_buffer_bytes = 0U;
    std::uint64_t worker_source_buffer_bytes = 0U;
    std::uint64_t worker_io_buffer_bytes = 0U;
    std::uint64_t worker_memory_bytes = 0U;
    std::uint64_t scheduler_window_bytes = 0U;
    std::uint64_t active_partials_bytes = 0U;
    std::uint64_t parallel_iteration_peak_bytes = 0U;
    std::uint64_t managed_phase_peak_bytes = 0U;
    std::uint64_t main_stack_bytes = 0U;
    std::uint64_t runtime_reserve_bytes = 0U;
    std::uint64_t required_bytes = 0U;
    std::uint64_t hard_memory_budget_bytes =
        kPageRankHardMemoryBudgetBytes;

    friend bool operator==(
        const PageRankMemoryPlan&,
        const PageRankMemoryPlan&
    ) = default;
};

class PageRankMemoryLimitError final : public std::runtime_error {
public:
    PageRankMemoryLimitError(
        std::uint64_t required_bytes,
        std::uint64_t budget_bytes,
        std::uint32_t vertex_count
    );

    [[nodiscard]] std::uint64_t required_bytes() const noexcept;
    [[nodiscard]] std::uint64_t budget_bytes() const noexcept;
    [[nodiscard]] std::uint32_t vertex_count() const noexcept;

private:
    std::uint64_t required_bytes_;
    std::uint64_t budget_bytes_;
    std::uint32_t vertex_count_;
};

// Rejects before O(V) allocation or edge traversal; diagnostics may allocate bounded memory.
[[nodiscard]] PageRankMemoryPlan page_rank_memory_plan(
    std::uint32_t vertex_count,
    const PageRankResourcePolicy& policy
);

}  // namespace tbank::resources

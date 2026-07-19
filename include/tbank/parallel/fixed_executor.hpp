#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace tbank::parallel {

namespace detail {
struct FixedExecutorClockFunctions;
}

struct FixedExecutorConfig {
    std::size_t worker_count{};
    std::size_t usable_stack_bytes{};
    std::size_t guard_bytes{};
};

struct FixedExecutorWorkerObservation {
    std::size_t worker_index{};
    std::size_t usable_stack_bytes{};
    std::size_t guard_bytes{};
};

enum class FixedExecutorConcurrencyStatus {
    valid,
    invalid,
    not_applicable,
};

enum class FixedExecutorConcurrencyFailure {
    none,
    wall_clock_unavailable,
    worker_clock_id_unavailable,
    worker_cpu_clock_unavailable,
    clock_resolution_unsupported,
    clock_regression,
    clock_model_violation,
    counter_overflow,
    snapshot_incomplete,
};

// Aggregate monotonic wall and worker CPU time for completed non-empty waves.
struct FixedExecutorConcurrencyEvidence {
    FixedExecutorConcurrencyStatus status =
        FixedExecutorConcurrencyStatus::invalid;
    FixedExecutorConcurrencyFailure failure =
        FixedExecutorConcurrencyFailure::snapshot_incomplete;
    std::uint64_t wall_clock_resolution_ns = 0U;
    std::uint64_t worker_cpu_clock_resolution_ns = 0U;
    std::uint64_t configured_worker_count = 0U;
    std::uint64_t wave_count = 0U;
    std::uint64_t claimed_job_count = 0U;
    std::uint64_t participating_worker_count = 0U;
    std::uint64_t positive_worker_cpu_count = 0U;
    std::uint64_t accounted_worker_cpu_ns = 0U;
    std::uint64_t executor_wall_ns = 0U;
    std::uint64_t quantization_guard_ns = 0U;

    friend bool operator==(
        const FixedExecutorConcurrencyEvidence&,
        const FixedExecutorConcurrencyEvidence&
    ) = default;
};

class FixedExecutorPoisonedError final : public std::logic_error {
public:
    FixedExecutorPoisonedError();
};

using ErasedIndexedJob = void (*)(
    void* context,
    std::size_t worker_index,
    std::size_t job_index
);

// Executor object payload for T workers; excludes stack/guard mappings and allocator metadata.
[[nodiscard]] std::uint64_t fixed_executor_control_bytes(
    std::uint64_t worker_count
);

// Fixed pthread pool with synchronous, non-overlapping waves and no work queue.
// A job failure poisons the pool, joins every worker and rethrows deterministically.
class FixedExecutor final {
public:
    explicit FixedExecutor(FixedExecutorConfig config);
    // Failure-injection clock table is copied; referenced context must outlive the executor.
    FixedExecutor(
        FixedExecutorConfig config,
        const detail::FixedExecutorClockFunctions& clock_functions
    );
    ~FixedExecutor() noexcept;

    FixedExecutor(const FixedExecutor&) = delete;
    FixedExecutor& operator=(const FixedExecutor&) = delete;
    FixedExecutor(FixedExecutor&&) = delete;
    FixedExecutor& operator=(FixedExecutor&&) = delete;

    [[nodiscard]] std::size_t worker_count() const noexcept;
    [[nodiscard]] bool healthy() const noexcept;

    // Valid for the executor lifetime and ordered by worker_index; construction audits every Linux
    // pthread stack and guard.
    [[nodiscard]] std::span<const FixedExecutorWorkerObservation>
    worker_observations() const noexcept;

    // Last committed evidence prefix; zero-job calls do not update it. Before the first non-empty
    // wave: invalid/snapshot_incomplete.
    [[nodiscard]] FixedExecutorConcurrencyEvidence concurrency_evidence()
        const;

    // Allocation-free synchronous entry; referenced state must outlive the call, and exceptions
    // never cross pthread entry.
    void run_indexed_erased(
        std::size_t job_count,
        void* context,
        ErasedIndexedJob callback
    );

    template <class Function>
        requires std::invocable<
            std::remove_reference_t<Function>&,
            std::size_t,
            std::size_t
        >
    void run_indexed(std::size_t job_count, Function&& function) {
        using Callable = std::remove_reference_t<Function>;
        struct Invocation final {
            Callable* callable;
        };

        Invocation invocation{std::addressof(function)};
        run_indexed_erased(
            job_count,
            std::addressof(invocation),
            [](void* const raw_context,
               const std::size_t worker_index,
               const std::size_t job_index) {
                auto& erased = *static_cast<Invocation*>(raw_context);
                static_cast<void>(
                    std::invoke(
                        *erased.callable,
                        worker_index,
                        job_index
                    )
                );
            }
        );
    }

private:
    friend std::uint64_t fixed_executor_control_bytes(std::uint64_t);

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace tbank::parallel

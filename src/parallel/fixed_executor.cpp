#include "tbank/parallel/fixed_executor.hpp"

#include "fixed_executor_clock.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <vector>

#include <pthread.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

namespace tbank::parallel {
namespace {

constexpr std::uint64_t kNanosecondsPerSecond = 1'000'000'000U;
constexpr std::uint64_t kMaximumClockResolutionNs = 1'000'000U;

enum class ClockReadResult {
    success,
    unavailable,
    unrepresentable,
};

[[nodiscard]] bool checked_add_u64(
    const std::uint64_t left,
    const std::uint64_t right,
    std::uint64_t& result
) noexcept {
    if (left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    result = left + right;
    return true;
}

[[nodiscard]] bool checked_multiply_u64(
    const std::uint64_t left,
    const std::uint64_t right,
    std::uint64_t& result
) noexcept {
    if (
        left != 0U
        && right > std::numeric_limits<std::uint64_t>::max() / left
    ) {
        return false;
    }
    result = left * right;
    return true;
}

[[nodiscard]] bool timespec_to_nanoseconds(
    const timespec& value,
    std::uint64_t& result
) noexcept {
    if (
        value.tv_sec < 0
        || value.tv_nsec < 0
        || value.tv_nsec >= static_cast<long>(kNanosecondsPerSecond)
    ) {
        return false;
    }
    const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
    std::uint64_t seconds_ns = 0U;
    if (!checked_multiply_u64(
            seconds, kNanosecondsPerSecond, seconds_ns
        )) {
        return false;
    }
    return checked_add_u64(
        seconds_ns, static_cast<std::uint64_t>(value.tv_nsec), result
    );
}

[[nodiscard]] ClockReadResult read_clock_nanoseconds(
    const detail::FixedExecutorClockFunctions& clock_functions,
    const clockid_t clock_id,
    std::uint64_t& result
) noexcept {
    timespec value{};
    if (
        clock_functions.get_time(
            clock_functions.context, clock_id, &value
        ) != 0
    ) {
        result = 0U;
        return ClockReadResult::unavailable;
    }
    if (!timespec_to_nanoseconds(value, result)) {
        result = 0U;
        return ClockReadResult::unrepresentable;
    }
    return ClockReadResult::success;
}

[[nodiscard]] ClockReadResult read_clock_resolution_nanoseconds(
    const detail::FixedExecutorClockFunctions& clock_functions,
    const clockid_t clock_id,
    std::uint64_t& result
) noexcept {
    timespec value{};
    if (
        clock_functions.get_resolution(
            clock_functions.context, clock_id, &value
        ) != 0
    ) {
        result = 0U;
        return ClockReadResult::unavailable;
    }
    if (!timespec_to_nanoseconds(value, result)) {
        result = 0U;
        return ClockReadResult::unrepresentable;
    }
    return ClockReadResult::success;
}

int system_get_time(
    void*,
    const clockid_t clock_id,
    timespec* const value
) noexcept {
    return ::clock_gettime(clock_id, value);
}

int system_get_resolution(
    void*,
    const clockid_t clock_id,
    timespec* const value
) noexcept {
    return ::clock_getres(clock_id, value);
}

int system_get_worker_clock_id(
    void*,
    const pthread_t thread,
    clockid_t* const clock_id
) noexcept {
    return ::pthread_getcpuclockid(thread, clock_id);
}

[[nodiscard]] const detail::FixedExecutorClockFunctions&
system_clock_functions() noexcept {
    static const detail::FixedExecutorClockFunctions functions{
        .context = nullptr,
        .get_time = &system_get_time,
        .get_resolution = &system_get_resolution,
        .get_worker_clock_id = &system_get_worker_clock_id,
    };
    return functions;
}

void validate_clock_functions(
    const detail::FixedExecutorClockFunctions& clock_functions
) {
    if (
        clock_functions.get_time == nullptr
        || clock_functions.get_resolution == nullptr
        || clock_functions.get_worker_clock_id == nullptr
    ) {
        throw std::invalid_argument(
            "fixed executor clock function table is incomplete"
        );
    }
}

[[nodiscard]] FixedExecutorConcurrencyFailure higher_priority_failure(
    const FixedExecutorConcurrencyFailure left,
    const FixedExecutorConcurrencyFailure right
) noexcept {
    if (left == FixedExecutorConcurrencyFailure::none) {
        return right;
    }
    if (right == FixedExecutorConcurrencyFailure::none) {
        return left;
    }
    return static_cast<unsigned int>(left)
            < static_cast<unsigned int>(right)
        ? left
        : right;
}

[[noreturn]] void throw_pthread_error(
    const int error_number,
    const char* const operation
) {
    throw std::system_error(
        error_number,
        std::generic_category(),
        operation
    );
}

[[nodiscard]] std::size_t system_page_size() {
    errno = 0;
    const long result = ::sysconf(_SC_PAGESIZE);
    if (result <= 0) {
        const int error_number = errno == 0 ? EINVAL : errno;
        throw std::system_error(
            error_number,
            std::generic_category(),
            "sysconf(_SC_PAGESIZE)"
        );
    }

    const auto unsigned_result = static_cast<unsigned long long>(result);
    if (unsigned_result > std::numeric_limits<std::size_t>::max()) {
        throw std::overflow_error("system page size is not representable");
    }
    return static_cast<std::size_t>(unsigned_result);
}

[[nodiscard]] std::size_t minimum_pthread_stack_size() {
    const auto result = static_cast<long long>(PTHREAD_STACK_MIN);
    if (result <= 0) {
        throw std::runtime_error("PTHREAD_STACK_MIN is not positive");
    }
    const auto unsigned_result = static_cast<unsigned long long>(result);
    if (unsigned_result > std::numeric_limits<std::size_t>::max()) {
        throw std::overflow_error("PTHREAD_STACK_MIN is not representable");
    }
    return static_cast<std::size_t>(unsigned_result);
}

void validate_config(const FixedExecutorConfig& config) {
#if !defined(__linux__) || !defined(__x86_64__)
    static_cast<void>(config);
    throw std::runtime_error(
        "owned guarded worker stacks require Linux x86-64"
    );
#else
    if (config.worker_count == 0U) {
        throw std::invalid_argument("worker_count must be positive");
    }

    const std::size_t page_size = system_page_size();
    const std::size_t minimum_stack = minimum_pthread_stack_size();
    if (config.usable_stack_bytes < minimum_stack) {
        throw std::invalid_argument(
            "usable_stack_bytes is smaller than PTHREAD_STACK_MIN"
        );
    }
    if (config.usable_stack_bytes % page_size != 0U) {
        throw std::invalid_argument(
            "usable_stack_bytes must be page-aligned"
        );
    }
    if (config.guard_bytes % page_size != 0U) {
        throw std::invalid_argument("guard_bytes must be page-aligned");
    }

    constexpr std::size_t kMaximum =
        std::numeric_limits<std::size_t>::max();
    if (config.usable_stack_bytes > kMaximum - config.guard_bytes) {
        throw std::invalid_argument(
            "usable stack plus guard size is not representable"
        );
    }
    const std::size_t per_worker_bytes =
        config.usable_stack_bytes + config.guard_bytes;
    if (config.worker_count > kMaximum / per_worker_bytes) {
        throw std::invalid_argument(
            "aggregate worker stack reservation is not representable"
        );
    }
#endif
}

class PthreadAttributes final {
public:
    PthreadAttributes() {
        const int error_number = ::pthread_attr_init(&attributes_);
        if (error_number != 0) {
            throw_pthread_error(error_number, "pthread_attr_init");
        }
        initialized_ = true;
    }

    ~PthreadAttributes() noexcept {
        if (initialized_) {
            static_cast<void>(::pthread_attr_destroy(&attributes_));
        }
    }

    PthreadAttributes(const PthreadAttributes&) = delete;
    PthreadAttributes& operator=(const PthreadAttributes&) = delete;

    [[nodiscard]] pthread_attr_t* get() noexcept {
        return &attributes_;
    }

    void destroy_checked() {
        const int error_number = ::pthread_attr_destroy(&attributes_);
        initialized_ = false;
        if (error_number != 0) {
            throw_pthread_error(error_number, "pthread_attr_destroy");
        }
    }

private:
    pthread_attr_t attributes_{};
    bool initialized_{false};
};

class OwnedStackMapping final {
public:
    OwnedStackMapping() = default;

    ~OwnedStackMapping() noexcept {
        release_noexcept();
    }

    OwnedStackMapping(const OwnedStackMapping&) = delete;
    OwnedStackMapping& operator=(const OwnedStackMapping&) = delete;
    OwnedStackMapping(OwnedStackMapping&&) = delete;
    OwnedStackMapping& operator=(OwnedStackMapping&&) = delete;

    void allocate(
        const std::size_t usable_stack_bytes,
        const std::size_t guard_bytes
    ) {
        if (allocated_) {
            throw std::logic_error("worker stack mapping already allocated");
        }
        if (
            usable_stack_bytes
            > std::numeric_limits<std::size_t>::max() - guard_bytes
        ) {
            throw std::overflow_error(
                "worker stack mapping size is not representable"
            );
        }

        const std::size_t mapping_bytes =
            usable_stack_bytes + guard_bytes;
        int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#if defined(MAP_STACK)
        flags |= MAP_STACK;
#endif
        void* const mapping = ::mmap(
            nullptr,
            mapping_bytes,
            PROT_READ | PROT_WRITE,
            flags,
            -1,
            0
        );
        if (mapping == MAP_FAILED) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "mmap(worker stack)"
            );
        }

        mapping_ = mapping;
        allocated_ = true;
        mapping_bytes_ = mapping_bytes;
        usable_stack_bytes_ = usable_stack_bytes;
        guard_bytes_ = guard_bytes;
        if (
            guard_bytes_ != 0U
            && ::mprotect(mapping_, guard_bytes_, PROT_NONE) != 0
        ) {
            const int error_number = errno;
            release_noexcept();
            throw std::system_error(
                error_number,
                std::generic_category(),
                "mprotect(worker stack guard)"
            );
        }
    }

    [[nodiscard]] void* stack_address() const noexcept {
        return static_cast<void*>(
            static_cast<std::byte*>(mapping_) + guard_bytes_
        );
    }

    [[nodiscard]] std::size_t usable_stack_bytes() const noexcept {
        return usable_stack_bytes_;
    }

    [[nodiscard]] std::size_t guard_bytes() const noexcept {
        return guard_bytes_;
    }

private:
    void release_noexcept() noexcept {
        if (!allocated_) {
            return;
        }
        if (::munmap(mapping_, mapping_bytes_) != 0) {
            std::terminate();
        }
        mapping_ = nullptr;
        allocated_ = false;
        mapping_bytes_ = 0U;
        usable_stack_bytes_ = 0U;
        guard_bytes_ = 0U;
    }

    void* mapping_{nullptr};
    bool allocated_{false};
    std::size_t mapping_bytes_{0U};
    std::size_t usable_stack_bytes_{0U};
    std::size_t guard_bytes_{0U};
};

[[nodiscard]] FixedExecutorWorkerObservation audit_current_worker(
    const FixedExecutorConfig& config,
    const std::size_t worker_index,
    void* const expected_stack_address
) {
#if !defined(__linux__)
    static_cast<void>(config);
    static_cast<void>(worker_index);
    static_cast<void>(expected_stack_address);
    throw std::runtime_error(
        "effective pthread stack audit requires Linux"
    );
#else
    PthreadAttributes attributes;
    // pthread_getattr_np initializes its output object itself. Destroy the
    // object initialized by PthreadAttributes before replacing it.
    attributes.destroy_checked();

    pthread_attr_t effective_attributes{};
    const int getattr_error =
        ::pthread_getattr_np(::pthread_self(), &effective_attributes);
    if (getattr_error != 0) {
        throw_pthread_error(getattr_error, "pthread_getattr_np");
    }

    bool effective_attributes_valid = true;
    try {
        void* stack_address = nullptr;
        std::size_t stack_size = 0U;
        int error_number = ::pthread_attr_getstack(
            &effective_attributes,
            &stack_address,
            &stack_size
        );
        if (error_number != 0) {
            throw_pthread_error(error_number, "pthread_attr_getstack");
        }
        if (stack_address == nullptr) {
            throw std::runtime_error(
                "pthread_attr_getstack returned a null stack address"
            );
        }

        std::size_t guard_size = 0U;
        error_number = ::pthread_attr_getguardsize(
            &effective_attributes,
            &guard_size
        );
        if (error_number != 0) {
            throw_pthread_error(error_number, "pthread_attr_getguardsize");
        }

        error_number = ::pthread_attr_destroy(&effective_attributes);
        effective_attributes_valid = false;
        if (error_number != 0) {
            throw_pthread_error(error_number, "pthread_attr_destroy");
        }

        if (stack_address != expected_stack_address) {
            throw std::runtime_error(
                "effective pthread stack address differs from owned mapping"
            );
        }
        if (stack_size != config.usable_stack_bytes) {
            throw std::runtime_error(
                "effective pthread stack size differs from owned mapping"
            );
        }
        if (guard_size != 0U) {
            throw std::runtime_error(
                "pthread installed an unexpected implicit stack guard"
            );
        }

        return FixedExecutorWorkerObservation{
            .worker_index = worker_index,
            .usable_stack_bytes = config.usable_stack_bytes,
            .guard_bytes = config.guard_bytes,
        };
    } catch (...) {
        if (effective_attributes_valid) {
            static_cast<void>(
                ::pthread_attr_destroy(&effective_attributes)
            );
        }
        throw;
    }
#endif
}

}  // namespace

class FixedExecutor::Impl final {
public:
    Impl(
        const FixedExecutorConfig config,
        const detail::FixedExecutorClockFunctions& clock_functions
    )
        : config_(config),
          clock_functions_(clock_functions),
          stack_mappings_(config.worker_count),
          threads_(config.worker_count),
          arguments_(config.worker_count),
          observations_(config.worker_count),
          failures_(config.worker_count),
          joined_(config.worker_count, 0U) {
        evidence_.configured_worker_count = config.worker_count;
        validate_control_capacities();
        for (std::size_t index = 0U; index < config_.worker_count; ++index) {
            arguments_[index] = WorkerArgument{
                .owner = this,
                .worker_index = index,
            };
        }

        try {
            create_workers();
            wait_until_ready();
            initialize_concurrency_clocks();
        } catch (...) {
            const std::exception_ptr primary_error =
                std::current_exception();
            request_stop_noexcept();
            if (join_workers_noexcept() != 0) {
                std::terminate();
            }
            std::rethrow_exception(primary_error);
        }
    }

    ~Impl() noexcept {
        request_stop_noexcept();
        if (join_workers_noexcept() != 0) {
            // Continuing destruction could free state still reachable by a
            // worker. There is no safe recovery path from pthread_join here.
            std::terminate();
        }
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    [[nodiscard]] std::size_t worker_count() const noexcept {
        return config_.worker_count;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::span<const FixedExecutorWorkerObservation>
    observations() const noexcept {
        return observations_;
    }

    [[nodiscard]] FixedExecutorConcurrencyEvidence
    concurrency_evidence() const {
        std::lock_guard lock(mutex_);
        return evidence_;
    }

    [[nodiscard]] static std::uint64_t control_bytes(
        std::uint64_t worker_count
    );

    void run_indexed_erased(
        const std::size_t job_count,
        void* const context,
        const ErasedIndexedJob callback
    ) {
        if (callback == nullptr) {
            throw std::invalid_argument("indexed job callback is null");
        }

        std::unique_lock lock(mutex_);
        if (infrastructure_error_) {
            const std::exception_ptr error = infrastructure_error_;
            stop_requested_ = true;
            healthy_.store(false, std::memory_order_release);
            lock.unlock();
            work_condition_.notify_all();
            join_workers_checked();
            std::rethrow_exception(error);
        }
        if (!healthy_.load(std::memory_order_acquire)) {
            throw FixedExecutorPoisonedError();
        }
        if (wave_active_) {
            throw std::logic_error(
                "run_indexed calls on one executor must not overlap"
            );
        }
        if (job_count == 0U) {
            return;
        }
        if (generation_ == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error("executor wave generation overflow");
        }

        for (FailureRecord& failure : failures_) {
            failure.job_index = std::numeric_limits<std::size_t>::max();
            failure.error = nullptr;
        }
        for (WorkerArgument& argument : arguments_) {
            argument.successful_jobs = 0U;
            argument.successful_jobs_overflow = false;
            argument.cpu_before_valid = false;
            argument.cpu_after_valid = false;
        }

        job_count_ = job_count;
        context_ = context;
        callback_ = callback;
        next_job_.store(0U, std::memory_order_relaxed);
        failure_cutoff_.store(job_count, std::memory_order_release);
        completed_workers_ = 0U;
        wave_active_ = true;

        const bool capture_concurrency =
            clock_preflight_valid_ && !telemetry_defect_sticky_;
        std::uint64_t wall_before_ns = 0U;
        bool wall_before_valid = false;
        FixedExecutorConcurrencyFailure capture_failure =
            FixedExecutorConcurrencyFailure::none;
        if (capture_concurrency) {
            const FixedExecutorConcurrencyFailure wall_failure =
                capture_wall_clock(wall_before_ns);
            wall_before_valid =
                wall_failure == FixedExecutorConcurrencyFailure::none;
            capture_failure = higher_priority_failure(
                capture_failure, wall_failure
            );
            if (
                wall_failure == FixedExecutorConcurrencyFailure::none
                && evidence_.wave_count > 0U
                && wall_before_ns < last_wall_after_ns_
            ) {
                capture_failure = higher_priority_failure(
                    capture_failure,
                    FixedExecutorConcurrencyFailure::clock_regression
                );
            }
            capture_failure = higher_priority_failure(
                capture_failure, capture_worker_clocks(true)
            );
        }
        ++generation_;

        work_condition_.notify_all();
        coordinator_condition_.wait(lock, [this] {
            return completed_workers_ == config_.worker_count
                || infrastructure_error_ != nullptr;
        });

        if (infrastructure_error_) {
            const std::exception_ptr error = infrastructure_error_;
            wave_active_ = false;
            stop_requested_ = true;
            healthy_.store(false, std::memory_order_release);
            lock.unlock();
            work_condition_.notify_all();
            join_workers_checked();
            std::rethrow_exception(error);
        }

        std::uint64_t wall_after_ns = 0U;
        bool wall_after_valid = false;
        if (capture_concurrency) {
            capture_failure = higher_priority_failure(
                capture_failure, capture_worker_clocks(false)
            );
            const FixedExecutorConcurrencyFailure wall_failure =
                capture_wall_clock(wall_after_ns);
            wall_after_valid =
                wall_failure == FixedExecutorConcurrencyFailure::none;
            capture_failure = higher_priority_failure(
                capture_failure, wall_failure
            );
        }

        std::size_t selected_index =
            std::numeric_limits<std::size_t>::max();
        std::exception_ptr selected_error;
        for (const FailureRecord& failure : failures_) {
            if (failure.error && failure.job_index < selected_index) {
                selected_index = failure.job_index;
                selected_error = failure.error;
            }
        }

        wave_active_ = false;
        if (!selected_error) {
            if (capture_concurrency) {
                capture_failure = commit_concurrency_wave(
                    job_count,
                    wall_before_ns,
                    wall_after_ns,
                    wall_before_valid,
                    wall_after_valid,
                    capture_failure
                );
                if (
                    capture_failure
                    != FixedExecutorConcurrencyFailure::none
                ) {
                    record_concurrency_failure(capture_failure);
                }
            }
            return;
        }

        stop_requested_ = true;
        healthy_.store(false, std::memory_order_release);
        lock.unlock();
        work_condition_.notify_all();
        join_workers_checked();
        std::rethrow_exception(selected_error);
    }

private:
    struct WorkerArgument final {
        Impl* owner{};
        std::size_t worker_index{};
        clockid_t cpu_clock_id{};
        std::uint64_t cpu_before_ns{};
        std::uint64_t cpu_after_ns{};
        std::uint64_t successful_jobs{};
        bool successful_jobs_overflow{false};
        bool cpu_before_valid{false};
        bool cpu_after_valid{false};
        bool ever_participated{false};
        bool ever_positive_cpu{false};
    };

    struct FailureRecord final {
        std::size_t job_index{std::numeric_limits<std::size_t>::max()};
        std::exception_ptr error;
    };

    void record_concurrency_failure(
        const FixedExecutorConcurrencyFailure failure
    ) noexcept {
        if (failure == FixedExecutorConcurrencyFailure::none) {
            return;
        }
        telemetry_defect_sticky_ = true;
        evidence_.status = FixedExecutorConcurrencyStatus::invalid;
        if (
            evidence_.failure
                == FixedExecutorConcurrencyFailure::snapshot_incomplete
            || evidence_.failure == FixedExecutorConcurrencyFailure::none
        ) {
            evidence_.failure = failure;
            return;
        }
        evidence_.failure = higher_priority_failure(
            evidence_.failure, failure
        );
    }

    void initialize_concurrency_clocks() noexcept {
        FixedExecutorConcurrencyFailure failure =
            FixedExecutorConcurrencyFailure::none;

        std::uint64_t wall_resolution = 0U;
        const ClockReadResult wall_result =
            read_clock_resolution_nanoseconds(
                clock_functions_, CLOCK_MONOTONIC, wall_resolution
            );
        if (wall_result == ClockReadResult::unavailable) {
            failure = higher_priority_failure(
                failure,
                FixedExecutorConcurrencyFailure::wall_clock_unavailable
            );
        } else if (wall_result == ClockReadResult::unrepresentable) {
            failure = higher_priority_failure(
                failure,
                FixedExecutorConcurrencyFailure::clock_resolution_unsupported
            );
        } else {
            evidence_.wall_clock_resolution_ns = wall_resolution;
            if (
                wall_resolution == 0U
                || wall_resolution > kMaximumClockResolutionNs
            ) {
                failure = higher_priority_failure(
                    failure,
                    FixedExecutorConcurrencyFailure::
                        clock_resolution_unsupported
                );
            }
        }

        bool complete_worker_resolution = true;
        std::uint64_t maximum_worker_resolution = 0U;
        for (std::size_t index = 0U; index < config_.worker_count; ++index) {
            clockid_t clock_id{};
            const int clock_id_error =
                clock_functions_.get_worker_clock_id(
                    clock_functions_.context,
                    threads_[index],
                    &clock_id
                );
            if (clock_id_error != 0) {
                complete_worker_resolution = false;
                failure = higher_priority_failure(
                    failure,
                    FixedExecutorConcurrencyFailure::
                        worker_clock_id_unavailable
                );
                continue;
            }
            arguments_[index].cpu_clock_id = clock_id;

            std::uint64_t resolution = 0U;
            const ClockReadResult resolution_result =
                read_clock_resolution_nanoseconds(
                    clock_functions_, clock_id, resolution
                );
            if (resolution_result == ClockReadResult::unavailable) {
                complete_worker_resolution = false;
                failure = higher_priority_failure(
                    failure,
                    FixedExecutorConcurrencyFailure::
                        worker_cpu_clock_unavailable
                );
                continue;
            }
            if (resolution_result == ClockReadResult::unrepresentable) {
                complete_worker_resolution = false;
                failure = higher_priority_failure(
                    failure,
                    FixedExecutorConcurrencyFailure::
                        clock_resolution_unsupported
                );
                continue;
            }
            maximum_worker_resolution = std::max(
                maximum_worker_resolution, resolution
            );
            if (
                resolution == 0U
                || resolution > kMaximumClockResolutionNs
            ) {
                failure = higher_priority_failure(
                    failure,
                    FixedExecutorConcurrencyFailure::
                        clock_resolution_unsupported
                );
            }
        }
        if (complete_worker_resolution) {
            evidence_.worker_cpu_clock_resolution_ns =
                maximum_worker_resolution;
        }

        if (failure != FixedExecutorConcurrencyFailure::none) {
            record_concurrency_failure(failure);
            return;
        }
        clock_preflight_valid_ = true;
    }

    [[nodiscard]] FixedExecutorConcurrencyFailure capture_wall_clock(
        std::uint64_t& result
    ) const noexcept {
        switch (read_clock_nanoseconds(
            clock_functions_, CLOCK_MONOTONIC, result
        )) {
            case ClockReadResult::success:
                return FixedExecutorConcurrencyFailure::none;
            case ClockReadResult::unavailable:
                return FixedExecutorConcurrencyFailure::
                    wall_clock_unavailable;
            case ClockReadResult::unrepresentable:
                return FixedExecutorConcurrencyFailure::counter_overflow;
        }
        return FixedExecutorConcurrencyFailure::snapshot_incomplete;
    }

    [[nodiscard]] FixedExecutorConcurrencyFailure capture_worker_clocks(
        const bool before_wave
    ) noexcept {
        FixedExecutorConcurrencyFailure failure =
            FixedExecutorConcurrencyFailure::none;
        for (WorkerArgument& argument : arguments_) {
            std::uint64_t value = 0U;
            const ClockReadResult result = read_clock_nanoseconds(
                clock_functions_, argument.cpu_clock_id, value
            );
            if (result == ClockReadResult::unavailable) {
                failure = higher_priority_failure(
                    failure,
                    FixedExecutorConcurrencyFailure::
                        worker_cpu_clock_unavailable
                );
                continue;
            }
            if (result == ClockReadResult::unrepresentable) {
                failure = higher_priority_failure(
                    failure,
                    FixedExecutorConcurrencyFailure::counter_overflow
                );
                continue;
            }
            if (before_wave) {
                if (
                    evidence_.wave_count > 0U
                    && value < argument.cpu_after_ns
                ) {
                    failure = higher_priority_failure(
                        failure,
                        FixedExecutorConcurrencyFailure::clock_regression
                    );
                }
                argument.cpu_before_ns = value;
                argument.cpu_before_valid = true;
            } else {
                argument.cpu_after_ns = value;
                argument.cpu_after_valid = true;
            }
        }
        return failure;
    }

    [[nodiscard]] bool compute_quantization_guard(
        const std::uint64_t wave_count,
        std::uint64_t& result
    ) const noexcept {
        std::uint64_t worker_resolution_sum = 0U;
        if (!checked_multiply_u64(
                evidence_.configured_worker_count,
                evidence_.worker_cpu_clock_resolution_ns,
                worker_resolution_sum
            )) {
            return false;
        }
        std::uint64_t per_wave = 0U;
        if (!checked_add_u64(
                worker_resolution_sum,
                evidence_.wall_clock_resolution_ns,
                per_wave
            )) {
            return false;
        }
        std::uint64_t all_waves = 0U;
        return checked_multiply_u64(wave_count, per_wave, all_waves)
            && checked_multiply_u64(2U, all_waves, result);
    }

    [[nodiscard]] FixedExecutorConcurrencyFailure commit_concurrency_wave(
        const std::size_t expected_job_count,
        const std::uint64_t wall_before_ns,
        const std::uint64_t wall_after_ns,
        const bool wall_before_valid,
        const bool wall_after_valid,
        const FixedExecutorConcurrencyFailure capture_failure
    ) noexcept {
        FixedExecutorConcurrencyFailure observed_failure = capture_failure;
        std::uint64_t wall_delta = 0U;
        const bool wall_pair_valid =
            wall_before_valid && wall_after_valid;
        if (wall_pair_valid && wall_after_ns < wall_before_ns) {
            observed_failure = higher_priority_failure(
                observed_failure,
                FixedExecutorConcurrencyFailure::clock_regression
            );
        } else if (wall_pair_valid) {
            wall_delta = wall_after_ns - wall_before_ns;
            if (wall_delta == 0U) {
                observed_failure = higher_priority_failure(
                    observed_failure,
                    FixedExecutorConcurrencyFailure::snapshot_incomplete
                );
            }
        }

        bool model_limit_available =
            wall_pair_valid && wall_after_ns >= wall_before_ns;
        std::uint64_t model_worker_guard = 0U;
        std::uint64_t model_wall_guard = 0U;
        if (
            !checked_multiply_u64(
                2U,
                evidence_.worker_cpu_clock_resolution_ns,
                model_worker_guard
            )
            || !checked_multiply_u64(
                2U,
                evidence_.wall_clock_resolution_ns,
                model_wall_guard
            )
        ) {
            model_limit_available = false;
            observed_failure = higher_priority_failure(
                observed_failure,
                FixedExecutorConcurrencyFailure::counter_overflow
            );
        }
        std::uint64_t model_allowance = 0U;
        if (
            model_limit_available
            && !checked_add_u64(
                model_worker_guard, model_wall_guard, model_allowance
            )
        ) {
            model_limit_available = false;
            observed_failure = higher_priority_failure(
                observed_failure,
                FixedExecutorConcurrencyFailure::counter_overflow
            );
        }
        std::uint64_t model_limit = 0U;
        if (
            model_limit_available
            && !checked_add_u64(
                wall_delta, model_allowance, model_limit
            )
        ) {
            model_limit_available = false;
            observed_failure = higher_priority_failure(
                observed_failure,
                FixedExecutorConcurrencyFailure::counter_overflow
            );
        }

        std::uint64_t completed_jobs = 0U;
        bool completed_jobs_representable = true;
        for (const WorkerArgument& argument : arguments_) {
            if (argument.successful_jobs_overflow) {
                observed_failure = higher_priority_failure(
                    observed_failure,
                    FixedExecutorConcurrencyFailure::counter_overflow
                );
            }
            if (
                completed_jobs_representable
                && !checked_add_u64(
                    completed_jobs,
                    argument.successful_jobs,
                    completed_jobs
                )
            ) {
                completed_jobs_representable = false;
                observed_failure = higher_priority_failure(
                    observed_failure,
                    FixedExecutorConcurrencyFailure::counter_overflow
                );
            }
            if (!argument.cpu_before_valid || !argument.cpu_after_valid) {
                continue;
            }
            if (argument.cpu_after_ns < argument.cpu_before_ns) {
                observed_failure = higher_priority_failure(
                    observed_failure,
                    FixedExecutorConcurrencyFailure::clock_regression
                );
                continue;
            }
            const std::uint64_t cpu_delta =
                argument.cpu_after_ns - argument.cpu_before_ns;
            if (model_limit_available && cpu_delta > model_limit) {
                observed_failure = higher_priority_failure(
                    observed_failure,
                    FixedExecutorConcurrencyFailure::clock_model_violation
                );
            }
        }
        if (
            completed_jobs_representable
            && static_cast<std::uint64_t>(expected_job_count)
                != completed_jobs
        ) {
            observed_failure = higher_priority_failure(
                observed_failure,
                FixedExecutorConcurrencyFailure::snapshot_incomplete
            );
        }
        if (observed_failure != FixedExecutorConcurrencyFailure::none) {
            return observed_failure;
        }

        FixedExecutorConcurrencyEvidence candidate = evidence_;
        candidate.status = FixedExecutorConcurrencyStatus::valid;
        candidate.failure = FixedExecutorConcurrencyFailure::none;
        if (!checked_add_u64(candidate.wave_count, 1U, candidate.wave_count)) {
            return FixedExecutorConcurrencyFailure::counter_overflow;
        }
        if (!checked_add_u64(
                candidate.executor_wall_ns,
                wall_delta,
                candidate.executor_wall_ns
            )) {
            return FixedExecutorConcurrencyFailure::counter_overflow;
        }

        for (const WorkerArgument& argument : arguments_) {
            const std::uint64_t cpu_delta =
                argument.cpu_after_ns - argument.cpu_before_ns;
            if (argument.successful_jobs == 0U) {
                continue;
            }
            if (!argument.ever_participated) {
                if (!checked_add_u64(
                        candidate.participating_worker_count,
                        1U,
                        candidate.participating_worker_count
                    )) {
                    return FixedExecutorConcurrencyFailure::counter_overflow;
                }
            }
            if (cpu_delta > 0U && !argument.ever_positive_cpu) {
                if (!checked_add_u64(
                        candidate.positive_worker_cpu_count,
                        1U,
                        candidate.positive_worker_cpu_count
                    )) {
                    return FixedExecutorConcurrencyFailure::counter_overflow;
                }
            }
            if (!checked_add_u64(
                    candidate.accounted_worker_cpu_ns,
                    cpu_delta,
                    candidate.accounted_worker_cpu_ns
                )) {
                return FixedExecutorConcurrencyFailure::counter_overflow;
            }
        }

        if (!checked_add_u64(
                candidate.claimed_job_count,
                completed_jobs,
                candidate.claimed_job_count
            )) {
            return FixedExecutorConcurrencyFailure::counter_overflow;
        }
        if (!compute_quantization_guard(
                candidate.wave_count, candidate.quantization_guard_ns
            )) {
            return FixedExecutorConcurrencyFailure::counter_overflow;
        }
        std::uint64_t overlap_threshold = 0U;
        if (!checked_add_u64(
                candidate.executor_wall_ns,
                candidate.quantization_guard_ns,
                overlap_threshold
            )) {
            return FixedExecutorConcurrencyFailure::counter_overflow;
        }
        static_cast<void>(overlap_threshold);
        if (
            candidate.participating_worker_count
                > candidate.configured_worker_count
            || candidate.positive_worker_cpu_count
                > candidate.participating_worker_count
            || candidate.claimed_job_count < candidate.wave_count
            || (candidate.accounted_worker_cpu_ns > 0U)
                != (candidate.positive_worker_cpu_count > 0U)
        ) {
            return FixedExecutorConcurrencyFailure::snapshot_incomplete;
        }

        evidence_ = candidate;
        last_wall_after_ns_ = wall_after_ns;
        for (WorkerArgument& argument : arguments_) {
            if (argument.successful_jobs == 0U) {
                continue;
            }
            argument.ever_participated = true;
            if (argument.cpu_after_ns > argument.cpu_before_ns) {
                argument.ever_positive_cpu = true;
            }
        }
        return FixedExecutorConcurrencyFailure::none;
    }

    void validate_control_capacities() const {
        // fixed_executor_control_bytes accounts exactly one element of each
        // owned O(T) array. Refuse a standard-library growth policy that
        // would reserve a larger hidden payload than the preflight formula.
        const std::size_t expected = config_.worker_count;
        if (
            stack_mappings_.capacity() != expected
            || threads_.capacity() != expected
            || arguments_.capacity() != expected
            || observations_.capacity() != expected
            || failures_.capacity() != expected
            || joined_.capacity() != expected
        ) {
            throw std::runtime_error(
                "executor control allocation capacity is not exact"
            );
        }
    }

    static void* thread_entry(void* const raw_argument) noexcept {
        auto& argument = *static_cast<WorkerArgument*>(raw_argument);
        argument.owner->worker_main(argument.worker_index);
        return nullptr;
    }

    void create_workers() {
        for (OwnedStackMapping& mapping : stack_mappings_) {
            mapping.allocate(
                config_.usable_stack_bytes,
                config_.guard_bytes
            );
        }

        PthreadAttributes attributes;

        int error_number = ::pthread_attr_setguardsize(
            attributes.get(),
            0U
        );
        if (error_number != 0) {
            throw_pthread_error(error_number, "pthread_attr_setguardsize");
        }

        for (std::size_t index = 0U; index < config_.worker_count; ++index) {
            error_number = ::pthread_attr_setstack(
                attributes.get(),
                stack_mappings_[index].stack_address(),
                stack_mappings_[index].usable_stack_bytes()
            );
            if (error_number != 0) {
                throw_pthread_error(error_number, "pthread_attr_setstack");
            }
            error_number = ::pthread_create(
                &threads_[index],
                attributes.get(),
                &Impl::thread_entry,
                &arguments_[index]
            );
            if (error_number != 0) {
                throw_pthread_error(error_number, "pthread_create");
            }
            ++created_workers_;

            std::lock_guard lock(mutex_);
            if (infrastructure_error_) {
                std::rethrow_exception(infrastructure_error_);
            }
        }

        attributes.destroy_checked();
    }

    void wait_until_ready() {
        std::unique_lock lock(mutex_);
        coordinator_condition_.wait(lock, [this] {
            return ready_workers_ == config_.worker_count
                || infrastructure_error_ != nullptr;
        });
        if (infrastructure_error_) {
            std::rethrow_exception(infrastructure_error_);
        }
    }

    void worker_main(const std::size_t worker_index) noexcept {
        bool reported_ready = false;
        try {
            const FixedExecutorWorkerObservation observation =
                audit_current_worker(
                    config_,
                    worker_index,
                    stack_mappings_[worker_index].stack_address()
                );

            std::unique_lock lock(mutex_);
            if (stop_requested_) {
                return;
            }
            observations_[worker_index] = observation;
            ++ready_workers_;
            reported_ready = true;
            coordinator_condition_.notify_all();

            std::uint64_t observed_generation = 0U;
            while (!stop_requested_) {
                work_condition_.wait(lock, [this, &observed_generation] {
                    return stop_requested_
                        || generation_ != observed_generation;
                });
                if (stop_requested_) {
                    return;
                }

                observed_generation = generation_;
                lock.unlock();
                execute_wave(worker_index);
                lock.lock();

                ++completed_workers_;
                coordinator_condition_.notify_all();
            }
        } catch (...) {
            report_infrastructure_failure(
                reported_ready,
                std::current_exception()
            );
        }
    }

    void execute_wave(const std::size_t worker_index) noexcept {
        std::uint64_t successful_jobs = 0U;
        bool successful_jobs_overflow = false;
        std::size_t job_index = 0U;
        while (claim_job(job_index)) {
            if (
                job_index
                >= failure_cutoff_.load(std::memory_order_acquire)
            ) {
                break;
            }

            try {
                callback_(context_, worker_index, job_index);
                if (
                    successful_jobs
                    == std::numeric_limits<std::uint64_t>::max()
                ) {
                    successful_jobs_overflow = true;
                } else {
                    ++successful_jobs;
                }
            } catch (...) {
                FailureRecord& failure = failures_[worker_index];
                failure.job_index = job_index;
                failure.error = std::current_exception();

                std::size_t cutoff =
                    failure_cutoff_.load(std::memory_order_relaxed);
                while (
                    job_index < cutoff
                    && !failure_cutoff_.compare_exchange_weak(
                        cutoff,
                        job_index,
                        std::memory_order_release,
                        std::memory_order_relaxed
                    )
                ) {
                }
                break;
            }
        }
        WorkerArgument& argument = arguments_[worker_index];
        argument.successful_jobs = successful_jobs;
        argument.successful_jobs_overflow = successful_jobs_overflow;
    }

    [[nodiscard]] bool claim_job(std::size_t& job_index) noexcept {
        std::size_t candidate = next_job_.load(std::memory_order_relaxed);
        for (;;) {
            const std::size_t cutoff =
                failure_cutoff_.load(std::memory_order_acquire);
            const std::size_t limit = std::min(job_count_, cutoff);
            if (candidate >= limit) {
                return false;
            }
            if (next_job_.compare_exchange_weak(
                    candidate,
                    candidate + 1U,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed
                )) {
                job_index = candidate;
                return true;
            }
        }
    }

    void report_infrastructure_failure(
        const bool reported_ready,
        const std::exception_ptr error
    ) noexcept {
        try {
            std::lock_guard lock(mutex_);
            if (!infrastructure_error_) {
                infrastructure_error_ = error;
            }
            static_cast<void>(reported_ready);
            stop_requested_ = true;
            healthy_.store(false, std::memory_order_release);
            coordinator_condition_.notify_all();
            work_condition_.notify_all();
        } catch (...) {
            std::terminate();
        }
    }

    void request_stop_noexcept() noexcept {
        try {
            {
                std::lock_guard lock(mutex_);
                stop_requested_ = true;
                healthy_.store(false, std::memory_order_release);
            }
            work_condition_.notify_all();
        } catch (...) {
            std::terminate();
        }
    }

    [[nodiscard]] int join_workers_noexcept() noexcept {
        int first_error = 0;
        for (std::size_t index = 0U; index < created_workers_; ++index) {
            if (joined_[index]) {
                continue;
            }
            const int error_number = ::pthread_join(threads_[index], nullptr);
            if (error_number == 0) {
                joined_[index] = 1U;
            } else if (first_error == 0) {
                first_error = error_number;
            }
        }
        return first_error;
    }

    void join_workers_checked() {
        const int error_number = join_workers_noexcept();
        if (error_number != 0) {
            std::terminate();
        }
    }

    FixedExecutorConfig config_;
    detail::FixedExecutorClockFunctions clock_functions_;
    std::vector<OwnedStackMapping> stack_mappings_;
    std::vector<pthread_t> threads_;
    std::vector<WorkerArgument> arguments_;
    std::vector<FixedExecutorWorkerObservation> observations_;
    std::vector<FailureRecord> failures_;
    std::vector<std::uint8_t> joined_;
    FixedExecutorConcurrencyEvidence evidence_{};

    mutable std::mutex mutex_;
    std::condition_variable work_condition_;
    std::condition_variable coordinator_condition_;
    std::atomic<std::size_t> next_job_{0U};
    std::atomic<std::size_t> failure_cutoff_{0U};
    std::atomic<bool> healthy_{true};

    std::size_t created_workers_{0U};
    std::size_t ready_workers_{0U};
    std::size_t completed_workers_{0U};
    std::uint64_t generation_{0U};
    std::size_t job_count_{0U};
    void* context_{nullptr};
    ErasedIndexedJob callback_{nullptr};
    bool wave_active_{false};
    bool stop_requested_{false};
    bool clock_preflight_valid_{false};
    bool telemetry_defect_sticky_{false};
    std::uint64_t last_wall_after_ns_{0U};
    std::exception_ptr infrastructure_error_;
};

std::uint64_t FixedExecutor::Impl::control_bytes(
    const std::uint64_t worker_count
) {
    if (worker_count == 0U) {
        return 0U;
    }
#if !defined(__linux__) || !defined(__x86_64__)
    throw std::runtime_error(
        "owned guarded worker stacks require Linux x86-64"
    );
#else

    constexpr std::uint64_t kPerWorkerBytes =
        sizeof(OwnedStackMapping)
        + sizeof(pthread_t)
        + sizeof(WorkerArgument)
        + sizeof(FixedExecutorWorkerObservation)
        + sizeof(FailureRecord)
        + sizeof(std::uint8_t);
    constexpr std::uint64_t kFixedBytes = sizeof(Impl);
    constexpr std::uint64_t kMaximum =
        std::numeric_limits<std::uint64_t>::max();
    if (worker_count > (kMaximum - kFixedBytes) / kPerWorkerBytes) {
        throw std::overflow_error(
            "fixed executor control payload is not representable"
        );
    }
    return kFixedBytes + worker_count * kPerWorkerBytes;
#endif
}

FixedExecutorPoisonedError::FixedExecutorPoisonedError()
    : std::logic_error("fixed executor is poisoned") {}

FixedExecutor::FixedExecutor(const FixedExecutorConfig config)
    : FixedExecutor(config, system_clock_functions()) {}

FixedExecutor::FixedExecutor(
    const FixedExecutorConfig config,
    const detail::FixedExecutorClockFunctions& clock_functions
) {
    validate_config(config);
    validate_clock_functions(clock_functions);
    impl_ = std::make_unique<Impl>(config, clock_functions);
}

FixedExecutor::~FixedExecutor() noexcept = default;

std::uint64_t fixed_executor_control_bytes(
    const std::uint64_t worker_count
) {
    return FixedExecutor::Impl::control_bytes(worker_count);
}

std::size_t FixedExecutor::worker_count() const noexcept {
    return impl_->worker_count();
}

bool FixedExecutor::healthy() const noexcept {
    return impl_->healthy();
}

std::span<const FixedExecutorWorkerObservation>
FixedExecutor::worker_observations() const noexcept {
    return impl_->observations();
}

FixedExecutorConcurrencyEvidence
FixedExecutor::concurrency_evidence() const {
    return impl_->concurrency_evidence();
}

void FixedExecutor::run_indexed_erased(
    const std::size_t job_count,
    void* const context,
    const ErasedIndexedJob callback
) {
    impl_->run_indexed_erased(job_count, context, callback);
}

}  // namespace tbank::parallel

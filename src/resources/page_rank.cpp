#include "tbank/resources/page_rank.hpp"

#include "tbank/parallel/fixed_executor.hpp"
#include "tbank/platform/checked_io.hpp"
#include "tbank/storage/binary.hpp"
#include "tbank/storage/file_reader.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <string>
#include <system_error>

#include <pthread.h>
#include <unistd.h>

namespace tbank::resources {
namespace {

inline constexpr std::uint64_t kValidationCursorCount = 2U;
inline constexpr std::uint64_t kIterationBytesPerBatchRecord =
    storage::kTaskRecordBytes + 2U * storage::kScalarRecordBytes;
inline constexpr std::uint64_t kActivePartialBytesPerTask = sizeof(double);

struct ParallelShape {
    std::uint64_t per_worker_count_buffer_bytes = 0U;
    std::uint64_t per_worker_source_buffer_bytes = 0U;
    std::uint64_t scheduler_window_bytes = 0U;
    std::uint64_t active_partials_bytes = 0U;
};

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) noexcept {
    static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));
    return static_cast<std::uint64_t>(value);
}

void require_size_t_representable(
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
}

[[nodiscard]] std::uint64_t checked_buffer_bytes(
    const std::size_t record_count,
    const std::uint64_t record_bytes,
    const char* const description
) {
    const std::uint64_t bytes = platform::checked_multiply(
        size_to_u64(record_count), record_bytes
    );
    require_size_t_representable(bytes, description);
    if (bytes % record_bytes != 0U) {
        throw std::logic_error(
            std::string(description) + " is not record-aligned"
        );
    }
    return bytes;
}

[[nodiscard]] std::uint64_t host_page_size_bytes() {
    errno = 0;
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0L) {
        throw std::system_error(
            errno == 0 ? EIO : errno,
            std::generic_category(),
            "sysconf(_SC_PAGESIZE)"
        );
    }
    return static_cast<std::uint64_t>(page_size);
}

[[nodiscard]] ParallelShape validate_policy(
    const PageRankResourcePolicy& policy
) {
    if (policy.record_batch_records == 0U) {
        throw std::invalid_argument(
            "PageRank record batch must be explicitly positive"
        );
    }
    if (policy.validation_io_chunk_bytes == 0U
        || policy.validation_io_chunk_bytes
            > storage::kMaximumCrcChunkBytes) {
        throw std::invalid_argument(
            "PageRank validation chunk must be in [1, 1 MiB]"
        );
    }
    if (policy.main_stack_bytes == 0U) {
        throw std::invalid_argument(
            "PageRank main stack inventory must be explicitly positive"
        );
    }
    if (policy.runtime_reserve_bytes == 0U) {
        throw std::invalid_argument(
            "PageRank runtime reserve must be explicitly positive"
        );
    }

    const bool has_parallel_claim = policy.worker_stack_bytes != 0U
        || policy.worker_guard_bytes != 0U
        || policy.worker_count_batch_records != 0U
        || policy.worker_source_batch_records != 0U
        || policy.scheduler_window_records != 0U;
    if (policy.worker_count == 0U && has_parallel_claim) {
        throw std::invalid_argument(
            "PageRank zero-worker policy has nonzero parallel inventory"
        );
    }
    if (policy.worker_count == 0U) {
        return {};
    }
    if (policy.worker_stack_bytes == 0U
        || policy.worker_guard_bytes == 0U
        || policy.worker_count_batch_records == 0U
        || policy.worker_source_batch_records == 0U
        || policy.scheduler_window_records == 0U) {
        throw std::invalid_argument(
            "PageRank workers require complete positive parallel inventory"
        );
    }

    require_size_t_representable(policy.worker_count, "worker count");
    require_size_t_representable(policy.worker_stack_bytes, "worker stack");
    require_size_t_representable(policy.worker_guard_bytes, "worker guard");

    const std::uint64_t page_size = host_page_size_bytes();
    const auto minimum_stack = static_cast<std::uint64_t>(PTHREAD_STACK_MIN);
    if (policy.worker_stack_bytes < minimum_stack) {
        throw std::invalid_argument(
            "PageRank worker stack is below PTHREAD_STACK_MIN"
        );
    }
    if (policy.worker_stack_bytes % page_size != 0U) {
        throw std::invalid_argument(
            "PageRank worker stack must be host-page-aligned"
        );
    }
    if (policy.worker_guard_bytes % page_size != 0U) {
        throw std::invalid_argument(
            "PageRank worker guard must be a host-page multiple"
        );
    }

    return ParallelShape{
        .per_worker_count_buffer_bytes = checked_buffer_bytes(
            policy.worker_count_batch_records,
            storage::kScalarRecordBytes,
            "worker count buffer"
        ),
        .per_worker_source_buffer_bytes = checked_buffer_bytes(
            policy.worker_source_batch_records,
            storage::kScalarRecordBytes,
            "worker source buffer"
        ),
        .scheduler_window_bytes = checked_buffer_bytes(
            policy.scheduler_window_records,
            storage::kTaskRecordBytes,
            "scheduler window"
        ),
        .active_partials_bytes = checked_buffer_bytes(
            policy.scheduler_window_records,
            kActivePartialBytesPerTask,
            "active partials"
        ),
    };
}

[[nodiscard]] std::uint64_t add_term(
    const std::uint64_t total,
    const std::uint64_t term
) {
    return platform::checked_add(total, term);
}

}  // namespace

PageRankMemoryLimitError::PageRankMemoryLimitError(
    const std::uint64_t required_bytes,
    const std::uint64_t budget_bytes,
    const std::uint32_t vertex_count
)
    : std::runtime_error(
          "PageRank preflight requires " + std::to_string(required_bytes)
          + " bytes but hard budget is " + std::to_string(budget_bytes)
      ),
      required_bytes_(required_bytes),
      budget_bytes_(budget_bytes),
      vertex_count_(vertex_count) {}

std::uint64_t PageRankMemoryLimitError::required_bytes() const noexcept {
    return required_bytes_;
}

std::uint64_t PageRankMemoryLimitError::budget_bytes() const noexcept {
    return budget_bytes_;
}

std::uint32_t PageRankMemoryLimitError::vertex_count() const noexcept {
    return vertex_count_;
}

PageRankMemoryPlan page_rank_memory_plan(
    const std::uint32_t vertex_count,
    const PageRankResourcePolicy& policy
) {
    const ParallelShape parallel = validate_policy(policy);
    if (vertex_count == 0U) {
        throw std::invalid_argument(
            "PageRank requires a nonempty vertex universe"
        );
    }

    const std::uint64_t vertices = vertex_count;
    const std::uint64_t current_rank_bytes = platform::checked_multiply(
        vertices, kPageRankRankBytesPerVertex
    );
    const std::uint64_t scratch_rank_bytes = current_rank_bytes;
    const std::uint64_t out_degree_bytes = platform::checked_multiply(
        vertices, kPageRankDegreeBytesPerVertex
    );
    const std::uint64_t hot_vertex_payload_bytes = platform::checked_multiply(
        vertices, kPageRankHotBytesPerVertex
    );
    const std::uint64_t validation_io_peak_bytes =
        platform::checked_multiply(
            size_to_u64(policy.validation_io_chunk_bytes),
            kValidationCursorCount
        );
    const std::uint64_t iteration_io_peak_bytes =
        platform::checked_multiply(
            size_to_u64(policy.record_batch_records),
            kIterationBytesPerBatchRecord
        );
    const std::uint64_t single_thread_io_peak_bytes = std::max(
        validation_io_peak_bytes, iteration_io_peak_bytes
    );
    const std::uint64_t worker_stack_bytes = platform::checked_multiply(
        policy.worker_count, policy.worker_stack_bytes
    );
    const std::uint64_t worker_guard_bytes = platform::checked_multiply(
        policy.worker_count, policy.worker_guard_bytes
    );
    const std::uint64_t worker_control_bytes =
        parallel::fixed_executor_control_bytes(policy.worker_count);
    const std::uint64_t worker_count_buffer_bytes =
        platform::checked_multiply(
            policy.worker_count, parallel.per_worker_count_buffer_bytes
        );
    const std::uint64_t worker_source_buffer_bytes =
        platform::checked_multiply(
            policy.worker_count, parallel.per_worker_source_buffer_bytes
        );
    const std::uint64_t worker_io_buffer_bytes = platform::checked_add(
        worker_count_buffer_bytes, worker_source_buffer_bytes
    );
    std::uint64_t worker_memory_bytes = platform::checked_add(
        worker_stack_bytes, worker_guard_bytes
    );
    worker_memory_bytes = platform::checked_add(
        worker_memory_bytes, worker_control_bytes
    );
    worker_memory_bytes = platform::checked_add(
        worker_memory_bytes, worker_io_buffer_bytes
    );
    std::uint64_t parallel_iteration_peak_bytes = platform::checked_add(
        worker_memory_bytes, parallel.scheduler_window_bytes
    );
    parallel_iteration_peak_bytes = platform::checked_add(
        parallel_iteration_peak_bytes, parallel.active_partials_bytes
    );
    // Phase-local workspaces combine by max; stack and reserve remain live throughout.
    const std::uint64_t managed_phase_peak_bytes = std::max(
        single_thread_io_peak_bytes, parallel_iteration_peak_bytes
    );

    std::uint64_t required_bytes = hot_vertex_payload_bytes;
    required_bytes = add_term(required_bytes, managed_phase_peak_bytes);
    required_bytes = add_term(required_bytes, policy.main_stack_bytes);
    required_bytes = add_term(required_bytes, policy.runtime_reserve_bytes);

    if (required_bytes > kPageRankHardMemoryBudgetBytes) {
        throw PageRankMemoryLimitError(
            required_bytes, kPageRankHardMemoryBudgetBytes, vertex_count
        );
    }

    return PageRankMemoryPlan{
        .vertex_count = vertex_count,
        .current_rank_bytes = current_rank_bytes,
        .scratch_rank_bytes = scratch_rank_bytes,
        .out_degree_bytes = out_degree_bytes,
        .hot_vertex_payload_bytes = hot_vertex_payload_bytes,
        .validation_io_peak_bytes = validation_io_peak_bytes,
        .iteration_io_peak_bytes = iteration_io_peak_bytes,
        .single_thread_io_peak_bytes = single_thread_io_peak_bytes,
        .worker_stack_bytes = worker_stack_bytes,
        .worker_guard_bytes = worker_guard_bytes,
        .worker_control_bytes = worker_control_bytes,
        .worker_count_buffer_bytes = worker_count_buffer_bytes,
        .worker_source_buffer_bytes = worker_source_buffer_bytes,
        .worker_io_buffer_bytes = worker_io_buffer_bytes,
        .worker_memory_bytes = worker_memory_bytes,
        .scheduler_window_bytes = parallel.scheduler_window_bytes,
        .active_partials_bytes = parallel.active_partials_bytes,
        .parallel_iteration_peak_bytes = parallel_iteration_peak_bytes,
        .managed_phase_peak_bytes = managed_phase_peak_bytes,
        .main_stack_bytes = policy.main_stack_bytes,
        .runtime_reserve_bytes = policy.runtime_reserve_bytes,
        .required_bytes = required_bytes,
        .hard_memory_budget_bytes = kPageRankHardMemoryBudgetBytes,
    };
}

}  // namespace tbank::resources

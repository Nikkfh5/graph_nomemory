#include "tbank/preprocess/ingest.hpp"

#include "tbank/io/edge_csv.hpp"
#include "tbank/platform/checked_io.hpp"
#include "tbank/preprocess/run_file.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <sys/stat.h>
#include <unistd.h>

namespace tbank::preprocess {
namespace {

constexpr std::string_view kRawEdgeRunName = "raw_edges.bin";
constexpr std::string_view kEndpointRunPrefix = "endpoint_ids.";
constexpr std::string_view kEndpointRunSuffix = ".bin";
constexpr std::size_t kRunIndexDigits = 20U;

static_assert(
    std::numeric_limits<std::size_t>::digits
        <= std::numeric_limits<std::uint64_t>::digits
);

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

[[nodiscard]] std::uint64_t size_to_u64(const std::size_t value) noexcept {
    return static_cast<std::uint64_t>(value);
}

}  // namespace

std::array<std::byte, kRawEdgeRunRecordBytes> encode_raw_edge_record(
    const RawEdgeRecord& record
) {
    std::array<std::byte, kRawEdgeRunRecordBytes> bytes{};
    storage::encode_i32_le(record.source, bytes, 0U);
    storage::encode_i32_le(record.destination, bytes, 4U);
    return bytes;
}

RawEdgeRecord decode_raw_edge_record(const std::span<const std::byte> bytes) {
    if (bytes.size() != kRawEdgeRunRecordBytes) {
        throw std::invalid_argument(
            "raw edge record must contain exactly 8 bytes"
        );
    }
    return RawEdgeRecord{
        .source = storage::decode_i32_le(bytes, 0U),
        .destination = storage::decode_i32_le(bytes, 4U),
    };
}

InitialRunMemoryPlan initial_run_memory_plan(
    const InitialRunConfig& config
) {
    if (config.endpoint_ids_per_run == 0U) {
        throw std::invalid_argument(
            "endpoint_ids_per_run must be positive"
        );
    }
    if (config.writer_buffer_bytes < kRawEdgeRunRecordBytes) {
        throw std::invalid_argument(
            "writer_buffer_bytes must hold one raw edge record"
        );
    }

    const std::uint64_t endpoint_buffer_bytes =
        platform::checked_multiply(
            size_to_u64(config.endpoint_ids_per_run),
            sizeof(std::int32_t)
        );
    const std::uint64_t writer_buffers_bytes = platform::checked_multiply(
        size_to_u64(config.writer_buffer_bytes),
        2U
    );
    return InitialRunMemoryPlan{
        .endpoint_buffer_bytes = endpoint_buffer_bytes,
        .writer_buffers_bytes = writer_buffers_bytes,
        .managed_peak_bytes = platform::checked_add(
            endpoint_buffer_bytes,
            writer_buffers_bytes
        ),
    };
}

std::filesystem::path raw_edge_run_path(
    const std::filesystem::path& workspace
) {
    return workspace / kRawEdgeRunName;
}

std::string endpoint_id_run_name(const std::uint64_t run_index) {
    std::array<char, kRunIndexDigits> digits{};
    std::array<char, kRunIndexDigits> unpadded{};
    const auto conversion = std::to_chars(
        unpadded.data(),
        unpadded.data() + unpadded.size(),
        run_index
    );
    if (conversion.ec != std::errc{}) {
        throw std::runtime_error("failed to format endpoint run index");
    }
    const std::size_t digit_count = static_cast<std::size_t>(
        conversion.ptr - unpadded.data()
    );
    if (digit_count > digits.size()) {
        throw std::overflow_error("endpoint run index exceeds filename width");
    }
    const std::size_t padding = digits.size() - digit_count;
    std::fill_n(digits.data(), padding, '0');
    std::copy_n(unpadded.data(), digit_count, digits.data() + padding);

    std::string name;
    name.reserve(
        kEndpointRunPrefix.size() + digits.size() + kEndpointRunSuffix.size()
    );
    name.append(kEndpointRunPrefix);
    name.append(digits.data(), digits.size());
    name.append(kEndpointRunSuffix);
    return name;
}

std::filesystem::path endpoint_id_run_path(
    const std::filesystem::path& workspace,
    const std::uint64_t run_index
) {
    return workspace / endpoint_id_run_name(run_index);
}

class InitialRunIngestor::Impl final {
public:
    Impl(std::filesystem::path workspace, const InitialRunConfig config)
        : workspace_(std::move(workspace)),
          config_(config) {
        static_cast<void>(initial_run_memory_plan(config_));
        if (contains_embedded_nul(workspace_)) {
            throw std::invalid_argument(
                "initial-run workspace path contains an embedded NUL"
            );
        }

        // Anchor cleanup before creating entries in case the working directory changes.
        workspace_ = std::filesystem::absolute(workspace_);

        endpoint_ids_ = std::make_unique<std::int32_t[]>(
            config_.endpoint_ids_per_run
        );
        parser_ = std::make_unique<io::EdgeCsvParser>(
            [this](const io::ParsedEdge edge) { accept_edge(edge); }
        );

        errno = 0;
        if (::mkdir(workspace_.c_str(), 0700) == -1) {
            const int error_number = errno;
            throw_posix_error(error_number, "create initial-run workspace");
        }
        owns_workspace_ = true;
        try {
            raw_writer_.emplace(RunFileWriter::create(
                raw_edge_run_path(workspace_),
                kRawEdgeRunMagic,
                kRawEdgeRunRecordBytes,
                config_.writer_buffer_bytes
            ));
        } catch (...) {
            abort_noexcept();
            throw;
        }
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    ~Impl() noexcept {
        if (state_ != State::finished) {
            abort_noexcept();
        }
    }

    void consume(const std::span<const char> csv_bytes) {
        ensure_active();
        try {
            parser_->consume(csv_bytes);
        } catch (...) {
            fail(std::current_exception());
            throw;
        }
    }

    [[nodiscard]] InitialRunResult finish() {
        ensure_active();
        try {
            const io::EdgeCsvSummary parser_summary = parser_->finish();
            parser_.reset();
            flush_endpoint_run();

            if (parser_summary.data_rows != summary_.raw_edge_count) {
                throw std::logic_error(
                    "parser and initial-run edge counts diverged"
                );
            }
            const std::uint64_t expected_endpoints =
                platform::checked_multiply(summary_.raw_edge_count, 2U);
            if (summary_.endpoint_ids_seen != expected_endpoints) {
                throw std::logic_error(
                    "initial-run endpoint count diverged from edge count"
                );
            }

            const RunFileInfo raw_info = raw_writer_->finish();
            raw_writer_.reset();
            if (raw_info.header.record_count != summary_.raw_edge_count) {
                throw std::logic_error(
                    "raw run header count diverged from ingest summary"
                );
            }
            telemetry_.raw_writer_peak_buffered_bytes =
                raw_info.peak_buffered_bytes;

            state_ = State::finished;
            owns_workspace_ = false;
            return InitialRunResult{
                .summary = summary_,
                .telemetry = telemetry_,
            };
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
            throw std::logic_error("initial-run ingest is already finished");
        }
        if (state_ != State::active || parser_ == nullptr
            || !raw_writer_.has_value() || !owns_workspace_) {
            throw std::logic_error("initial-run ingest has invalid state");
        }
    }

    void accept_edge(const io::ParsedEdge edge) {
        const std::uint64_t next_edge_count = platform::checked_add(
            summary_.raw_edge_count,
            1U
        );
        const std::uint64_t next_endpoint_count = platform::checked_add(
            summary_.endpoint_ids_seen,
            2U
        );

        const auto encoded = encode_raw_edge_record(RawEdgeRecord{
            .source = edge.source,
            .destination = edge.destination,
        });
        raw_writer_->append_records(encoded);
        append_endpoint(edge.source);
        append_endpoint(edge.destination);

        summary_.raw_edge_count = next_edge_count;
        summary_.endpoint_ids_seen = next_endpoint_count;
    }

    void append_endpoint(const std::int32_t endpoint) {
        if (endpoint_id_count_ >= config_.endpoint_ids_per_run) {
            throw std::logic_error("endpoint buffer exceeded its capacity");
        }
        endpoint_ids_[endpoint_id_count_] = endpoint;
        ++endpoint_id_count_;
        telemetry_.peak_endpoint_ids = std::max(
            telemetry_.peak_endpoint_ids,
            endpoint_id_count_
        );
        if (endpoint_id_count_ == config_.endpoint_ids_per_run) {
            flush_endpoint_run();
        }
    }

    void flush_endpoint_run() {
        if (endpoint_id_count_ == 0U) {
            return;
        }

        const std::span<std::int32_t> endpoints(
            endpoint_ids_.get(),
            endpoint_id_count_
        );
        std::sort(endpoints.begin(), endpoints.end());
        const auto unique_end = std::unique(
            endpoints.begin(),
            endpoints.end()
        );
        const std::size_t unique_count = static_cast<std::size_t>(
            unique_end - endpoints.begin()
        );
        const std::uint64_t next_run_count = platform::checked_add(
            summary_.endpoint_run_count,
            1U
        );
        const std::uint64_t next_run_records = platform::checked_add(
            summary_.endpoint_run_records,
            size_to_u64(unique_count)
        );

        RunFileWriter writer = RunFileWriter::create(
            endpoint_id_run_path(
                workspace_,
                summary_.endpoint_run_count
            ),
            kEndpointIdRunMagic,
            kEndpointIdRunRecordBytes,
            config_.writer_buffer_bytes
        );
        for (std::size_t index = 0U; index < unique_count; ++index) {
            std::array<std::byte, kEndpointIdRunRecordBytes> encoded{};
            storage::encode_i32_le(endpoint_ids_[index], encoded);
            writer.append_records(encoded);
        }
        const RunFileInfo info = writer.finish();
        completed_run_files_ = next_run_count;
        if (info.header.record_count != size_to_u64(unique_count)) {
            throw std::logic_error(
                "endpoint run header count diverged from local unique count"
            );
        }

        summary_.endpoint_run_count = next_run_count;
        summary_.endpoint_run_records = next_run_records;
        telemetry_.max_endpoint_writer_peak_buffered_bytes = std::max(
            telemetry_.max_endpoint_writer_peak_buffered_bytes,
            info.peak_buffered_bytes
        );
        endpoint_id_count_ = 0U;
    }

    void fail(std::exception_ptr failure) noexcept {
        if (state_ != State::failed) {
            failure_ = std::move(failure);
            state_ = State::failed;
        }
        abort_noexcept();
    }

    void abort_noexcept() noexcept {
        parser_.reset();
        raw_writer_.reset();
        if (!owns_workspace_) {
            return;
        }

        try {
            static_cast<void>(::unlink(raw_edge_run_path(workspace_).c_str()));
            for (std::uint64_t index = 0U;
                 index < completed_run_files_;
                 ++index) {
                static_cast<void>(
                    ::unlink(endpoint_id_run_path(workspace_, index).c_str())
                );
            }
            static_cast<void>(::rmdir(workspace_.c_str()));
        } catch (...) {
            // Preserve the primary failure if cleanup also fails.
        }
        owns_workspace_ = false;
    }

    std::filesystem::path workspace_;
    InitialRunConfig config_{};
    std::unique_ptr<std::int32_t[]> endpoint_ids_{};
    std::size_t endpoint_id_count_ = 0U;
    std::unique_ptr<io::EdgeCsvParser> parser_{};
    std::optional<RunFileWriter> raw_writer_{};
    InitialRunSummary summary_{};
    InitialRunTelemetry telemetry_{};
    std::uint64_t completed_run_files_ = 0U;
    State state_ = State::active;
    std::exception_ptr failure_{};
    bool owns_workspace_ = false;
};

InitialRunIngestor InitialRunIngestor::create(
    const std::filesystem::path& workspace,
    const InitialRunConfig config
) {
    return InitialRunIngestor(
        std::make_unique<Impl>(workspace, config)
    );
}

InitialRunIngestor::InitialRunIngestor(
    std::unique_ptr<Impl> impl
) noexcept
    : impl_(std::move(impl)) {}

InitialRunIngestor::InitialRunIngestor(InitialRunIngestor&&) noexcept =
    default;

InitialRunIngestor& InitialRunIngestor::operator=(
    InitialRunIngestor&&
) noexcept = default;

InitialRunIngestor::~InitialRunIngestor() = default;

void InitialRunIngestor::consume(const std::span<const char> csv_bytes) {
    if (impl_ == nullptr) {
        throw std::logic_error("initial-run ingestor was moved from");
    }
    impl_->consume(csv_bytes);
}

InitialRunResult InitialRunIngestor::finish() {
    if (impl_ == nullptr) {
        throw std::logic_error("initial-run ingestor was moved from");
    }
    return impl_->finish();
}

}  // namespace tbank::preprocess

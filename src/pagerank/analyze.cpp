#include "tbank/pagerank/analyze.hpp"

#include "engine.hpp"
#include "timing.hpp"
#include "tbank/storage/binary.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace tbank::pagerank {
namespace {

[[noreturn]] void fail(const char* const detail) {
    throw PageRankExecutionError(
        std::string("PageRank result emission: ") + detail
    );
}

void reject_output_inside_graph(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path
) {
    std::filesystem::path output_parent = output_path.parent_path();
    if (output_parent.empty()) {
        output_parent = ".";
    }
    std::error_code error;
    const bool same_directory = std::filesystem::equivalent(
        graph_directory, output_parent, error
    );
    if (!error && same_directory) {
        throw std::invalid_argument(
            "PageRank output must not be created inside the immutable graph "
            "generation"
        );
    }
}

void encode_verified_ranks(
    const storage::ValidatedGraph& graph,
    const std::span<const double> ranks,
    const std::size_t batch_records,
    const platform::PublicationByteSink& publication_sink,
    std::optional<io::RankCsvSummary>& summary
) {
    const storage::ValidatedBinaryFileReader& vertex_ids = graph.vertex_ids();
    const std::uint64_t vertex_count = graph.manifest().vertex_count;
    if (ranks.size() != vertex_count
        || vertex_ids.header().record_count != vertex_count
        || vertex_ids.header().record_bytes != storage::kScalarRecordBytes) {
        fail("rank and vertex-ID streams do not cover the same graph");
    }
    if (batch_records == 0U
        || batch_records > std::numeric_limits<std::size_t>::max()
            / storage::kScalarRecordBytes) {
        fail("invalid vertex-ID record batch size");
    }

    io::RankCsvEncoder encoder(
        [&](const std::span<const char> bytes) {
            publication_sink(std::as_bytes(bytes));
        }
    );
    encoder.begin();

    std::vector<std::byte> buffer(
        batch_records * storage::kScalarRecordBytes
    );
    std::uint64_t row = 0U;
    while (row < vertex_count) {
        const std::uint64_t selected_u64 = std::min(
            vertex_count - row,
            static_cast<std::uint64_t>(batch_records)
        );
        const std::size_t selected = static_cast<std::size_t>(selected_u64);
        const std::size_t selected_bytes =
            selected * storage::kScalarRecordBytes;
        vertex_ids.read_records(
            row, std::span<std::byte>(buffer.data(), selected_bytes)
        );
        for (std::size_t offset = 0U; offset < selected; ++offset) {
            const std::span<const std::byte> record(buffer);
            const std::int32_t vertex = storage::decode_i32_le(
                record.subspan(
                    offset * storage::kScalarRecordBytes,
                    storage::kScalarRecordBytes
                )
            );
            encoder.write(io::RankRow{
                .vertex = vertex,
                .rank = ranks[static_cast<std::size_t>(row) + offset],
            });
        }
        row += selected_u64;
    }

    const io::RankCsvSummary completed = encoder.finish();
    if (completed.rows != vertex_count || row != vertex_count) {
        fail("rank CSV row count differs from validated vertex count");
    }
    summary = completed;
}

template <class Publisher>
[[nodiscard]] AnalyzeResult analyze_opened_impl(
    const detail::OpenedPageRankGraph& opened,
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config,
    AnalyzeTelemetry telemetry,
    const detail::TelemetryTimePoint total_started,
    Publisher&& publisher
) {
    reject_output_inside_graph(graph_directory, output_path);
    detail::PageRankTraversalTelemetry traversal_telemetry{};
    const detail::TelemetryTimePoint engine_started =
        detail::TelemetryClock::now();
    PageRankResult page_rank = config.resources.worker_count == 0U
        ? detail::run_page_rank_on_opened_graph(
              opened, config, {}, &traversal_telemetry
          )
        : detail::run_page_rank_on_opened_graph_parallel(
              opened, config, {}, &traversal_telemetry
          );
    const detail::TelemetryTimePoint engine_finished =
        detail::TelemetryClock::now();
    telemetry.pagerank_engine_ns = detail::elapsed_nanoseconds(
        engine_started, engine_finished
    );
    telemetry.candidate_destination_traversal_ns =
        traversal_telemetry.candidate_destination_traversal_ns;
    telemetry.first_candidate_destination_traversal_ns =
        traversal_telemetry.first_candidate_destination_traversal_ns;
    telemetry.true_residual_destination_traversal_ns =
        traversal_telemetry.true_residual_destination_traversal_ns;
    telemetry.core_destination_traversal_ns =
        telemetry.candidate_destination_traversal_ns;
    detail::add_telemetry_value(
        telemetry.core_destination_traversal_ns,
        telemetry.true_residual_destination_traversal_ns
    );
    if (telemetry.core_destination_traversal_ns
        > telemetry.pagerank_engine_ns) {
        throw std::logic_error(
            "destination traversal telemetry exceeds PageRank engine time"
        );
    }
    telemetry.pagerank_engine_other_ns = telemetry.pagerank_engine_ns
        - telemetry.core_destination_traversal_ns;
    telemetry.candidate_destination_traversal_count =
        traversal_telemetry.candidate_destination_traversal_count;
    telemetry.true_residual_destination_traversal_count =
        traversal_telemetry.true_residual_destination_traversal_count;
    telemetry.internal_concurrency =
        traversal_telemetry.internal_concurrency;

    AnalyzeResult result{
        .status = AnalyzeStatus::non_converged,
        .pagerank_report = page_rank.report(),
        .csv_summary = std::nullopt,
        .publication = {},
        .telemetry = telemetry,
    };
    if (page_rank.report().status == PageRankStatus::numerical_failure) {
        result.status = AnalyzeStatus::numerical_failure;
        result.telemetry.total_ns = detail::elapsed_nanoseconds(
            total_started, detail::TelemetryClock::now()
        );
        return result;
    }
    if (page_rank.report().status == PageRankStatus::non_converged) {
        result.status = AnalyzeStatus::non_converged;
        result.telemetry.total_ns = detail::elapsed_nanoseconds(
            total_started, detail::TelemetryClock::now()
        );
        return result;
    }

    const std::span<const double> ranks = page_rank.verified_ranks();
    const platform::PublicationContentWriter writer =
        [&](const platform::PublicationByteSink& sink) {
            encode_verified_ranks(
                opened.graph,
                ranks,
                config.resources.record_batch_records,
                sink,
                result.csv_summary
            );
        };
    const detail::TelemetryTimePoint publication_started =
        detail::TelemetryClock::now();
    result.publication = std::forward<Publisher>(publisher)(output_path, writer);
    const detail::TelemetryTimePoint publication_finished =
        detail::TelemetryClock::now();
    result.telemetry.csv_publication_ns = detail::elapsed_nanoseconds(
        publication_started, publication_finished
    );
    switch (result.publication.state) {
        case platform::PublicationState::published:
            result.status = AnalyzeStatus::published;
            break;
        case platform::PublicationState::durability_uncertain:
            result.status = AnalyzeStatus::durability_uncertain;
            break;
        case platform::PublicationState::not_published:
        case platform::PublicationState::target_exists:
        case platform::PublicationState::no_replace_unsupported:
            result.status = AnalyzeStatus::publication_failed;
            break;
    }
    result.telemetry.total_ns = detail::elapsed_nanoseconds(
        total_started, detail::TelemetryClock::now()
    );
    return result;
}

template <class Publisher>
[[nodiscard]] AnalyzeResult analyze_impl(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config,
    Publisher&& publisher
) {
    const detail::TelemetryTimePoint total_started =
        detail::TelemetryClock::now();
    const detail::TelemetryTimePoint open_started = total_started;
    const detail::OpenedPageRankGraph opened = detail::open_page_rank_graph(
        graph_directory, config
    );
    const detail::TelemetryTimePoint open_finished =
        detail::TelemetryClock::now();
    AnalyzeTelemetry telemetry{};
    telemetry.graph_validation_open_preflight_ns =
        detail::elapsed_nanoseconds(open_started, open_finished);
    return analyze_opened_impl(
        opened,
        graph_directory,
        output_path,
        config,
        telemetry,
        total_started,
        std::forward<Publisher>(publisher)
    );
}

}  // namespace

AnalyzeResult detail::analyze_page_rank_to_csv_with_after_open_hook(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config,
    platform::PublicationBackend& publication_backend,
    const std::function<void()>& after_open
) {
    const detail::TelemetryTimePoint total_started =
        detail::TelemetryClock::now();
    const detail::TelemetryTimePoint open_started = total_started;
    const detail::OpenedPageRankGraph opened = detail::open_page_rank_graph(
        graph_directory, config
    );
    const detail::TelemetryTimePoint open_finished =
        detail::TelemetryClock::now();
    AnalyzeTelemetry telemetry{};
    telemetry.graph_validation_open_preflight_ns =
        detail::elapsed_nanoseconds(open_started, open_finished);
    reject_output_inside_graph(graph_directory, output_path);
    after_open();
    return analyze_opened_impl(
        opened,
        graph_directory,
        output_path,
        config,
        telemetry,
        total_started,
        [&](const std::filesystem::path& target,
            const platform::PublicationContentWriter& writer) {
            return platform::publish_file_no_replace(
                target, writer, publication_backend
            );
        }
    );
}

AnalyzeResult analyze_page_rank_to_csv(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config
) {
    return analyze_impl(
        graph_directory,
        output_path,
        config,
        [](const std::filesystem::path& target,
           const platform::PublicationContentWriter& writer) {
            return platform::publish_file_no_replace(target, writer);
        }
    );
}

AnalyzeResult analyze_page_rank_to_csv(
    const std::filesystem::path& graph_directory,
    const std::filesystem::path& output_path,
    const PageRankConfig& config,
    platform::PublicationBackend& publication_backend
) {
    return analyze_impl(
        graph_directory,
        output_path,
        config,
        [&](const std::filesystem::path& target,
            const platform::PublicationContentWriter& writer) {
            return platform::publish_file_no_replace(
                target, writer, publication_backend
            );
        }
    );
}

}  // namespace tbank::pagerank

#include "cli_common.hpp"

#include "tbank/storage/binary.hpp"
#include "tbank/storage/graph.hpp"
#include "tbank/storage/manifest.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace {

using tbank::cli::ParsedOptions;
using tbank::cli::UsageError;
using tbank::storage::TaskRecord;
using tbank::storage::TaskTag;

constexpr std::array<std::string_view, 2U> kAllowedOptions{
    "graph-dir",
    "validation-chunk-bytes",
};

constexpr std::string_view kHelp =
    "Usage: tbank-inspect-tasks [options]\n"
    "\n"
    "Required:\n"
    "  --graph-dir PATH\n"
    "  --validation-chunk-bytes UINT\n"
    "\n"
    "Validates the complete counted-graph generation, then emits one\n"
    "canonical JSON line describing physical ordinary tasks and hub slices.\n"
    "The scan is bounded by validation-chunk-bytes and retains no task list.\n";

class ResultWriter final {
public:
    ResultWriter() {
        buffer_.reserve(kFlushBytes);
    }

    void append(const std::string_view value) {
        if (value.size() >= kFlushBytes) {
            flush();
            tbank::cli::write_stdout(value);
            return;
        }
        if (buffer_.size() > kFlushBytes - value.size()) {
            flush();
        }
        buffer_.append(value);
    }

    void append_u64(const std::uint64_t value) {
        std::array<char, std::numeric_limits<std::uint64_t>::digits10 + 1U>
            encoded{};
        const auto result = std::to_chars(
            encoded.data(), encoded.data() + encoded.size(), value
        );
        if (result.ec != std::errc{}) {
            throw std::logic_error("cannot encode uint64 JSON value");
        }
        append(std::string_view(
            encoded.data(), static_cast<std::size_t>(result.ptr - encoded.data())
        ));
    }

    void append_crc64(const std::uint64_t value) {
        constexpr std::string_view digits = "0123456789abcdef";
        std::array<char, 16U> encoded{};
        for (std::size_t index = 0U; index < encoded.size(); ++index) {
            const std::size_t shift = (encoded.size() - index - 1U) * 4U;
            const auto nibble = static_cast<std::size_t>(
                (value >> shift) & 0x0fU
            );
            encoded[index] = digits[nibble];
        }
        append(std::string_view(encoded.data(), encoded.size()));
    }

    void finish() {
        append("\n");
        flush();
    }

private:
    void flush() {
        if (!buffer_.empty()) {
            tbank::cli::write_stdout(buffer_);
            buffer_.clear();
        }
    }

    static constexpr std::size_t kFlushBytes = 64U * 1024U;
    std::string buffer_;
};

struct InspectionCounters {
    std::uint64_t ordinary_records = 0U;
    std::uint64_t hub_slice_records = 0U;
    std::uint64_t hub_destinations = 0U;
    std::uint64_t covered_edges = 0U;
};

struct ActiveHub {
    bool active = false;
    std::uint32_t destination = 0U;
    std::uint32_t next_slice = 0U;
    std::uint32_t slice_count = 0U;
    std::uint64_t edge_begin = 0U;
    std::uint64_t incoming_edges = 0U;
};

void checked_add(
    std::uint64_t& total,
    const std::uint64_t increment,
    const std::string_view field
) {
    if (total > std::numeric_limits<std::uint64_t>::max() - increment) {
        throw std::logic_error(std::string(field) + " overflows uint64");
    }
    total += increment;
}

void append_graph_prefix(
    ResultWriter& output,
    const tbank::storage::GraphManifest& manifest
) {
    output.append(
        "{\"schema\":\"TBANK_TASK_INSPECTION_V1\",\"graph\":{"
        "\"vertex_count\":"
    );
    output.append_u64(manifest.vertex_count);
    output.append(",\"edge_count\":");
    output.append_u64(manifest.edge_count);
    output.append(",\"edge_slice_size\":");
    output.append_u64(manifest.edge_slice_size);
    output.append(",\"max_task_edges\":");
    output.append_u64(manifest.max_task_edges);
    output.append(",\"max_task_vertices\":");
    output.append_u64(manifest.max_task_vertices);
    output.append("},\"hubs\":[");
}

void begin_hub(
    ResultWriter& output,
    ActiveHub& hub,
    InspectionCounters& counters,
    const TaskRecord& record
) {
    if (hub.active || record.b != 0U) {
        throw std::logic_error(
            "validated task stream starts a malformed hub group"
        );
    }
    if (counters.hub_destinations != 0U) {
        output.append(",");
    }
    output.append("{\"destination\":");
    output.append_u64(record.a);
    output.append(",\"edge_begin\":");
    output.append_u64(record.edge_begin);
    output.append(",\"slice_count\":");
    output.append_u64(record.c);
    output.append(",\"slices\":[");

    hub = ActiveHub{
        .active = true,
        .destination = record.a,
        .next_slice = 0U,
        .slice_count = record.c,
        .edge_begin = record.edge_begin,
        .incoming_edges = 0U,
    };
    checked_add(counters.hub_destinations, 1U, "hub_destinations");
}

void consume_hub_slice(
    ResultWriter& output,
    ActiveHub& hub,
    InspectionCounters& counters,
    const TaskRecord& record
) {
    if (!hub.active) {
        begin_hub(output, hub, counters, record);
    }
    if (record.a != hub.destination || record.b != hub.next_slice
        || record.c != hub.slice_count) {
        throw std::logic_error(
            "validated task stream changed active hub geometry"
        );
    }
    if (hub.edge_begin > std::numeric_limits<std::uint64_t>::max()
            - hub.incoming_edges
        || record.edge_begin != hub.edge_begin + hub.incoming_edges) {
        throw std::logic_error("validated hub slices are not edge-contiguous");
    }
    if (hub.next_slice != 0U) {
        output.append(",");
    }
    output.append("{\"index\":");
    output.append_u64(record.b);
    output.append(",\"edge_begin\":");
    output.append_u64(record.edge_begin);
    output.append(",\"edge_count\":");
    output.append_u64(record.edge_count);
    output.append("}");

    checked_add(hub.incoming_edges, record.edge_count, "hub incoming_edges");
    checked_add(counters.covered_edges, record.edge_count, "covered_edges");
    checked_add(counters.hub_slice_records, 1U, "hub_slice_records");
    ++hub.next_slice;

    if (hub.next_slice == hub.slice_count) {
        output.append("],\"incoming_edges\":");
        output.append_u64(hub.incoming_edges);
        output.append("}");
        hub.active = false;
    }
}

void consume_record(
    ResultWriter& output,
    ActiveHub& hub,
    InspectionCounters& counters,
    const TaskRecord& record
) {
    if (record.tag == TaskTag::hub_slice) {
        consume_hub_slice(output, hub, counters, record);
        return;
    }
    if (record.tag != TaskTag::ordinary) {
        throw std::logic_error(
            "validated task stream contains an unknown tag"
        );
    }
    if (hub.active) {
        throw std::logic_error(
            "validated task stream interrupts an active hub"
        );
    }
    checked_add(counters.covered_edges, record.edge_count, "covered_edges");
    checked_add(counters.ordinary_records, 1U, "ordinary_records");
}

InspectionCounters inspect_tasks(
    ResultWriter& output,
    const tbank::storage::ValidatedBinaryFileReader& tasks,
    const std::size_t validation_chunk_bytes
) {
    const auto header = tasks.header();
    const std::size_t batch_records = std::max<std::size_t>(
        1U, validation_chunk_bytes / tbank::storage::kTaskRecordBytes
    );
    std::vector<std::byte> encoded(
        batch_records * tbank::storage::kTaskRecordBytes
    );

    InspectionCounters counters{};
    ActiveHub hub{};
    std::uint64_t record_begin = 0U;
    while (record_begin < header.record_count) {
        const std::uint64_t remaining = header.record_count - record_begin;
        const std::size_t current_records = static_cast<std::size_t>(
            std::min<std::uint64_t>(remaining, batch_records)
        );
        const std::size_t current_bytes =
            current_records * tbank::storage::kTaskRecordBytes;
        tasks.read_records(
            record_begin,
            std::span<std::byte>(encoded).first(current_bytes)
        );
        for (std::size_t index = 0U; index < current_records; ++index) {
            const std::size_t offset =
                index * tbank::storage::kTaskRecordBytes;
            consume_record(
                output,
                hub,
                counters,
                tbank::storage::decode_task_record(
                    std::span<const std::byte>(encoded).subspan(
                        offset, tbank::storage::kTaskRecordBytes
                    )
                )
            );
        }
        record_begin += current_records;
    }
    if (hub.active) {
        throw std::logic_error(
            "validated task stream ends inside a hub group"
        );
    }
    return counters;
}

void append_task_summary(
    ResultWriter& output,
    const tbank::storage::ValidatedBinaryFileReader& tasks,
    const InspectionCounters& counters,
    const std::uint64_t expected_edges
) {
    const auto header = tasks.header();
    if (counters.covered_edges != expected_edges) {
        throw std::logic_error(
            "inspected tasks do not cover manifest edge_count"
        );
    }
    if (counters.ordinary_records
        > std::numeric_limits<std::uint64_t>::max()
            - counters.hub_slice_records
        || counters.ordinary_records + counters.hub_slice_records
            != header.record_count) {
        throw std::logic_error("inspected task record count changed");
    }

    output.append("],\"tasks\":{\"file_bytes\":");
    output.append_u64(tasks.file_size());
    output.append(",\"record_count\":");
    output.append_u64(header.record_count);
    output.append(",\"payload_crc64\":\"");
    output.append_crc64(header.payload_crc64);
    output.append("\",\"ordinary_records\":");
    output.append_u64(counters.ordinary_records);
    output.append(",\"hub_slice_records\":");
    output.append_u64(counters.hub_slice_records);
    output.append(",\"hub_destinations\":");
    output.append_u64(counters.hub_destinations);
    output.append(",\"covered_edges\":");
    output.append_u64(counters.covered_edges);
    output.append("}}");
}

int run(const ParsedOptions& options) {
    const std::string_view graph_path = options.require("graph-dir");
    tbank::cli::require_valid_utf8("graph-dir", graph_path);
    const std::size_t validation_chunk_bytes = tbank::cli::parse_size(
        "validation-chunk-bytes", options.require("validation-chunk-bytes")
    );
    if (validation_chunk_bytes == 0U) {
        throw UsageError(
            "option --validation-chunk-bytes: value must be positive"
        );
    }
    if (validation_chunk_bytes
        > tbank::storage::kMaximumCrcChunkBytes) {
        throw UsageError(
            "option --validation-chunk-bytes: value exceeds the 1 MiB limit"
        );
    }

    const tbank::storage::ValidatedGraph graph =
        tbank::storage::ValidatedGraph::open(
            std::filesystem::path(graph_path),
            {.io_chunk_bytes = validation_chunk_bytes}
        );

    ResultWriter output;
    append_graph_prefix(output, graph.manifest());
    const InspectionCounters counters = inspect_tasks(
        output, graph.tasks(), validation_chunk_bytes
    );
    append_task_summary(
        output, graph.tasks(), counters, graph.manifest().edge_count
    );
    output.finish();
    return tbank::cli::kExitSuccess;
}

}  // namespace

int main(const int argument_count, char* arguments[]) {
    try {
        tbank::cli::initialize_result_channels();
        if (tbank::cli::is_help_request(argument_count, arguments)) {
            tbank::cli::write_stdout(kHelp);
            return tbank::cli::kExitSuccess;
        }
        const ParsedOptions options = ParsedOptions::parse(
            argument_count, arguments, kAllowedOptions
        );
        return run(options);
    } catch (const UsageError& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "usage", "invalid_arguments", error.what()
        );
        return tbank::cli::kExitUsage;
    } catch (const std::bad_alloc& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "resource", "allocation_failed", error.what()
        );
        return tbank::cli::kExitResource;
    } catch (const tbank::storage::GraphValidationError& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "data", "invalid_graph", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const tbank::storage::ManifestError& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "data", "invalid_graph", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const tbank::storage::BinaryError& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "data", "invalid_graph", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const std::system_error& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "system", "system_error", error.what()
        );
        return tbank::cli::kExitSystem;
    } catch (const std::invalid_argument& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "data", "invalid_data", error.what()
        );
        return tbank::cli::kExitData;
    } catch (const std::logic_error& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "internal", "invariant_failure", error.what()
        );
        return tbank::cli::kExitInternal;
    } catch (const std::exception& error) {
        tbank::cli::write_error_json(
            "inspect-tasks", "internal", "unexpected_exception", error.what()
        );
        return tbank::cli::kExitInternal;
    } catch (...) {
        tbank::cli::write_error_json(
            "inspect-tasks",
            "internal",
            "unknown_exception",
            "unknown non-standard exception"
        );
        return tbank::cli::kExitInternal;
    }
}

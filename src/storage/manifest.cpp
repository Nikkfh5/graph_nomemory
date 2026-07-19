#include "tbank/storage/manifest.hpp"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>

namespace tbank::storage {
namespace {

constexpr std::array<ManifestFileKind, kManifestFileCount> kFileKinds{
    ManifestFileKind::vertex_ids,
    ManifestFileKind::incoming_sources,
    ManifestFileKind::incoming_counts,
    ManifestFileKind::out_degree,
    ManifestFileKind::tasks,
};

constexpr std::array<std::string_view, kManifestFileCount> kFileNames{
    "vertex_ids.bin",
    "incoming_sources.bin",
    "incoming_counts.bin",
    "out_degree.bin",
    "tasks.bin",
};

constexpr std::array<std::string_view, kManifestFileCount> kFileMagics{
    "TBVID001",
    "TBSRC001",
    "TBINC001",
    "TBOUT001",
    "TBTASK01",
};

constexpr std::array<std::uint32_t, kManifestFileCount> kRecordBytes{
    4U,
    4U,
    4U,
    4U,
    32U,
};

[[nodiscard]] std::size_t kind_index(const ManifestFileKind kind) noexcept {
    switch (kind) {
        case ManifestFileKind::vertex_ids:
            return 0U;
        case ManifestFileKind::incoming_sources:
            return 1U;
        case ManifestFileKind::incoming_counts:
            return 2U;
        case ManifestFileKind::out_degree:
            return 3U;
        case ManifestFileKind::tasks:
            return 4U;
    }
    return kManifestFileCount;
}

[[noreturn]] void validation_error(const std::string& detail) {
    throw ManifestError("metadata.json: " + detail);
}

[[nodiscard]] std::uint64_t checked_payload_bytes(
    const std::uint64_t count,
    const std::uint32_t record_bytes,
    const std::string_view file_name
) {
    if (record_bytes == 0U) {
        validation_error(
            std::string(file_name) + ": record_bytes must be positive"
        );
    }
    if (count > std::numeric_limits<std::uint64_t>::max() / record_bytes) {
        validation_error(
            std::string(file_name) + ": record_count * record_bytes overflows uint64"
        );
    }
    return count * record_bytes;
}

[[nodiscard]] std::uint64_t checked_file_bytes(
    const std::uint64_t payload_bytes,
    const std::string_view file_name
) {
    if (payload_bytes >
        std::numeric_limits<std::uint64_t>::max() - kManifestBinaryHeaderBytes) {
        validation_error(
            std::string(file_name) + ": header_bytes + payload_bytes overflows uint64"
        );
    }
    return kManifestBinaryHeaderBytes + payload_bytes;
}

void append_unsigned(std::string& output, const std::uint64_t value) {
    std::array<char, std::numeric_limits<std::uint64_t>::digits10 + 2U> buffer{};
    const auto result = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    if (result.ec != std::errc{}) {
        validation_error("failed to encode an unsigned integer");
    }
    output.append(buffer.data(), result.ptr);
}

void append_crc64(std::string& output, const std::uint64_t value) {
    constexpr std::string_view digits = "0123456789abcdef";
    for (unsigned shift = 60U;; shift -= 4U) {
        output.push_back(digits[static_cast<std::size_t>((value >> shift) & 0xFU)]);
        if (shift == 0U) {
            break;
        }
    }
}

class CanonicalParser final {
public:
    explicit CanonicalParser(const std::string_view input) : input_(input) {}

    void expect(const std::string_view literal, const std::string_view context) {
        if (literal.size() > input_.size() - position_ ||
            input_.substr(position_, literal.size()) != literal) {
            fail("expected " + std::string(context));
        }
        position_ += literal.size();
    }

    template <class Integer>
    [[nodiscard]] Integer parse_unsigned(const std::string_view field_name) {
        static_assert(std::is_unsigned_v<Integer>);
        if (position_ == input_.size() || input_[position_] < '0' ||
            input_[position_] > '9') {
            fail(std::string(field_name) + " must be an unsigned decimal");
        }

        const std::size_t first = position_;
        if (input_[position_] == '0') {
            ++position_;
            if (position_ < input_.size() && input_[position_] >= '0' &&
                input_[position_] <= '9') {
                fail(std::string(field_name) + " has a leading zero");
            }
            return 0U;
        }

        Integer value = 0U;
        constexpr Integer maximum = std::numeric_limits<Integer>::max();
        while (position_ < input_.size() && input_[position_] >= '0' &&
               input_[position_] <= '9') {
            const Integer digit = static_cast<Integer>(input_[position_] - '0');
            if (value > (maximum - digit) / 10U) {
                position_ = first;
                fail(std::string(field_name) + " exceeds its integer range");
            }
            value = static_cast<Integer>(value * 10U + digit);
            ++position_;
        }
        return value;
    }

    [[nodiscard]] std::uint64_t parse_crc64(const std::string_view field_name) {
        if (input_.size() - position_ < 16U) {
            fail(std::string(field_name) + " must contain 16 lowercase hex digits");
        }

        std::uint64_t value = 0U;
        for (std::size_t index = 0U; index < 16U; ++index) {
            const char byte = input_[position_ + index];
            std::uint64_t digit = 0U;
            if (byte >= '0' && byte <= '9') {
                digit = static_cast<std::uint64_t>(byte - '0');
            } else if (byte >= 'a' && byte <= 'f') {
                digit = static_cast<std::uint64_t>(byte - 'a' + 10);
            } else {
                position_ += index;
                fail(std::string(field_name) + " must contain 16 lowercase hex digits");
            }
            value = (value << 4U) | digit;
        }
        position_ += 16U;
        return value;
    }

    [[nodiscard]] std::size_t position() const noexcept {
        return position_;
    }

    [[nodiscard]] bool finished() const noexcept {
        return position_ == input_.size();
    }

    [[noreturn]] void fail(const std::string& detail) const {
        throw ManifestError(
            "metadata.json: " + detail + " at byte " + std::to_string(position_)
        );
    }

private:
    std::string_view input_;
    std::size_t position_ = 0U;
};

}  // namespace

std::string_view manifest_file_name(const ManifestFileKind kind) noexcept {
    const std::size_t index = kind_index(kind);
    return index < kFileNames.size() ? kFileNames[index] : std::string_view{};
}

std::string_view manifest_file_magic(const ManifestFileKind kind) noexcept {
    const std::size_t index = kind_index(kind);
    return index < kFileMagics.size() ? kFileMagics[index] : std::string_view{};
}

std::uint32_t manifest_record_bytes(const ManifestFileKind kind) noexcept {
    const std::size_t index = kind_index(kind);
    return index < kRecordBytes.size() ? kRecordBytes[index] : 0U;
}

void validate_manifest(const GraphManifest& manifest) {
    if (manifest.schema_version != kManifestSchemaVersion) {
        validation_error("schema_version must be 1");
    }
    if (manifest.vertex_count == 0U) {
        validation_error("vertex_count must be in 1..UINT32_MAX");
    }
    if (manifest.edge_slice_size == 0U) {
        validation_error("edge_slice_size must be positive");
    }
    if (manifest.max_task_edges == 0U) {
        validation_error("max_task_edges must be positive");
    }
    if (manifest.max_task_vertices == 0U) {
        validation_error("max_task_vertices must be positive");
    }
    if (manifest.edge_slice_size > manifest.max_task_edges) {
        validation_error("edge_slice_size must not exceed max_task_edges");
    }

    for (std::size_t index = 0U; index < manifest.files.size(); ++index) {
        const ManifestFileDescriptor& descriptor = manifest.files[index];
        const ManifestFileKind expected_kind = kFileKinds[index];
        const std::string_view expected_name = kFileNames[index];
        if (descriptor.kind != expected_kind) {
            validation_error(
                "files[" + std::to_string(index) + "] must describe " +
                std::string(expected_name)
            );
        }
        if (descriptor.record_bytes != kRecordBytes[index]) {
            validation_error(
                std::string(expected_name) + ": record_bytes must be " +
                std::to_string(kRecordBytes[index])
            );
        }

        std::uint64_t expected_count = descriptor.record_count;
        if (index == 0U || index == 2U || index == 3U) {
            expected_count = manifest.vertex_count;
        } else if (index == 1U) {
            expected_count = manifest.edge_count;
        }
        if (descriptor.record_count != expected_count) {
            validation_error(
                std::string(expected_name) + ": record_count is inconsistent with " +
                (index == 1U ? "edge_count" : "vertex_count")
            );
        }

        const std::uint64_t expected_payload = checked_payload_bytes(
            descriptor.record_count, descriptor.record_bytes, expected_name
        );
        if (descriptor.payload_bytes != expected_payload) {
            validation_error(
                std::string(expected_name) +
                ": payload_bytes must equal record_count * record_bytes"
            );
        }
        const std::uint64_t expected_file = checked_file_bytes(
            descriptor.payload_bytes, expected_name
        );
        if (descriptor.file_bytes != expected_file) {
            validation_error(
                std::string(expected_name) +
                ": file_bytes must equal 48 + payload_bytes"
            );
        }
    }
}

std::string encode_manifest(const GraphManifest& manifest) {
    validate_manifest(manifest);

    std::string output;
    output.reserve(1536U);
    output.append("{\"format\":\"tbank-counted-graph\",\"schema_version\":");
    append_unsigned(output, manifest.schema_version);
    output.append(",\"vertex_count\":");
    append_unsigned(output, manifest.vertex_count);
    output.append(",\"edge_count\":");
    append_unsigned(output, manifest.edge_count);
    output.append(",\"edge_slice_size\":");
    append_unsigned(output, manifest.edge_slice_size);
    output.append(",\"max_task_edges\":");
    append_unsigned(output, manifest.max_task_edges);
    output.append(",\"max_task_vertices\":");
    append_unsigned(output, manifest.max_task_vertices);
    output.append(
        ",\"duplicate_edges\":\"removed\",\"self_loops\":\"preserved\","
        "\"vertex_ids\":\"signed-int32-ascending\","
        "\"checksum\":\"crc64-ecma-182\",\"files\":["
    );

    for (std::size_t index = 0U; index < manifest.files.size(); ++index) {
        if (index != 0U) {
            output.push_back(',');
        }
        const ManifestFileDescriptor& descriptor = manifest.files[index];
        output.append("{\"name\":\"");
        output.append(kFileNames[index]);
        output.append("\",\"magic\":\"");
        output.append(kFileMagics[index]);
        output.append("\",\"record_bytes\":");
        append_unsigned(output, descriptor.record_bytes);
        output.append(",\"record_count\":");
        append_unsigned(output, descriptor.record_count);
        output.append(",\"payload_bytes\":");
        append_unsigned(output, descriptor.payload_bytes);
        output.append(",\"file_bytes\":");
        append_unsigned(output, descriptor.file_bytes);
        output.append(",\"crc64\":\"");
        append_crc64(output, descriptor.crc64);
        output.append("\"}");
    }
    output.append("]}\n");

    if (output.size() > kMaxManifestBytes) {
        validation_error("encoded manifest exceeds kMaxManifestBytes");
    }
    return output;
}

GraphManifest parse_manifest(const std::string_view bytes) {
    if (bytes.size() > kMaxManifestBytes) {
        validation_error(
            "input exceeds kMaxManifestBytes (" +
            std::to_string(kMaxManifestBytes) + " bytes)"
        );
    }
    if (bytes.empty() || bytes.back() != '\n') {
        validation_error("canonical manifest must end with exactly one LF");
    }
    const std::size_t embedded_lf = bytes.substr(0U, bytes.size() - 1U).find('\n');
    if (embedded_lf != std::string_view::npos) {
        validation_error(
            "canonical manifest must be one line; LF found at byte " +
            std::to_string(embedded_lf)
        );
    }
    const std::size_t carriage_return = bytes.find('\r');
    if (carriage_return != std::string_view::npos) {
        validation_error(
            "carriage return is forbidden at byte " +
            std::to_string(carriage_return)
        );
    }
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        if (static_cast<unsigned char>(bytes[index]) >= 0x80U) {
            validation_error(
                "canonical v1 manifest must be ASCII; non-ASCII byte at " +
                std::to_string(index)
            );
        }
    }

    CanonicalParser parser(bytes);
    GraphManifest manifest;
    parser.expect(
        "{\"format\":\"tbank-counted-graph\",\"schema_version\":",
        "format followed by schema_version"
    );
    manifest.schema_version = parser.parse_unsigned<std::uint32_t>("schema_version");
    parser.expect(",\"vertex_count\":", "vertex_count after schema_version");
    manifest.vertex_count = parser.parse_unsigned<std::uint32_t>("vertex_count");
    parser.expect(",\"edge_count\":", "edge_count after vertex_count");
    manifest.edge_count = parser.parse_unsigned<std::uint64_t>("edge_count");
    parser.expect(",\"edge_slice_size\":", "edge_slice_size after edge_count");
    manifest.edge_slice_size =
        parser.parse_unsigned<std::uint64_t>("edge_slice_size");
    parser.expect(",\"max_task_edges\":", "max_task_edges after edge_slice_size");
    manifest.max_task_edges = parser.parse_unsigned<std::uint64_t>("max_task_edges");
    parser.expect(",\"max_task_vertices\":", "max_task_vertices after max_task_edges");
    manifest.max_task_vertices =
        parser.parse_unsigned<std::uint32_t>("max_task_vertices");
    parser.expect(
        ",\"duplicate_edges\":\"removed\",\"self_loops\":\"preserved\","
        "\"vertex_ids\":\"signed-int32-ascending\","
        "\"checksum\":\"crc64-ecma-182\",\"files\":[",
        "fixed policies followed by files"
    );

    for (std::size_t index = 0U; index < manifest.files.size(); ++index) {
        if (index != 0U) {
            parser.expect(",", "comma between file descriptors");
        }
        parser.expect("{\"name\":\"", "descriptor name");
        parser.expect(kFileNames[index], kFileNames[index]);
        parser.expect("\",\"magic\":\"", "magic after descriptor name");
        parser.expect(kFileMagics[index], kFileMagics[index]);
        parser.expect("\",\"record_bytes\":", "record_bytes after magic");

        ManifestFileDescriptor& descriptor = manifest.files[index];
        descriptor.kind = kFileKinds[index];
        descriptor.record_bytes =
            parser.parse_unsigned<std::uint32_t>("record_bytes");
        parser.expect(",\"record_count\":", "record_count after record_bytes");
        descriptor.record_count =
            parser.parse_unsigned<std::uint64_t>("record_count");
        parser.expect(",\"payload_bytes\":", "payload_bytes after record_count");
        descriptor.payload_bytes =
            parser.parse_unsigned<std::uint64_t>("payload_bytes");
        parser.expect(",\"file_bytes\":", "file_bytes after payload_bytes");
        descriptor.file_bytes = parser.parse_unsigned<std::uint64_t>("file_bytes");
        parser.expect(",\"crc64\":\"", "crc64 after file_bytes");
        descriptor.crc64 = parser.parse_crc64("crc64");
        parser.expect("\"}", "descriptor close after crc64");
    }
    parser.expect("]}\n", "files array, object close, and final LF");
    if (!parser.finished()) {
        parser.fail("trailing bytes are forbidden");
    }

    validate_manifest(manifest);
    return manifest;
}

}  // namespace tbank::storage

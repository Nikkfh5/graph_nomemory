#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace tbank::storage {

using BinaryMagic = std::array<std::byte, 8>;

inline constexpr BinaryMagic kVertexIdsMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x56}, std::byte{0x49},
    std::byte{0x44}, std::byte{0x30}, std::byte{0x30}, std::byte{0x31},
};
inline constexpr BinaryMagic kIncomingSourcesMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x53}, std::byte{0x52},
    std::byte{0x43}, std::byte{0x30}, std::byte{0x30}, std::byte{0x31},
};
inline constexpr BinaryMagic kIncomingCountsMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x49}, std::byte{0x4E},
    std::byte{0x43}, std::byte{0x30}, std::byte{0x30}, std::byte{0x31},
};
inline constexpr BinaryMagic kOutDegreeMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x4F}, std::byte{0x55},
    std::byte{0x54}, std::byte{0x30}, std::byte{0x30}, std::byte{0x31},
};
inline constexpr BinaryMagic kTasksMagic{
    std::byte{0x54}, std::byte{0x42}, std::byte{0x54}, std::byte{0x41},
    std::byte{0x53}, std::byte{0x4B}, std::byte{0x30}, std::byte{0x31},
};

inline constexpr std::uint32_t kBinarySchemaVersion = 1U;
inline constexpr std::uint32_t kBinaryHeaderBytes = 48U;
inline constexpr std::uint32_t kScalarRecordBytes = 4U;
inline constexpr std::uint32_t kTaskRecordBytes = 32U;

class BinaryError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// CRC-64/ECMA-182: polynomial 0x42F0E1EBA9EA3693, init/xor-out zero,
// non-reflected input and output. update() may be called with arbitrary chunks.
class Crc64Ecma final {
public:
    void update(std::span<const std::byte> bytes) noexcept;
    [[nodiscard]] std::uint64_t value() const noexcept;
    void reset() noexcept;

private:
    std::uint64_t value_ = 0U;
};

[[nodiscard]] std::uint64_t crc64_ecma(
    std::span<const std::byte> bytes
) noexcept;

// All integer helpers encode explicitly; the binary format never relies on
// native endianness, alignment, object representation, or struct padding.
void encode_u32_le(
    std::uint32_t value,
    std::span<std::byte> destination,
    std::size_t offset = 0U
);
void encode_u64_le(
    std::uint64_t value,
    std::span<std::byte> destination,
    std::size_t offset = 0U
);
void encode_i32_le(
    std::int32_t value,
    std::span<std::byte> destination,
    std::size_t offset = 0U
);

[[nodiscard]] std::uint32_t decode_u32_le(
    std::span<const std::byte> source,
    std::size_t offset = 0U
);
[[nodiscard]] std::uint64_t decode_u64_le(
    std::span<const std::byte> source,
    std::size_t offset = 0U
);
[[nodiscard]] std::int32_t decode_i32_le(
    std::span<const std::byte> source,
    std::size_t offset = 0U
);

struct BinaryHeader {
    BinaryMagic magic{};
    std::uint32_t schema_version = kBinarySchemaVersion;
    std::uint32_t header_bytes = kBinaryHeaderBytes;
    std::uint32_t record_bytes = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t record_count = 0U;
    std::uint64_t payload_bytes = 0U;
    std::uint64_t payload_crc64 = 0U;

    friend bool operator==(const BinaryHeader&, const BinaryHeader&) = default;
};

// Validation returns copied framing metadata rather than a borrowed payload
// view. This remains safe when the input is a temporary owning container;
// callers that need payload bytes must subspan their still-live input.
struct BinaryFileInfo {
    BinaryHeader header;
    std::size_t payload_offset = kBinaryHeaderBytes;
    std::size_t payload_size = 0U;
};

[[nodiscard]] BinaryHeader decode_binary_header(
    std::span<const std::byte> file_bytes
);
[[nodiscard]] std::array<std::byte, kBinaryHeaderBytes> encode_binary_header(
    const BinaryHeader& header
);

// Checks the v1 header independently of payload I/O and returns the declared
// total file length. This lets a later pread-based reader validate framing
// before allocating or streaming the payload.
[[nodiscard]] std::uint64_t validate_binary_header(
    const BinaryHeader& header,
    const BinaryMagic& expected_magic,
    std::uint32_t expected_record_bytes
);

// record_count is explicit so callers cannot accidentally publish a payload
// whose logical count differs from its physical length.
[[nodiscard]] std::vector<std::byte> encode_binary_file(
    const BinaryMagic& magic,
    std::uint32_t record_bytes,
    std::uint64_t record_count,
    std::span<const std::byte> payload
);

// Validation is strict: expected kind/schema/record size, reserved flags,
// overflow-safe count*size, exact (not minimum) file length, and payload CRC.
[[nodiscard]] BinaryFileInfo validate_binary_file(
    std::span<const std::byte> file_bytes,
    const BinaryMagic& expected_magic,
    std::uint32_t expected_record_bytes
);

[[nodiscard]] std::vector<std::byte> encode_i32_payload(
    std::span<const std::int32_t> records
);
[[nodiscard]] std::vector<std::int32_t> decode_i32_payload(
    std::span<const std::byte> payload
);
[[nodiscard]] std::vector<std::byte> encode_u32_payload(
    std::span<const std::uint32_t> records
);
[[nodiscard]] std::vector<std::uint32_t> decode_u32_payload(
    std::span<const std::byte> payload
);

[[nodiscard]] std::vector<std::byte> encode_i32_file(
    const BinaryMagic& magic,
    std::span<const std::int32_t> records
);
[[nodiscard]] std::vector<std::int32_t> decode_i32_file(
    std::span<const std::byte> file_bytes,
    const BinaryMagic& expected_magic
);
[[nodiscard]] std::vector<std::byte> encode_u32_file(
    const BinaryMagic& magic,
    std::span<const std::uint32_t> records
);
[[nodiscard]] std::vector<std::uint32_t> decode_u32_file(
    std::span<const std::byte> file_bytes,
    const BinaryMagic& expected_magic
);

enum class TaskTag : std::uint32_t {
    ordinary = 1U,
    hub_slice = 2U,
};

// Physical v1 mapping (32 bytes):
//   tag=ordinary: a=dst_begin, b=dst_count, c=0 (reserved)
//   tag=hub_slice: a=dst, b=slice_index, c=slice_count
// followed by edge_begin and edge_count for both variants.
struct TaskRecord {
    TaskTag tag = TaskTag::ordinary;
    std::uint32_t a = 0U;
    std::uint32_t b = 0U;
    std::uint32_t c = 0U;
    std::uint64_t edge_begin = 0U;
    std::uint64_t edge_count = 0U;

    friend bool operator==(const TaskRecord&, const TaskRecord&) = default;
};

[[nodiscard]] TaskRecord make_ordinary_task_record(
    std::uint32_t dst_begin,
    std::uint32_t dst_count,
    std::uint64_t edge_begin,
    std::uint64_t edge_count
);
[[nodiscard]] TaskRecord make_hub_slice_task_record(
    std::uint32_t dst,
    std::uint32_t slice_index,
    std::uint32_t slice_count,
    std::uint64_t edge_begin,
    std::uint64_t edge_count
);

void validate_task_record(const TaskRecord& record);

// Allocation-free codec for one canonical physical v1 task record. The
// decoder requires exactly 32 bytes and both directions validate all typed
// tag-specific invariants rather than merely copying fields.
[[nodiscard]] std::array<std::byte, kTaskRecordBytes> encode_task_record(
    const TaskRecord& record
);
[[nodiscard]] TaskRecord decode_task_record(
    std::span<const std::byte> encoded
);

[[nodiscard]] std::vector<std::byte> encode_task_payload(
    std::span<const TaskRecord> records
);
[[nodiscard]] std::vector<TaskRecord> decode_task_payload(
    std::span<const std::byte> payload
);
[[nodiscard]] std::vector<std::byte> encode_task_file(
    std::span<const TaskRecord> records
);
[[nodiscard]] std::vector<TaskRecord> decode_task_file(
    std::span<const std::byte> file_bytes
);

}  // namespace tbank::storage

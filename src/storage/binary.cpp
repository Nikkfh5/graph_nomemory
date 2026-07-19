#include "tbank/storage/binary.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <string>

namespace tbank::storage {
namespace {

constexpr std::uint64_t kCrc64EcmaPolynomial = 0x42F0E1EBA9EA3693ULL;

constexpr std::array<std::uint64_t, 256> make_crc64_table() noexcept {
    std::array<std::uint64_t, 256> table{};
    for (std::size_t index = 0U; index < table.size(); ++index) {
        std::uint64_t value = static_cast<std::uint64_t>(index) << 56U;
        for (unsigned bit = 0U; bit < 8U; ++bit) {
            const bool high_bit = (value & (1ULL << 63U)) != 0U;
            value <<= 1U;
            if (high_bit) {
                value ^= kCrc64EcmaPolynomial;
            }
        }
        table[index] = value;
    }
    return table;
}

constexpr auto kCrc64Table = make_crc64_table();

constexpr std::size_t kMagicOffset = 0U;
constexpr std::size_t kSchemaVersionOffset = 8U;
constexpr std::size_t kHeaderBytesOffset = 12U;
constexpr std::size_t kRecordBytesOffset = 16U;
constexpr std::size_t kFlagsOffset = 20U;
constexpr std::size_t kRecordCountOffset = 24U;
constexpr std::size_t kPayloadBytesOffset = 32U;
constexpr std::size_t kPayloadCrcOffset = 40U;

void require_range(
    const std::size_t available,
    const std::size_t offset,
    const std::size_t width
) {
    if (offset > available || width > available - offset) {
        throw BinaryError("little-endian field is truncated");
    }
}

std::uint64_t size_to_u64(const std::size_t value) {
    if constexpr (sizeof(std::size_t) > sizeof(std::uint64_t)) {
        if (value > std::numeric_limits<std::uint64_t>::max()) {
            throw BinaryError("byte length does not fit uint64");
        }
    }
    return static_cast<std::uint64_t>(value);
}

std::size_t u64_to_size(const std::uint64_t value) {
    if (value > std::numeric_limits<std::size_t>::max()) {
        throw BinaryError("declared byte length does not fit size_t");
    }
    return static_cast<std::size_t>(value);
}

std::uint64_t checked_payload_bytes(
    const std::uint64_t record_count,
    const std::uint32_t record_bytes
) {
    if (record_bytes == 0U) {
        throw BinaryError("record_bytes must be positive");
    }
    if (record_count
        > std::numeric_limits<std::uint64_t>::max() / record_bytes) {
        throw BinaryError("record_count * record_bytes overflows uint64");
    }
    return record_count * record_bytes;
}

std::uint64_t checked_total_u64(const std::uint64_t payload_bytes) {
    if (payload_bytes
        > std::numeric_limits<std::uint64_t>::max() - kBinaryHeaderBytes) {
        throw BinaryError("header + payload length overflows uint64");
    }
    return payload_bytes + kBinaryHeaderBytes;
}

std::size_t checked_total_size(const std::uint64_t payload_bytes) {
    return u64_to_size(checked_total_u64(payload_bytes));
}

std::uint64_t checked_record_count(const std::size_t count) {
    return size_to_u64(count);
}

template <class Record>
std::size_t checked_payload_size(
    const std::span<const Record> records,
    const std::size_t record_bytes
) {
    if (records.size() > std::numeric_limits<std::size_t>::max() / record_bytes) {
        throw BinaryError("payload byte length overflows size_t");
    }
    return records.size() * record_bytes;
}

void validate_edge_range(const TaskRecord& record) {
    if (record.edge_begin
        > std::numeric_limits<std::uint64_t>::max() - record.edge_count) {
        throw BinaryError("task edge range overflows uint64");
    }
}

}  // namespace

void Crc64Ecma::update(const std::span<const std::byte> bytes) noexcept {
    for (const std::byte byte : bytes) {
        const auto byte_value = std::to_integer<std::uint8_t>(byte);
        const auto table_index = static_cast<std::uint8_t>(
            static_cast<std::uint8_t>(value_ >> 56U) ^ byte_value
        );
        value_ = kCrc64Table[table_index] ^ (value_ << 8U);
    }
}

std::uint64_t Crc64Ecma::value() const noexcept {
    return value_;
}

void Crc64Ecma::reset() noexcept {
    value_ = 0U;
}

std::uint64_t crc64_ecma(const std::span<const std::byte> bytes) noexcept {
    Crc64Ecma checksum;
    checksum.update(bytes);
    return checksum.value();
}

void encode_u32_le(
    const std::uint32_t value,
    const std::span<std::byte> destination,
    const std::size_t offset
) {
    require_range(destination.size(), offset, sizeof(value));
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const auto shift = static_cast<unsigned>(index * 8U);
        destination[offset + index] = static_cast<std::byte>(
            (value >> shift) & 0xFFU
        );
    }
}

void encode_u64_le(
    const std::uint64_t value,
    const std::span<std::byte> destination,
    const std::size_t offset
) {
    require_range(destination.size(), offset, sizeof(value));
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const auto shift = static_cast<unsigned>(index * 8U);
        destination[offset + index] = static_cast<std::byte>(
            (value >> shift) & 0xFFU
        );
    }
}

void encode_i32_le(
    const std::int32_t value,
    const std::span<std::byte> destination,
    const std::size_t offset
) {
    encode_u32_le(std::bit_cast<std::uint32_t>(value), destination, offset);
}

std::uint32_t decode_u32_le(
    const std::span<const std::byte> source,
    const std::size_t offset
) {
    require_range(source.size(), offset, sizeof(std::uint32_t));
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const auto shift = static_cast<unsigned>(index * 8U);
        value |= std::to_integer<std::uint32_t>(source[offset + index]) << shift;
    }
    return value;
}

std::uint64_t decode_u64_le(
    const std::span<const std::byte> source,
    const std::size_t offset
) {
    require_range(source.size(), offset, sizeof(std::uint64_t));
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const auto shift = static_cast<unsigned>(index * 8U);
        value |= std::to_integer<std::uint64_t>(source[offset + index]) << shift;
    }
    return value;
}

std::int32_t decode_i32_le(
    const std::span<const std::byte> source,
    const std::size_t offset
) {
    return std::bit_cast<std::int32_t>(decode_u32_le(source, offset));
}

BinaryHeader decode_binary_header(
    const std::span<const std::byte> file_bytes
) {
    require_range(file_bytes.size(), 0U, kBinaryHeaderBytes);

    BinaryHeader header{};
    std::copy_n(
        file_bytes.begin() + static_cast<std::ptrdiff_t>(kMagicOffset),
        header.magic.size(),
        header.magic.begin()
    );
    header.schema_version = decode_u32_le(file_bytes, kSchemaVersionOffset);
    header.header_bytes = decode_u32_le(file_bytes, kHeaderBytesOffset);
    header.record_bytes = decode_u32_le(file_bytes, kRecordBytesOffset);
    header.flags = decode_u32_le(file_bytes, kFlagsOffset);
    header.record_count = decode_u64_le(file_bytes, kRecordCountOffset);
    header.payload_bytes = decode_u64_le(file_bytes, kPayloadBytesOffset);
    header.payload_crc64 = decode_u64_le(file_bytes, kPayloadCrcOffset);
    return header;
}

std::array<std::byte, kBinaryHeaderBytes> encode_binary_header(
    const BinaryHeader& header
) {
    if (header.schema_version != kBinarySchemaVersion) {
        throw BinaryError("cannot encode unsupported binary schema version");
    }
    if (header.header_bytes != kBinaryHeaderBytes) {
        throw BinaryError("cannot encode unsupported binary header length");
    }
    if (header.flags != 0U) {
        throw BinaryError("cannot encode nonzero reserved binary flags");
    }
    const std::uint64_t computed_payload_bytes = checked_payload_bytes(
        header.record_count, header.record_bytes
    );
    if (header.payload_bytes != computed_payload_bytes) {
        throw BinaryError("cannot encode inconsistent binary payload length");
    }
    static_cast<void>(checked_total_u64(header.payload_bytes));

    std::array<std::byte, kBinaryHeaderBytes> bytes{};
    std::copy(header.magic.begin(), header.magic.end(), bytes.begin());
    encode_u32_le(header.schema_version, bytes, kSchemaVersionOffset);
    encode_u32_le(header.header_bytes, bytes, kHeaderBytesOffset);
    encode_u32_le(header.record_bytes, bytes, kRecordBytesOffset);
    encode_u32_le(header.flags, bytes, kFlagsOffset);
    encode_u64_le(header.record_count, bytes, kRecordCountOffset);
    encode_u64_le(header.payload_bytes, bytes, kPayloadBytesOffset);
    encode_u64_le(header.payload_crc64, bytes, kPayloadCrcOffset);
    return bytes;
}

std::uint64_t validate_binary_header(
    const BinaryHeader& header,
    const BinaryMagic& expected_magic,
    const std::uint32_t expected_record_bytes
) {
    if (expected_record_bytes == 0U) {
        throw BinaryError("expected record_bytes must be positive");
    }
    if (header.magic != expected_magic) {
        throw BinaryError("binary file kind/magic mismatch");
    }
    if (header.schema_version != kBinarySchemaVersion) {
        throw BinaryError("unsupported binary schema version");
    }
    if (header.header_bytes != kBinaryHeaderBytes) {
        throw BinaryError("unsupported binary header length");
    }
    if (header.record_bytes != expected_record_bytes) {
        throw BinaryError("binary record size mismatch");
    }
    if (header.flags != 0U) {
        throw BinaryError("nonzero reserved binary flags");
    }

    const std::uint64_t computed_payload_bytes = checked_payload_bytes(
        header.record_count, header.record_bytes
    );
    if (header.payload_bytes != computed_payload_bytes) {
        throw BinaryError("declared payload length does not match record count");
    }
    return checked_total_u64(header.payload_bytes);
}

std::vector<std::byte> encode_binary_file(
    const BinaryMagic& magic,
    const std::uint32_t record_bytes,
    const std::uint64_t record_count,
    const std::span<const std::byte> payload
) {
    const std::uint64_t declared_payload_bytes = checked_payload_bytes(
        record_count, record_bytes
    );
    if (declared_payload_bytes != size_to_u64(payload.size())) {
        throw BinaryError("payload length does not equal record_count * record_bytes");
    }

    const BinaryHeader header{
        .magic = magic,
        .schema_version = kBinarySchemaVersion,
        .header_bytes = kBinaryHeaderBytes,
        .record_bytes = record_bytes,
        .flags = 0U,
        .record_count = record_count,
        .payload_bytes = declared_payload_bytes,
        .payload_crc64 = crc64_ecma(payload),
    };
    const auto encoded_header = encode_binary_header(header);
    const std::size_t total_size = checked_total_size(declared_payload_bytes);
    std::vector<std::byte> file_bytes(total_size);
    std::copy(encoded_header.begin(), encoded_header.end(), file_bytes.begin());
    std::copy(
        payload.begin(),
        payload.end(),
        file_bytes.begin() + static_cast<std::ptrdiff_t>(kBinaryHeaderBytes)
    );
    return file_bytes;
}

namespace {

template <class PayloadEncoder>
std::vector<std::byte> encode_records_file_once(
    const BinaryMagic& magic,
    const std::uint32_t record_bytes,
    const std::uint64_t record_count,
    PayloadEncoder&& encode_payload
) {
    const std::uint64_t payload_bytes = checked_payload_bytes(
        record_count, record_bytes
    );
    std::vector<std::byte> file_bytes(checked_total_size(payload_bytes));
    const std::size_t payload_size = u64_to_size(payload_bytes);
    std::span<std::byte> payload = std::span<std::byte>(file_bytes).subspan(
        kBinaryHeaderBytes, payload_size
    );
    encode_payload(payload);

    const BinaryHeader header{
        .magic = magic,
        .schema_version = kBinarySchemaVersion,
        .header_bytes = kBinaryHeaderBytes,
        .record_bytes = record_bytes,
        .flags = 0U,
        .record_count = record_count,
        .payload_bytes = payload_bytes,
        .payload_crc64 = crc64_ecma(payload),
    };
    const auto encoded_header = encode_binary_header(header);
    std::copy(encoded_header.begin(), encoded_header.end(), file_bytes.begin());
    return file_bytes;
}

}  // namespace

BinaryFileInfo validate_binary_file(
    const std::span<const std::byte> file_bytes,
    const BinaryMagic& expected_magic,
    const std::uint32_t expected_record_bytes
) {
    const BinaryHeader header = decode_binary_header(file_bytes);
    const std::size_t expected_total_size = u64_to_size(
        validate_binary_header(header, expected_magic, expected_record_bytes)
    );
    if (file_bytes.size() != expected_total_size) {
        throw BinaryError("binary file length is not exact");
    }

    const std::size_t payload_size = u64_to_size(header.payload_bytes);
    const std::span<const std::byte> payload = file_bytes.subspan(
        kBinaryHeaderBytes, payload_size
    );
    if (crc64_ecma(payload) != header.payload_crc64) {
        throw BinaryError("binary payload CRC-64 mismatch");
    }
    return BinaryFileInfo{
        .header = header,
        .payload_offset = kBinaryHeaderBytes,
        .payload_size = payload_size,
    };
}

std::vector<std::byte> encode_i32_payload(
    const std::span<const std::int32_t> records
) {
    std::vector<std::byte> payload(
        checked_payload_size(records, kScalarRecordBytes)
    );
    for (std::size_t index = 0U; index < records.size(); ++index) {
        encode_i32_le(records[index], payload, index * kScalarRecordBytes);
    }
    return payload;
}

std::vector<std::int32_t> decode_i32_payload(
    const std::span<const std::byte> payload
) {
    if (payload.size() % kScalarRecordBytes != 0U) {
        throw BinaryError("int32 payload length is not divisible by four");
    }
    const std::size_t count = payload.size() / kScalarRecordBytes;
    std::vector<std::int32_t> records(count);
    for (std::size_t index = 0U; index < count; ++index) {
        records[index] = decode_i32_le(payload, index * kScalarRecordBytes);
    }
    return records;
}

std::vector<std::byte> encode_u32_payload(
    const std::span<const std::uint32_t> records
) {
    std::vector<std::byte> payload(
        checked_payload_size(records, kScalarRecordBytes)
    );
    for (std::size_t index = 0U; index < records.size(); ++index) {
        encode_u32_le(records[index], payload, index * kScalarRecordBytes);
    }
    return payload;
}

std::vector<std::uint32_t> decode_u32_payload(
    const std::span<const std::byte> payload
) {
    if (payload.size() % kScalarRecordBytes != 0U) {
        throw BinaryError("uint32 payload length is not divisible by four");
    }
    const std::size_t count = payload.size() / kScalarRecordBytes;
    std::vector<std::uint32_t> records(count);
    for (std::size_t index = 0U; index < count; ++index) {
        records[index] = decode_u32_le(payload, index * kScalarRecordBytes);
    }
    return records;
}

std::vector<std::byte> encode_i32_file(
    const BinaryMagic& magic,
    const std::span<const std::int32_t> records
) {
    return encode_records_file_once(
        magic,
        kScalarRecordBytes,
        checked_record_count(records.size()),
        [records](const std::span<std::byte> payload) {
            for (std::size_t index = 0U; index < records.size(); ++index) {
                encode_i32_le(
                    records[index], payload, index * kScalarRecordBytes
                );
            }
        }
    );
}

std::vector<std::int32_t> decode_i32_file(
    const std::span<const std::byte> file_bytes,
    const BinaryMagic& expected_magic
) {
    const BinaryFileInfo info = validate_binary_file(
        file_bytes, expected_magic, kScalarRecordBytes
    );
    return decode_i32_payload(file_bytes.subspan(
        info.payload_offset, info.payload_size
    ));
}

std::vector<std::byte> encode_u32_file(
    const BinaryMagic& magic,
    const std::span<const std::uint32_t> records
) {
    return encode_records_file_once(
        magic,
        kScalarRecordBytes,
        checked_record_count(records.size()),
        [records](const std::span<std::byte> payload) {
            for (std::size_t index = 0U; index < records.size(); ++index) {
                encode_u32_le(
                    records[index], payload, index * kScalarRecordBytes
                );
            }
        }
    );
}

std::vector<std::uint32_t> decode_u32_file(
    const std::span<const std::byte> file_bytes,
    const BinaryMagic& expected_magic
) {
    const BinaryFileInfo info = validate_binary_file(
        file_bytes, expected_magic, kScalarRecordBytes
    );
    return decode_u32_payload(file_bytes.subspan(
        info.payload_offset, info.payload_size
    ));
}

TaskRecord make_ordinary_task_record(
    const std::uint32_t dst_begin,
    const std::uint32_t dst_count,
    const std::uint64_t edge_begin,
    const std::uint64_t edge_count
) {
    const TaskRecord record{
        .tag = TaskTag::ordinary,
        .a = dst_begin,
        .b = dst_count,
        .c = 0U,
        .edge_begin = edge_begin,
        .edge_count = edge_count,
    };
    validate_task_record(record);
    return record;
}

TaskRecord make_hub_slice_task_record(
    const std::uint32_t dst,
    const std::uint32_t slice_index,
    const std::uint32_t slice_count,
    const std::uint64_t edge_begin,
    const std::uint64_t edge_count
) {
    const TaskRecord record{
        .tag = TaskTag::hub_slice,
        .a = dst,
        .b = slice_index,
        .c = slice_count,
        .edge_begin = edge_begin,
        .edge_count = edge_count,
    };
    validate_task_record(record);
    return record;
}

void validate_task_record(const TaskRecord& record) {
    validate_edge_range(record);
    switch (record.tag) {
        case TaskTag::ordinary:
            if (record.c != 0U) {
                throw BinaryError("ordinary task reserved field c must be zero");
            }
            if (record.b == 0U) {
                throw BinaryError("ordinary task dst_count must be positive");
            }
            if (record.a
                > std::numeric_limits<std::uint32_t>::max() - record.b) {
                throw BinaryError("ordinary task destination range overflows uint32");
            }
            return;
        case TaskTag::hub_slice:
            if (record.c == 0U) {
                throw BinaryError("hub task slice_count must be positive");
            }
            if (record.b >= record.c) {
                throw BinaryError("hub task slice_index is outside slice_count");
            }
            if (record.edge_count == 0U) {
                throw BinaryError("hub task edge_count must be positive");
            }
            return;
    }
    throw BinaryError("unknown task record tag");
}

std::array<std::byte, kTaskRecordBytes> encode_task_record(
    const TaskRecord& record
) {
    validate_task_record(record);
    std::array<std::byte, kTaskRecordBytes> encoded{};
    encode_u32_le(static_cast<std::uint32_t>(record.tag), encoded, 0U);
    encode_u32_le(record.a, encoded, 4U);
    encode_u32_le(record.b, encoded, 8U);
    encode_u32_le(record.c, encoded, 12U);
    encode_u64_le(record.edge_begin, encoded, 16U);
    encode_u64_le(record.edge_count, encoded, 24U);
    return encoded;
}

TaskRecord decode_task_record(const std::span<const std::byte> encoded) {
    if (encoded.size() != kTaskRecordBytes) {
        throw BinaryError("task record must contain exactly 32 bytes");
    }
    const TaskRecord record{
        .tag = static_cast<TaskTag>(decode_u32_le(encoded, 0U)),
        .a = decode_u32_le(encoded, 4U),
        .b = decode_u32_le(encoded, 8U),
        .c = decode_u32_le(encoded, 12U),
        .edge_begin = decode_u64_le(encoded, 16U),
        .edge_count = decode_u64_le(encoded, 24U),
    };
    validate_task_record(record);
    return record;
}

std::vector<std::byte> encode_task_payload(
    const std::span<const TaskRecord> records
) {
    std::vector<std::byte> payload(
        checked_payload_size(records, kTaskRecordBytes)
    );
    for (std::size_t index = 0U; index < records.size(); ++index) {
        const std::size_t offset = index * kTaskRecordBytes;
        const auto encoded = encode_task_record(records[index]);
        std::copy(
            encoded.begin(),
            encoded.end(),
            std::span<std::byte>(payload).subspan(
                offset, kTaskRecordBytes
            ).begin()
        );
    }
    return payload;
}

std::vector<TaskRecord> decode_task_payload(
    const std::span<const std::byte> payload
) {
    if (payload.size() % kTaskRecordBytes != 0U) {
        throw BinaryError("task payload length is not divisible by 32");
    }
    const std::size_t count = payload.size() / kTaskRecordBytes;
    std::vector<TaskRecord> records;
    records.reserve(count);
    for (std::size_t index = 0U; index < count; ++index) {
        const std::size_t offset = index * kTaskRecordBytes;
        records.push_back(decode_task_record(
            payload.subspan(offset, kTaskRecordBytes)
        ));
    }
    return records;
}

std::vector<std::byte> encode_task_file(
    const std::span<const TaskRecord> records
) {
    return encode_records_file_once(
        kTasksMagic,
        kTaskRecordBytes,
        checked_record_count(records.size()),
        [records](const std::span<std::byte> payload) {
            for (std::size_t index = 0U; index < records.size(); ++index) {
                const auto encoded = encode_task_record(records[index]);
                std::copy(
                    encoded.begin(),
                    encoded.end(),
                    payload.subspan(
                        index * kTaskRecordBytes, kTaskRecordBytes
                    ).begin()
                );
            }
        }
    );
}

std::vector<TaskRecord> decode_task_file(
    const std::span<const std::byte> file_bytes
) {
    const BinaryFileInfo info = validate_binary_file(
        file_bytes, kTasksMagic, kTaskRecordBytes
    );
    return decode_task_payload(file_bytes.subspan(
        info.payload_offset, info.payload_size
    ));
}

}  // namespace tbank::storage

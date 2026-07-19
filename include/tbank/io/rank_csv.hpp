#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>

namespace tbank::io {

struct RankRow {
    std::int32_t vertex;
    double rank;
};

struct RankCsvSummary {
    std::uint64_t rows;
    std::uint64_t bytes;
};

class RankCsvError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// A successful sink call accepts the full span; durability is handled above.
using ByteConsumer = std::function<void(std::span<const char>)>;

// Bounded canonical encoder. IDs must increase strictly, delivery is synchronous, and the caller
// verifies graph binding and row count.
class RankCsvEncoder final {
public:
    explicit RankCsvEncoder(ByteConsumer consumer);

    // Destruction abandons unfinished output; finish() proves encoder completion.
    ~RankCsvEncoder();

    RankCsvEncoder(RankCsvEncoder&&) = delete;
    RankCsvEncoder& operator=(RankCsvEncoder&&) = delete;
    RankCsvEncoder(const RankCsvEncoder&) = delete;
    RankCsvEncoder& operator=(const RankCsvEncoder&) = delete;

    void begin();
    void write(RankRow row);
    // The summary counts all rows and all complete spans acknowledged by the
    // consumer, including the header bytes. It does not imply fsync/rename.
    [[nodiscard]] RankCsvSummary finish();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace tbank::io

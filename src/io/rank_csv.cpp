#include "tbank/io/rank_csv.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <limits>
#include <string_view>
#include <system_error>
#include <utility>

namespace tbank::io {
namespace {

constexpr std::string_view kHeader = "vertex,rank\n";

}  // namespace

class RankCsvEncoder::Impl final {
public:
    explicit Impl(ByteConsumer consumer) : consumer_(std::move(consumer)) {
        if (!consumer_) {
            throw std::invalid_argument("rank CSV consumer must not be empty");
        }
    }

    void begin() {
        ensure_state(State::created, "rank CSV encoder has already begun");
        try {
            emit(kHeader);
            state_ = State::active;
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

    void write(const RankRow row) {
        ensure_state(State::active, "rank CSV encoder is not active");
        try {
            validate_row(row);
            const std::string_view encoded = encode_row(row);
            emit(encoded);
            previous_vertex_ = row.vertex;
            has_previous_vertex_ = true;
            ++rows_;
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

    RankCsvSummary finish() {
        ensure_state(State::active, "rank CSV encoder is not active");
        if (rows_ == 0U) {
            state_ = State::failed;
            throw RankCsvError("rank CSV requires at least one row");
        }
        state_ = State::finished;
        return RankCsvSummary{.rows = rows_, .bytes = bytes_};
    }

private:
    enum class State { created, active, finished, failed };

    void ensure_state(const State expected, const char* const message) {
        if (in_consumer_) {
            state_ = State::failed;
            throw std::logic_error("reentrant rank CSV encoder call");
        }
        if (state_ != expected) {
            throw std::logic_error(message);
        }
    }

    void validate_row(const RankRow row) {
        if (!std::isfinite(row.rank)) {
            throw RankCsvError("rank must be finite");
        }
        if (has_previous_vertex_ && row.vertex <= previous_vertex_) {
            throw RankCsvError(
                "vertex IDs must be unique and strictly increasing"
            );
        }
        if (rows_ == std::numeric_limits<std::uint64_t>::max()) {
            throw RankCsvError("rank CSV row count overflow");
        }
    }

    std::string_view encode_row(const RankRow row) {
        char* cursor = row_buffer_.data();
        char* const end = row_buffer_.data() + row_buffer_.size();

        const auto vertex_result = std::to_chars(cursor, end, row.vertex);
        if (vertex_result.ec != std::errc{}) {
            throw RankCsvError("failed to format vertex ID");
        }
        cursor = vertex_result.ptr;
        if (cursor == end) {
            throw RankCsvError("rank CSV row buffer exhausted");
        }
        *cursor++ = ',';

        const auto rank_result = std::to_chars(
            cursor,
            end,
            row.rank,
            std::chars_format::general,
            std::numeric_limits<double>::max_digits10
        );
        if (rank_result.ec != std::errc{}) {
            throw RankCsvError("failed to format rank");
        }
        cursor = rank_result.ptr;
        if (cursor == end) {
            throw RankCsvError("rank CSV row buffer exhausted");
        }
        *cursor++ = '\n';
        return std::string_view(
            row_buffer_.data(), static_cast<std::size_t>(cursor - row_buffer_.data())
        );
    }

    void emit(const std::string_view bytes) {
        const auto size = static_cast<std::uint64_t>(bytes.size());
        if (bytes_ > std::numeric_limits<std::uint64_t>::max() - size) {
            throw RankCsvError("rank CSV byte count overflow");
        }

        in_consumer_ = true;
        try {
            consumer_(std::span<const char>(bytes.data(), bytes.size()));
        } catch (...) {
            in_consumer_ = false;
            state_ = State::failed;
            throw;
        }
        in_consumer_ = false;
        if (state_ == State::failed) {
            throw std::logic_error(
                "rank CSV encoder was poisoned by a reentrant consumer call"
            );
        }
        bytes_ += size;
    }

    ByteConsumer consumer_;
    State state_ = State::created;
    bool in_consumer_ = false;
    bool has_previous_vertex_ = false;
    std::int32_t previous_vertex_ = 0;
    std::uint64_t rows_ = 0U;
    std::uint64_t bytes_ = 0U;
    std::array<char, 128> row_buffer_{};
};

RankCsvEncoder::RankCsvEncoder(ByteConsumer consumer)
    : impl_(std::make_unique<Impl>(std::move(consumer))) {}

RankCsvEncoder::~RankCsvEncoder() = default;

void RankCsvEncoder::begin() {
    impl_->begin();
}

void RankCsvEncoder::write(const RankRow row) {
    impl_->write(row);
}

RankCsvSummary RankCsvEncoder::finish() {
    return impl_->finish();
}

}  // namespace tbank::io

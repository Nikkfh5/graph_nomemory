#include "tbank/io/edge_csv.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <string_view>
#include <utility>

namespace tbank::io {
namespace {

constexpr std::string_view kHeader = "from,to";
constexpr std::uint64_t kPositiveInt32Limit = 2'147'483'647ULL;
constexpr std::uint64_t kNegativeInt32Limit = 2'147'483'648ULL;

bool is_digit(const char byte) noexcept {
    return byte >= '0' && byte <= '9';
}

bool is_field_space(const char byte) noexcept {
    return byte == ' ' || byte == '\t';
}

}  // namespace

EdgeCsvError::EdgeCsvError(
    const std::uint64_t input_line,
    const char* const message
)
    : std::runtime_error(message), input_line_(input_line) {}

std::uint64_t EdgeCsvError::input_line() const noexcept {
    return input_line_;
}

class EdgeCsvParser::Impl final {
public:
    explicit Impl(EdgeConsumer consumer) : consumer_(std::move(consumer)) {
        if (!consumer_) {
            throw std::invalid_argument("edge CSV consumer must not be empty");
        }
    }

    void consume(const std::span<const char> bytes) {
        ensure_usable();
        try {
            for (const char byte : bytes) {
                consume_byte(byte);
            }
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

    EdgeCsvSummary finish() {
        ensure_usable();
        try {
            finish_active_state();
            state_ = State::finished;
            return EdgeCsvSummary{.data_rows = data_rows_};
        } catch (...) {
            state_ = State::failed;
            throw;
        }
    }

private:
    enum class State {
        header,
        header_cr,
        first_leading,
        first_need_digit,
        first_digits,
        first_trailing,
        second_leading,
        second_need_digit,
        second_digits,
        second_trailing,
        row_cr,
        finished,
        failed,
    };

    [[noreturn]] void fail_input(const char* const message) {
        state_ = State::failed;
        throw EdgeCsvError(line_, message);
    }

    [[noreturn]] void fail_lifecycle(const char* const message) {
        state_ = State::failed;
        throw std::logic_error(message);
    }

    void ensure_usable() {
        if (in_consumer_) {
            fail_lifecycle("reentrant edge CSV parser call");
        }
        if (state_ == State::finished) {
            throw std::logic_error("edge CSV parser is already finished");
        }
        if (state_ == State::failed) {
            throw std::logic_error("edge CSV parser is poisoned");
        }
    }

    void consume_byte(const char byte) {
        switch (state_) {
            case State::header:
                consume_header(byte);
                return;
            case State::header_cr:
                consume_header_cr(byte);
                return;
            case State::first_leading:
                consume_first_leading(byte);
                return;
            case State::first_need_digit:
                consume_first_need_digit(byte);
                return;
            case State::first_digits:
                consume_first_digits(byte);
                return;
            case State::first_trailing:
                consume_first_trailing(byte);
                return;
            case State::second_leading:
                consume_second_leading(byte);
                return;
            case State::second_need_digit:
                consume_second_need_digit(byte);
                return;
            case State::second_digits:
                consume_second_digits(byte);
                return;
            case State::second_trailing:
                consume_second_trailing(byte);
                return;
            case State::row_cr:
                consume_row_cr(byte);
                return;
            case State::finished:
            case State::failed:
                fail_lifecycle("edge CSV parser is terminal");
        }
    }

    void consume_header(const char byte) {
        if (header_index_ < kHeader.size()) {
            if (header_index_ == 0U
                && static_cast<unsigned char>(byte) == 0xEFU) {
                fail_input("UTF-8 BOM is not allowed");
            }
            if (byte != kHeader[header_index_]) {
                fail_input("expected exact header from,to");
            }
            ++header_index_;
            return;
        }

        if (byte == '\n') {
            begin_data_lines();
            return;
        }
        if (byte == '\r') {
            state_ = State::header_cr;
            return;
        }
        fail_input("unexpected byte after exact header from,to");
    }

    void consume_header_cr(const char byte) {
        if (byte != '\n') {
            fail_input("carriage return must be followed by line feed");
        }
        begin_data_lines();
    }

    void begin_data_lines() {
        line_ = 2U;
        row_touched_ = false;
        state_ = State::first_leading;
    }

    void mark_row_touched() noexcept {
        row_touched_ = true;
    }

    void begin_number(const bool negative, const State next_state) noexcept {
        negative_ = negative;
        has_digit_ = false;
        leading_zero_ = false;
        magnitude_ = 0U;
        state_ = next_state;
    }

    void append_digit(const char byte) {
        const std::uint64_t digit = static_cast<std::uint64_t>(byte - '0');
        if (!has_digit_) {
            if (negative_ && digit == 0U) {
                fail_input("integer must use canonical decimal form");
            }
            has_digit_ = true;
            leading_zero_ = digit == 0U;
        } else if (leading_zero_) {
            fail_input("integer must use canonical decimal form");
        }

        const std::uint64_t limit = negative_
            ? kNegativeInt32Limit
            : kPositiveInt32Limit;
        if (magnitude_ > (limit - digit) / 10U) {
            fail_input("integer is outside signed int32 range");
        }
        magnitude_ = magnitude_ * 10U + digit;
    }

    std::int32_t completed_number() const noexcept {
        if (!negative_) {
            return static_cast<std::int32_t>(magnitude_);
        }
        if (magnitude_ == kNegativeInt32Limit) {
            return std::numeric_limits<std::int32_t>::min();
        }
        return -static_cast<std::int32_t>(magnitude_);
    }

    void consume_first_leading(const char byte) {
        mark_row_touched();
        if (is_field_space(byte)) {
            return;
        }
        if (byte == '-') {
            begin_number(true, State::first_need_digit);
            return;
        }
        if (is_digit(byte)) {
            begin_number(false, State::first_digits);
            append_digit(byte);
            return;
        }
        if (byte == '\n' || byte == '\r') {
            fail_input("blank data line is not allowed");
        }
        if (byte == ',') {
            fail_input("missing source integer");
        }
        if (byte == '"') {
            fail_input("quoted fields are not allowed");
        }
        fail_input("invalid source integer");
    }

    void consume_first_need_digit(const char byte) {
        if (!is_digit(byte)) {
            fail_input("minus sign must be followed by a digit");
        }
        state_ = State::first_digits;
        append_digit(byte);
    }

    void consume_first_digits(const char byte) {
        if (is_digit(byte)) {
            append_digit(byte);
            return;
        }
        source_ = completed_number();
        if (byte == ',') {
            state_ = State::second_leading;
            return;
        }
        if (is_field_space(byte)) {
            state_ = State::first_trailing;
            return;
        }
        if (byte == '\n' || byte == '\r') {
            fail_input("missing destination field");
        }
        fail_input("trailing junk in source field");
    }

    void consume_first_trailing(const char byte) {
        if (is_field_space(byte)) {
            return;
        }
        if (byte == ',') {
            state_ = State::second_leading;
            return;
        }
        if (byte == '\n' || byte == '\r') {
            fail_input("missing destination field");
        }
        fail_input("trailing junk in source field");
    }

    void consume_second_leading(const char byte) {
        if (is_field_space(byte)) {
            return;
        }
        if (byte == '-') {
            begin_number(true, State::second_need_digit);
            return;
        }
        if (is_digit(byte)) {
            begin_number(false, State::second_digits);
            append_digit(byte);
            return;
        }
        if (byte == ',') {
            fail_input("extra CSV field");
        }
        if (byte == '\n' || byte == '\r') {
            fail_input("missing destination integer");
        }
        if (byte == '"') {
            fail_input("quoted fields are not allowed");
        }
        fail_input("invalid destination integer");
    }

    void consume_second_need_digit(const char byte) {
        if (!is_digit(byte)) {
            fail_input("minus sign must be followed by a digit");
        }
        state_ = State::second_digits;
        append_digit(byte);
    }

    void consume_second_digits(const char byte) {
        if (is_digit(byte)) {
            append_digit(byte);
            return;
        }
        destination_ = completed_number();
        if (is_field_space(byte)) {
            state_ = State::second_trailing;
            return;
        }
        if (byte == '\n') {
            complete_line();
            return;
        }
        if (byte == '\r') {
            state_ = State::row_cr;
            return;
        }
        if (byte == ',') {
            fail_input("extra CSV field");
        }
        fail_input("trailing junk in destination field");
    }

    void consume_second_trailing(const char byte) {
        if (is_field_space(byte)) {
            return;
        }
        if (byte == '\n') {
            complete_line();
            return;
        }
        if (byte == '\r') {
            state_ = State::row_cr;
            return;
        }
        if (byte == ',') {
            fail_input("extra CSV field");
        }
        fail_input("trailing junk in destination field");
    }

    void consume_row_cr(const char byte) {
        if (byte != '\n') {
            fail_input("carriage return must be followed by line feed");
        }
        complete_line();
    }

    void deliver_edge() {
        if (data_rows_ == std::numeric_limits<std::uint64_t>::max()) {
            fail_input("CSV data-row count overflow");
        }

        in_consumer_ = true;
        try {
            consumer_(ParsedEdge{
                .source = source_,
                .destination = destination_,
                .input_line = line_,
            });
        } catch (...) {
            in_consumer_ = false;
            state_ = State::failed;
            throw;
        }
        in_consumer_ = false;
        if (state_ == State::failed) {
            fail_lifecycle(
                "edge CSV parser was poisoned by a reentrant consumer call"
            );
        }
        ++data_rows_;
    }

    void complete_line() {
        if (line_ == std::numeric_limits<std::uint64_t>::max()) {
            fail_input("CSV physical line count overflow");
        }
        deliver_edge();
        ++line_;
        row_touched_ = false;
        state_ = State::first_leading;
    }

    void finish_active_state() {
        switch (state_) {
            case State::header:
                if (header_index_ == kHeader.size()) {
                    throw EdgeCsvError(2U, "input has an empty vertex universe");
                }
                fail_input("missing or unterminated header from,to");
            case State::header_cr:
                fail_input("carriage return must be followed by line feed");
            case State::first_leading:
                if (!row_touched_) {
                    if (data_rows_ == 0U) {
                        fail_input("input has an empty vertex universe");
                    }
                    return;
                }
                fail_input("blank or incomplete data line");
            case State::first_need_digit:
                fail_input("minus sign must be followed by a digit");
            case State::first_digits:
            case State::first_trailing:
                fail_input("missing destination field");
            case State::second_leading:
                fail_input("missing destination integer");
            case State::second_need_digit:
                fail_input("minus sign must be followed by a digit");
            case State::second_digits:
            case State::second_trailing:
                destination_ = completed_number();
                deliver_edge();
                return;
            case State::row_cr:
                fail_input("carriage return must be followed by line feed");
            case State::finished:
            case State::failed:
                fail_lifecycle("edge CSV parser is terminal");
        }
    }

    EdgeConsumer consumer_;
    State state_ = State::header;
    std::size_t header_index_ = 0U;
    std::uint64_t line_ = 1U;
    std::uint64_t data_rows_ = 0U;
    bool row_touched_ = false;
    bool negative_ = false;
    bool has_digit_ = false;
    bool leading_zero_ = false;
    bool in_consumer_ = false;
    std::uint64_t magnitude_ = 0U;
    std::int32_t source_ = 0;
    std::int32_t destination_ = 0;
};

EdgeCsvParser::EdgeCsvParser(EdgeConsumer consumer)
    : impl_(std::make_unique<Impl>(std::move(consumer))) {}

EdgeCsvParser::~EdgeCsvParser() = default;

void EdgeCsvParser::consume(const std::span<const char> bytes) {
    impl_->consume(bytes);
}

EdgeCsvSummary EdgeCsvParser::finish() {
    return impl_->finish();
}

}  // namespace tbank::io
